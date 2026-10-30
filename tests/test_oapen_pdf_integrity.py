"""Offline tests for the OAPEN PDF integrity check (DIS-OAPEN-SAFE-01C, #124).

PDFs are generated in memory with pikepdf. The PDF source, OAPEN search, Thoth
and SWORD are all faked: no test sends a network request or needs credentials.
"""

import hashlib
import importlib.metadata
import io
import re
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

import pikepdf
import requests
from requests.structures import CaseInsensitiveDict

import pdfcheck
from culuploader import CULUploader
from dspaceuploader import DSpaceUploader
from errors import DisseminationError
from oapensworduploader import OAPENSWORDUploader
from pdfcheck import (
    PDF_CHUNK_BYTES,
    PDF_MAX_BYTES,
    PDF_SOURCE_USER_AGENT,
    PDF_TIMEOUT,
    PDFCheckError,
    PDFSource,
    check_pdf,
    describe_pdf_problem,
)
from swordv2uploader import SwordV2Uploader
from tests.test_oapensword_preflight import (
    PDF_PUBLICATION_ID,
    PDF_URL,
    WORK_ID,
    FakeRemote,
    FakeSwordApi,
    OfflineTestCase,
    make_uploader,
    oapen_location,
    thoth_metadata,
)
from uploader import Uploader


ROOT = Path(__file__).resolve().parents[1]

SECRET = 's3cr3t-token'
SIGNED_URL = 'https://cdn.example/books/0001.pdf?X-Amz-Signature={}'.format(SECRET)


def make_pdf(pages=3, encryption=None, linearize=False):
    """Build a small PDF whose pages each carry a text content stream."""
    pdf = pikepdf.Pdf.new()
    for number in range(pages):
        pdf.add_blank_page(page_size=(200, 200))
        pdf.pages[number].Contents = pdf.make_stream(
            b'BT /F1 12 Tf 20 100 Td (Page %d) Tj ET' % number)
    buffer = io.BytesIO()
    # qpdf cannot derive a deterministic ID for an encrypted file
    pdf.save(buffer, encryption=encryption, linearize=linearize,
             deterministic_id=encryption is None)
    return buffer.getvalue()


def corrupt_stream_pdf():
    """A one-page PDF whose Flate-compressed content stream does not decode."""
    pdf = pikepdf.Pdf.new()
    pdf.add_blank_page(page_size=(200, 200))
    stream = pdf.make_stream(zlib.compress(b'BT /F1 12 Tf 20 100 Td (Hello) Tj ET'))
    stream.Filter = pikepdf.Name.FlateDecode
    pdf.pages[0].Contents = stream
    buffer = io.BytesIO()
    pdf.save(buffer, compress_streams=False, deterministic_id=True)
    data = buffer.getvalue()
    start = data.index(b'stream\n') + len(b'stream\n')
    return data[:start] + b'\x00\xff\x00\xff' + data[start + 4:]


VALID_PDF = make_pdf()
HTML_PAGE = b'<!DOCTYPE html><html><body>Please sign in</body></html>'


class FakeResponse:
    """A streamed `requests` response from a PDF source."""

    def __init__(self, body=VALID_PDF, status_code=200, content_type='application/pdf',
                 content_length='auto', encoding=None, url=PDF_URL, error=None):
        self.status_code = status_code
        self.url = url
        self.headers = CaseInsensitiveDict()
        if content_type is not None:
            self.headers['Content-Type'] = content_type
        if content_length == 'auto':
            content_length = str(len(body))
        if content_length is not None:
            self.headers['Content-Length'] = content_length
        if encoding is not None:
            self.headers['Content-Encoding'] = encoding
        self.body = body
        # Raised after the first chunk, as a dropped connection would be
        self.error = error
        self.chunk_sizes = []
        self.chunks_sent = 0
        self.closed = False

    def iter_content(self, chunk_size):
        self.chunk_sizes.append(chunk_size)
        third = max(1, len(self.body) // 3)
        for start in range(0, len(self.body), third):
            if self.error is not None and self.chunks_sent == 1:
                raise self.error
            self.chunks_sent += 1
            yield self.body[start:start + third]

    def close(self):
        self.closed = True


class FakePDFHost:
    """Records PDF source requests and answers each with one response or error."""

    def __init__(self, response=None, error=None, events=None):
        self.response = FakeResponse() if response is None and error is None else response
        self.error = error
        self.events = events if events is not None else []
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        self.events.append('pdf_download')
        if self.error is not None:
            raise self.error
        return self.response


class PDFCheckTests(unittest.TestCase):
    def test_a_sound_pdf_passes_with_its_page_count(self):
        self.assertEqual(check_pdf(VALID_PDF), 3)
        self.assertEqual(check_pdf(make_pdf(pages=1)), 1)
        self.assertEqual(check_pdf(make_pdf(linearize=True)), 3)

    def test_unsound_files_are_rejected(self):
        cases = {
            'empty': (b'', 'the file is empty'),
            'HTML error page': (HTML_PAGE, 'no PDF header'),
            'header only': (b'%PDF-1.7\n', 'could not be read as a PDF'),
            'truncated': (VALID_PDF[:len(VALID_PDF) // 2], 'could not be read as a PDF'),
            'missing end of file': (VALID_PDF[:-20], 'could not be read as a PDF'),
            'wrong cross-reference offset': (
                re.sub(rb'startxref\s+\d+', b'startxref\n999999', VALID_PDF),
                'could not be read as a PDF'),
            'broken page tree': (VALID_PDF.replace(b'/Type /Pages', b'/Type /Pagex', 1),
                                 '1 structural problem(s)'),
            'corrupt compressed stream': (corrupt_stream_pdf(), 'errors while decoding'),
            'no pages': (make_pdf(pages=0), 'the file has no pages'),
            'user password': (make_pdf(encryption=pikepdf.Encryption(owner='o', user='u')),
                              'encrypted with a password'),
            'owner password only': (make_pdf(encryption=pikepdf.Encryption(owner='o', user='')),
                                    'the file is encrypted'),
        }
        for name, (data, expected) in cases.items():
            with self.subTest(name):
                with self.assertRaises(PDFCheckError) as raised:
                    check_pdf(data)

                self.assertIn(expected, str(raised.exception))
                self.assertNotIn('BytesIO', str(raised.exception))

    def test_a_damaged_cross_reference_table_is_not_rebuilt(self):
        damaged = re.sub(rb'startxref\s+\d+', b'startxref\n999999', VALID_PDF)
        # qpdf could repair this file, which would hide the damage
        with pikepdf.open(io.BytesIO(damaged)) as repaired:
            self.assertEqual(len(repaired.pages), 3)

        with self.assertRaises(PDFCheckError):
            check_pdf(damaged)

    def test_unexpected_parser_failures_are_reported_by_type_only(self):
        with patch('pikepdf.open', side_effect=RuntimeError('internal detail')):
            with self.assertRaises(PDFCheckError) as raised:
                check_pdf(VALID_PDF)

        self.assertEqual(str(raised.exception), 'the file could not be checked (RuntimeError)')

    def test_parser_messages_are_shortened(self):
        self.assertEqual(len(describe_pdf_problem('x ' * 500)), 200)
        self.assertEqual(describe_pdf_problem('stream <_io.BytesIO object at 0x10fe> (offset 9): x'),
                         'PDF (offset 9): x')


class PDFSourceTests(OfflineTestCase):
    def fetch(self, response=None, error=None, url=PDF_URL):
        host = FakePDFHost(response, error)
        return host, PDFSource(http_get=host.get).fetch(url)

    def test_the_whole_body_is_returned_from_one_bounded_request(self):
        response = FakeResponse()

        host, data = self.fetch(response)

        self.assertEqual(data, VALID_PDF)
        self.assertEqual(host.requests, [(PDF_URL, {
            'headers': {'User-Agent': PDF_SOURCE_USER_AGENT, 'Accept-Encoding': 'identity'},
            'timeout': (10, 90),
            'stream': True,
        })])
        self.assertEqual(response.chunk_sizes, [1024 * 1024])
        self.assertTrue(response.closed)
        self.assertEqual((PDF_MAX_BYTES, PDF_TIMEOUT, PDF_CHUNK_BYTES),
                         (512 * 1024 * 1024, (10, 90), 1024 * 1024))

    def test_acceptable_headers(self):
        cases = {
            'media type parameter': FakeResponse(content_type='application/pdf; charset=binary'),
            'upper-case media type': FakeResponse(content_type='Application/PDF'),
            'no Content-Length': FakeResponse(content_length=None),
            # Requests decodes the body, so it no longer matches the compressed length
            'compressed transfer': FakeResponse(content_length='10', encoding='gzip'),
        }
        for name, response in cases.items():
            with self.subTest(name):
                self.assertEqual(self.fetch(response)[1], VALID_PDF)

    def test_unusable_responses_are_rejected_without_naming_the_url(self):
        too_large = str(PDF_MAX_BYTES + 1)
        cases = {
            'forbidden': (FakeResponse(HTML_PAGE, 403, 'text/html'), 'answered HTTP status 403'),
            'not found': (FakeResponse(HTML_PAGE, 404, 'text/html'), 'answered HTTP status 404'),
            'server error': (FakeResponse(b'', 503, None), 'answered HTTP status 503'),
            'login page': (FakeResponse(HTML_PAGE, content_type='text/html; charset=utf-8'),
                           "served 'text/html' instead of application/pdf"),
            'no content type': (FakeResponse(content_type=None), 'served no content type'),
            'invalid length': (FakeResponse(content_length='12x'), 'invalid Content-Length'),
            # Passes str.isdigit(), but int() cannot read it
            'non-ASCII digit length': (FakeResponse(content_length='²'),
                                       'invalid Content-Length'),
            'declared too large': (FakeResponse(content_length=too_large),
                                   'declared {} bytes, more than the'.format(too_large)),
            'short body': (FakeResponse(content_length=str(len(VALID_PDF) + 100)),
                           'declared {} bytes but sent {}'.format(len(VALID_PDF) + 100,
                                                                  len(VALID_PDF))),
        }
        for name, (response, expected) in cases.items():
            response.url = SIGNED_URL
            with self.subTest(name):
                with self.assertRaises(PDFCheckError) as raised:
                    self.fetch(response, url=SIGNED_URL)

                message = str(raised.exception)
                self.assertIn('cdn.example', message)
                self.assertIn(expected, message)
                self.assertNotIn(SECRET, message)
                self.assertNotIn('/books/', message)
                self.assertTrue(response.closed)

    def test_a_declared_oversize_body_is_not_read(self):
        response = FakeResponse(content_length=str(PDF_MAX_BYTES + 1))

        with self.assertRaises(PDFCheckError):
            self.fetch(response)

        self.assertEqual(response.chunks_sent, 0)

    def test_an_oversize_body_without_a_declared_length_stops_while_streaming(self):
        response = FakeResponse(b'%PDF-' + b'x' * 995, content_length=None)

        with patch.object(pdfcheck, 'PDF_MAX_BYTES', 100):
            with self.assertRaises(PDFCheckError) as raised:
                self.fetch(response)

        self.assertIn('sent more than the 100-byte limit', str(raised.exception))
        self.assertEqual(response.chunks_sent, 1)

    def test_request_failures_are_reported_by_type_only(self):
        for error in (requests.exceptions.ConnectTimeout(SIGNED_URL),
                      requests.exceptions.ReadTimeout(SIGNED_URL),
                      requests.exceptions.ConnectionError(SIGNED_URL),
                      requests.exceptions.TooManyRedirects(SIGNED_URL),
                      requests.exceptions.SSLError(SIGNED_URL)):
            with self.subTest(type(error).__name__):
                with self.assertRaises(PDFCheckError) as raised:
                    self.fetch(error=error, url=SIGNED_URL)

                self.assertEqual(str(raised.exception), 'the request to cdn.example failed '
                                 '({})'.format(type(error).__name__))

    def test_a_connection_lost_partway_through_the_body_is_an_error(self):
        for error in (requests.exceptions.ChunkedEncodingError(SIGNED_URL),
                      requests.exceptions.ConnectionError(SIGNED_URL)):
            with self.subTest(type(error).__name__):
                response = FakeResponse(error=error, url=SIGNED_URL)

                with self.assertRaises(PDFCheckError) as raised:
                    self.fetch(response, url=SIGNED_URL)

                self.assertEqual(str(raised.exception), 'the download from cdn.example failed '
                                 '({})'.format(type(error).__name__))
                self.assertTrue(response.closed)

    def test_errors_name_the_host_redirected_to(self):
        response = FakeResponse(HTML_PAGE, content_type='text/html',
                                url='https://login.example/sso?next={}'.format(SECRET))

        with self.assertRaises(PDFCheckError) as raised:
            self.fetch(response, url=SIGNED_URL)

        self.assertIn('login.example served', str(raised.exception))
        self.assertNotIn(SECRET, str(raised.exception))


class OAPENUploadIntegrityTests(OfflineTestCase):
    def make(self, response=None, error=None, metadata=None):
        remote = FakeRemote()
        uploader = make_uploader(remote, metadata)
        # Use the uploader's own PDF path instead of the preflight tests' stand-in
        del uploader.get_publication_details
        host = FakePDFHost(response, error, remote.events)
        uploader.pdf_source = PDFSource(http_get=host.get)
        return remote, uploader, host

    def test_a_sound_pdf_is_uploaded_as_the_exact_bytes_checked(self):
        remote, uploader, host = self.make()
        fetched = []
        fetch = uploader.pdf_source.fetch
        uploader.pdf_source.fetch = lambda url: fetched.append(fetch(url)) or fetched[-1]
        publications = []
        get_details = uploader.get_publication_details
        uploader.get_publication_details = (
            lambda publication_type: publications.append(get_details(publication_type))
            or publications[-1])
        uploaded = []
        upload_pdf = FakeSwordApi.upload_pdf

        def record_upload(api, edit_media_iri, payload):
            uploaded.append(payload)
            return upload_pdf(api, edit_media_iri, payload)

        with patch.object(FakeSwordApi, 'upload_pdf', record_upload), \
                self.assertLogs(level='INFO') as logs:
            locations = uploader.upload_to_platform()

        self.assertEqual(len(host.requests), 1)
        self.assertEqual(len(uploaded), 1)
        self.assertIs(uploaded[0], fetched[0])
        self.assertEqual(uploaded[0], VALID_PDF)
        self.assertEqual([(publication.type, publication.id, publication.file_ext,
                           publication.source_url) for publication in publications],
                         [('PDF', PDF_PUBLICATION_ID, '.pdf', PDF_URL)])
        self.assertIs(publications[0].bytes, fetched[0])
        self.assertEqual([location.publication_id for location in locations],
                         [PDF_PUBLICATION_ID])
        events = [event for event in remote.events if not event.startswith('search')]
        self.assertEqual(events, ['export_metadata', 'pdf_download', 'create_item',
                                  'upload_metadata', 'upload_pdf', 'complete_deposit'])
        self.assertIn('PDF for work {}: publication {}, {} bytes, 3 pages, SHA-256 {}'.format(
            WORK_ID, PDF_PUBLICATION_ID, len(VALID_PDF), hashlib.sha256(VALID_PDF).hexdigest()),
            '\n'.join(logs.output))

    def test_an_unaccepted_pdf_stops_the_upload_before_any_create(self):
        metadata = thoth_metadata()
        pdf = [publication for publication in metadata['data']['work']['publications']
               if publication['publicationType'] == 'PDF'][0]
        pdf['locations'][0]['fullTextUrl'] = SIGNED_URL
        cases = {
            'login page': (FakeResponse(HTML_PAGE, content_type='text/html', url=SIGNED_URL), None,
                           "served 'text/html' instead of application/pdf"),
            'truncated PDF': (FakeResponse(VALID_PDF[:-20], url=SIGNED_URL), None,
                              'could not be read as a PDF'),
            'encrypted PDF': (FakeResponse(make_pdf(encryption=pikepdf.Encryption(owner='o')),
                                           url=SIGNED_URL), None, 'the file is encrypted'),
            'forbidden': (FakeResponse(HTML_PAGE, 403, 'text/html', url=SIGNED_URL), None,
                          'answered HTTP status 403'),
            'timeout': (None, requests.exceptions.ReadTimeout(SIGNED_URL),
                        'failed (ReadTimeout)'),
        }
        for name, (response, error, expected) in cases.items():
            with self.subTest(name):
                remote, uploader, host = self.make(response, error, metadata)

                with self.assertRaises(SystemExit), self.assertLogs(level='ERROR') as logs:
                    uploader.upload_to_platform()

                output = '\n'.join(logs.output)
                self.assertIn('Not creating OAPEN item for work {}: PDF publication {} was not '
                              'accepted: '.format(WORK_ID, PDF_PUBLICATION_ID), output)
                self.assertIn(expected, output)
                self.assertNotIn(SECRET, output)
                self.assertEqual(host.requests[0][0], SIGNED_URL)
                self.assertNotIn('create_item', remote.events)
                # The preflight's clearance does not outlive the stopped upload
                with self.assertRaises(DisseminationError):
                    uploader.api.create_item(None)
                self.assertNotIn('create_item', remote.events)

    def test_a_held_preflight_downloads_no_pdf(self):
        remote, uploader, host = self.make(
            metadata=thoth_metadata(pdf_locations=[oapen_location()]))

        with self.assertRaises(DisseminationError):
            uploader.upload_to_platform()

        self.assertEqual(host.requests, [])

    def test_other_formats_use_the_shared_download(self):
        remote, uploader, host = self.make()
        with patch.object(Uploader, 'get_data_from_url', return_value=b'epub bytes') as shared:
            publication = uploader.get_publication_details('EPUB')

        self.assertEqual(publication.bytes, b'epub bytes')
        shared.assert_called_once()
        self.assertEqual(host.requests, [])

    def test_oapen_uses_the_shared_pdf_check_with_its_own_message(self):
        # Every platform checks PDFs since #147; OAPEN differs only in how it reports a rejection
        for uploader_class in (DSpaceUploader, SwordV2Uploader, CULUploader, OAPENSWORDUploader):
            with self.subTest(uploader_class.__name__):
                self.assertIs(uploader_class.get_publication_details,
                              Uploader.get_publication_details)
                self.assertIs(uploader_class.get_checked_pdf, Uploader.get_checked_pdf)
        self.assertIsNot(OAPENSWORDUploader.describe_rejected_pdf,
                         Uploader.describe_rejected_pdf)

    def test_uploader_downloads_pdfs_with_requests(self):
        uploader = make_uploader(FakeRemote())

        self.assertIsInstance(uploader.pdf_source, PDFSource)
        self.assertIs(uploader.pdf_source.http_get, requests.get)


class DependencyContractTests(unittest.TestCase):
    def test_the_pinned_validator_is_installed_and_supports_every_runtime(self):
        pins = dict(line.split('==') for line in
                    (ROOT / 'requirements.txt').read_text().split() if '==' in line)
        self.assertEqual(pins['pikepdf'], '10.17.0')
        self.assertEqual(importlib.metadata.version('pikepdf'), pins['pikepdf'])
        requires = importlib.metadata.metadata('pikepdf')['Requires-Python']
        minimum = tuple(map(int, re.fullmatch(r'>=\s*(\d+)\.(\d+)', requires).groups()))
        # The Docker image and the workflows that install requirements.txt
        runtimes = re.findall(r'^FROM python:(\d+)\.(\d+)', (ROOT / 'Dockerfile').read_text(),
                              re.MULTILINE)
        for workflow in ('tests.yml', 'disseminate.yml'):
            runtimes += re.findall(r"python-version: '(\d+)\.(\d+)'",
                                   (ROOT / '.github' / 'workflows' / workflow).read_text())
        self.assertGreaterEqual(len(runtimes), 3)
        for runtime in runtimes:
            with self.subTest(runtime):
                self.assertGreaterEqual(tuple(map(int, runtime)), minimum)


if __name__ == '__main__':
    unittest.main()
