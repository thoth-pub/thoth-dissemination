"""Offline tests for checking every delivered PDF, not just OAPEN's (DIS-PDF-CHECK-01, #147).

Each platform's own upload code runs with its network client faked, and the PDF
source answers through a fake host: no test sends a network request or needs credentials.
"""

import io
import zipfile
from unittest.mock import MagicMock, patch

import requests

import bkciuploader
import ebscouploader
import googleplayuploader
import jstoruploader
import museuploader
import proquestuploader
import souploader
from culuploader import CULUploader
from disseminator import UPLOADERS
from errors import DisseminationError
from iauploader import IAUploader
from oapensworduploader import OAPENSWORDUploader
from pdfcheck import PDFCheckError, PDFSource
from tests.test_oapen_pdf_integrity import (
    HTML_PAGE,
    SECRET,
    SIGNED_URL,
    VALID_PDF,
    FakePDFHost,
    FakeResponse,
)
from tests.test_oapensword_preflight import (
    PDF_PUBLICATION_ID,
    WORK_ID,
    FakeRemote,
    OfflineTestCase,
    make_uploader,
    thoth_metadata,
)
from uploader import Uploader


EPUB_URL = 'https://publisher.example/books/0001.epub'
EPUB_BYTES = b'PK\x03\x04 epub bytes'
REJECTION = 'Not uploading work {}: PDF publication {} was not accepted: '.format(
    WORK_ID, PDF_PUBLICATION_ID)

# Platforms whose upload code runs here, and the client each sends files through
CLIENTS = {
    'ProjectMUSE': (museuploader, 'SFTPClient'),
    'EBSCOHost': (ebscouploader, 'SFTPClient'),
    'ProQuest': (proquestuploader, 'SFTPClient'),
    'JSTOR': (jstoruploader, 'SFTPClient'),
    'ScienceOpen': (souploader, 'SFTPClient'),
    'BKCI': (bkciuploader, 'FTP'),
    'GooglePlay': (googleplayuploader, 'storage'),
}
# These deliver an EPUB without a PDF when Thoth lists no PDF
PDF_OPTIONAL = ('ProjectMUSE', 'EBSCOHost', 'ProQuest', 'GooglePlay')


def metadata(pdf_listed=True):
    """Thoth metadata listing an EPUB and, unless told otherwise, a PDF."""
    data = thoth_metadata()
    publications = data['data']['work']['publications']
    for publication in publications:
        if publication['publicationType'] == 'EPUB':
            publication['locations'][0]['fullTextUrl'] = EPUB_URL
    if not pdf_listed:
        publications[:] = [publication for publication in publications
                           if publication['publicationType'] != 'PDF']
    return data


def make_platform_uploader(platform, response=None, error=None, pdf_listed=True):
    """Build a platform's uploader around `metadata()`, with its PDF source on a fake host."""
    uploader_class = UPLOADERS[platform]
    uploader = uploader_class.__new__(uploader_class)
    uploader.work_id = WORK_ID
    uploader.export_url = 'https://export.example'
    uploader.version = '0.0.0-test'
    uploader.metadata = metadata(pdf_listed)
    host = FakePDFHost(response, error)
    uploader.pdf_source = PDFSource(http_get=host.get)
    # Metadata and credentials each platform reads before it downloads anything
    uploader.get_variable_from_env = lambda name, platform_name: 'value'
    uploader.get_publisher_id = lambda: 'publisher-id'
    uploader.get_publisher_name = lambda: 'Publisher'
    uploader.get_isbn = lambda publication_type: '9781999900014'
    uploader.get_formatted_metadata = lambda format: b'<metadata/>'
    uploader.get_cover_image = lambda required_format=None: b'cover bytes'
    uploader.get_cover_url = lambda: 'https://publisher.example/cover.jpg'
    uploader.parse_metadata = lambda: 'metadata'
    uploader.get_data_from_url = shared_download
    return uploader, host


def shared_download(url, expected_format=None):
    # Only EPUBs still use the shared download
    if url != EPUB_URL:
        raise AssertionError('unexpected shared download of {}'.format(url))
    return EPUB_BYTES


def pdf_publication(uploader):
    return [publication for publication in uploader.metadata['data']['work']['publications']
            if publication['publicationType'] == 'PDF'][0]


class PlatformRun:
    """Run one platform's upload with its network client faked, recording what it sent."""

    def __init__(self, platform, uploader):
        self.platform = platform
        self.uploader = uploader
        self.client = MagicMock()
        self.sent = {}

    def run(self):
        module, client_name = CLIENTS[self.platform]
        if client_name == 'storage':
            bucket = self.client.return_value.bucket.return_value

            def blob(name):
                stored = MagicMock()
                stored.upload_from_string.side_effect = (
                    lambda data, **kwargs: self.sent.__setitem__(name.split('/')[-1], data))
                return stored
            bucket.blob.side_effect = blob
            target = patch.object(module.storage, 'Client', self.client)
        else:
            session = self.client.return_value.__enter__.return_value
            session.putfo.side_effect = (
                lambda flo, remotepath: self.sent.__setitem__(remotepath, flo.getvalue()))
            session.storbinary.side_effect = (
                lambda command, flo: self.sent.__setitem__(
                    command.split(' ', 1)[1], flo.getvalue() if hasattr(flo, 'getvalue') else flo))
            target = patch.object(module, client_name, self.client)
        with target:
            return self.uploader.upload_to_platform()

    @property
    def files(self):
        """Every file name sent, including those inside a ZIP."""
        names = []
        for name, data in self.sent.items():
            names.append(name)
            if name.endswith('.zip'):
                names += zipfile.ZipFile(io.BytesIO(data)).namelist()
        return names


class EveryPlatformChecksItsPDFTests(OfflineTestCase):
    def test_no_platform_bypasses_the_shared_pdf_check(self):
        for platform, uploader_class in UPLOADERS.items():
            with self.subTest(platform):
                self.assertIs(uploader_class.get_publication_details,
                              Uploader.get_publication_details)
                self.assertIs(uploader_class.get_checked_pdf, Uploader.get_checked_pdf)
                self.assertIs(uploader_class.pdf_source, Uploader.pdf_source)
        self.assertIsInstance(Uploader.pdf_source, PDFSource)
        self.assertIs(Uploader.pdf_source.http_get, requests.get)

    def test_only_oapen_reports_a_rejection_in_its_own_words(self):
        for platform, uploader_class in UPLOADERS.items():
            with self.subTest(platform):
                if uploader_class is OAPENSWORDUploader:
                    self.assertIsNot(uploader_class.describe_rejected_pdf,
                                     Uploader.describe_rejected_pdf)
                else:
                    self.assertIs(uploader_class.describe_rejected_pdf,
                                  Uploader.describe_rejected_pdf)

    def test_an_uploader_built_without_init_still_checks_pdfs(self):
        # Internet Archive reconciliation builds its uploader this way
        uploader = IAUploader.__new__(IAUploader)

        self.assertIs(uploader.pdf_source, Uploader.pdf_source)

    def test_a_sound_pdf_is_returned_as_the_exact_bytes_checked(self):
        uploader, host = make_platform_uploader('JSTOR')

        with self.assertLogs(level='INFO') as logs:
            publication = uploader.get_publication_details('PDF')

        self.assertEqual(publication.bytes, VALID_PDF)
        self.assertEqual((publication.type, publication.id, publication.file_ext),
                         ('PDF', PDF_PUBLICATION_ID, '.pdf'))
        self.assertEqual(len(host.requests), 1)
        self.assertIn('PDF for work {}: publication {}, {} bytes, 3 pages'.format(
            WORK_ID, PDF_PUBLICATION_ID, len(VALID_PDF)), '\n'.join(logs.output))

    def test_a_rejected_pdf_names_the_work_and_publication_but_not_the_url(self):
        uploader, host = make_platform_uploader(
            'JSTOR', FakeResponse(HTML_PAGE, content_type='text/html', url=SIGNED_URL))
        pdf_publication(uploader)['locations'][0]['fullTextUrl'] = SIGNED_URL

        with self.assertRaises(PDFCheckError) as raised:
            uploader.get_publication_details('PDF')

        message = str(raised.exception)
        self.assertTrue(message.startswith(REJECTION), message)
        self.assertIn("cdn.example served 'text/html' instead of application/pdf", message)
        self.assertNotIn(SECRET, message)

    def test_a_missing_pdf_is_not_a_rejected_pdf(self):
        # No PDF publication, or one without a full-text URL
        for listed in (False, True):
            with self.subTest(listed=listed):
                uploader, host = make_platform_uploader('JSTOR', pdf_listed=listed)
                if listed:
                    pdf_publication(uploader)['locations'] = []

                with self.assertRaises(DisseminationError) as raised:
                    uploader.get_publication_details('PDF')

                self.assertNotIsInstance(raised.exception, PDFCheckError)
                self.assertEqual(host.requests, [])

    def test_other_formats_keep_the_shared_download(self):
        uploader, host = make_platform_uploader('ProjectMUSE')

        publication = uploader.get_publication_details('EPUB')

        self.assertEqual(publication.bytes, EPUB_BYTES)
        self.assertEqual(host.requests, [])


class PlatformDeliveryTests(OfflineTestCase):
    def test_every_platform_sends_the_checked_pdf_bytes(self):
        for platform in CLIENTS:
            with self.subTest(platform):
                uploader, host = make_platform_uploader(platform)
                run = PlatformRun(platform, uploader)

                with self.assertLogs(level='INFO'):
                    run.run()

                self.assertEqual(len(host.requests), 1)
                self.assertIn('9781999900014.pdf', run.files)
                if platform != 'ScienceOpen':
                    self.assertEqual(run.sent['9781999900014.pdf'], VALID_PDF)

    def test_a_rejected_pdf_stops_every_platform_before_it_sends_anything(self):
        cases = {
            'login page': (FakeResponse(HTML_PAGE, content_type='text/html'), None,
                           "served 'text/html' instead of application/pdf"),
            'truncated PDF': (FakeResponse(VALID_PDF[:-20]), None, 'could not be read as a PDF'),
            'timeout': (None, requests.exceptions.ReadTimeout('slow'), 'failed (ReadTimeout)'),
        }
        for platform in CLIENTS:
            for name, (response, error, expected) in cases.items():
                with self.subTest(platform=platform, case=name):
                    uploader, host = make_platform_uploader(platform, response, error)
                    run = PlatformRun(platform, uploader)

                    # ScienceOpen logs a failed PDF and exits; the others raise it to the CLI
                    with self.assertRaises((PDFCheckError, SystemExit)) as raised, \
                            self.assertLogs(level='INFO') as logs:
                        run.run()

                    if isinstance(raised.exception, SystemExit):
                        message = '\n'.join(logs.output)
                    else:
                        message = str(raised.exception)
                    self.assertIn(REJECTION, message)
                    self.assertIn(expected, message)
                    self.assertEqual(run.sent, {})
                    if platform != 'GooglePlay':
                        # The connection is never opened
                        run.client.assert_not_called()

    def test_platforms_with_an_optional_pdf_still_send_the_epub_when_no_pdf_is_listed(self):
        for platform in PDF_OPTIONAL:
            with self.subTest(platform):
                uploader, host = make_platform_uploader(platform, pdf_listed=False)
                run = PlatformRun(platform, uploader)

                with self.assertLogs(level='INFO'):
                    run.run()

                self.assertEqual(host.requests, [])
                self.assertEqual(run.sent['9781999900014.epub'], EPUB_BYTES)
                self.assertNotIn('9781999900014.pdf', run.files)

    def test_figshare_and_zenodo_create_nothing_for_a_rejected_pdf(self):
        for platform, search, create in (('Figshare', 'search_articles', 'create_project'),
                                         ('Zenodo', 'search_records', 'create_deposition')):
            with self.subTest(platform):
                uploader, host = make_platform_uploader(
                    platform, FakeResponse(HTML_PAGE, content_type='text/html'))
                uploader.api = MagicMock()
                getattr(uploader.api, search).return_value = (
                    [] if platform == 'Figshare' else {'total': 0})
                uploader.parse_metadata = lambda: ({}, {'title': 'T'}) if platform == 'Figshare' else {}

                with self.assertRaises(PDFCheckError) as raised:
                    uploader.upload_to_platform()

                self.assertIn(REJECTION, str(raised.exception))
                getattr(uploader.api, create).assert_not_called()
                uploader.api.upload_file.assert_not_called()

    def test_cul_creates_no_sword_item_for_a_rejected_pdf(self):
        remote = FakeRemote()
        uploader = make_uploader(remote, uploader_class=CULUploader)
        # Use the uploader's own PDF path instead of the preflight tests' stand-in
        del uploader.get_publication_details
        host = FakePDFHost(FakeResponse(VALID_PDF[:-20]), None, remote.events)
        uploader.pdf_source = PDFSource(http_get=host.get)

        with self.assertRaises(SystemExit), self.assertLogs(level='ERROR') as logs:
            uploader.upload_to_platform()

        self.assertIn(REJECTION, '\n'.join(logs.output))
        self.assertIn('pdf_download', remote.events)
        self.assertNotIn('create_item', remote.events)
