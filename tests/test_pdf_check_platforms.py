"""Offline tests for checking every delivered PDF, not just OAPEN's (DIS-PDF-CHECK-01, #147),
for links that serve no PDF file (DIS-PDF-CHECK-02, #155) and for PDFs whose only
problem is a page Resources entry that readers repair (DIS-PDF-CHECK-03, #158).

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
from disseminator import UPLOADERS
from errors import DisseminationError, InternetArchiveDesiredStateError
from iauploader import IAUploader
from oapensworduploader import OAPENSWORDUploader
from pdfcheck import PDFCheckError, PDFNotServedError, PDFSource
from tests.test_oapen_pdf_integrity import (
    HTML_PAGE,
    REPAIRED_PDF,
    SECRET,
    SIGNED_URL,
    VALID_PDF,
    FakePDFHost,
    FakeResponse,
)
from tests.test_oapensword_preflight import (
    PDF_PUBLICATION_ID,
    WORK_ID,
    OfflineTestCase,
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
# These deliver an EPUB without a PDF when the PDF publication has no file
PDF_OPTIONAL = ('ProjectMUSE', 'EBSCOHost', 'ProQuest', 'GooglePlay')
# MUSE and ProQuest name every file after the PDF ISBN, so only these send an EPUB
# for a work with no PDF publication at all
PDF_PUBLICATION_OPTIONAL = ('EBSCOHost', 'GooglePlay')
# These cannot send without the PDF
PDF_REQUIRED = ('JSTOR', 'ScienceOpen', 'BKCI')
# Logged when a PDF passes with page Resources that readers repair (#158)
REPAIRED_WARNING = ('PDF publication {} of work {} from {} has 1 page(s) with missing or invalid '
                    'Resources, which readers repair; sending it unchanged')


def not_served_cases():
    """Fresh responses from links that serve no PDF file, and the reason each gives (#155)."""
    return {
        'landing page': (FakeResponse(HTML_PAGE, content_type='text/html'),
                         "publisher.example served 'text/html' instead of application/pdf"),
        'not found': (FakeResponse(HTML_PAGE, 404, 'text/html'),
                      'publisher.example answered HTTP status 404'),
        'forbidden': (FakeResponse(HTML_PAGE, 403, 'text/html'),
                      'publisher.example answered HTTP status 403'),
        'octet stream that is not a PDF': (
            FakeResponse(HTML_PAGE, content_type='application/octet-stream'),
            "publisher.example served 'application/octet-stream' that is not a PDF"),
    }


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

    def file(self, name):
        """The bytes sent under a file name, looking inside a ZIP too."""
        if name in self.sent:
            return self.sent[name]
        for data in [data for sent, data in self.sent.items() if sent.endswith('.zip')]:
            archive = zipfile.ZipFile(io.BytesIO(data))
            if name in archive.namelist():
                return archive.read(name)
        raise KeyError(name)


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
            'JSTOR', FakeResponse(VALID_PDF[:-20], url=SIGNED_URL))
        pdf_publication(uploader)['locations'][0]['fullTextUrl'] = SIGNED_URL

        with self.assertRaises(PDFCheckError) as raised:
            uploader.get_publication_details('PDF')

        message = str(raised.exception)
        self.assertTrue(message.startswith(REJECTION), message)
        self.assertIn('could not be read as a PDF', message)
        self.assertNotIn(SECRET, message)

    def test_a_link_without_a_pdf_names_the_work_and_publication_but_not_the_url(self):
        uploader, host = make_platform_uploader(
            'JSTOR', FakeResponse(HTML_PAGE, content_type='text/html', url=SIGNED_URL))
        pdf_publication(uploader)['locations'][0]['fullTextUrl'] = SIGNED_URL

        with self.assertRaises(PDFNotServedError) as raised, \
                self.assertLogs(level='WARNING') as logs:
            uploader.get_publication_details('PDF')

        self.assertNotIsInstance(raised.exception, PDFCheckError)
        message = str(raised.exception)
        self.assertTrue(message.startswith(REJECTION), message)
        self.assertIn("cdn.example served 'text/html' instead of application/pdf", message)
        self.assertNotIn(SECRET, message)
        warning = '\n'.join(logs.output)
        self.assertIn('PDF publication {} of work {} has no PDF file to send: cdn.example served '
                      "'text/html' instead of application/pdf".format(PDF_PUBLICATION_ID, WORK_ID),
                      warning)
        self.assertNotIn(SECRET, warning)

    def test_a_pdf_with_repaired_page_resources_is_returned_unchanged_with_a_warning(self):
        uploader, host = make_platform_uploader(
            'JSTOR', FakeResponse(REPAIRED_PDF, url=SIGNED_URL))
        pdf_publication(uploader)['locations'][0]['fullTextUrl'] = SIGNED_URL

        with self.assertLogs(level='INFO') as logs:
            publication = uploader.get_publication_details('PDF')

        self.assertEqual(publication.bytes, REPAIRED_PDF)
        self.assertEqual(len(host.requests), 1)
        warnings = [record.getMessage() for record in logs.records
                    if record.levelname == 'WARNING']
        self.assertEqual(warnings, [REPAIRED_WARNING.format(
            PDF_PUBLICATION_ID, WORK_ID, 'cdn.example')])
        self.assertIn('PDF for work {}: publication {}, {} bytes, 3 pages'.format(
            WORK_ID, PDF_PUBLICATION_ID, len(REPAIRED_PDF)), '\n'.join(logs.output))
        self.assertNotIn(SECRET, '\n'.join(logs.output))

    def test_a_sound_pdf_logs_no_warning(self):
        uploader, host = make_platform_uploader('JSTOR')

        with self.assertLogs(level='INFO') as logs:
            uploader.get_publication_details('PDF')

        self.assertEqual([record.levelname for record in logs.records], ['INFO'])

    def test_an_octet_stream_pdf_is_returned_as_the_exact_bytes_checked(self):
        uploader, host = make_platform_uploader(
            'JSTOR', FakeResponse(content_type='application/octet-stream'))

        with self.assertLogs(level='INFO') as logs:
            publication = uploader.get_publication_details('PDF')

        self.assertEqual(publication.bytes, VALID_PDF)
        self.assertIn('PDF for work {}: publication {}, {} bytes, 3 pages'.format(
            WORK_ID, PDF_PUBLICATION_ID, len(VALID_PDF)), '\n'.join(logs.output))

    def test_a_missing_pdf_is_not_a_rejected_pdf(self):
        # No PDF publication, no location, or a canonical location with only a landing page
        for case in ('no publication', 'no location', None, '', '  '):
            with self.subTest(case=case):
                uploader, host = make_platform_uploader(
                    'JSTOR', pdf_listed=case != 'no publication')
                if case == 'no location':
                    pdf_publication(uploader)['locations'] = []
                elif case != 'no publication':
                    pdf_publication(uploader)['locations'][0]['fullTextUrl'] = case

                with self.assertRaises(DisseminationError) as raised:
                    uploader.get_publication_details('PDF')

                self.assertNotIsInstance(raised.exception, PDFCheckError)
                self.assertEqual(str(raised.exception), 'No PDF {} found for Work'.format(
                    'publication' if case == 'no publication' else 'Full Text URL'))
                self.assertEqual(host.requests, [])

    def test_other_formats_keep_the_shared_download(self):
        uploader, host = make_platform_uploader('ProjectMUSE')

        publication = uploader.get_publication_details('EPUB')

        self.assertEqual(publication.bytes, EPUB_BYTES)
        self.assertEqual(host.requests, [])


class PlatformDeliveryTests(OfflineTestCase):
    def test_every_platform_sends_the_checked_pdf_bytes(self):
        for platform in CLIENTS:
            for content_type in ('application/pdf', 'application/octet-stream'):
                with self.subTest(platform=platform, content_type=content_type):
                    uploader, host = make_platform_uploader(
                        platform, FakeResponse(content_type=content_type))
                    run = PlatformRun(platform, uploader)

                    with self.assertLogs(level='INFO'):
                        run.run()

                    self.assertEqual(len(host.requests), 1)
                    self.assertIn('9781999900014.pdf', run.files)
                    self.assertEqual(run.file('9781999900014.pdf'), VALID_PDF)

    def test_every_platform_sends_a_pdf_with_repaired_page_resources_unchanged(self):
        # Platforms where the PDF is optional and platforms that need it alike
        self.assertTrue(set(PDF_OPTIONAL) & set(CLIENTS) and set(PDF_REQUIRED) & set(CLIENTS))
        for platform in CLIENTS:
            with self.subTest(platform=platform):
                uploader, host = make_platform_uploader(platform, FakeResponse(REPAIRED_PDF))
                run = PlatformRun(platform, uploader)

                with self.assertLogs(level='INFO') as logs:
                    run.run()

                self.assertEqual(len(host.requests), 1)
                self.assertEqual(run.file('9781999900014.pdf'), REPAIRED_PDF)
                self.assertIn(REPAIRED_WARNING.format(PDF_PUBLICATION_ID, WORK_ID,
                                                      'publisher.example'),
                              [record.getMessage() for record in logs.records
                               if record.levelname == 'WARNING'])

    def test_a_rejected_pdf_stops_every_platform_before_it_sends_anything(self):
        cases = {
            'truncated PDF': (FakeResponse(VALID_PDF[:-20]), None, 'could not be read as a PDF'),
            'truncated octet-stream PDF': (
                FakeResponse(VALID_PDF[:-20], content_type='application/octet-stream'), None,
                'could not be read as a PDF'),
            'timeout': (None, requests.exceptions.ReadTimeout('slow'), 'failed (ReadTimeout)'),
            'server error': (FakeResponse(HTML_PAGE, 503, 'text/html'), None,
                             'answered HTTP status 503'),
            'rate limited': (FakeResponse(HTML_PAGE, 429, 'text/html'), None,
                             'answered HTTP status 429'),
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

    def test_platforms_with_an_optional_pdf_send_the_epub_when_the_pdf_has_no_file(self):
        for platform in PDF_OPTIONAL:
            for full_text_url in (None, ''):
                with self.subTest(platform=platform, full_text_url=full_text_url):
                    uploader, host = make_platform_uploader(platform)
                    pdf_publication(uploader)['locations'][0]['fullTextUrl'] = full_text_url
                    # The platform's own ISBN lookup
                    del uploader.get_isbn
                    run = PlatformRun(platform, uploader)

                    with self.assertLogs(level='INFO'):
                        run.run()

                    self.assertEqual(host.requests, [])
                    # MUSE and ProQuest name files after the PDF ISBN, the others after the
                    # ISBN of the first file they send
                    isbn = ('9781999900038' if platform in PDF_PUBLICATION_OPTIONAL
                            else '9781999900021')
                    self.assertEqual(run.sent['{}.epub'.format(isbn)], EPUB_BYTES)
                    self.assertFalse([name for name in run.files if name.endswith('.pdf')])

    def test_platforms_with_an_optional_pdf_send_the_epub_when_the_pdf_link_has_no_pdf(self):
        for platform in PDF_OPTIONAL:
            for name, (response, expected) in not_served_cases().items():
                with self.subTest(platform=platform, case=name):
                    uploader, host = make_platform_uploader(platform, response)
                    run = PlatformRun(platform, uploader)

                    with self.assertLogs(level='INFO') as logs:
                        run.run()

                    self.assertEqual(len(host.requests), 1)
                    self.assertEqual(run.sent['9781999900014.epub'], EPUB_BYTES)
                    self.assertFalse([name for name in run.files if name.endswith('.pdf')])
                    self.assertIn('WARNING', '\n'.join(logs.output))
                    self.assertIn('has no PDF file to send: ' + expected,
                                  '\n'.join(logs.output))

    def test_platforms_that_need_the_pdf_stop_when_the_pdf_link_has_no_pdf(self):
        for platform in PDF_REQUIRED:
            for name, (response, expected) in not_served_cases().items():
                with self.subTest(platform=platform, case=name):
                    uploader, host = make_platform_uploader(platform, response)
                    run = PlatformRun(platform, uploader)

                    # ScienceOpen logs a failed PDF and exits; the others raise it to the CLI
                    with self.assertRaises((PDFNotServedError, SystemExit)) as raised, \
                            self.assertLogs(level='INFO') as logs:
                        run.run()

                    if isinstance(raised.exception, SystemExit):
                        message = '\n'.join(logs.output)
                    else:
                        message = str(raised.exception)
                    self.assertIn(REJECTION, message)
                    self.assertIn(expected, message)
                    self.assertEqual(run.sent, {})
                    run.client.assert_not_called()

    def test_internet_archive_reports_a_pdf_link_without_a_pdf_as_a_pdf_failure(self):
        # Reconciliation reports a `pdf` failure as pdf_source_unavailable
        for name, (response, expected) in not_served_cases().items():
            with self.subTest(name):
                uploader, host = make_platform_uploader('InternetArchive', response)
                uploader.get_formatted_metadata = lambda format: b'{"work": "metadata"}'

                with self.assertRaises(InternetArchiveDesiredStateError) as raised, \
                        self.assertLogs(level='WARNING'):
                    uploader.build_desired_state()

                self.assertEqual(raised.exception.source, 'pdf')
                self.assertIsInstance(raised.exception.__cause__, PDFNotServedError)
                self.assertIn(REJECTION + expected, str(raised.exception))
                self.assertEqual(len(host.requests), 1)

    def test_ebsco_and_google_play_send_the_epub_when_no_pdf_is_listed(self):
        for platform in PDF_PUBLICATION_OPTIONAL:
            with self.subTest(platform):
                uploader, host = make_platform_uploader(platform, pdf_listed=False)
                del uploader.get_isbn
                run = PlatformRun(platform, uploader)

                with self.assertLogs(level='INFO'):
                    run.run()

                self.assertEqual(host.requests, [])
                # Named after the EPUB ISBN when there is no PDF publication
                self.assertEqual(run.sent['9781999900038.epub'], EPUB_BYTES)
                self.assertFalse([name for name in run.files if name.endswith('.pdf')])

    def make_zenodo(self, response):
        uploader, host = make_platform_uploader('Zenodo', response)
        uploader.api = MagicMock()
        uploader.api.search_records.return_value = {'total': 0}
        uploader.api.create_deposition.return_value = (1, 'bucket')
        uploader.api.upload_file.return_value = 'md5'
        uploader.parse_metadata = lambda: {}
        return uploader

    def test_zenodo_creates_nothing_for_a_rejected_pdf(self):
        uploader = self.make_zenodo(FakeResponse(VALID_PDF[:-20]))

        with self.assertRaises(PDFCheckError) as raised:
            uploader.upload_to_platform()

        self.assertIn(REJECTION, str(raised.exception))
        uploader.api.create_deposition.assert_not_called()
        uploader.api.upload_file.assert_not_called()

    def test_zenodo_sends_the_other_files_when_the_pdf_link_has_no_pdf(self):
        for name, (response, expected) in not_served_cases().items():
            with self.subTest(name):
                uploader = self.make_zenodo(response)

                with self.assertLogs(level='INFO'):
                    locations = uploader.upload_to_platform()

                sent = [call.args[1] for call in uploader.api.upload_file.call_args_list]
                self.assertEqual(sent, ['{}_book.epub'.format(WORK_ID),
                                        '{}_metadata.json'.format(WORK_ID)])
                self.assertEqual(len(locations), 1)
                uploader.api.publish_deposition.assert_called_once_with(1)
