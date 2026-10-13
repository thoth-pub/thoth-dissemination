"""
Offline tests for DIS-MAINT-GCS-01: Google Play uploads use google-cloud-storage
3.x with an enforced CRC32c checksum and no SDK upload retry.

Level 1 runs the real uploader against a mocked storage client. Level 2 runs it
against the real SDK with anonymous credentials and a fake HTTP transport, so
the SDK's own checksum, retry and cleanup code decides which requests are sent.
Neither level shows how the real Cloud Storage service or bucket behaves.
"""

import base64
import io
import json
import logging
import os
import socket
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch
from urllib.parse import parse_qsl, quote, urlsplit

import google.auth
import google_crc32c
import requests
from google.api_core import exceptions as api_exceptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from google.cloud.storage import exceptions as storage_exceptions
from google.cloud.storage._bucket_metadata_cache import BucketMetadataCache

import googleplayuploader
from errors import DisseminationError
from googleplayuploader import GooglePlayUploader
from uploader import Publication


# Synthetic, non-secret values only. Every provider-controlled value below
# contains SENTINEL, so its absence from all output proves nothing leaked.
SENTINEL = 'SENTINEL-dis-maint-gcs-01-5d8e27'
FORBIDDEN_FRAGMENTS = (SENTINEL, 'upload_id', 'googleapis.com', 'Bearer')

WORK_ID = '11111111-2222-3333-4444-555555555555'
PUBLISHER_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
PROJECT = 'synthetic-project'
BUCKET = 'synthetic-bucket'
COLLECTION = 'SYNTHCOLL'
BUCKET_VARIABLE = 'google_play_bucket'
COLLECTION_VARIABLE = 'google_play_coll_aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee'
ENVIRONMENT = {BUCKET_VARIABLE: BUCKET, COLLECTION_VARIABLE: COLLECTION}
# Test-only: stops the SDK fetching bucket metadata on a background thread.
# The SDK reads it on every call, so it must stay set for the whole upload.
NO_BUCKET_METADATA = {'DISABLE_GCS_PYTHON_CLIENT_OTEL_BUCKET_METADATA': 'true'}
# Would point the SDK somewhere other than its default endpoint
ENDPOINT_OVERRIDES = ('STORAGE_EMULATOR_HOST', 'API_ENDPOINT_OVERRIDE')
TODAY = date(2026, 1, 2)

PDF_BYTES = b'%PDF-1.4 synthetic pdf bytes'
EPUB_BYTES = b'PK synthetic epub bytes'
ONIX_BYTES = b'<ONIXMessage>synthetic onix bytes</ONIXMessage>'
# The SDK sends payloads of up to 8 MiB as one multipart request and anything
# larger through a resumable upload session
LARGEST_MULTIPART_PDF = bytes(range(256)) * (8388608 // 256)
SMALLEST_RESUMABLE_PDF = LARGEST_MULTIPART_PDF + b'\x00'

PDF_KEY = 'ebooks/SYNTHCOLL/9781234567890.pdf'
# Existing behaviour: when both formats exist, the EPUB is named with the PDF ISBN
EPUB_KEY_WITH_PDF = 'ebooks/SYNTHCOLL/9781234567890.epub'
EPUB_KEY_ONLY = 'ebooks/SYNTHCOLL/9781234567897.epub'
ONIX_KEY = 'onix/SYNTHCOLL-full/SyntheticTestPress_9781234567890_20260102.xml'
ONIX_KEY_EPUB_ONLY = (
    'onix/SYNTHCOLL-full/SyntheticTestPress_9781234567897_20260102.xml')
CONTENT_TYPE = 'application/octet-stream'

CONTENT_FAILURE = 'Google Play content upload failed'
ONIX_FAILURE = 'Google Play ONIX metadata upload failed'
SUCCESS = 'Successfully uploaded to Thoth Google Cloud bucket for Google Play crawl'
# Format used by disseminator.py; ERROR lines are copied into job summaries
LOG_FORMAT = '%(levelname)s:%(asctime)s: %(message)s'

UPLOAD_PATH = '/upload/storage/v1/b/synthetic-bucket/o'
MULTIPART = [('uploadType', 'multipart')]
RESUMABLE = [('uploadType', 'resumable')]
SESSION = [('uploadType', 'resumable'), ('upload_id', SENTINEL)]
SESSION_URL = 'https://storage.googleapis.com{}?uploadType=resumable&upload_id={}'.format(
    UPLOAD_PATH, SENTINEL)
PDF_OBJECT_PATH = '/storage/v1/b/synthetic-bucket/o/' + quote(PDF_KEY, safe='')
# Captured before any test replaces storage.Client
REAL_CLIENT = storage.Client


def crc32c_base64(data):
    """CRC32c as Cloud Storage represents it: base64 of the big-endian value."""
    return base64.b64encode(google_crc32c.value(data).to_bytes(4, 'big')).decode('ascii')


def make_uploader(formats=('PDF', 'EPUB'), pdf_bytes=PDF_BYTES):
    uploader = GooglePlayUploader.__new__(GooglePlayUploader)
    uploader.work_id = WORK_ID
    uploader.export_url = 'https://export.example.invalid'
    uploader.version = 'test'
    publications = []
    if 'PDF' in formats:
        publications.append({'publicationType': 'PDF', 'isbn': '978-1-234-56789-0'})
    if 'EPUB' in formats:
        publications.append({'publicationType': 'EPUB', 'isbn': '978-1-234-56789-7'})
    uploader.metadata = {'data': {'work': {
        'workId': WORK_ID,
        'publications': publications,
        'imprint': {'publisher': {
            'publisherId': PUBLISHER_ID,
            'publisherName': 'Synthetic Test Press',
        }},
    }}}
    content = {
        'PDF': Publication('PDF', 'pdf-id', pdf_bytes, '.pdf'),
        'EPUB': Publication('EPUB', 'epub-id', EPUB_BYTES, '.epub'),
    }

    def publication_details(publication_type):
        if publication_type in formats:
            return content[publication_type]
        raise DisseminationError(
            'No {} publication found for Work'.format(publication_type))

    uploader.get_publication_details = MagicMock(side_effect=publication_details)
    uploader.get_formatted_metadata = MagicMock(return_value=ONIX_BYTES)
    return uploader


class NetworkAccess(BaseException):
    """Not an Exception, so the uploader's handlers cannot turn it into a failed upload."""


class UnexpectedRequest(BaseException):
    """Raised by the fake transport for a request no test expected."""


class OfflineTestCase(unittest.TestCase):
    """Blocks sockets and credential discovery for every test."""

    def setUp(self):
        self.blocked = []

        def blocker(name):
            def block(*args, **kwargs):
                self.blocked.append(name)
                raise NetworkAccess(name)
            return block

        for target, name in ((socket.socket, 'connect'), (socket.socket, 'connect_ex'),
                             (socket, 'create_connection'), (socket, 'getaddrinfo'),
                             (google.auth, 'default')):
            patcher = patch.object(target, name, blocker(name))
            patcher.start()
            self.addCleanup(patcher.stop)

    def capture(self, uploader, environment, client_factory, missing=()):
        """Run the upload; return its SystemExit (or None), log records and output."""
        stdout, stderr = io.StringIO(), io.StringIO()
        outcome = None
        with patch.dict(os.environ, environment), \
                patch.object(googleplayuploader.storage, 'Client',
                             side_effect=client_factory) as client_class, \
                patch.object(googleplayuploader, 'date') as mock_date, \
                redirect_stdout(stdout), redirect_stderr(stderr), \
                self.assertLogs(level='DEBUG') as captured:
            for name in missing:
                os.environ.pop(name, None)
            mock_date.today.return_value = TODAY
            try:
                uploader.upload_to_platform()
            except SystemExit as exit_error:
                # Any other exception escapes and fails the test
                outcome = exit_error
        self.assertEqual(self.blocked, [])
        return SimpleNamespace(
            outcome=outcome, records=captured.records, stdout=stdout.getvalue(),
            stderr=stderr.getvalue(), client_class=client_class)

    def assertSucceeded(self, result):
        self.assertIsNone(result.outcome)
        self.assertEqual(
            [(record.levelno, record.getMessage()) for record in result.records],
            [(logging.INFO, SUCCESS)])
        self.assertEqual(result.stdout, '')
        self.assertEqual(result.stderr, '')

    def assertSafeFailure(self, result, expected_message, error_class):
        """Exit 1 with only the fixed #117 message, raised from the expected error."""
        self.assertIsInstance(result.outcome, SystemExit)
        self.assertEqual(result.outcome.code, 1)
        self.assertIs(type(result.outcome.__context__), error_class)
        self.assertEqual(
            [(record.levelno, record.getMessage()) for record in result.records],
            [(logging.ERROR, expected_message)])
        record = result.records[0]
        self.assertEqual(record.args, ())
        self.assertIsNone(record.exc_info)
        rendered = logging.Formatter(LOG_FORMAT).format(record)
        for text in (rendered, repr(vars(record)), result.stdout, result.stderr):
            for fragment in FORBIDDEN_FRAGMENTS:
                self.assertNotIn(fragment, text)
        self.assertEqual(result.stdout, '')
        self.assertEqual(result.stderr, '')


# Level 1: the real uploader with a mocked storage client


def sentinel_response(status_code):
    response = requests.Response()
    response.status_code = status_code
    response.url = SESSION_URL
    response.headers['x-goog-synthetic'] = SENTINEL
    response._content = json.dumps({'error': {'message': SENTINEL}}).encode('utf-8')
    response.request = requests.Request('PUT', SESSION_URL).prepare()
    return response


def sdk_failures():
    """Errors the 3.x SDK raises from upload_from_string, carrying provider details."""
    message = 'PUT {}: {}'.format(SESSION_URL, SENTINEL)
    return [
        (api_exceptions.Forbidden,
         lambda: api_exceptions.Forbidden(message, response=sentinel_response(403))),
        (api_exceptions.BadRequest,
         lambda: api_exceptions.from_http_status(
             400, message, response=sentinel_response(400))),
        (api_exceptions.ServiceUnavailable,
         lambda: api_exceptions.from_http_status(
             503, message, response=sentinel_response(503))),
        # A completed upload whose response carries no checksum
        (api_exceptions.GoogleAPICallError,
         lambda: api_exceptions.from_http_status(
             200, message, response=sentinel_response(200))),
        (storage_exceptions.DataCorruption,
         lambda: storage_exceptions.DataCorruption(
             sentinel_response(200),
             'Checksum mismatch while uploading. Got {}'.format(SENTINEL))),
        (storage_exceptions.InvalidResponse,
         lambda: storage_exceptions.InvalidResponse(
             sentinel_response(503), 'Request failed with status code', 503,
             SESSION_URL)),
        (requests.exceptions.ConnectionError,
         lambda: requests.exceptions.ConnectionError(
             'Connection aborted {}'.format(SESSION_URL),
             request=requests.Request('PUT', SESSION_URL).prepare())),
    ]


class MockedClientTest(OfflineTestCase):

    def run_uploader(self, uploader, failures=None, missing=()):
        failures = failures or {}
        blobs = {}
        attempted = []

        def make_blob(name):
            blob = MagicMock(name='blob {}'.format(name))

            def upload_from_string(**kwargs):
                attempted.append(name)
                if name in failures:
                    raise failures[name]

            blob.upload_from_string.side_effect = upload_from_string
            blobs[name] = blob
            return blob

        bucket = MagicMock(name='bucket')
        bucket.blob.side_effect = make_blob
        client = MagicMock(name='client')
        client.bucket.return_value = bucket
        result = self.capture(
            uploader, ENVIRONMENT, lambda *args, **kwargs: client, missing)
        result.client, result.bucket = client, bucket
        result.blobs, result.attempted = blobs, attempted
        return result

    def assertUploads(self, result, expected):
        """expected: [(object key, payload)] in upload order."""
        # Unchanged: default client and credentials, then the configured bucket
        result.client_class.assert_called_once_with()
        self.assertEqual(result.client.mock_calls, [call.bucket(BUCKET)])
        keys = [key for key, _ in expected]
        self.assertEqual(result.bucket.mock_calls, [call.blob(key) for key in keys])
        self.assertEqual(result.attempted, keys)
        for key, payload in expected:
            blob = result.blobs[key]
            self.assertEqual(blob.mock_calls, [call.upload_from_string(
                data=payload, content_type=CONTENT_TYPE, checksum='crc32c', retry=None)])
            self.assertIs(blob.upload_from_string.call_args.kwargs['data'], payload)

    def test_pdf_epub_and_onix_upload_with_crc32c_and_no_retry(self):
        result = self.run_uploader(make_uploader())
        self.assertSucceeded(result)
        self.assertUploads(result, [
            (PDF_KEY, PDF_BYTES),
            (EPUB_KEY_WITH_PDF, EPUB_BYTES),
            (ONIX_KEY, ONIX_BYTES),
        ])

    def test_pdf_only_upload_with_crc32c_and_no_retry(self):
        result = self.run_uploader(make_uploader(formats=('PDF',)))
        self.assertSucceeded(result)
        self.assertUploads(result, [(PDF_KEY, PDF_BYTES), (ONIX_KEY, ONIX_BYTES)])

    def test_epub_only_upload_with_crc32c_and_no_retry(self):
        result = self.run_uploader(make_uploader(formats=('EPUB',)))
        self.assertSucceeded(result)
        self.assertUploads(
            result, [(EPUB_KEY_ONLY, EPUB_BYTES), (ONIX_KEY_EPUB_ONLY, ONIX_BYTES)])

    def test_neither_format_fails_without_uploading(self):
        result = self.run_uploader(make_uploader(formats=()))
        self.assertEqual(result.outcome.code, 1)
        self.assertEqual(
            [(record.levelno, record.getMessage()) for record in result.records], [
                (logging.ERROR,
                 'Errors occurred with both PDF and EPUB files for Google Play upload. '
                 'Fix either one or both of the errors below:'),
                (logging.ERROR,
                 'No PDF publication found for Work\n'
                 'Make sure the PDF Publication in Thoth has a valid ISBN.'),
                (logging.ERROR, 'No EPUB publication found for Work'),
            ])
        self.assertUploads(result, [])

    def test_missing_bucket_or_collection_fails_before_creating_a_client(self):
        for variable in (BUCKET_VARIABLE, COLLECTION_VARIABLE):
            with self.subTest(missing=variable):
                result = self.run_uploader(make_uploader(), missing=(variable,))
                self.assertEqual(result.outcome.code, 1)
                self.assertEqual(
                    [(record.levelno, record.getMessage()) for record in result.records],
                    [(logging.ERROR,
                      'Error uploading to Google Play: missing value for {}'.format(
                          variable))])
                result.client_class.assert_not_called()
                self.assertEqual(result.attempted, [])

    def test_upload_failure_stops_at_failed_object_and_logs_only_fixed_message(self):
        positions = (
            (PDF_KEY, CONTENT_FAILURE, [(PDF_KEY, PDF_BYTES)]),
            (EPUB_KEY_WITH_PDF, CONTENT_FAILURE,
             [(PDF_KEY, PDF_BYTES), (EPUB_KEY_WITH_PDF, EPUB_BYTES)]),
            (ONIX_KEY, ONIX_FAILURE, [
                (PDF_KEY, PDF_BYTES),
                (EPUB_KEY_WITH_PDF, EPUB_BYTES),
                (ONIX_KEY, ONIX_BYTES),
            ]),
        )
        for failed_key, message, uploaded in positions:
            for error_class, make_error in sdk_failures():
                with self.subTest(failed=failed_key, error=error_class.__name__):
                    raised = make_error()
                    result = self.run_uploader(make_uploader(), {failed_key: raised})
                    self.assertSafeFailure(result, message, error_class)
                    self.assertIs(result.outcome.__context__, raised)
                    self.assertUploads(result, uploaded)


# Level 2: the real uploader and the real SDK over a fake HTTP transport


def http_response(sent, status, body=b'', headers=None):
    response = requests.Response()
    response.status_code = status
    response.headers.update(headers or {})
    response._content = body if isinstance(body, bytes) else json.dumps(body).encode('utf-8')
    response.encoding = 'utf-8'
    response.url = sent.url
    # The SDK builds its error messages from the originating request
    response.request = requests.Request(sent.method, sent.url).prepare()
    return response


def parse_multipart(sent):
    """Split the SDK's multipart/related body into (metadata, part type, media)."""
    boundary = sent.headers['content-type'].split(b'boundary="', 1)[1][:-1]
    separator = b'--' + boundary
    head = separator + b'\r\ncontent-type: application/json; charset=UTF-8\r\n\r\n'
    tail = b'\r\n' + separator + b'--'
    if not (sent.body.startswith(head) and sent.body.endswith(tail)):
        raise AssertionError('unexpected multipart framing')
    metadata_end = sent.body.index(b'\r\n' + separator + b'\r\n')
    part_start = metadata_end + len(separator) + 4
    part_headers_end = sent.body.index(b'\r\n\r\n', part_start)
    part_type = sent.body[part_start:part_headers_end]
    media = sent.body[part_headers_end + 4:-len(tail)]
    return json.loads(sent.body[len(head):metadata_end]), part_type, media


def in_transit(media, corrupt):
    """The bytes the server receives; corruption flips one bit of the first byte."""
    return bytes([media[0] ^ 1]) + media[1:] if corrupt else media


def stored(name, media, crc32c=True):
    resource = {'kind': 'storage#object', 'bucket': BUCKET, 'name': name,
                'generation': '1767312000000001', 'size': str(len(media)),
                'contentType': CONTENT_TYPE}
    if crc32c:
        resource['crc32c'] = crc32c_base64(media)
    return resource


def failure_body(status):
    return {'error': {'code': status, 'message': 'synthetic {}'.format(SENTINEL)}}


def multipart_stored(corrupt=False):
    """Store a multipart upload, rejecting a declared CRC32c that does not match."""
    def respond(sent):
        metadata, _, media = parse_multipart(sent)
        media = in_transit(media, corrupt)
        if metadata.get('crc32c', crc32c_base64(media)) != crc32c_base64(media):
            return http_response(sent, 400, failure_body(400))
        return http_response(sent, 200, stored(metadata['name'], media))
    return respond


def session_started(sent):
    return http_response(sent, 200, headers={'location': SESSION_URL})


def chunk_stored(name, corrupt=False, enforce=True, crc32c=True):
    """Store the final chunk of a resumable upload, checking x-goog-hash if enforce."""
    def respond(sent):
        media = in_transit(sent.body, corrupt)
        declared = sent.headers.get('x-goog-hash', 'crc32c=' + crc32c_base64(media))
        if enforce and declared != 'crc32c=' + crc32c_base64(media):
            return http_response(sent, 400, failure_body(400))
        return http_response(sent, 200, stored(name, media, crc32c=crc32c))
    return respond


def failed(status):
    def respond(sent):
        return http_response(
            sent, status, failure_body(status), {'x-goog-synthetic': SENTINEL})
    return respond


def connection_dropped(sent):
    raise requests.exceptions.ConnectionError(
        'Connection aborted {} {}'.format(sent.url, SENTINEL))


def deleted(sent):
    return http_response(sent, 204)


class FakeTransport:
    """Stands in for the client's authorized HTTP session.

    Records every request and answers each with the next scripted responder.
    It never opens a connection.
    """

    # As for an AuthorizedSession without a client certificate
    is_mtls = False

    def __init__(self, responders):
        self.sent = []
        self.responders = list(responders)

    def request(self, method, url, data=None, headers=None, timeout=None):
        parts = urlsplit(url)
        sent = SimpleNamespace(
            method=method, url=url, path=parts.path, query=parse_qsl(parts.query),
            headers={name.lower(): value for name, value in (headers or {}).items()},
            body=data, timeout=timeout)
        self.sent.append(sent)
        if not self.responders:
            raise UnexpectedRequest('{} {}'.format(method, parts.path))
        return self.responders.pop(0)(sent)


class RealSdkTest(OfflineTestCase):

    def run_uploader(self, uploader, *responders):
        transport = FakeTransport(responders)
        constructed = []

        def make_client(*args, **kwargs):
            # The uploader builds its client exactly as before; only the
            # credentials and the transport behind the real client are fake
            constructed.append((args, kwargs))
            return REAL_CLIENT(
                project=PROJECT, credentials=AnonymousCredentials(), _http=transport)

        with patch.object(BucketMetadataCache, 'get_or_queue_fetch',
                          return_value=None) as metadata_fetch:
            result = self.capture(uploader, {**ENVIRONMENT, **NO_BUCKET_METADATA},
                                  make_client, ENDPOINT_OVERRIDES)
        self.assertEqual(constructed, [((), {})])
        # Tripwire: the SDK never queued its background bucket-metadata GET
        metadata_fetch.assert_not_called()
        self.assertEqual(transport.responders, [], 'scripted responses left unused')
        result.sent = transport.sent
        return result

    def assertRequests(self, result, expected):
        """expected: [(method, path, query)] in order. No retries, no preconditions."""
        self.assertEqual(
            [(sent.method, sent.path, sent.query) for sent in result.sent], expected)
        for sent in result.sent:
            self.assertFalse([key for key, _ in sent.query if key.lower().startswith('if')])
            # SDK default per-request timeout, unchanged
            self.assertEqual(sent.timeout, 60)

    def assertMultipart(self, sent, key, payload):
        metadata, part_type, media = parse_multipart(sent)
        self.assertEqual(metadata, {'name': key, 'crc32c': crc32c_base64(payload)})
        self.assertEqual(part_type, b'content-type: ' + CONTENT_TYPE.encode('ascii'))
        self.assertEqual(media, payload)
        self.assertNotIn('x-goog-hash', sent.headers)

    def assertResumable(self, initiate, chunk, key, payload):
        self.assertEqual(json.loads(initiate.body), {'name': key})
        self.assertEqual(initiate.headers['x-upload-content-type'], CONTENT_TYPE)
        self.assertEqual(initiate.headers['x-upload-content-length'], str(len(payload)))
        self.assertNotIn('x-goog-hash', initiate.headers)
        self.assertEqual(chunk.url, SESSION_URL)
        self.assertEqual(chunk.headers['content-type'], CONTENT_TYPE)
        self.assertEqual(chunk.headers['content-range'],
                         'bytes 0-{}/{}'.format(len(payload) - 1, len(payload)))
        self.assertEqual(chunk.headers['x-goog-hash'], 'crc32c=' + crc32c_base64(payload))
        self.assertTrue(chunk.body == payload, 'resumable chunk differs from payload')

    def test_checksum_helper_matches_the_crc32c_check_value(self):
        # Standard CRC-32C check value for b'123456789' is 0xE3069283
        self.assertEqual(crc32c_base64(b'123456789'),
                         base64.b64encode(bytes.fromhex('e3069283')).decode('ascii'))

    def test_small_objects_send_crc32c_in_one_multipart_request_each(self):
        result = self.run_uploader(
            make_uploader(), multipart_stored(), multipart_stored(), multipart_stored())
        self.assertSucceeded(result)
        self.assertRequests(result, [('POST', UPLOAD_PATH, MULTIPART)] * 3)
        self.assertMultipart(result.sent[0], PDF_KEY, PDF_BYTES)
        self.assertMultipart(result.sent[1], EPUB_KEY_WITH_PDF, EPUB_BYTES)
        self.assertMultipart(result.sent[2], ONIX_KEY, ONIX_BYTES)

    def test_crc32c_is_sent_even_where_the_sdk_default_would_use_md5(self):
        # Without the C extension the SDK's default "auto" checksum is MD5
        with patch.object(google_crc32c, 'implementation', 'python'):
            result = self.run_uploader(
                make_uploader(formats=('PDF',)), multipart_stored(), multipart_stored())
        self.assertSucceeded(result)
        self.assertRequests(result, [('POST', UPLOAD_PATH, MULTIPART)] * 2)
        self.assertMultipart(result.sent[0], PDF_KEY, PDF_BYTES)
        self.assertMultipart(result.sent[1], ONIX_KEY, ONIX_BYTES)

    def test_exactly_8_mib_is_sent_as_one_multipart_request(self):
        self.assertEqual(len(LARGEST_MULTIPART_PDF), 8388608)
        result = self.run_uploader(
            make_uploader(formats=('PDF',), pdf_bytes=LARGEST_MULTIPART_PDF),
            multipart_stored(), multipart_stored())
        self.assertSucceeded(result)
        self.assertRequests(result, [('POST', UPLOAD_PATH, MULTIPART)] * 2)
        self.assertMultipart(result.sent[0], PDF_KEY, LARGEST_MULTIPART_PDF)
        self.assertMultipart(result.sent[1], ONIX_KEY, ONIX_BYTES)

    def test_8_mib_plus_one_byte_uses_resumable_upload_with_final_crc32c(self):
        self.assertEqual(len(SMALLEST_RESUMABLE_PDF), 8388609)
        result = self.run_uploader(
            make_uploader(formats=('PDF',), pdf_bytes=SMALLEST_RESUMABLE_PDF),
            session_started, chunk_stored(PDF_KEY), multipart_stored())
        self.assertSucceeded(result)
        # Exactly one initiate POST and one chunk PUT
        self.assertRequests(result, [
            ('POST', UPLOAD_PATH, RESUMABLE),
            ('PUT', UPLOAD_PATH, SESSION),
            ('POST', UPLOAD_PATH, MULTIPART),
        ])
        self.assertResumable(result.sent[0], result.sent[1], PDF_KEY, SMALLEST_RESUMABLE_PDF)
        self.assertMultipart(result.sent[2], ONIX_KEY, ONIX_BYTES)

    def test_server_rejected_multipart_checksum_fails_without_retry(self):
        result = self.run_uploader(make_uploader(), multipart_stored(corrupt=True))
        self.assertSafeFailure(result, CONTENT_FAILURE, api_exceptions.BadRequest)
        self.assertRequests(result, [('POST', UPLOAD_PATH, MULTIPART)])
        self.assertMultipart(result.sent[0], PDF_KEY, PDF_BYTES)

    def test_unavailable_server_on_later_content_fails_without_retry(self):
        result = self.run_uploader(make_uploader(), multipart_stored(), failed(503))
        self.assertSafeFailure(result, CONTENT_FAILURE, api_exceptions.ServiceUnavailable)
        # One POST for the EPUB, which the SDK default would have retried
        self.assertRequests(result, [('POST', UPLOAD_PATH, MULTIPART)] * 2)
        self.assertMultipart(result.sent[0], PDF_KEY, PDF_BYTES)
        self.assertMultipart(result.sent[1], EPUB_KEY_WITH_PDF, EPUB_BYTES)

    def test_dropped_connection_on_onix_after_content_fails_without_retry(self):
        result = self.run_uploader(
            make_uploader(), multipart_stored(), multipart_stored(), connection_dropped)
        self.assertSafeFailure(result, ONIX_FAILURE, requests.exceptions.ConnectionError)
        # Not atomic: both content objects were stored before the ONIX failure
        self.assertRequests(result, [('POST', UPLOAD_PATH, MULTIPART)] * 3)
        self.assertMultipart(result.sent[0], PDF_KEY, PDF_BYTES)
        self.assertMultipart(result.sent[1], EPUB_KEY_WITH_PDF, EPUB_BYTES)
        self.assertMultipart(result.sent[2], ONIX_KEY, ONIX_BYTES)

    def test_server_rejected_resumable_checksum_fails_without_retry_or_delete(self):
        result = self.run_uploader(
            make_uploader(formats=('PDF',), pdf_bytes=SMALLEST_RESUMABLE_PDF),
            session_started, chunk_stored(PDF_KEY, corrupt=True))
        self.assertSafeFailure(result, CONTENT_FAILURE, api_exceptions.BadRequest)
        self.assertRequests(result, [
            ('POST', UPLOAD_PATH, RESUMABLE),
            ('PUT', UPLOAD_PATH, SESSION),
        ])
        self.assertResumable(result.sent[0], result.sent[1], PDF_KEY, SMALLEST_RESUMABLE_PDF)

    def test_unavailable_server_on_resumable_chunk_fails_without_retry(self):
        result = self.run_uploader(
            make_uploader(formats=('PDF',), pdf_bytes=SMALLEST_RESUMABLE_PDF),
            session_started, failed(503))
        self.assertSafeFailure(result, CONTENT_FAILURE, api_exceptions.ServiceUnavailable)
        self.assertRequests(result, [
            ('POST', UPLOAD_PATH, RESUMABLE),
            ('PUT', UPLOAD_PATH, SESSION),
        ])
        self.assertResumable(result.sent[0], result.sent[1], PDF_KEY, SMALLEST_RESUMABLE_PDF)

    def test_mismatched_returned_checksum_deletes_object_and_fails(self):
        # The server stored corrupted bytes without enforcing x-goog-hash, so
        # its returned CRC32c differs and the SDK deletes the object it named
        result = self.run_uploader(
            make_uploader(formats=('PDF',), pdf_bytes=SMALLEST_RESUMABLE_PDF),
            session_started, chunk_stored(PDF_KEY, corrupt=True, enforce=False), deleted)
        self.assertSafeFailure(result, CONTENT_FAILURE, storage_exceptions.DataCorruption)
        # Exactly one cleanup DELETE, with no generation or precondition
        self.assertRequests(result, [
            ('POST', UPLOAD_PATH, RESUMABLE),
            ('PUT', UPLOAD_PATH, SESSION),
            ('DELETE', PDF_OBJECT_PATH, [('prettyPrint', 'false')]),
        ])
        self.assertResumable(result.sent[0], result.sent[1], PDF_KEY, SMALLEST_RESUMABLE_PDF)

    def test_failed_cleanup_delete_fails_closed(self):
        result = self.run_uploader(
            make_uploader(formats=('PDF',), pdf_bytes=SMALLEST_RESUMABLE_PDF),
            session_started, chunk_stored(PDF_KEY, corrupt=True, enforce=False),
            failed(403))
        # The cleanup error replaces the checksum error the SDK was raising
        self.assertSafeFailure(result, CONTENT_FAILURE, api_exceptions.Forbidden)
        self.assertIs(type(result.outcome.__context__.__context__),
                      storage_exceptions.DataCorruption)
        # 403 is not retryable, so exactly one DELETE
        self.assertRequests(result, [
            ('POST', UPLOAD_PATH, RESUMABLE),
            ('PUT', UPLOAD_PATH, SESSION),
            ('DELETE', PDF_OBJECT_PATH, [('prettyPrint', 'false')]),
        ])
        self.assertResumable(result.sent[0], result.sent[1], PDF_KEY, SMALLEST_RESUMABLE_PDF)

    def test_missing_returned_checksum_fails_without_delete(self):
        # The object may already be committed, but its checksum cannot be confirmed
        result = self.run_uploader(
            make_uploader(formats=('PDF',), pdf_bytes=SMALLEST_RESUMABLE_PDF),
            session_started, chunk_stored(PDF_KEY, crc32c=False))
        self.assertSafeFailure(result, CONTENT_FAILURE, api_exceptions.GoogleAPICallError)
        self.assertEqual(result.outcome.__context__.code, 200)
        self.assertRequests(result, [
            ('POST', UPLOAD_PATH, RESUMABLE),
            ('PUT', UPLOAD_PATH, SESSION),
        ])
        self.assertResumable(result.sent[0], result.sent[1], PDF_KEY, SMALLEST_RESUMABLE_PDF)


if __name__ == '__main__':
    unittest.main()
