"""
Offline tests for SEC-DIS-GP-01: a failed Google Play upload must log only a
fixed message, never content from the provider exception, because dissemination
logs and job summaries are public.
"""

import io
import logging
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from http import HTTPStatus
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import requests
import urllib3
from google.api_core import exceptions as api_exceptions
from google.auth import exceptions as auth_exceptions
from google.cloud import storage
from google.resumable_media import DataCorruption, InvalidResponse

import googleplayuploader
from errors import DisseminationError
from googleplayuploader import GooglePlayUploader
from uploader import Publication


# Synthetic, non-secret values only. Every exception-controlled value below
# contains SENTINEL, so its absence from all output proves nothing leaked.
SENTINEL = 'SENTINEL-sec-dis-gp-01-7f3a9c'
SENSITIVE_HOST = 'storage.example.invalid'
SENSITIVE_PATH = (
    '/upload/storage/v1/b/synthetic-bucket/o?uploadType=resumable'
    '&upload_id={}'.format(SENTINEL))
SENSITIVE_URL = 'https://{}{}'.format(SENSITIVE_HOST, SENSITIVE_PATH)
SENSITIVE_HEADER = 'Bearer {}'.format(SENTINEL)
SENSITIVE_BODY = '{{"error": {{"message": "denied {}"}}}}'.format(SENTINEL)
FORBIDDEN_FRAGMENTS = (SENTINEL, SENSITIVE_HOST, 'upload_id', 'Bearer')

WORK_ID = '11111111-2222-3333-4444-555555555555'
PUBLISHER_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
BUCKET = 'synthetic-bucket'
COLLECTION = 'SYNTHCOLL'
ENVIRONMENT = {
    'google_play_bucket': BUCKET,
    'google_play_coll_aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee': COLLECTION,
}
TODAY = date(2026, 1, 2)

PDF_BYTES = b'%PDF-1.4 synthetic pdf bytes'
EPUB_BYTES = b'PK synthetic epub bytes'
ONIX_BYTES = b'<ONIXMessage>synthetic onix bytes</ONIXMessage>'

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


def sensitive_request():
    return requests.Request(
        'PUT', SENSITIVE_URL,
        headers={'Authorization': SENSITIVE_HEADER}).prepare()


def sensitive_response(status_code):
    response = requests.Response()
    response.status_code = status_code
    response.url = SENSITIVE_URL
    response.headers['x-goog-synthetic'] = SENTINEL
    response._content = SENSITIVE_BODY.encode('utf-8')
    response.request = sensitive_request()
    return response


def provider_message(method='PUT'):
    # Shape produced by google-cloud-storage when re-raising HTTP failures
    return '{} {}: {}: synthetic'.format(method, SENSITIVE_URL, SENSITIVE_BODY)


class HostileError(Exception):
    """Fails loudly if the handler converts it or reads any attribute."""

    def __str__(self):
        raise AssertionError('__str__ called: {}'.format(SENTINEL))

    def __repr__(self):
        raise AssertionError('__repr__ called: {}'.format(SENTINEL))

    def __format__(self, format_spec):
        raise AssertionError('__format__ called: {}'.format(SENTINEL))

    @property
    def code(self):
        raise AssertionError('code read: {}'.format(SENTINEL))

    def __getattr__(self, name):
        # Dunder lookups stay normal so Python's own traceback machinery works
        if name.startswith('__'):
            raise AttributeError(name)
        raise AssertionError('{} read: {}'.format(name, SENTINEL))


class SentinelInt(int):
    def __str__(self):
        return SENTINEL

    __repr__ = __str__

    def __format__(self, format_spec):
        return SENTINEL


class CodedError(Exception):
    def __init__(self, message, code):
        super().__init__(message)
        self.code = code


def chained_error():
    error = Exception('outer {}'.format(SENSITIVE_URL))
    error.__cause__ = api_exceptions.from_http_status(
        403, provider_message(), response=sensitive_response(403))
    error.__context__ = requests.exceptions.ConnectionError(
        'inner {}'.format(SENSITIVE_PATH), request=sensitive_request())
    # Set directly: BaseException.add_note() only exists from Python 3.11
    error.__notes__ = ['note {}'.format(SENTINEL)]
    return error


def failure_cases():
    """Representative exceptions from the pinned dependency stack plus hostile ones."""
    cases = [
        ('api_core BadRequest from from_http_status', lambda: api_exceptions.from_http_status(
            400, provider_message(), response=sensitive_response(400),
            errors=[{'message': SENTINEL}])),
        ('api_core Forbidden', lambda: api_exceptions.Forbidden(
            provider_message('POST'), response=sensitive_response(403))),
        ('api_core ServiceUnavailable', lambda: api_exceptions.ServiceUnavailable(
            provider_message(), response=sensitive_response(503))),
        ('api_core RetryError', lambda: api_exceptions.RetryError(
            'Deadline exceeded {}'.format(SENSITIVE_URL),
            api_exceptions.ServiceUnavailable(SENSITIVE_BODY))),
        ('requests ConnectionError from urllib3 MaxRetryError',
         lambda: requests.exceptions.ConnectionError(
             urllib3.exceptions.MaxRetryError(
                 None, SENSITIVE_PATH,
                 reason=urllib3.exceptions.ProtocolError(
                     'Connection aborted {}'.format(SENTINEL))),
             request=sensitive_request())),
        ('requests ReadTimeout', lambda: requests.exceptions.ReadTimeout(
            'Read timed out {}'.format(SENSITIVE_URL), request=sensitive_request())),
        ('google.auth RefreshError', lambda: auth_exceptions.RefreshError(
            'Error code invalid_grant: {}'.format(SENTINEL), SENSITIVE_BODY)),
        ('google.resumable_media InvalidResponse', lambda: InvalidResponse(
            sensitive_response(503), 'Request failed with status code', 503,
            SENSITIVE_URL)),
        ('google.resumable_media DataCorruption', lambda: DataCorruption(
            sensitive_response(200), 'Checksum mismatch {}'.format(SENTINEL))),
        ('chained exception with cause, context and notes', chained_error),
        ('hostile __str__, __repr__, __format__, code and attributes',
         lambda: HostileError(SENSITIVE_URL)),
    ]
    hostile_codes = [
        ('HTTPStatus', HTTPStatus.BAD_REQUEST),
        ('plain int', 418),
        ('bool', True),
        ('negative int', -1),
        ('out-of-range int', 600),
        ('numeric string', '400'),
        ('int subclass rendering the sentinel', SentinelInt(400)),
        ('sentinel string', SENTINEL),
    ]
    for label, code in hostile_codes:
        cases.append((
            'code attribute: {}'.format(label),
            lambda code=code: CodedError(provider_message(), code)))
    return cases


class GooglePlayUploadLoggingTest(unittest.TestCase):

    def _uploader(self, formats=('PDF', 'EPUB')):
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
            'PDF': Publication('PDF', 'pdf-id', PDF_BYTES, '.pdf'),
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

    def _run(self, uploader, failures=None):
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
        stdout, stderr = io.StringIO(), io.StringIO()
        outcome = None
        network_guard = AssertionError('network access attempted')
        with patch.dict(os.environ, ENVIRONMENT), \
                patch.object(googleplayuploader.storage, 'Client',
                             return_value=client) as client_class, \
                patch.object(googleplayuploader, 'date') as mock_date, \
                patch('requests.get', side_effect=network_guard) as get, \
                patch('requests.head', side_effect=network_guard) as head, \
                redirect_stdout(stdout), redirect_stderr(stderr), \
                self.assertLogs(level='DEBUG') as captured:
            mock_date.today.return_value = TODAY
            try:
                uploader.upload_to_platform()
            except SystemExit as exit_error:
                # Any other exception escapes and fails the test
                outcome = exit_error

        get.assert_not_called()
        head.assert_not_called()
        client_class.assert_called_once_with()
        self.assertEqual(client.mock_calls, [call.bucket(BUCKET)])
        return SimpleNamespace(
            outcome=outcome, records=captured.records, stdout=stdout.getvalue(),
            stderr=stderr.getvalue(), bucket=bucket, blobs=blobs, attempted=attempted)

    def assertUploads(self, result, expected):
        """expected: [(object key, payload)] in upload order."""
        keys = [key for key, _ in expected]
        self.assertEqual(result.bucket.mock_calls, [call.blob(key) for key in keys])
        self.assertEqual(result.attempted, keys)
        for key, payload in expected:
            blob = result.blobs[key]
            # Only upload_from_string, with exactly the existing arguments
            self.assertEqual(
                blob.mock_calls,
                [call.upload_from_string(data=payload, content_type=CONTENT_TYPE)])
            self.assertIs(blob.upload_from_string.call_args.kwargs['data'], payload)

    def assertSafeFailure(self, result, expected_message, raised):
        self.assertIsInstance(result.outcome, SystemExit)
        self.assertEqual(result.outcome.code, 1)
        # The exit came from the handler that caught the injected exception
        self.assertIs(result.outcome.__context__, raised)
        self.assertEqual(
            [(record.levelno, record.getMessage()) for record in result.records],
            [(logging.ERROR, expected_message)])
        record = result.records[0]
        self.assertEqual(record.msg, expected_message)
        self.assertEqual(record.args, ())
        self.assertIsNone(record.exc_info)
        self.assertIsNone(record.exc_text)
        self.assertIsNone(record.stack_info)
        rendered = logging.Formatter(LOG_FORMAT).format(record)
        self.assertTrue(rendered.startswith('ERROR:'))
        self.assertTrue(rendered.endswith(': ' + expected_message))
        for text in (rendered, repr(vars(record)), result.stdout, result.stderr):
            for fragment in FORBIDDEN_FRAGMENTS:
                self.assertNotIn(fragment, text)
        self.assertEqual(result.stdout, '')
        self.assertEqual(result.stderr, '')

    def test_uses_real_google_cloud_storage_module(self):
        self.assertIs(googleplayuploader.storage, storage)
        self.assertEqual(storage.__name__, 'google.cloud.storage')
        self.assertIsInstance(storage.Client, type)

    def test_first_content_upload_failure_logs_only_fixed_message(self):
        for label, make_error in failure_cases():
            with self.subTest(exception=label):
                raised = make_error()
                result = self._run(self._uploader(), {PDF_KEY: raised})
                self.assertSafeFailure(result, CONTENT_FAILURE, raised)
                self.assertUploads(result, [(PDF_KEY, PDF_BYTES)])

    def test_later_content_upload_failure_logs_only_fixed_message(self):
        for label, make_error in failure_cases():
            with self.subTest(exception=label):
                raised = make_error()
                result = self._run(self._uploader(), {EPUB_KEY_WITH_PDF: raised})
                self.assertSafeFailure(result, CONTENT_FAILURE, raised)
                self.assertUploads(
                    result, [(PDF_KEY, PDF_BYTES), (EPUB_KEY_WITH_PDF, EPUB_BYTES)])

    def test_onix_upload_failure_after_content_logs_only_fixed_message(self):
        for label, make_error in failure_cases():
            with self.subTest(exception=label):
                raised = make_error()
                result = self._run(self._uploader(), {ONIX_KEY: raised})
                self.assertSafeFailure(result, ONIX_FAILURE, raised)
                self.assertUploads(result, [
                    (PDF_KEY, PDF_BYTES),
                    (EPUB_KEY_WITH_PDF, EPUB_BYTES),
                    (ONIX_KEY, ONIX_BYTES),
                ])

    def test_successful_pdf_epub_and_onix_upload(self):
        result = self._run(self._uploader())
        self.assertIsNone(result.outcome)
        self.assertEqual(
            [(record.levelno, record.getMessage()) for record in result.records],
            [(logging.INFO, SUCCESS)])
        self.assertUploads(result, [
            (PDF_KEY, PDF_BYTES),
            (EPUB_KEY_WITH_PDF, EPUB_BYTES),
            (ONIX_KEY, ONIX_BYTES),
        ])

    def test_successful_pdf_only_upload(self):
        result = self._run(self._uploader(formats=('PDF',)))
        self.assertIsNone(result.outcome)
        self.assertEqual(
            [(record.levelno, record.getMessage()) for record in result.records],
            [(logging.INFO, SUCCESS)])
        self.assertUploads(result, [(PDF_KEY, PDF_BYTES), (ONIX_KEY, ONIX_BYTES)])

    def test_successful_epub_only_upload(self):
        result = self._run(self._uploader(formats=('EPUB',)))
        self.assertIsNone(result.outcome)
        self.assertEqual(
            [(record.levelno, record.getMessage()) for record in result.records],
            [(logging.INFO, SUCCESS)])
        self.assertUploads(
            result, [(EPUB_KEY_ONLY, EPUB_BYTES), (ONIX_KEY_EPUB_ONLY, ONIX_BYTES)])


if __name__ == '__main__':
    unittest.main()
