"""Unit tests for the DIS-02A per-work Crossref runner and adapter hardening.

The Crossref HTTP boundary is always faked. No test submits anything to
Crossref, reads a provider, or requires a production credential.
"""

import json
import unittest
from unittest.mock import MagicMock, patch

import requests

import crossref_worker_runner as runner
import crossrefuploader
from crossrefuploader import CrossrefUploader
from errors import DisseminationError


WORK_ID = '55555555-5555-4555-8555-555555555555'
PUBLISHER_ID = '66666666-6666-4666-8666-666666666666'
OTHER_PUBLISHER_ID = '77777777-7777-4777-8777-777777777777'
SECRET_USER = 'crossref-login-id'
SECRET_PW = 'crossref-super-secret-password'


def _uploader(publisher_id=PUBLISHER_ID, doi='https://doi.org/10.1000/xyz'):
    """A CrossrefUploader with its Thoth metadata already supplied."""
    instance = CrossrefUploader.__new__(CrossrefUploader)
    instance.work_id = WORK_ID
    instance.export_url = 'https://export.example'
    instance.version = '1.7.0'
    instance.metadata = {'data': {'work': {
        'workId': WORK_ID,
        'doi': doi,
        'imprint': {'publisher': {'publisherId': publisher_id}},
    }}}
    return instance


def _credential_env(publisher_id=PUBLISHER_ID):
    suffix = publisher_id.replace('-', '_')
    return {
        'crossref_user_' + suffix: SECRET_USER,
        'crossref_pw_' + suffix: SECRET_PW,
    }


class TestCrossrefCredentialHandling(unittest.TestCase):
    """Missing credentials raise a reusable error rather than exiting."""

    def test_missing_credentials_raise_instead_of_exiting(self):
        instance = _uploader()
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(
                    crossrefuploader.CrossrefCredentialsMissingError):
                instance.upload_to_platform()

    def test_credential_error_is_a_dissemination_error(self):
        # The legacy CLI turns DisseminationError into a non-zero exit.
        self.assertTrue(issubclass(
            crossrefuploader.CrossrefCredentialsMissingError,
            DisseminationError))

    def test_credential_error_never_quotes_a_credential_name_value(self):
        instance = _uploader()
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(DisseminationError) as caught:
                instance.upload_to_platform()
        self.assertNotIn(SECRET_PW, str(caught.exception))


SUCCESS_TEXT = 'Your batch submission was successfully received.'


def _response(status_code=200, text=SUCCESS_TEXT, reason='OK'):
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    response.reason = reason
    return response


class TestCrossrefTimeouts(unittest.TestCase):
    """Every Crossref call is explicitly bounded."""

    def _upload(self, get_response=None, post_response=None,
                get_side_effect=None, post_side_effect=None):
        instance = _uploader()
        with patch.dict('os.environ', _credential_env(), clear=True), \
                patch.object(instance, 'get_formatted_metadata',
                             return_value=b'<xml/>'), \
                patch.object(crossrefuploader.requests, 'get') as mock_get, \
                patch.object(crossrefuploader.requests, 'post') as mock_post:
            mock_get.return_value = get_response or _response()
            mock_post.return_value = post_response or _response()
            if get_side_effect is not None:
                mock_get.side_effect = get_side_effect
            if post_side_effect is not None:
                mock_post.side_effect = post_side_effect
            try:
                instance.upload_to_platform()
                raised = None
            except DisseminationError as error:
                raised = error
            return raised, mock_get, mock_post

    def test_prefix_lookup_uses_connect_ten_read_thirty(self):
        _, mock_get, _ = self._upload()
        self.assertEqual(mock_get.call_args.kwargs['timeout'], (10, 30))

    def test_deposit_post_uses_connect_ten_read_ninety(self):
        _, _, mock_post = self._upload()
        self.assertEqual(mock_post.call_args.kwargs['timeout'], (10, 90))

    def test_success_requires_the_exact_confirmation_text(self):
        raised, _, _ = self._upload()
        self.assertIsNone(raised)

        raised, _, _ = self._upload(
            post_response=_response(text='Received, probably.'))
        self.assertIsInstance(
            raised, crossrefuploader.CrossrefDepositRejectedError)

    def test_invalid_prefix_is_classified_and_never_deposits(self):
        raised, _, mock_post = self._upload(
            get_response=_response(status_code=404, reason='Not Found'))
        self.assertIsInstance(
            raised, crossrefuploader.CrossrefPrefixInvalidError)
        mock_post.assert_not_called()

    def test_prefix_transport_failure_is_a_lookup_failure(self):
        raised, _, mock_post = self._upload(
            get_side_effect=requests.exceptions.ReadTimeout('slow'))
        self.assertIsInstance(
            raised, crossrefuploader.CrossrefPrefixLookupError)
        mock_post.assert_not_called()

    def test_deposit_transport_failure_is_indeterminate(self):
        raised, _, _ = self._upload(
            post_side_effect=requests.exceptions.ConnectionError('reset'))
        self.assertIsInstance(
            raised, crossrefuploader.CrossrefDepositIndeterminateError)

    def test_missing_root_doi_never_reaches_the_provider(self):
        instance = _uploader(doi=None)
        with patch.dict('os.environ', _credential_env(), clear=True), \
                patch.object(instance, 'get_formatted_metadata',
                             return_value=b'<xml/>'), \
                patch.object(crossrefuploader.requests, 'get') as mock_get, \
                patch.object(crossrefuploader.requests, 'post') as mock_post:
            with self.assertRaises(crossrefuploader.CrossrefMetadataError):
                instance.upload_to_platform()
        mock_get.assert_not_called()
        mock_post.assert_not_called()


class TestCrossrefSecretContainment(unittest.TestCase):
    """Credential-bearing transport detail cannot escape the adapter."""

    def _deposit_exception_text(self, exception):
        instance = _uploader()
        with patch.dict('os.environ', _credential_env(), clear=True), \
                patch.object(instance, 'get_formatted_metadata',
                             return_value=b'<xml/>'), \
                patch.object(crossrefuploader.requests, 'get',
                             return_value=_response()), \
                patch.object(crossrefuploader.requests, 'post',
                             side_effect=exception):
            with self.assertRaises(DisseminationError) as caught:
                instance.upload_to_platform()
        return str(caught.exception)

    def test_deposit_exception_text_cannot_leak_the_effective_url(self):
        leaky = requests.exceptions.ConnectionError(
            'HTTPSConnectionPool(host=doi.crossref.org): failed for '
            '/servlet/deposit?operation=doMDUpload&login_id={}'
            '&login_passwd={}'.format(SECRET_USER, SECRET_PW))

        message = self._deposit_exception_text(leaky)

        self.assertNotIn(SECRET_PW, message)
        self.assertNotIn(SECRET_USER, message)
        self.assertNotIn('login_passwd', message)
        self.assertIn('ConnectionError', message)

    def test_leaked_transport_text_cannot_reach_the_runner_result(self):
        leaky = requests.exceptions.ConnectionError(
            'login_passwd={}'.format(SECRET_PW))
        instance = _uploader()

        with patch.dict('os.environ', _credential_env(), clear=True), \
                patch.object(instance, 'get_formatted_metadata',
                             return_value=b'<xml/>'), \
                patch.object(crossrefuploader.requests, 'get',
                             return_value=_response()), \
                patch.object(crossrefuploader.requests, 'post',
                             side_effect=leaky):
            result = runner.run(WORK_ID, PUBLISHER_ID,
                                uploader_factory=lambda _: instance)

        serialised = json.dumps(result)
        self.assertNotIn(SECRET_PW, serialised)
        self.assertNotIn('login_passwd', serialised)
        self.assertEqual(result['code'],
                         runner.CROSSREF_DEPOSIT_INDETERMINATE)

    def test_rejected_deposit_never_carries_the_raw_provider_body(self):
        instance = _uploader()
        body = 'RAW CROSSREF BODY with {} inside'.format(SECRET_PW)

        with patch.dict('os.environ', _credential_env(), clear=True), \
                patch.object(instance, 'get_formatted_metadata',
                             return_value=b'<xml/>'), \
                patch.object(crossrefuploader.requests, 'get',
                             return_value=_response()), \
                patch.object(crossrefuploader.requests, 'post',
                             return_value=_response(status_code=401,
                                                    text=body,
                                                    reason='Unauthorized')):
            result = runner.run(WORK_ID, PUBLISHER_ID,
                                uploader_factory=lambda _: instance)

        serialised = json.dumps(result)
        self.assertNotIn(SECRET_PW, serialised)
        self.assertNotIn('RAW CROSSREF BODY', serialised)
        self.assertEqual(result['code'], runner.CROSSREF_DEPOSIT_REJECTED)


class TestRunnerResultContract(unittest.TestCase):
    """The runner emits exactly one bounded, versioned structured result."""

    def _run(self, uploader_instance=None, work_id=WORK_ID,
             expected_publisher_id=PUBLISHER_ID, factory=None):
        instance = uploader_instance or _uploader()
        factory = factory or (lambda _: instance)
        return runner.run(work_id, expected_publisher_id,
                          uploader_factory=factory)

    def test_accepted_result_shape(self):
        instance = _uploader()
        with patch.object(instance, 'upload_to_platform'):
            result = self._run(instance)

        self.assertEqual(result, {
            'schemaVersion': 1,
            'status': 'ACCEPTED',
            'code': None,
            'detail': None,
            'externalWriteStarted': True,
        })

    def test_result_keys_are_exactly_the_contract(self):
        instance = _uploader()
        with patch.object(instance, 'upload_to_platform'):
            result = self._run(instance)

        self.assertEqual(sorted(result), [
            'code', 'detail', 'externalWriteStarted', 'schemaVersion',
            'status'])

    def test_publisher_mismatch_stops_before_the_deposit_path(self):
        instance = _uploader(publisher_id=OTHER_PUBLISHER_ID)
        with patch.object(instance, 'upload_to_platform') as upload:
            result = self._run(instance)

        upload.assert_not_called()
        self.assertEqual(result['status'], 'FAILED')
        self.assertEqual(result['code'], runner.CATALOGUE_PUBLISHER_MISMATCH)
        self.assertFalse(result['externalWriteStarted'])

    def test_missing_root_doi_is_metadata_invalid_before_provider_write(self):
        instance = _uploader(doi=None)
        with patch.object(instance, 'upload_to_platform') as upload:
            result = self._run(instance)

        upload.assert_not_called()
        self.assertEqual(result['code'], runner.CROSSREF_METADATA_INVALID)
        self.assertFalse(result['externalWriteStarted'])

    def test_legacy_system_exit_while_loading_metadata_is_contained(self):
        def exiting_factory(_):
            raise SystemExit(1)

        result = self._run(factory=exiting_factory)

        self.assertEqual(result['code'], runner.CROSSREF_METADATA_INVALID)
        self.assertFalse(result['externalWriteStarted'])

    def test_legacy_system_exit_inside_the_deposit_path_is_indeterminate(self):
        instance = _uploader()
        with patch.object(instance, 'upload_to_platform',
                          side_effect=SystemExit(1)):
            result = self._run(instance)

        self.assertEqual(result['status'], 'INDETERMINATE')
        self.assertEqual(result['code'],
                         runner.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertTrue(result['externalWriteStarted'])

    def test_unexpected_exception_in_the_deposit_path_is_indeterminate(self):
        instance = _uploader()
        with patch.object(instance, 'upload_to_platform',
                          side_effect=RuntimeError('boom')):
            result = self._run(instance)

        self.assertEqual(result['code'],
                         runner.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertTrue(result['externalWriteStarted'])

    def test_export_failure_is_pre_write_and_named(self):
        instance = _uploader()
        with patch.object(instance, 'upload_to_platform',
                          side_effect=DisseminationError(
                              'Error retrieving data from "https://export"')):
            result = self._run(instance)

        self.assertEqual(result['code'], runner.CROSSREF_EXPORT_FAILED)
        self.assertFalse(result['externalWriteStarted'])
        self.assertNotIn('https://export', json.dumps(result))

    def test_credential_failure_is_named_and_pre_write(self):
        instance = _uploader()
        with patch.object(
                instance, 'upload_to_platform',
                side_effect=crossrefuploader.CrossrefCredentialsMissingError(
                    'missing value for crossref_pw_x')):
            result = self._run(instance)

        self.assertEqual(result['code'], runner.CROSSREF_CREDENTIAL_MISSING)
        self.assertFalse(result['externalWriteStarted'])

    def test_non_canonical_arguments_are_refused_before_any_work(self):
        for work_id, publisher_id in [
                ('not-a-uuid', PUBLISHER_ID),
                (WORK_ID, 'not-a-uuid'),
                # Upper-case hexadecimal is a valid UUID but not canonical.
                ('AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE', PUBLISHER_ID),
                ('{55555555-5555-4555-8555-555555555555}', PUBLISHER_ID)]:
            with self.subTest(work_id=work_id, publisher_id=publisher_id):
                called = []
                result = runner.run(
                    work_id, publisher_id,
                    uploader_factory=lambda _: called.append(1))
                self.assertEqual(called, [])
                self.assertEqual(result['code'], runner.INTERNAL_WORKER_ERROR)
                self.assertFalse(result['externalWriteStarted'])

    def test_detail_is_bounded_and_single_line(self):
        instance = _uploader()
        with patch.object(
                instance, 'upload_to_platform',
                side_effect=crossrefuploader.CrossrefPrefixInvalidError(
                    'x' * 5000 + '\nsecond line')):
            result = self._run(instance)

        self.assertLessEqual(len(result['detail']), runner.MAX_DETAIL_CHARS)
        self.assertNotIn('\n', result['detail'])


class TestRunnerCommandLine(unittest.TestCase):
    """The runner exposes only the two authorised arguments."""

    def test_arguments_are_exactly_work_and_expected_publisher(self):
        args = runner.get_arguments(
            ['--work', WORK_ID, '--expected-publisher-id', PUBLISHER_ID])

        self.assertEqual(args.work_id, WORK_ID)
        self.assertEqual(args.expected_publisher_id, PUBLISHER_ID)

    def test_there_is_no_platform_argument(self):
        with patch('sys.stderr', new_callable=MagicMock):
            with self.assertRaises(SystemExit):
                runner.get_arguments([
                    '--work', WORK_ID,
                    '--expected-publisher-id', PUBLISHER_ID,
                    '--platform', 'InternetArchive'])

    def test_both_arguments_are_required(self):
        with patch('sys.stderr', new_callable=MagicMock):
            with self.assertRaises(SystemExit):
                runner.get_arguments(['--work', WORK_ID])

    def test_main_writes_one_json_object_to_stdout(self):
        with patch.object(runner, 'run',
                          return_value=runner.build_result('ACCEPTED',
                                                           external_write_started=True)):
            with patch('sys.stdout', new_callable=MagicMock) as stdout:
                exit_code = runner.main(
                    ['--work', WORK_ID,
                     '--expected-publisher-id', PUBLISHER_ID])

        written = ''.join(call.args[0] for call in stdout.write.call_args_list)
        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(written.strip())['status'], 'ACCEPTED')

    def test_main_exits_non_zero_on_a_non_accepted_result(self):
        with patch.object(runner, 'run',
                          return_value=runner.build_result(
                              'FAILED', runner.CROSSREF_PREFIX_INVALID,
                              'bad prefix')):
            with patch('sys.stdout', new_callable=MagicMock):
                exit_code = runner.main(
                    ['--work', WORK_ID,
                     '--expected-publisher-id', PUBLISHER_ID])

        self.assertEqual(exit_code, 1)


class TestLegacyCrossrefCompatibility(unittest.TestCase):
    """The legacy CLI keeps failing non-zero on every Crossref failure."""

    def test_disseminator_returns_non_zero_for_each_crossref_failure(self):
        import disseminator

        failures = [
            crossrefuploader.CrossrefCredentialsMissingError('no credentials'),
            crossrefuploader.CrossrefMetadataError('no root DOI'),
            crossrefuploader.CrossrefPrefixInvalidError('bad prefix'),
            crossrefuploader.CrossrefPrefixLookupError('lookup failed'),
            crossrefuploader.CrossrefDepositRejectedError('rejected'),
            crossrefuploader.CrossrefDepositIndeterminateError('unproven'),
        ]
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                with patch.object(disseminator, 'run', side_effect=failure), \
                        patch.object(disseminator, 'get_arguments',
                                     return_value=MagicMock(
                                         work_id=WORK_ID, platform='Crossref',
                                         export_url='https://export.example',
                                         client_url=None)):
                    self.assertEqual(disseminator.main(), 1)

    def test_crossref_stays_registered_on_the_legacy_platform_map(self):
        import disseminator

        self.assertIs(disseminator.UPLOADERS['Crossref'], CrossrefUploader)

    def test_success_confirmation_text_is_unchanged(self):
        self.assertEqual(crossrefuploader.SUCCESS_MSG,
                         'Your batch submission was successfully received.')
