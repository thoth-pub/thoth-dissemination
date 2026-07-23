import importlib.util
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from reconcile_internet_archive import (
    InternetArchiveReconciler,
    ReconciliationConfigurationError,
    _base_result,
    load_local_environment,
    main,
    validate_apply_credentials,
)
from write_locations import LocationInput


WORK_ID = '11111111-2222-3333-4444-555555555555'
WORK_ID_2 = '22222222-3333-4444-5555-666666666666'
PUBLICATION_ID = '99999999-8888-7777-6666-555555555555'
LANDING_PAGE = 'https://archive.org/details/{}'.format(WORK_ID)
FULL_TEXT_URL = 'https://archive.org/download/{}/{}.pdf'.format(
    WORK_ID, WORK_ID)
CREDENTIALS = {
    'ia_s3_access': 'config-access',
    'ia_s3_secret': 'config-secret',
    'THOTH_PAT': 'config-token',
}


def result_for(work_id=WORK_ID, status='current', actions=()):
    result = _base_result(work_id)
    result.update({
        'publisher_id': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
        'title': 'A Test Book',
        'publication_id': PUBLICATION_ID,
        'pdf_source_url': 'https://source.example/book.pdf',
        'eligible': True,
        'status': status,
        'issues': [] if status == 'current' else [status],
        'recommended_actions': list(actions),
        'auto_applicable_actions': list(actions),
        'error': None,
    })
    return result


def location_input():
    return LocationInput(
        PUBLICATION_ID,
        'INTERNET_ARCHIVE',
        LANDING_PAGE,
        FULL_TEXT_URL,
        '0123456789abcdef0123456789abcdef',
        'MD5',
    )


def existing_location(**overrides):
    values = {
        'locationId': 'existing-location',
        'publicationId': PUBLICATION_ID,
        'locationPlatform': 'INTERNET_ARCHIVE',
        'landingPage': LANDING_PAGE,
        'fullTextUrl': FULL_TEXT_URL,
        'canonical': False,
        'checksum': '0123456789abcdef0123456789abcdef',
        'checksumAlgorithm': 'MD5',
    }
    values.update(overrides)
    return values


def apply_context():
    return {
        'uploader': MagicMock(),
        'desired': SimpleNamespace(publication_id=PUBLICATION_ID),
        'item': object(),
        'archive_inspection': {},
        'location_input': location_input(),
    }


def write_config(path, values=CREDENTIALS):
    path.write_text(
        ''.join('{}={}\n'.format(key, value) for key, value in values.items()),
        encoding='utf-8',
    )


class TestReconciliationCliStdout(unittest.TestCase):
    def _reconciler(self, before, context=None, final=None):
        thoth = MagicMock()
        thoth.create_location.return_value = 'created-location'
        thoth.update_location.return_value = 'updated-location'
        reconciler = InternetArchiveReconciler(thoth=thoth)
        reconciler.select_work_ids = MagicMock(return_value=[WORK_ID])
        reconciler.inspect_work = MagicMock(
            return_value=(before, context if context is not None else {}))
        reconciler._inspect_remote = MagicMock(
            return_value=final or result_for())
        return reconciler

    def _run(
            self, reconciler, arguments, existing=(), environment=None,
            capture_logs=False):
        stdout = io.StringIO()
        stderr = io.StringIO()
        environment = {} if environment is None else environment
        contexts = [
            patch.dict(os.environ, environment, clear=True),
            patch(
                'reconcile_internet_archive.load_local_environment',
                return_value=None,
            ),
            patch(
                'reconcile_internet_archive.InternetArchiveReconciler',
                return_value=reconciler,
            ),
            patch(
                'write_locations.retrieve_existing_locations',
                return_value=list(existing),
            ),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ]
        for context in contexts:
            context.__enter__()
        log_context = self.assertLogs(level='INFO') if capture_logs else None
        log_watcher = None
        try:
            if log_context is not None:
                log_watcher = log_context.__enter__()
            status = main(arguments)
        finally:
            if log_context is not None:
                log_context.__exit__(None, None, None)
            for context in reversed(contexts):
                context.__exit__(None, None, None)
        return SimpleNamespace(
            status=status,
            stdout=stdout.getvalue(),
            stderr=stderr.getvalue(),
            logs=[] if log_watcher is None else log_watcher.output,
        )

    def test_apply_create_keeps_default_json_stdout_parseable(self):
        before = result_for(
            status='location_missing',
            actions=('create_thoth_location',),
        )
        reconciler = self._reconciler(before, apply_context())

        run = self._run(
            reconciler,
            ['--work-id', WORK_ID, '--apply'],
            environment=CREDENTIALS,
            capture_logs=True,
        )

        report = json.loads(run.stdout)
        self.assertEqual(run.status, 0)
        self.assertEqual(report['results'][0]['status'], 'current')
        self.assertNotIn('created-location', run.stdout)
        self.assertIn('created-location', '\n'.join(run.logs))

    def test_apply_update_keeps_default_json_stdout_parseable(self):
        before = result_for(
            status='location_stale',
            actions=('update_thoth_location',),
        )
        reconciler = self._reconciler(before, apply_context())

        run = self._run(
            reconciler,
            ['--work-id', WORK_ID, '--apply'],
            existing=[existing_location(
                landingPage='https://archive.org/details/old')],
            environment=CREDENTIALS,
        )

        json.loads(run.stdout)
        self.assertNotIn('updated-location', run.stdout)
        reconciler.thoth.update_location.assert_called_once()

    def test_apply_location_jsonl_has_only_json_lines(self):
        before = result_for(
            status='location_missing',
            actions=('create_thoth_location',),
        )
        reconciler = self._reconciler(before, apply_context())

        run = self._run(
            reconciler,
            ['--work-id', WORK_ID, '--apply', '--format', 'jsonl'],
            environment=CREDENTIALS,
        )

        lines = run.stdout.strip().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            json.loads(line)
        self.assertFalse(any(line == 'created-location' for line in lines))

    def test_output_file_receives_report_and_stdout_stays_empty(self):
        before = result_for(
            status='location_missing',
            actions=('create_thoth_location',),
        )
        reconciler = self._reconciler(before, apply_context())

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'report.json'
            run = self._run(
                reconciler,
                [
                    '--work-id', WORK_ID,
                    '--apply',
                    '--output', str(output),
                ],
                environment=CREDENTIALS,
                capture_logs=True,
            )
            report = json.loads(output.read_text(encoding='utf-8'))

        self.assertEqual(run.stdout, '')
        self.assertEqual(report['results'][0]['status'], 'current')
        self.assertIn('created-location', '\n'.join(run.logs))

    def test_dry_run_without_credentials_has_valid_json_stdout(self):
        reconciler = self._reconciler(result_for())

        run = self._run(reconciler, ['--work-id', WORK_ID])

        report = json.loads(run.stdout)
        self.assertEqual(run.status, 0)
        self.assertEqual(report['results'][0]['status'], 'current')

    def test_apply_without_location_action_keeps_json_contract(self):
        reconciler = self._reconciler(result_for())

        run = self._run(
            reconciler,
            ['--work-id', WORK_ID, '--apply'],
            environment=CREDENTIALS,
        )

        report = json.loads(run.stdout)
        self.assertEqual(report['results'][0]['applied_actions'], [])
        reconciler.thoth.create_location.assert_not_called()
        reconciler.thoth.update_location.assert_not_called()

    def test_location_mutation_failure_still_emits_valid_report_only(self):
        before = result_for(
            status='location_missing',
            actions=('create_thoth_location',),
        )
        reconciler = self._reconciler(before, apply_context())
        reconciler.thoth.create_location.side_effect = RuntimeError(
            'mutation failed before an ID existed')

        run = self._run(
            reconciler,
            ['--work-id', WORK_ID, '--apply'],
            environment=CREDENTIALS,
        )

        report = json.loads(run.stdout)
        self.assertEqual(run.status, 1)
        self.assertIn(
            'thoth_location_mutation_failed',
            report['results'][0]['issues'],
        )
        self.assertNotIn('created-location', run.stdout)

    def test_multiple_location_mutations_do_not_add_jsonl_id_lines(self):
        first_before = result_for(
            status='location_missing',
            actions=('create_thoth_location',),
        )
        second_before = result_for(
            WORK_ID_2,
            status='location_missing',
            actions=('create_thoth_location',),
        )
        reconciler = self._reconciler(first_before, apply_context())
        reconciler.select_work_ids.return_value = [WORK_ID, WORK_ID_2]
        reconciler.inspect_work.side_effect = [
            (first_before, apply_context()),
            (second_before, apply_context()),
        ]
        reconciler._inspect_remote.side_effect = [
            result_for(WORK_ID),
            result_for(WORK_ID_2),
        ]
        reconciler.thoth.create_location.side_effect = [
            'first-location',
            'second-location',
        ]

        run = self._run(
            reconciler,
            [
                '--work-id', WORK_ID,
                '--work-id', WORK_ID_2,
                '--apply',
                '--format', 'jsonl',
            ],
            environment=CREDENTIALS,
        )

        lines = run.stdout.strip().splitlines()
        self.assertEqual(len(lines), 3)
        for line in lines:
            json.loads(line)
        self.assertNotIn('first-location', run.stdout)
        self.assertNotIn('second-location', run.stdout)


class TestLocalEnvironmentLoading(unittest.TestCase):
    def test_config_only_apply_credentials_pass_validation(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, {}, clear=True):
            config = Path(directory) / 'config.env'
            write_config(config)

            load_local_environment(config)
            credentials = validate_apply_credentials()

        self.assertEqual(credentials, CREDENTIALS)

    def test_main_loads_config_before_validation_and_client_creation(self):
        events = []
        reconciler = MagicMock()
        reconciler.select_work_ids.return_value = []
        reconciler.reconcile.return_value = []

        def load():
            events.append('load')

        def validate():
            events.append('validate')
            return CREDENTIALS

        def create():
            events.append('create')
            return reconciler

        with patch(
                'reconcile_internet_archive.load_local_environment',
                side_effect=load), patch(
                'reconcile_internet_archive.validate_apply_credentials',
                side_effect=validate), patch(
                'reconcile_internet_archive.InternetArchiveReconciler',
                side_effect=create), redirect_stdout(io.StringIO()):
            status = main(['--work-id', WORK_ID, '--apply'])

        self.assertEqual(status, 0)
        self.assertEqual(events, ['load', 'validate', 'create'])

    def test_process_environment_overrides_config_file(self):
        process_values = {
            'ia_s3_access': 'process-access',
            'ia_s3_secret': 'process-secret',
            'THOTH_PAT': 'process-token',
        }
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, process_values, clear=True):
            config = Path(directory) / 'config.env'
            write_config(config)

            load_local_environment(config)
            credentials = validate_apply_credentials()

        self.assertEqual(credentials, process_values)

    def test_missing_config_file_is_harmless(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, {}, clear=True):
            missing = Path(directory) / 'missing-config.env'
            self.assertIsNone(load_local_environment(missing))

    def test_partial_config_reports_exact_missing_variables(self):
        partial = {'ia_s3_access': 'access'}
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, {}, clear=True):
            config = Path(directory) / 'config.env'
            write_config(config, partial)
            load_local_environment(config)

            with self.assertRaisesRegex(
                    ReconciliationConfigurationError,
                    r'Apply mode requires: ia_s3_secret, THOTH_PAT$'):
                validate_apply_credentials()

    def test_dry_run_succeeds_without_credentials_or_config_file(self):
        reconciler = MagicMock()
        reconciler.select_work_ids.return_value = [WORK_ID]
        reconciler.reconcile.return_value = [result_for()]
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, {}, clear=True), patch(
                'reconcile_internet_archive.load_local_environment',
                side_effect=lambda: load_local_environment(
                    Path(directory) / 'missing.env')), patch(
                'reconcile_internet_archive.InternetArchiveReconciler',
                return_value=reconciler), redirect_stdout(io.StringIO()):
            status = main(['--work-id', WORK_ID])

        self.assertEqual(status, 0)

    def test_github_style_process_environment_works_without_config(self):
        reconciler = MagicMock()
        reconciler.select_work_ids.return_value = [WORK_ID]
        reconciler.reconcile.return_value = [result_for()]
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, CREDENTIALS, clear=True), patch(
                'reconcile_internet_archive.load_local_environment',
                side_effect=lambda: load_local_environment(
                    Path(directory) / 'missing.env')), patch(
                'reconcile_internet_archive.InternetArchiveReconciler',
                return_value=reconciler), redirect_stdout(io.StringIO()):
            status = main(['--work-id', WORK_ID, '--apply'])

        self.assertEqual(status, 0)
        reconciler.thoth.set_token.assert_called_once_with(
            CREDENTIALS['THOTH_PAT'])

    def test_config_thoth_pat_is_passed_to_client(self):
        reconciler = MagicMock()
        reconciler.select_work_ids.return_value = [WORK_ID]
        reconciler.reconcile.return_value = [result_for()]
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, {}, clear=True):
            config = Path(directory) / 'config.env'
            write_config(config)
            with patch(
                    'reconcile_internet_archive.load_local_environment',
                    side_effect=lambda: load_local_environment(config)), patch(
                    'reconcile_internet_archive.InternetArchiveReconciler',
                    return_value=reconciler), redirect_stdout(io.StringIO()):
                status = main(['--work-id', WORK_ID, '--apply'])

        self.assertEqual(status, 0)
        reconciler.thoth.set_token.assert_called_once_with(
            CREDENTIALS['THOTH_PAT'])

    def test_config_archive_credentials_reach_selected_mutation(self):
        before = result_for(
            status='metadata_stale',
            actions=('update_archive_metadata',),
        )
        context = apply_context()
        reconciler = InternetArchiveReconciler(thoth=MagicMock())
        reconciler.inspect_work = MagicMock(return_value=(before, context))
        reconciler._inspect_remote = MagicMock(return_value=result_for())

        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, {}, clear=True):
            config = Path(directory) / 'config.env'
            write_config(config)
            load_local_environment(config)
            credentials = validate_apply_credentials()
            result = reconciler.reconcile_one(
                WORK_ID,
                apply=True,
                credentials=credentials,
            )

        self.assertEqual(result['status'], 'current')
        context['uploader'].apply_archive_repairs.assert_called_once()
        call = context['uploader'].apply_archive_repairs.call_args
        self.assertEqual(call.kwargs['access_key'], 'config-access')
        self.assertEqual(call.kwargs['secret_key'], 'config-secret')

    def test_current_apply_does_not_consume_archive_credentials(self):
        context = apply_context()
        reconciler = InternetArchiveReconciler(thoth=MagicMock())
        reconciler.inspect_work = MagicMock(
            return_value=(result_for(), context))

        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, {}, clear=True):
            config = Path(directory) / 'config.env'
            write_config(config)
            load_local_environment(config)
            credentials = validate_apply_credentials()
            result = reconciler.reconcile_one(
                WORK_ID,
                apply=True,
                credentials=credentials,
            )

        self.assertEqual(result['status'], 'current')
        context['uploader'].apply_archive_repairs.assert_not_called()

    def test_import_does_not_load_config(self):
        module_path = (
            Path(__file__).resolve().parents[1]
            / 'reconcile_internet_archive.py'
        )
        spec = importlib.util.spec_from_file_location(
            'reconcile_import_probe',
            module_path,
        )
        module = importlib.util.module_from_spec(spec)

        with patch('dotenv.load_dotenv') as load_dotenv:
            spec.loader.exec_module(module)

        load_dotenv.assert_not_called()

    def test_config_secret_in_exception_is_redacted_from_report(self):
        reconciler = InternetArchiveReconciler(thoth=MagicMock())
        reconciler.select_work_ids = MagicMock(return_value=[WORK_ID])
        reconciler.reconcile_one = MagicMock(
            side_effect=RuntimeError(
                'remote rejected {}'.format(CREDENTIALS['ia_s3_secret'])))
        stdout = io.StringIO()

        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, {}, clear=True):
            config = Path(directory) / 'config.env'
            write_config(config)
            with patch(
                    'reconcile_internet_archive.load_local_environment',
                    side_effect=lambda: load_local_environment(config)), patch(
                    'reconcile_internet_archive.InternetArchiveReconciler',
                    return_value=reconciler), redirect_stdout(stdout):
                status = main(['--work-id', WORK_ID, '--apply'])

        self.assertEqual(status, 1)
        json.loads(stdout.getvalue())
        self.assertNotIn(CREDENTIALS['ia_s3_secret'], stdout.getvalue())
        self.assertIn('[REDACTED]', stdout.getvalue())


if __name__ == '__main__':
    unittest.main()
