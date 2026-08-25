import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github' / 'workflows' / 'ia_bulk_disseminate.yml'
DISSEMINATE = ROOT / '.github' / 'workflows' / 'disseminate.yml'
HELPER = ROOT / '.github' / 'scripts' / 'ia_selection_workflow.py'
WORK_ID = '11111111-1111-4111-8111-111111111111'


def load_yaml(path):
    """Parse workflow YAML semantically using Ruby's standard YAML parser."""
    program = (
        'data=YAML.safe_load(File.read(ARGV[0]), aliases: true);'
        'data["on"]=data.delete(true) if data.key?(true);'
        'puts JSON.generate(data)'
    )
    result = subprocess.run(
        ['ruby', '-rjson', '-ryaml', '-e', program, str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


class TestIABulkDisseminateWorkflow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.workflow = load_yaml(WORKFLOW)
        cls.disseminate = load_yaml(DISSEMINATE)

    def test_daily_schedule_replaces_old_monthly_schedule(self):
        schedules = self.workflow['on']['schedule']
        self.assertEqual(schedules, [{'cron': '40 4 * * *'}])
        self.assertNotIn('40 4 7 * *', [
            schedule['cron'] for schedule in schedules])

    def test_manual_defaults_are_bounded(self):
        inputs = self.workflow['on']['workflow_dispatch']['inputs']
        self.assertEqual(inputs['lookback_hours']['default'], 30)
        self.assertEqual(inputs['max_works']['default'], 200)
        self.assertEqual(inputs['max_works']['type'], 'number')

    def test_dedicated_workflow_does_not_call_generic_bulk(self):
        uses = [
            job.get('uses') for job in self.workflow['jobs'].values()
            if isinstance(job, dict)
        ]
        self.assertNotIn(
            './.github/workflows/bulk_disseminate.yml', uses)
        self.assertIn('./.github/workflows/disseminate.yml', uses)

    def test_workflow_concurrency_queues_runs(self):
        self.assertEqual(self.workflow['concurrency'], {
            'group': 'internet-archive-scheduled-dissemination',
            'cancel-in-progress': False,
        })

    def test_selection_job_is_read_only_and_has_no_environment(self):
        select = self.workflow['jobs']['select']
        self.assertNotIn('environment', select)
        self.assertEqual(select['permissions'], {'contents': 'read'})

    def test_selection_job_uses_required_runtime(self):
        steps = self.workflow['jobs']['select']['steps']
        self.assertEqual(steps[0]['uses'], 'actions/checkout@v6')
        setup = next(
            step for step in steps
            if step.get('uses') == 'actions/setup-python@v6')
        self.assertEqual(setup['with']['python-version'], '3.12')
        install = next(
            step for step in steps
            if step.get('name') == 'Install selection dependencies')
        self.assertIn('requirements_obtain_new_ids.txt', install['run'])

    def test_selection_step_passes_only_non_secret_selection_variables(self):
        step = next(
            step for step in self.workflow['jobs']['select']['steps']
            if step.get('name') == 'Select recently updated eligible works')
        self.assertEqual(set(step['env']), {
            'ENV_PUBLISHERS', 'ENV_EXCEPTIONS', 'PUBLISHER_SOURCE_MODES'})
        serialised = json.dumps(step)
        for credential in (
                'THOTH_PAT', 'IA_S3_ACCESS', 'IA_S3_SECRET',
                'ia_s3_access', 'ia_s3_secret'):
            self.assertNotIn(credential, serialised)

    def test_selection_command_uses_bounded_inputs_and_report(self):
        step = next(
            step for step in self.workflow['jobs']['select']['steps']
            if step.get('name') == 'Select recently updated eligible works')
        command = step['run']
        self.assertIn('--lookback-hours', command)
        self.assertIn('lookback_hours="${{ inputs.lookback_hours }}"', command)
        self.assertIn('lookback_hours=30', command)
        self.assertIn('--max-ids', command)
        self.assertIn('max_works="${{ inputs.max_works }}"', command)
        self.assertIn('max_works=200', command)
        self.assertIn('--report internet-archive-selection.json', command)
        self.assertIn(
            '--comparison-report publisher-comparison.json', command)

    def test_selection_artifact_is_always_uploaded_for_30_days(self):
        step = next(
            step for step in self.workflow['jobs']['select']['steps']
            if step.get('uses') == 'actions/upload-artifact@v7')
        self.assertEqual(step['if'], '${{ always() }}')
        self.assertEqual(step['with']['retention-days'], 30)
        self.assertEqual(step['with']['if-no-files-found'], 'warn')
        self.assertIn('internet-archive-selection.json', step['with']['path'])
        self.assertIn('internet-archive-selection.log', step['with']['path'])
        self.assertIn('publisher-comparison.json', step['with']['path'])
        artifact = self.workflow['jobs']['select']['env']['ARTIFACT_NAME']
        self.assertIn('ia-selection-', artifact)
        self.assertIn('github.run_id', artifact)
        self.assertIn('github.run_attempt', artifact)

    def test_selection_summary_always_runs(self):
        step = next(
            step for step in self.workflow['jobs']['select']['steps']
            if step.get('name') == 'Write selection summary')
        self.assertEqual(step['if'], '${{ always() }}')
        self.assertIn(' summary', step['run'])

    def test_selection_outputs_include_status_and_overflow(self):
        outputs = self.workflow['jobs']['select']['outputs']
        self.assertEqual(set(outputs), {
            'work_ids', 'selected_count', 'omitted_count', 'truncated',
            'selection_exit_status', 'artifact_name',
        })

    def test_selection_reads_the_central_publisher_source_variable(self):
        step = next(
            step for step in self.workflow['jobs']['select']['steps']
            if step.get('name') == 'Select recently updated eligible works')
        self.assertEqual(
            step['env']['PUBLISHER_SOURCE_MODES'],
            '${{ vars.PUBLISHER_SOURCE_MODES }}',
        )

    def test_publisher_comparison_summary_is_separate_and_non_gating(self):
        steps = self.workflow['jobs']['select']['steps']
        step = next(
            step for step in steps
            if step.get('name') == 'Summarise publisher comparison')
        self.assertEqual(step['if'], '${{ always() }}')
        self.assertTrue(step['continue-on-error'])
        self.assertIn('publisher_source.py summary', step['run'])
        self.assertIn('--platform InternetArchive', step['run'])
        self.assertEqual(
            step['env']['PUBLISHER_SOURCE_MODES'],
            '${{ vars.PUBLISHER_SOURCE_MODES }}',
        )

    def test_publisher_comparison_does_not_change_the_selection_guards(self):
        steps = self.workflow['jobs']['select']['steps']
        guards = [
            step for step in steps
            if 'ia_selection_workflow.py' in step.get('run', '')
        ]
        self.assertEqual(len(guards), 3)
        for step in guards:
            with self.subTest(step=step.get('name')):
                self.assertNotIn('publisher-comparison', step['run'])
                self.assertNotIn('continue-on-error', step)

    def test_ia_selection_helper_is_unchanged_by_this_task(self):
        helper = HELPER.read_text(encoding='utf-8')
        self.assertNotIn('publisher-comparison', helper)
        self.assertNotIn('PUBLISHER_SOURCE_MODES', helper)
        self.assertNotIn('publisher_source', helper)

    def test_oapen_location_catchup_workflow_is_untouched(self):
        workflow = load_yaml(
            ROOT / '.github' / 'workflows' / 'oapen_catchup_locations.yaml')
        serialised = json.dumps(workflow)
        self.assertNotIn('PUBLISHER_SOURCE_MODES', serialised)
        self.assertNotIn('publisher-comparison', serialised)
        self.assertIn('--locations', serialised)

    def test_matrix_uses_only_selected_ids_with_four_way_parallelism(self):
        job = self.workflow['jobs']['disseminate']
        self.assertEqual(job['strategy']['fail-fast'], False)
        self.assertEqual(job['strategy']['max-parallel'], 4)
        self.assertEqual(
            job['strategy']['matrix']['work-id'],
            '${{ fromJSON(needs.select.outputs.work_ids) }}',
        )
        self.assertEqual(job['with'], {
            'platform': 'InternetArchive',
            'work-id': '${{ matrix.work-id }}',
        })

    def test_empty_selection_skips_dissemination(self):
        self.assertEqual(
            self.workflow['jobs']['disseminate']['if'],
            "${{ needs.select.outputs.selected_count != '0' }}",
        )

    def test_final_status_always_depends_on_both_upstreams(self):
        final = self.workflow['jobs']['final-status']
        self.assertEqual(final['if'], '${{ always() }}')
        self.assertEqual(set(final['needs']), {'select', 'disseminate'})
        self.assertEqual(final['permissions'], {'contents': 'read'})
        self.assertNotIn('environment', final)
        step = final['steps'][-1]
        self.assertEqual(set(step['env']), {
            'SELECTION_RESULT', 'DISSEMINATION_RESULT', 'SELECTED_COUNT',
            'OMITTED_COUNT', 'TRUNCATED', 'ARTIFACT_NAME',
        })

    def test_reusable_dissemination_has_platform_work_concurrency(self):
        concurrency = self.disseminate['concurrency']
        self.assertEqual(concurrency['cancel-in-progress'], False)
        self.assertIn('inputs.platform', concurrency['group'])
        self.assertIn('inputs.work-id', concurrency['group'])

    def test_other_platform_schedules_are_unchanged(self):
        expected = {
            'cr_bulk_disseminate.yml': '45 * * * *',
            'fs_bulk_disseminate.yml': '40 4 7 * *',
            'zn_bulk_disseminate.yml': '40 4 7 * *',
            'cul_bulk_disseminate.yml': '40 4 7 * *',
            'gp_bulk_disseminate.yaml': '50 5 * * *',
            'bkci_bulk_disseminate.yaml': '40 6 6 * *',
            'oapen_bulk_disseminate.yaml': '20 2 * * 1',
            'eh_bulk_disseminate.yaml': '20 2 * * 2',
            'jstor_bulk_disseminate.yaml': '20 2 * * 3',
            'muse_bulk_disseminate.yaml': '20 2 * * 4',
            'pq_bulk_disseminate.yaml': '20 2 * * 5',
        }
        for filename, cron in expected.items():
            with self.subTest(filename=filename):
                workflow = load_yaml(
                    ROOT / '.github' / 'workflows' / filename)
                self.assertEqual(
                    workflow['on']['schedule'], [{'cron': cron}])

    def test_tests_do_not_dispatch_mutation_workflows(self):
        source = Path(__file__).read_text(encoding='utf-8')
        self.assertNotIn('gh' + ' workflow run', source)
        self.assertNotIn('curl ' + '-X POST', source)


class TestIASelectionWorkflowHelper(unittest.TestCase):

    def run_helper(self, *arguments, report=None, ids=None, status=0, **env):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        workdir = Path(temporary_directory.name)
        report_path = workdir / 'report.json'
        ids_path = workdir / 'selected.json'
        status_path = workdir / 'status.txt'
        output_path = workdir / 'github-output'
        summary_path = workdir / 'step-summary'
        if report is not None:
            report_path.write_text(json.dumps(report), encoding='utf-8')
        if ids is not None:
            ids_path.write_text(json.dumps(ids), encoding='utf-8')
        status_path.write_text('{}\n'.format(status), encoding='utf-8')
        environment = {
            **os.environ,
            'GITHUB_OUTPUT': str(output_path),
            'GITHUB_STEP_SUMMARY': str(summary_path),
            **env,
        }
        replacements = {
            '{report}': str(report_path),
            '{ids}': str(ids_path),
            '{status}': str(status_path),
        }
        command = [
            replacements.get(argument, argument) for argument in arguments]
        result = subprocess.run(
            [sys.executable, str(HELPER), *command],
            cwd=workdir,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        return result, workdir

    @staticmethod
    def report(selected=None, omitted=None, truncated=False):
        selected = selected or []
        omitted = omitted or []
        return {
            'window': {
                'start': '2026-07-21T22:40:00Z',
                'end': '2026-07-23T04:40:00Z',
                'lookback_hours': 30,
            },
            'publisher_ids': ['aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'],
            'queried_count': len(selected) + len(omitted),
            'eligible_count': len(selected) + len(omitted),
            'selected_count': len(selected),
            'omitted_count': len(omitted),
            'truncated': truncated,
            'selected': [
                {
                    'work_id': work_id,
                    'updated_at_with_relations': '2026-07-23T04:00:00Z',
                } for work_id in selected
            ],
            'omitted': [
                {
                    'work_id': work_id,
                    'updated_at_with_relations': '2026-07-23T04:01:00Z',
                } for work_id in omitted
            ],
            'excluded_counts': {'configured_exception': 2},
        }

    def test_outputs_are_compact_and_machine_readable(self):
        report = self.report([WORK_ID])
        result, workdir = self.run_helper(
            'outputs',
            '--report', '{report}',
            '--selected-ids', '{ids}',
            '--status', '{status}',
            '--artifact-name', 'ia-selection-123-1',
            report=report,
            ids=[WORK_ID],
        )
        self.assertEqual(result.returncode, 0)
        output = (workdir / 'github-output').read_text(encoding='utf-8')
        self.assertIn('work_ids=["{}"]\n'.format(WORK_ID), output)
        self.assertIn('selected_count=1\n', output)
        self.assertIn('truncated=false\n', output)

    def test_selection_guard_rejects_failed_selection(self):
        result, _workdir = self.run_helper(
            'selection-guard',
            '--report', '{report}',
            '--selected-ids', '{ids}',
            '--status', '{status}',
            report=self.report(),
            ids=[],
            status=1,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn('Selection failed', result.stderr)

    def test_selection_guard_rejects_mismatched_report_and_stdout(self):
        result, _workdir = self.run_helper(
            'selection-guard',
            '--report', '{report}',
            '--selected-ids', '{ids}',
            '--status', '{status}',
            report=self.report([WORK_ID]),
            ids=[],
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn('does not match', result.stderr)

    def test_summary_contains_counts_not_work_ids(self):
        result, workdir = self.run_helper(
            'summary',
            '--report', '{report}',
            '--status', '{status}',
            '--artifact-name', 'ia-selection-123-1',
            report=self.report([WORK_ID]),
            ids=[WORK_ID],
        )
        self.assertEqual(result.returncode, 0)
        summary = (workdir / 'step-summary').read_text(encoding='utf-8')
        self.assertIn('- Selected works: `1`', summary)
        self.assertIn('- `configured_exception`: 2', summary)
        self.assertIn('ia-selection-123-1', summary)
        self.assertNotIn(WORK_ID, summary)

    def test_truncation_causes_final_guard_failure(self):
        result, workdir = self.run_helper(
            'final-guard',
            SELECTION_RESULT='success',
            DISSEMINATION_RESULT='success',
            SELECTED_COUNT='200',
            OMITTED_COUNT='3',
            TRUNCATED='true',
            ARTIFACT_NAME='ia-selection-123-1',
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn('3 eligible works were omitted', result.stderr)
        summary = (workdir / 'step-summary').read_text(encoding='utf-8')
        self.assertIn('bounded manual reconciliation', summary)
        self.assertIn('ia-selection-123-1', summary)

    def test_selection_failure_causes_final_guard_failure(self):
        result, _workdir = self.run_helper(
            'final-guard',
            SELECTION_RESULT='failure',
            DISSEMINATION_RESULT='skipped',
            SELECTED_COUNT='0',
            OMITTED_COUNT='0',
            TRUNCATED='false',
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn('selection job result', result.stderr)

    def test_dissemination_failure_is_not_concealed(self):
        result, _workdir = self.run_helper(
            'final-guard',
            SELECTION_RESULT='success',
            DISSEMINATION_RESULT='failure',
            SELECTED_COUNT='2',
            OMITTED_COUNT='0',
            TRUNCATED='false',
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn('dissemination job result', result.stderr)

    def test_non_truncated_empty_selection_succeeds(self):
        result, _workdir = self.run_helper(
            'final-guard',
            SELECTION_RESULT='success',
            DISSEMINATION_RESULT='skipped',
            SELECTED_COUNT='0',
            OMITTED_COUNT='0',
            TRUNCATED='false',
        )
        self.assertEqual(result.returncode, 0)

    def test_successful_nonempty_selection_succeeds(self):
        result, _workdir = self.run_helper(
            'final-guard',
            SELECTION_RESULT='success',
            DISSEMINATION_RESULT='success',
            SELECTED_COUNT='2',
            OMITTED_COUNT='0',
            TRUNCATED='false',
        )
        self.assertEqual(result.returncode, 0)


if __name__ == '__main__':
    unittest.main()
