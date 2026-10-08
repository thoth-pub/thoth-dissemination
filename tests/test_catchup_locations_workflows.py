"""Run the catch-up location workflows' shell steps locally.

Steps are rendered the way the GitHub Actions runner renders them: `${{ }}`
expressions in `run:` are substituted textually into the script before the
shell parses it, while expressions in `env:` become plain environment values.
Scripts then run under the runner's default shell for steps without `shell:`
(`bash -e`). OAPEN/DOAB are replaced by a local `requests` double and Thoth by
a mock client, so nothing here contacts an external API or writes to Thoth.
"""
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID

import obtain_new_ids
import write_locations


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / '.github' / 'workflows'
OAPEN_WORKFLOW = WORKFLOWS / 'oapen_catchup_locations.yaml'
MUSE_WORKFLOW = WORKFLOWS / 'muse_catchup_locations.yaml'
EXPRESSION = re.compile(r'\$\{\{\s*(.*?)\s*\}\}')

PUBLISHER_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
PUBLICATION_1 = '9a8b7c6d-1111-4222-8333-444455556666'
PUBLICATION_2 = '0f1e2d3c-5555-4666-8777-888899990000'
PUBLICATION_3 = '12345678-9abc-4def-8123-456789abcdef'
# DOIs may legitimately contain characters that are special to the shell.
SICI_DOI = '10.1002/(SICI)1097-4636(199706)35:4<425::AID-JBM3>3.0.CO;2-K'
SHELL_DOI = "10.5555/o'neill-$HOME-`id`"
PLAIN_DOI = '10.11647/obp.0001'
# Bitstream names come from the OAPEN API and are written verbatim into URLs.
HOSTILE_FILE_NAME = (
    'chapter$(touch${IFS}pwned)`touch${IFS}pwned`"dq"\'sq\';&|*?<>\\bs.pdf')

FAKE_REQUESTS = '''\
"""Local stand-in for requests: canned responses only, every URL recorded."""
import json
import os


class ConnectionError(Exception):
    pass


class _Response:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.content = json.dumps(body).encode('utf-8')


with open(os.environ['FAKE_API_RESPONSES'], encoding='utf-8') as _file:
    _RESPONSES = json.load(_file)


def get(url, headers=None, **kwargs):
    with open(os.environ['FAKE_API_REQUESTS'], 'a', encoding='utf-8') as log:
        log.write(json.dumps(url) + '\\n')
    status_code, body = _RESPONSES[url]
    return _Response(status_code, body)
'''


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


def render(template, context):
    """Substitute `${{ }}` expressions textually, as the runner does."""
    return EXPRESSION.sub(lambda match: context[match.group(1)], str(template))


def read_outputs(path):
    """Parse `name=value` lines the way the runner reads $GITHUB_OUTPUT."""
    outputs = {}
    if path.exists():
        for line in path.read_text(encoding='utf-8').splitlines():
            name, separator, value = line.partition('=')
            if not separator:
                raise AssertionError(
                    'Unsupported GITHUB_OUTPUT line: {!r}'.format(line))
            outputs[name] = value
    return outputs


def compact(records):
    return json.dumps(records, separators=(',', ':'))


def oapen_url(doi):
    return ('https://library.oapen.org/rest/search?query='
            'oapen.identifier.doi:%22{}%22&expand=metadata,bitstreams'
            .format(doi))


def doab_url(doi):
    return ('https://directory.doabooks.org/rest/search?query='
            'oapen.identifier.doi:%22{}%22&expand=metadata'.format(doi))


def oapen_body(handle, file_name):
    return [{
        'handle': handle,
        'bitstreams': [{'bundleName': 'ORIGINAL', 'name': file_name}],
    }]


def oapen_urls(handle, file_name):
    return (
        'https://library.oapen.org/handle/{}'.format(handle),
        'https://library.oapen.org/bitstream/handle/{}/{}'
        '?sequence=1&isAllowed=y'.format(handle, file_name),
    )


def doab_landing_page(handle):
    return 'https://directory.doabooks.org/handle/{}'.format(handle)


def location_line(publication_id, platform, landing_page, full_text_url):
    return '{} {} {} {} None None'.format(
        publication_id, platform, landing_page, full_text_url)


def created_location(publication_id, platform, landing_page, full_text_url):
    return {
        'publicationId': publication_id,
        'landingPage': landing_page,
        'fullTextUrl': full_text_url,
        'locationPlatform': platform,
        'canonical': False,
        'checksum': None,
        'checksumAlgorithm': None,
    }


def thoth_work(work_id, doi, publication_id, present=()):
    return SimpleNamespace(
        workId=work_id,
        doi='https://doi.org/{}'.format(doi),
        publications=[SimpleNamespace(
            publicationId=publication_id,
            publicationType='PDF',
            locations=[
                SimpleNamespace(locationPlatform=platform)
                for platform in present
            ],
        )],
    )


class WorkflowStepTestCase(unittest.TestCase):
    workflow_path = None

    @classmethod
    def setUpClass(cls):
        cls.workflow = load_yaml(cls.workflow_path)

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        bin_dir = self.root / 'bin'
        bin_dir.mkdir()
        shim = bin_dir / 'python'
        shim.write_text('#!/bin/sh\nexec "{}" "$@"\n'.format(sys.executable))
        shim.chmod(0o755)
        fakes = self.root / 'fakes'
        fakes.mkdir()
        (fakes / 'requests.py').write_text(FAKE_REQUESTS, encoding='utf-8')
        self.runs = 0
        # A minimal environment: no inherited credentials reach the steps.
        self.base_env = {
            'PATH': '{}:{}'.format(bin_dir, os.environ.get('PATH', '')),
            'PYTHONPATH': str(fakes),
            'PYTHONDONTWRITEBYTECODE': '1',
            'HOME': str(self.root),
            'FAKE_API_REQUESTS': str(self.root / 'api-requests.log'),
            'FAKE_API_RESPONSES': str(self.root / 'api-responses.json'),
        }
        self.set_api_responses({})

    def set_api_responses(self, responses):
        Path(self.base_env['FAKE_API_RESPONSES']).write_text(
            json.dumps(responses), encoding='utf-8')
        Path(self.base_env['FAKE_API_REQUESTS']).unlink(missing_ok=True)

    def api_requests(self):
        path = Path(self.base_env['FAKE_API_REQUESTS'])
        if not path.exists():
            return []
        return [json.loads(line) for line in
                path.read_text(encoding='utf-8').splitlines()]

    def step(self, job, key):
        for step in self.workflow['jobs'][job]['steps']:
            if key in (step.get('id'), step.get('name')):
                return step
        raise AssertionError('No step {!r} in job {!r}'.format(key, job))

    def run_step(self, step, context, workdir):
        """Run one `run:` step as the hosted runner would."""
        self.runs += 1
        workdir.mkdir(exist_ok=True)
        output = self.root / 'github-output-{}'.format(self.runs)
        script = self.root / 'step-{}.sh'.format(self.runs)
        script.write_text(render(step['run'], context), encoding='utf-8')
        env = dict(self.base_env, GITHUB_OUTPUT=str(output))
        env.update({
            name: render(value, context)
            for name, value in step.get('env', {}).items()
        })
        result = subprocess.run(
            ['bash', '--noprofile', '--norc', '-e', str(script)],
            cwd=workdir,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        return result, read_outputs(output)

    def write_location(self, location):
        """Run a matrix job's temp-file step in a fresh checkout."""
        self.runs += 1
        workdir = self.root / 'write-locations-{}'.format(self.runs)
        result, _ = self.run_step(
            self.step('write-locations', 'Write location to temp file'),
            {'matrix.location': location},
            workdir,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return workdir / 'location.txt'

    def converge(self, location_file):
        """Hand the temp file to the real writer with Thoth mocked."""
        thoth = MagicMock()
        thoth.client.execute.side_effect = lambda query, variables: json.dumps({
            'data': {'publication': {
                'publicationId': variables['publicationId'],
                'locations': [],
            }},
        })
        thoth.create_location.return_value = 'created-location'
        with patch('write_locations.configure_thoth_client',
                   return_value=thoth), \
                redirect_stdout(io.StringIO()), \
                self.assertLogs(level='INFO'):
            status = write_locations.main([str(location_file)])
        self.assertEqual(status, 0)
        thoth.update_location.assert_not_called()
        return [call.args[0] for call in thoth.create_location.call_args_list]

    def assert_writer_step_unchanged(self):
        step = self.step('write-locations', 'Write location to Thoth using Python script')
        self.assertEqual(step['run'], 'python write_locations.py location.txt')
        self.assertEqual(step['env'], {'THOTH_PAT': '${{ secrets.THOTH_PAT }}'})


class TestCatchupWorkflowInterpolation(unittest.TestCase):

    def test_run_scripts_contain_no_expressions(self):
        for path in (OAPEN_WORKFLOW, MUSE_WORKFLOW):
            for job_name, job in load_yaml(path)['jobs'].items():
                for step in job.get('steps', []):
                    if 'run' not in step:
                        continue
                    with self.subTest(workflow=path.name, job=job_name,
                                      step=step.get('name')):
                        self.assertNotIn('${{', step['run'])


class TestOapenCatchupLocationsWorkflow(WorkflowStepTestCase):
    workflow_path = OAPEN_WORKFLOW

    def context(self, new_ids='', max_locations=''):
        return {
            'vars.OAPEN_ENV_PUBLISHERS': json.dumps([PUBLISHER_ID]),
            'vars.OAPEN_ENV_EXCEPTIONS': '',
            'steps.get-ids.outputs.NEW_IDS': new_ids,
            'inputs.max_locations': max_locations,
        }

    def select(self, works):
        """Run the real selection stage against a mocked Thoth read client."""
        thoth = MagicMock()
        thoth.bookIds.return_value = [
            SimpleNamespace(workId=work.workId) for work in works]
        by_id = {work.workId: work for work in works}
        thoth.work_by_id.side_effect = lambda work_id: by_id[work_id]
        stdout = io.StringIO()
        with patch.dict(os.environ, {
                'ENV_PUBLISHERS': json.dumps([PUBLISHER_ID]),
                'ENV_EXCEPTIONS': ''}), \
                redirect_stdout(stdout), \
                self.assertLogs(level='INFO'):
            status = obtain_new_ids.main(
                ['--platform', 'OAPEN', '--locations'], thoth=thoth)
        self.assertEqual(status, 0)
        return stdout.getvalue()

    def run_get_ids(self, selection_stdout):
        """Run the real selection step, replaying the producer's stdout."""
        workdir = self.root / 'obtain-locations'
        workdir.mkdir(exist_ok=True)
        (workdir / 'selection-stdout.txt').write_text(
            selection_stdout, encoding='utf-8')
        (workdir / 'obtain_new_ids.py').write_text(
            "import sys\n"
            "sys.stdout.write(open('selection-stdout.txt').read())\n",
            encoding='utf-8')
        return self.run_step(
            self.step('obtain-locations', 'get-ids'), self.context(), workdir)

    def run_get_locations(self, new_ids, max_locations=''):
        workdir = self.root / 'obtain-locations'
        workdir.mkdir(exist_ok=True)
        script = workdir / 'obtain_oapen_locations.py'
        if not script.exists():
            script.symlink_to(ROOT / 'obtain_oapen_locations.py')
        return self.run_step(
            self.step('obtain-locations', 'get-locations'),
            self.context(new_ids, max_locations),
            workdir,
        )

    def test_untrusted_values_are_passed_through_env(self):
        self.assertEqual(
            self.step('obtain-locations', 'get-locations')['env'], {
                'NEW_IDS': '${{ steps.get-ids.outputs.NEW_IDS }}',
                'MAX_LOCATIONS': '${{ inputs.max_locations }}',
            })
        self.assertEqual(
            self.step('write-locations', 'Write location to temp file')['env'],
            {'LOCATION': '${{ matrix.location }}'})
        self.assert_writer_step_unchanged()

    def test_schedule_and_max_locations_input_are_unchanged(self):
        self.assertEqual(
            self.workflow['on']['schedule'], [{'cron': '20 2 * * 2'}])
        max_locations = (
            self.workflow['on']['workflow_dispatch']['inputs']['max_locations'])
        self.assertEqual(max_locations['default'], '250')
        self.assertEqual(max_locations['type'], 'number')

    def test_writes_only_follow_a_successful_lookup(self):
        job = self.workflow['jobs']['write-locations']
        self.assertEqual(job['needs'], 'obtain-locations')
        # No status function: the implicit success() skips every write when
        # the lookup job fails.
        self.assertEqual(
            job['if'], "needs.obtain-locations.outputs.NEW_LOCATIONS != '[]'")
        self.assertEqual(
            job['strategy']['matrix']['location'],
            '${{ fromJSON(needs.obtain-locations.outputs.NEW_LOCATIONS) }}')

    def test_selection_output_reaches_lookup_matrix_and_writer_unchanged(self):
        works = [
            thoth_work('work-1', SICI_DOI, PUBLICATION_1),
            thoth_work('work-2', SHELL_DOI, PUBLICATION_2, ['OAPEN']),
            thoth_work('work-3', PLAIN_DOI, PUBLICATION_3, ['DOAB']),
        ]
        selection = self.select(works)
        self.assertEqual(json.loads(selection), [
            [PUBLICATION_1, SICI_DOI, ['OAPEN', 'DOAB']],
            [PUBLICATION_2, SHELL_DOI, ['DOAB']],
            [PUBLICATION_3, PLAIN_DOI, ['OAPEN']],
        ])

        result, outputs = self.run_get_ids(selection)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(outputs['NEW_IDS'], selection.strip())

        self.set_api_responses({
            oapen_url(SICI_DOI): [
                200, oapen_body('20.500.12657/1', HOSTILE_FILE_NAME)],
            doab_url(SICI_DOI): [200, [{'handle': '20.500.12854/1'}]],
            doab_url(SHELL_DOI): [200, [{'handle': '20.500.12854/2'}]],
            oapen_url(PLAIN_DOI): [
                200, oapen_body('20.500.12657/3', 'third.pdf')],
        })
        result, outputs = self.run_get_locations(outputs['NEW_IDS'])
        self.assertEqual(result.returncode, 0, result.stderr)
        # The lookup received every DOI intact and in selection order.
        self.assertEqual(self.api_requests(), [
            oapen_url(SICI_DOI),
            doab_url(SICI_DOI),
            doab_url(SHELL_DOI),
            oapen_url(PLAIN_DOI),
        ])

        expected = [
            (PUBLICATION_1, 'OAPEN',
             *oapen_urls('20.500.12657/1', HOSTILE_FILE_NAME)),
            (PUBLICATION_1, 'DOAB', doab_landing_page('20.500.12854/1'), None),
            (PUBLICATION_2, 'DOAB', doab_landing_page('20.500.12854/2'), None),
            (PUBLICATION_3, 'OAPEN', *oapen_urls('20.500.12657/3', 'third.pdf')),
        ]
        matrix = json.loads(outputs['NEW_LOCATIONS'])
        self.assertEqual(
            matrix, [location_line(*location) for location in expected])

        for location, fields in zip(matrix, expected):
            with self.subTest(location=location):
                location_file = self.write_location(location)
                self.assertEqual(
                    location_file.read_text(encoding='utf-8'), location + '\n')
                self.assertEqual(
                    self.converge(location_file), [created_location(*fields)])
        self.assertEqual(list(self.root.rglob('pwned')), [])

    def test_two_element_records_remain_supported(self):
        self.set_api_responses({
            oapen_url(PLAIN_DOI): [200, oapen_body('20.500.12657/3', 'a.pdf')],
            doab_url(PLAIN_DOI): [200, [{'handle': '20.500.12854/3'}]],
        })

        result, outputs = self.run_get_locations(
            compact([[PUBLICATION_3, PLAIN_DOI]]))

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.api_requests(), [oapen_url(PLAIN_DOI), doab_url(PLAIN_DOI)])
        self.assertEqual(json.loads(outputs['NEW_LOCATIONS']), [
            location_line(PUBLICATION_3, 'OAPEN',
                          *oapen_urls('20.500.12657/3', 'a.pdf')),
            location_line(PUBLICATION_3, 'DOAB',
                          doab_landing_page('20.500.12854/3'), None),
        ])

    def test_empty_selection_yields_empty_matrix_without_requests(self):
        selection = self.select([
            thoth_work('work-1', PLAIN_DOI, PUBLICATION_1, ['OAPEN', 'DOAB'])])
        self.assertEqual(selection, '[]\n')

        result, outputs = self.run_get_ids(selection)
        self.assertEqual(result.returncode, 0, result.stderr)
        result, outputs = self.run_get_locations(outputs['NEW_IDS'])

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(outputs['NEW_LOCATIONS'], '[]')
        self.assertEqual(self.api_requests(), [])

    def test_malformed_ids_fail_before_requests_or_outputs(self):
        for new_ids in (
                '[[{},{},[OAPEN,DOAB]]]'.format(PUBLICATION_1, PLAIN_DOI),
                ''):
            with self.subTest(new_ids=new_ids):
                result, outputs = self.run_get_locations(new_ids)

                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Invalid input', result.stderr)
                self.assertNotIn('NEW_LOCATIONS', outputs)
                self.assertEqual(self.api_requests(), [])

    def test_api_failure_fails_step_without_matrix_output(self):
        self.set_api_responses({
            oapen_url(PLAIN_DOI): [500, None],
            doab_url(PLAIN_DOI): [500, None],
        })

        result, outputs = self.run_get_locations(
            compact([[PUBLICATION_3, PLAIN_DOI, ['OAPEN', 'DOAB']]]))

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn('NEW_LOCATIONS', outputs)

    def test_max_locations_caps_matrix_in_order(self):
        records, responses, every_location = [], {}, []
        for index in range(130):
            publication_id = str(UUID(int=index + 1))
            doi = '10.5555/cap.{}'.format(index)
            oapen_handle = '20.500.12657/{}'.format(index)
            doab_handle = '20.500.12854/{}'.format(index)
            records.append([publication_id, doi, ['OAPEN', 'DOAB']])
            responses[oapen_url(doi)] = [200, oapen_body(oapen_handle, 'f.pdf')]
            responses[doab_url(doi)] = [200, [{'handle': doab_handle}]]
            every_location += [
                location_line(publication_id, 'OAPEN',
                              *oapen_urls(oapen_handle, 'f.pdf')),
                location_line(publication_id, 'DOAB',
                              doab_landing_page(doab_handle), None),
            ]
        self.set_api_responses(responses)

        for max_locations, expected_count in (
                ('', 250), ('1', 1), ('256', 256), ('300', 256)):
            with self.subTest(max_locations=max_locations):
                result, outputs = self.run_get_locations(
                    compact(records), max_locations)

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    json.loads(outputs['NEW_LOCATIONS']),
                    every_location[:expected_count])

        for max_locations in ('0', 'abc'):
            with self.subTest(max_locations=max_locations):
                result, outputs = self.run_get_locations(
                    compact(records), max_locations)

                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('NEW_LOCATIONS', outputs)

    def test_harness_reproduces_the_legacy_inline_interpolation(self):
        """These tests only prove the fix if the emulation exposes the bug."""
        workdir = self.root / 'legacy'
        new_ids = compact([[PUBLICATION_1, PLAIN_DOI, ['OAPEN', 'DOAB']]])
        location = location_line(
            PUBLICATION_1, 'OAPEN',
            *oapen_urls('20.500.12657/1', HOSTILE_FILE_NAME))

        self.run_step(
            {'run': 'echo "${{ steps.get-ids.outputs.NEW_IDS }}" > ids.txt'},
            {'steps.get-ids.outputs.NEW_IDS': new_ids},
            workdir,
        )
        self.run_step(
            {'run': 'echo "${{ matrix.location }}" >> location.txt'},
            {'matrix.location': location},
            workdir,
        )

        self.assertEqual(
            (workdir / 'ids.txt').read_text(encoding='utf-8'),
            '[[{},{},[OAPEN,DOAB]]]\n'.format(PUBLICATION_1, PLAIN_DOI))
        self.assertNotEqual(
            (workdir / 'location.txt').read_text(encoding='utf-8'),
            location + '\n')
        self.assertTrue((workdir / 'pwned').exists())


class TestMuseCatchupLocationsWorkflow(WorkflowStepTestCase):
    workflow_path = MUSE_WORKFLOW

    def test_untrusted_values_are_passed_through_env(self):
        self.assertEqual(
            self.step('write-locations', 'Write location to temp file')['env'],
            {'LOCATION': '${{ matrix.location }}'})
        self.assert_writer_step_unchanged()

    def test_write_gating_is_unchanged(self):
        job = self.workflow['jobs']['write-locations']
        self.assertEqual(job['needs'], 'obtain-locations')
        self.assertEqual(
            job['if'],
            "${{ always() && needs.obtain-locations.outputs.NEW_LOCATIONS != '[]' }}")

    def test_location_with_shell_metacharacters_reaches_writer_unchanged(self):
        landing_page = (
            'https://muse.jhu.edu/pub/1/oa_monograph/book/1'
            '$(touch${IFS}pwned)`touch${IFS}pwned`"dq"\'sq\';&|')
        full_text_url = landing_page + '/pdf/download'
        location = location_line(
            PUBLICATION_1, 'PROJECT_MUSE', landing_page, full_text_url)

        location_file = self.write_location(location)

        self.assertEqual(
            location_file.read_text(encoding='utf-8'), location + '\n')
        self.assertEqual(self.converge(location_file), [created_location(
            PUBLICATION_1, 'PROJECT_MUSE', landing_page, full_text_url)])
        self.assertEqual(list(self.root.rglob('pwned')), [])


if __name__ == '__main__':
    unittest.main()
