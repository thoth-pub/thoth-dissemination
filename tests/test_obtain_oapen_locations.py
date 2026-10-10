import io
import json
import runpy
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'obtain_oapen_locations.py'

PUBLICATION_ID = '9a8b7c6d-1111-4222-8333-444455556666'
PUBLICATION_ID_2 = '0f1e2d3c-5555-4666-8777-888899990000'
DOI = '10.1002/(SICI)1097-4636(199706)35:4<425::AID-JBM3>3.0.CO;2-K'
DOI_2 = '10.11647/obp.0001'
OAPEN_HANDLE = '20.500.12657/12345'
DOAB_HANDLE = '20.500.12854/67890'
OAPEN_FILE = 'example.pdf'
USER_AGENT = 'Thoth-Dissemination/1.0 (+https://thoth.pub)'
EXPECTED_HEADERS = {'Accept': 'application/json', 'User-Agent': USER_AGENT}


def oapen_url(doi):
    return ('https://library.oapen.org/rest/search?query='
            'oapen.identifier.doi:%22{}%22&expand=metadata,bitstreams'
            .format(doi))


def doab_url(doi):
    return ('https://directory.doabooks.org/rest/search?query='
            'oapen.identifier.doi:%22{}%22&expand=metadata'.format(doi))


def oapen_location(publication_id, handle=OAPEN_HANDLE, file_name=OAPEN_FILE):
    return ('{} OAPEN https://library.oapen.org/handle/{} '
            'https://library.oapen.org/bitstream/handle/{}/{}'
            '?sequence=1&isAllowed=y None None'
            .format(publication_id, handle, handle, file_name))


def doab_location(publication_id, handle=DOAB_HANDLE):
    return '{} DOAB https://directory.doabooks.org/handle/{} None None None'.format(
        publication_id, handle)


class FakeResponse:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self.content = b'' if body is None else json.dumps(body).encode()


def oapen_result(handle=OAPEN_HANDLE, file_name=OAPEN_FILE):
    return FakeResponse(200, [{
        'handle': handle,
        'bitstreams': [
            {'bundleName': 'THUMBNAIL', 'name': 'thumbnail.jpg'},
            {'bundleName': 'ORIGINAL', 'name': file_name},
        ],
    }])


def doab_result(handle=DOAB_HANDLE):
    return FakeResponse(200, [{'handle': handle}])


def compact(records):
    """Serialise records exactly as obtain_new_ids.py emits them."""
    return json.dumps(records, separators=(',', ':'))


class TestObtainOapenLocations(unittest.TestCase):
    """Run the real script as __main__ with only the HTTP layer faked."""

    def run_script(self, stdin_text, responses=None):
        responses = responses or {}
        requested = []
        self.sent_headers = []

        def fake_get(url, headers=None, **kwargs):
            requested.append(url)
            self.sent_headers.append((url, headers))
            self.assertEqual(headers, EXPECTED_HEADERS)
            if url not in responses:
                raise AssertionError('Unexpected API request: {}'.format(url))
            return responses[url]

        stdout = io.StringIO()
        with patch('sys.stdin', io.StringIO(stdin_text)), \
                patch('sys.stdout', stdout), \
                patch('requests.get', side_effect=fake_get), \
                patch('time.sleep'), \
                self.assertLogs(level='INFO') as logs:
            with self.assertRaises(SystemExit) as exit_:
                runpy.run_path(str(SCRIPT), run_name='__main__')
        return exit_.exception.code, stdout.getvalue(), requested, logs.output

    def test_producer_json_keeps_identifiers_and_queries_both_platforms(self):
        status, stdout, requested, _ = self.run_script(
            compact([[PUBLICATION_ID, DOI, ['OAPEN', 'DOAB']]]) + '\n',
            {oapen_url(DOI): oapen_result(), doab_url(DOI): doab_result()},
        )

        self.assertEqual(status, 0)
        self.assertEqual(requested, [oapen_url(DOI), doab_url(DOI)])
        self.assertEqual(json.loads(stdout), [
            oapen_location(PUBLICATION_ID),
            doab_location(PUBLICATION_ID),
        ])

    def test_oapen_and_doab_requests_send_dedicated_user_agent(self):
        self.run_script(
            compact([[PUBLICATION_ID, DOI, ['OAPEN', 'DOAB']]]),
            {oapen_url(DOI): oapen_result(), doab_url(DOI): doab_result()},
        )

        self.assertEqual(self.sent_headers, [
            (oapen_url(DOI), EXPECTED_HEADERS),
            (doab_url(DOI), EXPECTED_HEADERS),
        ])

    def test_oapen_only_record_does_not_query_doab(self):
        status, stdout, requested, _ = self.run_script(
            compact([[PUBLICATION_ID, DOI, ['OAPEN']]]),
            {oapen_url(DOI): oapen_result()},
        )

        self.assertEqual(status, 0)
        self.assertEqual(requested, [oapen_url(DOI)])
        self.assertEqual(json.loads(stdout), [oapen_location(PUBLICATION_ID)])

    def test_doab_only_record_does_not_query_oapen(self):
        status, stdout, requested, _ = self.run_script(
            compact([[PUBLICATION_ID, DOI, ['DOAB']]]),
            {doab_url(DOI): doab_result()},
        )

        self.assertEqual(status, 0)
        self.assertEqual(requested, [doab_url(DOI)])
        self.assertEqual(json.loads(stdout), [doab_location(PUBLICATION_ID)])

    def test_two_element_record_is_treated_as_missing_both(self):
        status, stdout, requested, _ = self.run_script(
            compact([[PUBLICATION_ID, DOI]]),
            {oapen_url(DOI): oapen_result(), doab_url(DOI): doab_result()},
        )

        self.assertEqual(status, 0)
        self.assertEqual(requested, [oapen_url(DOI), doab_url(DOI)])
        self.assertEqual(json.loads(stdout), [
            oapen_location(PUBLICATION_ID),
            doab_location(PUBLICATION_ID),
        ])

    def test_two_and_three_element_records_mix_in_input_order(self):
        status, stdout, requested, _ = self.run_script(
            compact([
                [PUBLICATION_ID, DOI, ['DOAB']],
                [PUBLICATION_ID_2, DOI_2],
            ]),
            {
                doab_url(DOI): doab_result(),
                oapen_url(DOI_2): oapen_result('20.500.12657/2', 'second.pdf'),
                doab_url(DOI_2): doab_result('20.500.12854/2'),
            },
        )

        self.assertEqual(status, 0)
        self.assertEqual(
            requested, [doab_url(DOI), oapen_url(DOI_2), doab_url(DOI_2)])
        self.assertEqual(json.loads(stdout), [
            doab_location(PUBLICATION_ID),
            oapen_location(PUBLICATION_ID_2, '20.500.12657/2', 'second.pdf'),
            doab_location(PUBLICATION_ID_2, '20.500.12854/2'),
        ])

    def test_empty_array_emits_empty_array_without_requests(self):
        status, stdout, requested, _ = self.run_script('[]\n')

        self.assertEqual(status, 0)
        self.assertEqual(requested, [])
        self.assertEqual(json.loads(stdout), [])

    def test_unmatched_and_ambiguous_results_emit_no_location(self):
        status, stdout, requested, logs = self.run_script(
            compact([[PUBLICATION_ID, DOI, ['OAPEN', 'DOAB']]]),
            {
                oapen_url(DOI): FakeResponse(200, []),
                doab_url(DOI): FakeResponse(
                    200, [{'handle': 'a'}, {'handle': 'b'}]),
            },
        )

        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout), [])
        self.assertTrue(any(
            'More than one DOAB API result' in line for line in logs))

    def test_failed_api_platform_still_exits_non_zero(self):
        status, stdout, requested, logs = self.run_script(
            compact([[PUBLICATION_ID, DOI, ['OAPEN', 'DOAB']]]),
            {oapen_url(DOI): FakeResponse(500), doab_url(DOI): doab_result()},
        )

        self.assertEqual(status, 1)
        # Existing behaviour: an OAPEN failure skips the rest of that record.
        self.assertEqual(requested, [oapen_url(DOI)])
        self.assertEqual(json.loads(stdout), [])
        self.assertTrue(any(
            'All attempts to contact OAPEN API failed' in line
            for line in logs))

    def test_invalid_input_fails_before_any_api_request(self):
        invalid_inputs = {
            # What bash made of the JSON when the workflow echoed it inline.
            'shell-stripped JSON':
                '[[{},{},[OAPEN,DOAB]]]'.format(PUBLICATION_ID, DOI_2),
            'Python literal': repr([(PUBLICATION_ID, DOI_2, ['OAPEN'])]),
            'empty input': '',
            'object': '{}',
            'string': '"{}"'.format(PUBLICATION_ID),
            'record is not an array': compact(['ab']),
            'record too short': compact([[PUBLICATION_ID]]),
            'record too long': compact([[PUBLICATION_ID, DOI_2, ['OAPEN'], 'x']]),
            'platforms not an array': compact([[PUBLICATION_ID, DOI_2, 'OAPEN']]),
            'non-string platform': compact([[PUBLICATION_ID, DOI_2, [1]]]),
            'non-string publication ID': compact([[1, DOI_2]]),
            'null DOI': compact([[PUBLICATION_ID, None]]),
        }
        for description, stdin_text in invalid_inputs.items():
            with self.subTest(description):
                status, stdout, requested, logs = self.run_script(stdin_text)

                self.assertEqual(status, 1)
                self.assertEqual(stdout, '')
                self.assertEqual(requested, [])
                self.assertTrue(any(
                    line.startswith('ERROR:') and 'Invalid input' in line
                    for line in logs), logs)


if __name__ == '__main__':
    unittest.main()
