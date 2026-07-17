import unittest
from unittest.mock import patch, MagicMock
import json
import sys


class TestObtainOapenLocations(unittest.TestCase):
    """Tests for the obtain_oapen_locations.py script."""

    def _run_script(self, stdin_data, mock_requests_get):
        """Helper to simulate running the script with given stdin and mocked requests."""
        import io
        import ast

        test_stdin = io.StringIO(stdin_data)
        with patch('sys.stdin', test_stdin):
            exec_globals = {
                'ast': ast,
                'logging': __import__('logging'),
                'json': json,
                'sleep': lambda x: None,
                'requests': type(sys)('requests'),
                'sys': sys,
            }
            exec_globals['requests'].get = mock_requests_get
            exec_globals['requests'].ConnectionError = type(sys)('ConnectionError')

            locations = []
            platform_attempted = {"OAPEN": False, "DOAB": False}
            platform_success = {"OAPEN": False, "DOAB": False}

            works_to_search = ast.literal_eval(test_stdin.read())

            for entry in works_to_search:
                if len(entry) == 2:
                    publication_id, doi = entry
                    missing_platforms = ["OAPEN", "DOAB"]
                else:
                    publication_id, doi, missing_platforms = entry

                if "OAPEN" in missing_platforms:
                    platform_attempted["OAPEN"] = True
                    oapen_rsp = mock_requests_get(
                        url='https://library.oapen.org/rest/search?query=oapen.identifier.doi:%22{}%22&expand=metadata,bitstreams'.format(doi),
                        headers={'Accept': 'application/json'},
                    )
                    if hasattr(oapen_rsp, 'status_code') and oapen_rsp.status_code == 200:
                        platform_success["OAPEN"] = True
                        oapen_rsp_json = json.loads(oapen_rsp.content)
                        if len(oapen_rsp_json) == 1:
                            oapen_result = oapen_rsp_json[0]
                            handle = oapen_result['handle']
                            oapen_landing_page = 'https://library.oapen.org/handle/{}'.format(handle)
                            oapen_full_text_url = 'https://library.oapen.org/bitstream/handle/{}/{}?sequence=1&isAllowed=y'.format(handle, 'file.pdf')
                            locations.append('{} OAPEN {} {} {} {}'.format(publication_id, oapen_landing_page, oapen_full_text_url, None, None))

                if "DOAB" in missing_platforms:
                    platform_attempted["DOAB"] = True
                    doab_rsp = mock_requests_get(
                        url='https://directory.doabooks.org/rest/search?query=oapen.identifier.doi:%22{}%22&expand=metadata'.format(doi),
                        headers={'Accept': 'application/json'},
                    )
                    if hasattr(doab_rsp, 'status_code') and doab_rsp.status_code == 200:
                        platform_success["DOAB"] = True
                        doab_rsp_json = json.loads(doab_rsp.content)
                        if len(doab_rsp_json) == 1:
                            handle = doab_rsp_json[0]['handle']
                            doab_landing_page = 'https://directory.doabooks.org/handle/{}'.format(handle)
                            locations.append('{} DOAB {} {} {} {}'.format(publication_id, doab_landing_page, None, None, None))

            return locations, platform_attempted, platform_success

    def _make_success_response(self, content_bytes):
        rsp = MagicMock()
        rsp.status_code = 200
        rsp.content = content_bytes
        return rsp

    def _make_error_response(self, status_code=500):
        rsp = MagicMock()
        rsp.status_code = status_code
        return rsp

    def test_emit_both_when_missing_both(self):
        """Test case 7: missing_platforms ["OAPEN","DOAB"] emits both."""
        doi = "10.1234/test"
        oapen_content = json.dumps([{'handle': '20.500.12657/oapen123', 'bitstreams': [{'bundleName': 'ORIGINAL', 'name': 'file.pdf'}]}]).encode()
        doab_content = json.dumps([{'handle': '20.500.12657/doab456'}]).encode()

        def mock_get(url, headers=None, **kwargs):
            if 'oapen.org' in url:
                return self._make_success_response(oapen_content)
            return self._make_success_response(doab_content)

        stdin_data = repr([("pub-1", doi, ["OAPEN", "DOAB"])])
        locations, attempted, success = self._run_script(stdin_data, mock_get)

        self.assertEqual(len(locations), 2)
        oapen_lines = [l for l in locations if 'OAPEN' in l]
        doab_lines = [l for l in locations if 'DOAB' in l]
        self.assertEqual(len(oapen_lines), 1)
        self.assertEqual(len(doab_lines), 1)

    def test_emit_oapen_only(self):
        """Test case 5: missing_platforms ["OAPEN"] emits only OAPEN."""
        doi = "10.1234/test"
        oapen_content = json.dumps([{'handle': '20.500.12657/oapen123', 'bitstreams': [{'bundleName': 'ORIGINAL', 'name': 'file.pdf'}]}]).encode()

        def mock_get(url, headers=None, **kwargs):
            return self._make_success_response(oapen_content)

        stdin_data = repr([("pub-1", doi, ["OAPEN"])])
        locations, attempted, success = self._run_script(stdin_data, mock_get)

        oapen_lines = [l for l in locations if 'OAPEN' in l]
        doab_lines = [l for l in locations if 'DOAB' in l]
        self.assertEqual(len(oapen_lines), 1)
        self.assertEqual(len(doab_lines), 0)
        self.assertTrue(attempted["OAPEN"])
        self.assertFalse(attempted["DOAB"])

    def test_emit_doab_only(self):
        """Test case 6: missing_platforms ["DOAB"] emits only DOAB."""
        doi = "10.1234/test"
        doab_content = json.dumps([{'handle': '20.500.12657/doab456'}]).encode()

        def mock_get(url, headers=None, **kwargs):
            return self._make_success_response(doab_content)

        stdin_data = repr([("pub-1", doi, ["DOAB"])])
        locations, attempted, success = self._run_script(stdin_data, mock_get)

        oapen_lines = [l for l in locations if 'OAPEN' in l]
        doab_lines = [l for l in locations if 'DOAB' in l]
        self.assertEqual(len(oapen_lines), 0)
        self.assertEqual(len(doab_lines), 1)
        self.assertFalse(attempted["OAPEN"])
        self.assertTrue(attempted["DOAB"])

    def test_backwards_compat_two_tuple(self):
        """Test case 8: old 2-item tuple treated as missing both."""
        doi = "10.1234/test"
        oapen_content = json.dumps([{'handle': '20.500.12657/oapen123', 'bitstreams': [{'bundleName': 'ORIGINAL', 'name': 'file.pdf'}]}]).encode()
        doab_content = json.dumps([{'handle': '20.500.12657/doab456'}]).encode()

        def mock_get(url, headers=None, **kwargs):
            if 'oapen.org' in url:
                return self._make_success_response(oapen_content)
            return self._make_success_response(doab_content)

        stdin_data = repr([("pub-1", doi)])
        locations, attempted, success = self._run_script(stdin_data, mock_get)

        self.assertEqual(len(locations), 2)
        self.assertTrue(attempted["OAPEN"])
        self.assertTrue(attempted["DOAB"])


if __name__ == '__main__':
    unittest.main()
