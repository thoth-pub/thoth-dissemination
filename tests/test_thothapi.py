import copy
import json
import unittest
from unittest.mock import MagicMock, patch

from thothlibrary import ThothClient
from thothlibrary.mutation import ThothMutation

import thothapi
import uploader as uploader_module
from iauploader import IAUploader
from reconcile_internet_archive import InternetArchiveReconciler
from uploader import Uploader


WORK_ID = '11111111-2222-3333-4444-555555555555'


def _work_json():
    return json.dumps({
        'data': {
            'work': {
                'workId': WORK_ID,
                'titles': [{
                    'fullTitle': 'A Plain Title',
                    'title': 'A Plain Title',
                    'subtitle': None,
                    'canonical': True,
                    'localeCode': 'en',
                }],
                'abstracts': [{
                    'content': 'A plain abstract.',
                    'abstractType': 'LONG',
                    'canonical': True,
                    'localeCode': 'en',
                }],
            }
        }
    })


class TestSharedMetadataFetchRequestsPlainText(unittest.TestCase):
    """The shared metadata fetch requests plain text through thothlibrary's
    public markup_format argument (no downstream query mutation)."""

    def test_get_thoth_metadata_requests_plain_text_and_processes_json(self):
        mock_client = MagicMock()
        mock_client.work_by_id.return_value = _work_json()
        up = Uploader.__new__(Uploader)
        up.work_id = WORK_ID

        with patch.object(uploader_module, 'get_thoth_client',
                          return_value=mock_client) as mock_get_client:
            metadata = up.get_thoth_metadata('https://api.example')

        mock_get_client.assert_called_once_with('https://api.example')
        mock_client.work_by_id.assert_called_once_with(
            work_id=WORK_ID, markup_format='PLAIN_TEXT', raw=True)

        # The returned JSON is processed normally (top-level keys backfilled).
        work = metadata['data']['work']
        self.assertEqual(work['fullTitle'], 'A Plain Title')
        self.assertEqual(work['longAbstract'], 'A plain abstract.')

    def test_reconciler_loads_metadata_as_plain_text(self):
        thoth = MagicMock()
        thoth.work_by_id.return_value = _work_json()
        reconciler = InternetArchiveReconciler.__new__(
            InternetArchiveReconciler)
        reconciler.thoth = thoth

        reconciler._load_work_metadata(WORK_ID)

        thoth.work_by_id.assert_called_once_with(
            work_id=WORK_ID, markup_format='PLAIN_TEXT', raw=True)


class TestGetThothClientDoesNotMutateQueries(unittest.TestCase):
    def test_queries_are_left_unchanged(self):
        before = copy.deepcopy(ThothClient.QUERIES)

        thothapi.get_thoth_client()

        self.assertEqual(ThothClient.QUERIES, before)

    def test_no_query_patch_function_exists(self):
        self.assertFalse(hasattr(thothapi, 'patch_thoth_client_queries'))

    def test_mutation_compatibility_patch_still_applied(self):
        self.assertTrue(hasattr(thothapi, 'patch_thoth_client_mutations'))
        thothapi.get_thoth_client()
        self.assertTrue(
            getattr(ThothMutation, '_thoth_dissemination_boolean_patch', False))


class TestNoLocalMarkupStripping(unittest.TestCase):
    """Guard against reintroducing the removed downstream markup handling."""

    def test_iauploader_has_no_markup_normalisation(self):
        self.assertFalse(
            hasattr(IAUploader, 'MARKUP_NORMALISED_METADATA_FIELDS'))
        self.assertFalse(hasattr(IAUploader, '_normalise_markup'))

    def test_swordv2_has_no_strip_tags(self):
        import swordv2uploader
        self.assertFalse(hasattr(swordv2uploader, 'STRIP_TAGS'))


if __name__ == '__main__':
    unittest.main()
