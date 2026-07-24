import unittest

from thothlibrary import ThothClient

from thothapi import patch_thoth_client_queries


class TestPatchThothClientQueries(unittest.TestCase):
    def setUp(self):
        patch_thoth_client_queries()

    def _fields(self, query_name):
        return ThothClient.QUERIES[query_name]['fields']

    def _field_starting(self, query_name, prefix):
        return next(
            field for field in self._fields(query_name)
            if field.lstrip().startswith(prefix)
        )

    def test_titles_and_abstracts_request_plain_text(self):
        for query_name in ('work', 'works'):
            for prefix in ('titles(', 'abstracts('):
                field = self._field_starting(query_name, prefix)
                self.assertIn('markupFormat: PLAIN_TEXT', field)
                self.assertNotIn('markupFormat: JATS_XML', field)

    def test_unrelated_selections_keep_jats_xml(self):
        # Selections we do not consume as the work title/abstract must be
        # left untouched.
        awards = self._field_starting('work', 'awards')
        self.assertIn('markupFormat: JATS_XML', awards)

    def test_featured_videos_selection_removed(self):
        for field in self._fields('work'):
            self.assertFalse(field.lstrip().startswith('workFeaturedVideos '))

    def test_patch_is_idempotent(self):
        patch_thoth_client_queries()
        titles = self._field_starting('work', 'titles(')
        self.assertIn('markupFormat: PLAIN_TEXT', titles)
        self.assertEqual(titles.count('markupFormat'), 1)


if __name__ == '__main__':
    unittest.main()
