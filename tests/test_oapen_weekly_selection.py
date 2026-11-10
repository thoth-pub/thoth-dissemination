from contextlib import redirect_stdout
from datetime import datetime, UTC
from io import StringIO
import json
import os
import unittest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

import oapensworduploader
from obtain_new_ids import (
    OapenLocationsIDFinder,
    OapenWeeklyIDFinder,
    WeeklyIDFinder,
    get_arguments,
    get_id_finder,
    main,
)


PUBLISHER_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
OAPEN_WORK_TYPES = '[MONOGRAPH, EDITED_BOOK, TEXTBOOK]'
DEFAULT_WORK_TYPES = '[MONOGRAPH, EDITED_BOOK, JOURNAL_ISSUE, TEXTBOOK, BOOK_SET]'
OTHER_WEEKLY_PLATFORMS = ('EBSCOHost', 'JSTOR', 'ProjectMUSE', 'ProQuest')

# Tuesday: the previous week is Tuesday 2026-11-03 to Monday 2026-11-09.
NOW = datetime(2026, 11, 10, 2, 20, tzinfo=UTC)
AFTER_WINDOW_ID = '00000000-0000-4000-8000-000000000001'
WINDOW_END_ID = '00000000-0000-4000-8000-000000000002'
WINDOW_START_ID = '00000000-0000-4000-8000-000000000003'
BEFORE_WINDOW_ID = '00000000-0000-4000-8000-000000000004'
UNREACHED_ID = '00000000-0000-4000-8000-000000000005'
WORKS_BY_PUBLICATION_DATE_DESC = [
    SimpleNamespace(workId=AFTER_WINDOW_ID, publicationDate='2026-11-10'),
    SimpleNamespace(workId=WINDOW_END_ID, publicationDate='2026-11-09'),
    SimpleNamespace(workId=WINDOW_START_ID, publicationDate='2026-11-03'),
    SimpleNamespace(workId=BEFORE_WINDOW_ID, publicationDate='2026-11-02'),
    SimpleNamespace(workId=UNREACHED_ID, publicationDate='2026-11-01'),
]


class FixedDateTime(datetime):
    """datetime whose now() is fixed, so the weekly window is deterministic"""

    @classmethod
    def now(cls, tz=None):
        return NOW if tz is None else NOW.astimezone(tz)


def type_names(work_types):
    """Split a GraphQL enum list such as '[A, B]' into its names"""
    return [name.strip() for name in work_types.strip('[]').split(',')]


def works_page(**kwargs):
    """Mimic thoth.works(limit=1, offset=n) over the fixture list"""
    offset = kwargs['offset']
    return WORKS_BY_PUBLICATION_DATE_DESC[offset:offset + kwargs['limit']]


class OapenWeeklySelectionTestCase(unittest.TestCase):

    def setUp(self):
        self.thoth = MagicMock()
        self.thoth.publisher.return_value = SimpleNamespace(
            publisherId=PUBLISHER_ID)
        self.thoth.works.side_effect = works_page
        client = patch(
            'obtain_new_ids.get_thoth_client', return_value=self.thoth)
        client.start()
        self.addCleanup(client.stop)
        environment = patch.dict(os.environ, {
            'ENV_PUBLISHERS': json.dumps([PUBLISHER_ID]),
            'ENV_EXCEPTIONS': '',
            'PUBLISHER_SOURCE_MODES': '',
        })
        environment.start()
        self.addCleanup(environment.stop)
        clock = patch('obtain_new_ids.datetime', FixedDateTime)
        clock.start()
        self.addCleanup(clock.stop)

    def finder_for(self, *argv):
        return get_id_finder(get_arguments(list(argv)))


class TestOapenWeeklyFinderMapping(OapenWeeklySelectionTestCase):

    def test_oapen_maps_to_oapen_weekly_finder(self):
        finder = self.finder_for('--platform', 'OAPEN')
        self.assertIs(type(finder), OapenWeeklyIDFinder)
        self.assertIsInstance(finder, WeeklyIDFinder)

    def test_other_weekly_platforms_keep_plain_weekly_finder(self):
        for platform in OTHER_WEEKLY_PLATFORMS:
            with self.subTest(platform=platform):
                finder = self.finder_for('--platform', platform)
                self.assertIs(type(finder), WeeklyIDFinder)
                self.assertNotIsInstance(finder, OapenWeeklyIDFinder)
                finder.get_query_parameters()
                self.assertEqual(finder.work_types, DEFAULT_WORK_TYPES)

    def test_oapen_locations_keeps_locations_finder(self):
        finder = self.finder_for('--platform', 'OAPEN', '--locations')
        self.assertIs(type(finder), OapenLocationsIDFinder)
        self.assertNotIsInstance(finder, WeeklyIDFinder)


class TestOapenWeeklyQueryParameters(OapenWeeklySelectionTestCase):

    def test_oapen_work_types_are_monograph_edited_book_textbook(self):
        finder = self.finder_for('--platform', 'OAPEN')
        finder.get_query_parameters()
        self.assertEqual(finder.work_types, OAPEN_WORK_TYPES)
        self.assertEqual(
            type_names(finder.work_types),
            ['MONOGRAPH', 'EDITED_BOOK', 'TEXTBOOK'])
        self.assertNotIn('JOURNAL_ISSUE', finder.work_types)
        self.assertNotIn('BOOK_SET', finder.work_types)

    def test_oapen_statuses_and_order_match_plain_weekly_finder(self):
        oapen = self.finder_for('--platform', 'OAPEN')
        oapen.get_query_parameters()
        plain = WeeklyIDFinder(platform='OAPEN', thoth=self.thoth)
        plain.get_query_parameters()
        self.assertEqual(oapen.work_statuses, plain.work_statuses)
        self.assertEqual(oapen.work_statuses, '[ACTIVE]')
        self.assertEqual(oapen.order, plain.order)
        self.assertEqual(
            oapen.updated_at_with_relations, plain.updated_at_with_relations)

    def test_oapen_work_types_equal_uploader_work_types(self):
        finder = self.finder_for('--platform', 'OAPEN')
        finder.get_query_parameters()
        names = type_names(finder.work_types)
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(
            set(names), set(oapensworduploader.OAPEN_WORK_TYPES))


class TestOapenWeeklyThothQuery(OapenWeeklySelectionTestCase):

    def assert_oapen_queries(self):
        self.assertEqual(self.thoth.works.call_count, 4)
        for call in self.thoth.works.call_args_list:
            self.assertEqual(call.kwargs['work_types'], OAPEN_WORK_TYPES)
            self.assertEqual(call.kwargs['work_statuses'], '[ACTIVE]')
            self.assertEqual(
                call.kwargs['order'],
                '{field: PUBLICATION_DATE, direction: DESC}')
            self.assertEqual(
                call.kwargs['publishers'], json.dumps([PUBLISHER_ID]))

    def test_get_thoth_ids_sends_oapen_types_within_previous_week(self):
        finder = self.finder_for('--platform', 'OAPEN')
        finder.get_publishers()
        finder.get_query_parameters()
        finder.get_thoth_ids()
        self.assertEqual(finder.thoth_ids, [WINDOW_END_ID, WINDOW_START_ID])
        self.assert_oapen_queries()

    def test_main_prints_oapen_selection(self):
        output = StringIO()
        with redirect_stdout(output):
            status = main(['--platform', 'OAPEN'])
        self.assertEqual(status, 0)
        self.assertEqual(
            json.loads(output.getvalue()), [WINDOW_END_ID, WINDOW_START_ID])
        self.assert_oapen_queries()


if __name__ == '__main__':
    unittest.main()
