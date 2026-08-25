from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, UTC
from io import StringIO
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

from thothlibrary import errors

from internet_archive_policy import SUPPORTED_WORK_TYPES
from obtain_new_ids import (
    DEFAULT_IA_LOOKBACK_HOURS,
    DEFAULT_IA_MAX_IDS,
    IA_QUERY_PAGE_SIZE,
    IDFinder,
    InternetArchiveIDFinder,
    InternetArchiveSelectionError,
    MonthlyIDFinder,
    OapenLocationsIDFinder,
    WeeklyIDFinder,
    canonical_utc_timestamp,
    get_arguments,
    get_id_finder,
    lookback_hours_type,
    main,
    max_ids_type,
    parse_api_timestamp,
)
from publisher_source import (
    MODE_API,
    MODE_COMPARE,
    MODE_ENV,
    resolve_source_mode,
)
from thothapi import (
    INTERNET_ARCHIVE_SELECTION_QUERY,
    ThothGraphQLResponseError,
    get_internet_archive_selection_works,
)


NOW = datetime(2026, 7, 23, 4, 40, tzinfo=UTC)
PUBLISHER_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
WORK_ID = '11111111-1111-4111-8111-111111111111'


def selection_work(
        work_id=WORK_ID, updated_at='2026-07-23T04:00:00Z',
        status='ACTIVE', work_type='MONOGRAPH', publications=None):
    if publications is None:
        publications = [{
            'publicationType': 'PDF',
            'locations': [{
                'canonical': True,
                'fullTextUrl': 'https://example.test/book.pdf',
            }],
        }]
    return {
        'workId': work_id,
        'updatedAtWithRelations': updated_at,
        'workStatus': status,
        'workType': work_type,
        'publications': publications,
    }


def numbered_work(number, updated_at=None):
    work_id = '00000000-0000-4000-8000-{:012x}'.format(number)
    if updated_at is None:
        updated_at = (
            NOW - timedelta(hours=29) + timedelta(minutes=number)
        ).isoformat().replace('+00:00', 'Z')
    return selection_work(work_id=work_id, updated_at=updated_at)


class InternetArchiveFinderTestCase(unittest.TestCase):

    def setUp(self):
        self.thoth = MagicMock()
        self.thoth.publisher.return_value = SimpleNamespace(
            publisherId=PUBLISHER_ID)
        self.environment = patch.dict(os.environ, {
            'ENV_PUBLISHERS': json.dumps([PUBLISHER_ID]),
            'ENV_EXCEPTIONS': '',
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def finder(self, works=None, **kwargs):
        finder = InternetArchiveIDFinder(
            thoth=self.thoth,
            now_provider=lambda: NOW,
            **kwargs,
        )
        finder_patch = patch(
            'obtain_new_ids.get_internet_archive_selection_works',
            return_value=list(works or []),
        )
        query = finder_patch.start()
        self.addCleanup(finder_patch.stop)
        return finder, query


class TestInternetArchiveArgumentsAndMapping(
        InternetArchiveFinderTestCase):

    def test_internet_archive_maps_to_dedicated_finder(self):
        args = get_arguments(['--platform', 'InternetArchive'])
        self.assertIsInstance(
            get_id_finder(args, thoth=self.thoth),
            InternetArchiveIDFinder,
        )

    def test_other_monthly_platforms_stay_monthly(self):
        for platform in ('Figshare', 'Zenodo', 'CUL'):
            with self.subTest(platform=platform), patch(
                    'obtain_new_ids.get_thoth_client',
                    return_value=self.thoth):
                args = get_arguments(['--platform', platform])
                self.assertIsInstance(get_id_finder(args), MonthlyIDFinder)

    def test_weekly_platforms_stay_weekly(self):
        for platform in (
                'OAPEN', 'EBSCOHost', 'JSTOR', 'ProjectMUSE', 'ProQuest'):
            with self.subTest(platform=platform), patch(
                    'obtain_new_ids.get_thoth_client',
                    return_value=self.thoth):
                args = get_arguments(['--platform', platform])
                self.assertIsInstance(get_id_finder(args), WeeklyIDFinder)

    def test_default_lookback_and_limit(self):
        args = get_arguments(['--platform', 'InternetArchive'])
        self.assertEqual(args.lookback_hours, DEFAULT_IA_LOOKBACK_HOURS)
        self.assertEqual(args.max_ids, DEFAULT_IA_MAX_IDS)

    def test_maximum_lookback_is_accepted(self):
        self.assertEqual(lookback_hours_type('168'), 168)

    def test_invalid_lookbacks_are_rejected(self):
        for value in ('0', '-1', 'nan', 'inf', '-inf', '169', 'not-a-number'):
            with self.subTest(value=value), self.assertRaises(
                    Exception):
                lookback_hours_type(value)

    def test_fractional_positive_lookback_is_accepted(self):
        self.assertEqual(lookback_hours_type('1.5'), 1.5)

    def test_max_ids_bounds(self):
        self.assertEqual(max_ids_type('1'), 1)
        self.assertEqual(max_ids_type('200'), 200)
        for value in ('0', '201', '1.5', 'nan'):
            with self.subTest(value=value), self.assertRaises(Exception):
                max_ids_type(value)


class TestInternetArchiveConfiguration(InternetArchiveFinderTestCase):

    def test_publisher_ids_are_normalised_deduplicated_and_sorted(self):
        os.environ['ENV_PUBLISHERS'] = json.dumps([
            PUBLISHER_ID.upper(), PUBLISHER_ID])
        finder, _query = self.finder()
        finder.select()
        self.assertEqual(finder.publisher_ids, [PUBLISHER_ID])
        self.thoth.publisher.assert_called_once_with(
            publisher_id=PUBLISHER_ID)

    def test_empty_publisher_list_fails_safely(self):
        os.environ['ENV_PUBLISHERS'] = '[]'
        finder, _query = self.finder()
        with self.assertRaises(InternetArchiveSelectionError):
            finder.select()

    def test_malformed_publisher_uuid_fails(self):
        os.environ['ENV_PUBLISHERS'] = '["not-a-uuid"]'
        finder, _query = self.finder()
        with self.assertRaises(InternetArchiveSelectionError):
            finder.select()

    def test_non_array_publishers_fail(self):
        os.environ['ENV_PUBLISHERS'] = '"{}"'.format(PUBLISHER_ID)
        finder, _query = self.finder()
        with self.assertRaises(InternetArchiveSelectionError):
            finder.select()

    def test_unknown_publisher_fails(self):
        self.thoth.publisher.return_value = None
        finder, _query = self.finder()
        with self.assertRaises(InternetArchiveSelectionError):
            finder.select()

    def test_publisher_lookup_error_is_sanitised(self):
        self.thoth.publisher.side_effect = RuntimeError(
            'upstream response with irrelevant internals')
        finder, _query = self.finder()
        with self.assertRaisesRegex(
                InternetArchiveSelectionError,
                'could not be confirmed'):
            finder.select()

    def test_exceptions_are_normalised_and_deduplicated(self):
        os.environ['ENV_EXCEPTIONS'] = json.dumps([
            WORK_ID.upper(), WORK_ID])
        finder, _query = self.finder([selection_work()])
        report = finder.select()
        self.assertEqual(report['exception_ids'], [WORK_ID])
        self.assertEqual(
            report['excluded_counts'], {'configured_exception': 1})

    def test_invalid_exception_configuration_fails(self):
        for value in ('{}', '["bad"]', '[1]', 'null'):
            with self.subTest(value=value):
                os.environ['ENV_EXCEPTIONS'] = value
                finder, _query = self.finder()
                with self.assertRaises(InternetArchiveSelectionError):
                    finder.select()


class TestInternetArchiveWindowAndQuery(InternetArchiveFinderTestCase):

    def test_one_injected_now_is_used_for_entire_run(self):
        now_provider = MagicMock(return_value=NOW)
        finder = InternetArchiveIDFinder(
            thoth=self.thoth, now_provider=now_provider)
        with patch(
                'obtain_new_ids.get_internet_archive_selection_works',
                return_value=[selection_work()]):
            finder.select()
        now_provider.assert_called_once_with()

    def test_window_is_deterministic(self):
        finder, _query = self.finder([selection_work()])
        report = finder.select()
        self.assertEqual(report['window'], {
            'start': '2026-07-21T22:40:00Z',
            'end': '2026-07-23T04:40:00Z',
            'lookback_hours': 30,
        })
        self.assertEqual(report['generated_at'], report['window']['end'])

    def test_query_uses_relation_update_start_and_policy(self):
        finder, query = self.finder()
        finder.select()
        query.assert_called_once_with(
            self.thoth,
            [PUBLISHER_ID],
            SUPPORTED_WORK_TYPES,
            '2026-07-21T22:40:00Z',
            page_size=IA_QUERY_PAGE_SIZE,
        )

    def test_relation_only_update_is_included(self):
        work = selection_work()
        work['updatedAt'] = '2020-01-01T00:00:00Z'
        finder, _query = self.finder([work])
        self.assertEqual(finder.select()['selected_count'], 1)

    def test_update_equal_to_window_start_is_excluded(self):
        finder, _query = self.finder([
            selection_work(updated_at='2026-07-21T22:40:00Z')])
        report = finder.select()
        self.assertEqual(report['selected_count'], 0)
        self.assertEqual(report['excluded'][0]['reason'], 'outside_window')

    def test_update_before_window_start_is_excluded(self):
        finder, _query = self.finder([
            selection_work(updated_at='2026-07-21T22:39:59Z')])
        self.assertEqual(
            finder.select()['excluded'][0]['reason'], 'outside_window')

    def test_update_at_window_end_is_included(self):
        finder, _query = self.finder([
            selection_work(updated_at='2026-07-23T04:40:00Z')])
        self.assertEqual(finder.select()['selected_count'], 1)

    def test_update_after_window_end_is_reported(self):
        finder, _query = self.finder([
            selection_work(updated_at='2026-07-23T04:40:01Z')])
        report = finder.select()
        self.assertEqual(report['selected_count'], 0)
        self.assertEqual(report['excluded'][0]['reason'], 'after_window_end')

    def test_missing_timestamp_is_reported(self):
        finder, _query = self.finder([
            selection_work(updated_at=None)])
        self.assertEqual(
            finder.select()['excluded'][0]['reason'],
            'missing_update_timestamp',
        )

    def test_malformed_and_naive_timestamps_are_reported(self):
        finder, _query = self.finder([
            selection_work(
                work_id=WORK_ID, updated_at='not-a-timestamp'),
            selection_work(
                work_id='22222222-2222-4222-8222-222222222222',
                updated_at='2026-07-23T04:00:00'),
        ])
        report = finder.select()
        self.assertEqual(
            report['excluded_counts'], {'malformed_update_timestamp': 2})

    def test_utc_and_offset_timestamps_normalise(self):
        self.assertEqual(
            canonical_utc_timestamp(
                parse_api_timestamp('2026-07-23T05:00:00+01:00')),
            '2026-07-23T04:00:00Z',
        )
        self.assertEqual(
            canonical_utc_timestamp(
                parse_api_timestamp('2026-07-23T04:00:00Z')),
            '2026-07-23T04:00:00Z',
        )


class TestInternetArchiveEligibility(InternetArchiveFinderTestCase):

    def assert_excluded(self, work, reason):
        finder, _query = self.finder([work])
        report = finder.select()
        self.assertEqual(report['selected_count'], 0)
        self.assertEqual(report['excluded'][0]['reason'], reason)

    def test_active_supported_work_with_source_is_included(self):
        finder, _query = self.finder([selection_work()])
        self.assertEqual(finder.select()['selected_count'], 1)

    def test_inactive_work_is_excluded(self):
        self.assert_excluded(
            selection_work(status='FORTHCOMING'), 'inactive')

    def test_unsupported_work_type_is_excluded(self):
        self.assert_excluded(
            selection_work(work_type='BOOK_CHAPTER'),
            'unsupported_work_type',
        )

    def test_work_without_pdf_is_excluded(self):
        self.assert_excluded(
            selection_work(publications=[]), 'no_pdf_publication')

    def test_pdf_without_canonical_location_is_excluded(self):
        self.assert_excluded(selection_work(publications=[{
            'publicationType': 'PDF',
            'locations': [{
                'canonical': False,
                'fullTextUrl': 'https://example.test/book.pdf',
            }],
        }]), 'no_canonical_pdf_location')

    def test_canonical_pdf_without_url_is_excluded(self):
        for value in (None, '', '   '):
            with self.subTest(value=value):
                self.assert_excluded(selection_work(publications=[{
                    'publicationType': 'PDF',
                    'locations': [{
                        'canonical': True,
                        'fullTextUrl': value,
                    }],
                }]), 'canonical_pdf_location_missing_full_text_url')

    def test_source_eligibility_does_not_make_network_requests(self):
        finder, _query = self.finder([selection_work()])
        with patch('requests.get') as get, patch('requests.head') as head:
            finder.select()
        get.assert_not_called()
        head.assert_not_called()

    def test_exceptions_are_applied_before_capacity(self):
        exception_id = numbered_work(1)['workId']
        os.environ['ENV_EXCEPTIONS'] = json.dumps([exception_id])
        finder, _query = self.finder(
            [numbered_work(1), numbered_work(2)],
            max_ids=1,
        )
        report = finder.select()
        self.assertEqual(
            [entry['work_id'] for entry in report['selected']],
            [numbered_work(2)['workId']],
        )
        self.assertFalse(report['truncated'])


class TestInternetArchiveOrderingAndCap(InternetArchiveFinderTestCase):

    def test_duplicate_work_ids_retain_newest_valid_timestamp(self):
        finder, _query = self.finder([
            selection_work(updated_at='2026-07-23T01:00:00Z'),
            selection_work(updated_at='2026-07-23T03:00:00Z'),
        ])
        report = finder.select()
        self.assertEqual(report['eligible_count'], 1)
        self.assertEqual(
            report['selected'][0]['updated_at_with_relations'],
            '2026-07-23T03:00:00Z',
        )

    def test_oldest_updates_sort_first(self):
        finder, _query = self.finder([
            numbered_work(2, '2026-07-23T03:00:00Z'),
            numbered_work(1, '2026-07-23T01:00:00Z'),
        ])
        report = finder.select()
        self.assertEqual(
            [entry['work_id'] for entry in report['selected']],
            [numbered_work(1)['workId'], numbered_work(2)['workId']],
        )

    def test_equal_timestamps_sort_by_work_id(self):
        timestamp = '2026-07-23T03:00:00Z'
        finder, _query = self.finder([
            numbered_work(2, timestamp), numbered_work(1, timestamp)])
        report = finder.select()
        self.assertEqual(
            [entry['work_id'] for entry in report['selected']],
            sorted([numbered_work(2)['workId'], numbered_work(1)['workId']]),
        )

    def test_cap_selects_200_and_reports_every_overflow(self):
        works = [numbered_work(number) for number in range(1, 204)]
        finder, _query = self.finder(works)
        report = finder.select()
        self.assertEqual(report['selected_count'], 200)
        self.assertEqual(report['omitted_count'], 3)
        self.assertTrue(report['truncated'])
        self.assertEqual(len(report['omitted']), 3)
        self.assertEqual(
            report['selected'] + report['omitted'],
            sorted(
                report['selected'] + report['omitted'],
                key=lambda entry: (
                    entry['updated_at_with_relations'], entry['work_id']),
            ),
        )

    def test_custom_cap_is_applied_after_eligibility(self):
        works = [
            numbered_work(1),
            selection_work(
                work_id=numbered_work(2)['workId'],
                updated_at=numbered_work(2)['updatedAtWithRelations'],
                publications=[]),
            numbered_work(3),
        ]
        finder, _query = self.finder(works, max_ids=1)
        report = finder.select()
        self.assertEqual(report['eligible_count'], 2)
        self.assertEqual(report['selected_count'], 1)
        self.assertEqual(report['omitted_count'], 1)

    def test_selected_and_omitted_timestamps_are_canonical_utc(self):
        finder, _query = self.finder([
            numbered_work(1, '2026-07-23T03:00:00+01:00'),
            numbered_work(2, '2026-07-23T04:00:00+01:00'),
        ], max_ids=1)
        report = finder.select()
        self.assertEqual(
            report['selected'][0]['updated_at_with_relations'],
            '2026-07-23T02:00:00Z',
        )
        self.assertEqual(
            report['omitted'][0]['updated_at_with_relations'],
            '2026-07-23T03:00:00Z',
        )


class TestInternetArchiveOutputAndFailure(InternetArchiveFinderTestCase):

    def test_empty_selection_outputs_exact_json_array(self):
        finder, _query = self.finder([])
        stdout = StringIO()
        with redirect_stdout(stdout):
            finder.run()
        self.assertEqual(stdout.getvalue(), '[]\n')

    def test_selected_stdout_is_compact_json_and_logs_stay_off_stdout(self):
        finder, _query = self.finder([selection_work()])
        stdout = StringIO()
        stderr = StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            finder.run()
        self.assertEqual(json.loads(stdout.getvalue()), [WORK_ID])
        self.assertEqual(stdout.getvalue(), '["{}"]\n'.format(WORK_ID))

    def test_report_output_is_deterministic(self):
        first, _query = self.finder([selection_work()])
        second, _query = self.finder([selection_work()])
        self.assertEqual(first.select(), second.select())

    def test_report_file_is_sorted_json_with_trailing_newline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'selection.json'
            finder, _query = self.finder(
                [selection_work()], report_path=path)
            with redirect_stdout(StringIO()):
                finder.run()
            content = path.read_text(encoding='utf-8')
            self.assertTrue(content.endswith('\n'))
            self.assertEqual(json.loads(content), finder.report)

    def test_query_failure_returns_nonzero_and_writes_failure_report(self):
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / 'failure.json'
            with patch(
                    'obtain_new_ids.get_internet_archive_selection_works',
                    side_effect=RuntimeError('query unavailable')):
                status = main([
                    '--platform', 'InternetArchive',
                    '--report', str(report_path),
                ], now_provider=lambda: NOW, thoth=self.thoth)
            self.assertEqual(status, 1)
            report = json.loads(report_path.read_text(encoding='utf-8'))
            self.assertEqual(report['status'], 'failed')
            self.assertNotIn('environ', json.dumps(report).lower())

    def test_truncation_warns_to_stderr_but_outputs_valid_json(self):
        finder, _query = self.finder(
            [numbered_work(1), numbered_work(2)], max_ids=1)
        stdout = StringIO()
        with redirect_stdout(stdout), self.assertLogs(level='WARNING') as logs:
            finder.run()
        self.assertEqual(json.loads(stdout.getvalue()), [
            numbered_work(1)['workId']])
        self.assertIn('selection artifact', '\n'.join(logs.output))


class TestInternetArchiveGraphQLHelper(unittest.TestCase):

    def test_query_requests_only_selection_fields_and_relation_order(self):
        self.assertIn('updatedAtWithRelations', INTERNET_ARCHIVE_SELECTION_QUERY)
        self.assertIn('UPDATED_AT_WITH_RELATIONS', INTERNET_ARCHIVE_SELECTION_QUERY)
        self.assertIn('publications {', INTERNET_ARCHIVE_SELECTION_QUERY)
        self.assertIn('locations {', INTERNET_ARCHIVE_SELECTION_QUERY)
        self.assertNotIn('publicationDate', INTERNET_ARCHIVE_SELECTION_QUERY)

    def test_pagination_uses_bounded_pages_and_retrieves_all(self):
        first_page = [numbered_work(number) for number in range(100)]
        second_page = [numbered_work(100)]
        thoth = MagicMock()
        thoth.client.execute.side_effect = [
            {'data': {'works': first_page}},
            {'data': {'works': second_page}},
        ]
        works = get_internet_archive_selection_works(
            thoth, [PUBLISHER_ID], SUPPORTED_WORK_TYPES,
            '2026-07-21T22:40:00Z')
        self.assertEqual(len(works), 101)
        self.assertEqual(
            [call.args[1]['offset'] for call in thoth.client.execute.call_args_list],
            [0, 100],
        )
        self.assertTrue(all(
            call.args[1]['limit'] == 100
            for call in thoth.client.execute.call_args_list))

    def test_query_applies_publishers_status_types_and_start_filter(self):
        thoth = MagicMock()
        thoth.client.execute.return_value = {'data': {'works': []}}
        get_internet_archive_selection_works(
            thoth, [PUBLISHER_ID], SUPPORTED_WORK_TYPES,
            '2026-07-21T22:40:00Z')
        variables = thoth.client.execute.call_args.args[1]
        self.assertEqual(variables['publishers'], [PUBLISHER_ID])
        self.assertEqual(variables['workStatuses'], ['ACTIVE'])
        self.assertEqual(variables['workTypes'], list(SUPPORTED_WORK_TYPES))
        self.assertEqual(variables['updatedAtWithRelations'], {
            'timestamp': '2026-07-21T22:40:00Z',
            'expression': 'GREATER_THAN',
        })

    def test_graphql_errors_retain_message_without_extensions(self):
        thoth = MagicMock()
        thoth.client.execute.return_value = {
            'errors': [{
                'message': 'useful failure',
                'path': ['works'],
                'extensions': {'debug': 'internal'},
            }],
        }
        with self.assertRaises(ThothGraphQLResponseError) as raised:
            get_internet_archive_selection_works(
                thoth, [PUBLISHER_ID], SUPPORTED_WORK_TYPES,
                '2026-07-21T22:40:00Z')
        self.assertIn('useful failure', str(raised.exception))
        self.assertNotIn('internal', str(raised.exception))

    def test_page_size_must_be_bounded(self):
        for value in (0, 101, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                get_internet_archive_selection_works(
                    MagicMock(), [PUBLISHER_ID], SUPPORTED_WORK_TYPES,
                    '2026-07-21T22:40:00Z', page_size=value)


class TestExistingFinderJSONCompatibility(unittest.TestCase):

    def test_existing_finder_stdout_is_valid_json(self):
        finder = MagicMock()
        finder.thoth_ids = ['a', 'b']
        finder.get_publishers = MagicMock()
        finder.get_query_parameters = MagicMock()
        finder.publisher_selection_is_empty = MagicMock(return_value=False)
        finder.get_thoth_ids = MagicMock()
        finder.remove_exceptions = MagicMock()
        finder.post_process = MagicMock()
        from obtain_new_ids import IDFinder
        stdout = StringIO()
        with redirect_stdout(stdout):
            IDFinder.run(finder)
        self.assertEqual(json.loads(stdout.getvalue()), ['a', 'b'])

def make_location(platform):
    return SimpleNamespace(locationPlatform=platform)


def make_publication(pub_id, pub_type, locations=None):
    return SimpleNamespace(
        publicationId=pub_id,
        publicationType=pub_type,
        locations=locations or [],
    )


def make_work(work_id, doi, publications):
    return SimpleNamespace(
        workId=work_id,
        doi=doi,
        publications=publications,
    )


class TestOapenLocationsPostProcess(unittest.TestCase):

    def setUp(self):
        patcher = patch('obtain_new_ids.get_thoth_client')
        self.mock_get_thoth = patcher.start()
        self.addCleanup(patcher.stop)

        self.mock_thoth = MagicMock()
        self.mock_get_thoth.return_value = self.mock_thoth

        from obtain_new_ids import OapenLocationsIDFinder
        self.finder = OapenLocationsIDFinder()

    def test_missing_both_platforms(self):
        """Test case 1: PDF with neither OAPEN nor DOAB produces ["OAPEN", "DOAB"]."""
        pdf_pub = make_publication("pub-1", "PDF", [])
        work = make_work("work-1", "https://doi.org/10.1234/test", [pdf_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-1"]

        self.finder.post_process()

        self.assertEqual(len(self.finder.thoth_ids), 1)
        pub_id, doi, missing = self.finder.thoth_ids[0]
        self.assertEqual(pub_id, "pub-1")
        self.assertEqual(doi, "10.1234/test")
        self.assertEqual(missing, ["OAPEN", "DOAB"])

    def test_missing_doab_only(self):
        """Test case 3: PDF with OAPEN but no DOAB produces ["DOAB"] only."""
        locations = [make_location("OAPEN")]
        pdf_pub = make_publication("pub-2", "PDF", locations)
        work = make_work("work-2", "https://doi.org/10.1234/test2", [pdf_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-2"]

        self.finder.post_process()

        self.assertEqual(len(self.finder.thoth_ids), 1)
        pub_id, doi, missing = self.finder.thoth_ids[0]
        self.assertEqual(pub_id, "pub-2")
        self.assertEqual(missing, ["DOAB"])

    def test_missing_oapen_only(self):
        """Test case 2: PDF with DOAB but no OAPEN produces ["OAPEN"] only."""
        locations = [make_location("DOAB")]
        pdf_pub = make_publication("pub-3", "PDF", locations)
        work = make_work("work-3", "https://doi.org/10.1234/test3", [pdf_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-3"]

        self.finder.post_process()

        self.assertEqual(len(self.finder.thoth_ids), 1)
        pub_id, doi, missing = self.finder.thoth_ids[0]
        self.assertEqual(pub_id, "pub-3")
        self.assertEqual(missing, ["OAPEN"])

    def test_both_present_excluded(self):
        """Test case 4: PDF with both locations is excluded."""
        locations = [make_location("OAPEN"), make_location("DOAB")]
        pdf_pub = make_publication("pub-4", "PDF", locations)
        work = make_work("work-4", "https://doi.org/10.1234/test4", [pdf_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-4"]

        self.finder.post_process()

        self.assertEqual(self.finder.thoth_ids, [])

    def test_no_pdf_publication_skipped(self):
        """Non-PDF publications are skipped."""
        epub_pub = make_publication("pub-5", "EPUB", [])
        work = make_work("work-5", "https://doi.org/10.1234/test5", [epub_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-5"]

        self.finder.post_process()

        self.assertEqual(self.finder.thoth_ids, [])

    def test_no_doi_skipped(self):
        """Works without DOI are skipped even if locations missing."""
        pdf_pub = make_publication("pub-6", "PDF", [])
        work = make_work("work-6", None, [pdf_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-6"]

        self.finder.post_process()

        self.assertEqual(self.finder.thoth_ids, [])


class OrdinaryIDFinderCharacterizationTestCase(unittest.TestCase):
    """
    Pre-refactor characterization of the ordinary (non-Internet Archive)
    `IDFinder` publisher/exception/stdout/exit contract required by DIS-01.

    These tests record existing behaviour exactly as implemented at the
    DIS-01 baseline, including behaviour that is arguably surprising. They
    are the compatibility baseline for `env` and legacy-authoritative
    `compare` mode and must keep passing unchanged.
    """

    def setUp(self):
        self.thoth = MagicMock()
        self.thoth.publisher.return_value = SimpleNamespace(
            publisherId=PUBLISHER_ID)
        self.thoth.works.return_value = []
        client = patch(
            'obtain_new_ids.get_thoth_client', return_value=self.thoth)
        client.start()
        self.addCleanup(client.stop)
        self.environment = patch.dict(os.environ, {
            'ENV_PUBLISHERS': json.dumps([PUBLISHER_ID]),
        }, clear=False)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        os.environ.pop('ENV_EXCEPTIONS', None)

    def finder(self):
        return IDFinder()


class TestOrdinaryPublisherConfigurationCharacterization(
        OrdinaryIDFinderCharacterizationTestCase):

    def test_valid_publishers_are_confirmed_and_re_serialised_verbatim(self):
        second_publisher = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
        os.environ['ENV_PUBLISHERS'] = json.dumps(
            [second_publisher, PUBLISHER_ID])
        finder = self.finder()
        finder.get_publishers()
        self.assertEqual(
            finder.publishers,
            json.dumps([second_publisher, PUBLISHER_ID]),
        )
        self.assertEqual(
            [call.kwargs['publisher_id']
             for call in self.thoth.publisher.call_args_list],
            [second_publisher, PUBLISHER_ID],
        )

    def test_publisher_order_and_duplicates_are_preserved(self):
        os.environ['ENV_PUBLISHERS'] = json.dumps(
            [PUBLISHER_ID, PUBLISHER_ID])
        finder = self.finder()
        finder.get_publishers()
        self.assertEqual(
            finder.publishers, json.dumps([PUBLISHER_ID, PUBLISHER_ID]))

    def test_publisher_ids_are_not_normalised_or_validated_as_uuids(self):
        os.environ['ENV_PUBLISHERS'] = json.dumps([PUBLISHER_ID.upper()])
        finder = self.finder()
        finder.get_publishers()
        self.assertEqual(
            finder.publishers, json.dumps([PUBLISHER_ID.upper()]))

    def test_missing_publisher_variable_exits_one(self):
        os.environ.pop('ENV_PUBLISHERS')
        finder = self.finder()
        with self.assertRaises(SystemExit) as raised:
            finder.get_publishers()
        self.assertEqual(raised.exception.code, 1)
        self.thoth.publisher.assert_not_called()

    def test_empty_publisher_list_exits_one(self):
        os.environ['ENV_PUBLISHERS'] = '[]'
        finder = self.finder()
        with self.assertRaises(SystemExit) as raised:
            finder.get_publishers()
        self.assertEqual(raised.exception.code, 1)
        self.thoth.publisher.assert_not_called()

    def test_malformed_publisher_configuration_exits_one(self):
        for value in ('not-json', '[', '[1, 2', ''):
            with self.subTest(value=value):
                os.environ['ENV_PUBLISHERS'] = value
                finder = self.finder()
                with self.assertRaises(SystemExit) as raised:
                    finder.get_publishers()
                self.assertEqual(raised.exception.code, 1)

    def test_unsized_json_publishers_raise_type_error_rather_than_exiting(
            self):
        # Characterization only: the existing bare `except` covers the JSON
        # decode, not the subsequent length check, so a valid but unsized
        # JSON value propagates a TypeError instead of the exit-1
        # configuration failure.
        for value in ('null', 'true', '3'):
            with self.subTest(value=value):
                os.environ['ENV_PUBLISHERS'] = value
                finder = self.finder()
                with self.assertRaises(TypeError):
                    finder.get_publishers()

    def test_json_object_publishers_are_accepted_as_their_keys(self):
        # Characterization only: any non-empty JSON container passes the
        # emptiness check, so an object's keys are confirmed as publisher IDs
        # and re-serialised verbatim.
        os.environ['ENV_PUBLISHERS'] = '{"' + PUBLISHER_ID + '": true}'
        finder = self.finder()
        finder.get_publishers()
        self.assertEqual(
            self.thoth.publisher.call_args.kwargs['publisher_id'],
            PUBLISHER_ID,
        )
        self.assertEqual(
            finder.publishers, json.dumps({PUBLISHER_ID: True}))

    def test_unknown_publisher_exits_one(self):
        self.thoth.publisher.side_effect = errors.ThothError(
            'publisher query', 'no record')
        finder = self.finder()
        with self.assertRaises(SystemExit) as raised:
            finder.get_publishers()
        self.assertEqual(raised.exception.code, 1)


class TestOrdinaryExceptionCharacterization(
        OrdinaryIDFinderCharacterizationTestCase):

    def apply_exceptions(self, work_ids, raw_exceptions):
        finder = self.finder()
        finder.thoth_ids = list(work_ids)
        if raw_exceptions is None:
            os.environ.pop('ENV_EXCEPTIONS', None)
        else:
            os.environ['ENV_EXCEPTIONS'] = raw_exceptions
        finder.remove_exceptions()
        return finder.thoth_ids

    def test_unset_and_empty_exceptions_preserve_the_list_exactly(self):
        work_ids = ['b', 'a', 'c']
        for value in (None, ''):
            with self.subTest(value=value):
                self.assertEqual(
                    self.apply_exceptions(work_ids, value), work_ids)

    def test_configured_exceptions_are_removed_as_a_set_difference(self):
        remaining = self.apply_exceptions(
            [WORK_ID, 'aaaaaaaa-0000-4000-8000-000000000001'],
            json.dumps([WORK_ID]),
        )
        self.assertEqual(
            set(remaining), {'aaaaaaaa-0000-4000-8000-000000000001'})

    def test_exception_values_are_lowercased_before_comparison(self):
        remaining = self.apply_exceptions(
            [WORK_ID], json.dumps([WORK_ID.upper()]))
        self.assertEqual(remaining, [])

    def test_uppercase_selected_ids_are_not_matched_by_exceptions(self):
        # Characterization only: only the exception configuration is
        # lowercased, so an upper-case selected ID is never matched.
        mixed_case_work_id = 'AAAAAAAA-1111-4111-8111-111111111111'
        remaining = self.apply_exceptions(
            [mixed_case_work_id], json.dumps([mixed_case_work_id]))
        self.assertEqual(remaining, [mixed_case_work_id])

    def test_malformed_exception_configuration_exits_one(self):
        for value in ('not-json', '['):
            with self.subTest(value=value):
                with self.assertRaises(SystemExit) as raised:
                    self.apply_exceptions([WORK_ID], value)
                self.assertEqual(raised.exception.code, 1)


class TestOrdinaryOutputAndExitCharacterization(
        OrdinaryIDFinderCharacterizationTestCase):

    def test_run_prints_compact_json_array_with_trailing_newline(self):
        finder = self.finder()
        self.thoth.bookIds.return_value = [
            SimpleNamespace(workId=WORK_ID),
            SimpleNamespace(workId='22222222-2222-4222-8222-222222222222'),
        ]
        stdout = StringIO()
        with redirect_stdout(stdout):
            finder.run()
        self.assertEqual(
            sorted(json.loads(stdout.getvalue())),
            [WORK_ID, '22222222-2222-4222-8222-222222222222'],
        )
        self.assertEqual(
            stdout.getvalue(),
            json.dumps(finder.thoth_ids, separators=(',', ':')) + '\n',
        )

    def test_run_passes_the_verbatim_publisher_list_to_the_work_query(self):
        finder = self.finder()
        self.thoth.bookIds.return_value = []
        with redirect_stdout(StringIO()):
            finder.run()
        self.assertEqual(
            self.thoth.bookIds.call_args.kwargs['publishers'],
            json.dumps([PUBLISHER_ID]),
        )

    def test_empty_result_prints_empty_json_array(self):
        finder = self.finder()
        self.thoth.bookIds.return_value = []
        stdout = StringIO()
        with redirect_stdout(stdout):
            finder.run()
        self.assertEqual(stdout.getvalue(), '[]\n')

    def test_successful_ordinary_selection_returns_zero(self):
        stdout = StringIO()
        with redirect_stdout(stdout):
            status = main(['--platform', 'OAPEN'])
        self.assertEqual(status, 0)
        self.assertEqual(stdout.getvalue(), '[]\n')

    def test_legacy_publisher_failure_exits_one_through_main(self):
        os.environ['ENV_PUBLISHERS'] = 'not-json'
        with self.assertRaises(SystemExit) as raised:
            main(['--platform', 'OAPEN'])
        self.assertEqual(raised.exception.code, 1)

    def test_legacy_exception_failure_exits_one_through_main(self):
        os.environ['ENV_EXCEPTIONS'] = 'not-json'
        with self.assertRaises(SystemExit) as raised, redirect_stdout(
                StringIO()):
            main(['--platform', 'OAPEN'])
        self.assertEqual(raised.exception.code, 1)

    def test_unknown_platform_returns_one(self):
        self.assertEqual(main(['--platform', 'NotAPlatform']), 1)


PUBLISHER_B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'


def publisher_discovery_stub(assignments):
    """Stand in for the reconciled Publisher Services discovery helper."""
    def discover(_thoth, api_platform, page_size=None):
        return sorted(assignments[api_platform])
    return discover


class PublisherSourceModeTestCase(unittest.TestCase):
    """Shared fixtures for DIS-01 publisher-source mode behaviour."""

    def setUp(self):
        self.thoth = MagicMock()
        self.thoth.publisher.return_value = SimpleNamespace(
            publisherId=PUBLISHER_ID)
        self.thoth.works.return_value = []
        self.thoth.bookIds.return_value = [SimpleNamespace(workId=WORK_ID)]
        client = patch(
            'obtain_new_ids.get_thoth_client', return_value=self.thoth)
        client.start()
        self.addCleanup(client.stop)
        self.environment = patch.dict(os.environ, {
            'ENV_PUBLISHERS': json.dumps([PUBLISHER_ID]),
        }, clear=False)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        for variable in ('ENV_EXCEPTIONS', 'PUBLISHER_SOURCE_MODES'):
            os.environ.pop(variable, None)

    def patch_discovery(self, assignments=None, side_effect=None):
        """Patch the single Publisher Services page/count reconciliation."""
        patcher = patch(
            'publisher_source.get_distribution_platform_publisher_ids')
        discovery = patcher.start()
        self.addCleanup(patcher.stop)
        if side_effect is not None:
            discovery.side_effect = side_effect
        else:
            discovery.side_effect = publisher_discovery_stub(assignments or {})
        return discovery

    def ordinary_finder(self, source_mode=MODE_ENV, platform='OAPEN', **kwargs):
        return IDFinder(
            platform=platform,
            source_mode=source_mode,
            thoth=self.thoth,
            **kwargs,
        )

    def run_finder(self, finder):
        stdout = StringIO()
        with redirect_stdout(stdout):
            finder.run()
        return stdout.getvalue()


class TestEnvModeCompatibility(PublisherSourceModeTestCase):

    def test_env_is_the_default_for_every_pathway(self):
        for platform in ('OAPEN', 'InternetArchive', 'Crossref'):
            with self.subTest(platform=platform):
                self.assertEqual(
                    resolve_source_mode(
                        platform, os.environ.get('PUBLISHER_SOURCE_MODES')),
                    MODE_ENV,
                )

    def test_ordinary_env_selection_makes_no_discovery_call(self):
        discovery = self.patch_discovery()
        finder = self.ordinary_finder()
        stdout = self.run_finder(finder)
        self.assertEqual(json.loads(stdout), [WORK_ID])
        self.assertEqual(
            self.thoth.bookIds.call_args.kwargs['publishers'],
            json.dumps([PUBLISHER_ID]),
        )
        discovery.assert_not_called()
        self.assertIsNone(finder.comparison_report)

    def test_internet_archive_env_selection_makes_no_discovery_call(self):
        discovery = self.patch_discovery()
        finder = InternetArchiveIDFinder(
            thoth=self.thoth, now_provider=lambda: NOW)
        with patch(
                'obtain_new_ids.get_internet_archive_selection_works',
                return_value=[selection_work()]):
            report = finder.select()
        self.assertEqual(report['selected_count'], 1)
        self.assertEqual(report['publisher_ids'], [PUBLISHER_ID])
        discovery.assert_not_called()

    def test_env_mode_preserves_ordinary_exception_behaviour(self):
        self.patch_discovery()
        os.environ['ENV_EXCEPTIONS'] = json.dumps([WORK_ID])
        stdout = self.run_finder(self.ordinary_finder())
        self.assertEqual(json.loads(stdout), [])

    def test_env_mode_preserves_internet_archive_exception_behaviour(self):
        self.patch_discovery()
        os.environ['ENV_EXCEPTIONS'] = json.dumps([WORK_ID.upper()])
        finder = InternetArchiveIDFinder(
            thoth=self.thoth, now_provider=lambda: NOW)
        with patch(
                'obtain_new_ids.get_internet_archive_selection_works',
                return_value=[selection_work()]):
            report = finder.select()
        self.assertEqual(report['exception_ids'], [WORK_ID])
        self.assertEqual(report['selected_count'], 0)
        self.assertEqual(
            report['excluded_counts'], {'configured_exception': 1})


class TestCompareModeCompatibility(PublisherSourceModeTestCase):

    def test_compare_reports_match_without_changing_selection(self):
        self.patch_discovery({
            'OAPEN': [PUBLISHER_ID], 'DOAB': [PUBLISHER_ID]})
        finder = self.ordinary_finder(MODE_COMPARE)
        stdout = self.run_finder(finder)
        self.assertEqual(json.loads(stdout), [WORK_ID])
        self.assertEqual(finder.comparison_report['status'], 'MATCH')
        self.assertEqual(
            self.thoth.bookIds.call_args.kwargs['publishers'],
            json.dumps([PUBLISHER_ID]),
        )

    def test_compare_reports_diff_without_changing_selection(self):
        self.patch_discovery({
            'OAPEN': [PUBLISHER_ID, PUBLISHER_B],
            'DOAB': [PUBLISHER_ID, PUBLISHER_B],
        })
        finder = self.ordinary_finder(MODE_COMPARE)
        stdout = self.run_finder(finder)
        report = finder.comparison_report
        self.assertEqual(report['status'], 'DIFF')
        self.assertEqual(report['apiOnly'], [PUBLISHER_B])
        self.assertEqual(json.loads(stdout), [WORK_ID])
        # An API-only publisher is never disseminated.
        self.assertEqual(
            self.thoth.bookIds.call_args.kwargs['publishers'],
            json.dumps([PUBLISHER_ID]),
        )

    def test_legacy_only_publishers_remain_selected(self):
        self.patch_discovery({'OAPEN': [], 'DOAB': []})
        finder = self.ordinary_finder(MODE_COMPARE)
        stdout = self.run_finder(finder)
        self.assertEqual(finder.comparison_report['legacyOnly'],
                         [PUBLISHER_ID])
        self.assertEqual(json.loads(stdout), [WORK_ID])

    def test_compare_stdout_is_byte_identical_to_env(self):
        self.patch_discovery({
            'OAPEN': [PUBLISHER_ID, PUBLISHER_B],
            'DOAB': [PUBLISHER_ID, PUBLISHER_B],
        })
        os.environ['ENV_EXCEPTIONS'] = json.dumps(
            ['22222222-2222-4222-8222-222222222222'])
        self.thoth.bookIds.return_value = [
            SimpleNamespace(workId=WORK_ID),
            SimpleNamespace(workId='33333333-3333-4333-8333-333333333333'),
            SimpleNamespace(workId='22222222-2222-4222-8222-222222222222'),
        ]
        env_stdout = self.run_finder(self.ordinary_finder(MODE_ENV))
        compare_stdout = self.run_finder(self.ordinary_finder(MODE_COMPARE))
        self.assertEqual(env_stdout, compare_stdout)

    def test_discovery_failure_preserves_ids_stdout_and_exit_status(self):
        self.patch_discovery(
            side_effect=RuntimeError('publisher discovery unavailable'))
        env_stdout = self.run_finder(self.ordinary_finder(MODE_ENV))
        finder = self.ordinary_finder(MODE_COMPARE)
        compare_stdout = self.run_finder(finder)
        self.assertEqual(compare_stdout, env_stdout)
        self.assertEqual(finder.thoth_ids, [WORK_ID])
        self.assertEqual(finder.comparison_report['status'], 'ERROR')
        self.assertEqual(
            finder.comparison_report['contractIssues'][0]['issue'],
            'api_discovery_failed',
        )

    def test_comparison_error_is_reported_on_stderr_not_stdout(self):
        self.patch_discovery(side_effect=RuntimeError('unavailable'))
        finder = self.ordinary_finder(MODE_COMPARE)
        stdout = StringIO()
        with redirect_stdout(stdout), self.assertLogs(level='ERROR') as logs:
            finder.run()
        self.assertEqual(json.loads(stdout.getvalue()), [WORK_ID])
        self.assertIn('Publisher comparison (ERROR)', '\n'.join(logs.output))
        self.assertNotIn('Publisher comparison', stdout.getvalue())

    def test_compare_discovery_failure_still_exits_zero(self):
        self.patch_discovery(side_effect=RuntimeError('unavailable'))
        with patch.dict(os.environ, {
                'PUBLISHER_SOURCE_MODES': json.dumps({'OAPEN': 'compare'})}):
            stdout = StringIO()
            with redirect_stdout(stdout):
                status = main(['--platform', 'OAPEN'], thoth=self.thoth)
        self.assertEqual(status, 0)
        self.assertEqual(stdout.getvalue(), '[]\n')

    def test_unexpected_comparison_failure_is_isolated(self):
        self.patch_discovery({
            'OAPEN': [PUBLISHER_ID], 'DOAB': [PUBLISHER_ID]})
        with patch(
                'obtain_new_ids.build_comparison_report',
                side_effect=RuntimeError('comparison exploded')):
            finder = self.ordinary_finder(MODE_COMPARE)
            stdout = self.run_finder(finder)
        self.assertEqual(json.loads(stdout), [WORK_ID])
        self.assertEqual(finder.comparison_report['status'], 'ERROR')
        self.assertEqual(
            finder.comparison_report['contractIssues'][0]['issue'],
            'comparison_failed',
        )

    def test_report_write_failure_preserves_stdout_and_exit_status(self):
        self.patch_discovery({
            'OAPEN': [PUBLISHER_ID], 'DOAB': [PUBLISHER_ID]})
        with patch(
                'obtain_new_ids.write_comparison_report',
                side_effect=OSError('read-only file system')):
            finder = self.ordinary_finder(
                MODE_COMPARE, comparison_report_path='publisher.json')
            with self.assertLogs(level='ERROR'):
                stdout = self.run_finder(finder)
        self.assertEqual(json.loads(stdout), [WORK_ID])
        self.assertEqual(finder.comparison_report['status'], 'MATCH')

    def test_comparison_report_is_written_to_its_own_file(self):
        self.patch_discovery({
            'OAPEN': [PUBLISHER_ID], 'DOAB': [PUBLISHER_ID]})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'publisher-comparison.json'
            finder = self.ordinary_finder(
                MODE_COMPARE, comparison_report_path=str(path))
            stdout = self.run_finder(finder)
            written = json.loads(path.read_text(encoding='utf-8'))
        self.assertEqual(written, finder.comparison_report)
        self.assertEqual(json.loads(stdout), [WORK_ID])
        self.assertNotIn('schemaVersion', stdout)

    def test_compare_never_calls_a_mutation_or_uploader(self):
        self.patch_discovery({
            'OAPEN': [PUBLISHER_ID], 'DOAB': [PUBLISHER_ID]})
        finder = self.ordinary_finder(MODE_COMPARE)
        self.run_finder(finder)
        for forbidden in (
                'mutation', 'create_location', 'update_location',
                'replacePublisherServiceConfiguration'):
            self.assertFalse(
                any(forbidden in str(call)
                    for call in self.thoth.mock_calls),
                forbidden,
            )

    def test_internet_archive_compare_preserves_selection_and_exceptions(self):
        self.patch_discovery({'INTERNET_ARCHIVE': [PUBLISHER_B]})
        os.environ['ENV_EXCEPTIONS'] = json.dumps([WORK_ID])
        finder = InternetArchiveIDFinder(
            thoth=self.thoth, now_provider=lambda: NOW,
            source_mode=MODE_COMPARE)
        with patch(
                'obtain_new_ids.get_internet_archive_selection_works',
                return_value=[selection_work()]):
            report = finder.select()
        self.assertEqual(report['publisher_ids'], [PUBLISHER_ID])
        self.assertEqual(report['exception_ids'], [WORK_ID])
        self.assertEqual(report['selected_count'], 0)
        self.assertEqual(finder.comparison_report['status'], 'DIFF')
        self.assertEqual(
            finder.comparison_report['apiOnly'], [PUBLISHER_B])

    def test_internet_archive_comparison_error_keeps_selection_successful(
            self):
        self.patch_discovery(side_effect=RuntimeError('unavailable'))
        finder = InternetArchiveIDFinder(
            thoth=self.thoth, now_provider=lambda: NOW,
            source_mode=MODE_COMPARE)
        with patch(
                'obtain_new_ids.get_internet_archive_selection_works',
                return_value=[selection_work()]):
            stdout = StringIO()
            with redirect_stdout(stdout):
                finder.run()
        self.assertEqual(json.loads(stdout.getvalue()), [WORK_ID])
        # The Internet Archive selection report records no failure status.
        self.assertNotIn('status', finder.report)
        self.assertEqual(finder.comparison_report['status'], 'ERROR')


class TestApiModeAuthority(PublisherSourceModeTestCase):

    def test_api_mode_selects_publishers_from_the_api(self):
        self.patch_discovery({
            'OAPEN': [PUBLISHER_B], 'DOAB': [PUBLISHER_B]})
        finder = self.ordinary_finder(MODE_API)
        stdout = self.run_finder(finder)
        self.assertEqual(
            self.thoth.bookIds.call_args.kwargs['publishers'],
            json.dumps([PUBLISHER_B]),
        )
        self.assertEqual(json.loads(stdout), [WORK_ID])

    def test_api_mode_ignores_environment_publishers(self):
        self.patch_discovery({
            'OAPEN': [PUBLISHER_B], 'DOAB': [PUBLISHER_B]})
        os.environ['ENV_PUBLISHERS'] = 'not-json'
        finder = self.ordinary_finder(MODE_API)
        self.run_finder(finder)
        self.assertEqual(finder.api_publisher_ids, [PUBLISHER_B])

    def test_api_failure_never_falls_back_to_environment_publishers(self):
        self.patch_discovery(side_effect=RuntimeError('unavailable'))
        with patch.dict(os.environ, {
                'PUBLISHER_SOURCE_MODES': json.dumps({'OAPEN': 'api'})}):
            status = main(['--platform', 'OAPEN'], thoth=self.thoth)
        self.assertEqual(status, 1)
        self.thoth.bookIds.assert_not_called()
        self.thoth.works.assert_not_called()

    def test_reconciled_zero_publishers_is_a_successful_empty_selection(self):
        self.patch_discovery({'OAPEN': [], 'DOAB': []})
        finder = self.ordinary_finder(MODE_API)
        stdout = self.run_finder(finder)
        self.assertEqual(stdout, '[]\n')
        self.assertEqual(finder.thoth_ids, [])
        # An empty publisher filter must never reach an existing work query,
        # where it would select every publisher's works.
        self.thoth.bookIds.assert_not_called()
        self.thoth.works.assert_not_called()

    def test_zero_publisher_api_selection_exits_zero(self):
        self.patch_discovery({'OAPEN': [], 'DOAB': []})
        with patch.dict(os.environ, {
                'PUBLISHER_SOURCE_MODES': json.dumps({'OAPEN': 'api'})}):
            stdout = StringIO()
            with redirect_stdout(stdout):
                status = main(['--platform', 'OAPEN'], thoth=self.thoth)
        self.assertEqual(status, 0)
        self.assertEqual(stdout.getvalue(), '[]\n')

    def test_linked_platform_mismatch_fails_closed_in_api_mode(self):
        self.patch_discovery({
            'OAPEN': [PUBLISHER_ID], 'DOAB': [PUBLISHER_B]})
        with patch.dict(os.environ, {
                'PUBLISHER_SOURCE_MODES': json.dumps({'OAPEN': 'api'})}):
            status = main(['--platform', 'OAPEN'], thoth=self.thoth)
        self.assertEqual(status, 1)
        self.thoth.works.assert_not_called()

    def test_internet_archive_api_zero_publishers_queries_no_works(self):
        self.patch_discovery({'INTERNET_ARCHIVE': []})
        finder = InternetArchiveIDFinder(
            thoth=self.thoth, now_provider=lambda: NOW, source_mode=MODE_API)
        with patch(
                'obtain_new_ids.get_internet_archive_selection_works') as query:
            report = finder.select()
        query.assert_not_called()
        self.assertEqual(report['selected_count'], 0)
        self.assertEqual(report['publisher_ids'], [])

    def test_internet_archive_api_failure_reports_and_exits_one(self):
        self.patch_discovery(side_effect=RuntimeError('unavailable'))
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / 'failure.json'
            with patch.dict(os.environ, {
                    'PUBLISHER_SOURCE_MODES': json.dumps(
                        {'InternetArchive': 'api'})}):
                status = main([
                    '--platform', 'InternetArchive',
                    '--report', str(report_path),
                ], now_provider=lambda: NOW, thoth=self.thoth)
            report = json.loads(report_path.read_text(encoding='utf-8'))
        self.assertEqual(status, 1)
        self.assertEqual(report['status'], 'failed')

    def test_configuration_failure_exits_one_without_selecting(self):
        self.patch_discovery()
        with patch.dict(os.environ, {
                'PUBLISHER_SOURCE_MODES': 'not-json'}):
            status = main(['--platform', 'OAPEN'], thoth=self.thoth)
        self.assertEqual(status, 1)
        self.thoth.works.assert_not_called()


class TestLocationsPathwayIsolation(PublisherSourceModeTestCase):

    def locations_arguments(self):
        return get_arguments(['--platform', 'OAPEN', '--locations'])

    def test_locations_always_uses_legacy_env_publisher_authority(self):
        for mode in ('compare', 'api'):
            with self.subTest(mode=mode), patch.dict(os.environ, {
                    'PUBLISHER_SOURCE_MODES': json.dumps({'OAPEN': mode})}):
                finder = get_id_finder(
                    self.locations_arguments(), thoth=self.thoth)
                self.assertIsInstance(finder, OapenLocationsIDFinder)
                self.assertEqual(finder.source_mode, MODE_ENV)

    def test_locations_makes_zero_publisher_services_calls(self):
        discovery = self.patch_discovery({
            'OAPEN': [PUBLISHER_B], 'DOAB': [PUBLISHER_B]})
        for mode in ('compare', 'api'):
            with self.subTest(mode=mode), patch.dict(os.environ, {
                    'PUBLISHER_SOURCE_MODES': json.dumps({'OAPEN': mode})}):
                finder = get_id_finder(
                    self.locations_arguments(), thoth=self.thoth)
                self.thoth.work_by_id.return_value = SimpleNamespace(
                    workId=WORK_ID, doi=None, publications=[])
                stdout = StringIO()
                with redirect_stdout(stdout):
                    finder.run()
                self.assertEqual(stdout.getvalue(), '[]\n')
                self.assertEqual(
                    self.thoth.bookIds.call_args.kwargs['publishers'],
                    json.dumps([PUBLISHER_ID]),
                )
        discovery.assert_not_called()

    def test_locations_never_resolves_the_publisher_source_configuration(self):
        with patch.dict(os.environ, {
                'PUBLISHER_SOURCE_MODES': json.dumps({'OAPEN': 'api'})}), patch(
                    'obtain_new_ids.resolve_source_mode') as resolve:
            get_id_finder(self.locations_arguments(), thoth=self.thoth)
        resolve.assert_not_called()

    def test_locations_ignores_even_malformed_publisher_source_modes(self):
        with patch.dict(os.environ, {'PUBLISHER_SOURCE_MODES': 'not-json'}):
            finder = get_id_finder(
                self.locations_arguments(), thoth=self.thoth)
        self.assertIsInstance(finder, OapenLocationsIDFinder)
        self.assertEqual(finder.source_mode, MODE_ENV)


if __name__ == '__main__':
    unittest.main()
