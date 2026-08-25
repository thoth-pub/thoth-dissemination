import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import publisher_source
from publisher_source import (
    COMPARISON_SCHEMA_VERSION,
    DESTINATIONS,
    DISSEMINATION_PLATFORMS,
    MODE_API,
    MODE_COMPARE,
    MODE_ENV,
    LinkedPlatformMismatchError,
    PublisherDiscoveryError,
    PublisherSourceConfigurationError,
    api_platforms_for,
    build_comparison_report,
    comparison_report_error,
    discover_api_publisher_ids,
    main,
    parse_source_modes,
    resolve_source_mode,
    sanitise_detail,
    serialise_comparison_report,
    summarise_comparison,
    supports_publisher_discovery,
    write_comparison_report,
)
from thothapi import (
    DISTRIBUTION_PLATFORM_PUBLISHERS_QUERY,
    DISTRIBUTION_PLATFORM_PUBLISHER_COUNT_QUERY,
    ThothGraphQLResponseError,
    ThothGraphQLTransportError,
    ThothPublisherDiscoveryError,
    get_distribution_platform_publisher_ids,
)


ROOT = Path(__file__).resolve().parents[1]
BULK_WORKFLOW = ROOT / '.github' / 'workflows' / 'bulk_disseminate.yml'

PUBLISHER_A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
PUBLISHER_B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
PUBLISHER_C = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'

# The 17 pinned DistributionPlatform values of thoth-pub/thoth v1.7.0
# (commit 40e9c06d4ab76217c3ef277dd539d3b5580e2bb8).
PINNED_DISTRIBUTION_PLATFORMS = (
    'INTERNET_ARCHIVE',
    'OAPEN',
    'DOAB',
    'SCIENCE_OPEN',
    'CAMBRIDGE_UNIVERSITY_LIBRARY',
    'CROSSREF',
    'FIGSHARE',
    'ZENODO',
    'PROJECT_MUSE',
    'JSTOR',
    'EBSCO_HOST',
    'PROQUEST_EBOOK_CENTRAL',
    'GOOGLE_PLAY',
    'BKCI',
    'OCLC_KB',
    'EX_LIBRIS_KB',
    'JISC_NBK',
)


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


def publisher_page(publisher_ids):
    return {
        'data': {
            'publishersByDistributionPlatform': [
                {'publisherId': publisher_id} for publisher_id in publisher_ids
            ],
        },
    }


def count_page(count):
    return {'data': {'publisherCountByDistributionPlatform': count}}


def discovery_client(assignments, page_size=100):
    """A Thoth stub answering count and page queries per platform."""
    thoth = MagicMock()

    def execute(query, variables):
        platform = variables['platform']
        publisher_ids = sorted(assignments.get(platform, []))
        if query == DISTRIBUTION_PLATFORM_PUBLISHER_COUNT_QUERY:
            return count_page(len(publisher_ids))
        offset = variables['offset']
        limit = variables['limit']
        return publisher_page(publisher_ids[offset:offset + limit])

    thoth.client.execute.side_effect = execute
    return thoth


class TestSourceModeConfiguration(unittest.TestCase):

    def test_missing_and_empty_configuration_resolve_to_env(self):
        for value in (None, '', '   '):
            with self.subTest(value=value):
                self.assertEqual(parse_source_modes(value), {})
                self.assertEqual(
                    resolve_source_mode('OAPEN', value), MODE_ENV)

    def test_missing_platform_key_resolves_to_env(self):
        self.assertEqual(
            resolve_source_mode(
                'OAPEN', json.dumps({'InternetArchive': MODE_COMPARE})),
            MODE_ENV,
        )

    def test_configured_modes_are_resolved_exactly(self):
        raw = json.dumps({'OAPEN': 'compare', 'InternetArchive': 'api'})
        self.assertEqual(resolve_source_mode('OAPEN', raw), MODE_COMPARE)
        self.assertEqual(
            resolve_source_mode('InternetArchive', raw), MODE_API)

    def test_environment_variable_is_used_when_no_value_is_passed(self):
        with patch.dict(os.environ, {
                'PUBLISHER_SOURCE_MODES': json.dumps({'OAPEN': 'compare'})}):
            self.assertEqual(resolve_source_mode('OAPEN'), MODE_COMPARE)

    def test_malformed_json_is_a_visible_configuration_failure(self):
        for value in ('not-json', '{', '[1,'):
            with self.subTest(value=value), self.assertRaises(
                    PublisherSourceConfigurationError):
                parse_source_modes(value)

    def test_non_object_configuration_fails(self):
        for value in ('[]', '"env"', '3', 'true', 'null'):
            with self.subTest(value=value), self.assertRaises(
                    PublisherSourceConfigurationError):
                parse_source_modes(value)

    def test_non_string_mode_fails(self):
        for value in (json.dumps({'OAPEN': 1}), json.dumps({'OAPEN': None}),
                      json.dumps({'OAPEN': ['compare']})):
            with self.subTest(value=value), self.assertRaises(
                    PublisherSourceConfigurationError):
                parse_source_modes(value)

    def test_unsupported_mode_fails(self):
        for mode in ('ENV', 'Compare', 'observe', 'legacy', ''):
            with self.subTest(mode=mode), self.assertRaises(
                    PublisherSourceConfigurationError):
                parse_source_modes(json.dumps({'OAPEN': mode}))

    def test_unknown_dissemination_platform_key_fails(self):
        for platform in ('Oapen', 'DOAB', 'OCLC_KB', 'MUSE'):
            with self.subTest(platform=platform), self.assertRaises(
                    PublisherSourceConfigurationError):
                parse_source_modes(json.dumps({platform: 'compare'}))

    def test_no_wildcard_key_can_activate_a_non_legacy_mode(self):
        for key in ('*', '', 'all', 'ALL', 'default', 'DEFAULT'):
            for mode in ('compare', 'api', 'env'):
                with self.subTest(key=key, mode=mode), self.assertRaises(
                        PublisherSourceConfigurationError):
                    parse_source_modes(json.dumps({key: mode}))

    def test_non_legacy_mode_is_rejected_for_manual_only_platforms(self):
        for mode in ('compare', 'api'):
            with self.subTest(mode=mode), self.assertRaises(
                    PublisherSourceConfigurationError):
                resolve_source_mode(
                    'ScienceOpen', json.dumps({'ScienceOpen': mode}))

    def test_env_remains_available_for_manual_only_platforms(self):
        self.assertEqual(
            resolve_source_mode(
                'ScienceOpen', json.dumps({'ScienceOpen': 'env'})),
            MODE_ENV,
        )


class TestPlatformClassification(unittest.TestCase):

    def test_every_pinned_platform_is_classified_exactly_once(self):
        self.assertEqual(
            tuple(
                destination.api_platform for destination in DESTINATIONS),
            PINNED_DISTRIBUTION_PLATFORMS,
        )

    def test_classification_has_no_wildcard_or_fallback_entry(self):
        api_platforms = {
            destination.api_platform for destination in DESTINATIONS}
        for value in ('*', 'OTHER', 'UNKNOWN', 'DEFAULT', ''):
            self.assertNotIn(value, api_platforms)

    def test_unknown_upstream_platform_fails_closed(self):
        with self.assertRaises(PublisherSourceConfigurationError):
            publisher_source.destination_for_api_platform('NEW_PLATFORM')

    def test_each_dissemination_platform_maps_to_its_api_platforms(self):
        expected = {
            'InternetArchive': ('INTERNET_ARCHIVE',),
            'OAPEN': ('OAPEN', 'DOAB'),
            'CUL': ('CAMBRIDGE_UNIVERSITY_LIBRARY',),
            'Crossref': ('CROSSREF',),
            'Figshare': ('FIGSHARE',),
            'Zenodo': ('ZENODO',),
            'ProjectMUSE': ('PROJECT_MUSE',),
            'JSTOR': ('JSTOR',),
            'EBSCOHost': ('EBSCO_HOST',),
            'ProQuest': ('PROQUEST_EBOOK_CENTRAL',),
            'GooglePlay': ('GOOGLE_PLAY',),
            'BKCI': ('BKCI',),
        }
        for platform, api_platforms in expected.items():
            with self.subTest(platform=platform):
                self.assertEqual(
                    api_platforms_for(platform), api_platforms)

    def test_oapen_and_doab_share_one_execution_adapter(self):
        self.assertEqual(api_platforms_for('OAPEN'), ('OAPEN', 'DOAB'))
        doab = publisher_source.destination_for_api_platform('DOAB')
        self.assertIsNone(doab.dissemination_platform)
        self.assertEqual(
            doab.linked_group,
            publisher_source.destination_for_api_platform(
                'OAPEN').linked_group,
        )

    def test_manual_pull_feed_and_inactive_platforms_have_no_pathway(self):
        expected = {
            'SCIENCE_OPEN': 'manual',
            'OCLC_KB': 'pull_feed',
            'EX_LIBRIS_KB': 'pull_feed',
            'JISC_NBK': 'inactive',
        }
        for api_platform, classification in expected.items():
            with self.subTest(api_platform=api_platform):
                destination = publisher_source.destination_for_api_platform(
                    api_platform)
                self.assertEqual(destination.classification, classification)
                self.assertFalse(destination.supports_publisher_discovery)

    def test_pull_feed_and_inactive_platforms_are_not_dissemination_targets(
            self):
        for api_platform in ('OCLC_KB', 'EX_LIBRIS_KB', 'JISC_NBK'):
            with self.subTest(api_platform=api_platform):
                destination = publisher_source.destination_for_api_platform(
                    api_platform)
                self.assertIsNone(destination.dissemination_platform)
                self.assertNotIn(api_platform, DISSEMINATION_PLATFORMS)

    def test_unmapped_dissemination_platform_fails_closed(self):
        for platform in ('OCLC_KB', 'NotAPlatform', 'DOAB', None):
            with self.subTest(platform=platform):
                self.assertFalse(supports_publisher_discovery(platform))
                with self.assertRaises(PublisherSourceConfigurationError):
                    api_platforms_for(platform)

    def test_supported_platforms_cover_every_automated_selector(self):
        for platform in DISSEMINATION_PLATFORMS:
            with self.subTest(platform=platform):
                self.assertEqual(
                    supports_publisher_discovery(platform),
                    platform != 'ScienceOpen',
                )


class TestApiPaginationAndReconciliation(unittest.TestCase):

    def test_count_query_precedes_the_publisher_pages(self):
        thoth = discovery_client({'CROSSREF': [PUBLISHER_A]})
        get_distribution_platform_publisher_ids(thoth, 'CROSSREF')
        queries = [
            call.args[0] for call in thoth.client.execute.call_args_list]
        self.assertEqual(
            queries[0], DISTRIBUTION_PLATFORM_PUBLISHER_COUNT_QUERY)
        self.assertEqual(queries[1], DISTRIBUTION_PLATFORM_PUBLISHERS_QUERY)

    def test_pages_use_an_explicit_positive_bounded_limit(self):
        thoth = discovery_client({'CROSSREF': [PUBLISHER_A]})
        get_distribution_platform_publisher_ids(thoth, 'CROSSREF')
        page_calls = [
            call.args[1] for call in thoth.client.execute.call_args_list
            if call.args[0] == DISTRIBUTION_PLATFORM_PUBLISHERS_QUERY
        ]
        for variables in page_calls:
            self.assertIsInstance(variables['limit'], int)
            self.assertGreater(variables['limit'], 0)
            self.assertLessEqual(variables['limit'], 100)
            self.assertIsNotNone(variables['limit'])

    def test_null_or_zero_page_sizes_are_rejected(self):
        for page_size in (None, 0, -1, 101, 1.5, True, '100'):
            with self.subTest(page_size=page_size), self.assertRaises(
                    ValueError):
                get_distribution_platform_publisher_ids(
                    MagicMock(), 'CROSSREF', page_size=page_size)

    def test_query_requests_deterministic_publisher_id_ascending_order(self):
        self.assertIn(
            'order: {field: PUBLISHER_ID, direction: ASC}',
            DISTRIBUTION_PLATFORM_PUBLISHERS_QUERY,
        )
        self.assertIn(
            'publishersByDistributionPlatform',
            DISTRIBUTION_PLATFORM_PUBLISHERS_QUERY,
        )
        self.assertIn(
            'publisherCountByDistributionPlatform',
            DISTRIBUTION_PLATFORM_PUBLISHER_COUNT_QUERY,
        )

    def test_pagination_continues_to_completion(self):
        publisher_ids = [
            '00000000-0000-4000-8000-{:012x}'.format(number)
            for number in range(250)
        ]
        thoth = discovery_client({'CROSSREF': publisher_ids})
        resolved = get_distribution_platform_publisher_ids(thoth, 'CROSSREF')
        self.assertEqual(resolved, sorted(publisher_ids))
        offsets = [
            call.args[1]['offset']
            for call in thoth.client.execute.call_args_list
            if call.args[0] == DISTRIBUTION_PLATFORM_PUBLISHERS_QUERY
        ]
        self.assertEqual(offsets, [0, 100, 200])

    def test_smaller_explicit_page_sizes_are_honoured(self):
        publisher_ids = [PUBLISHER_A, PUBLISHER_B, PUBLISHER_C]
        thoth = discovery_client({'CROSSREF': publisher_ids})
        resolved = get_distribution_platform_publisher_ids(
            thoth, 'CROSSREF', page_size=2)
        self.assertEqual(resolved, sorted(publisher_ids))

    def test_count_mismatch_fails_closed(self):
        thoth = MagicMock()
        thoth.client.execute.side_effect = [
            count_page(3), publisher_page([PUBLISHER_A, PUBLISHER_B])]
        with self.assertRaises(ThothPublisherDiscoveryError):
            get_distribution_platform_publisher_ids(thoth, 'CROSSREF')

    def test_more_publishers_than_reported_fails_closed(self):
        thoth = MagicMock()
        thoth.client.execute.side_effect = [
            count_page(1), publisher_page([PUBLISHER_A, PUBLISHER_B])]
        with self.assertRaises(ThothPublisherDiscoveryError):
            get_distribution_platform_publisher_ids(thoth, 'CROSSREF')

    def test_duplicate_publisher_identity_fails_closed(self):
        thoth = MagicMock()
        thoth.client.execute.side_effect = [
            count_page(2), publisher_page([PUBLISHER_A, PUBLISHER_A])]
        with self.assertRaises(ThothPublisherDiscoveryError):
            get_distribution_platform_publisher_ids(thoth, 'CROSSREF')

    def test_invalid_publisher_identity_fails_closed(self):
        for page in (
                publisher_page(['not-a-uuid']),
                {'data': {'publishersByDistributionPlatform': [
                    {'publisherId': None}]}},
                {'data': {'publishersByDistributionPlatform': ['x']}},
                {'data': {'publishersByDistributionPlatform': {}}},
                {'data': {}},
        ):
            with self.subTest(page=page):
                thoth = MagicMock()
                thoth.client.execute.side_effect = [count_page(1), page]
                with self.assertRaises((
                        ThothPublisherDiscoveryError,
                        ThothGraphQLTransportError)):
                    get_distribution_platform_publisher_ids(thoth, 'CROSSREF')

    def test_invalid_count_fails_closed(self):
        for count in (None, -1, '3', True, {}):
            with self.subTest(count=count):
                thoth = MagicMock()
                thoth.client.execute.return_value = count_page(count)
                with self.assertRaises((
                        ThothPublisherDiscoveryError,
                        ThothGraphQLTransportError)):
                    get_distribution_platform_publisher_ids(thoth, 'CROSSREF')

    def test_publisher_ids_are_normalised_and_sorted(self):
        thoth = MagicMock()
        thoth.client.execute.side_effect = [
            count_page(2),
            publisher_page([PUBLISHER_B.upper(), PUBLISHER_A]),
        ]
        self.assertEqual(
            get_distribution_platform_publisher_ids(thoth, 'CROSSREF'),
            [PUBLISHER_A, PUBLISHER_B],
        )

    def test_zero_count_is_reconciled_against_an_empty_page(self):
        thoth = discovery_client({'CROSSREF': []})
        self.assertEqual(
            get_distribution_platform_publisher_ids(thoth, 'CROSSREF'), [])
        self.assertEqual(thoth.client.execute.call_count, 2)

    def test_zero_count_with_a_non_empty_page_fails_closed(self):
        thoth = MagicMock()
        thoth.client.execute.side_effect = [
            count_page(0), publisher_page([PUBLISHER_A])]
        with self.assertRaises(ThothPublisherDiscoveryError):
            get_distribution_platform_publisher_ids(thoth, 'CROSSREF')

    def test_transport_failure_fails_closed(self):
        thoth = MagicMock()
        thoth.client.execute.side_effect = RuntimeError('connection reset')
        with self.assertRaises(ThothGraphQLTransportError):
            get_distribution_platform_publisher_ids(thoth, 'CROSSREF')

    def test_graphql_errors_are_sanitised_to_message_and_path(self):
        thoth = MagicMock()
        thoth.client.execute.return_value = {
            'errors': [{
                'message': 'useful failure',
                'path': ['publisherCountByDistributionPlatform'],
                'extensions': {'debug': 'internal-secret-detail'},
            }],
        }
        with self.assertRaises(ThothGraphQLResponseError) as raised:
            get_distribution_platform_publisher_ids(thoth, 'CROSSREF')
        self.assertIn('useful failure', str(raised.exception))
        self.assertNotIn('internal-secret-detail', str(raised.exception))


class TestLinkedPlatformReconciliation(unittest.TestCase):

    def test_matching_oapen_and_doab_sets_resolve_to_one_set(self):
        thoth = discovery_client({
            'OAPEN': [PUBLISHER_A, PUBLISHER_B],
            'DOAB': [PUBLISHER_B, PUBLISHER_A],
        })
        self.assertEqual(
            discover_api_publisher_ids(thoth, 'OAPEN'),
            [PUBLISHER_A, PUBLISHER_B],
        )

    def test_both_linked_platforms_are_queried_independently(self):
        thoth = discovery_client({
            'OAPEN': [PUBLISHER_A], 'DOAB': [PUBLISHER_A]})
        discover_api_publisher_ids(thoth, 'OAPEN')
        platforms = {
            call.args[1]['platform']
            for call in thoth.client.execute.call_args_list
        }
        self.assertEqual(platforms, {'OAPEN', 'DOAB'})

    def test_mismatched_linked_sets_are_never_silently_unioned(self):
        thoth = discovery_client({
            'OAPEN': [PUBLISHER_A, PUBLISHER_B],
            'DOAB': [PUBLISHER_A],
        })
        with self.assertRaises(LinkedPlatformMismatchError) as raised:
            discover_api_publisher_ids(thoth, 'OAPEN')
        self.assertEqual(
            raised.exception.differing_publisher_ids, (PUBLISHER_B,))
        self.assertEqual(
            raised.exception.platforms, ('DOAB', 'OAPEN'))

    def test_linked_mismatch_is_reported_as_an_error_not_a_difference(self):
        thoth = discovery_client({
            'OAPEN': [PUBLISHER_A], 'DOAB': [PUBLISHER_B]})
        report = build_comparison_report(thoth, 'OAPEN', [PUBLISHER_A])
        self.assertEqual(report['status'], 'ERROR')
        self.assertEqual(report['apiPublisherIds'], [])
        self.assertEqual(report['apiOnly'], [])
        self.assertEqual(report['legacyOnly'], [])
        self.assertEqual(len(report['linkedPlatformIssues']), 1)
        issue = report['linkedPlatformIssues'][0]
        self.assertEqual(
            issue['issue'], 'linked_platform_publisher_set_mismatch')
        self.assertEqual(issue['platforms'], ['DOAB', 'OAPEN'])
        self.assertEqual(issue['publisherIds'], [PUBLISHER_A, PUBLISHER_B])

    def test_discovery_failure_on_one_linked_platform_fails_closed(self):
        thoth = MagicMock()
        thoth.client.execute.side_effect = [
            count_page(1),
            publisher_page([PUBLISHER_A]),
            RuntimeError('connection reset'),
        ]
        with self.assertRaises(PublisherDiscoveryError):
            discover_api_publisher_ids(thoth, 'OAPEN')


class TestComparisonReport(unittest.TestCase):

    def report(self, assignments, legacy_publisher_ids, platform='OAPEN'):
        return build_comparison_report(
            discovery_client(assignments), platform, legacy_publisher_ids)

    def test_identical_sets_produce_match(self):
        report = self.report(
            {'OAPEN': [PUBLISHER_A, PUBLISHER_B],
             'DOAB': [PUBLISHER_A, PUBLISHER_B]},
            [PUBLISHER_B, PUBLISHER_A],
        )
        self.assertEqual(report['status'], 'MATCH')
        self.assertEqual(report['legacyOnly'], [])
        self.assertEqual(report['apiOnly'], [])
        self.assertEqual(report['schemaVersion'], COMPARISON_SCHEMA_VERSION)
        self.assertEqual(report['mode'], MODE_COMPARE)
        self.assertEqual(report['disseminationPlatform'], 'OAPEN')
        self.assertEqual(report['apiPlatforms'], ['OAPEN', 'DOAB'])
        self.assertEqual(report['contractIssues'], [])
        self.assertEqual(report['linkedPlatformIssues'], [])

    def test_case_differences_are_not_reported_as_differences(self):
        report = self.report(
            {'OAPEN': [PUBLISHER_A], 'DOAB': [PUBLISHER_A]},
            [PUBLISHER_A.upper()],
        )
        self.assertEqual(report['status'], 'MATCH')

    def test_legacy_only_and_api_only_publishers_are_reported(self):
        report = self.report(
            {'OAPEN': [PUBLISHER_B, PUBLISHER_C],
             'DOAB': [PUBLISHER_B, PUBLISHER_C]},
            [PUBLISHER_A, PUBLISHER_B],
        )
        self.assertEqual(report['status'], 'DIFF')
        self.assertEqual(report['legacyOnly'], [PUBLISHER_A])
        self.assertEqual(report['apiOnly'], [PUBLISHER_C])

    def test_publisher_arrays_are_sorted_lexicographically(self):
        report = self.report(
            {'OAPEN': [PUBLISHER_C, PUBLISHER_A],
             'DOAB': [PUBLISHER_A, PUBLISHER_C]},
            [PUBLISHER_C, PUBLISHER_B, PUBLISHER_A],
        )
        for key in (
                'legacyPublisherIds', 'apiPublisherIds', 'legacyOnly',
                'apiOnly'):
            with self.subTest(key=key):
                self.assertEqual(report[key], sorted(report[key]))

    def test_reports_are_deterministic_and_free_of_timestamps(self):
        assignments = {'OAPEN': [PUBLISHER_A], 'DOAB': [PUBLISHER_A]}
        first = self.report(assignments, [PUBLISHER_A])
        second = self.report(assignments, [PUBLISHER_A])
        self.assertEqual(first, second)
        serialised = serialise_comparison_report(first)
        self.assertEqual(serialised, serialise_comparison_report(second))
        self.assertTrue(serialised.endswith('\n'))
        self.assertEqual(json.loads(serialised), first)
        for token in ('generated_at', 'generatedAt', 'timestamp', 'runId'):
            self.assertNotIn(token, serialised)

    def test_report_keys_are_stable_and_use_the_approved_vocabulary(self):
        report = self.report(
            {'OAPEN': [PUBLISHER_A], 'DOAB': [PUBLISHER_A]}, [PUBLISHER_A])
        for key in (
                'schemaVersion', 'mode', 'disseminationPlatform',
                'apiPlatforms', 'legacyPublisherIds', 'apiPublisherIds',
                'legacyOnly', 'apiOnly', 'linkedPlatformIssues',
                'contractIssues', 'status'):
            self.assertIn(key, report)
        serialised = serialise_comparison_report(report)
        self.assertEqual(
            list(json.loads(serialised)), sorted(report))

    def test_destination_classification_is_reported(self):
        report = self.report(
            {'OAPEN': [PUBLISHER_A], 'DOAB': [PUBLISHER_A]}, [PUBLISHER_A])
        self.assertEqual(report['destinationClassifications'], {
            'OAPEN': 'shared_adapter', 'DOAB': 'shared_adapter'})
        self.assertEqual(
            report['apiPlatformPublisherCounts'], {'DOAB': 1, 'OAPEN': 1})

    def test_api_zero_publishers_is_a_clean_comparison(self):
        report = self.report({'OAPEN': [], 'DOAB': []}, [PUBLISHER_A])
        self.assertEqual(report['status'], 'DIFF')
        self.assertEqual(report['apiPublisherIds'], [])
        self.assertEqual(report['legacyOnly'], [PUBLISHER_A])

    def test_discovery_failure_produces_a_sanitised_error_report(self):
        thoth = MagicMock()
        thoth.client.execute.side_effect = RuntimeError(
            'https://user:hunter2@api.test/graphql failed; token=abcdef')
        report = build_comparison_report(thoth, 'Crossref', [PUBLISHER_A])
        self.assertEqual(report['status'], 'ERROR')
        self.assertEqual(report['apiPublisherIds'], [])
        self.assertEqual(report['legacyPublisherIds'], [PUBLISHER_A])
        self.assertEqual(len(report['contractIssues']), 1)
        self.assertEqual(
            report['contractIssues'][0]['issue'], 'api_discovery_failed')
        serialised = serialise_comparison_report(report)
        self.assertNotIn('hunter2', serialised)
        self.assertNotIn('abcdef', serialised)

    def test_error_reports_never_present_a_reconciled_difference(self):
        thoth = MagicMock()
        thoth.client.execute.side_effect = RuntimeError('unavailable')
        report = build_comparison_report(thoth, 'Crossref', [PUBLISHER_A])
        self.assertEqual(report['status'], 'ERROR')
        self.assertEqual(report['legacyOnly'], [])
        self.assertEqual(report['apiOnly'], [])

    def test_unsupported_platform_is_an_error_without_discovery(self):
        thoth = MagicMock()
        report = build_comparison_report(thoth, 'ScienceOpen', [PUBLISHER_A])
        self.assertEqual(report['status'], 'ERROR')
        self.assertEqual(report['apiPlatforms'], [])
        self.assertEqual(
            report['contractIssues'][0]['issue'],
            'unsupported_dissemination_platform',
        )
        thoth.client.execute.assert_not_called()

    def test_unusable_legacy_publisher_configuration_is_an_error(self):
        report = self.report(
            {'OAPEN': [PUBLISHER_A], 'DOAB': [PUBLISHER_A]},
            [PUBLISHER_A, 'not-a-uuid'],
        )
        self.assertEqual(report['status'], 'ERROR')
        self.assertEqual(
            report['contractIssues'][0]['issue'],
            'legacy_publisher_id_not_a_uuid',
        )

    def test_comparison_error_helper_records_the_failure(self):
        report = comparison_report_error(
            'OAPEN', RuntimeError('reporting failed'))
        self.assertEqual(report['status'], 'ERROR')
        self.assertEqual(
            report['contractIssues'][0]['issue'], 'comparison_failed')
        self.assertIn('reporting failed', report['contractIssues'][0]['detail'])

    def test_comparison_performs_no_mutation_or_provider_call(self):
        thoth = discovery_client({'OAPEN': [PUBLISHER_A], 'DOAB': [PUBLISHER_A]})
        with patch('requests.get') as get, patch('requests.post') as post:
            build_comparison_report(thoth, 'OAPEN', [PUBLISHER_A])
        get.assert_not_called()
        post.assert_not_called()
        for call in thoth.client.execute.call_args_list:
            self.assertNotIn('mutation', call.args[0])
        for forbidden in (
                'mutation', 'replacePublisherServiceConfiguration',
                'claimDistributionJobs', 'completeDistributionJob',
                'failDistributionJob'):
            self.assertNotIn(
                forbidden, DISTRIBUTION_PLATFORM_PUBLISHERS_QUERY)
            self.assertNotIn(
                forbidden, DISTRIBUTION_PLATFORM_PUBLISHER_COUNT_QUERY)


class TestSanitisation(unittest.TestCase):

    def test_credentials_and_userinfo_are_redacted(self):
        detail = sanitise_detail(
            'https://user:hunter2@api.test/graphql token=abcdef '
            'password: s3cret api_key=zzz')
        self.assertNotIn('hunter2', detail)
        self.assertNotIn('abcdef', detail)
        self.assertNotIn('s3cret', detail)
        self.assertNotIn('zzz', detail)
        self.assertIn('[redacted]', detail)

    def test_details_are_bounded_and_single_line(self):
        detail = sanitise_detail('a\nb\tc  d ' + 'x' * 5000)
        self.assertLessEqual(len(detail), 300 + len('...(truncated)'))
        self.assertNotIn('\n', detail)
        self.assertIn('a b c d', detail)


class TestComparisonEvidenceTransport(unittest.TestCase):

    def report(self):
        return build_comparison_report(
            discovery_client({'OAPEN': [PUBLISHER_A], 'DOAB': [PUBLISHER_A]}),
            'OAPEN',
            [PUBLISHER_A],
        )

    def test_report_is_written_as_deterministic_sorted_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'nested' / 'publisher-comparison.json'
            report = self.report()
            write_comparison_report(path, report)
            content = path.read_text(encoding='utf-8')
            self.assertEqual(json.loads(content), report)
            self.assertEqual(content, serialise_comparison_report(report))

    def test_summary_reports_counts_and_status(self):
        summary = summarise_comparison(self.report())
        self.assertIn('- Status: `MATCH`', summary)
        self.assertIn('- Legacy publishers: `1`', summary)
        self.assertIn('- API publishers: `1`', summary)
        self.assertIn('- API platforms: `OAPEN, DOAB`', summary)

    def run_summary(self, report=None, modes=None, platform='OAPEN'):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        workdir = Path(temporary_directory.name)
        report_path = workdir / 'publisher-comparison.json'
        summary_path = workdir / 'step-summary'
        if report is not None:
            report_path.write_text(
                report if isinstance(report, str) else json.dumps(report),
                encoding='utf-8')
        environment = {
            **os.environ, 'GITHUB_STEP_SUMMARY': str(summary_path)}
        if modes is None:
            environment.pop('PUBLISHER_SOURCE_MODES', None)
        else:
            environment['PUBLISHER_SOURCE_MODES'] = modes
        result = subprocess.run(
            [
                sys.executable, str(ROOT / 'publisher_source.py'), 'summary',
                '--platform', platform, '--report', str(report_path),
            ],
            cwd=workdir,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        summary = (
            summary_path.read_text(encoding='utf-8')
            if summary_path.exists() else '')
        return result, summary

    def test_env_mode_publishes_no_comparison_evidence(self):
        result, summary = self.run_summary()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(summary, '')

    def test_compare_mode_publishes_the_report_summary(self):
        result, summary = self.run_summary(
            report=self.report(),
            modes=json.dumps({'OAPEN': 'compare'}),
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn('Publisher source (OAPEN)', summary)
        self.assertIn('- Status: `MATCH`', summary)

    def test_missing_or_malformed_evidence_is_visible_but_not_fatal(self):
        for report in (None, 'not-json', '[]'):
            with self.subTest(report=report):
                result, summary = self.run_summary(
                    report=report, modes=json.dumps({'OAPEN': 'compare'}))
                self.assertEqual(result.returncode, 0)
                self.assertIn('UNAVAILABLE', summary)

    def test_malformed_configuration_does_not_fail_the_summary_step(self):
        result, summary = self.run_summary(
            report=self.report(), modes='not-json')
        self.assertEqual(result.returncode, 0)
        self.assertIn('unresolved', summary)

    def test_summary_command_returns_zero_for_api_mode(self):
        result, summary = self.run_summary(
            modes=json.dumps({'OAPEN': 'api'}))
        self.assertEqual(result.returncode, 0)
        self.assertIn('`api`', summary)

    def test_summary_is_written_to_stdout_without_a_step_summary(self):
        report = self.report()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'publisher-comparison.json'
            write_comparison_report(path, report)
            environment = patch.dict(os.environ, {
                'PUBLISHER_SOURCE_MODES': json.dumps({'OAPEN': 'compare'})})
            environment.start()
            self.addCleanup(environment.stop)
            os.environ.pop('GITHUB_STEP_SUMMARY', None)
            from contextlib import redirect_stdout
            from io import StringIO
            stdout = StringIO()
            with redirect_stdout(stdout):
                status = main([
                    'summary', '--platform', 'OAPEN', '--report', str(path)])
        self.assertEqual(status, 0)
        self.assertIn('- Status: `MATCH`', stdout.getvalue())


class TestBulkDisseminateWorkflowTransport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.workflow = load_yaml(BULK_WORKFLOW)

    def selection_step(self):
        return next(
            step for step in self.workflow['jobs']['obtain-new-ids']['steps']
            if step.get('id') == 'get-ids')

    def test_publisher_source_modes_are_read_centrally(self):
        step = self.selection_step()
        self.assertEqual(
            step['env']['PUBLISHER_SOURCE_MODES'],
            '${{ vars.PUBLISHER_SOURCE_MODES }}',
        )
        self.assertEqual(step['env']['ENV_PUBLISHERS'],
                         '${{ inputs.env_publishers }}')
        self.assertEqual(step['env']['ENV_EXCEPTIONS'],
                         '${{ inputs.env_exceptions }}')

    def test_reusable_workflow_inputs_are_unchanged(self):
        self.assertEqual(
            set(self.workflow['on']['workflow_call']['inputs']),
            {'platform', 'env_publishers', 'env_exceptions'},
        )

    def test_selection_writes_the_comparison_report_to_its_own_file(self):
        command = self.selection_step()['run']
        self.assertIn(
            '--comparison-report publisher-comparison.json', command)
        self.assertIn('NEW_IDS=$output', command)

    def test_comparison_evidence_is_published_separately_and_never_gating(
            self):
        steps = self.workflow['jobs']['obtain-new-ids']['steps']
        summary = next(
            step for step in steps
            if step.get('name') == 'Summarise publisher comparison')
        self.assertEqual(summary['if'], '${{ always() }}')
        self.assertTrue(summary['continue-on-error'])
        self.assertIn('publisher_source.py summary', summary['run'])
        self.assertEqual(
            summary['env']['PUBLISHER_SOURCE_MODES'],
            '${{ vars.PUBLISHER_SOURCE_MODES }}',
        )

    def test_comparison_artifact_is_retained_for_30_days(self):
        step = next(
            step for step in self.workflow['jobs']['obtain-new-ids']['steps']
            if step.get('uses', '').startswith('actions/upload-artifact'))
        self.assertEqual(step['if'], '${{ always() }}')
        self.assertTrue(step['continue-on-error'])
        self.assertEqual(step['with']['retention-days'], 30)
        self.assertIn('publisher-comparison.json', step['with']['path'])

    def test_no_credential_is_exposed_to_publisher_discovery(self):
        serialised = json.dumps(self.workflow['jobs']['obtain-new-ids'])
        for credential in (
                'secrets', 'THOTH_PAT', 'ia_s3_access', 'ia_s3_secret',
                'environment'):
            self.assertNotIn(credential, serialised)

    def test_platform_callers_still_need_no_publisher_source_input(self):
        callers = (
            'cr_bulk_disseminate.yml', 'fs_bulk_disseminate.yml',
            'zn_bulk_disseminate.yml', 'cul_bulk_disseminate.yml',
            'gp_bulk_disseminate.yaml', 'bkci_bulk_disseminate.yaml',
            'oapen_bulk_disseminate.yaml', 'eh_bulk_disseminate.yaml',
            'jstor_bulk_disseminate.yaml', 'muse_bulk_disseminate.yaml',
            'pq_bulk_disseminate.yaml',
        )
        for filename in callers:
            with self.subTest(filename=filename):
                workflow = load_yaml(
                    ROOT / '.github' / 'workflows' / filename)
                serialised = json.dumps(workflow)
                self.assertNotIn('PUBLISHER_SOURCE_MODES', serialised)


if __name__ == '__main__':
    unittest.main()
