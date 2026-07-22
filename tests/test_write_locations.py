import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from thothlibrary.mutation import ThothMutation

from thothapi import patch_thoth_client_mutations
from write_locations import (
    AuthenticationError,
    DuplicateLocationsError,
    LocationInput,
    LocationLookupError,
    LocationMutationError,
    MalformedLocationError,
    MissingTokenError,
    PublicationNotFoundError,
    configure_thoth_client,
    main,
    parse_location_line,
    upsert_location,
)


PUBLICATION_ID = '11111111-2222-3333-4444-555555555555'
LANDING_PAGE = 'https://archive.org/details/work-id'
FULL_TEXT_URL = 'https://archive.org/download/work-id/work-id.pdf'
CHECKSUM = '0123456789abcdef0123456789abcdef'


def location_input(**overrides):
    values = {
        'publication_id': PUBLICATION_ID,
        'location_platform': 'INTERNET_ARCHIVE',
        'landing_page': LANDING_PAGE,
        'full_text_url': FULL_TEXT_URL,
        'checksum': CHECKSUM,
        'checksum_algorithm': 'MD5',
    }
    values.update(overrides)
    return LocationInput(**values)


def existing_location(**overrides):
    values = {
        'locationId': 'location-1',
        'publicationId': PUBLICATION_ID,
        'locationPlatform': 'INTERNET_ARCHIVE',
        'landingPage': LANDING_PAGE,
        'fullTextUrl': FULL_TEXT_URL,
        'canonical': False,
        'checksum': CHECKSUM,
        'checksumAlgorithm': 'MD5',
    }
    values.update(overrides)
    return values


def graphql_response(locations):
    return json.dumps({
        'data': {
            'publication': {
                'publicationId': PUBLICATION_ID,
                'locations': locations,
            },
        },
    })


def mock_thoth_with_locations(*responses):
    thoth = MagicMock()
    thoth.client.execute.side_effect = [
        graphql_response(locations) for locations in responses
    ]
    thoth.create_location.return_value = 'created-location'
    thoth.update_location.return_value = 'updated-location'
    return thoth


class TestLocationUpsert(unittest.TestCase):
    def test_no_existing_platform_location_creates(self):
        thoth = mock_thoth_with_locations([])

        result = upsert_location(thoth, location_input())

        self.assertEqual(result, 'created-location')
        thoth.create_location.assert_called_once_with({
            'publicationId': PUBLICATION_ID,
            'landingPage': LANDING_PAGE,
            'fullTextUrl': FULL_TEXT_URL,
            'locationPlatform': 'INTERNET_ARCHIVE',
            'canonical': False,
            'checksum': CHECKSUM,
            'checksumAlgorithm': 'MD5',
        })
        thoth.update_location.assert_not_called()

    def test_identical_location_is_noop(self):
        thoth = mock_thoth_with_locations([existing_location()])

        with patch('write_locations.logging.info') as log_info:
            result = upsert_location(thoth, location_input())

        self.assertIsNone(result)
        thoth.create_location.assert_not_called()
        thoth.update_location.assert_not_called()
        self.assertIn('already current', log_info.call_args.args[0])

    def test_changed_landing_page_updates(self):
        thoth = mock_thoth_with_locations([
            existing_location(landingPage='https://archive.org/details/old')
        ])

        upsert_location(thoth, location_input())

        update = thoth.update_location.call_args.args[0]
        self.assertEqual(update['landingPage'], LANDING_PAGE)
        thoth.create_location.assert_not_called()

    def test_changed_full_text_url_updates(self):
        thoth = mock_thoth_with_locations([
            existing_location(fullTextUrl='https://archive.org/download/old.pdf')
        ])

        upsert_location(thoth, location_input())

        update = thoth.update_location.call_args.args[0]
        self.assertEqual(update['fullTextUrl'], FULL_TEXT_URL)
        thoth.create_location.assert_not_called()

    def test_clearing_full_text_url_sends_explicit_graphql_null(self):
        thoth = mock_thoth_with_locations([existing_location()])

        upsert_location(thoth, location_input(full_text_url=None))

        update = thoth.update_location.call_args.args[0]
        patch_thoth_client_mutations()
        request = ThothMutation('updateLocation', update).request
        self.assertIn('fullTextUrl: null', request)
        self.assertNotIn('fullTextUrl: "null"', request)
        thoth.create_location.assert_not_called()

    def test_changed_checksum_and_algorithm_update(self):
        thoth = mock_thoth_with_locations([
            existing_location(checksum='old', checksumAlgorithm='SHA1')
        ])

        upsert_location(thoth, location_input())

        update = thoth.update_location.call_args.args[0]
        self.assertEqual(update['checksum'], CHECKSUM)
        self.assertEqual(update['checksumAlgorithm'], 'MD5')

    def test_missing_incoming_checksum_preserves_existing_pair(self):
        thoth = mock_thoth_with_locations([
            existing_location(landingPage='https://archive.org/details/old')
        ])

        upsert_location(thoth, location_input(
            checksum=None,
            checksum_algorithm=None,
        ))

        update = thoth.update_location.call_args.args[0]
        self.assertEqual(update['checksum'], CHECKSUM)
        self.assertEqual(update['checksumAlgorithm'], 'MD5')

    def test_no_incoming_or_existing_checksum_is_valid_null_pair(self):
        thoth = mock_thoth_with_locations([
            existing_location(
                landingPage='https://archive.org/details/old',
                checksum=None,
                checksumAlgorithm=None,
            )
        ])

        upsert_location(thoth, location_input(
            checksum=None,
            checksum_algorithm=None,
        ))

        update = thoth.update_location.call_args.args[0]
        self.assertIsNone(update['checksum'])
        self.assertIsNone(update['checksumAlgorithm'])

    def test_incoming_checksum_without_algorithm_is_rejected(self):
        thoth = mock_thoth_with_locations([])

        with self.assertRaisesRegex(MalformedLocationError, 'both checksum'):
            upsert_location(thoth, location_input(checksum_algorithm=None))

        thoth.create_location.assert_not_called()
        thoth.update_location.assert_not_called()

    def test_incoming_algorithm_without_checksum_is_rejected(self):
        thoth = mock_thoth_with_locations([])

        with self.assertRaisesRegex(MalformedLocationError, 'both checksum'):
            upsert_location(thoth, location_input(checksum=None))

        thoth.create_location.assert_not_called()
        thoth.update_location.assert_not_called()

    def test_existing_canonical_status_is_preserved_on_update(self):
        for canonical in (True, False):
            with self.subTest(canonical=canonical):
                thoth = mock_thoth_with_locations([
                    existing_location(
                        landingPage='https://archive.org/details/old',
                        canonical=canonical,
                    )
                ])

                upsert_location(thoth, location_input())

                update = thoth.update_location.call_args.args[0]
                self.assertIs(update['canonical'], canonical)
                self.assertEqual(update['locationId'], 'location-1')
                self.assertEqual(update['publicationId'], PUBLICATION_ID)

    def test_another_platform_does_not_block_creation(self):
        thoth = mock_thoth_with_locations([
            existing_location(
                locationId='oapen-location',
                locationPlatform='OAPEN',
            )
        ])

        upsert_location(thoth, location_input())

        thoth.create_location.assert_called_once()
        thoth.update_location.assert_not_called()

    def test_multiple_platform_locations_fail_with_all_ids(self):
        thoth = mock_thoth_with_locations([
            existing_location(locationId='location-1'),
            existing_location(locationId='location-2'),
        ])

        with self.assertRaises(DuplicateLocationsError) as raised:
            upsert_location(thoth, location_input())

        message = str(raised.exception)
        self.assertIn(PUBLICATION_ID, message)
        self.assertIn('INTERNET_ARCHIVE', message)
        self.assertIn('location-1', message)
        self.assertIn('location-2', message)
        thoth.create_location.assert_not_called()
        thoth.update_location.assert_not_called()

    def test_publication_not_found_is_useful_error(self):
        thoth = MagicMock()
        thoth.client.execute.return_value = json.dumps({
            'errors': [{
                'message': 'No record was found for the given ID.',
                'path': ['publication'],
            }],
            'data': None,
        })

        with self.assertRaisesRegex(
                PublicationNotFoundError, PUBLICATION_ID):
            upsert_location(thoth, location_input())

        thoth.create_location.assert_not_called()
        thoth.update_location.assert_not_called()

    def test_authentication_failure_is_useful_error(self):
        thoth = MagicMock()
        thoth.client.execute.return_value = json.dumps({
            'errors': [{'message': 'Unauthorized'}],
            'data': None,
        })

        with self.assertRaisesRegex(AuthenticationError, 'Unauthorized'):
            upsert_location(thoth, location_input())

        thoth.create_location.assert_not_called()
        thoth.update_location.assert_not_called()

    def test_graphql_transport_failure_preserves_diagnostic(self):
        thoth = MagicMock()
        thoth.client.execute.side_effect = ConnectionError('network down')

        with self.assertRaisesRegex(LocationLookupError, 'network down'):
            upsert_location(thoth, location_input())

    def test_mutation_failure_preserves_diagnostic_and_kind(self):
        thoth = mock_thoth_with_locations([])
        thoth.create_location.side_effect = RuntimeError('validation exploded')

        with self.assertRaisesRegex(
                LocationMutationError,
                'create location mutation failed.*validation exploded'):
            upsert_location(thoth, location_input())

    def test_update_mutation_failure_preserves_diagnostic_and_kind(self):
        thoth = mock_thoth_with_locations([
            existing_location(landingPage='https://archive.org/details/old')
        ])
        thoth.update_location.side_effect = RuntimeError('permission mismatch')

        with self.assertRaisesRegex(
                LocationMutationError,
                'update location mutation failed.*permission mismatch'):
            upsert_location(thoth, location_input())

        thoth.create_location.assert_not_called()

    def test_second_identical_execution_after_create_is_noop(self):
        current = existing_location(locationId='created-location')
        thoth = mock_thoth_with_locations([], [current])

        upsert_location(thoth, location_input())
        upsert_location(thoth, location_input())

        thoth.create_location.assert_called_once()
        thoth.update_location.assert_not_called()

    def test_second_identical_execution_after_update_is_noop(self):
        stale = existing_location(landingPage='https://archive.org/details/old')
        current = existing_location()
        thoth = mock_thoth_with_locations([stale], [current])

        upsert_location(thoth, location_input())
        upsert_location(thoth, location_input())

        thoth.create_location.assert_not_called()
        thoth.update_location.assert_called_once()

    def test_complete_lookup_supplies_checksum_omitted_by_high_level_query(self):
        thoth = mock_thoth_with_locations([
            existing_location(landingPage='https://archive.org/details/old')
        ])

        upsert_location(thoth, location_input(
            checksum=None,
            checksum_algorithm=None,
        ))

        thoth.publication.assert_not_called()
        query = thoth.client.execute.call_args.args[0]
        self.assertIn('checksum\n', query)
        self.assertIn('checksumAlgorithm', query)
        update = thoth.update_location.call_args.args[0]
        self.assertEqual(update['checksum'], CHECKSUM)
        self.assertEqual(update['checksumAlgorithm'], 'MD5')

    def test_incomplete_lookup_fields_are_not_treated_as_null(self):
        incomplete = existing_location()
        del incomplete['checksum']
        thoth = mock_thoth_with_locations([incomplete])

        with self.assertRaisesRegex(LocationLookupError, 'omitted.*checksum'):
            upsert_location(thoth, location_input())

        thoth.create_location.assert_not_called()
        thoth.update_location.assert_not_called()

    def test_lookup_paginates_until_all_locations_are_retrieved(self):
        first_page = [
            existing_location(
                locationId='other-{}'.format(index),
                locationPlatform='OAPEN',
            )
            for index in range(100)
        ]
        requested = existing_location(locationId='requested-location')
        thoth = mock_thoth_with_locations(first_page, [requested])

        upsert_location(thoth, location_input())

        self.assertEqual(thoth.client.execute.call_count, 2)
        first_variables = thoth.client.execute.call_args_list[0].args[1]
        second_variables = thoth.client.execute.call_args_list[1].args[1]
        self.assertEqual(first_variables['offset'], 0)
        self.assertEqual(second_variables['offset'], 100)
        thoth.create_location.assert_not_called()
        thoth.update_location.assert_not_called()


class TestLocationParsingAndConfiguration(unittest.TestCase):
    def test_malformed_line_reports_line_number_and_field_count(self):
        for line in ('one two three', 'one two three four five six seven'):
            with self.subTest(line=line):
                with self.assertRaisesRegex(
                        MalformedLocationError, 'Line 7: expected 6 fields'):
                    parse_location_line(line, 7)

    def test_blank_line_is_ignored(self):
        self.assertIsNone(parse_location_line('  \n', 3))

    def test_literal_none_values_are_parsed(self):
        parsed = parse_location_line(
            '{} INTERNET_ARCHIVE {} None None None'.format(
                PUBLICATION_ID, LANDING_PAGE
            ),
            4,
        )

        self.assertIsNone(parsed.full_text_url)
        self.assertIsNone(parsed.checksum)
        self.assertIsNone(parsed.checksum_algorithm)

    def test_partial_checksum_pair_reports_line_number(self):
        with self.assertRaisesRegex(
                MalformedLocationError, 'Line 9 incoming location'):
            parse_location_line(
                '{} INTERNET_ARCHIVE {} {} {} None'.format(
                    PUBLICATION_ID, LANDING_PAGE, FULL_TEXT_URL, CHECKSUM
                ),
                9,
            )

    @patch('write_locations.get_thoth_client')
    def test_missing_thoth_pat_is_distinct(self, get_client):
        with patch.dict('write_locations.environ', {}, clear=True):
            with self.assertRaisesRegex(MissingTokenError, 'THOTH_PAT'):
                configure_thoth_client()

        get_client.assert_not_called()

    def test_native_booleans_are_rendered_as_graphql_booleans(self):
        patch_thoth_client_mutations()

        mutation = ThothMutation('createLocation', {
            'publicationId': PUBLICATION_ID,
            'landingPage': LANDING_PAGE,
            'fullTextUrl': FULL_TEXT_URL,
            'locationPlatform': 'INTERNET_ARCHIVE',
            'canonical': False,
            'checksum': CHECKSUM,
            'checksumAlgorithm': 'MD5',
        })

        self.assertIn('canonical: false', mutation.request)
        self.assertNotIn('canonical: False', mutation.request)

    def test_main_returns_nonzero_for_malformed_input(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'locations.txt'
            path.write_text('too few fields\n')

            with patch('write_locations.logging.error') as log_error:
                result = main([str(path)])

        self.assertEqual(result, 1)
        self.assertIn('Line 1', str(log_error.call_args))


if __name__ == '__main__':
    unittest.main()
