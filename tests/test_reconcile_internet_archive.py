import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from requests import exceptions as req_except

from errors import (
    DisseminationError,
    InternetArchiveDesiredStateError,
    InternetArchiveVerificationError,
)
from iauploader import IAUploader
from reconcile_internet_archive import (
    InternetArchiveReconciler,
    ReconciliationConfigurationError,
    _base_result,
    main,
    parse_arguments,
    render_report,
    summarise,
    validate_apply_credentials,
)
from uploader import Publication
from version import __version__


WORK_ID = '11111111-2222-3333-4444-555555555555'
WORK_ID_2 = '22222222-3333-4444-5555-666666666666'
WORK_ID_3 = '33333333-4444-5555-6666-777777777777'
PUBLISHER_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
PUBLICATION_ID = '99999999-8888-7777-6666-555555555555'
PDF_BYTES = b'current PDF bytes'
# Canonical sidecar bytes: the deterministic representation produced by
# IAUploader._normalise_json_sidecar (sorted keys, compact separators, single
# trailing newline). Using the canonical form means the raw export and its
# normalisation are byte-identical, so remote originals seeded with these bytes
# still compare as current.
JSON_BYTES = b'{"current":"metadata"}\n'
PDF_MD5 = hashlib.md5(PDF_BYTES).hexdigest()
PDF_NAME = '{}.pdf'.format(WORK_ID)
JSON_NAME = '{}.json'.format(WORK_ID)
LANDING_PAGE = 'https://archive.org/details/{}'.format(WORK_ID)
FULL_TEXT_URL = 'https://archive.org/download/{}/{}'.format(
    WORK_ID, PDF_NAME)


def work_metadata(**overrides):
    work = {
        'workId': WORK_ID,
        'workType': 'MONOGRAPH',
        'workStatus': 'ACTIVE',
        'fullTitle': 'A Test Book',
        'title': 'A Test Book',
        'publicationDate': '2026-01-02',
        'longAbstract': 'A long description',
        'pageCount': 250,
        'lccn': '2026000001',
        'license': 'https://creativecommons.org/licenses/by/4.0/',
        'oclc': '12345',
        'doi': 'https://doi.org/10.0000/test',
        'contributions': [
            {'fullName': 'First Author', 'mainContribution': True},
        ],
        'publications': [{
            'publicationType': 'PDF',
            'publicationId': PUBLICATION_ID,
            'isbn': '978-1-234-56789-0',
            'locations': [{
                'canonical': True,
                'fullTextUrl': 'https://source.example/book.pdf',
            }],
        }],
        'subjects': [{'subjectCode': 'ABC123'}],
        'languages': [{'languageCode': 'eng'}],
        'issues': [],
        'imprint': {
            'publisher': {
                'publisherId': PUBLISHER_ID,
                'publisherName': 'Test Publisher',
            },
        },
    }
    work.update(overrides)
    return {'data': {'work': work}}


def original_file(name, contents):
    return {
        'name': name,
        'source': 'original',
        'md5': hashlib.md5(contents).hexdigest(),
    }


class FakeItem:
    def __init__(
            self, exists=True, metadata=None, files=None,
            identifier_available=True, identifier=WORK_ID):
        self.identifier = identifier
        self.exists = exists
        self.metadata = dict(metadata or {})
        self.files = list(files or [])
        self.refresh = MagicMock()
        self.modify_metadata = MagicMock()
        self.identifier_available = MagicMock(
            return_value=identifier_available)


def desired_metadata(metadata=None):
    uploader = IAUploader.__new__(IAUploader)
    uploader.work_id = WORK_ID
    uploader.version = __version__
    uploader.metadata = metadata or work_metadata()
    return uploader.parse_metadata()


def current_item(metadata=None, files=None):
    return FakeItem(
        metadata=metadata or desired_metadata(),
        files=files or [
            original_file(PDF_NAME, PDF_BYTES),
            original_file(JSON_NAME, JSON_BYTES),
        ],
    )


def current_location(**overrides):
    location = {
        'locationId': 'location-1',
        'publicationId': PUBLICATION_ID,
        'locationPlatform': 'INTERNET_ARCHIVE',
        'landingPage': LANDING_PAGE,
        'fullTextUrl': FULL_TEXT_URL,
        'canonical': False,
        'checksum': PDF_MD5,
        'checksumAlgorithm': 'MD5',
    }
    location.update(overrides)
    return location


class InspectionHarness:
    def inspect(self, item=None, locations=None, metadata=None,
                json_bytes=JSON_BYTES, pdf_bytes=PDF_BYTES,
                json_error=None, pdf_error=None):
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(metadata or work_metadata())
        reconciler = InternetArchiveReconciler(thoth=thoth)
        item = item if item is not None else current_item(
            metadata=desired_metadata(metadata))
        locations = [current_location()] if locations is None else locations
        json_side_effect = json_error
        pdf_side_effect = pdf_error
        publication = Publication(
            'PDF', PUBLICATION_ID, pdf_bytes, '.pdf',
            'https://source.example/book.pdf')
        with patch.object(
                IAUploader, 'get_formatted_metadata',
                return_value=json_bytes, side_effect=json_side_effect), \
                patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=publication, side_effect=pdf_side_effect), \
                patch(
                    'reconcile_internet_archive.get_item',
                    return_value=item) as get_item, \
                patch(
                    'reconcile_internet_archive.retrieve_existing_locations',
                    return_value=locations) as get_locations:
            result, context = reconciler.inspect_work(WORK_ID)
        return result, context, get_item, get_locations


class TestOwnershipPreflight(unittest.TestCase):
    def _inspect(
            self, item, metadata=None, locations=None, json_error=None,
            pdf_error=None, apply=False):
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(metadata or work_metadata())
        reconciler = InternetArchiveReconciler(thoth=thoth)
        events = []
        publication = Publication(
            'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
            'https://source.example/book.pdf')
        original_classify = IAUploader.classify_item_ownership
        original_build = IAUploader.build_desired_state

        def get_archive_item(identifier):
            events.append('get_item')
            self.assertEqual(identifier, WORK_ID)
            if isinstance(item, Exception):
                raise item
            return item

        def classify(uploader, archive_item):
            events.append('classify_ownership')
            return original_classify(uploader, archive_item)

        def build_desired(uploader):
            events.append('build_desired_state')
            return original_build(uploader)

        def get_json(_uploader, _format):
            events.append('get_json_export')
            if json_error is not None:
                raise json_error
            return JSON_BYTES

        def get_pdf(_uploader, _publication_type):
            events.append('download_pdf')
            if pdf_error is not None:
                raise pdf_error
            return publication

        with patch(
                'reconcile_internet_archive.get_item',
                side_effect=get_archive_item) as get_item_mock, patch.object(
                    IAUploader, 'classify_item_ownership',
                    autospec=True, side_effect=classify), patch.object(
                    IAUploader, 'build_desired_state',
                    autospec=True, side_effect=build_desired) as build, \
                patch.object(
                    IAUploader, 'get_formatted_metadata',
                    autospec=True, side_effect=get_json) as get_json_mock, \
                patch.object(
                    IAUploader, 'get_publication_details',
                    autospec=True, side_effect=get_pdf) as get_pdf_mock, patch(
                    'reconcile_internet_archive.retrieve_existing_locations',
                    return_value=(
                        [current_location()] if locations is None else locations
                    )) as get_locations, patch.object(
                    IAUploader, 'apply_archive_repairs') as archive_repair, \
                patch(
                    'reconcile_internet_archive.upsert_location') as upsert:
            if apply:
                result = reconciler.reconcile_one(
                    WORK_ID, apply=True, credentials=CREDENTIALS)
                context = None
            else:
                result, context = reconciler.inspect_work(WORK_ID)

        return SimpleNamespace(
            result=result,
            context=context,
            events=events,
            get_item=get_item_mock,
            build=build,
            get_json=get_json_mock,
            get_pdf=get_pdf_mock,
            get_locations=get_locations,
            archive_repair=archive_repair,
            upsert=upsert,
        )

    def test_conflicting_marker_stops_before_desired_state_and_sources(self):
        metadata = desired_metadata()
        metadata['thoth-work-id'] = 'another-work'
        inspected = self._inspect(
            FakeItem(exists=True, metadata=metadata),
            locations=[],
        )

        self.assertEqual(
            inspected.events, ['get_item', 'classify_ownership'])
        self.assertIn('identifier_collision', inspected.result['issues'])
        self.assertEqual(
            inspected.result['recommended_actions'], [
                'resolve_identifier_collision', 'create_thoth_location'])
        self.assertEqual(inspected.result['auto_applicable_actions'], [])
        inspected.get_item.assert_called_once_with(WORK_ID)
        inspected.build.assert_not_called()
        inspected.get_json.assert_not_called()
        inspected.get_pdf.assert_not_called()

    def test_unavailable_missing_identifier_checks_once_before_sources(self):
        item = FakeItem(exists=False, identifier_available=False)

        inspected = self._inspect(item, locations=[])

        self.assertEqual(
            inspected.events, ['get_item', 'classify_ownership'])
        item.identifier_available.assert_called_once_with()
        self.assertIn('identifier_collision', inspected.result['issues'])
        self.assertNotIn('item_missing', inspected.result['issues'])
        inspected.build.assert_not_called()
        inspected.get_json.assert_not_called()
        inspected.get_pdf.assert_not_called()

    def test_availability_exception_stops_sources_as_archive_failure(self):
        item = FakeItem(exists=False)
        item.identifier_available.side_effect = RuntimeError(
            'availability endpoint failed')

        inspected = self._inspect(item, locations=[])

        self.assertIn('archive_request_failed', inspected.result['issues'])
        self.assertIn('availability endpoint failed', inspected.result['error'])
        self.assertEqual(inspected.result['auto_applicable_actions'], [])
        inspected.build.assert_not_called()
        inspected.get_json.assert_not_called()
        inspected.get_pdf.assert_not_called()

    def test_archive_request_failure_stops_sources(self):
        inspected = self._inspect(
            RuntimeError('Archive request failed'),
            locations=[],
        )

        self.assertIn('archive_request_failed', inspected.result['issues'])
        self.assertIn('Archive request failed', inspected.result['error'])
        self.assertEqual(inspected.result['auto_applicable_actions'], [])
        inspected.build.assert_not_called()
        inspected.get_json.assert_not_called()
        inspected.get_pdf.assert_not_called()

    def test_invalid_availability_response_stops_sources(self):
        item = FakeItem(exists=False, identifier_available='unknown')

        inspected = self._inspect(item, locations=[])

        self.assertIn('archive_request_failed', inspected.result['issues'])
        self.assertIn('invalid response', inspected.result['error'])
        item.identifier_available.assert_called_once_with()
        inspected.build.assert_not_called()
        inspected.get_json.assert_not_called()
        inspected.get_pdf.assert_not_called()

    def test_owned_item_skips_availability_then_builds_desired_state(self):
        item = current_item()

        inspected = self._inspect(item)

        self.assertEqual(inspected.events[:3], [
            'get_item', 'classify_ownership', 'build_desired_state'])
        item.identifier_available.assert_not_called()
        inspected.get_item.assert_called_once_with(WORK_ID)
        inspected.build.assert_called_once()
        self.assertEqual(inspected.result['status'], 'current')

    def test_legacy_item_skips_availability_then_builds_desired_state(self):
        metadata = desired_metadata()
        metadata.pop('thoth-work-id')
        item = current_item(metadata=metadata)

        inspected = self._inspect(item)

        self.assertEqual(inspected.events[:3], [
            'get_item', 'classify_ownership', 'build_desired_state'])
        item.identifier_available.assert_not_called()
        self.assertTrue(
            inspected.result['internet_archive']['accepted_legacy_item'])

    def test_available_missing_identifier_builds_after_single_check(self):
        item = FakeItem(exists=False, identifier_available=True)

        inspected = self._inspect(item, locations=[])

        self.assertEqual(inspected.events[:3], [
            'get_item', 'classify_ownership', 'build_desired_state'])
        item.identifier_available.assert_called_once_with()
        inspected.get_item.assert_called_once_with(WORK_ID)
        self.assertIn('item_missing', inspected.result['issues'])
        self.assertIn(
            'create_archive_item', inspected.result['auto_applicable_actions'])

    def test_pdf_failure_preserves_ownership_and_location_inventory(self):
        item = current_item()

        inspected = self._inspect(
            item,
            pdf_error=DisseminationError('broken PDF URL'),
        )

        self.assertIn('pdf_source_unavailable', inspected.result['issues'])
        self.assertEqual(
            inspected.result['internet_archive']['ownership'], 'owned')
        self.assertIsNone(inspected.result['internet_archive']['expected'])
        self.assertEqual(inspected.result['thoth_location']['state'], 'observed')
        self.assertEqual(inspected.result['auto_applicable_actions'], [])
        inspected.get_item.assert_called_once_with(WORK_ID)

    def test_json_failure_preserves_ownership_and_location_inventory(self):
        item = current_item()

        inspected = self._inspect(
            item,
            json_error=DisseminationError('export unavailable'),
        )

        self.assertIn('json_export_unavailable', inspected.result['issues'])
        self.assertEqual(
            inspected.result['internet_archive']['ownership'], 'owned')
        self.assertEqual(inspected.result['thoth_location']['count'], 1)
        self.assertEqual(inspected.result['auto_applicable_actions'], [])
        inspected.get_item.assert_called_once_with(WORK_ID)

    def test_collision_observes_existing_location_without_auto_action(self):
        metadata = desired_metadata()
        metadata['thoth-work-id'] = 'another-work'

        inspected = self._inspect(
            FakeItem(exists=True, metadata=metadata),
            locations=[current_location()],
        )

        self.assertEqual(inspected.result['thoth_location']['state'], 'observed')
        self.assertEqual(
            inspected.result['recommended_actions'],
            ['resolve_identifier_collision'])
        self.assertEqual(inspected.result['auto_applicable_actions'], [])

    def test_apply_collision_performs_zero_archive_or_location_mutations(self):
        metadata = desired_metadata()
        metadata['thoth-work-id'] = 'another-work'
        item = FakeItem(exists=True, metadata=metadata)

        inspected = self._inspect(item, locations=[], apply=True)

        self.assertEqual(inspected.result['status'], 'identifier_collision')
        self.assertEqual(inspected.result['attempted_actions'], [])
        inspected.archive_repair.assert_not_called()
        inspected.upsert.assert_not_called()
        item.modify_metadata.assert_not_called()

    def test_mixed_batch_skips_collided_broken_source_and_continues(self):
        first_metadata = work_metadata()
        first_metadata['data']['work']['publications'][0]['locations'][0][
            'fullTextUrl'] = 'https://broken.example/book.pdf'
        second_metadata = work_metadata(workId=WORK_ID_2)
        thoth = MagicMock()
        thoth.work_by_id.side_effect = [
            json.dumps(first_metadata),
            json.dumps(second_metadata),
        ]
        reconciler = InternetArchiveReconciler(thoth=thoth)
        collided_metadata = desired_metadata()
        collided_metadata['thoth-work-id'] = 'another-work'
        items = {
            WORK_ID: FakeItem(exists=True, metadata=collided_metadata),
            WORK_ID_2: FakeItem(
                exists=False, identifier_available=True, identifier=WORK_ID_2),
        }

        with patch(
                'reconcile_internet_archive.get_item',
                side_effect=lambda identifier: items[identifier]), patch.object(
                    IAUploader, 'get_formatted_metadata',
                    return_value=JSON_BYTES) as get_json, patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=Publication(
                        'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
                        'https://source.example/book.pdf')) as get_pdf, patch(
                    'reconcile_internet_archive.retrieve_existing_locations',
                    return_value=[]):
            results = reconciler.reconcile([WORK_ID, WORK_ID_2])

        self.assertEqual(results[0]['status'], 'identifier_collision')
        self.assertEqual(results[1]['status'], 'item_missing')
        self.assertEqual(get_json.call_count, 1)
        self.assertEqual(get_pdf.call_count, 1)
        items[WORK_ID].identifier_available.assert_not_called()
        items[WORK_ID_2].identifier_available.assert_called_once_with()


class TestSelectionAndCLI(unittest.TestCase):
    def test_publisher_selection_is_stable_before_limit_and_offset(self):
        thoth = MagicMock()
        thoth.publisher.return_value = SimpleNamespace(publisherId=PUBLISHER_ID)
        thoth.works.return_value = [
            SimpleNamespace(
                workId=WORK_ID_3,
                publications=[SimpleNamespace(publicationType='PDF')]),
            SimpleNamespace(
                workId=WORK_ID,
                publications=[SimpleNamespace(publicationType='PDF')]),
            SimpleNamespace(
                workId=WORK_ID_2,
                publications=[SimpleNamespace(publicationType='PDF')]),
        ]
        reconciler = InternetArchiveReconciler(thoth=thoth)

        selected = reconciler.publisher_work_ids(PUBLISHER_ID, 1, 1)

        self.assertEqual(selected, [WORK_ID_2])
        kwargs = thoth.works.call_args.kwargs
        self.assertEqual(kwargs['work_statuses'], '[ACTIVE]')
        self.assertNotIn('CHAPTER', kwargs['work_types'])

    def test_repeatable_explicit_work_ids_are_parsed(self):
        arguments = parse_arguments([
            '--work-id', WORK_ID, '--work-id', WORK_ID_2])

        self.assertEqual(arguments.work_id, [WORK_ID, WORK_ID_2])

    def test_combined_selection_is_deduplicated_and_sorted(self):
        reconciler = InternetArchiveReconciler(thoth=MagicMock())
        with patch.object(
                reconciler, 'publisher_work_ids',
                return_value=[WORK_ID_2, WORK_ID]):
            selected = reconciler.select_work_ids(
                PUBLISHER_ID, [WORK_ID_2, WORK_ID_3], 100, 0)

        self.assertEqual(selected, [WORK_ID, WORK_ID_2, WORK_ID_3])
        self.assertEqual(reconciler.selection_by_work_id[WORK_ID_2], {
            'explicit': True,
            'publisher': True,
        })
        self.assertEqual(reconciler.selection_by_work_id[WORK_ID], {
            'explicit': False,
            'publisher': True,
        })
        self.assertEqual(reconciler.selection_by_work_id[WORK_ID_3], {
            'explicit': True,
            'publisher': False,
        })

    def test_invalid_uuid_is_rejected(self):
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                parse_arguments(['--work-id', 'not-a-uuid'])
        self.assertEqual(raised.exception.code, 2)

    def test_missing_selection_criteria_is_rejected(self):
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                parse_arguments([])
        self.assertEqual(raised.exception.code, 2)

    def test_invalid_limit_is_rejected(self):
        for value in ('0', '-1'):
            with self.subTest(value=value), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    parse_arguments(['--work-id', WORK_ID, '--limit', value])

    def test_invalid_offset_is_rejected(self):
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parse_arguments(['--work-id', WORK_ID, '--offset', '-1'])

    def test_explicit_ineligible_work_is_reported(self):
        result, _, _, _ = InspectionHarness().inspect(
            metadata=work_metadata(workStatus='DRAFT'))

        self.assertFalse(result['eligible'])
        self.assertIn('ineligible_status', result['issues'])
        self.assertTrue(result['internet_archive']['exists'])

    def test_explicit_inactive_work_with_pdf_is_remotely_inspected(self):
        result, _, get_item, get_locations = InspectionHarness().inspect(
            metadata=work_metadata(workStatus='DRAFT'))

        self.assertFalse(result['eligible'])
        self.assertTrue(result['selection']['explicit'])
        self.assertIn('ineligible_status', result['issues'])
        get_item.assert_called_once_with(WORK_ID)
        get_locations.assert_called_once()

    def test_explicit_unsupported_work_with_pdf_is_remotely_inspected(self):
        result, _, get_item, get_locations = InspectionHarness().inspect(
            metadata=work_metadata(workType='CHAPTER'))

        self.assertFalse(result['eligible'])
        self.assertIn('unsupported_work_type', result['issues'])
        get_item.assert_called_once_with(WORK_ID)
        get_locations.assert_called_once()

    def test_publisher_selection_excludes_works_without_pdf(self):
        thoth = MagicMock()
        thoth.publisher.return_value = object()
        thoth.works.return_value = [
            SimpleNamespace(workId=WORK_ID, publications=[]),
            SimpleNamespace(
                workId=WORK_ID_2,
                publications=[SimpleNamespace(publicationType='EPUB')]),
            SimpleNamespace(
                workId=WORK_ID_3,
                publications=[SimpleNamespace(publicationType='PDF')]),
        ]

        selected = InternetArchiveReconciler(
            thoth=thoth).publisher_work_ids(PUBLISHER_ID, 100, 0)

        self.assertEqual(selected, [WORK_ID_3])

    def test_unknown_publisher_fails_initial_selection(self):
        thoth = MagicMock()
        thoth.publisher.side_effect = RuntimeError('not found')

        with self.assertRaises(ReconciliationConfigurationError):
            InternetArchiveReconciler(thoth=thoth).publisher_work_ids(
                PUBLISHER_ID, 100, 0)


class TestDryRunInspection(unittest.TestCase):
    def setUp(self):
        self.harness = InspectionHarness()

    def test_current_work(self):
        result, _, _, _ = self.harness.inspect()

        self.assertEqual(result['status'], 'current')
        self.assertEqual(result['issues'], [])

    def test_missing_archive_item(self):
        item = FakeItem(exists=False)
        result, _, _, _ = self.harness.inspect(item=item, locations=[])

        self.assertEqual(result['status'], 'item_missing')
        self.assertTrue(result['internet_archive']['identifier_available'])
        item.identifier_available.assert_called_once_with()
        self.assertEqual(result['recommended_actions'], [
            'create_archive_item',
            'upload_pdf_original',
            'upload_json_original',
            'create_thoth_location',
        ])
        self.assertEqual(
            result['auto_applicable_actions'], result['recommended_actions'])

    def test_unavailable_identifier_is_collision_not_missing(self):
        item = FakeItem(exists=False, identifier_available=False)

        result, _, _, _ = self.harness.inspect(item=item, locations=[])

        self.assertEqual(result['status'], 'identifier_collision')
        self.assertIn('identifier_collision', result['issues'])
        self.assertNotIn('item_missing', result['issues'])
        self.assertFalse(
            result['internet_archive']['identifier_available'])
        self.assertIn(
            'no public item metadata was available',
            result['internet_archive']['ownership_reason'],
        )
        self.assertEqual(result['auto_applicable_actions'], [])
        item.identifier_available.assert_called_once_with()

    def test_identifier_availability_failure_is_archive_request_error(self):
        item = FakeItem(exists=False)
        item.identifier_available.side_effect = RuntimeError(
            'availability endpoint failed')

        result, _, _, _ = self.harness.inspect(item=item, locations=[])

        self.assertIn('archive_request_failed', result['issues'])
        self.assertNotIn('item_missing', result['issues'])
        self.assertIn('availability endpoint failed', result['error'])
        self.assertEqual(result['auto_applicable_actions'], [])

    def test_unavailable_identifier_short_circuits_source_failure(self):
        item = FakeItem(exists=False, identifier_available=False)

        result, _, _, _ = self.harness.inspect(
            item=item,
            locations=[],
            pdf_error=DisseminationError('PDF unavailable'),
        )

        self.assertIn('identifier_collision', result['issues'])
        self.assertNotIn('pdf_source_unavailable', result['issues'])
        self.assertNotIn('item_missing', result['issues'])
        self.assertFalse(
            result['internet_archive']['identifier_available'])
        self.assertEqual(result['auto_applicable_actions'], [])
        item.identifier_available.assert_called_once_with()

    def test_missing_pdf_original(self):
        result, _, _, _ = self.harness.inspect(item=current_item(files=[
            original_file(JSON_NAME, JSON_BYTES)]))

        self.assertEqual(result['status'], 'item_incomplete')
        self.assertIn('missing_pdf_original', result['issues'])

    def test_missing_json_original(self):
        result, _, _, _ = self.harness.inspect(item=current_item(files=[
            original_file(PDF_NAME, PDF_BYTES)]))

        self.assertIn('missing_json_original', result['issues'])

    def test_stale_pdf_original(self):
        result, _, _, _ = self.harness.inspect(item=current_item(files=[
            original_file(PDF_NAME, b'old'),
            original_file(JSON_NAME, JSON_BYTES),
        ]))

        self.assertEqual(result['status'], 'files_stale')
        self.assertIn('upload_pdf_original', result['recommended_actions'])

    def test_stale_json_original(self):
        result, _, _, _ = self.harness.inspect(item=current_item(files=[
            original_file(PDF_NAME, PDF_BYTES),
            original_file(JSON_NAME, b'old'),
        ]))

        self.assertIn('stale_json_original', result['issues'])

    def test_stale_metadata(self):
        metadata = desired_metadata()
        metadata['title'] = 'Old title'
        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertEqual(result['status'], 'metadata_stale')
        self.assertIn('title', result['internet_archive']['metadata'][
            'patch_fields'])

    def test_immutable_mediatype_conflict_is_reported_for_manual_action(self):
        metadata = desired_metadata()
        metadata['mediatype'] = 'data'

        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertEqual(result['status'], 'metadata_conflict')
        self.assertIn(
            'archive_immutable_metadata_conflict', result['issues'])
        self.assertIn(
            'resolve_archive_immutable_metadata',
            result['recommended_actions'],
        )
        archive_metadata = result['internet_archive']['metadata']
        self.assertEqual(archive_metadata['mutable_problems'], [])
        self.assertEqual(archive_metadata['immutable_problems'], [
            "mediatype is 'data', expected 'texts'",
        ])
        self.assertNotIn('mediatype', archive_metadata['patch_fields'])

    def test_immutable_only_conflict_is_not_mutable_metadata_drift(self):
        metadata = desired_metadata()
        metadata['mediatype'] = 'data'

        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertNotIn('archive_metadata_stale', result['issues'])
        self.assertNotIn(
            'update_archive_metadata', result['recommended_actions'])

    def test_immutable_and_mutable_metadata_drift_retain_both_actions(self):
        metadata = desired_metadata()
        metadata['mediatype'] = 'data'
        metadata['title'] = 'Old title'

        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertIn(
            'archive_immutable_metadata_conflict', result['issues'])
        self.assertIn('archive_metadata_stale', result['issues'])
        self.assertEqual(result['recommended_actions'], [
            'resolve_archive_immutable_metadata',
            'update_archive_metadata',
        ])
        archive_metadata = result['internet_archive']['metadata']
        self.assertEqual(archive_metadata['immutable_problems'], [
            "mediatype is 'data', expected 'texts'",
        ])
        self.assertIn('title', archive_metadata['patch_fields'])
        self.assertNotIn('mediatype', archive_metadata['patch_fields'])

    def test_immutable_conflict_retains_missing_file_recommendations(self):
        metadata = desired_metadata()
        metadata['mediatype'] = 'data'

        result, _, _, _ = self.harness.inspect(item=FakeItem(
            exists=True,
            metadata=metadata,
            files=[],
        ))

        self.assertEqual(result['recommended_actions'], [
            'resolve_archive_immutable_metadata',
            'upload_pdf_original',
            'upload_json_original',
        ])
        self.assertIn('missing_pdf_original', result['issues'])
        self.assertIn('missing_json_original', result['issues'])

    def test_immutable_conflict_retains_missing_location_recommendation(self):
        metadata = desired_metadata()
        metadata['mediatype'] = 'data'

        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata),
            locations=[],
        )

        self.assertEqual(result['recommended_actions'], [
            'resolve_archive_immutable_metadata',
            'create_thoth_location',
        ])
        self.assertIn('location_missing', result['issues'])

    def test_immutable_conflict_blocks_all_auto_applicable_actions(self):
        metadata = desired_metadata()
        metadata['mediatype'] = 'data'
        metadata['title'] = 'Old title'

        result, _, _, _ = self.harness.inspect(
            item=FakeItem(exists=True, metadata=metadata, files=[]),
            locations=[],
        )

        self.assertEqual(result['auto_applicable_actions'], [])
        self.assertIn('update_archive_metadata', result[
            'recommended_actions'])
        self.assertIn('upload_pdf_original', result['recommended_actions'])
        self.assertIn('create_thoth_location', result[
            'recommended_actions'])

    def test_correct_mediatype_with_mutable_drift_is_auto_repairable(self):
        metadata = desired_metadata()
        metadata['title'] = 'Old title'

        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertEqual(result['status'], 'metadata_stale')
        self.assertEqual(
            result['auto_applicable_actions'],
            ['update_archive_metadata'],
        )
        self.assertEqual(
            result['internet_archive']['metadata']['immutable_problems'], [])

    def test_immutable_conflict_report_is_deterministic_across_inspections(self):
        metadata = desired_metadata()
        metadata['mediatype'] = 'data'
        item = current_item(metadata=metadata)

        first, _, _, _ = self.harness.inspect(item=item, locations=[])
        second, _, _, _ = self.harness.inspect(item=item, locations=[])

        self.assertEqual(first, second)

    def test_collection_conflict_is_reported_as_admin_only_manual_action(self):
        metadata = desired_metadata()
        metadata.pop('collection')

        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertEqual(result['status'], 'metadata_conflict')
        self.assertIn(
            'archive_collection_membership_conflict', result['issues'])
        self.assertIn(
            'resolve_archive_collection_membership',
            result['recommended_actions'],
        )
        archive_metadata = result['internet_archive']['metadata']
        self.assertEqual(archive_metadata['mutable_problems'], [])
        self.assertEqual(archive_metadata['initial_only_problems'], [])
        self.assertEqual(archive_metadata['immutable_problems'], [])
        self.assertEqual(archive_metadata['admin_only_problems'], [
            "collection is None, expected to include "
            "'thoth-archiving-network'",
        ])
        self.assertEqual(
            archive_metadata['restricted_problems'],
            archive_metadata['admin_only_problems'],
        )
        self.assertNotIn('collection', archive_metadata['patch_fields'])

    def test_collection_only_conflict_is_not_mutable_metadata_drift(self):
        metadata = desired_metadata()
        metadata['collection'] = 'unrelated-collection'

        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertNotIn('archive_metadata_stale', result['issues'])
        self.assertNotIn(
            'update_archive_metadata', result['recommended_actions'])
        self.assertEqual(result['recommended_actions'], [
            'resolve_archive_collection_membership',
        ])

    def test_collection_and_mutable_drift_retain_both_actions(self):
        metadata = desired_metadata()
        metadata.pop('collection')
        metadata['title'] = 'Old title'

        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertEqual(result['recommended_actions'], [
            'resolve_archive_collection_membership',
            'update_archive_metadata',
        ])
        self.assertIn('archive_metadata_stale', result['issues'])
        self.assertEqual(
            result['internet_archive']['metadata']['patch_fields'], ['title'])

    def test_collection_conflict_retains_file_recommendations(self):
        metadata = desired_metadata()
        metadata['collection'] = []

        result, _, _, _ = self.harness.inspect(item=FakeItem(
            exists=True,
            metadata=metadata,
            files=[],
        ))

        self.assertEqual(result['recommended_actions'], [
            'resolve_archive_collection_membership',
            'upload_pdf_original',
            'upload_json_original',
        ])
        self.assertIn('missing_pdf_original', result['issues'])
        self.assertIn('missing_json_original', result['issues'])

    def test_collection_conflict_retains_location_recommendation(self):
        metadata = desired_metadata()
        metadata['collection'] = 'unrelated-collection'

        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata),
            locations=[],
        )

        self.assertEqual(result['recommended_actions'], [
            'resolve_archive_collection_membership',
            'create_thoth_location',
        ])
        self.assertIn('location_missing', result['issues'])

    def test_collection_conflict_blocks_all_auto_applicable_actions(self):
        metadata = desired_metadata()
        metadata.pop('collection')
        metadata['title'] = 'Old title'

        result, _, _, _ = self.harness.inspect(
            item=FakeItem(exists=True, metadata=metadata, files=[]),
            locations=[],
        )

        self.assertEqual(result['auto_applicable_actions'], [])
        self.assertIn(
            'resolve_archive_collection_membership',
            result['recommended_actions'],
        )
        self.assertIn(
            'update_archive_metadata', result['recommended_actions'])
        self.assertIn('upload_pdf_original', result['recommended_actions'])
        self.assertIn(
            'create_thoth_location', result['recommended_actions'])

    def test_correct_collection_with_mutable_drift_is_auto_repairable(self):
        metadata = desired_metadata()
        metadata['collection'] = [
            'unrelated-collection', IAUploader.THOTH_COLLECTION]
        metadata['title'] = 'Old title'

        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertEqual(result['status'], 'metadata_stale')
        self.assertEqual(
            result['auto_applicable_actions'],
            ['update_archive_metadata'],
        )
        self.assertEqual(
            result['internet_archive']['metadata']['admin_only_problems'], [])
        self.assertEqual(
            result['internet_archive']['metadata']['patch_fields'], ['title'])

    def test_collection_conflict_report_is_deterministic(self):
        metadata = desired_metadata()
        metadata['collection'] = 'unrelated-collection'
        item = current_item(metadata=metadata)

        first, _, _, _ = self.harness.inspect(item=item, locations=[])
        second, _, _, _ = self.harness.inspect(item=item, locations=[])

        self.assertEqual(first, second)

    def test_multiple_archive_discrepancies_are_ordered(self):
        metadata = desired_metadata()
        metadata['title'] = 'Old title'
        result, _, _, _ = self.harness.inspect(item=current_item(
            metadata=metadata,
            files=[original_file(PDF_NAME, PDF_BYTES)],
        ))

        self.assertEqual(result['status'], 'item_incomplete')
        self.assertEqual(result['issues'], [
            'missing_json_original', 'archive_metadata_stale'])

    def test_accepted_legacy_item_is_reported(self):
        metadata = desired_metadata()
        metadata.pop('thoth-work-id')
        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertTrue(result['internet_archive']['accepted_legacy_item'])
        self.assertIn('update_archive_metadata', result['recommended_actions'])

    def test_conflicting_work_marker_is_collision(self):
        metadata = desired_metadata()
        metadata['thoth-work-id'] = 'another-work'
        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata), locations=[])

        self.assertEqual(result['status'], 'identifier_collision')
        self.assertEqual(
            result['recommended_actions'], [
                'resolve_identifier_collision', 'create_thoth_location'])
        self.assertEqual(result['auto_applicable_actions'], [])
        self.assertIn('location_missing', result['issues'])

    def test_ineligible_work_retains_remote_issues_and_has_no_auto_actions(self):
        result, _, _, _ = self.harness.inspect(
            metadata=work_metadata(workStatus='DRAFT'),
            item=FakeItem(exists=False),
            locations=[],
        )

        self.assertIn('ineligible_status', result['issues'])
        self.assertIn('item_missing', result['issues'])
        self.assertIn('location_missing', result['issues'])
        self.assertEqual(result['recommended_actions'], [
            'fix_work_eligibility',
            'create_archive_item',
            'upload_pdf_original',
            'upload_json_original',
            'create_thoth_location',
        ])
        self.assertEqual(result['auto_applicable_actions'], [])

    def test_unknown_ownership_is_collision(self):
        metadata = desired_metadata()
        metadata.pop('thoth-work-id')
        metadata['collection'] = 'not-thoth'
        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertEqual(result['status'], 'identifier_collision')

    def test_pdf_source_unavailable(self):
        result, _, get_item, get_locations = self.harness.inspect(
            pdf_error=DisseminationError('download failed'))

        self.assertEqual(result['status'], 'source_unavailable')
        self.assertEqual(result['issues'], ['pdf_source_unavailable'])
        get_item.assert_called_once_with(WORK_ID)
        get_locations.assert_called_once()

    def test_json_export_unavailable(self):
        result, _, _, _ = self.harness.inspect(
            json_error=DisseminationError('export failed'))

        self.assertEqual(result['issues'], ['json_export_unavailable'])

    def test_missing_thoth_location(self):
        result, _, _, _ = self.harness.inspect(locations=[])

        self.assertEqual(result['status'], 'location_missing')
        self.assertEqual(result['thoth_location']['count'], 0)

    def test_stale_thoth_location(self):
        result, _, _, _ = self.harness.inspect(locations=[
            current_location(landingPage='https://archive.org/details/old')])

        self.assertEqual(result['status'], 'location_stale')
        self.assertEqual(result['thoth_location']['location_id'], 'location-1')

    def test_duplicate_thoth_locations(self):
        result, _, _, _ = self.harness.inspect(locations=[
            current_location(locationId='one'),
            current_location(locationId='two'),
        ])

        self.assertEqual(result['status'], 'duplicate_locations')
        self.assertEqual(
            result['recommended_actions'], ['resolve_duplicate_locations'])

    def test_duplicate_locations_retain_independent_archive_recommendation(self):
        result, _, _, _ = self.harness.inspect(
            item=current_item(files=[
                original_file(PDF_NAME, b'old'),
                original_file(JSON_NAME, JSON_BYTES),
            ]),
            locations=[
                current_location(locationId='two'),
                current_location(locationId='one'),
            ],
        )

        self.assertEqual(result['recommended_actions'], [
            'upload_pdf_original', 'resolve_duplicate_locations'])
        self.assertEqual(result['auto_applicable_actions'], [])
        self.assertEqual(
            [location['location_id']
             for location in result['thoth_location']['locations']],
            ['one', 'two'],
        )

    def test_archive_current_but_location_checksum_stale(self):
        result, _, _, _ = self.harness.inspect(locations=[
            current_location(checksum='old')])

        self.assertEqual(result['status'], 'location_stale')
        self.assertTrue(result['internet_archive']['metadata']['current'])

    def test_unrelated_archive_data_is_ignored_but_reported(self):
        metadata = desired_metadata()
        metadata['unrelated-field'] = 'keep'
        metadata['collection'] = [
            IAUploader.THOTH_COLLECTION, 'another-collection']
        result, _, _, _ = self.harness.inspect(item=current_item(
            metadata=metadata,
            files=[
                original_file(PDF_NAME, PDF_BYTES),
                original_file(JSON_NAME, JSON_BYTES),
                original_file('unrelated.txt', b'keep'),
            ],
        ))

        self.assertEqual(result['status'], 'current')
        unrelated = result['internet_archive']['unrelated']
        self.assertEqual(unrelated['original_files'], ['unrelated.txt'])
        self.assertIn('unrelated-field', unrelated['metadata_fields'])
        self.assertEqual(unrelated['collections'], ['another-collection'])

    def test_unrelated_collection_reporting_is_sorted(self):
        metadata = desired_metadata()
        metadata['collection'] = [
            'zeta', IAUploader.THOTH_COLLECTION, 'alpha']
        result, _, _, _ = self.harness.inspect(
            item=current_item(metadata=metadata))

        self.assertEqual(
            result['internet_archive']['unrelated']['collections'],
            ['alpha', 'zeta'],
        )

    def test_recommended_and_auto_actions_use_stable_ordering(self):
        metadata = desired_metadata()
        metadata['title'] = 'old'
        result, _, _, _ = self.harness.inspect(
            item=FakeItem(exists=True, metadata=metadata, files=[]),
            locations=[],
        )

        expected = [
            'upload_pdf_original',
            'upload_json_original',
            'update_archive_metadata',
            'create_thoth_location',
        ]
        self.assertEqual(result['recommended_actions'], expected)
        self.assertEqual(result['auto_applicable_actions'], expected)

    def test_dry_run_makes_zero_mutations(self):
        item = current_item(files=[original_file(PDF_NAME, PDF_BYTES)])
        result, _, _, _ = self.harness.inspect(item=item, locations=[])

        self.assertNotEqual(result['status'], 'current')
        item.modify_metadata.assert_not_called()

    def test_dry_run_does_not_require_write_credentials(self):
        with patch.dict('reconcile_internet_archive.environ', {}, clear=True):
            result, _, _, _ = self.harness.inspect()

        self.assertEqual(result['status'], 'current')

    def test_location_lookup_failure_is_distinct(self):
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(work_metadata())
        reconciler = InternetArchiveReconciler(thoth=thoth)
        with patch.object(
                IAUploader, 'get_formatted_metadata',
                return_value=JSON_BYTES), patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=Publication(
                        'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
                        'https://source.example/book.pdf')), patch(
                    'reconcile_internet_archive.get_item',
                    return_value=current_item()), patch(
                    'reconcile_internet_archive.retrieve_existing_locations',
                    side_effect=RuntimeError('graphql down')):
            result, _ = reconciler.inspect_work(WORK_ID)

        self.assertIn('thoth_location_lookup_failed', result['issues'])
        self.assertEqual(result['status'], 'error')


def repairable_result(actions, status='item_missing'):
    result = _base_result(WORK_ID)
    result.update({
        'publisher_id': PUBLISHER_ID,
        'title': 'A Test Book',
        'publication_id': PUBLICATION_ID,
        'pdf_source_url': 'https://source.example/book.pdf',
        'eligible': True,
        'status': status,
        'issues': [status],
        'recommended_actions': list(actions),
        'auto_applicable_actions': [
            action for action in actions
            if action in {
                'create_archive_item',
                'upload_pdf_original',
                'upload_json_original',
                'update_archive_metadata',
                'create_thoth_location',
                'update_thoth_location',
            }
        ],
        'error': None,
    })
    return result


def current_result():
    result = repairable_result([], status='current')
    result['issues'] = []
    return result


def apply_context():
    return {
        'uploader': MagicMock(),
        'desired': SimpleNamespace(publication_id=PUBLICATION_ID),
        'item': object(),
        'archive_inspection': {},
        'location_input': object(),
    }


CREDENTIALS = {
    'ia_s3_access': 'access',
    'ia_s3_secret': 'secret',
    'THOTH_PAT': 'token',
}


class TestApplyMode(unittest.TestCase):
    def setUp(self):
        self.reconciler = InternetArchiveReconciler(thoth=MagicMock())
        # Real-apply tests exercise IAUploader verification (including the
        # bounded extended propagation phase); patch its sleep so no test waits.
        self._sleep_patcher = patch('iauploader.sleep')
        self._sleep_patcher.start()
        self.addCleanup(self._sleep_patcher.stop)

    def _apply(self, before, context, final=None):
        final = final or current_result()
        with patch.object(
                self.reconciler, 'inspect_work',
                return_value=(before, context)), patch.object(
                    self.reconciler, '_inspect_remote',
                    return_value=final) as inspect_remote, patch(
                    'reconcile_internet_archive.upsert_location') as upsert:
            result = self.reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)
        return result, inspect_remote, upsert

    def _real_archive_apply(self, item, upload_side_effect=None, metadata=None):
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(metadata or work_metadata())
        reconciler = InternetArchiveReconciler(thoth=thoth)
        publication = Publication(
            'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
            'https://source.example/book.pdf')
        response = MagicMock(status_code=200, text='')
        response.json.return_value = {'success': True}
        if upload_side_effect is None:
            def upload_side_effect(**kwargs):
                for name, file_object in kwargs['files'].items():
                    item.files = [
                        entry for entry in item.files
                        if entry.get('name') != name
                    ]
                    item.files.append(original_file(name, file_object.read()))
                if kwargs.get('metadata') is not None:
                    item.metadata = dict(kwargs['metadata'])
                item.exists = True
                return [response]

        with patch.object(
                IAUploader, 'get_formatted_metadata',
                return_value=JSON_BYTES), patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=publication), patch(
                    'reconcile_internet_archive.get_item',
                    return_value=item), patch(
                    'reconcile_internet_archive.retrieve_existing_locations',
                    return_value=[current_location()]), patch(
                    'iauploader.upload', side_effect=upload_side_effect
                ) as upload:
            result = reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)
        return result, upload

    def test_apply_without_credentials_fails_before_mutation(self):
        with self.assertRaises(ReconciliationConfigurationError):
            validate_apply_credentials({})

    def test_missing_item_is_created_and_reinspected(self):
        context = apply_context()
        before = repairable_result(['create_archive_item'])

        result, inspect_remote, _ = self._apply(before, context)

        context['uploader'].apply_archive_repairs.assert_called_once()
        inspect_remote.assert_called_once()
        self.assertEqual(result['status'], 'current')

    def test_partial_item_repairs_only_requested_original(self):
        context = apply_context()
        before = repairable_result(
            ['upload_json_original'], status='item_incomplete')

        result, _, _ = self._apply(before, context)

        self.assertEqual(result['applied_actions'], ['upload_json_original'])
        context['uploader'].apply_archive_repairs.assert_called_once()

    def test_stale_metadata_is_updated(self):
        context = apply_context()
        before = repairable_result(
            ['update_archive_metadata'], status='metadata_stale')

        result, _, _ = self._apply(before, context)

        self.assertEqual(result['applied_actions'], [
            'update_archive_metadata'])

    def test_missing_location_does_not_mutate_current_archive(self):
        context = apply_context()
        before = repairable_result(
            ['create_thoth_location'], status='location_missing')

        result, _, upsert = self._apply(before, context)

        context['uploader'].apply_archive_repairs.assert_not_called()
        upsert.assert_called_once()
        self.assertEqual(upsert.call_args.args, (
            self.reconciler.thoth, context['location_input']))
        self.assertIn('progress', upsert.call_args.kwargs)
        self.assertEqual(result['applied_actions'], ['create_thoth_location'])

    def test_stale_location_does_not_mutate_current_archive(self):
        context = apply_context()
        before = repairable_result(
            ['update_thoth_location'], status='location_stale')

        _, _, upsert = self._apply(before, context)

        context['uploader'].apply_archive_repairs.assert_not_called()
        upsert.assert_called_once()

    def test_collision_is_not_mutated(self):
        context = apply_context()
        before = repairable_result(
            ['resolve_identifier_collision'], status='identifier_collision')
        before['issues'] = ['identifier_collision']

        with patch.object(
                self.reconciler, 'inspect_work',
                return_value=(before, context)), patch(
                    'reconcile_internet_archive.upsert_location') as upsert:
            result = self.reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        context['uploader'].apply_archive_repairs.assert_not_called()
        upsert.assert_not_called()
        self.assertEqual(result['status'], 'identifier_collision')

    def test_duplicate_locations_are_not_mutated(self):
        context = apply_context()
        before = repairable_result(
            ['resolve_duplicate_locations'], status='duplicate_locations')
        before['issues'] = ['duplicate_locations']

        with patch.object(
                self.reconciler, 'inspect_work',
                return_value=(before, context)), patch(
                    'reconcile_internet_archive.upsert_location') as upsert:
            self.reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        context['uploader'].apply_archive_repairs.assert_not_called()
        upsert.assert_not_called()

    def test_mixed_batch_continues_after_failed_result(self):
        failed = repairable_result([], status='error')
        failed['error'] = 'failed'
        with patch.object(
                self.reconciler, 'reconcile_one',
                side_effect=[failed, current_result()]) as reconcile_one:
            results = self.reconciler.reconcile(
                [WORK_ID, WORK_ID_2], apply=True, credentials=CREDENTIALS)

        self.assertEqual(len(results), 2)
        self.assertEqual(reconcile_one.call_count, 2)

    def test_failed_final_verification_reports_failure(self):
        context = apply_context()
        before = repairable_result(['upload_pdf_original'], 'files_stale')
        final = repairable_result(['upload_pdf_original'], 'files_stale')

        result, _, _ = self._apply(before, context, final=final)

        self.assertEqual(result['status'], 'error')
        self.assertIn('verification_failed', result['issues'])

    def test_archive_verification_timeout_does_not_attempt_location(self):
        context = apply_context()
        before = repairable_result([
            'upload_pdf_original',
            'create_thoth_location',
        ], 'files_stale')

        def fail_verification(*args, **kwargs):
            progress = kwargs['progress']
            progress('upload_pdf_original', 'attempted')
            progress('upload_pdf_original', 'completed')
            raise InternetArchiveVerificationError('timed out')

        context['uploader'].apply_archive_repairs.side_effect = \
            fail_verification

        result, _, upsert = self._apply(before, context)

        self.assertEqual(result['status'], 'error')
        self.assertEqual(
            result['attempted_actions'], ['upload_pdf_original'])
        self.assertEqual(result['applied_actions'], [])
        self.assertEqual(
            result['uncertain_actions'], ['upload_pdf_original'])
        self.assertIn('verification_failed', result['issues'])
        self.assertNotIn(
            'create_thoth_location', result['attempted_actions'])
        upsert.assert_not_called()

    def test_second_identical_apply_run_makes_zero_mutations(self):
        context = apply_context()
        before = repairable_result([
            'update_archive_metadata',
            'create_thoth_location',
        ], 'metadata_stale')
        verification_current = current_result()
        second_current = current_result()
        with patch.object(
                self.reconciler, 'inspect_work',
                side_effect=[
                    (before, context),
                    (second_current, context),
                ]), \
                patch.object(
                    self.reconciler, '_inspect_remote',
                    return_value=verification_current), patch(
                    'reconcile_internet_archive.upsert_location',
                    return_value='location-id') as upsert:
            first = self.reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)
            second = self.reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        self.assertEqual(first['status'], 'current')
        self.assertEqual(first['applied_actions'], [
            'update_archive_metadata',
            'create_thoth_location',
        ])
        self.assertEqual(second['status'], 'current')
        self.assertEqual(second['attempted_actions'], [])
        self.assertEqual(second['applied_actions'], [])
        context['uploader'].apply_archive_repairs.assert_called_once()
        upsert.assert_called_once()

    def test_applied_result_records_before_actions_and_final_state(self):
        context = apply_context()
        before = repairable_result(['upload_pdf_original'], 'files_stale')

        result, _, _ = self._apply(before, context)

        self.assertEqual(result['before'], before)
        self.assertEqual(result['applied_actions'], ['upload_pdf_original'])
        self.assertEqual(result['status'], 'current')

    def test_archive_dissemination_error_does_not_attempt_location(self):
        context = apply_context()
        context['uploader'].apply_archive_repairs.side_effect = \
            DisseminationError('upload failed')
        before = repairable_result([
            'upload_pdf_original',
            'create_thoth_location',
        ], 'files_stale')

        result, _, upsert = self._apply(before, context)

        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['attempted_actions'], [])
        self.assertIn('archive_mutation_failed', result['issues'])
        self.assertNotIn(
            'thoth_location_mutation_failed', result['issues'])
        upsert.assert_not_called()

    def test_archive_unexpected_error_does_not_attempt_location(self):
        context = apply_context()
        context['uploader'].apply_archive_repairs.side_effect = \
            RuntimeError('unexpected archive failure')
        before = repairable_result([
            'upload_pdf_original',
            'create_thoth_location',
        ], 'files_stale')

        result, _, upsert = self._apply(before, context)

        self.assertEqual(result['attempted_actions'], [])
        self.assertEqual(result['applied_actions'], [])
        self.assertIn('archive_mutation_failed', result['issues'])
        self.assertNotIn(
            'thoth_location_mutation_failed', result['issues'])
        upsert.assert_not_called()

    def test_archive_only_unexpected_error_is_archive_failure(self):
        context = apply_context()
        context['uploader'].apply_archive_repairs.side_effect = \
            RuntimeError('unexpected archive failure')
        before = repairable_result(
            ['upload_pdf_original'], 'files_stale')

        result, _, upsert = self._apply(before, context)

        self.assertEqual(result['attempted_actions'], [])
        self.assertIn('archive_mutation_failed', result['issues'])
        upsert.assert_not_called()

    def test_archive_success_then_location_failure_preserves_progress(self):
        before = repairable_result([
            'upload_pdf_original',
            'create_thoth_location',
        ], status='files_stale')
        post_archive = repairable_result(
            ['create_thoth_location'],
            status='location_missing',
        )
        context = apply_context()

        def fail_location(*args, **kwargs):
            kwargs['progress']('create_thoth_location', 'attempted')
            raise RuntimeError('location failed')

        with patch.object(
                self.reconciler, 'inspect_work',
                return_value=(before, context)), patch.object(
                    self.reconciler, '_inspect_remote',
                    return_value=post_archive) as inspect_remote, patch(
                    'reconcile_internet_archive.upsert_location',
                    side_effect=fail_location) as upsert:
            result = self.reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        self.assertEqual(result['attempted_actions'], [
            'upload_pdf_original',
            'create_thoth_location',
        ])
        self.assertEqual(
            result['applied_actions'], ['upload_pdf_original'])
        self.assertIn(
            'thoth_location_mutation_failed', result['issues'])
        self.assertIn('location_missing', result['issues'])
        self.assertNotIn('files_stale', result['issues'])
        self.assertEqual(
            result['recommended_actions'], ['create_thoth_location'])
        self.assertNotIn(
            'upload_pdf_original', result['recommended_actions'])
        self.assertNotIn('archive_mutation_failed', result['issues'])
        self.assertEqual(result['before'], before)
        inspect_remote.assert_called_once()
        upsert.assert_called_once()

    def test_partial_apply_reinspection_failure_retains_both_diagnostics(self):
        before = repairable_result([
            'upload_pdf_original',
            'create_thoth_location',
        ], status='files_stale')
        context = apply_context()

        def fail_location(*args, **kwargs):
            kwargs['progress']('create_thoth_location', 'attempted')
            raise RuntimeError('location failed')

        with patch.object(
                self.reconciler, 'inspect_work',
                return_value=(before, context)), patch.object(
                    self.reconciler, '_inspect_remote',
                    side_effect=RuntimeError('inspection failed')), patch(
                    'reconcile_internet_archive.upsert_location',
                    side_effect=fail_location):
            result = self.reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        self.assertEqual(
            result['applied_actions'], ['upload_pdf_original'])
        self.assertIn('location failed', result['error'])
        self.assertIn(
            'post-apply reinspection failed: inspection failed',
            result['error'],
        )
        self.assertIn(
            'thoth_location_mutation_failed', result['issues'])
        self.assertEqual(result['before'], before)

    def test_successful_mixed_repair_runs_both_phases_and_verification(self):
        context = apply_context()
        before = repairable_result([
            'upload_pdf_original',
            'create_thoth_location',
        ], 'files_stale')

        result, inspect_remote, upsert = self._apply(before, context)

        context['uploader'].apply_archive_repairs.assert_called_once()
        upsert.assert_called_once()
        inspect_remote.assert_called_once()
        self.assertEqual(result['attempted_actions'], [
            'upload_pdf_original',
            'create_thoth_location',
        ])
        self.assertEqual(result['applied_actions'], [
            'upload_pdf_original',
            'create_thoth_location',
        ])
        self.assertEqual(result['status'], 'current')

    def test_ineligible_explicit_work_is_never_mutated(self):
        metadata = work_metadata(workStatus='DRAFT')
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(metadata)
        reconciler = InternetArchiveReconciler(thoth=thoth)
        with patch.object(
                IAUploader, 'get_formatted_metadata',
                return_value=JSON_BYTES), patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=Publication(
                        'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
                        'https://source.example/book.pdf')), \
                patch('reconcile_internet_archive.get_item',
                      return_value=FakeItem(exists=False)), patch(
                    'reconcile_internet_archive.retrieve_existing_locations',
                    return_value=[]), patch.object(
                    IAUploader, 'apply_archive_repairs') as archive_repair, \
                patch('reconcile_internet_archive.upsert_location') as upsert:
            result = reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        self.assertFalse(result['eligible'])
        self.assertEqual(result['attempted_actions'], [])
        archive_repair.assert_not_called()
        upsert.assert_not_called()

    def test_unavailable_identifier_apply_performs_zero_mutations(self):
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(work_metadata())
        reconciler = InternetArchiveReconciler(thoth=thoth)
        item = FakeItem(exists=False, identifier_available=False)
        with patch.object(
                IAUploader, 'get_formatted_metadata',
                return_value=JSON_BYTES), patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=Publication(
                        'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
                        'https://source.example/book.pdf')), \
                patch('reconcile_internet_archive.get_item',
                      return_value=item), patch(
                    'reconcile_internet_archive.retrieve_existing_locations',
                    return_value=[]), patch.object(
                    IAUploader, 'apply_archive_repairs') as archive_repair, \
                patch('reconcile_internet_archive.upsert_location') as upsert:
            result = reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        self.assertIn('identifier_collision', result['issues'])
        self.assertEqual(result['auto_applicable_actions'], [])
        self.assertEqual(result['attempted_actions'], [])
        archive_repair.assert_not_called()
        upsert.assert_not_called()
        item.modify_metadata.assert_not_called()

    def test_immutable_conflict_apply_performs_zero_mutations(self):
        metadata = desired_metadata()
        metadata['mediatype'] = 'data'
        metadata.pop('thoth-work-id')
        item = FakeItem(exists=True, metadata=metadata, files=[])
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(work_metadata())
        reconciler = InternetArchiveReconciler(thoth=thoth)
        publication = Publication(
            'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
            'https://source.example/book.pdf')

        with patch.object(
                IAUploader, 'get_formatted_metadata',
                return_value=JSON_BYTES), patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=publication), patch(
                    'reconcile_internet_archive.get_item',
                    return_value=item), patch(
                    'reconcile_internet_archive.retrieve_existing_locations',
                    return_value=[]), patch.object(
                    IAUploader, 'apply_archive_repairs') as archive_repair, \
                patch('iauploader.upload') as upload, patch(
                    'reconcile_internet_archive.upsert_location') as upsert:
            result = reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        self.assertEqual(result['status'], 'metadata_conflict')
        self.assertEqual(result['auto_applicable_actions'], [])
        self.assertEqual(result['attempted_actions'], [])
        archive_repair.assert_not_called()
        upload.assert_not_called()
        item.modify_metadata.assert_not_called()
        upsert.assert_not_called()

    def test_collection_conflict_apply_performs_zero_mutations(self):
        metadata = desired_metadata()
        metadata['collection'] = 'unrelated-collection'
        metadata['title'] = 'Old title'
        item = FakeItem(exists=True, metadata=metadata, files=[])
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(work_metadata())
        reconciler = InternetArchiveReconciler(thoth=thoth)
        publication = Publication(
            'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
            'https://source.example/book.pdf')

        with patch.object(
                IAUploader, 'get_formatted_metadata',
                return_value=JSON_BYTES), patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=publication), patch(
                    'reconcile_internet_archive.get_item',
                    return_value=item), patch(
                    'reconcile_internet_archive.retrieve_existing_locations',
                    return_value=[]), patch.object(
                    IAUploader, 'apply_archive_repairs') as archive_repair, \
                patch('iauploader.upload') as upload, patch(
                    'reconcile_internet_archive.upsert_location') as upsert:
            result = reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        self.assertEqual(result['status'], 'metadata_conflict')
        self.assertEqual(result['auto_applicable_actions'], [])
        self.assertEqual(result['attempted_actions'], [])
        archive_repair.assert_not_called()
        upload.assert_not_called()
        item.modify_metadata.assert_not_called()
        upsert.assert_not_called()

    def test_availability_failure_mixed_batch_continues(self):
        failed = repairable_result([], status='error')
        failed['issues'] = ['archive_request_failed']
        failed['error'] = 'identifier availability request failed'
        with patch.object(
                self.reconciler, 'reconcile_one',
                side_effect=[failed, current_result()]) as reconcile_one:
            results = self.reconciler.reconcile(
                [WORK_ID, WORK_ID_2], apply=True, credentials=CREDENTIALS)

        self.assertEqual(results[0]['issues'], ['archive_request_failed'])
        self.assertEqual(results[1]['status'], 'current')
        self.assertEqual(reconcile_one.call_count, 2)

    def test_real_orchestration_records_successful_archive_repair(self):
        item = current_item(files=[
            original_file(PDF_NAME, b'old'),
            original_file(JSON_NAME, JSON_BYTES),
        ])

        result, upload = self._real_archive_apply(item)

        self.assertEqual(result['status'], 'current')
        self.assertEqual(result['attempted_actions'], ['upload_pdf_original'])
        self.assertEqual(result['applied_actions'], ['upload_pdf_original'])
        self.assertEqual(result['uncertain_actions'], [])
        upload.assert_called_once()

    def test_real_orchestration_timeout_records_uncertain_activity(self):
        item = current_item(files=[
            original_file(PDF_NAME, b'old'),
            original_file(JSON_NAME, JSON_BYTES),
        ])
        response = MagicMock(status_code=200, text='')
        response.json.return_value = {'success': True}

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1):
            result, _ = self._real_archive_apply(
                item, upload_side_effect=lambda **kwargs: [response])

        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['attempted_actions'], ['upload_pdf_original'])
        self.assertEqual(result['applied_actions'], [])
        self.assertEqual(result['uncertain_actions'], ['upload_pdf_original'])
        self.assertIn('verification_failed', result['issues'])

    def test_real_orchestration_partial_archive_failure_is_accurate(self):
        metadata = desired_metadata()
        metadata['title'] = 'old'
        item = current_item(metadata=metadata, files=[
            original_file(PDF_NAME, b'old'),
            original_file(JSON_NAME, JSON_BYTES),
        ])
        item.modify_metadata.side_effect = RuntimeError('metadata failed')

        result, _ = self._real_archive_apply(item)

        self.assertEqual(result['attempted_actions'], [
            'upload_pdf_original', 'update_archive_metadata'])
        self.assertEqual(result['applied_actions'], ['upload_pdf_original'])
        self.assertIn('archive_mutation_failed', result['issues'])

    def test_real_orchestration_second_apply_is_mutation_free(self):
        item = current_item(files=[
            original_file(PDF_NAME, b'old'),
            original_file(JSON_NAME, JSON_BYTES),
        ])

        first, upload = self._real_archive_apply(item)
        second, second_upload = self._real_archive_apply(item)

        self.assertEqual(first['applied_actions'], ['upload_pdf_original'])
        self.assertEqual(second['status'], 'current')
        self.assertEqual(second['attempted_actions'], [])
        upload.assert_called_once()
        second_upload.assert_not_called()

    def test_location_failure_records_attempt_without_success(self):
        before = repairable_result(
            ['create_thoth_location'], status='location_missing')
        context = apply_context()

        def fail_location(*args, **kwargs):
            kwargs['progress']('create_thoth_location', 'attempted')
            raise RuntimeError('location failed')

        with patch.object(
                self.reconciler, 'inspect_work',
                return_value=(before, context)), patch(
                    'reconcile_internet_archive.upsert_location',
                    side_effect=fail_location):
            result = self.reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        self.assertEqual(result['attempted_actions'], [
            'create_thoth_location'])
        self.assertEqual(result['applied_actions'], [])
        self.assertIn('thoth_location_mutation_failed', result['issues'])
        context['uploader'].apply_archive_repairs.assert_not_called()


class TestOutput(unittest.TestCase):
    def test_json_output_is_deterministic(self):
        results = [current_result()]

        self.assertEqual(render_report(results), render_report(results))
        parsed = json.loads(render_report(results))
        self.assertEqual(parsed['results'][0]['work_id'], WORK_ID)

    def test_jsonl_contains_one_work_per_line_and_summary(self):
        report = render_report(
            [current_result(), current_result()], output_format='jsonl')
        lines = report.strip().splitlines()

        self.assertEqual(len(lines), 3)
        self.assertIn('work_id', json.loads(lines[0]))
        self.assertIn('summary', json.loads(lines[-1]))

    def test_summary_counts_are_correct(self):
        repairable = repairable_result(
            ['update_archive_metadata'], status='metadata_stale')
        ambiguous = repairable_result(
            ['resolve_identifier_collision'], status='identifier_collision')
        failed = repairable_result([], status='error')
        repaired = current_result()
        repaired['applied_actions'] = ['update_archive_metadata']

        summary = summarise([
            current_result(), repairable, ambiguous, failed, repaired])

        self.assertEqual(summary, {
            'inspected': 5,
            'current': 2,
            'repairable': 1,
            'ambiguous': 1,
            'failed': 1,
            'repaired': 1,
            'by_status': {
                'identifier_collision': 1,
                'error': 1,
                'metadata_stale': 1,
                'current': 2,
            },
        })

    def test_ineligible_remote_discrepancy_is_failed_not_repairable(self):
        result = repairable_result(
            ['create_archive_item'], status='item_missing')
        result['eligible'] = False
        result['issues'] = ['ineligible_status', 'item_missing']
        result['auto_applicable_actions'] = []

        summary = summarise([result])

        self.assertEqual(summary['failed'], 1)
        self.assertEqual(summary['repairable'], 0)
        self.assertEqual(summary['ambiguous'], 0)

    def test_metadata_conflict_is_ambiguous_not_repairable(self):
        result = repairable_result(
            ['resolve_archive_immutable_metadata'],
            status='metadata_conflict',
        )
        result['issues'] = ['archive_immutable_metadata_conflict']
        result['auto_applicable_actions'] = []

        summary = summarise([result])

        self.assertEqual(summary['repairable'], 0)
        self.assertEqual(summary['ambiguous'], 1)
        self.assertEqual(summary['failed'], 0)
        self.assertEqual(summary['repaired'], 0)
        self.assertEqual(summary['by_status'], {'metadata_conflict': 1})

    def test_collection_conflict_is_ambiguous_not_repairable(self):
        result = repairable_result(
            ['resolve_archive_collection_membership'],
            status='metadata_conflict',
        )
        result['issues'] = ['archive_collection_membership_conflict']
        result['auto_applicable_actions'] = []

        summary = summarise([result])

        self.assertEqual(summary['repairable'], 0)
        self.assertEqual(summary['ambiguous'], 1)
        self.assertEqual(summary['failed'], 0)
        self.assertEqual(summary['repaired'], 0)
        self.assertEqual(summary['by_status'], {'metadata_conflict': 1})

    def test_output_redacts_credential_values(self):
        result = current_result()
        result['error'] = 'request included super-secret-value'

        report = render_report([result], secrets=['super-secret-value'])

        self.assertNotIn('super-secret-value', report)
        self.assertIn('[REDACTED]', report)

    def test_report_is_written_when_some_works_fail(self):
        failed = repairable_result([], status='error')
        failed['error'] = 'remote failure'
        reconciler = MagicMock()
        reconciler.select_work_ids.return_value = [WORK_ID]
        reconciler.reconcile.return_value = [failed]
        reconciler.thoth = MagicMock()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'report.json'
            with patch(
                    'reconcile_internet_archive.InternetArchiveReconciler',
                    return_value=reconciler), patch(
                    'reconcile_internet_archive.load_local_environment'):
                status = main([
                    '--work-id', WORK_ID,
                    '--output', str(output),
                ])

            self.assertEqual(status, 1)
            self.assertEqual(
                json.loads(output.read_text())['results'][0]['error'],
                'remote failure')


PRE_EXPORT = (
    b'{"jsonGeneratedAt":"2026-07-28T08:00:00.000000Z",'
    b'"publications":[{"locations":[]}]}')
POST_EXPORT = (
    b'{"jsonGeneratedAt":"2026-07-28T08:05:00.000000Z",'
    b'"publications":[{"locations":['
    b'{"locationPlatform":"INTERNET_ARCHIVE",'
    b'"landingPage":"' + LANDING_PAGE.encode() + b'"}]}]}')
PRE_JSON = IAUploader._normalise_json_sidecar(PRE_EXPORT)
POST_JSON = IAUploader._normalise_json_sidecar(POST_EXPORT)
PRE_JSON_MD5 = hashlib.md5(PRE_JSON).hexdigest()
POST_JSON_MD5 = hashlib.md5(POST_JSON).hexdigest()


def _ok_response():
    response = MagicMock(status_code=200, text='')
    response.json.return_value = {'success': True}
    return response


class DeferredJsonHarness:
    """Drive a real reconcile_one apply where creating the Thoth location
    changes the json::thoth export (as it does in production)."""

    def __init__(self, item, initial_locations):
        self.item = item
        self.initial_locations = list(initial_locations)
        self.location_created = False
        self.uploaded_names = []
        self.uploaded_bytes = {}
        self.json_upload_count = 0
        self.location_present_at_json_upload = None
        self.upsert_calls = 0

    def export_bytes(self, specification):
        location_present = self.location_created or bool(self.initial_locations)
        return POST_EXPORT if location_present else PRE_EXPORT

    def locations(self, thoth, publication_id):
        if self.location_created:
            return [current_location()]
        return list(self.initial_locations)

    def upsert(self, thoth, location_input, progress=None,
               emit_location_id=False):
        self.upsert_calls += 1
        action = ('update_thoth_location' if self.initial_locations
                  else 'create_thoth_location')
        if progress is not None:
            progress(action, 'attempted')
            progress(action, 'completed')
        self.location_created = True
        return 'location-id'

    def upload(self, **kwargs):
        for name, file_object in kwargs['files'].items():
            contents = file_object.read()
            self.uploaded_names.append(name)
            self.uploaded_bytes[name] = contents
            if name.endswith('.json'):
                self.json_upload_count += 1
                self.location_present_at_json_upload = self.location_created
            self.item.files = [
                entry for entry in self.item.files
                if entry.get('name') != name
            ]
            self.item.files.append(original_file(name, contents))
        if kwargs.get('metadata') is not None:
            self.item.metadata = dict(kwargs['metadata'])
        self.item.exists = True
        return [_ok_response() for _ in kwargs['files']]


def _standalone_uploader():
    """Build an IAUploader wired only for direct build/upload_json_sidecar use."""
    uploader = IAUploader.__new__(IAUploader)
    uploader.work_id = WORK_ID
    uploader.version = __version__
    uploader.metadata = work_metadata()
    return uploader


def _post_location_desired():
    """A freshly rebuilt post-location desired state (JSON == POST_JSON)."""
    uploader = _standalone_uploader()
    publication = Publication(
        'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
        'https://source.example/book.pdf')
    with patch.object(
            IAUploader, 'get_formatted_metadata', return_value=POST_EXPORT), \
            patch.object(
                IAUploader, 'get_publication_details',
                return_value=publication):
        return uploader.build_desired_state()


class TestPostLocationJsonStaging(unittest.TestCase):
    """Part B: defer the JSON sidecar until after the Thoth location mutation."""

    def setUp(self):
        self._sleep_patcher = patch('iauploader.sleep')
        self._sleep_patcher.start()
        self.addCleanup(self._sleep_patcher.stop)

    def _run(self, item, initial_locations, apply=True,
             upload=None, publication_details=None,
             get_formatted_metadata=None):
        harness = DeferredJsonHarness(item, initial_locations)
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(work_metadata())
        reconciler = InternetArchiveReconciler(thoth=thoth)
        publication = Publication(
            'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
            'https://source.example/book.pdf')
        format_side = (
            get_formatted_metadata if get_formatted_metadata is not None
            else (lambda specification: harness.export_bytes(specification)))
        details = (
            publication_details if publication_details is not None
            else publication)
        details_kwargs = (
            {'side_effect': details} if callable(details)
            else {'return_value': details})
        with patch.object(
                IAUploader, 'get_formatted_metadata',
                side_effect=format_side), \
                patch.object(
                    IAUploader, 'get_publication_details',
                    **details_kwargs), \
                patch('reconcile_internet_archive.get_item',
                      return_value=item), \
                patch('reconcile_internet_archive.retrieve_existing_locations',
                      side_effect=harness.locations), \
                patch('reconcile_internet_archive.upsert_location',
                      side_effect=harness.upsert) as upsert, \
                patch('iauploader.upload',
                      side_effect=(upload or harness.upload)):
            result = reconciler.reconcile_one(
                WORK_ID, apply=apply, credentials=CREDENTIALS)
        return result, harness, upsert

    def _missing_item(self):
        return FakeItem(exists=False, metadata={}, files=[])

    def _current_item_without_location(self):
        return FakeItem(
            exists=True,
            metadata=desired_metadata(),
            files=[
                original_file(PDF_NAME, PDF_BYTES),
                original_file(JSON_NAME, PRE_JSON),
            ],
        )

    # 1. New item: created, location made, JSON rebuilt+uploaded once, current.
    def test_new_item_converges_in_one_apply(self):
        result, harness, upsert = self._run(self._missing_item(), [])

        self.assertEqual(result['status'], 'current')
        upsert.assert_called_once()
        self.assertEqual(harness.json_upload_count, 1)
        self.assertEqual(
            result['applied_actions'],
            ['create_archive_item', 'upload_pdf_original',
             'upload_json_original', 'create_thoth_location'])
        self.assertEqual(result['uncertain_actions'], [])

    # 2. A fresh dry-run after successful creation proposes zero actions.
    def test_fresh_dry_run_after_creation_is_current(self):
        item = self._missing_item()
        self._run(item, [])
        # After creation the export includes the location; a fresh inspection
        # (location now present) must find everything current.
        result, harness, _ = self._run(item, [current_location()], apply=False)

        self.assertEqual(result['status'], 'current')
        self.assertEqual(result['recommended_actions'], [])
        self.assertEqual(result['auto_applicable_actions'], [])
        self.assertEqual(harness.uploaded_names, [])

    # 3. JSON bytes uploaded include the post-location export.
    def test_uploaded_json_is_the_post_location_export(self):
        _, harness, _ = self._run(self._missing_item(), [])

        self.assertEqual(harness.uploaded_bytes[JSON_NAME], POST_JSON)
        self.assertNotEqual(POST_JSON, PRE_JSON)

    # 4. JSON is not uploaded before the location is created.
    def test_json_uploaded_only_after_location(self):
        _, harness, _ = self._run(self._missing_item(), [])

        self.assertTrue(harness.location_present_at_json_upload)
        self.assertLess(
            harness.uploaded_names.index(PDF_NAME),
            harness.uploaded_names.index(JSON_NAME))

    # 5. JSON is uploaded exactly once.
    def test_json_uploaded_exactly_once(self):
        _, harness, _ = self._run(self._missing_item(), [])

        self.assertEqual(harness.json_upload_count, 1)
        self.assertEqual(harness.uploaded_names.count(JSON_NAME), 1)

    # 6. Existing item, missing location follows the post-location JSON path.
    def test_existing_item_missing_location_uses_post_location_json(self):
        result, harness, upsert = self._run(
            self._current_item_without_location(), [])

        self.assertEqual(result['status'], 'current')
        upsert.assert_called_once()
        self.assertEqual(harness.json_upload_count, 1)
        self.assertEqual(harness.uploaded_bytes[JSON_NAME], POST_JSON)
        self.assertIn('upload_json_original', result['applied_actions'])
        self.assertIn('create_thoth_location', result['applied_actions'])

    # 7. Existing stale location follows the same path.
    def test_existing_stale_location_uses_post_location_json(self):
        stale = [current_location(
            landingPage='https://archive.org/details/old')]
        result, harness, upsert = self._run(
            self._current_item_without_location(), stale)

        self.assertEqual(result['status'], 'current')
        upsert.assert_called_once()
        self.assertEqual(harness.json_upload_count, 1)
        self.assertIn('update_thoth_location', result['applied_actions'])

    # 8. Initially stale JSON + missing location still uploads JSON once.
    def test_initially_stale_json_plus_missing_location_one_upload(self):
        item = FakeItem(
            exists=True,
            metadata=desired_metadata(),
            files=[
                original_file(PDF_NAME, PDF_BYTES),
                original_file(JSON_NAME, b'{"stale":true}\n'),
            ],
        )
        result, harness, _ = self._run(item, [])

        self.assertEqual(result['status'], 'current')
        self.assertEqual(harness.json_upload_count, 1)
        self.assertEqual(harness.uploaded_bytes[JSON_NAME], POST_JSON)

    # 9. Initially current JSON + missing location predicts and performs the
    #    post-location JSON upload.
    def test_current_json_missing_location_predicts_and_uploads(self):
        item = self._current_item_without_location()
        dry_run, _, _ = self._run(item, [], apply=False)
        self.assertEqual(dry_run['status'], 'location_missing')
        self.assertIn(
            'upload_json_original', dry_run['auto_applicable_actions'])
        self.assertIn(
            'create_thoth_location', dry_run['auto_applicable_actions'])

        result, harness, _ = self._run(
            self._current_item_without_location(), [])
        self.assertEqual(result['status'], 'current')
        self.assertEqual(harness.json_upload_count, 1)

    # 10. PDF verification failure prevents location and JSON stages.
    def test_pdf_verification_failure_blocks_location_and_json(self):
        def upload_without_pdf(**kwargs):
            # Accept the request but never expose the PDF original, so
            # verification cannot confirm it.
            return [_ok_response() for _ in kwargs['files']]

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 1):
            result, harness, upsert = self._run(
                self._missing_item(), [], upload=upload_without_pdf)

        self.assertEqual(result['status'], 'error')
        upsert.assert_not_called()
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('create_thoth_location', result['applied_actions'])

    # 11. Metadata verification failure prevents location and JSON stages.
    def test_metadata_verification_failure_blocks_location_and_json(self):
        def upload_dropping_mediatype(**kwargs):
            for name, file_object in kwargs['files'].items():
                harness_item.files = [
                    entry for entry in harness_item.files
                    if entry.get('name') != name
                ]
                harness_item.files.append(
                    original_file(name, file_object.read()))
            if kwargs.get('metadata') is not None:
                metadata = dict(kwargs['metadata'])
                metadata.pop('mediatype', None)
                harness_item.metadata = metadata
            harness_item.exists = True
            return [_ok_response() for _ in kwargs['files']]

        harness_item = self._missing_item()
        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1):
            result, harness, upsert = self._run(
                harness_item, [], upload=upload_dropping_mediatype)

        self.assertEqual(result['status'], 'error')
        upsert.assert_not_called()
        self.assertEqual(harness.json_upload_count, 0)

    # 12. Location creation failure prevents the deferred JSON upload.
    def test_location_failure_blocks_deferred_json(self):
        def failing_upsert(thoth, location_input, progress=None,
                           emit_location_id=False):
            raise DisseminationError('location mutation failed')

        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(work_metadata())
        reconciler = InternetArchiveReconciler(thoth=thoth)
        publication = Publication(
            'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
            'https://source.example/book.pdf')
        with patch.object(
                IAUploader, 'get_formatted_metadata',
                side_effect=lambda s: harness.export_bytes(s)), \
                patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=publication), \
                patch('reconcile_internet_archive.get_item',
                      return_value=item), \
                patch('reconcile_internet_archive.retrieve_existing_locations',
                      side_effect=harness.locations), \
                patch('reconcile_internet_archive.upsert_location',
                      side_effect=failing_upsert), \
                patch('iauploader.upload', side_effect=harness.upload):
            result = reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        self.assertEqual(result['status'], 'error')
        self.assertIn('thoth_location_mutation_failed', result['issues'])
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('upload_json_original', result['applied_actions'])

    # 13. Desired-state rebuild failure after location creation is reported.
    def test_rebuild_failure_after_location_is_reported(self):
        def export(specification, harness):
            if harness.location_created:
                raise DisseminationError('export unavailable')
            return PRE_EXPORT

        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])
        with patch.object(
                IAUploader, 'get_formatted_metadata',
                side_effect=lambda s: export(s, harness)), \
                patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=Publication(
                        'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
                        'https://source.example/book.pdf')), \
                patch('reconcile_internet_archive.get_item',
                      return_value=item), \
                patch('reconcile_internet_archive.retrieve_existing_locations',
                      side_effect=harness.locations), \
                patch('reconcile_internet_archive.upsert_location',
                      side_effect=harness.upsert), \
                patch('iauploader.upload', side_effect=harness.upload):
            reconciler = InternetArchiveReconciler(thoth=MagicMock())
            reconciler.thoth.work_by_id.return_value = json.dumps(
                work_metadata())
            result = reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        self.assertEqual(result['status'], 'error')
        self.assertIn('json_export_unavailable', result['issues'])
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertEqual(harness.json_upload_count, 0)
        # Even though the fresh desired could not be rebuilt, the failure is
        # reinspected: the created item and location are shown, not the stale
        # pre-apply "item_missing"/"location_missing" snapshot (which stays in
        # ``before``). The old JSON comparison is not claimed authoritative.
        self.assertTrue(result['internet_archive']['exists'])
        self.assertEqual(result['thoth_location']['state'], 'current')
        self.assertTrue(result['internet_archive']['json_state_unverified'])
        self.assertNotIn('item_missing', result['issues'])
        self.assertNotIn('location_missing', result['issues'])
        self.assertIn('item_missing', result['before']['issues'])
        self.assertIn('location_missing', result['before']['issues'])

    # 14. PDF MD5 drift between initial and rebuilt desired blocks JSON upload.
    def test_pdf_md5_drift_blocks_json_upload(self):
        publications = [
            Publication('PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
                        'https://source.example/book.pdf'),
            Publication('PDF', PUBLICATION_ID, b'different pdf bytes', '.pdf',
                        'https://source.example/book.pdf'),
        ]

        def details(_publication_type):
            return publications.pop(0)

        result, harness, upsert = self._run(
            self._missing_item(), [], publication_details=details)

        self.assertEqual(result['status'], 'error')
        self.assertIn('pdf_source_drift', result['issues'])
        upsert.assert_called_once()
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        # The current item and location are reinspected (reflecting the verified
        # remote PDF), not the pre-apply snapshot; the JSON stays unverified.
        self.assertTrue(result['internet_archive']['exists'])
        self.assertTrue(
            result['internet_archive']['files'][PDF_NAME]['current'])
        self.assertEqual(result['thoth_location']['state'], 'current')
        self.assertTrue(result['internet_archive']['json_state_unverified'])
        self.assertIn('item_missing', result['before']['issues'])

    # 15. Deferred JSON synchronous rejection is not retried.
    def test_deferred_json_rejection_not_retried(self):
        def upload(**kwargs):
            names = list(kwargs['files'])
            if any(name.endswith('.json') for name in names):
                harness.json_upload_count += 1
                raise DisseminationError(
                    'Internet Archive file upload failed: unacceptable')
            return DeferredJsonHarness.upload(harness, **kwargs)

        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])
        result, _, upsert = self._run(item, [], upload=upload)

        self.assertEqual(result['status'], 'error')
        self.assertIn('archive_mutation_failed', result['issues'])
        upsert.assert_called_once()
        self.assertEqual(harness.json_upload_count, 1)
        # A synchronous rejection is attempted but neither applied nor uncertain.
        self.assertIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertNotIn('upload_json_original', result['uncertain_actions'])

    # 16. Deferred JSON propagation timeout records uncertainty, no re-upload.
    def test_deferred_json_propagation_timeout_is_uncertain(self):
        def upload(**kwargs):
            names = list(kwargs['files'])
            if any(name.endswith('.json') for name in names):
                # Accept the upload but never expose the JSON original.
                harness.json_upload_count += 1
                return [_ok_response() for _ in kwargs['files']]
            return DeferredJsonHarness.upload(harness, **kwargs)

        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])
        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 1):
            result, _, upsert = self._run(item, [], upload=upload)

        self.assertEqual(result['status'], 'error')
        self.assertIn('verification_failed', result['issues'])
        upsert.assert_called_once()
        self.assertEqual(harness.json_upload_count, 1)
        self.assertEqual(result['uncertain_actions'], ['upload_json_original'])
        # Accepted-but-unverified: attempted and uncertain, never applied.
        self.assertIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertIn('create_thoth_location', result['applied_actions'])

    # 17. Final inspection uses the rebuilt (post-location) desired state.
    def test_final_inspection_uses_rebuilt_desired(self):
        result, harness, _ = self._run(self._missing_item(), [])

        self.assertEqual(
            result['internet_archive']['expected']['json_md5'], POST_JSON_MD5)
        self.assertTrue(
            result['internet_archive']['files'][JSON_NAME]['current'])

    # 18. No-location-action reconciliation retains current (inline) behaviour.
    def test_no_location_action_uploads_json_inline(self):
        item = FakeItem(
            exists=True,
            metadata=desired_metadata(),
            files=[original_file(PDF_NAME, PDF_BYTES)],
        )

        # Location already current; only the JSON original is missing.
        def export(_specification):
            return POST_EXPORT

        result, harness, upsert = self._run(
            item, [current_location()], get_formatted_metadata=export)

        self.assertEqual(result['status'], 'current')
        upsert.assert_not_called()
        self.assertEqual(harness.json_upload_count, 1)
        self.assertTrue(harness.location_present_at_json_upload is False)
        self.assertEqual(result['applied_actions'], ['upload_json_original'])

    # 19. Dry-run remains entirely non-mutating.
    def test_dry_run_is_non_mutating(self):
        result, harness, upsert = self._run(
            self._current_item_without_location(), [], apply=False)

        self.assertEqual(result['status'], 'location_missing')
        upsert.assert_not_called()
        self.assertEqual(harness.uploaded_names, [])
        self.assertEqual(harness.json_upload_count, 0)

    # 20. Batch and workflow safety limits remain unchanged.
    def test_workflow_apply_cap_unchanged(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            'ia_reconcile_workflow',
            str(Path(__file__).resolve().parent.parent
                / '.github' / 'scripts' / 'ia_reconcile_workflow.py'))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module.APPLY_MAX_BATCH_SIZE, 7)
        self.assertEqual(module.maximum_batch_size('apply'), 7)

    # Focused canary regression fixture for c4a58e8f: a CRLF/LF duplicate
    # subject plus a location that must be created. Both defects together.
    def test_canary_crlf_subject_with_location_creation_converges(self):
        canary_work = work_metadata(subjects=[
            {'subjectCode': 'Ancient\nGreek Thought'},
            {'subjectCode': 'Ancient\r\nGreek Thought'},
            {'subjectCode': 'Classical Reception'},
        ])
        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(canary_work)
        reconciler = InternetArchiveReconciler(thoth=thoth)

        def upload(**kwargs):
            # Internet Archive collapses the CRLF subject to LF, so it stores a
            # single deduplicated value: exactly what canonicalisation expects.
            for name, file_object in kwargs['files'].items():
                contents = file_object.read()
                harness.uploaded_names.append(name)
                harness.uploaded_bytes[name] = contents
                if name.endswith('.json'):
                    harness.json_upload_count += 1
                item.files = [
                    entry for entry in item.files
                    if entry.get('name') != name
                ]
                item.files.append(original_file(name, contents))
            if kwargs.get('metadata') is not None:
                metadata = dict(kwargs['metadata'])
                metadata['subject'] = [
                    'Ancient\nGreek Thought', 'Classical Reception']
                item.metadata = metadata
            item.exists = True
            return [_ok_response() for _ in kwargs['files']]

        publication = Publication(
            'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
            'https://source.example/book.pdf')
        with patch.object(
                IAUploader, 'get_formatted_metadata',
                side_effect=lambda s: harness.export_bytes(s)), \
                patch.object(
                    IAUploader, 'get_publication_details',
                    return_value=publication), \
                patch('reconcile_internet_archive.get_item',
                      return_value=item), \
                patch('reconcile_internet_archive.retrieve_existing_locations',
                      side_effect=harness.locations), \
                patch('reconcile_internet_archive.upsert_location',
                      side_effect=harness.upsert) as upsert, \
                patch('iauploader.upload', side_effect=upload):
            result = reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=CREDENTIALS)

        # Metadata verification passes despite the CRLF/LF collapse, the
        # location is reached, the post-location export is uploaded once, and
        # the item converges to current.
        self.assertEqual(result['status'], 'current')
        upsert.assert_called_once()
        self.assertEqual(harness.json_upload_count, 1)
        self.assertEqual(harness.uploaded_bytes[JSON_NAME], POST_JSON)
        self.assertEqual(
            result['applied_actions'],
            ['create_archive_item', 'upload_pdf_original',
             'upload_json_original', 'create_thoth_location'])
        self.assertEqual(result['internet_archive']['metadata']['patch_fields'],
                         [])

    # ---- Deferred-stage revalidation and truthful no-op reporting ----------

    def _apply_prepared(self, item, harness, upload=None,
                        publication_details=None, get_formatted_metadata=None,
                        credentials=CREDENTIALS):
        """Drive a real reconcile_one apply against a caller-owned harness.

        Mirrors ``_run`` but takes an already-constructed ``harness`` so a test
        can wire item mutations that must fire between stage-one verification
        and the deferred JSON stage.
        """
        thoth = MagicMock()
        thoth.work_by_id.return_value = json.dumps(work_metadata())
        reconciler = InternetArchiveReconciler(thoth=thoth)
        publication = Publication(
            'PDF', PUBLICATION_ID, PDF_BYTES, '.pdf',
            'https://source.example/book.pdf')
        format_side = (
            get_formatted_metadata if get_formatted_metadata is not None
            else (lambda s: harness.export_bytes(s)))
        if callable(publication_details):
            details_kwargs = {'side_effect': publication_details}
        else:
            details_kwargs = {'return_value': (
                publication if publication_details is None
                else publication_details)}
        with patch.object(
                IAUploader, 'get_formatted_metadata',
                side_effect=format_side), \
                patch.object(
                    IAUploader, 'get_publication_details',
                    **details_kwargs), \
                patch('reconcile_internet_archive.get_item',
                      return_value=item), \
                patch('reconcile_internet_archive.retrieve_existing_locations',
                      side_effect=harness.locations), \
                patch('reconcile_internet_archive.upsert_location',
                      side_effect=harness.upsert) as upsert, \
                patch('iauploader.upload',
                      side_effect=(upload or harness.upload)):
            result = reconciler.reconcile_one(
                WORK_ID, apply=True, credentials=credentials)
        return result, harness, upsert

    def _apply_with_rebuild_failure(self, error):
        """Apply a new item where the post-location rebuild raises ``error``.

        The first ``build_desired_state`` (initial inspection) succeeds so the
        apply proceeds; the second (post-location) call raises, exercising the
        rebuild-failure classification in ``_stage_post_location_json``.
        """
        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])
        real_build = IAUploader.build_desired_state

        def build(uploader):
            if harness.location_created:
                raise error
            return real_build(uploader)

        with patch.object(
                IAUploader, 'build_desired_state',
                autospec=True, side_effect=build):
            return self._apply_prepared(item, harness)

    @staticmethod
    def _mutate_on_deferred_refresh(item, harness, mutate):
        """Apply ``mutate`` on the first item.refresh() after location creation.

        That refresh is the one performed by ``upload_json_sidecar`` at the start
        of the deferred JSON stage, so the mutation lands exactly between
        stage-one verification and the deferred revalidation.
        """
        state = {'fired': False}

        def refresh():
            if harness.location_created and not state['fired']:
                state['fired'] = True
                mutate()

        item.refresh = MagicMock(side_effect=refresh)

    # 21. Item disappears after stage-one PDF verification: never recreated.
    def test_item_missing_before_deferred_json_is_not_recreated(self):
        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])

        def vanish():
            item.exists = False
            item.files = []
            item.metadata = {}

        self._mutate_on_deferred_refresh(item, harness, vanish)
        result, harness, upsert = self._apply_prepared(item, harness)

        self.assertEqual(result['status'], 'error')
        # The location may already have been applied; that is not rolled back.
        upsert.assert_called_once()
        self.assertIn('create_thoth_location', result['applied_actions'])
        # No JSON-only item is recreated and the JSON is neither attempted nor
        # applied.
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn(JSON_NAME, harness.uploaded_names)
        self.assertNotIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        # The top-level report is reinspected and reflects the disappearance,
        # while the applied location mutation is preserved and ``before`` keeps
        # the original snapshot.
        self.assertFalse(result['internet_archive']['exists'])
        self.assertEqual(result['thoth_location']['state'], 'current')
        self.assertEqual(result['before']['internet_archive']['exists'], False)

    # 22. thoth-work-id changes to another UUID before the deferred stage.
    def test_work_id_change_before_deferred_json_blocks_upload(self):
        item = self._current_item_without_location()
        harness = DeferredJsonHarness(item, [])

        def hijack():
            item.metadata = dict(item.metadata)
            item.metadata['thoth-work-id'] = WORK_ID_2

        self._mutate_on_deferred_refresh(item, harness, hijack)
        result, harness, upsert = self._apply_prepared(item, harness)

        self.assertEqual(result['status'], 'error')
        self.assertIn('identifier_collision', result['issues'])
        upsert.assert_called_once()
        # No credentials are used and no JSON upload is attempted.
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        # The reinspected top-level report reflects the collision and the
        # applied location, not the pre-apply snapshot.
        self.assertEqual(
            result['internet_archive']['ownership'], 'collision')
        self.assertIn('create_thoth_location', result['applied_actions'])

    # 23. The item loses the Thoth collection before the deferred stage.
    def test_collection_loss_before_deferred_json_blocks_upload(self):
        item = self._current_item_without_location()
        harness = DeferredJsonHarness(item, [])

        def drop_collection():
            item.metadata = {
                field: value for field, value in item.metadata.items()
                if field != 'collection'
            }

        self._mutate_on_deferred_refresh(item, harness, drop_collection)
        result, harness, upsert = self._apply_prepared(item, harness)

        self.assertEqual(result['status'], 'error')
        self.assertIn(
            'archive_collection_membership_conflict', result['issues'])
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('upload_json_original', result['applied_actions'])
        # Reinspected: the current IA item and applied location are shown.
        self.assertTrue(result['internet_archive']['exists'])
        self.assertIn('create_thoth_location', result['applied_actions'])

    # 24. mediatype changes incompatibly before the deferred stage.
    def test_mediatype_change_before_deferred_json_blocks_upload(self):
        item = self._current_item_without_location()
        harness = DeferredJsonHarness(item, [])

        def change_mediatype():
            item.metadata = dict(item.metadata)
            item.metadata['mediatype'] = 'audio'

        self._mutate_on_deferred_refresh(item, harness, change_mediatype)
        result, harness, upsert = self._apply_prepared(item, harness)

        self.assertEqual(result['status'], 'error')
        self.assertIn('archive_immutable_metadata_conflict', result['issues'])
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('upload_json_original', result['applied_actions'])
        # Reinspected: the current IA item and applied location are shown.
        self.assertTrue(result['internet_archive']['exists'])
        self.assertIn('create_thoth_location', result['applied_actions'])

    # 25. A legitimate owned item reports the JSON upload exactly once and
    #     uploads it exactly once.
    def test_owned_deferred_json_reported_and_uploaded_once(self):
        result, harness, upsert = self._run(
            self._current_item_without_location(), [])

        self.assertEqual(result['status'], 'current')
        upsert.assert_called_once()
        self.assertEqual(harness.json_upload_count, 1)
        self.assertEqual(harness.uploaded_names.count(JSON_NAME), 1)
        self.assertEqual(
            result['attempted_actions'].count('upload_json_original'), 1)
        self.assertEqual(
            result['applied_actions'].count('upload_json_original'), 1)

    # 26. The rebuilt post-location JSON already matches the remote original: a
    #     truthful no-op that is neither attempted nor applied.
    def test_post_location_json_already_current_is_noop(self):
        item = FakeItem(
            exists=True,
            metadata=desired_metadata(),
            files=[
                original_file(PDF_NAME, PDF_BYTES),
                original_file(JSON_NAME, POST_JSON),
            ],
        )
        harness = DeferredJsonHarness(item, [])
        get_env = MagicMock()
        with patch.object(IAUploader, 'get_variable_from_env', get_env):
            result, harness, upsert = self._apply_prepared(item, harness)

        self.assertEqual(result['status'], 'current')
        upsert.assert_called_once()
        # No upload happened, so no credentials were read solely for the JSON.
        self.assertEqual(harness.json_upload_count, 0)
        self.assertEqual(harness.uploaded_names, [])
        get_env.assert_not_called()
        # The predicted-but-unperformed upload must not be reported.
        self.assertNotIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertEqual(result['applied_actions'], ['create_thoth_location'])
        # Strict final verification still ran and confirmed the JSON original.
        self.assertTrue(
            result['internet_archive']['files'][JSON_NAME]['current'])

    # 27. The deferred revalidation accepts an owned-legacy Thoth item under the
    #     repository's existing legacy policy rather than rejecting it.
    def test_deferred_stage_accepts_legacy_item(self):
        desired = _post_location_desired()
        item = FakeItem(
            exists=True,
            identifier=WORK_ID,
            metadata={
                'collection': IAUploader.THOTH_COLLECTION,
                'mediatype': 'texts',
            },
            files=[
                original_file(PDF_NAME, PDF_BYTES),
                original_file(JSON_NAME, POST_JSON),
            ],
        )
        uploader = _standalone_uploader()
        upload_mock = MagicMock()
        get_env = MagicMock()
        with patch('iauploader.upload', upload_mock), \
                patch.object(IAUploader, 'get_variable_from_env', get_env), \
                patch.object(
                    IAUploader, '_verify_final_state',
                    return_value={JSON_NAME: POST_JSON_MD5}) as verify:
            result = uploader.upload_json_sidecar(
                item, desired, access_key='key', secret_key='secret')

        # Legacy is accepted (no collision), the current JSON is not re-uploaded,
        # credentials are not read for the skipped upload, and strict final
        # verification still runs.
        self.assertEqual(
            result, {'verified': {JSON_NAME: POST_JSON_MD5}, 'uploaded': False})
        upload_mock.assert_not_called()
        get_env.assert_not_called()
        verify.assert_called_once()

    # ---- P2-A: preserve the post-location rebuild failure source -----------

    # 28. A PDF-source rebuild failure is reported as pdf_source_unavailable.
    def test_rebuild_pdf_failure_reports_pdf_source(self):
        error = InternetArchiveDesiredStateError(
            'pdf', 'PDF source unavailable for {}: connection reset'.format(
                WORK_ID))
        result, harness, upsert = self._apply_with_rebuild_failure(error)

        self.assertEqual(result['status'], 'error')
        self.assertIn('pdf_source_unavailable', result['issues'])
        self.assertNotIn('json_export_unavailable', result['issues'])
        # The location was applied; the JSON was never attempted or applied.
        upsert.assert_called_once()
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        # The original source-specific text is retained.
        self.assertIn('pdf source', result['error'])
        self.assertIn('connection reset', result['error'])
        self.assertIn('no rollback was attempted', result['error'])
        # The current item and location are reinspected; the unrebuildable JSON
        # comparison is flagged rather than claimed current.
        self.assertTrue(result['internet_archive']['exists'])
        self.assertEqual(result['thoth_location']['state'], 'current')
        self.assertTrue(result['internet_archive']['json_state_unverified'])

    # 29. A JSON-export rebuild failure is reported as json_export_unavailable.
    def test_rebuild_json_failure_reports_json_export(self):
        error = InternetArchiveDesiredStateError(
            'json', 'JSON export unavailable for {}: 503'.format(WORK_ID))
        result, harness, upsert = self._apply_with_rebuild_failure(error)

        self.assertEqual(result['status'], 'error')
        self.assertIn('json_export_unavailable', result['issues'])
        upsert.assert_called_once()
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertIn('503', result['error'])

    # 30. A metadata rebuild failure is reported as malformed_metadata.
    def test_rebuild_metadata_failure_reports_malformed_metadata(self):
        error = InternetArchiveDesiredStateError(
            'metadata',
            'Malformed Thoth metadata for {}: missing required fields '
            'title'.format(WORK_ID))
        result, harness, upsert = self._apply_with_rebuild_failure(error)

        self.assertEqual(result['status'], 'error')
        self.assertIn('malformed_metadata', result['issues'])
        self.assertNotIn('json_export_unavailable', result['issues'])
        upsert.assert_called_once()
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertIn('missing required fields', result['error'])

    # 31. An unknown desired-state source falls back to malformed_metadata,
    #     matching the initial inspection path.
    def test_rebuild_unknown_source_falls_back_to_malformed_metadata(self):
        error = InternetArchiveDesiredStateError(
            'somewhere-else', 'unexpected component failed')
        result, _, upsert = self._apply_with_rebuild_failure(error)

        self.assertEqual(result['status'], 'error')
        self.assertIn('malformed_metadata', result['issues'])
        self.assertNotIn('json_export_unavailable', result['issues'])
        upsert.assert_called_once()

    # 32. A non-DesiredStateError rebuild failure is generic, not a JSON outage.
    def test_rebuild_unexpected_error_is_generic_not_json(self):
        result, harness, upsert = self._apply_with_rebuild_failure(
            RuntimeError('unexpected programming error'))

        self.assertEqual(result['status'], 'error')
        self.assertIn('archive_mutation_failed', result['issues'])
        self.assertNotIn('json_export_unavailable', result['issues'])
        self.assertNotIn('pdf_source_unavailable', result['issues'])
        upsert.assert_called_once()
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertIn('unexpected programming error', result['error'])
        # The generic rebuild failure is still reinspected to current state.
        self.assertTrue(result['internet_archive']['exists'])
        self.assertEqual(result['thoth_location']['state'], 'current')
        self.assertTrue(result['internet_archive']['json_state_unverified'])

    # ---- P2-B: reinspect current state after deferred verification timeout --

    @staticmethod
    def _accept_json_without_exposing(harness):
        """Upload side effect: expose the PDF, accept but hide the JSON."""
        def upload(**kwargs):
            names = list(kwargs['files'])
            if any(name.endswith('.json') for name in names):
                harness.json_upload_count += 1
                return [_ok_response() for _ in kwargs['files']]
            return DeferredJsonHarness.upload(harness, **kwargs)
        return upload

    # 33. First-time create + accepted-but-unverified JSON: the top-level report
    #     reflects the real partial state, and `before` keeps the pre-apply one.
    def test_timeout_reinspects_first_time_partial_state(self):
        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])
        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 1):
            result, harness, upsert = self._apply_prepared(
                item, harness,
                upload=self._accept_json_without_exposing(harness))

        self.assertEqual(result['status'], 'error')
        self.assertIn('verification_failed', result['issues'])
        # Top-level report shows the actual mutated IA and Thoth state.
        self.assertTrue(result['internet_archive']['exists'])
        self.assertTrue(
            result['internet_archive']['files'][PDF_NAME]['current'])
        self.assertEqual(result['thoth_location']['state'], 'current')
        # The completed mutations remain applied.
        self.assertIn('create_archive_item', result['applied_actions'])
        self.assertIn('upload_pdf_original', result['applied_actions'])
        self.assertIn('create_thoth_location', result['applied_actions'])
        # The JSON is attempted and uncertain, never applied.
        self.assertIn('upload_json_original', result['attempted_actions'])
        self.assertEqual(result['uncertain_actions'], ['upload_json_original'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        # The top-level report no longer claims the item/location are missing.
        self.assertNotIn('item_missing', result['issues'])
        self.assertNotIn('location_missing', result['issues'])
        # The original pre-apply snapshot is preserved under `before`.
        self.assertIn('item_missing', result['before']['issues'])
        self.assertIn('location_missing', result['before']['issues'])
        # No re-upload happened during reinspection.
        self.assertEqual(harness.uploaded_names.count(PDF_NAME), 1)
        self.assertEqual(harness.json_upload_count, 1)

    # 34. The same reinspection holds for an existing item + missing location.
    def test_timeout_reinspects_existing_item_missing_location(self):
        item = self._current_item_without_location()
        harness = DeferredJsonHarness(item, [])
        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 1):
            result, harness, upsert = self._apply_prepared(
                item, harness,
                upload=self._accept_json_without_exposing(harness))

        self.assertEqual(result['status'], 'error')
        self.assertIn('verification_failed', result['issues'])
        self.assertTrue(result['internet_archive']['exists'])
        self.assertTrue(
            result['internet_archive']['files'][PDF_NAME]['current'])
        self.assertEqual(result['thoth_location']['state'], 'current')
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertIn('upload_json_original', result['attempted_actions'])
        self.assertEqual(result['uncertain_actions'], ['upload_json_original'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertNotIn('location_missing', result['issues'])
        self.assertIn('location_missing', result['before']['issues'])
        self.assertEqual(harness.json_upload_count, 1)

    # 35. If the post-timeout reinspection itself fails, the original timeout
    #     error is preserved and the reinspection error appended; safe fallback.
    def test_timeout_reinspection_failure_is_reported_and_falls_back(self):
        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])
        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 1), \
                patch.object(
                    InternetArchiveReconciler, '_inspect_after_apply',
                    side_effect=RuntimeError('reinspection boom')):
            result, harness, upsert = self._apply_prepared(
                item, harness,
                upload=self._accept_json_without_exposing(harness))

        self.assertEqual(result['status'], 'error')
        self.assertIn('verification_failed', result['issues'])
        # The original timeout is not hidden, and the reinspection error is
        # appended.
        self.assertIn('post-apply reinspection failed', result['error'])
        self.assertIn('reinspection boom', result['error'])
        # JSON reporting semantics are unchanged.
        self.assertIn('upload_json_original', result['attempted_actions'])
        self.assertEqual(result['uncertain_actions'], ['upload_json_original'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        # Safe fallback to the pre-apply base (no further mutation).
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertEqual(harness.json_upload_count, 1)

    # 36. A synchronous JSON rejection is still attempted, never applied or
    #     uncertain, and is not retried (unchanged by the reinspection work).
    def test_synchronous_rejection_reporting_unchanged(self):
        def upload(**kwargs):
            names = list(kwargs['files'])
            if any(name.endswith('.json') for name in names):
                harness.json_upload_count += 1
                raise DisseminationError(
                    'Internet Archive file upload failed: unacceptable')
            return DeferredJsonHarness.upload(harness, **kwargs)

        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])
        result, _, upsert = self._apply_prepared(item, harness, upload=upload)

        self.assertEqual(result['status'], 'error')
        self.assertIn('archive_mutation_failed', result['issues'])
        upsert.assert_called_once()
        self.assertEqual(harness.json_upload_count, 1)
        self.assertIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertNotIn('upload_json_original', result['uncertain_actions'])
        # The top-level report is reinspected: the already-applied PDF, item and
        # location are shown, and no JSON original is currently visible. The
        # rejection is not retried.
        self.assertTrue(result['internet_archive']['exists'])
        self.assertTrue(
            result['internet_archive']['files'][PDF_NAME]['current'])
        self.assertEqual(result['thoth_location']['state'], 'current')
        self.assertFalse(
            result['internet_archive']['files'][JSON_NAME]['current'])
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertIn('item_missing', result['before']['issues'])

    # 37. A skipped (already-current) JSON upload followed by a verification
    #     failure from an unrelated PDF discrepancy: the JSON must be absent from
    #     attempted/applied/uncertain, and the state is still reinspected.
    def test_timeout_after_skipped_upload_excludes_json(self):
        # The remote JSON already equals the post-location export, so the
        # deferred stage skips the upload; but the PDF vanishes just before the
        # deferred stage, so strict final verification still fails.
        item = FakeItem(
            exists=True,
            metadata=desired_metadata(),
            files=[
                original_file(PDF_NAME, PDF_BYTES),
                original_file(JSON_NAME, POST_JSON),
            ],
        )
        harness = DeferredJsonHarness(item, [])

        def drop_pdf():
            item.files = [
                entry for entry in item.files
                if entry.get('name') != PDF_NAME
            ]

        self._mutate_on_deferred_refresh(item, harness, drop_pdf)
        get_env = MagicMock()
        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 1), \
                patch.object(IAUploader, 'get_variable_from_env', get_env):
            result, harness, upsert = self._apply_prepared(item, harness)

        self.assertEqual(result['status'], 'error')
        self.assertIn('verification_failed', result['issues'])
        upsert.assert_called_once()
        # The JSON upload was skipped: no request, no credentials read for it,
        # and it appears in none of the audit lists.
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn(JSON_NAME, harness.uploaded_names)
        get_env.assert_not_called()
        self.assertNotIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertNotIn('upload_json_original', result['uncertain_actions'])
        # The verification failure is preserved and the state is reinspected:
        # the location is applied and the PDF is now shown missing.
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertTrue(result['internet_archive']['exists'])
        self.assertFalse(
            result['internet_archive']['files'][PDF_NAME]['present'])

    # 38. A rebuild failure whose reinspection itself fails preserves the
    #     original rebuild error and appends the reinspection failure.
    def test_rebuild_failure_reinspection_failure_falls_back(self):
        error = InternetArchiveDesiredStateError(
            'json', 'JSON export unavailable for {}: 503'.format(WORK_ID))
        with patch.object(
                InternetArchiveReconciler, '_inspect_after_apply',
                side_effect=RuntimeError('reinspection boom')):
            result, harness, upsert = self._apply_with_rebuild_failure(error)

        self.assertEqual(result['status'], 'error')
        self.assertIn('json_export_unavailable', result['issues'])
        upsert.assert_called_once()
        # The original rebuild error is preserved and the reinspection error is
        # appended; the result falls back safely to the pre-apply snapshot.
        self.assertIn('503', result['error'])
        self.assertIn('post-apply reinspection failed', result['error'])
        self.assertIn('reinspection boom', result['error'])
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn('upload_json_original', result['applied_actions'])
        # No further upload occurred during the failed reinspection.
        self.assertNotIn(JSON_NAME, harness.uploaded_names)

    # 39. A synchronous JSON rejection whose reinspection itself fails preserves
    #     the original rejection and appends the reinspection failure.
    def test_synchronous_rejection_reinspection_failure_falls_back(self):
        def upload(**kwargs):
            names = list(kwargs['files'])
            if any(name.endswith('.json') for name in names):
                harness.json_upload_count += 1
                raise DisseminationError(
                    'Internet Archive file upload failed: unacceptable')
            return DeferredJsonHarness.upload(harness, **kwargs)

        item = self._missing_item()
        harness = DeferredJsonHarness(item, [])
        with patch.object(
                InternetArchiveReconciler, '_inspect_after_apply',
                side_effect=RuntimeError('reinspection boom')):
            result, _, upsert = self._apply_prepared(
                item, harness, upload=upload)

        self.assertEqual(result['status'], 'error')
        self.assertIn('archive_mutation_failed', result['issues'])
        upsert.assert_called_once()
        self.assertEqual(harness.json_upload_count, 1)
        self.assertIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        # Original rejection preserved, reinspection error appended, safe
        # fallback to the pre-apply snapshot.
        self.assertIn('unacceptable', result['error'])
        self.assertIn('post-apply reinspection failed', result['error'])
        self.assertIn('reinspection boom', result['error'])
        self.assertIn('item_missing', result['before']['issues'])

    # ---- Only the progress callback may record an attempted JSON upload -----

    # 40. upload_json_sidecar()'s initial item.refresh() fails before the upload
    #     callback: no upload request, and the JSON attempt is not fabricated.
    def test_refresh_failure_before_callback_records_no_attempt(self):
        # An item that is fully current except for the missing location, so
        # stage one performs no refresh/upload and the very first refresh is the
        # one upload_json_sidecar() runs at the start of the deferred stage.
        item = self._current_item_without_location()
        harness = DeferredJsonHarness(item, [])

        def boom():
            raise req_except.ConnectionError('refresh boom')

        self._mutate_on_deferred_refresh(item, harness, boom)
        result, harness, upsert = self._apply_prepared(item, harness)

        self.assertEqual(result['status'], 'error')
        self.assertIn('archive_mutation_failed', result['issues'])
        upsert.assert_called_once()
        # No JSON upload request occurred.
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn(JSON_NAME, harness.uploaded_names)
        # The pre-callback failure is not fabricated as an attempt.
        self.assertNotIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertNotIn('upload_json_original', result['uncertain_actions'])
        # The original refresh error is preserved.
        self.assertIn('refresh boom', result['error'])
        self.assertIn('Unable to refresh', result['error'])
        # Best-effort read-only reinspection still ran: the applied location and
        # existing item are shown; `before` keeps the pre-apply snapshot.
        self.assertIn('create_thoth_location', result['applied_actions'])
        self.assertTrue(result['internet_archive']['exists'])
        self.assertEqual(result['thoth_location']['state'], 'current')
        self.assertIn('location_missing', result['before']['issues'])

    # 41. Credential retrieval fails before the upload callback: the JSON attempt
    #     is not recorded and _upload_files() is never called.
    def test_credential_failure_before_callback_records_no_attempt(self):
        # Current-except-location item, so stage one needs no credentials; the
        # deferred stage must fetch them because the post-location JSON differs
        # from the remote original. Supply falsy IA credentials so the env
        # fallback fires, and make that fallback raise before the callback.
        item = self._current_item_without_location()
        harness = DeferredJsonHarness(item, [])
        upload = MagicMock()
        falsy_credentials = {
            'ia_s3_access': '', 'ia_s3_secret': '', 'THOTH_PAT': 'token'}
        with patch.object(
                IAUploader, 'get_variable_from_env',
                side_effect=DisseminationError(
                    'Error uploading to Internet Archive: missing value for '
                    'ia_s3_access')) as get_env:
            result, harness, upsert = self._apply_prepared(
                item, harness, upload=upload, credentials=falsy_credentials)

        self.assertEqual(result['status'], 'error')
        self.assertIn('archive_mutation_failed', result['issues'])
        upsert.assert_called_once()
        # Credential retrieval was reached, but _upload_files() was not called.
        get_env.assert_called()
        upload.assert_not_called()
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn(JSON_NAME, harness.uploaded_names)
        # The pre-callback failure is not fabricated as an attempt.
        self.assertNotIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertNotIn('upload_json_original', result['uncertain_actions'])
        # The original credential error is preserved and state is reinspected.
        self.assertIn('missing value for ia_s3_access', result['error'])
        self.assertTrue(result['internet_archive']['exists'])
        self.assertIn('create_thoth_location', result['applied_actions'])

    # 42. A pre-callback failure whose reinspection also fails must still not
    #     invent an upload attempt.
    def test_reinspection_failure_does_not_invent_attempt(self):
        item = self._current_item_without_location()
        harness = DeferredJsonHarness(item, [])

        def boom():
            raise req_except.ConnectionError('refresh boom')

        self._mutate_on_deferred_refresh(item, harness, boom)
        with patch.object(
                InternetArchiveReconciler, '_inspect_after_apply',
                side_effect=RuntimeError('reinspection boom')):
            result, harness, upsert = self._apply_prepared(item, harness)

        self.assertEqual(result['status'], 'error')
        self.assertIn('archive_mutation_failed', result['issues'])
        upsert.assert_called_once()
        self.assertEqual(harness.json_upload_count, 0)
        self.assertNotIn(JSON_NAME, harness.uploaded_names)
        # Even with the reinspection failing, no upload attempt is fabricated.
        self.assertNotIn('upload_json_original', result['attempted_actions'])
        self.assertNotIn('upload_json_original', result['applied_actions'])
        self.assertNotIn('upload_json_original', result['uncertain_actions'])
        # Original refresh error preserved, reinspection error appended, safe
        # fallback to the pre-apply snapshot.
        self.assertIn('refresh boom', result['error'])
        self.assertIn('post-apply reinspection failed', result['error'])
        self.assertIn('reinspection boom', result['error'])
        self.assertIn('location_missing', result['before']['issues'])


if __name__ == '__main__':
    unittest.main()
