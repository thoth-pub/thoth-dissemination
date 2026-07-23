import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from errors import DisseminationError, InternetArchiveVerificationError
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
JSON_BYTES = b'{"current":"metadata"}'
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
        context = apply_context()

        def fail_location(*args, **kwargs):
            kwargs['progress']('create_thoth_location', 'attempted')
            raise RuntimeError('location failed')

        with patch.object(
                self.reconciler, 'inspect_work',
                return_value=(before, context)), patch(
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
        self.assertNotIn('archive_mutation_failed', result['issues'])
        upsert.assert_called_once()

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


if __name__ == '__main__':
    unittest.main()
