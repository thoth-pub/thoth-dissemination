import hashlib
import importlib
import json
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

from requests import Response
from requests.exceptions import HTTPError

from errors import (
    DisseminationError,
    InternetArchiveIdentifierCollisionError,
    InternetArchiveImmutableMetadataError,
    InternetArchiveVerificationError,
)
from iauploader import IAUploader
from uploader import Publication


WORK_ID = '11111111-2222-3333-4444-555555555555'
PDF_NAME = '{}.pdf'.format(WORK_ID)
JSON_NAME = '{}.json'.format(WORK_ID)


def make_response(status_code=200, body=None):
    response = Response()
    response.status_code = status_code
    response._content = json.dumps(
        body if body is not None else {'success': True}
    ).encode('utf-8')
    response.encoding = 'utf-8'
    return response


def original_file(name, contents):
    return {
        'name': name,
        'source': 'original',
        'md5': hashlib.md5(contents).hexdigest(),
    }


class FakeItem:
    def __init__(
            self, identifier, exists, metadata=None, files=None,
            identifier_available=True):
        self.identifier = identifier
        self.exists = exists
        self.metadata = dict(metadata or {})
        self.files = list(files or [])
        self.refresh = MagicMock(side_effect=self._refresh)
        self.modify_metadata = MagicMock(side_effect=self._modify_metadata)
        self.identifier_available = MagicMock(
            return_value=identifier_available)
        self.refresh_hook = None

    def _refresh(self):
        if self.refresh_hook is not None:
            self.refresh_hook(self)

    def _modify_metadata(self, metadata, **kwargs):
        for key, value in metadata.items():
            if value == 'REMOVE_TAG':
                self.metadata.pop(key, None)
            else:
                self.metadata[key] = value
        return make_response()

    def set_original(self, name, contents):
        self.files = [
            file_metadata for file_metadata in self.files
            if file_metadata.get('name') != name
        ]
        self.files.append(original_file(name, contents))


class TestIAUploader(unittest.TestCase):
    def setUp(self):
        self.get_item_patcher = patch('iauploader.get_item')
        self.upload_patcher = patch('iauploader.upload')
        self.sleep_patcher = patch('iauploader.sleep')
        self.mock_get_item = self.get_item_patcher.start()
        self.mock_upload = self.upload_patcher.start()
        self.mock_sleep = self.sleep_patcher.start()
        self.addCleanup(self.get_item_patcher.stop)
        self.addCleanup(self.upload_patcher.stop)
        self.addCleanup(self.sleep_patcher.stop)

        self.pdf_bytes = b'current PDF bytes'
        self.json_bytes = b'{"current":"metadata"}'
        self.uploader = IAUploader.__new__(IAUploader)
        self.uploader.work_id = WORK_ID
        self.uploader.export_url = 'https://export.example'
        self.uploader.version = '1.4.1'
        self.uploader.metadata = {
            'data': {
                'work': {
                    'fullTitle': 'A Test Book',
                    'publicationDate': '2026-01-02',
                    'longAbstract': 'A long description',
                    'pageCount': 250,
                    'lccn': '2026000001',
                    'license': 'https://creativecommons.org/licenses/by/4.0/',
                    'oclc': '12345',
                    'doi': 'https://doi.org/10.0000/test',
                    'contributions': [
                        {'fullName': 'First Author', 'mainContribution': True},
                        {'fullName': 'Editor', 'mainContribution': False},
                    ],
                    'publications': [
                        {
                            'publicationType': 'PDF',
                            'publicationId': 'publication-id',
                            'isbn': '978-1-234-56789-0',
                        },
                    ],
                    'subjects': [{'subjectCode': 'ABC123'}],
                    'languages': [{'languageCode': 'eng'}],
                    'issues': [{
                        'issueOrdinal': 2,
                        'series': {
                            'issnPrint': '1234-5678',
                            'issnDigital': None,
                        },
                    }],
                    'imprint': {
                        'publisher': {
                            'publisherName': 'Test Publisher',
                        },
                    },
                },
            },
        }
        self.uploader.get_variable_from_env = MagicMock(
            side_effect=lambda name, platform: {
                'ia_s3_access': 'access-key',
                'ia_s3_secret': 'secret-key',
            }[name]
        )
        self.uploader.get_formatted_metadata = MagicMock(
            return_value=self.json_bytes)
        self.uploader.get_publication_details = MagicMock(
            return_value=Publication(
                'PDF', 'publication-id', self.pdf_bytes, '.pdf'))

        self.item = FakeItem(WORK_ID, False)
        self.mock_get_item.return_value = self.item
        self.mock_upload.side_effect = self._successful_upload

    def _desired_metadata(self):
        return self.uploader.parse_metadata()

    def _set_existing_item(self, metadata=None, files=None):
        self.item.exists = True
        self.item.metadata = dict(
            self._desired_metadata() if metadata is None else metadata)
        self.item.files = list(
            [
                original_file(PDF_NAME, self.pdf_bytes),
                original_file(JSON_NAME, self.json_bytes),
            ] if files is None else files)

    def _successful_upload(self, **kwargs):
        for name, file_object in kwargs['files'].items():
            contents = file_object.read()
            self.item.set_original(name, contents)
        if kwargs.get('metadata') is not None:
            self.item.metadata = dict(kwargs['metadata'])
        self.item.exists = True
        return [make_response() for _ in kwargs['files']]

    def _assert_location(self, location):
        self.assertEqual(location.publication_id, 'publication-id')
        self.assertEqual(location.location_platform, 'INTERNET_ARCHIVE')
        self.assertEqual(
            location.landing_page,
            'https://archive.org/details/{}'.format(WORK_ID))
        self.assertEqual(
            location.full_text_url,
            'https://archive.org/download/{}/{}.pdf'.format(
                WORK_ID, WORK_ID))
        self.assertEqual(
            location.checksum, hashlib.md5(self.pdf_bytes).hexdigest())
        self.assertEqual(location.checksum_algorithm, 'MD5')

    def test_new_item_creation_uploads_both_files_with_metadata(self):
        locations = self.uploader.upload_to_platform()

        self.assertEqual(self.mock_upload.call_count, 2)
        pdf_call, json_call = [
            call.kwargs for call in self.mock_upload.call_args_list
        ]
        self.assertEqual(set(pdf_call['files']), {PDF_NAME})
        self.assertEqual(pdf_call['metadata'], self._desired_metadata())
        self.assertEqual(set(json_call['files']), {JSON_NAME})
        self.assertIsNone(json_call['metadata'])
        self.assertTrue(pdf_call['checksum'])
        self.assertTrue(pdf_call['verify'])
        self.assertTrue(pdf_call['queue_derive'])
        self.item.modify_metadata.assert_not_called()
        self.item.identifier_available.assert_called_once_with()
        self._assert_location(locations[0])

    def test_new_item_initial_upload_sets_mediatype_texts(self):
        self.uploader.upload_to_platform()

        initial_metadata = self.mock_upload.call_args_list[0].kwargs[
            'metadata']
        self.assertEqual(initial_metadata['mediatype'], 'texts')

    def test_new_item_final_verification_requires_mediatype(self):
        desired_metadata = self._desired_metadata()

        def upload_without_mediatype(**kwargs):
            for name, file_object in kwargs['files'].items():
                self.item.set_original(name, file_object.read())
            if kwargs.get('metadata') is not None:
                self.item.metadata = dict(desired_metadata)
                self.item.metadata.pop('mediatype')
            self.item.exists = True
            return [make_response() for _ in kwargs['files']]

        self.mock_upload.side_effect = upload_without_mediatype

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1):
            with self.assertRaisesRegex(
                    InternetArchiveVerificationError,
                    "mediatype is None, expected 'texts'"):
                self.uploader.upload_to_platform()

    def test_existing_correct_mediatype_remains_repairable(self):
        metadata = self._desired_metadata()
        metadata['title'] = 'Old title'
        self._set_existing_item(metadata=metadata)
        desired = self.uploader.build_desired_state()

        inspection = self.uploader.inspect_item(self.item, desired)

        self.assertEqual(inspection['immutable_metadata_problems'], [])
        self.assertEqual(
            inspection['metadata_patch'], {'title': 'A Test Book'})
        self.assertIn('title', inspection['mutable_metadata_problems'][0])

    def test_existing_different_mediatype_reports_immutable_conflict(self):
        metadata = self._desired_metadata()
        metadata['mediatype'] = 'data'
        self._set_existing_item(metadata=metadata)
        desired = self.uploader.build_desired_state()

        inspection = self.uploader.inspect_item(self.item, desired)

        self.assertEqual(inspection['immutable_metadata_problems'], [
            "mediatype is 'data', expected 'texts'",
        ])
        self.assertEqual(inspection['mutable_metadata_problems'], [])
        self.assertNotIn('mediatype', inspection['metadata_patch'])

    def test_existing_missing_mediatype_reports_immutable_conflict(self):
        metadata = self._desired_metadata()
        metadata.pop('mediatype')
        self._set_existing_item(metadata=metadata)
        desired = self.uploader.build_desired_state()

        inspection = self.uploader.inspect_item(self.item, desired)

        self.assertEqual(inspection['immutable_metadata_problems'], [
            "mediatype is None, expected 'texts'",
        ])
        self.assertNotIn('mediatype', inspection['metadata_patch'])

    def test_immutable_mediatype_is_never_in_metadata_patch(self):
        current = self._desired_metadata()
        current['mediatype'] = 'data'

        patch = self.uploader._managed_metadata_patch(
            current, self._desired_metadata())

        self.assertNotIn('mediatype', patch)

    def test_immutable_mediatype_is_never_removed(self):
        current = self._desired_metadata()
        desired = self._desired_metadata()
        desired.pop('mediatype')

        patch = self.uploader._managed_metadata_patch(current, desired)

        self.assertNotIn('mediatype', patch)
        self.assertNotIn('REMOVE_TAG', [
            value for field, value in patch.items()
            if field == 'mediatype'
        ])

    def test_direct_dissemination_rejects_immutable_mediatype_conflict(self):
        metadata = self._desired_metadata()
        metadata['mediatype'] = 'data'
        self._set_existing_item(metadata=metadata)

        with self.assertRaisesRegex(
                InternetArchiveImmutableMetadataError,
                "item {}.*mediatype is 'data', required 'texts'.*"
                "only be set during item creation.*"
                "no automatic mutation was attempted".format(WORK_ID)):
            self.uploader.upload_to_platform()

    def test_direct_immutable_conflict_performs_zero_mutations(self):
        metadata = self._desired_metadata()
        metadata.pop('mediatype')
        metadata.pop('thoth-work-id')
        self._set_existing_item(metadata=metadata, files=[])

        with self.assertRaises(InternetArchiveImmutableMetadataError):
            self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.item.refresh.assert_not_called()

    def test_apply_archive_repairs_rechecks_initial_only_metadata(self):
        self._set_existing_item()
        desired = self.uploader.build_desired_state()
        stale_inspection = self.uploader.inspect_item(self.item, desired)
        self.item.metadata['mediatype'] = 'data'

        with self.assertRaises(InternetArchiveImmutableMetadataError):
            self.uploader.apply_archive_repairs(
                self.item,
                desired,
                inspection=stale_inspection,
                access_key='access-key',
                secret_key='secret-key',
            )

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.item.refresh.assert_not_called()

    def test_correct_mediatype_with_title_drift_updates_only_title(self):
        metadata = self._desired_metadata()
        metadata['title'] = 'Old title'
        self._set_existing_item(metadata=metadata)

        self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.assertEqual(
            self.item.modify_metadata.call_args.args[0],
            {'title': 'A Test Book'},
        )

    def test_unavailable_missing_identifier_fast_fails_before_sources(self):
        self.item.identifier_available.return_value = False

        with self.assertRaisesRegex(
                InternetArchiveIdentifierCollisionError,
                'no public item metadata.*reported the identifier unavailable'):
            self.uploader.upload_to_platform()

        self.item.identifier_available.assert_called_once_with()
        self.uploader.get_formatted_metadata.assert_not_called()
        self.uploader.get_publication_details.assert_not_called()
        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()

    def test_identifier_availability_failure_fast_fails_before_sources(self):
        self.item.identifier_available.side_effect = RuntimeError(
            'availability endpoint failed')

        with self.assertRaisesRegex(
                DisseminationError, 'Unable to check.*availability endpoint failed'):
            self.uploader.upload_to_platform()

        self.uploader.get_formatted_metadata.assert_not_called()
        self.uploader.get_publication_details.assert_not_called()
        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()

    def test_invalid_identifier_availability_response_is_rejected(self):
        self.item.identifier_available.return_value = 'available'

        with self.assertRaisesRegex(
                DisseminationError, 'invalid response'):
            self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.uploader.get_formatted_metadata.assert_not_called()

    def test_existing_current_item_skips_files_and_metadata(self):
        self._set_existing_item()

        locations = self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.assertGreaterEqual(self.item.refresh.call_count, 1)
        self.item.identifier_available.assert_not_called()
        self._assert_location(locations[0])

    def test_existing_item_with_changed_pdf_uploads_only_pdf(self):
        self._set_existing_item(files=[
            original_file(PDF_NAME, b'old PDF'),
            original_file(JSON_NAME, self.json_bytes),
        ])

        self.uploader.upload_to_platform()

        self.assertEqual(
            set(self.mock_upload.call_args.kwargs['files']), {PDF_NAME})
        self.assertTrue(self.mock_upload.call_args.kwargs['queue_derive'])
        self.assertTrue(self.mock_upload.call_args.kwargs['checksum'])
        self.assertTrue(self.mock_upload.call_args.kwargs['verify'])
        self.assertIsNone(self.mock_upload.call_args.kwargs['metadata'])

    def test_existing_item_with_changed_json_uploads_only_json(self):
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, b'old JSON'),
        ])

        self.uploader.upload_to_platform()

        self.assertEqual(
            set(self.mock_upload.call_args.kwargs['files']), {JSON_NAME})

    def test_existing_item_missing_json_uploads_only_json(self):
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
        ])

        self.uploader.upload_to_platform()

        self.assertEqual(
            set(self.mock_upload.call_args.kwargs['files']), {JSON_NAME})

    def test_existing_item_missing_pdf_uploads_only_pdf(self):
        self._set_existing_item(files=[
            original_file(JSON_NAME, self.json_bytes),
        ])

        self.uploader.upload_to_platform()

        self.assertEqual(
            set(self.mock_upload.call_args.kwargs['files']), {PDF_NAME})

    def test_existing_item_uses_explicit_metadata_update(self):
        metadata = self._desired_metadata()
        metadata['title'] = 'Old title'
        metadata['unrelated-field'] = 'keep me'
        self._set_existing_item(metadata=metadata)

        self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_called_once()
        metadata_patch = self.item.modify_metadata.call_args.args[0]
        self.assertEqual(metadata_patch, {'title': 'A Test Book'})
        self.assertEqual(self.item.metadata['unrelated-field'], 'keep me')
        self.assertFalse(
            self.item.modify_metadata.call_args.kwargs.get('append', False))

    def test_obsolete_optional_metadata_field_is_removed(self):
        metadata = self._desired_metadata()
        self.uploader.metadata['data']['work']['longAbstract'] = None
        self._set_existing_item(metadata=metadata)

        self.uploader.upload_to_platform()

        metadata_patch = self.item.modify_metadata.call_args.args[0]
        self.assertEqual(metadata_patch['description'], 'REMOVE_TAG')
        self.assertNotIn('description', self.item.metadata)

    def test_missing_page_count_does_not_produce_none_string(self):
        self.uploader.metadata['data']['work']['pageCount'] = None

        metadata = self.uploader.parse_metadata()

        self.assertNotIn('imagecount', metadata)
        self.assertNotIn('None', metadata.values())

    @patch('iauploader.logging.warning')
    def test_legacy_thoth_collection_item_adds_ownership_marker(
            self, mock_warning):
        metadata = self._desired_metadata()
        metadata.pop('thoth-work-id')
        self._set_existing_item(metadata=metadata)

        self.uploader.upload_to_platform()

        mock_warning.assert_called_once()
        metadata_patch = self.item.modify_metadata.call_args.args[0]
        self.assertEqual(metadata_patch['thoth-work-id'], WORK_ID)
        self.item.identifier_available.assert_not_called()

    def test_existing_item_with_matching_marker_is_accepted(self):
        metadata = self._desired_metadata()
        metadata.pop('collection')
        metadata['thoth-work-id'] = [WORK_ID]
        self._set_existing_item(metadata=metadata)

        locations = self.uploader.upload_to_platform()

        metadata_patch = self.item.modify_metadata.call_args.args[0]
        self.assertEqual(
            metadata_patch['collection'], IAUploader.THOTH_COLLECTION)
        self.assertNotIn('thoth-work-id', metadata_patch)
        self._assert_location(locations[0])

    def test_collision_with_another_thoth_work_id_is_refused(self):
        metadata = self._desired_metadata()
        metadata['thoth-work-id'] = [WORK_ID, 'another-work-id']
        self._set_existing_item(metadata=metadata)

        with self.assertRaisesRegex(
                InternetArchiveIdentifierCollisionError,
                'another-work-id'):
            self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.uploader.get_formatted_metadata.assert_not_called()

    def test_collision_without_thoth_ownership_is_refused(self):
        metadata = self._desired_metadata()
        metadata.pop('thoth-work-id')
        metadata['collection'] = 'unrelated-collection'
        self._set_existing_item(metadata=metadata)

        with self.assertRaisesRegex(
                InternetArchiveIdentifierCollisionError,
                'refusing to modify'):
            self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()

    def test_checksum_verification_timeout_is_bounded(self):
        self._set_existing_item(files=[])
        self.mock_upload.side_effect = lambda **kwargs: [make_response()]

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 3):
            with self.assertRaisesRegex(
                    InternetArchiveVerificationError,
                    'after 3 attempts'):
                self.uploader.upload_to_platform()

        self.assertEqual(self.item.refresh.call_count, 3)
        self.assertEqual(self.mock_sleep.call_count, 2)

    def test_derived_files_do_not_satisfy_final_verification(self):
        self._set_existing_item(files=[
            {
                'name': PDF_NAME,
                'source': 'derivative',
                'md5': hashlib.md5(self.pdf_bytes).hexdigest(),
            },
            {
                'name': JSON_NAME,
                'source': 'derivative',
                'md5': hashlib.md5(self.json_bytes).hexdigest(),
            },
        ])
        self.mock_upload.side_effect = lambda **kwargs: [make_response()]

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1):
            with self.assertRaisesRegex(
                    InternetArchiveVerificationError,
                    'missing as an original file'):
                self.uploader.upload_to_platform()

    def test_metadata_verification_retries_until_change_is_visible(self):
        desired_metadata = self._desired_metadata()
        stale_metadata = dict(desired_metadata, title='Old title')
        self._set_existing_item(metadata=stale_metadata)
        self.item.modify_metadata.side_effect = lambda *args, **kwargs: \
            make_response()

        def reveal_metadata(item):
            if item.refresh.call_count >= 2:
                item.metadata = dict(desired_metadata)

        self.item.refresh_hook = reveal_metadata

        locations = self.uploader.upload_to_platform()

        self.assertEqual(self.item.refresh.call_count, 2)
        self.mock_sleep.assert_called_once()
        self.item.modify_metadata.assert_called_once()
        self._assert_location(locations[0])

    def test_metadata_verification_times_out_when_change_stays_stale(self):
        metadata = self._desired_metadata()
        metadata['title'] = 'Old title'
        self._set_existing_item(metadata=metadata)
        self.item.modify_metadata.side_effect = lambda *args, **kwargs: \
            make_response()

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 3):
            with self.assertRaises(InternetArchiveVerificationError) as raised:
                self.uploader.upload_to_platform()

        self.assertIn('metadata discrepancies', str(raised.exception))
        self.assertIn('title', str(raised.exception))
        self.assertIn('Old title', str(raised.exception))
        self.assertEqual(self.item.refresh.call_count, 3)
        self.assertEqual(self.mock_sleep.call_count, 2)

    def test_metadata_verification_times_out_when_removed_field_remains(self):
        metadata = self._desired_metadata()
        self.uploader.metadata['data']['work']['longAbstract'] = None
        self._set_existing_item(metadata=metadata)
        self.item.modify_metadata.side_effect = lambda *args, **kwargs: \
            make_response()

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 2):
            with self.assertRaises(InternetArchiveVerificationError) as raised:
                self.uploader.upload_to_platform()

        self.assertIn('description', str(raised.exception))
        self.assertIn('expected it to be absent', str(raised.exception))
        self.assertEqual(self.item.refresh.call_count, 2)
        self.mock_sleep.assert_called_once()

    def test_unrelated_metadata_does_not_prevent_verification(self):
        metadata = self._desired_metadata()
        metadata['unrelated-field'] = ['keep', 'both']
        self._set_existing_item(metadata=metadata)

        locations = self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.item.refresh.assert_called_once()
        self._assert_location(locations[0])

    def test_unrelated_collection_memberships_do_not_prevent_verification(self):
        metadata = self._desired_metadata()
        metadata['collection'] = [
            'other-collection', IAUploader.THOTH_COLLECTION]
        self._set_existing_item(metadata=metadata)

        locations = self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.item.refresh.assert_called_once()
        self._assert_location(locations[0])

    def test_new_item_waits_for_files_and_metadata_to_become_visible(self):
        desired_metadata = self._desired_metadata()
        self.mock_upload.side_effect = lambda **kwargs: [
            make_response() for _ in kwargs['files']]

        def reveal_new_item(item):
            item.files = [
                original_file(PDF_NAME, self.pdf_bytes),
                original_file(JSON_NAME, self.json_bytes),
            ]
            if item.refresh.call_count == 1:
                item.metadata = {'title': 'Stale title'}
            else:
                item.metadata = dict(desired_metadata)

        self.item.refresh_hook = reveal_new_item

        locations = self.uploader.upload_to_platform()

        self.assertEqual(self.mock_upload.call_count, 2)
        self.item.modify_metadata.assert_not_called()
        self.assertEqual(self.item.refresh.call_count, 2)
        self.mock_sleep.assert_called_once()
        self._assert_location(locations[0])

    def test_second_run_is_read_only_after_eventual_metadata_update(self):
        desired_metadata = self._desired_metadata()
        stale_metadata = dict(desired_metadata, title='Old title')
        self._set_existing_item(metadata=stale_metadata)
        self.item.modify_metadata.side_effect = lambda *args, **kwargs: \
            make_response()

        def reveal_metadata(item):
            if item.refresh.call_count >= 2:
                item.metadata = dict(desired_metadata)

        self.item.refresh_hook = reveal_metadata

        first_locations = self.uploader.upload_to_platform()
        self._assert_location(first_locations[0])
        self.assertEqual(self.item.refresh.call_count, 2)
        self.mock_upload.reset_mock()
        self.item.modify_metadata.reset_mock()

        second_locations = self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self._assert_location(second_locations[0])

    def test_file_verification_retries_until_originals_are_visible(self):
        self._set_existing_item(files=[])
        self.mock_upload.side_effect = lambda **kwargs: [
            make_response() for _ in kwargs['files']]

        def reveal_files(item):
            if item.refresh.call_count == 1:
                item.files = []
            elif item.refresh.call_count == 2:
                item.files = [
                    original_file(PDF_NAME, b'stale PDF'),
                    original_file(JSON_NAME, self.json_bytes),
                ]
            else:
                item.files = [
                    original_file(PDF_NAME, self.pdf_bytes),
                    original_file(JSON_NAME, self.json_bytes),
                ]

        self.item.refresh_hook = reveal_files

        locations = self.uploader.upload_to_platform()

        self.assertEqual(self.mock_upload.call_count, 2)
        self.item.modify_metadata.assert_not_called()
        self.assertEqual(self.item.refresh.call_count, 3)
        self.assertEqual(self.mock_sleep.call_count, 2)
        self._assert_location(locations[0])

    def test_checksum_skipped_empty_response_is_not_a_failure(self):
        self._set_existing_item(files=[
            original_file(PDF_NAME, b'old PDF'),
            original_file(JSON_NAME, self.json_bytes),
        ])

        def skipped_after_race(**kwargs):
            for name, file_object in kwargs['files'].items():
                self.item.set_original(name, file_object.read())
            return [Response()]

        self.mock_upload.side_effect = skipped_after_race

        locations = self.uploader.upload_to_platform()

        self.mock_upload.assert_called_once()
        self._assert_location(locations[0])

    def test_second_identical_run_makes_no_mutating_api_calls(self):
        first_locations = self.uploader.upload_to_platform()
        self._assert_location(first_locations[0])
        self.mock_upload.reset_mock()
        self.item.modify_metadata.reset_mock()

        second_locations = self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self._assert_location(second_locations[0])

    def test_partial_upload_failure_is_repaired_on_next_run(self):
        def fail_after_pdf(**kwargs):
            file_object = kwargs['files'][PDF_NAME]
            self.item.set_original(PDF_NAME, file_object.read())
            self.item.metadata = dict(kwargs['metadata'])
            self.item.exists = True
            raise HTTPError('simulated JSON upload failure')

        self.mock_upload.side_effect = fail_after_pdf
        with self.assertRaisesRegex(
                DisseminationError, 'simulated JSON upload failure'):
            self.uploader.upload_to_platform()

        self.assertEqual(
            {file_metadata['name'] for file_metadata in self.item.files},
            {PDF_NAME})
        self.mock_upload.reset_mock()
        self.mock_upload.side_effect = self._successful_upload

        locations = self.uploader.upload_to_platform()

        self.assertEqual(
            set(self.mock_upload.call_args.kwargs['files']), {JSON_NAME})
        self._assert_location(locations[0])

    def test_repeatable_values_remain_lists_and_other_collections_are_kept(self):
        metadata = self._desired_metadata()
        metadata['creator'] = 'Old Author'
        metadata['collection'] = ['other-collection', 'thoth-archiving-network']
        self._set_existing_item(metadata=metadata)

        self.uploader.upload_to_platform()

        metadata_patch = self.item.modify_metadata.call_args.args[0]
        self.assertEqual(metadata_patch['creator'], ['First Author'])
        self.assertEqual(
            self.item.metadata['collection'],
            ['other-collection', 'thoth-archiving-network'])

    def test_matching_marker_adds_thoth_collection_without_removing_other_one(self):
        metadata = self._desired_metadata()
        metadata['collection'] = 'other-collection'
        self._set_existing_item(metadata=metadata)

        self.uploader.upload_to_platform()

        self.assertEqual(
            self.item.modify_metadata.call_args.args[0]['collection'],
            ['other-collection', 'thoth-archiving-network'])


class TestDisseminatorCLI(unittest.TestCase):
    def test_main_returns_success_or_converts_uploader_error_to_failure(self):
        modules = {
            'oapensworduploader': 'OAPENSWORDUploader',
            'souploader': 'SOUploader',
            'culuploader': 'CULUploader',
            'crossrefuploader': 'CrossrefUploader',
            'fsuploader': 'FigshareUploader',
            'zenodouploader': 'ZenodoUploader',
            'museuploader': 'MUSEUploader',
            'jstoruploader': 'JSTORUploader',
            'ebscouploader': 'EBSCOUploader',
            'proquestuploader': 'ProquestUploader',
            'googleplayuploader': 'GooglePlayUploader',
            'bkciuploader': 'BKCIUploader',
        }
        stub_modules = {}
        for module_name, class_name in modules.items():
            module = ModuleType(module_name)
            setattr(module, class_name, type(class_name, (), {}))
            stub_modules[module_name] = module
        dotenv = ModuleType('dotenv')
        dotenv.load_dotenv = MagicMock()
        stub_modules['dotenv'] = dotenv

        sys.modules.pop('disseminator', None)
        self.addCleanup(lambda: sys.modules.pop('disseminator', None))
        with patch.dict(sys.modules, stub_modules):
            disseminator = importlib.import_module('disseminator')

        arguments = SimpleNamespace(
            work_id=WORK_ID,
            platform='InternetArchive',
            export_url='https://export.example',
            client_url=None,
        )
        with patch.object(
                disseminator, 'get_arguments', return_value=arguments), \
                patch.object(
                    disseminator, 'run',
                    side_effect=InternetArchiveIdentifierCollisionError(
                        'identifier collision')), \
                patch.object(disseminator.logging, 'error') as mock_logging:
            status = disseminator.main()

        self.assertEqual(status, 1)
        mock_logging.assert_called_once()

        with patch.object(
                disseminator, 'get_arguments', return_value=arguments), \
                patch.object(disseminator, 'run') as mock_run, \
                patch.object(disseminator.logging, 'error') as mock_logging:
            status = disseminator.main()

        self.assertEqual(status, 0)
        mock_run.assert_called_once()
        mock_logging.assert_not_called()
if __name__ == '__main__':
    unittest.main()
