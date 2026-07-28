import hashlib
import importlib
import json
import sys
import unittest
from decimal import Decimal
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, call, patch

from requests import Response
from requests.exceptions import ConnectionError as ReqConnectionError, HTTPError

from errors import (
    DisseminationError,
    InternetArchiveDesiredStateError,
    InternetArchiveIdentifierCollisionError,
    InternetArchiveImmutableMetadataError,
    InternetArchiveRestrictedMetadataError,
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
        # Canonical sidecar bytes: the deterministic representation produced by
        # IAUploader._normalise_json_sidecar (sorted keys, compact separators,
        # single trailing newline). Using the canonical form here means the raw
        # export and its normalisation are byte-identical, so remote originals
        # seeded with these bytes still compare as current.
        self.json_bytes = b'{"current":"metadata"}\n'
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
        self.assertEqual(self.item.identifier_available.call_count, 2)
        self._assert_location(locations[0])

    def test_new_item_initial_upload_sets_mediatype_texts(self):
        self.uploader.upload_to_platform()

        initial_metadata = self.mock_upload.call_args_list[0].kwargs[
            'metadata']
        self.assertEqual(initial_metadata['mediatype'], 'texts')

    def test_new_item_initial_upload_sets_thoth_collection(self):
        self.uploader.upload_to_platform()

        initial_metadata = self.mock_upload.call_args_list[0].kwargs[
            'metadata']
        self.assertEqual(
            initial_metadata['collection'], IAUploader.THOTH_COLLECTION)

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

    def test_new_item_final_verification_requires_thoth_collection(self):
        desired_metadata = self._desired_metadata()

        def upload_without_collection(**kwargs):
            for name, file_object in kwargs['files'].items():
                self.item.set_original(name, file_object.read())
            if kwargs.get('metadata') is not None:
                self.item.metadata = dict(desired_metadata)
                self.item.metadata.pop('collection')
            self.item.exists = True
            return [make_response() for _ in kwargs['files']]

        self.mock_upload.side_effect = upload_without_collection

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1):
            with self.assertRaisesRegex(
                    InternetArchiveVerificationError,
                    "collection is None, expected to include "
                    "'thoth-archiving-network'"):
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

    def test_ia_derived_imagecount_is_never_in_metadata_patch(self):
        # IA's derive process owns `imagecount`; a divergent value must not be
        # re-patched, or the item never converges (IA overwrites it again).
        current = self._desired_metadata()
        current['imagecount'] = '999'

        patch = self.uploader._managed_metadata_patch(
            current, self._desired_metadata())

        self.assertNotIn('imagecount', patch)

    def test_ia_derived_imagecount_is_never_removed(self):
        current = self._desired_metadata()
        desired = self._desired_metadata()
        desired.pop('imagecount')

        patch = self.uploader._managed_metadata_patch(current, desired)

        self.assertNotIn('imagecount', patch)

    def test_ia_derived_imagecount_divergence_is_not_a_problem(self):
        desired = self.uploader.build_desired_state()
        metadata = self._desired_metadata()
        metadata['imagecount'] = '999'
        self._set_existing_item(metadata=metadata)

        inspection = self.uploader.inspect_item(self.item, desired)

        self.assertTrue(inspection['metadata_current'])
        self.assertEqual(inspection['metadata_problems'], [])

    def test_imagecount_is_seeded_but_not_restricted(self):
        # Still managed (seeded on creation) but neither mutable nor restricted.
        self.assertIn('imagecount', IAUploader.MANAGED_METADATA_FIELDS)
        self.assertNotIn('imagecount', IAUploader.MUTABLE_MANAGED_METADATA_FIELDS)
        self.assertNotIn('imagecount', IAUploader.INITIAL_ONLY_METADATA_FIELDS)
        self.assertNotIn('imagecount', IAUploader.ADMIN_ONLY_METADATA_FIELDS)

    def test_final_verification_field_scope_excludes_only_derived(self):
        # Final verification must cover every managed field except the ones
        # Internet Archive derives and owns, keeping it consistent with
        # inspect_item's non-derived scoping.
        self.assertEqual(
            IAUploader.FINAL_VERIFICATION_METADATA_FIELDS,
            IAUploader.MANAGED_METADATA_FIELDS
            - IAUploader.DERIVED_METADATA_FIELDS)
        self.assertNotIn(
            'imagecount', IAUploader.FINAL_VERIFICATION_METADATA_FIELDS)
        for field in ('mediatype', 'collection', 'title', 'description',
                      'thoth-work-id', 'thoth-dissemination-service'):
            self.assertIn(
                field, IAUploader.FINAL_VERIFICATION_METADATA_FIELDS)

    def test_final_verification_ignores_present_derived_field(self):
        # Reproduces the post-PR #90 canary failure: Thoth has no pageCount so
        # desired metadata omits imagecount, but Internet Archive holds a
        # derived imagecount. Final verification must not demand its absence.
        self.uploader.metadata['data']['work']['pageCount'] = None
        desired = self._desired_metadata()
        self.assertNotIn('imagecount', desired)
        stale = dict(desired, description='Stale description', imagecount='120')
        self._set_existing_item(metadata=stale)

        locations = self.uploader.upload_to_platform()

        # The stale mutable field is repaired, but imagecount is neither
        # patched nor removed, and final verification succeeds.
        self.item.modify_metadata.assert_called_once()
        patch = self.item.modify_metadata.call_args.args[0]
        self.assertIn('description', patch)
        self.assertNotIn('imagecount', patch)
        self.assertEqual(self.item.metadata.get('imagecount'), '120')
        self.mock_upload.assert_not_called()
        self._assert_location(locations[0])

    def test_final_verification_ignores_divergent_derived_field(self):
        # Desired imagecount (from pageCount) differs from IA's derived value.
        desired = self._desired_metadata()
        self.assertEqual(desired.get('imagecount'), '250')
        stale = dict(desired, description='Stale description', imagecount='247')
        self._set_existing_item(metadata=stale)

        locations = self.uploader.upload_to_platform()

        patch = self.item.modify_metadata.call_args.args[0]
        self.assertNotIn('imagecount', patch)
        self.assertEqual(self.item.metadata.get('imagecount'), '247')
        self.mock_upload.assert_not_called()
        self._assert_location(locations[0])

    def test_final_verification_still_fails_on_stale_mutable_field(self):
        # A genuine mutable field that stays stale after the update must still
        # raise, proving the fix is a field-scope correction, not a bypass.
        metadata = dict(self._desired_metadata(), title='Old title')
        self._set_existing_item(metadata=metadata)
        # Acknowledge the patch without applying it, so the field stays stale.
        self.item.modify_metadata.side_effect = \
            lambda *args, **kwargs: make_response()

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 1):
            with self.assertRaises(InternetArchiveVerificationError) as raised:
                self.uploader.upload_to_platform()

        self.assertIn('metadata discrepancies', str(raised.exception))
        self.assertIn('title', str(raised.exception))
        self.assertIn('Old title', str(raised.exception))

    def test_genuine_description_change_is_still_detected(self):
        desired = self.uploader.build_desired_state()
        metadata = self._desired_metadata()
        metadata['description'] = 'A completely different description'
        self._set_existing_item(metadata=metadata)

        inspection = self.uploader.inspect_item(self.item, desired)

        self.assertFalse(inspection['metadata_current'])
        self.assertEqual(
            inspection['metadata_patch']['description'], 'A long description')

    def test_admin_only_collection_is_never_in_metadata_patch(self):
        current = self._desired_metadata()
        current.pop('collection')

        metadata_patch = self.uploader._managed_metadata_patch(
            current, self._desired_metadata())

        self.assertNotIn('collection', metadata_patch)

    def test_missing_desired_collection_never_produces_remove_tag(self):
        current = self._desired_metadata()
        desired = self._desired_metadata()
        desired.pop('collection')

        metadata_patch = self.uploader._managed_metadata_patch(
            current, desired)

        self.assertNotIn('collection', metadata_patch)

    def test_collection_comparison_accepts_scalar_and_list_membership(self):
        desired = self.uploader.build_desired_state()
        for collection in (
                IAUploader.THOTH_COLLECTION,
                ['unrelated', IAUploader.THOTH_COLLECTION],
        ):
            with self.subTest(collection=collection):
                metadata = self._desired_metadata()
                metadata['collection'] = collection
                self._set_existing_item(metadata=metadata)

                inspection = self.uploader.inspect_item(self.item, desired)

                self.assertEqual(
                    inspection['admin_only_metadata_problems'], [])
                self.assertNotIn('collection', inspection['metadata_patch'])

    def test_missing_collection_reports_admin_only_conflict(self):
        metadata = self._desired_metadata()
        metadata.pop('collection')
        self._set_existing_item(metadata=metadata)
        desired = self.uploader.build_desired_state()

        inspection = self.uploader.inspect_item(self.item, desired)

        self.assertEqual(inspection['admin_only_metadata_problems'], [
            "collection is None, expected to include "
            "'thoth-archiving-network'",
        ])
        self.assertNotIn('collection', inspection['metadata_patch'])

    def test_unrelated_collection_reports_admin_only_conflict(self):
        metadata = self._desired_metadata()
        metadata['collection'] = 'unrelated-collection'
        self._set_existing_item(metadata=metadata)
        desired = self.uploader.build_desired_state()

        inspection = self.uploader.inspect_item(self.item, desired)

        self.assertEqual(inspection['admin_only_metadata_problems'], [
            "collection is 'unrelated-collection', expected to include "
            "'thoth-archiving-network'",
        ])
        self.assertNotIn('collection', inspection['metadata_patch'])

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

    def test_immutable_conflict_does_not_read_credentials(self):
        metadata = self._desired_metadata()
        metadata['mediatype'] = 'data'
        self._set_existing_item(metadata=metadata)

        with self.assertRaises(InternetArchiveImmutableMetadataError):
            self.uploader.upload_to_platform()

        self.uploader.get_variable_from_env.assert_not_called()

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

    def test_apply_archive_repairs_rechecks_admin_only_collection(self):
        self._set_existing_item()
        desired = self.uploader.build_desired_state()
        stale_inspection = self.uploader.inspect_item(self.item, desired)
        self.item.metadata['collection'] = 'unrelated-collection'

        with self.assertRaisesRegex(
                InternetArchiveRestrictedMetadataError,
                "item {}.*collection membership "
                "\\['unrelated-collection'\\].*must include "
                "'thoth-archiving-network'.*administrator intervention.*"
                "no automatic mutation was attempted".format(WORK_ID)):
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

    def test_apply_archive_repairs_refreshes_owned_item_before_mutation(self):
        self._set_existing_item(files=[])
        desired = self.uploader.build_desired_state()
        stale_inspection = self.uploader.inspect_item(self.item, desired)

        def replace_ownership(item):
            item.metadata['thoth-work-id'] = 'another-work-id'

        self.item.refresh_hook = replace_ownership

        with self.assertRaisesRegex(
                InternetArchiveIdentifierCollisionError,
                'another-work-id'):
            self.uploader.apply_archive_repairs(
                self.item,
                desired,
                inspection=stale_inspection,
                access_key='access-key',
                secret_key='secret-key',
            )

        self.item.refresh.assert_called_once_with()
        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()

    def test_apply_archive_repairs_rechecks_legacy_collection_before_mutation(
            self):
        metadata = self._desired_metadata()
        metadata.pop('thoth-work-id')
        self._set_existing_item(metadata=metadata, files=[])
        desired = self.uploader.build_desired_state()
        stale_inspection = self.uploader.inspect_item(self.item, desired)

        def remove_collection(item):
            item.metadata['collection'] = 'unrelated-collection'

        self.item.refresh_hook = remove_collection

        with self.assertRaisesRegex(
                InternetArchiveIdentifierCollisionError,
                'not an identifiable legacy member'):
            self.uploader.apply_archive_repairs(
                self.item,
                desired,
                inspection=stale_inspection,
                access_key='access-key',
                secret_key='secret-key',
            )

        self.item.refresh.assert_called_once_with()
        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()

    def test_apply_archive_repairs_refresh_failure_is_non_mutating(self):
        self._set_existing_item(files=[])
        desired = self.uploader.build_desired_state()
        stale_inspection = self.uploader.inspect_item(self.item, desired)
        self.item.refresh.side_effect = HTTPError('refresh failed')

        with self.assertRaisesRegex(
                DisseminationError,
                'refresh.*immediately before mutation'):
            self.uploader.apply_archive_repairs(
                self.item,
                desired,
                inspection=stale_inspection,
                access_key='access-key',
                secret_key='secret-key',
            )

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()

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

    def test_current_item_does_not_read_credentials(self):
        self._set_existing_item()

        self.uploader.upload_to_platform()

        self.uploader.get_variable_from_env.assert_not_called()

    def test_item_requiring_mutation_reads_credentials(self):
        metadata = self._desired_metadata()
        metadata['title'] = 'Old title'
        self._set_existing_item(metadata=metadata)

        self.uploader.upload_to_platform()

        self.assertEqual(
            self.uploader.get_variable_from_env.call_args_list,
            [
                call('ia_s3_access', 'Internet Archive'),
                call('ia_s3_secret', 'Internet Archive'),
            ],
        )
        self.item.modify_metadata.assert_called_once()

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

    def test_matching_marker_with_missing_collection_is_restricted_conflict(self):
        metadata = self._desired_metadata()
        metadata.pop('collection')
        metadata['thoth-work-id'] = [WORK_ID]
        self._set_existing_item(metadata=metadata)

        with self.assertRaisesRegex(
                InternetArchiveRestrictedMetadataError,
                "item {}.*collection membership \\[\\].*must include "
                "'thoth-archiving-network'.*administrator intervention.*"
                "no automatic mutation was attempted".format(WORK_ID)):
            self.uploader.upload_to_platform()

        self.uploader.get_variable_from_env.assert_not_called()
        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.item.refresh.assert_not_called()

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

    def test_identifier_collision_does_not_read_credentials(self):
        metadata = self._desired_metadata()
        metadata['thoth-work-id'] = 'another-work-id'
        self._set_existing_item(metadata=metadata)

        with self.assertRaises(InternetArchiveIdentifierCollisionError):
            self.uploader.upload_to_platform()

        self.uploader.get_variable_from_env.assert_not_called()

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
        # Accepted uploads whose originals never appear: ordinary verification
        # expires, the extended propagation phase runs (both files were
        # uploaded this invocation), and the whole process is still bounded.
        self._set_existing_item(files=[])
        self.mock_upload.side_effect = lambda **kwargs: [make_response()]

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 3), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 2):
            with self.assertRaisesRegex(
                    InternetArchiveVerificationError,
                    'upload propagation'):
                self.uploader.upload_to_platform()

        # 1 pre-mutation refresh + 3 ordinary + 2 extended = 6 (bounded).
        self.assertEqual(self.item.refresh.call_count, 6)
        # 2 ordinary sleeps (between 3 attempts) + 2 extended sleeps.
        self.assertEqual(self.mock_sleep.call_count, 4)

    def test_uploaded_json_verified_after_extended_propagation(self):
        # Accepted JSON upload not yet exposed by IA during ordinary
        # verification, then revealed during the extended phase.
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, b'old json bytes'),
        ])
        self.mock_upload.side_effect = lambda **kwargs: [make_response()]

        def reveal(item):
            if item.refresh.call_count >= 5:
                item.set_original(JSON_NAME, self.json_bytes)
        self.item.refresh_hook = reveal

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 2), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 3):
            locations = self.uploader.upload_to_platform()

        self.assertEqual(self.mock_upload.call_count, 1)   # no duplicate upload
        self.assertEqual(self.item.refresh.call_count, 5)
        self.assertEqual(
            [c.args[0] for c in self.mock_sleep.call_args_list], [20, 30, 45])
        self._assert_location(locations[0])

    def test_uploaded_pdf_verified_after_extended_propagation(self):
        self._set_existing_item(files=[
            original_file(PDF_NAME, b'old pdf bytes'),
            original_file(JSON_NAME, self.json_bytes),
        ])
        self.mock_upload.side_effect = lambda **kwargs: [make_response()]

        def reveal(item):
            if item.refresh.call_count >= 5:
                item.set_original(PDF_NAME, self.pdf_bytes)
        self.item.refresh_hook = reveal

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 2), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 3):
            locations = self.uploader.upload_to_platform()

        self.assertEqual(self.mock_upload.call_count, 1)   # no duplicate upload
        self.assertEqual(
            locations[0].checksum, hashlib.md5(self.pdf_bytes).hexdigest())
        self._assert_location(locations[0])

    def test_uploaded_file_propagation_times_out_when_still_stale(self):
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, b'old json bytes'),
        ])
        self.mock_upload.side_effect = lambda **kwargs: [make_response()]

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 2), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 2):
            with self.assertRaises(InternetArchiveVerificationError) as raised:
                self.uploader.upload_to_platform()

        msg = str(raised.exception)
        self.assertIn('upload propagation', msg)
        self.assertIn(JSON_NAME, msg)
        self.assertIn(hashlib.md5(self.json_bytes).hexdigest(), msg)     # expected
        self.assertIn(hashlib.md5(b'old json bytes').hexdigest(), msg)   # observed
        self.assertEqual(self.mock_upload.call_count, 1)   # no second upload

    def test_uploaded_file_propagation_times_out_when_missing(self):
        self._set_existing_item(files=[original_file(PDF_NAME, self.pdf_bytes)])
        self.mock_upload.side_effect = lambda **kwargs: [make_response()]

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 2), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 2):
            with self.assertRaises(InternetArchiveVerificationError) as raised:
                self.uploader.upload_to_platform()

        msg = str(raised.exception)
        self.assertIn('upload propagation', msg)
        self.assertIn('is missing as an original file', msg)
        self.assertEqual(self.mock_upload.call_count, 1)

    def test_refresh_failure_during_extended_phase_stops_immediately(self):
        # Phase 2 begins legitimately (uploaded JSON still stale), then the next
        # refresh fails. Stop at once and report the actual, not full, window.
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, b'old json bytes'),
        ])
        self.mock_upload.side_effect = lambda **kwargs: [make_response()]

        def fail_on_first_extended_refresh(item):
            # 1 pre-mutation + 2 ordinary refreshes, then the first extended.
            if item.refresh.call_count >= 4:
                raise ReqConnectionError('connection reset')
        self.item.refresh_hook = fail_on_first_extended_refresh

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 2), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 8):
            with self.assertRaises(InternetArchiveVerificationError) as raised:
                self.uploader.upload_to_platform()

        msg = str(raised.exception)
        self.assertIn('stopped during upload propagation', msg)
        self.assertIn('refresh failed', msg)
        self.assertIn('after 1 extended attempts (~30s)', msg)
        self.assertNotIn('8 extended attempts', msg)
        self.assertNotIn('932', msg)
        self.assertNotIn('Timed out waiting', msg)
        # Ordinary sleep (20) once, then exactly one extended sleep (30).
        self.assertEqual(
            [c.args[0] for c in self.mock_sleep.call_args_list], [20, 30])
        self.assertEqual(self.mock_upload.call_count, 1)   # no duplicate upload

    def test_metadata_drift_during_extended_phase_stops_immediately(self):
        # Phase 2 begins legitimately, then a mutable metadata field goes stale.
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, b'old json bytes'),
        ])
        self.mock_upload.side_effect = lambda **kwargs: [make_response()]

        def drift_metadata_during_extended(item):
            if item.refresh.call_count >= 4:
                item.metadata = dict(item.metadata, title='Old title')
        self.item.refresh_hook = drift_metadata_during_extended

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 2), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 8):
            with self.assertRaises(InternetArchiveVerificationError) as raised:
                self.uploader.upload_to_platform()

        msg = str(raised.exception)
        self.assertIn('stopped during upload propagation', msg)
        self.assertIn('metadata discrepancies', msg)
        self.assertIn('title', msg)
        self.assertIn('after 1 extended attempts (~30s)', msg)
        self.assertNotIn('Timed out waiting', msg)
        self.assertNotIn('932', msg)
        self.assertEqual(
            [c.args[0] for c in self.mock_sleep.call_args_list], [20, 30])
        self.assertEqual(self.mock_upload.call_count, 1)

    def test_non_uploaded_stale_file_is_not_extended(self):
        self._set_existing_item(files=[
            original_file(PDF_NAME, b'wrong pdf bytes'),
            original_file(JSON_NAME, self.json_bytes),
        ])
        desired = self.uploader.build_desired_state()

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 2):
            with self.assertRaises(InternetArchiveVerificationError) as raised:
                self.uploader._verify_final_state(
                    self.item, desired.expected_md5s, desired.metadata,
                    desired.absent_metadata_fields,
                    uploaded_file_names=frozenset())

        msg = str(raised.exception)
        self.assertIn('after 2 attempts', msg)          # ordinary, not extended
        self.assertNotIn('upload propagation', msg)
        self.assertEqual(self.mock_sleep.call_count, 1)  # only ordinary backoff

    def test_metadata_drift_does_not_get_extended_polling(self):
        metadata = dict(self._desired_metadata(), title='Old title')
        self._set_existing_item(metadata=metadata, files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, b'old json bytes'),
        ])
        self.mock_upload.side_effect = lambda **kwargs: [make_response()]
        self.item.modify_metadata.side_effect = \
            lambda *args, **kwargs: make_response()

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 2), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 5):
            with self.assertRaises(InternetArchiveVerificationError) as raised:
                self.uploader.upload_to_platform()

        msg = str(raised.exception)
        self.assertIn('after 2 attempts', msg)   # metadata drift blocks extension
        self.assertIn('title', msg)
        self.assertNotIn('upload propagation', msg)
        self.assertTrue(
            all(c.args[0] == 20 for c in self.mock_sleep.call_args_list))

    def test_refresh_failure_is_not_treated_as_propagation(self):
        self.item.refresh = MagicMock(
            side_effect=ReqConnectionError('connection reset'))
        desired = self.uploader.build_desired_state()

        with patch.object(IAUploader, 'VERIFICATION_ATTEMPTS', 2), \
                patch.object(IAUploader, 'UPLOAD_PROPAGATION_ATTEMPTS', 3):
            with self.assertRaises(InternetArchiveVerificationError) as raised:
                self.uploader._verify_final_state(
                    self.item, desired.expected_md5s, desired.metadata,
                    desired.absent_metadata_fields,
                    uploaded_file_names=frozenset({JSON_NAME}))

        msg = str(raised.exception)
        self.assertIn('after 2 attempts', msg)     # ordinary, not extended
        self.assertIn('refresh failed', msg)
        self.assertNotIn('upload propagation', msg)
        self.assertEqual(self.mock_sleep.call_count, 1)

    def test_current_item_uses_no_extended_polling(self):
        self._set_existing_item()   # fully current

        locations = self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.uploader.get_variable_from_env.assert_not_called()  # no credentials
        self.mock_sleep.assert_not_called()                      # no polling
        self._assert_location(locations[0])

    def test_extended_propagation_schedule_is_bounded(self):
        sleeps = IAUploader._upload_propagation_sleeps()
        self.assertEqual(len(sleeps), IAUploader.UPLOAD_PROPAGATION_ATTEMPTS)
        self.assertTrue(all(
            s <= IAUploader.UPLOAD_PROPAGATION_MAX_SLEEP_SECONDS for s in sleeps))
        total = sum(sleeps)
        # Materially more than the ~200s ordinary window, but bounded well under
        # the apply job's 180-minute workflow timeout.
        self.assertGreater(total, 10 * 60)
        self.assertLessEqual(total, 20 * 60)

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
            if item.refresh.call_count >= 3:
                item.metadata = dict(desired_metadata)

        self.item.refresh_hook = reveal_metadata

        locations = self.uploader.upload_to_platform()

        self.assertEqual(self.item.refresh.call_count, 3)
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
        self.assertEqual(self.item.refresh.call_count, 4)
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
        self.assertEqual(self.item.refresh.call_count, 3)
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
            if item.refresh.call_count == 1:
                return
            item.files = [
                original_file(PDF_NAME, self.pdf_bytes),
                original_file(JSON_NAME, self.json_bytes),
            ]
            if item.refresh.call_count == 2:
                item.metadata = {'title': 'Stale title'}
            else:
                item.metadata = dict(desired_metadata)

        self.item.refresh_hook = reveal_new_item

        locations = self.uploader.upload_to_platform()

        self.assertEqual(self.mock_upload.call_count, 2)
        self.item.modify_metadata.assert_not_called()
        self.assertEqual(self.item.refresh.call_count, 3)
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
                item.files = []
            elif item.refresh.call_count == 3:
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
        self.assertEqual(self.item.refresh.call_count, 4)
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

    def test_matching_marker_with_only_unrelated_collection_is_restricted(self):
        metadata = self._desired_metadata()
        metadata['collection'] = 'other-collection'
        self._set_existing_item(metadata=metadata, files=[])

        with self.assertRaises(InternetArchiveRestrictedMetadataError):
            self.uploader.upload_to_platform()

        self.uploader.get_variable_from_env.assert_not_called()
        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.item.refresh.assert_not_called()

    # --- Deterministic JSON sidecar: desired-state behaviour ---

    # A pair of raw json::thoth exports that are semantically identical but
    # carry different top-level generation timestamps. Both must normalise to
    # the canonical self.json_bytes.
    RAW_EXPORT_EARLY = (
        b'{"jsonGeneratedAt":"2026-07-27T10:00:00.000000Z",'
        b'"current":"metadata"}')
    RAW_EXPORT_LATE = (
        b'  {\n  "current": "metadata",\n'
        b'  "jsonGeneratedAt": "2026-07-27T11:30:00.000000Z"\n}\n')

    def test_build_desired_state_stores_normalised_json_bytes(self):
        self.uploader.get_formatted_metadata.return_value = self.RAW_EXPORT_EARLY

        desired = self.uploader.build_desired_state()

        self.assertEqual(desired.file_bytes[JSON_NAME], self.json_bytes)

    def test_expected_json_md5_is_calculated_from_normalised_bytes(self):
        self.uploader.get_formatted_metadata.return_value = self.RAW_EXPORT_EARLY

        desired = self.uploader.build_desired_state()

        self.assertEqual(
            desired.expected_md5s[JSON_NAME],
            hashlib.md5(self.json_bytes).hexdigest())
        # Never the MD5 of the raw timestamp-bearing export.
        self.assertNotEqual(
            desired.expected_md5s[JSON_NAME],
            hashlib.md5(self.RAW_EXPORT_EARLY).hexdigest())

    def test_uploader_sends_exact_normalised_json_bytes(self):
        self.uploader.get_formatted_metadata.return_value = self.RAW_EXPORT_LATE
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, b'legacy raw sidecar'),
        ])

        sent = {}

        def capture_upload(**kwargs):
            for name, file_object in kwargs['files'].items():
                contents = file_object.read()
                sent[name] = contents
                self.item.set_original(name, contents)
            self.item.exists = True
            return [make_response() for _ in kwargs['files']]

        self.mock_upload.side_effect = capture_upload

        self.uploader.upload_to_platform()

        self.assertEqual(set(sent), {JSON_NAME})
        self.assertEqual(sent[JSON_NAME], self.json_bytes)

    def test_two_desired_builds_differing_only_by_timestamp_share_json_md5(self):
        self.uploader.get_formatted_metadata.return_value = self.RAW_EXPORT_EARLY
        early = self.uploader.build_desired_state()

        self.uploader.get_formatted_metadata.return_value = self.RAW_EXPORT_LATE
        late = self.uploader.build_desired_state()

        self.assertEqual(early.file_bytes[JSON_NAME], late.file_bytes[JSON_NAME])
        self.assertEqual(
            early.expected_md5s[JSON_NAME], late.expected_md5s[JSON_NAME])

    def test_remote_canonical_json_is_current_despite_new_timestamp(self):
        # Remote already holds the canonical bytes; the next raw export carries
        # a different generation timestamp. The sidecar must remain current.
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, self.json_bytes),
        ])
        self.uploader.get_formatted_metadata.return_value = self.RAW_EXPORT_LATE
        desired = self.uploader.build_desired_state()

        inspection = self.uploader.inspect_item(self.item, desired)

        self.assertTrue(inspection['files'][JSON_NAME]['current'])
        self.assertTrue(inspection['files'][PDF_NAME]['current'])

    def test_legacy_timestamp_sidecar_is_stale_and_proposes_one_json_upload(self):
        # A pre-fix original still carrying the raw timestamp bytes must be
        # classified stale exactly once, proposing only the JSON upload.
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, self.RAW_EXPORT_EARLY),
        ])
        self.uploader.get_formatted_metadata.return_value = self.RAW_EXPORT_LATE
        desired = self.uploader.build_desired_state()

        inspection = self.uploader.inspect_item(self.item, desired)

        self.assertFalse(inspection['files'][JSON_NAME]['current'])
        self.assertTrue(inspection['files'][PDF_NAME]['current'])
        self.assertEqual(inspection['metadata_patch'], {})

    def test_legacy_sidecar_upload_then_reinspection_is_current(self):
        # End-to-end determinism: repair uploads the canonical bytes, and a
        # later build with yet another timestamp classifies the item current.
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, self.RAW_EXPORT_EARLY),
        ])
        self.uploader.get_formatted_metadata.return_value = self.RAW_EXPORT_LATE

        self.uploader.upload_to_platform()

        self.assertEqual(
            set(self.mock_upload.call_args.kwargs['files']), {JSON_NAME})
        # Remote now holds the canonical bytes; a fresh export with a different
        # timestamp must produce no further action.
        self.uploader.get_formatted_metadata.return_value = (
            b'{"jsonGeneratedAt":"2026-07-28T00:00:00.000000Z",'
            b'"current":"metadata"}')
        desired = self.uploader.build_desired_state()
        inspection = self.uploader.inspect_item(self.item, desired)

        self.assertTrue(inspection['files'][JSON_NAME]['current'])
        self.assertTrue(inspection['files'][PDF_NAME]['current'])
        self.assertEqual(inspection['metadata_patch'], {})

    def test_current_item_with_new_timestamp_export_makes_no_api_calls(self):
        # The current-item fast path must still read no credentials and perform
        # no upload when only the volatile timestamp changed.
        self._set_existing_item(files=[
            original_file(PDF_NAME, self.pdf_bytes),
            original_file(JSON_NAME, self.json_bytes),
        ])
        self.uploader.get_formatted_metadata.return_value = self.RAW_EXPORT_LATE

        self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.uploader.get_variable_from_env.assert_not_called()

    # --- Deterministic JSON sidecar: error behaviour via build_desired_state ---

    def test_invalid_utf8_export_raises_desired_state_error_for_json(self):
        self.uploader.get_formatted_metadata.return_value = b'\xff\xfe not utf8'

        with self.assertRaises(InternetArchiveDesiredStateError) as raised:
            self.uploader.build_desired_state()

        self.assertEqual(raised.exception.source, 'json')

    def test_malformed_json_export_raises_desired_state_error_for_json(self):
        self.uploader.get_formatted_metadata.return_value = b'{"current":'

        with self.assertRaises(InternetArchiveDesiredStateError) as raised:
            self.uploader.build_desired_state()

        self.assertEqual(raised.exception.source, 'json')

    def test_non_object_json_roots_are_rejected(self):
        for raw in (b'[1, 2, 3]', b'"a string"', b'42', b'true', b'null'):
            with self.subTest(raw=raw):
                self.uploader.get_formatted_metadata.return_value = raw
                with self.assertRaises(
                        InternetArchiveDesiredStateError) as raised:
                    self.uploader.build_desired_state()
                self.assertEqual(raised.exception.source, 'json')

    def test_non_bytes_export_response_is_rejected(self):
        self.uploader.get_formatted_metadata.return_value = {'not': 'bytes'}

        with self.assertRaises(InternetArchiveDesiredStateError) as raised:
            self.uploader.build_desired_state()

        self.assertEqual(raised.exception.source, 'json')

    def test_desired_state_json_error_includes_uuid_not_full_payload(self):
        secret_marker = 'do-not-leak-this-entire-payload-marker'
        self.uploader.get_formatted_metadata.return_value = (
            '["{}", NaN]'.format(secret_marker).encode('utf-8'))

        with self.assertRaises(InternetArchiveDesiredStateError) as raised:
            self.uploader.build_desired_state()

        message = str(raised.exception)
        self.assertEqual(raised.exception.source, 'json')
        self.assertIn(WORK_ID, message)
        self.assertNotIn(secret_marker, message)


class TestJsonSidecarNormalisation(unittest.TestCase):
    """Pure canonicalisation contract for IAUploader._normalise_json_sidecar."""

    def normalise(self, metadata_bytes):
        return IAUploader._normalise_json_sidecar(metadata_bytes)

    def test_different_top_level_timestamps_produce_identical_bytes_and_md5(self):
        early = self.normalise(
            b'{"jsonGeneratedAt":"2026-01-01T00:00:00Z","title":"Book"}')
        late = self.normalise(
            b'{"jsonGeneratedAt":"2026-12-31T23:59:59Z","title":"Book"}')

        self.assertEqual(early, late)
        self.assertEqual(
            hashlib.md5(early).hexdigest(), hashlib.md5(late).hexdigest())

    def test_key_order_and_whitespace_do_not_affect_output(self):
        compact = self.normalise(b'{"a":1,"b":2,"c":3}')
        reordered = self.normalise(
            b'{\n  "c": 3,\n  "b": 2,\n  "a": 1\n}\n')

        self.assertEqual(compact, reordered)

    def test_non_ascii_text_is_preserved(self):
        result = self.normalise(
            '{"title":"Café Möbius — 日本語"}'.encode('utf-8'))

        self.assertEqual(
            result, '{"title":"Café Möbius — 日本語"}\n'.encode('utf-8'))
        self.assertEqual(
            json.loads(result.decode('utf-8'))['title'],
            'Café Möbius — 日本語')

    def test_real_metadata_change_produces_different_bytes_and_md5(self):
        original = self.normalise(
            b'{"jsonGeneratedAt":"2026-01-01T00:00:00Z","title":"Book"}')
        changed = self.normalise(
            b'{"jsonGeneratedAt":"2026-01-01T00:00:00Z","title":"Revised"}')

        self.assertNotEqual(original, changed)
        self.assertNotEqual(
            hashlib.md5(original).hexdigest(),
            hashlib.md5(changed).hexdigest())

    def test_missing_top_level_timestamp_is_accepted(self):
        result = self.normalise(b'{"title":"Book"}')

        self.assertEqual(result, b'{"title":"Book"}\n')

    def test_nested_timestamp_field_is_preserved(self):
        result = self.normalise(
            b'{"jsonGeneratedAt":"2026-01-01T00:00:00Z",'
            b'"work":{"jsonGeneratedAt":"nested-value","id":"x"}}')

        payload = json.loads(result.decode('utf-8'))
        self.assertNotIn('jsonGeneratedAt', payload)
        self.assertEqual(payload['work']['jsonGeneratedAt'], 'nested-value')

    def test_only_top_level_timestamp_field_is_removed(self):
        result = self.normalise(
            b'{"jsonGeneratedAt":"t","keep":"me","also":"here"}')

        payload = json.loads(result.decode('utf-8'))
        self.assertEqual(payload, {'keep': 'me', 'also': 'here'})

    def test_output_is_exact_documented_canonical_representation(self):
        result = self.normalise(
            b'{\n  "b": "second",\n'
            b'  "jsonGeneratedAt": "2026-01-01T00:00:00Z",\n'
            b'  "a": "first"\n}\n')

        self.assertEqual(result, b'{"a":"first","b":"second"}\n')

    def test_output_is_idempotent(self):
        once = self.normalise(
            b'{"jsonGeneratedAt":"2026-01-01T00:00:00Z","x":[1,2,{"y":"z"}]}')
        twice = self.normalise(once)

        self.assertEqual(once, twice)

    def test_non_bytes_input_is_rejected(self):
        for value in ('a string', {'a': 'dict'}, 42, None, ['list']):
            with self.subTest(value=value):
                with self.assertRaises(
                        InternetArchiveDesiredStateError) as raised:
                    self.normalise(value)
                self.assertEqual(raised.exception.source, 'json')

    def test_invalid_utf8_is_rejected(self):
        with self.assertRaises(InternetArchiveDesiredStateError) as raised:
            self.normalise(b'{"title": "\xff\xfe"}')
        self.assertEqual(raised.exception.source, 'json')

    def test_malformed_json_is_rejected(self):
        with self.assertRaises(InternetArchiveDesiredStateError) as raised:
            self.normalise(b'{"title": ')
        self.assertEqual(raised.exception.source, 'json')

    def test_non_object_roots_are_rejected(self):
        for raw in (b'[1,2,3]', b'"a string"', b'42', b'true', b'null'):
            with self.subTest(raw=raw):
                with self.assertRaises(
                        InternetArchiveDesiredStateError) as raised:
                    self.normalise(raw)
                self.assertEqual(raised.exception.source, 'json')

    def test_values_canonical_json_refuses_are_rejected(self):
        # Python's json.loads accepts NaN/Infinity, but canonical serialisation
        # must refuse them rather than emit invalid JSON. They are now rejected
        # during parsing (parse_constant) before any non-finite float is built.
        for raw in (b'{"x": NaN}', b'{"x": Infinity}', b'{"x": -Infinity}'):
            with self.subTest(raw=raw):
                with self.assertRaises(
                        InternetArchiveDesiredStateError) as raised:
                    self.normalise(raw)
                self.assertEqual(raised.exception.source, 'json')

    def test_high_precision_decimal_is_not_rounded_through_binary_float(self):
        # Regression for the P2 precision-preservation finding: a decimal token
        # needing more precision than a binary float must survive exactly.
        # 9007199254740993.0 is not representable as a double and ordinary
        # json.loads would round it down to 9007199254740992.0.
        result = self.normalise(b'{"value":9007199254740993.0}')

        self.assertIn(b'9007199254740993.0', result)
        self.assertNotIn(b'9007199254740992.0', result)
        parsed = json.loads(result.decode('utf-8'), parse_float=Decimal)
        self.assertEqual(parsed['value'], Decimal('9007199254740993.0'))

    def test_adjacent_high_precision_decimals_stay_distinct(self):
        # The two tokens collapse to the same Python float; the canonical
        # bytes and MD5s must nonetheless differ.
        higher = self.normalise(b'{"value":9007199254740993.0}')
        lower = self.normalise(b'{"value":9007199254740992.0}')

        self.assertNotEqual(higher, lower)
        self.assertNotEqual(
            hashlib.md5(higher).hexdigest(),
            hashlib.md5(lower).hexdigest())

    def test_long_fractional_value_is_preserved_exactly(self):
        raw = b'{"value":0.123456789012345678901234567890}'
        result = self.normalise(raw)

        self.assertIn(b'0.123456789012345678901234567890', result)
        parsed = json.loads(result.decode('utf-8'), parse_float=Decimal)
        self.assertEqual(
            parsed['value'], Decimal('0.123456789012345678901234567890'))

    def test_exact_decimals_survive_in_nested_structures(self):
        raw = (
            b'{"obj":{"n":1.5},'
            b'"arr":[2.5,3.5],'
            b'"obj_in_arr":[{"n":4.5}]}')
        result = self.normalise(raw)

        parsed = json.loads(result.decode('utf-8'), parse_float=Decimal)
        self.assertEqual(parsed['obj']['n'], Decimal('1.5'))
        self.assertEqual(parsed['arr'], [Decimal('2.5'), Decimal('3.5')])
        self.assertEqual(parsed['obj_in_arr'][0]['n'], Decimal('4.5'))

    def test_negative_and_exponent_decimals_are_preserved(self):
        raw = b'{"neg":-3.14,"exp":6.022e23,"small":1e-7}'
        result = self.normalise(raw)

        parsed = json.loads(result.decode('utf-8'), parse_float=Decimal)
        self.assertEqual(parsed['neg'], Decimal('-3.14'))
        self.assertEqual(parsed['exp'], Decimal('6.022e23'))
        self.assertEqual(parsed['small'], Decimal('1e-7'))

    def test_numbers_remain_json_numbers_not_quoted_strings(self):
        result = self.normalise(
            b'{"i":42,"f":1.5,"big":9007199254740993.0}')

        # Parse without parse_float so a quoted number would surface as str.
        parsed = json.loads(result.decode('utf-8'))
        self.assertIsInstance(parsed['i'], int)
        self.assertIsInstance(parsed['f'], float)
        self.assertIsInstance(parsed['big'], float)
        self.assertNotIsInstance(parsed['big'], str)

    def test_output_with_exact_decimals_is_idempotent(self):
        once = self.normalise(
            b'{"jsonGeneratedAt":"t",'
            b'"value":9007199254740993.0,'
            b'"nested":{"long":0.123456789012345678901234567890},'
            b'"arr":[-3.14,6.022e23]}')
        twice = self.normalise(once)

        self.assertEqual(once, twice)


class TestMetadataLineEndingCanonicalisation(unittest.TestCase):
    """Part A: Internet Archive metadata string canonicalisation.

    Internet Archive collapses ``\\r\\n`` and bare ``\\r`` to ``\\n`` in managed
    metadata. We apply the same canonicalisation to desired metadata, the
    current-state comparison, patches, and final verification so a value that
    differs only by line ending never looks like a perpetual discrepancy, and
    repeatable values that collapse to the same stored string are deduplicated.
    """

    ABSENT = frozenset()

    def _uploader(self, work):
        uploader = IAUploader.__new__(IAUploader)
        uploader.work_id = WORK_ID
        uploader.version = '1.6.3'
        uploader.metadata = {'data': {'work': work}}
        return uploader

    def _work(self, **overrides):
        work = {
            'fullTitle': 'A Test Book',
            'publicationDate': '2026-01-02',
            'longAbstract': 'A long description',
            'pageCount': 10,
            'doi': 'https://doi.org/10.0000/test',
            'contributions': [
                {'fullName': 'First Author', 'mainContribution': True},
            ],
            'publications': [
                {'publicationType': 'PDF', 'publicationId': 'p',
                 'isbn': '978-1-234-56789-0'},
            ],
            'subjects': [{'subjectCode': 'ABC123'}],
            'languages': [{'languageCode': 'eng'}],
            'issues': [],
            'imprint': {'publisher': {'publisherName': 'Test Publisher'}},
        }
        work.update(overrides)
        return work

    # 1. CRLF and LF compare equal.
    def test_crlf_and_lf_compare_equal(self):
        self.assertTrue(IAUploader._metadata_values_equal(
            'title', 'Line one\r\nLine two', 'Line one\nLine two'))

    # 2. Bare CR and LF compare equal.
    def test_bare_cr_and_lf_compare_equal(self):
        self.assertTrue(IAUploader._metadata_values_equal(
            'title', 'Line one\rLine two', 'Line one\nLine two'))

    # 3. Desired metadata is built/sent using LF.
    def test_desired_metadata_uses_lf(self):
        uploader = self._uploader(self._work(
            fullTitle='Tragic\r\nHomer',
            subjects=[{'subjectCode': 'Ancient\r\nGreek Thought'}]))
        desired = uploader.parse_metadata()
        self.assertEqual(desired['title'], 'Tragic\nHomer')
        self.assertEqual(desired['subject'], ['Ancient\nGreek Thought'])
        self.assertNotIn('\r', desired['title'])
        self.assertNotIn('\r', desired['subject'][0])

    # 4. Duplicate repeatable values created by line-ending normalisation are
    #    removed; 5. first-occurrence order preserved.
    def test_repeatable_line_ending_duplicates_removed_in_order(self):
        result = IAUploader._as_metadata_list([
            'Ancient\nGreek Thought',
            'Ancient\r\nGreek Thought',
            'Classical Reception',
        ])
        self.assertEqual(
            result, ['Ancient\nGreek Thought', 'Classical Reception'])

    def test_desired_subject_deduplicates_line_ending_variants_in_order(self):
        uploader = self._uploader(self._work(subjects=[
            {'subjectCode': 'Ancient\nGreek Thought'},
            {'subjectCode': 'Ancient\r\nGreek Thought'},
            {'subjectCode': 'Classical Reception'},
        ]))
        desired = uploader.parse_metadata()
        self.assertEqual(
            desired['subject'],
            ['Ancient\nGreek Thought', 'Classical Reception'])

    # 6. Distinct repeatable values remain distinct.
    def test_distinct_repeatable_values_remain_distinct(self):
        self.assertEqual(
            IAUploader._as_metadata_list(['Alpha', 'Beta', 'Alpha ']),
            ['Alpha', 'Beta', 'Alpha '])

    # 7. A CRLF/LF duplicate subject does not produce a metadata patch.
    def test_crlf_duplicate_subject_produces_no_patch(self):
        uploader = self._uploader(self._work(subjects=[
            {'subjectCode': 'Ancient\nGreek Thought'},
            {'subjectCode': 'Ancient\r\nGreek Thought'},
        ]))
        desired = uploader.parse_metadata()
        # IA returns the single normalised value it actually stored.
        current = dict(desired)
        current['subject'] = ['Ancient\nGreek Thought']
        patch = uploader._managed_metadata_patch(current, desired)
        self.assertNotIn('subject', patch)

    # 8. Final verification accepts the IA-normalised representation.
    def test_final_verification_accepts_ia_normalised_subject(self):
        uploader = self._uploader(self._work(subjects=[
            {'subjectCode': 'Ancient\nGreek Thought'},
            {'subjectCode': 'Ancient\r\nGreek Thought'},
        ]))
        desired = uploader.parse_metadata()
        current = dict(desired)
        current['subject'] = ['Ancient\nGreek Thought']
        problems = uploader._metadata_verification_problems(
            current, desired, self.ABSENT)
        self.assertEqual(problems, [])

    # 9. A genuinely different subject still produces a patch and a problem.
    def test_genuinely_different_subject_still_flagged(self):
        uploader = self._uploader(self._work(
            subjects=[{'subjectCode': 'Ancient Greek Thought'}]))
        desired = uploader.parse_metadata()
        current = dict(desired)
        current['subject'] = ['Something Else Entirely']
        patch = uploader._managed_metadata_patch(current, desired)
        self.assertEqual(patch['subject'], ['Ancient Greek Thought'])
        problems = uploader._metadata_verification_problems(
            current, desired, self.ABSENT)
        self.assertTrue(any('subject' in problem for problem in problems))

    # 10. Non-repeatable metadata strings use the same canonicalisation.
    def test_non_repeatable_string_is_canonicalised(self):
        self.assertEqual(
            IAUploader._clean_metadata_value('a\r\nb\rc'), 'a\nb\nc')
        self.assertTrue(IAUploader._metadata_values_equal(
            'description', 'para one\r\npara two', 'para one\npara two'))

    # 11. Internal spaces and ordinary newlines are otherwise preserved.
    def test_internal_spaces_and_newlines_preserved(self):
        self.assertEqual(
            IAUploader._clean_metadata_value('a  b\nc   d'), 'a  b\nc   d')
        self.assertEqual(
            IAUploader._canonicalise_ia_string('a  b\nc'), 'a  b\nc')

    # 12a. REMOVE_TAG behaviour remains intact.
    def test_remove_tag_behaviour_intact(self):
        uploader = self._uploader(self._work())
        desired = uploader.parse_metadata()
        desired.pop('description', None)
        current = uploader.parse_metadata()
        current['description'] = 'obsolete abstract'
        patch = uploader._managed_metadata_patch(current, desired)
        self.assertEqual(patch['description'], 'REMOVE_TAG')

    # 12b. Restricted-field (initial-only) behaviour remains intact.
    def test_restricted_initial_only_still_detected(self):
        uploader = self._uploader(self._work())
        desired = uploader.parse_metadata()
        current = dict(desired)
        current['mediatype'] = 'audio'
        problems = uploader._metadata_verification_problems(
            current, desired, self.ABSENT,
            fields=IAUploader.INITIAL_ONLY_METADATA_FIELDS)
        self.assertTrue(any('mediatype' in problem for problem in problems))

    # 12c. Repeatable-field list shape is preserved through canonicalisation.
    def test_repeatable_field_stays_list(self):
        uploader = self._uploader(self._work(contributions=[
            {'fullName': 'First Author', 'mainContribution': True},
            {'fullName': 'Second Author', 'mainContribution': True},
        ]))
        desired = uploader.parse_metadata()
        self.assertEqual(desired['creator'], ['First Author', 'Second Author'])


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
