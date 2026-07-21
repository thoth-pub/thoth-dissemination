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
    def __init__(self, identifier, exists, metadata=None, files=None):
        self.identifier = identifier
        self.exists = exists
        self.metadata = dict(metadata or {})
        self.files = list(files or [])
        self.refresh = MagicMock(side_effect=self._refresh)
        self.modify_metadata = MagicMock(side_effect=self._modify_metadata)
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

        self.mock_upload.assert_called_once()
        call = self.mock_upload.call_args.kwargs
        self.assertEqual(set(call['files']), {PDF_NAME, JSON_NAME})
        self.assertEqual(call['metadata'], self._desired_metadata())
        self.assertTrue(call['checksum'])
        self.assertTrue(call['verify'])
        self.assertTrue(call['queue_derive'])
        self.item.modify_metadata.assert_not_called()
        self._assert_location(locations[0])

    def test_existing_current_item_skips_files_and_metadata(self):
        self._set_existing_item()

        locations = self.uploader.upload_to_platform()

        self.mock_upload.assert_not_called()
        self.item.modify_metadata.assert_not_called()
        self.assertGreaterEqual(self.item.refresh.call_count, 1)
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
