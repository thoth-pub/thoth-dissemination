import unittest
from unittest.mock import MagicMock

from iauploader import IAUploader
from uploader import Publication, Uploader


WORK_ID = '11111111-2222-3333-4444-555555555555'
PUBLICATION_ID = '99999999-8888-7777-6666-555555555555'


def multilingual_metadata(**legacy_fields):
    work = {
        'workId': WORK_ID,
        'titles': [{
            'canonical': True,
            'title': 'Example',
            'subtitle': 'A Study',
            'fullTitle': 'Example: A Study',
        }],
    }
    work.update(legacy_fields)
    return {'data': {'work': work}}


def internet_archive_metadata(**legacy_fields):
    metadata = multilingual_metadata(**legacy_fields)
    metadata['data']['work'].update({
        'publicationDate': '2026-01-02',
        'longAbstract': 'A long description',
        'pageCount': 250,
        'lccn': '2026000001',
        'license': 'https://creativecommons.org/licenses/by/4.0/',
        'oclc': '12345',
        'doi': 'https://doi.org/10.0000/test',
        'contributions': [{
            'fullName': 'First Author',
            'mainContribution': True,
        }],
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
                'publisherName': 'Test Publisher',
            },
        },
    })
    return metadata


class TestNormaliseWorkMetadata(unittest.TestCase):
    def test_absent_full_title_is_backfilled(self):
        metadata = multilingual_metadata()

        Uploader.normalise_work_metadata(metadata)

        self.assertEqual(
            metadata['data']['work']['fullTitle'], 'Example: A Study')

    def test_null_full_title_is_backfilled(self):
        metadata = multilingual_metadata(fullTitle=None)

        Uploader.normalise_work_metadata(metadata)

        self.assertEqual(
            metadata['data']['work']['fullTitle'], 'Example: A Study')

    def test_full_title_is_synthesised_when_canonical_value_is_absent(self):
        metadata = multilingual_metadata()
        metadata['data']['work']['titles'][0].pop('fullTitle')

        Uploader.normalise_work_metadata(metadata)

        self.assertEqual(
            metadata['data']['work']['fullTitle'], 'Example: A Study')

    def test_null_title_is_backfilled(self):
        metadata = multilingual_metadata(title=None)

        Uploader.normalise_work_metadata(metadata)

        self.assertEqual(metadata['data']['work']['title'], 'Example')

    def test_null_subtitle_is_backfilled(self):
        metadata = multilingual_metadata(subtitle=None)

        Uploader.normalise_work_metadata(metadata)

        self.assertEqual(metadata['data']['work']['subtitle'], 'A Study')

    def test_all_null_legacy_title_fields_are_backfilled(self):
        metadata = multilingual_metadata(
            title=None, subtitle=None, fullTitle=None)

        Uploader.normalise_work_metadata(metadata)

        self.assertEqual(metadata['data']['work']['title'], 'Example')
        self.assertEqual(metadata['data']['work']['subtitle'], 'A Study')
        self.assertEqual(
            metadata['data']['work']['fullTitle'], 'Example: A Study')

    def test_non_null_legacy_title_fields_are_preserved(self):
        metadata = multilingual_metadata(
            title='Legacy title',
            subtitle='Legacy subtitle',
            fullTitle='Legacy full title',
        )

        Uploader.normalise_work_metadata(metadata)

        self.assertEqual(metadata['data']['work']['title'], 'Legacy title')
        self.assertEqual(
            metadata['data']['work']['subtitle'], 'Legacy subtitle')
        self.assertEqual(
            metadata['data']['work']['fullTitle'], 'Legacy full title')

    def test_missing_multilingual_titles_keeps_legacy_state_unchanged(self):
        metadata = {
            'data': {
                'work': {
                    'title': None,
                    'subtitle': None,
                    'fullTitle': None,
                },
            },
        }

        Uploader.normalise_work_metadata(metadata)

        self.assertEqual(metadata['data']['work'], {
            'title': None,
            'subtitle': None,
            'fullTitle': None,
            'shortAbstract': None,
            'longAbstract': None,
        })


class TestIAUploaderNormalisedTitles(unittest.TestCase):
    def setUp(self):
        self.metadata = internet_archive_metadata(
            title=None, subtitle=None, fullTitle=None)
        Uploader.normalise_work_metadata(self.metadata)
        self.uploader = IAUploader.__new__(IAUploader)
        self.uploader.work_id = WORK_ID
        self.uploader.export_url = 'https://export.example'
        self.uploader.version = '1.5.0'
        self.uploader.metadata = self.metadata

    def test_null_full_title_produces_valid_archive_title(self):
        parsed = self.uploader.parse_metadata()

        self.assertEqual(parsed['title'], 'Example: A Study')

    def test_null_full_title_does_not_reject_desired_state(self):
        self.uploader.get_formatted_metadata = MagicMock(
            return_value=b'{"example":"metadata"}')
        self.uploader.get_publication_details = MagicMock(
            return_value=Publication(
                'PDF',
                PUBLICATION_ID,
                b'PDF bytes',
                '.pdf',
                'https://source.example/book.pdf',
            ))

        desired = self.uploader.build_desired_state()

        self.assertEqual(desired.metadata['title'], 'Example: A Study')
        self.assertEqual(desired.publication_id, PUBLICATION_ID)


if __name__ == '__main__':
    unittest.main()
