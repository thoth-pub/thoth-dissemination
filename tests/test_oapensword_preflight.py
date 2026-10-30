"""Offline tests for the OAPEN SWORD duplicate preflight (DIS-OAPEN-SAFE-01B, #123).

Thoth metadata follows the shape of thothlibrary's raw ``work_by_id`` response
that uploaders receive, and OAPEN results follow the ``/rest/search`` responses
returned by library.oapen.org on 2026-10-26. Apart from the public index control
records, identifiers are synthetic. Thoth, OAPEN search and SWORD are all faked:
no test sends a network request or needs credentials.
"""

import json
import os
import re
import time
import unittest
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import requests

from culuploader import CULUploader
from errors import DisseminationError
from oapensworduploader import (
    OAPEN_MAX_RESULTS,
    OAPEN_PAGE_SIZE,
    OAPEN_SEARCH_URL,
    OAPEN_USER_AGENT,
    OAPENCoverageError,
    OAPENLibraryIndex,
    OAPENLookupError,
    OAPENSWORDUploader,
    PreflightDecision,
    PreflightGatedSwordApi,
    extends_doi,
    find_dois,
    normalise_doi,
    normalise_isbn,
    normalise_text,
)
from swordv2uploader import SwordV2Uploader
from uploader import Publication, Uploader


ROOT = Path(__file__).resolve().parents[1]

WORK_ID = '0f3a5c7e-1b2d-4e6f-8a9b-0c1d2e3f4a5b'
OTHER_WORK_ID = 'aaaabbbb-cccc-4ddd-8eee-ffff00001111'
PUBLISHER_ID = '5e1f0a2b-3c4d-4e5f-8a6b-7c8d9e0f1a2b'
PDF_PUBLICATION_ID = '1d2c3b4a-5e6f-4a7b-8c9d-0e1f2a3b4c5d'
DOI_URL = 'https://doi.org/10.99999/EXAMPLE.0001'
DOI = '10.99999/example.0001'
PAPERBACK_ISBN = '978-1-9999-0001-4'
PDF_ISBN = '978-1-9999-0002-1'
EPUB_ISBN = '978-1-9999-0003-8'
TITLE = 'A Study of Examples'
SUBTITLE = 'Evidence and Method'
PUBLISHER = 'Example Press'
PDF_URL = 'https://publisher.example/books/0001.pdf'
NEW_HANDLE = '20.500.12657/999001'


def thoth_metadata(doi=DOI_URL, isbns=(PAPERBACK_ISBN, PDF_ISBN, EPUB_ISBN),
                   pdf_locations=None, title=TITLE, publisher=PUBLISHER,
                   work_id=WORK_ID):
    """Build normalised Thoth work metadata as the uploader holds it."""
    formats = ('PAPERBACK', 'PDF', 'EPUB')
    publications = []
    for publication_type, isbn in zip(formats, isbns + (None,) * len(formats)):
        if publication_type == 'PDF':
            locations = [{
                '__typename': 'Location',
                'locationId': '9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d',
                'landingPage': 'https://publisher.example/books/0001',
                'fullTextUrl': PDF_URL,
                'locationPlatform': 'THOTH',
                'canonical': True,
            }] + list(pdf_locations or [])
            publication_id = PDF_PUBLICATION_ID
        else:
            locations = [{
                '__typename': 'Location',
                'locationId': '7b6a5c4d-3e2f-4a1b-8c0d-9e8f7a6b5c4d',
                'landingPage': 'https://publisher.example/books/0001',
                'fullTextUrl': None,
                'locationPlatform': 'PUBLISHER_WEBSITE',
                'canonical': True,
            }]
            publication_id = '{}-0000-4000-8000-00000000000{}'.format(
                '6f5e4d3c', formats.index(publication_type))
        publications.append({
            '__typename': 'Publication',
            'publicationId': publication_id,
            'publicationType': publication_type,
            'isbn': isbn,
            'accessibilityStandard': None,
            'accessibilityAdditionalStandard': None,
            'accessibilityException': None,
            'accessibilityReportUrl': None,
            'file': None,
            'locations': locations,
        })
    work = {
        '__typename': 'Work',
        'workId': work_id,
        'workType': 'MONOGRAPH',
        'workStatus': 'ACTIVE',
        'doi': doi,
        'edition': 1,
        'publicationDate': '2026-10-23',
        'withdrawnDate': None,
        'place': 'Cambridge, UK',
        'pageCount': 158,
        'license': 'https://creativecommons.org/licenses/by/4.0/',
        'landingPage': 'https://publisher.example/books/0001',
        'oclc': None,
        'titles': [{
            '__typename': 'Title',
            'titleId': '2b3c4d5e-6f7a-4b8c-9d0e-1f2a3b4c5d6e',
            'localeCode': 'EN',
            'fullTitle': '{}: {}'.format(title, SUBTITLE),
            'title': title,
            'subtitle': SUBTITLE,
            'canonical': True,
        }],
        'abstracts': [{
            '__typename': 'Abstract',
            'abstractId': '3c4d5e6f-7a8b-4c9d-8e0f-1a2b3c4d5e6f',
            'localeCode': 'EN',
            'content': 'A long abstract.',
            'abstractType': 'LONG',
            'canonical': True,
        }],
        'languages': [{'__typename': 'Language', 'languageId': 'a0113ae4-de06-492c-b4a2-fe5e3bcf9aab',
                       'languageCode': 'ENG', 'languageRelation': 'ORIGINAL'}],
        'subjects': [
            {'__typename': 'Subject', 'subjectId': '4d5e6f7a-8b9c-4d0e-8f1a-2b3c4d5e6f7a',
             'subjectType': 'THEMA', 'subjectCode': 'JP', 'subjectOrdinal': 1},
            {'__typename': 'Subject', 'subjectId': '5e6f7a8b-9c0d-4e1f-8a2b-3c4d5e6f7a8b',
             'subjectType': 'KEYWORD', 'subjectCode': 'examples', 'subjectOrdinal': 1},
        ],
        'contributions': [{
            '__typename': 'Contribution',
            'contributionId': '6f7a8b9c-0d1e-4f2a-8b3c-4d5e6f7a8b9c',
            'contributorId': '7a8b9c0d-1e2f-4a3b-8c4d-5e6f7a8b9c0d',
            'contributionType': 'AUTHOR',
            'mainContribution': True,
            'firstName': 'Ada',
            'lastName': 'Example',
            'fullName': 'Ada Example',
            'contributionOrdinal': 1,
            'biographies': [],
            'affiliations': [],
            'contributor': {'__typename': 'Contributor', 'contributorId': '7a8b9c0d-1e2f-4a3b-8c4d-5e6f7a8b9c0d',
                            'firstName': 'Ada', 'lastName': 'Example', 'fullName': 'Ada Example',
                            'orcid': None, 'website': None},
        }],
        'relations': [],
        'references': [],
        'fundings': [],
        'issues': [],
        'imprint': {
            '__typename': 'Imprint',
            'imprintId': '8b9c0d1e-2f3a-4b4c-8d5e-6f7a8b9c0d1e',
            'imprintName': publisher,
            'publisherId': PUBLISHER_ID,
            'publisher': {'__typename': 'Publisher', 'publisherId': PUBLISHER_ID,
                          'publisherName': publisher, 'publisherShortname': 'EP',
                          'publisherUrl': 'https://publisher.example/'},
        },
        'publications': publications,
    }
    metadata = {'data': {'work': work}}
    Uploader.normalise_work_metadata(metadata)
    return metadata


def oapen_location(handle=NEW_HANDLE, platform='OAPEN'):
    return {
        '__typename': 'Location',
        'locationId': '0c1d2e3f-4a5b-4c6d-8e7f-8a9b0c1d2e3f',
        'landingPage': 'https://library.oapen.org/handle/{}'.format(handle),
        'fullTextUrl': None,
        'locationPlatform': platform,
        'canonical': False,
    }


def metadata_entry(key, value, language=None):
    schema, element, *qualifier = key.split('.')
    return {'key': key, 'value': value, 'language': language, 'schema': schema,
            'element': element, 'qualifier': qualifier[0] if qualifier else None}


def oapen_item(handle, doi=None, isbns=(), work_id=None, title=TITLE,
               alternative=SUBTITLE, publisher=PUBLISHER, withdrawn='false'):
    """Build one OAPEN REST search result as returned with expand=metadata."""
    metadata = [metadata_entry('dc.title', title)]
    if alternative is not None:
        metadata.append(metadata_entry('dc.title.alternative', alternative))
    metadata.append(metadata_entry('dc.identifier.uri',
                                   'https://library.oapen.org/handle/{}'.format(handle)))
    if doi is not None:
        metadata.append(metadata_entry('oapen.identifier.doi', doi))
    if publisher is not None:
        metadata.append(metadata_entry('publisher.name', publisher))
    if work_id is not None:
        metadata.append(metadata_entry('oapen.identifier', 'thoth-work-id:{}'.format(work_id)))
    for isbn in isbns:
        metadata.append(metadata_entry('oapen.identifier.isbn', isbn, ''))
    uuid = '{:08x}-0000-4000-8000-000000000000'.format(abs(hash(handle)) % (1 << 32))
    return {
        'uuid': uuid,
        'name': title,
        'handle': handle,
        'type': 'item',
        'expand': ['parentCollection', 'parentCollectionList', 'parentCommunityList',
                   'bitstreams', 'all'],
        'lastModified': '2026-10-26 02:45:23.652',
        'parentCollection': None,
        'parentCollectionList': None,
        'parentCommunityList': None,
        'bitstreams': None,
        'archived': 'true',
        'withdrawn': withdrawn,
        'link': '/rest/items/{}'.format(uuid),
        'metadata': metadata,
    }


def control_items():
    """The public records that INDEX_CONTROLS expects OAPEN to return."""
    return [
        oapen_item('20.500.12657/25287', doi='10.26530/OAPEN_1004809', isbns=('9789085551201',),
                   title='The deliverance of open access books',
                   alternative='Examining usage and dissemination',
                   publisher='Amsterdam University Press'),
        # A Thoth SWORD deposit of a new title, accessioned 2026-05-18
        oapen_item('20.500.12657/113008', doi='10.11647/OBP.0517',
                   work_id='4344e3cc-b92a-4375-816c-56ce99e29c43',
                   title='Open Book Publishers control record', alternative=None,
                   publisher='Open Book Publishers'),
    ]


class FakeResponse:
    def __init__(self, status_code=200, body=None, text=None):
        self.status_code = status_code
        self._body = body
        self._text = text

    def json(self):
        if self._text is not None:
            return json.loads(self._text)
        return self._body


class FakeRemote:
    """OAPEN as seen through its REST search API and SWORD endpoint."""

    def __init__(self, items=(), controls=True):
        self.items = list(items) + (control_items() if controls else [])
        self.events = []
        self.searches = []
        # Keyed by field (every search on it) or by exact query: an exception to
        # raise, a response to return for every page, or a list of items that the
        # server returns for the query in place of its own matching
        self.failures = {}
        # Like the live API's default, the server may cap a page below `limit`
        self.server_page_cap = 100
        # Number of initial requests answered with HTTP 200 and no results
        self.silent_requests = 0
        self.index_on_complete = None
        self.pauses = []

    def pause(self, seconds):
        self.pauses.append(seconds)

    def get(self, url, params=None, headers=None, timeout=None):
        assert url == OAPEN_SEARCH_URL
        self.searches.append((params, headers, timeout))
        query = params['query']
        field, _, quoted = query.partition(':')
        self.events.append('search {}'.format(field))
        failure = self.failures.get(query, self.failures.get(field))
        if isinstance(failure, BaseException):
            raise failure
        if failure is not None and not isinstance(failure, list):
            return failure
        if len(self.searches) <= self.silent_requests:
            return FakeResponse(200, [])
        if isinstance(failure, list):
            matches = failure
        else:
            # Like OAPEN, match the quoted phrase's words anywhere within a field value
            wanted = ' {} '.format(normalise_text(quoted[1:-1]))
            matches = [item for item in self.items
                       if any(entry['key'] == field and wanted in ' {} '.format(
                           normalise_text(entry['value']) or '')
                           for entry in item['metadata'])]
        offset = params['offset']
        limit = min(params['limit'], self.server_page_cap)
        return FakeResponse(200, matches[offset:offset + limit])


class FakeSwordApi:
    """Stand-in for SwordV2Api that records calls instead of contacting SWORD."""

    def __init__(self, remote, user_name, user_pass, filename_root,
                 service_document_iri, collection_iri):
        self.remote = remote
        self.collection_iri = collection_iri
        self.filename_root = filename_root
        self.create_error = None
        self.upload_pdf_error = None
        self.delete_error = None
        # Like SwordV2Api, the raw connection and request helpers can also create items
        self.conn = SimpleNamespace(create=partial(self.raw_request, 'conn.create'))
        self.send_request = partial(self.raw_request, 'send_request')
        self.handle_request = partial(self.raw_request, 'handle_request')

    def raw_request(self, name, *args, **kwargs):
        self.remote.events.append(name)

    def create_item(self, metadata_entry):
        self.remote.events.append('create_item')
        if self.create_error is not None:
            raise self.create_error
        return SimpleNamespace(edit='https://library.oapen.org/swordv2/edit/abc',
                               edit_media='https://library.oapen.org/swordv2/edit-media/abc')

    def upload_metadata(self, edit_media_iri, payload):
        self.remote.events.append('upload_metadata')

    def upload_pdf(self, edit_media_iri, payload):
        self.remote.events.append('upload_pdf')
        if self.upload_pdf_error is not None:
            raise self.upload_pdf_error
        return SimpleNamespace(location='https://library.oapen.org/swordv2/edit-media/abc/x.pdf')

    def complete_deposit(self, se_iri):
        self.remote.events.append('complete_deposit')
        if self.remote.index_on_complete is not None:
            self.remote.items.append(self.remote.index_on_complete)
        return SimpleNamespace(alternate='https://library.oapen.org/handle/{}'.format(NEW_HANDLE))

    def delete_item(self, resource_iri):
        self.remote.events.append('delete_item')
        if self.delete_error is not None:
            raise self.delete_error


CREDENTIALS = {
    'oapen_sword_user': 'test-user', 'oapen_sword_pw': 'test-password',
    'cul_pilot_user': 'test-user', 'cul_pilot_pw': 'test-password',
}


def make_uploader(remote, metadata=None, uploader_class=OAPENSWORDUploader, work_id=WORK_ID):
    metadata = thoth_metadata() if metadata is None else metadata
    with patch.object(Uploader, 'get_thoth_metadata', return_value=metadata), \
            patch.dict(os.environ, CREDENTIALS), \
            patch('swordv2uploader.SwordV2Api', new=partial(FakeSwordApi, remote)):
        uploader = uploader_class(work_id, 'https://export.example', None, '0.0.0-test')
    if isinstance(uploader, OAPENSWORDUploader):
        uploader.oapen_index = OAPENLibraryIndex(http_get=remote.get, pause=remote.pause)

    def formatted_metadata(format):
        remote.events.append('export_metadata')
        return b'{}'

    def publication_details(publication_type):
        remote.events.append('pdf_download')
        return Publication('PDF', PDF_PUBLICATION_ID, b'%PDF-1.7\n', '.pdf', PDF_URL)

    uploader.get_formatted_metadata = formatted_metadata
    uploader.get_publication_details = publication_details
    return uploader


class OfflineTestCase(unittest.TestCase):
    def setUp(self):
        # Any real HTTP request through `requests` fails the test
        network = patch('requests.sessions.Session.request',
                        side_effect=AssertionError('unexpected network request'))
        network.start()
        self.addCleanup(network.stop)

    def assertNoCreate(self, remote):
        self.assertNotIn('create_item', remote.events)
        self.assertNotIn('pdf_download', remote.events)

    def assertHeld(self, uploader, remote, decision):
        """The upload stops with `decision` before any PDF download or SWORD request."""
        with self.assertRaises(DisseminationError) as raised:
            uploader.upload_to_platform()
        self.assertIn(decision.value, str(raised.exception))
        self.assertNoCreate(remote)
        return str(raised.exception)

    def assertCreatedOnce(self, uploader, remote):
        uploader.upload_to_platform()
        self.assertEqual(remote.events.count('create_item'), 1)


class IdentifierNormalisationTests(unittest.TestCase):
    def test_doi_forms_fold_to_one_bare_lower_case_value(self):
        for value in ('https://doi.org/10.99999/EXAMPLE.0001', 'http://dx.doi.org/10.99999/example.0001',
                      'https://www.doi.org/10.99999/example.0001', 'doi.org/10.99999/example.0001',
                      'doi:10.99999/Example.0001', 'DOI 10.99999/example.0001',
                      ' 10.99999/EXAMPLE.0001. ', '10.99999/example.0001;',
                      '10.99999/EXAMPLE.0001)', '(10.99999/EXAMPLE.0001)', '10.99999/EXAMPLE.0001"',
                      "'10.99999/example.0001'", '<10.99999/EXAMPLE.0001>', '[10.99999/example.0001]'):
            self.assertEqual(normalise_doi(value), DOI, value)

    def test_every_doi_in_a_value_is_read(self):
        self.assertEqual(find_dois('10.26530/OAPEN_1;10.99999/EXAMPLE.0001'),
                         ['10.26530/oapen_1', DOI])
        self.assertEqual(find_dois('doi:10.1111/a, https://doi.org/10.2222/b'),
                         ['10.1111/a', '10.2222/b'])
        # Balanced brackets and semicolons inside a DOI are kept
        self.assertEqual(find_dois('10.1002/(SICI)1097-4571(199806)49:8<693::AID-ASI4>3.0.CO;2-0'),
                         ['10.1002/(sici)1097-4571(199806)49:8<693::aid-asi4>3.0.co;2-0'])
        self.assertEqual(find_dois('10.1000/foo(bar)'), ['10.1000/foo(bar)'])

    def test_only_a_continuation_after_a_separator_extends_a_doi(self):
        for doi in ('10.99999/example.0001.01', '10.99999/example.0001/c1',
                    '10.99999/example.0001-2', '10.99999/example.0001_ch1'):
            self.assertTrue(extends_doi(doi, DOI), doi)
        for doi in (DOI, '10.99999/example.00012', '10.99999/example.0001-',
                    '10.99999/example.0001_', '10.99999/example.0001.-',
                    '10.99999/example.0001?', '10.99999/example.0001?x',
                    '10.99999/example.0001#c1', '10.99999/example.0001?-a',
                    '10.99999/other.0001.01', '10.99999/example.0002.01'):
            self.assertFalse(extends_doi(doi, DOI), doi)

    def test_invalid_dois_are_rejected(self):
        for value in (None, '', 'example.0001', 'https://doi.org/', 'https://doi.org/not-a-doi',
                      '10.1/x', '10.99999/', '10 99999 example 0001', 'x10.99999/example.0001'):
            self.assertIsNone(normalise_doi(value), value)

    def test_isbns_are_compared_as_validated_isbn13(self):
        self.assertEqual(normalise_isbn('978-1-9999-0002-1'), '9781999900021')
        self.assertEqual(normalise_isbn('978 1 9999 0002 1'), '9781999900021')
        self.assertEqual(normalise_isbn('0-306-40615-2'), '9780306406157')
        for value in ('978-1-9999-0002-2', '0-306-40615-3', '12345', None, 'ISBN'):
            self.assertIsNone(normalise_isbn(value), value)


class OAPENLibraryIndexTests(OfflineTestCase):
    def index(self, remote):
        return OAPENLibraryIndex(http_get=remote.get, pause=remote.pause)

    def test_search_sends_a_fielded_phrase_with_the_allowlisted_agent(self):
        remote = FakeRemote([oapen_item('20.500.12657/1', title='The "Real" Thing')])

        items = self.index(remote).search('dc.title', 'The "Real" Thing')

        self.assertEqual([item['handle'] for item in items], ['20.500.12657/1'])
        params, headers, timeout = remote.searches[0]
        # Quotes are dropped rather than escaped: phrase matching ignores them
        self.assertEqual(params, {'query': 'dc.title:"The Real Thing"', 'expand': 'metadata',
                                  'limit': 100, 'offset': 0})
        self.assertEqual(headers, {'Accept': 'application/json', 'User-Agent': OAPEN_USER_AGENT})
        self.assertEqual(timeout, (10, 30))

    def test_paging_continues_until_an_empty_page_and_requests_are_paced(self):
        items = [oapen_item('20.500.12657/{}'.format(n), doi='10.99999/x.{}'.format(n))
                 for n in range(OAPEN_PAGE_SIZE + 3)]
        remote = FakeRemote(items, controls=False)

        found = self.index(remote).search('oapen.identifier.doi', '10.99999/x')

        self.assertEqual(len(found), OAPEN_PAGE_SIZE + 3)
        self.assertEqual([params['offset'] for (params, _, _) in remote.searches], [0, 100, 103])
        self.assertEqual(remote.pauses, [1, 1])

    def test_a_server_page_cap_below_the_requested_limit_does_not_truncate_results(self):
        items = [oapen_item('20.500.12657/{}'.format(n), doi='10.99999/x.{}'.format(n))
                 for n in range(120)]
        remote = FakeRemote(items, controls=False)
        remote.server_page_cap = 50

        found = self.index(remote).search('oapen.identifier.doi', '10.99999/x')

        self.assertEqual(len(found), 120)
        self.assertEqual([params['offset'] for (params, _, _) in remote.searches],
                         [0, 50, 100, 120])

    def test_results_beyond_the_maximum_are_incomplete_coverage(self):
        items = [oapen_item('20.500.12657/{}'.format(n), title='Common Title {}'.format(n))
                 for n in range(OAPEN_MAX_RESULTS + 1)]
        remote = FakeRemote(items, controls=False)

        with self.assertRaisesRegex(OAPENCoverageError, 'coverage incomplete'):
            self.index(remote).search('dc.title', 'Common Title')
        self.assertEqual(len(remote.searches), 6)

    def test_results_shifting_between_pages_are_an_error(self):
        items = [oapen_item('20.500.12657/{}'.format(n), doi='10.99999/x.{}'.format(n))
                 for n in range(OAPEN_PAGE_SIZE + 3)]
        remote = FakeRemote(items, controls=False)
        get = remote.get

        def shifted_get(url, params=None, headers=None, timeout=None):
            response = get(url, params, headers, timeout)
            if params['offset'] == 100:
                # An item from the first page reappears, so another was skipped
                response._body = [items[99]] + response._body
            return response

        index = OAPENLibraryIndex(http_get=shifted_get, pause=remote.pause)
        with self.assertRaisesRegex(OAPENLookupError, 'returned item 20.500.12657/99 twice'):
            index.search('oapen.identifier.doi', '10.99999/x')

    def test_a_failure_on_a_later_page_is_an_error(self):
        items = [oapen_item('20.500.12657/{}'.format(n), doi='10.99999/x.{}'.format(n))
                 for n in range(OAPEN_PAGE_SIZE + 3)]
        remote = FakeRemote(items, controls=False)
        get = remote.get

        def failing_get(url, params=None, headers=None, timeout=None):
            if params['offset'] > 0:
                return FakeResponse(502)
            return get(url, params, headers, timeout)

        index = OAPENLibraryIndex(http_get=failing_get, pause=remote.pause)
        with self.assertRaisesRegex(OAPENLookupError, 'HTTP status 502'):
            index.search('oapen.identifier.doi', '10.99999/x')

    def test_unusable_responses_raise_without_echoing_urls_or_bodies(self):
        cases = {
            'HTTP 403': FakeResponse(403, text='Your address is not allowed to access the API'),
            'HTTP 429': FakeResponse(429, text='Too many requests'),
            'HTTP 500': FakeResponse(500, text='<html>Internal error at https://x</html>'),
            'HTTP 503': FakeResponse(503, text=''),
            # An error status is never an empty result, even with a JSON list body
            'HTTP 503 with an empty list': FakeResponse(503, body=[]),
            'HTTP 429 with an empty list': FakeResponse(429, body=[]),
            'timeout': requests.Timeout('Read timed out: https://library.oapen.org/rest/search?q'),
            'connection': requests.ConnectionError('Connection refused: https://library.oapen.org/'),
            'not JSON': FakeResponse(200, text='<html>maintenance</html>'),
            'JSON object': FakeResponse(200, body={'items': []}),
            'oversized page': FakeResponse(200, body=[{}] * (OAPEN_PAGE_SIZE + 1)),
        }
        for name, failure in cases.items():
            with self.subTest(name):
                remote = FakeRemote()
                remote.failures['oapen.identifier.doi'] = failure
                with self.assertRaises(OAPENLookupError) as raised:
                    self.index(remote).search('oapen.identifier.doi', DOI)
                self.assertNotIn('http', str(raised.exception))
                self.assertNotIn('allowed', str(raised.exception))


class PreflightDecisionTests(OfflineTestCase):
    def test_new_work_with_a_complete_verified_negative_search_is_created_once(self):
        remote = FakeRemote()
        uploader = make_uploader(remote)

        locations = uploader.upload_to_platform()

        self.assertEqual(remote.events.count('create_item'), 1)
        searched = [event for event in remote.events if event.startswith('search')]
        self.assertEqual(searched, [
            # Index control before the work's own searches
            'search oapen.identifier.doi',
            'search oapen.identifier.doi', 'search oapen.identifier',
            'search oapen.identifier.isbn', 'search oapen.identifier.isbn',
            'search oapen.identifier.isbn', 'search dc.title',
            # Index controls after them, because they were all empty
            'search oapen.identifier.doi', 'search oapen.identifier.isbn',
            'search oapen.identifier', 'search dc.title',
        ])
        queries = [params['query'] for (params, _, _) in remote.searches[1:7]]
        self.assertEqual(queries, [
            'oapen.identifier.doi:"10.99999/example.0001"',
            'oapen.identifier:"thoth-work-id:{}"'.format(WORK_ID),
            'oapen.identifier.isbn:"9781999900014"',
            'oapen.identifier.isbn:"9781999900021"',
            'oapen.identifier.isbn:"9781999900038"',
            'dc.title:"A Study of Examples"',
        ])
        self.assertEqual([str(location) for location in locations], [
            '{} OAPEN https://library.oapen.org/handle/{} https://library.oapen.org/bitstream/'
            'handle/{}/{}.pdf?sequence=2&isAllowed=y None None'.format(
                PDF_PUBLICATION_ID, NEW_HANDLE, NEW_HANDLE, WORK_ID)])

    def test_same_doi_in_another_form_prevents_create(self):
        for doi in ('https://doi.org/10.99999/Example.0001', 'https://www.doi.org/10.99999/example.0001',
                    'DOI 10.99999/example.0001', '10.99999/EXAMPLE.0001.', '10.99999/EXAMPLE.0001)',
                    '10.99999/EXAMPLE.0001"', '<10.99999/EXAMPLE.0001>',
                    '10.26530/OAPEN_1;10.99999/example.0001'):
            for title in (TITLE, 'Other'):
                with self.subTest(doi=doi, title=title):
                    remote = FakeRemote([oapen_item('20.500.12657/500', doi=doi, title=title)])

                    reason = self.assertHeld(make_uploader(remote), remote,
                                             PreflightDecision.RECONCILE_EXISTING)

                    self.assertIn('20.500.12657/500 was not deposited by Thoth', reason)

    def test_doi_search_hit_with_another_doi_needs_review_unless_it_extends_ours(self):
        # The live search has only been seen returning longer DOIs, such as chapters'
        query = 'oapen.identifier.doi:"10.99999/example.0001"'
        cases = {
            'different DOI': ('10.99999/other.0001', PreflightDecision.AMBIGUOUS),
            'ours as a later part': ('10.5555/10.99999/example.0001x', PreflightDecision.AMBIGUOUS),
            'ours without separator': ('10.99999/example.00012', PreflightDecision.AMBIGUOUS),
            # Decorations that are not a separator, or a separator not directly after ours
            'resolver query string': ('https://doi.org/10.99999/EXAMPLE.0001?locatt=mode:legacy',
                                      PreflightDecision.AMBIGUOUS),
            'fragment': ('10.99999/EXAMPLE.0001#c1', PreflightDecision.AMBIGUOUS),
            'separator after other characters': ('10.99999/EXAMPLE.0001?-a',
                                                 PreflightDecision.AMBIGUOUS),
        }
        for name, (doi, decision) in cases.items():
            with self.subTest(name):
                remote = FakeRemote()
                remote.failures[query] = [oapen_item('20.500.12657/524', doi=doi, title='Other')]

                reason = self.assertHeld(make_uploader(remote), remote, decision)

                self.assertIn('20.500.12657/524 was returned by an identifier search', reason)

    def test_our_doi_with_a_stray_trailing_separator_needs_review(self):
        # Nothing follows the separator, so this is not a chapter DOI
        for doi in ('10.99999/EXAMPLE.0001-', '10.99999/EXAMPLE.0001_', '10.99999/EXAMPLE.0001.-',
                    '10.99999/EXAMPLE.0001-"', '10.99999/EXAMPLE.0001_)'):
            for title in (TITLE, 'Other'):
                with self.subTest(doi=doi, title=title):
                    remote = FakeRemote([oapen_item('20.500.12657/529', doi=doi, title=title)])

                    reason = self.assertHeld(make_uploader(remote), remote,
                                             PreflightDecision.AMBIGUOUS)

                    self.assertIn('20.500.12657/529 was returned by an identifier search', reason)

    def test_doi_search_hit_needs_every_doi_beginning_with_ours_to_extend_it(self):
        # A chapter DOI does not explain another value that only begins with ours
        item = oapen_item('20.500.12657/530', doi='10.99999/EXAMPLE.0001.01', title='Other')
        item['metadata'].append(metadata_entry('oapen.identifier.doi', '10.99999/EXAMPLE.0001?'))
        remote = FakeRemote([item])

        self.assertHeld(make_uploader(remote), remote, PreflightDecision.AMBIGUOUS)

    def test_doi_search_hit_needs_every_doi_value_readable(self):
        # One value extends ours, as a chapter DOI does, but another cannot be read
        item = oapen_item('20.500.12657/525', doi='10.99999/EXAMPLE.0001.01', title='Other')
        item['metadata'].append(metadata_entry('oapen.identifier.doi', 'see printed edition'))
        remote = FakeRemote([item])

        self.assertHeld(make_uploader(remote), remote, PreflightDecision.AMBIGUOUS)

    def test_isbn_search_hit_without_our_isbn_needs_review(self):
        # An exact-value ISBN search has no harmless reason to return other ISBNs
        query = 'oapen.identifier.isbn:"9781999900021"'
        for name, isbns in {'other ISBN': ('9781999900069',), 'no ISBN': ()}.items():
            with self.subTest(name):
                remote = FakeRemote()
                remote.failures[query] = [oapen_item('20.500.12657/526', isbns=isbns, title='Other')]

                self.assertHeld(make_uploader(remote), remote, PreflightDecision.AMBIGUOUS)

    def test_identifier_search_overflow_is_unknown_remote_state(self):
        chapters = [oapen_item('20.500.12657/8{}'.format(n), doi='10.99999/EXAMPLE.0001.{}'.format(n),
                               title='Chapter {}'.format(n))
                    for n in range(OAPEN_MAX_RESULTS + 1)]
        remote = FakeRemote(chapters)

        reason = self.assertHeld(make_uploader(remote), remote,
                                 PreflightDecision.UNKNOWN_REMOTE_STATE)

        self.assertIn('search on oapen.identifier.doi returned more than 500 results', reason)

    def test_same_isbn_without_a_doi_prevents_create(self):
        remote = FakeRemote([oapen_item('20.500.12657/501', isbns=('9781999900021',),
                                        title='Different Catalogue Title')])
        uploader = make_uploader(remote, thoth_metadata(doi=None))

        self.assertHeld(uploader, remote, PreflightDecision.RECONCILE_EXISTING)
        self.assertNotIn('oapen.identifier.doi:"10.99999/example.0001"',
                         [params['query'] for (params, _, _) in remote.searches])

    def test_any_one_of_several_isbns_prevents_create(self):
        # OAPEN holds only the EPUB ISBN, in various annotated forms, and no DOI
        for value in ('9781999900038', '9781999900038 (EPUB)', 'ISBN 9781999900038',
                      '9781999900069 9781999900038'):
            with self.subTest(value):
                remote = FakeRemote([oapen_item('20.500.12657/502', isbns=(value,), title='Other')])
                self.assertHeld(make_uploader(remote), remote, PreflightDecision.RECONCILE_EXISTING)

    def test_hyphenated_isbn10_on_a_legacy_record_matches_the_isbn13(self):
        # Found by its title; 1-9999-0001-4 is the ISBN-10 form of 978-1-9999-0001-4
        remote = FakeRemote([oapen_item('20.500.12657/503', isbns=('1-9999-0001-4',))])

        self.assertHeld(make_uploader(remote), remote, PreflightDecision.RECONCILE_EXISTING)

    def test_thoth_deposit_without_a_thoth_location_is_reconciled_not_recreated(self):
        # OAPEN does not store ISBNs on Thoth SWORD deposits
        remote = FakeRemote([oapen_item('20.500.12657/504', doi='10.99999/EXAMPLE.0001',
                                        work_id=WORK_ID, alternative=None)])

        reason = self.assertHeld(make_uploader(remote), remote, PreflightDecision.RECONCILE_EXISTING)

        self.assertIn('was deposited by Thoth for this work', reason)

    def test_thoth_marker_alone_identifies_a_deposit(self):
        cases = {
            'no DOI recorded': (oapen_item('20.500.12657/516', work_id=WORK_ID, title='Old Title'),
                                PreflightDecision.RECONCILE_EXISTING),
            # Deposited before the work's DOI was corrected in Thoth
            'older DOI recorded': (oapen_item('20.500.12657/517', doi='10.99999/old.0001',
                                              work_id=WORK_ID, title='Old Title'),
                                   PreflightDecision.CONFLICT),
        }
        for name, (item, decision) in cases.items():
            with self.subTest(name):
                remote = FakeRemote([item])
                self.assertHeld(make_uploader(remote), remote, decision)

    def test_existing_thoth_oapen_location_stops_before_any_oapen_request(self):
        handle_url = 'https://library.oapen.org/handle/{}'.format(NEW_HANDLE)
        cases = {
            'OAPEN platform': (thoth_metadata(pdf_locations=[oapen_location()]), handle_url),
            'OAPEN URL on another platform': (
                thoth_metadata(pdf_locations=[oapen_location(platform='OTHER')]), handle_url),
            'OAPEN full-text URL only': (thoth_metadata(pdf_locations=[dict(
                oapen_location(platform='OTHER'), landingPage=None,
                fullTextUrl='https://library.oapen.org/bitstream/handle/{}/x.pdf'.format(
                    NEW_HANDLE))]), '/bitstream/handle/'),
            'OAPEN handle resolver': (thoth_metadata(pdf_locations=[dict(
                oapen_location(platform='OTHER'),
                landingPage='https://hdl.handle.net/{}'.format(NEW_HANDLE))]), 'hdl.handle.net'),
            'legacy OAPEN site': (thoth_metadata(pdf_locations=[dict(
                oapen_location(platform='OTHER'),
                landingPage='http://www.oapen.org/search?identifier=1004809')]), 'www.oapen.org'),
        }
        epub_location = thoth_metadata()
        epub_location['data']['work']['publications'][2]['locations'].append(oapen_location())
        cases['OAPEN location on the EPUB publication'] = (epub_location, handle_url)
        for name, (metadata, expected) in cases.items():
            with self.subTest(name):
                remote = FakeRemote()

                reason = self.assertHeld(make_uploader(remote, metadata), remote,
                                         PreflightDecision.ALREADY_PRESENT)

                self.assertIn(expected, reason)
                self.assertEqual(remote.searches, [])

    def test_item_deposited_for_another_thoth_record_is_a_conflict(self):
        remote = FakeRemote([oapen_item('20.500.12657/505', doi=DOI, work_id=OTHER_WORK_ID)])

        reason = self.assertHeld(make_uploader(remote), remote, PreflightDecision.CONFLICT)

        self.assertIn(OTHER_WORK_ID, reason)

    def test_conflicting_identifiers_on_a_matched_item_are_a_conflict(self):
        cases = {
            'DOI differs': oapen_item('20.500.12657/506', doi='10.99999/other.0009',
                                      isbns=('9781999900021',)),
            'ISBNs differ': oapen_item('20.500.12657/507', doi=DOI, isbns=('9781999900069',)),
        }
        for name, item in cases.items():
            with self.subTest(name):
                remote = FakeRemote([item])
                self.assertHeld(make_uploader(remote), remote, PreflightDecision.CONFLICT)

    def test_oapen_minted_doi_on_a_publisher_record_is_not_a_conflict(self):
        remote = FakeRemote([oapen_item('20.500.12657/508', doi='10.26530/OAPEN_9999',
                                        isbns=('9781999900014',))])

        self.assertHeld(make_uploader(remote), remote, PreflightDecision.RECONCILE_EXISTING)

    def test_several_matching_items_are_ambiguous(self):
        remote = FakeRemote([
            oapen_item('20.500.12657/509', doi=DOI, work_id=WORK_ID),
            oapen_item('20.500.12657/510', isbns=('9781999900021',), title='Other'),
        ])

        reason = self.assertHeld(make_uploader(remote), remote, PreflightDecision.AMBIGUOUS)

        self.assertIn('20.500.12657/509, 20.500.12657/510', reason)

    def test_identifier_search_hit_with_an_unreadable_identifier_needs_review(self):
        cases = {
            # Found by the DOI search, but the stored value is not a readable DOI
            'DOI': oapen_item('20.500.12657/518', doi='10 99999 example 0001', title='Other'),
            'Thoth marker': oapen_item('20.500.12657/519', title='Other'),
        }
        cases['Thoth marker']['metadata'].append(metadata_entry(
            'oapen.identifier', 'thoth work id {}'.format(WORK_ID.replace('-', ' '))))
        for name, item in cases.items():
            with self.subTest(name):
                remote = FakeRemote([item])

                reason = self.assertHeld(make_uploader(remote), remote,
                                         PreflightDecision.AMBIGUOUS)

                self.assertIn('was returned by an identifier search', reason)

    def test_same_title_and_publisher_without_a_distinguishing_doi_needs_review(self):
        cases = {
            'no identifiers': oapen_item('20.500.12657/511', title='A study of examples',
                                         alternative='Evidence and method'),
            # OAPEN and Thoth may each hold only some of a book's format ISBNs
            'only other ISBNs': oapen_item('20.500.12657/520', isbns=('9781999900052',)),
            'OAPEN-minted DOI only': oapen_item('20.500.12657/521', doi='10.26530/OAPEN_9998',
                                                isbns=('9781999900052',)),
            'longer publisher name': oapen_item('20.500.12657/527', publisher='Example Press Ltd'),
            'imprint name': oapen_item('20.500.12657/528', publisher='Example Imprint'),
            'no publisher recorded': oapen_item('20.500.12657/532', publisher=None),
        }
        for name, item in cases.items():
            with self.subTest(name):
                remote = FakeRemote([item])
                metadata = thoth_metadata()
                metadata['data']['work']['imprint']['imprintName'] = 'Example Imprint'

                reason = self.assertHeld(make_uploader(remote, metadata), remote,
                                         PreflightDecision.AMBIGUOUS)

                self.assertIn('requires review', reason)

    def test_same_title_record_with_a_doi_beginning_with_ours_needs_review(self):
        # Found only by the title search; such a DOI does not tell the books apart
        query = 'dc.title:"A Study of Examples"'
        for doi in ('10.99999/EXAMPLE.0001-', '10.99999/EXAMPLE.0001.01'):
            with self.subTest(doi):
                remote = FakeRemote()
                remote.failures[query] = [oapen_item('20.500.12657/531', doi=doi)]

                reason = self.assertHeld(make_uploader(remote), remote,
                                         PreflightDecision.AMBIGUOUS)

                self.assertIn('20.500.12657/531 has the same title and publisher', reason)

    def test_unrelated_search_hits_do_not_block_create(self):
        cases = {
            # OAPEN phrase search also returns DOIs that merely contain the query
            'chapter DOI': oapen_item('20.500.12657/512', doi='10.99999/EXAMPLE.0001.01',
                                      title='Chapter One'),
            'same title, other publisher': oapen_item('20.500.12657/513', publisher='Other Press'),
            # A different edition with its own DOI
            'other edition': oapen_item('20.500.12657/514', doi='10.99999/example.0002',
                                        isbns=('9781999900052',)),
        }
        for name, item in cases.items():
            with self.subTest(name):
                remote = FakeRemote([item])
                self.assertCreatedOnce(make_uploader(remote), remote)

    def test_withdrawn_matching_item_still_prevents_create(self):
        remote = FakeRemote([oapen_item('20.500.12657/515', doi=DOI, withdrawn='true')])

        self.assertHeld(make_uploader(remote), remote, PreflightDecision.RECONCILE_EXISTING)

    def test_unknown_remote_state_never_reaches_create(self):
        cases = {
            'HTTP 403': FakeResponse(403, text='not allowed'),
            'HTTP 429': FakeResponse(429),
            'HTTP 500': FakeResponse(500),
            'timeout': requests.Timeout('timed out'),
            'connection error': requests.ConnectionError('reset'),
            'malformed JSON': FakeResponse(200, text='{'),
            'item without handle': FakeResponse(200, body=[{'metadata': []}]),
            'item without metadata': FakeResponse(200, body=[{'handle': '20.500.12657/1'}]),
            'malformed metadata': FakeResponse(200, body=[{'handle': '20.500.12657/1',
                                                           'metadata': [{'value': 'x'}]}]),
        }
        queries = {
            'DOI': 'oapen.identifier.doi:"10.99999/example.0001"',
            'Thoth marker': 'oapen.identifier:"thoth-work-id:{}"'.format(WORK_ID),
            'ISBN': 'oapen.identifier.isbn:"9781999900021"',
            'title': 'dc.title:"A Study of Examples"',
        }
        for (search, query) in queries.items():
            for name, failure in cases.items():
                with self.subTest(search=search, failure=name):
                    remote = FakeRemote()
                    remote.failures[query] = failure
                    self.assertHeld(make_uploader(remote), remote,
                                    PreflightDecision.UNKNOWN_REMOTE_STATE)

    def test_an_index_answering_empty_before_recovering_is_not_trusted(self):
        # The existing deposit would be missed by searches answered while the
        # index returned nothing, so the opening control must catch the outage
        remote = FakeRemote([oapen_item('20.500.12657/522', doi=DOI, work_id=WORK_ID,
                                        title='Old Title')])
        remote.silent_requests = 3

        reason = self.assertHeld(make_uploader(remote), remote,
                                 PreflightDecision.UNKNOWN_REMOTE_STATE)

        self.assertIn('control for oapen.identifier.doi', reason)

    def test_empty_search_is_not_trusted_when_a_control_record_is_not_found(self):
        # As when a field is no longer indexed, or the index is being rebuilt
        remote = FakeRemote()
        remote.items = [item for item in remote.items if item['handle'] != '20.500.12657/113008']

        reason = self.assertHeld(make_uploader(remote), remote,
                                 PreflightDecision.UNKNOWN_REMOTE_STATE)

        self.assertIn('control for oapen.identifier did not return record 20.500.12657/113008',
                      reason)

    def test_empty_search_is_not_trusted_when_a_control_identifier_cannot_be_read(self):
        # Search still finds each control record, but its identifier is in a form
        # the parser cannot read
        def unreadable(handle, key, value):
            item = next(item for item in control_items() if item['handle'] == handle)
            for entry in item['metadata']:
                if entry['key'] == key:
                    entry['value'] = value
            return [item]

        cases = {
            'oapen.identifier.doi': ('oapen.identifier.doi:"10.26530/OAPEN_1004809"', unreadable(
                '20.500.12657/25287', 'oapen.identifier.doi', '10 26530 OAPEN 1004809')),
            'oapen.identifier.isbn': ('oapen.identifier.isbn:"9789085551201"', unreadable(
                '20.500.12657/25287', 'oapen.identifier.isbn', '978-90-8555-120')),
            'oapen.identifier': (
                'oapen.identifier:"thoth-work-id:4344e3cc-b92a-4375-816c-56ce99e29c43"',
                unreadable('20.500.12657/113008', 'oapen.identifier',
                           'thoth work id 4344e3cc b92a 4375 816c 56ce99e29c43')),
            'dc.title': ('dc.title:"The deliverance of open access books"', unreadable(
                '20.500.12657/25287', 'dc.title', 'The deliverance')),
        }
        for field, (query, response) in cases.items():
            with self.subTest(field):
                remote = FakeRemote()
                remote.failures[query] = response

                reason = self.assertHeld(make_uploader(remote), remote,
                                         PreflightDecision.UNKNOWN_REMOTE_STATE)

                self.assertIn('control for {} did not return record'.format(field), reason)

    def test_empty_search_is_not_trusted_when_a_control_search_fails(self):
        remote = FakeRemote()
        remote.failures['dc.title:"The deliverance of open access books"'] = FakeResponse(429)

        reason = self.assertHeld(make_uploader(remote), remote,
                                 PreflightDecision.UNKNOWN_REMOTE_STATE)

        self.assertIn('HTTP status 429', reason)

    def test_a_title_too_common_to_check_needs_review(self):
        items = [oapen_item('20.500.12657/9{}'.format(n), title='Europe {}'.format(n),
                            doi='10.99999/europe.{}'.format(n))
                 for n in range(OAPEN_MAX_RESULTS + 1)]
        cases = {
            'no identifier match': (FakeRemote(items), PreflightDecision.AMBIGUOUS),
            'identifier match decides': (FakeRemote(items + [oapen_item(
                '20.500.12657/523', doi=DOI, title='Europe')]), PreflightDecision.RECONCILE_EXISTING),
        }
        for name, (remote, decision) in cases.items():
            with self.subTest(name):
                reason = self.assertHeld(make_uploader(remote, thoth_metadata(title='Europe')),
                                         remote, decision)
                if decision == PreflightDecision.AMBIGUOUS:
                    self.assertIn('more than 500 OAPEN items contain the title "Europe"', reason)

    def test_ineligible_identity_is_decided_without_any_oapen_request(self):
        no_pdf = thoth_metadata()
        no_pdf['data']['work']['publications'] = [
            publication for publication in no_pdf['data']['work']['publications']
            if publication['publicationType'] != 'PDF']
        no_locations = thoth_metadata()
        no_locations['data']['work']['publications'][0]['locations'] = None
        cases = {
            'no location list': (no_locations, 'no location list'),
            'no DOI and no ISBN': (thoth_metadata(doi=None, isbns=()), 'neither a DOI nor an ISBN'),
            'malformed DOI': (thoth_metadata(doi='https://doi.org/not-a-doi'), 'not a valid DOI'),
            'invalid ISBN': (thoth_metadata(isbns=('978-1-9999-0002-2',)), 'not a valid ISBN'),
            'no PDF publication': (no_pdf, 'exactly one PDF publication'),
            'metadata for another work': (thoth_metadata(work_id=OTHER_WORK_ID), 'not the requested'),
        }
        for name, (metadata, expected) in cases.items():
            with self.subTest(name):
                remote = FakeRemote()

                result = make_uploader(remote, metadata).assess_existing_deposit()

                self.assertEqual(result.decision, PreflightDecision.INELIGIBLE)
                self.assertIn(expected, result.reason)
                self.assertEqual(remote.searches, [])

    def test_ineligible_identity_stops_the_upload(self):
        remote = FakeRemote()

        self.assertHeld(make_uploader(remote, thoth_metadata(doi='https://doi.org/not-a-doi')),
                        remote, PreflightDecision.INELIGIBLE)

    def test_missing_mandatory_metadata_exits_before_any_oapen_request(self):
        metadata = thoth_metadata()
        metadata['data']['work']['longAbstract'] = None
        remote = FakeRemote()

        with self.assertLogs(level='ERROR') as logs, self.assertRaises(SystemExit):
            make_uploader(remote, metadata).upload_to_platform()

        self.assertIn('Work must have a Long Abstract', logs.output[0])
        self.assertEqual(remote.events, [])

    def test_missing_doi_still_searches_every_other_identifier(self):
        remote = FakeRemote()

        self.assertCreatedOnce(make_uploader(remote, thoth_metadata(doi=None)), remote)

        fields = [params['query'].partition(':')[0] for (params, _, _) in remote.searches[1:6]]
        self.assertEqual(fields, ['oapen.identifier', 'oapen.identifier.isbn',
                                  'oapen.identifier.isbn', 'oapen.identifier.isbn', 'dc.title'])


class RepeatAndRetryTests(OfflineTestCase):
    def thoth_deposit(self):
        return oapen_item(NEW_HANDLE, doi='10.99999/EXAMPLE.0001', work_id=WORK_ID,
                          alternative=SUBTITLE)

    def test_repeating_a_submission_creates_at_most_one_item(self):
        """A rerun of the weekly workflow, or a manual resend, after a completed deposit"""
        for name, metadata_after in {
                'location written back': thoth_metadata(pdf_locations=[oapen_location()]),
                'location write-back failed': thoth_metadata()}.items():
            with self.subTest(name):
                remote = FakeRemote()
                remote.index_on_complete = self.thoth_deposit()
                make_uploader(remote).upload_to_platform()

                with self.assertRaises(DisseminationError):
                    make_uploader(remote, metadata_after).upload_to_platform()

                self.assertEqual(remote.events.count('create_item'), 1)

    def test_a_cleared_preflight_permits_only_one_create_request(self):
        remote = FakeRemote()
        uploader = make_uploader(remote)
        uploader.upload_to_platform()

        with self.assertRaisesRegex(DisseminationError, 'has not cleared it'):
            uploader.api.create_item('metadata')
        self.assertEqual(remote.events.count('create_item'), 1)

    def test_one_clearance_allows_one_create_even_within_one_upload(self):
        remote = FakeRemote()
        uploader = make_uploader(remote)
        uploader.api.clear_create()

        uploader.api.create_item('metadata')
        with self.assertRaisesRegex(DisseminationError, 'has not cleared it'):
            uploader.api.create_item('metadata')
        self.assertEqual(remote.events.count('create_item'), 1)

    def test_clearance_is_revoked_when_the_upload_stops_before_create(self):
        remote = FakeRemote()
        uploader = make_uploader(remote)

        def failed_download(publication_type):
            raise DisseminationError('Error retrieving data')
        uploader.get_publication_details = failed_download

        with self.assertLogs(level='ERROR'), self.assertRaises(SystemExit):
            uploader.upload_to_platform()
        with self.assertRaisesRegex(DisseminationError, 'has not cleared it'):
            uploader.api.create_item('metadata')
        self.assertNotIn('create_item', remote.events)

    def test_create_cannot_be_reached_without_the_preflight(self):
        remote = FakeRemote()
        uploader = make_uploader(remote)

        with self.assertLogs(level='ERROR') as logs, self.assertRaises(SystemExit):
            # The shared SWORD upload, bypassing the OAPEN override
            SwordV2Uploader.upload_to_platform(uploader)
        self.assertNotIn('create_item', remote.events)
        self.assertIn('duplicate preflight has not cleared it', logs.output[0])
        # Only the calls the shared upload makes are exposed, not the raw connection
        for name in ('conn', 'send_request', 'handle_request'):
            self.assertFalse(hasattr(uploader.api, name), name)

    def test_same_work_dissemination_is_serialised_by_the_workflow(self):
        """Concurrent runs for one work queue behind each other, so the second run's
        preflight sees the first run's deposit"""
        workflow = (ROOT / '.github' / 'workflows' / 'disseminate.yml').read_text()
        self.assertRegex(workflow, re.compile(
            r"^concurrency:\n  group: \$\{\{ format\('disseminate-\{0\}-\{1\}', "
            r"inputs\.platform, inputs\.work-id\) \}\}\n  cancel-in-progress: false$",
            re.MULTILINE))

    def test_uncertain_create_is_reported_and_not_retried(self):
        for name, error in {
                'timeout': TimeoutError('timed out'),
                'server error': DisseminationError(
                    'Could not connect to SWORD v2 server (status code 500)')}.items():
            with self.subTest(name):
                remote = FakeRemote()
                uploader = make_uploader(remote)
                uploader.api._api.create_error = error

                with self.assertLogs(level='ERROR') as logs, self.assertRaises(SystemExit):
                    uploader.upload_to_platform()

                self.assertEqual(remote.events.count('create_item'), 1)
                self.assertNotIn('upload_metadata', remote.events)
                self.assertNotIn('delete_item', remote.events)
                self.assertIn('was not confirmed', logs.output[0])
                # A timed-out create can leave an unfinished deposit that search cannot show
                self.assertIn('unfinished deposit, which OAPEN search cannot show', logs.output[0])
                self.assertIn('do not retry until OAPEN has confirmed whether it holds an item '
                              'for "thoth-work-id:{}"'.format(WORK_ID), logs.output[0])

    def test_failed_cleanup_after_a_partial_deposit_requires_reconciliation(self):
        for name, delete_error in {
                'SWORD error': DisseminationError('Error deleting item from SWORD v2'),
                'timeout': TimeoutError('timed out')}.items():
            with self.subTest(name):
                remote = FakeRemote()
                uploader = make_uploader(remote)
                uploader.api._api.upload_pdf_error = DisseminationError(
                    'Error uploading PDF file to SWORD v2')
                uploader.api._api.delete_error = delete_error

                with self.assertLogs(level='ERROR') as logs, \
                        self.assertRaises((SystemExit, TimeoutError)):
                    uploader.upload_to_platform()

                self.assertEqual(remote.events[-4:], ['create_item', 'upload_metadata',
                                                      'upload_pdf', 'delete_item'])
                self.assertTrue(any('may remain in the SWORD workspace' in line
                                    for line in logs.output))

    def test_preflight_runs_before_any_download_or_sword_request(self):
        remote = FakeRemote()

        make_uploader(remote).upload_to_platform()

        first_other = next(index for index, event in enumerate(remote.events)
                           if not event.startswith('search'))
        self.assertTrue(all(event.startswith('search') for event in remote.events[:first_other]))
        self.assertEqual(remote.events[first_other:], [
            'export_metadata', 'pdf_download', 'create_item', 'upload_metadata', 'upload_pdf',
            'complete_deposit'])


class OtherUploaderTests(OfflineTestCase):
    def test_cul_sword_upload_is_unchanged(self):
        remote = FakeRemote()
        uploader = make_uploader(remote, uploader_class=CULUploader)

        uploader.upload_to_platform()

        self.assertIsInstance(uploader.api, FakeSwordApi)
        self.assertEqual(remote.searches, [])
        self.assertEqual(remote.events, ['export_metadata', 'pdf_download', 'create_item',
                                         'upload_metadata', 'upload_pdf', 'complete_deposit'])

    def test_preflight_is_defined_only_on_the_oapen_uploader(self):
        self.assertFalse(hasattr(SwordV2Uploader, 'assess_existing_deposit'))
        self.assertFalse(hasattr(CULUploader, 'assess_existing_deposit'))


class ProductionWiringTests(OfflineTestCase):
    def test_uploader_searches_oapen_with_requests_and_real_pacing(self):
        with patch.object(Uploader, 'get_thoth_metadata', return_value=thoth_metadata()), \
                patch.dict(os.environ, CREDENTIALS), \
                patch('swordv2uploader.SwordV2Api', new=partial(FakeSwordApi, FakeRemote())):
            uploader = OAPENSWORDUploader(WORK_ID, 'https://export.example', None, '0.0.0-test')

        self.assertIsInstance(uploader.oapen_index, OAPENLibraryIndex)
        self.assertIs(uploader.oapen_index.http_get, requests.get)
        self.assertIs(uploader.oapen_index.pause, time.sleep)
        self.assertIsInstance(uploader.api, PreflightGatedSwordApi)


if __name__ == '__main__':
    unittest.main()
