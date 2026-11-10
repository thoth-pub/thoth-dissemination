"""Offline tests for the OAPEN metadata validation (DIS-OAPEN-SAFE-01G, #145).

Thoth metadata is built with the helpers of the #123 preflight tests, and OAPEN
search, the PDF download and SWORD are faked: no test sends a network request
or needs credentials.
"""

import copy
import re
import unittest
from datetime import date
from unittest.mock import patch

from lxml import etree

import oapensworduploader
import tests.test_oapensword_preflight as preflight_tests
from culuploader import CULUploader
from oapensworduploader import (
    OAPENSWORDUploader,
    is_open_licence,
    is_valid_issn,
    validate_oapen_work,
)
from swordv2uploader import SwordV2Uploader
from tests.test_oapensword_preflight import WORK_ID, FakeRemote, make_uploader, thoth_metadata


TODAY = date(2026, 11, 10)
DCTERMS = '{http://purl.org/dc/terms/}'
ORCID = 'https://orcid.org/0000-0002-1825-0097'
UPDATED = re.compile(r'(<atom:updated\b[^>]*>)[^<]*(</atom:updated>)')

# The CUL (JISC_ROUTER) entry for cul_metadata(), captured at develop @ 861238a6
# with atom:updated replaced by 'UPDATED'
CUL_GOLDEN_861238A6 = (
    '<?xml version="1.0"?><entry xmlns="http://www.w3.org/2005/Atom" xmlns:dcterms="http://purl.org/dc/terms/">\n    <generator uri="http://bitbucket.org/beno/python-sword2" version="0.1"/>\n'
    '<dcterms:publisher xmlns:atom="http://www.w3.org/2005/Atom">Example Press</dcterms:publisher>'
    '<dcterms:title xmlns:atom="http://www.w3.org/2005/Atom">A Study of Examples: Evidence and Method</dcterms:title>'
    '<dcterms:abstract xmlns:atom="http://www.w3.org/2005/Atom">A long abstract.</dcterms:abstract>'
    '<dcterms:identifier xmlns:atom="http://www.w3.org/2005/Atom">doi: https://doi.org/10.99999/EXAMPLE.0001</dcterms:identifier>'
    '<dcterms:issued xmlns:atom="http://www.w3.org/2005/Atom">2026-10-23</dcterms:issued>'
    '<dcterms:rights xmlns:atom="http://www.w3.org/2005/Atom">License for VoR version of this work: https://creativecommons.org/licenses/by/4.0/</dcterms:rights>'
    '<dcterms:description xmlns:atom="http://www.w3.org/2005/Atom">Publication status: ACTIVE</dcterms:description>'
    '<atom:updated xmlns:atom="http://www.w3.org/2005/Atom">UPDATED</atom:updated>'
    '<dcterms:language xmlns:atom="http://www.w3.org/2005/Atom">ENG</dcterms:language>'
    '<dcterms:identifier xmlns:atom="http://www.w3.org/2005/Atom">isbn: 9781999900014</dcterms:identifier>'
    '<dcterms:identifier xmlns:atom="http://www.w3.org/2005/Atom">isbn: 9781999900021</dcterms:identifier>'
    '<dcterms:identifier xmlns:atom="http://www.w3.org/2005/Atom">isbn: 9781999900038</dcterms:identifier>'
    '<dcterms:subject xmlns:atom="http://www.w3.org/2005/Atom">JP</dcterms:subject>'
    '<dcterms:subject xmlns:atom="http://www.w3.org/2005/Atom">examples</dcterms:subject>'
    '<dcterms:creator xmlns:atom="http://www.w3.org/2005/Atom">Example, Ada; orcid: https://orcid.org/0000-0002-1825-0097; University of Examples</dcterms:creator>'
    '<dcterms:contributor xmlns:atom="http://www.w3.org/2005/Atom">EDITOR: Example Editorial Collective</dcterms:contributor>'
    '<dcterms:rights xmlns:atom="http://www.w3.org/2005/Atom">Embargo: none</dcterms:rights>'
    '<dcterms:description xmlns:atom="http://www.w3.org/2005/Atom">Work version: VoR</dcterms:description>'
    '<dcterms:description xmlns:atom="http://www.w3.org/2005/Atom">From Example Press via Thoth</dcterms:description>'
    '<dcterms:description xmlns:atom="http://www.w3.org/2005/Atom">Funder: Example Foundation; ror: https://ror.org/00x0x0x00; Grant(s): G-1</dcterms:description>'
    '<dcterms:identifier xmlns:atom="http://www.w3.org/2005/Atom">thoth-work-id:0f3a5c7e-1b2d-4e6f-8a9b-0c1d2e3f4a5b</dcterms:identifier>'
    '</entry>'
)


def normalise_updated(entry):
    text, count = UPDATED.subn(r'\1UPDATED\2', str(entry))
    assert count == 1
    return text


def contribution(contribution_type, first_name, last_name, full_name, orcid=None, main=True):
    return {
        '__typename': 'Contribution',
        'contributionType': contribution_type,
        'mainContribution': main,
        'firstName': first_name,
        'lastName': last_name,
        'fullName': full_name,
        'biographies': [],
        'affiliations': [],
        'contributor': {'__typename': 'Contributor', 'orcid': orcid},
    }


def issue(series_name, issn, ordinal):
    return {'__typename': 'Issue', 'issueOrdinal': ordinal,
            'series': {'__typename': 'Series', 'seriesName': series_name,
                       'issnPrint': None, 'issnDigital': issn}}


def funding(grant_number, program, project_name, institution_name):
    return {'__typename': 'Funding', 'grantNumber': grant_number, 'program': program,
            'projectName': project_name,
            'institution': {'__typename': 'Institution', 'institutionName': institution_name,
                            'ror': None, 'institutionDoi': None}}


def complete_metadata():
    """A work with every optional value present, two series and two fundings, and no nulls."""
    metadata = thoth_metadata()
    work = metadata['data']['work']
    work['oclc'] = '1234567890'
    work['contributions'] = [
        contribution('AUTHOR', 'Ada', 'Example', 'Ada Example', orcid=ORCID),
        contribution('EDITOR', None, 'Collective', 'Example Editorial Collective'),
        contribution('TRANSLATOR', 'Tom', 'Translator', 'Tom Translator'),
        contribution('AUTHOR', 'Not', 'Main', 'Not Main', main=False),
    ]
    work['languages'].append({'__typename': 'Language', 'languageCode': 'GER',
                              'languageRelation': 'TRANSLATED_FROM'})
    work['subjects'].extend([
        {'__typename': 'Subject', 'subjectType': 'THEMA', 'subjectCode': 'QDTS'},
        {'__typename': 'Subject', 'subjectType': 'KEYWORD', 'subjectCode': 'method'},
        {'__typename': 'Subject', 'subjectType': 'BIC', 'subjectCode': 'JP'},
    ])
    work['issues'] = [issue('Example Series', '0378-5955', 3),
                      issue('Second Series', '1050-124X', 12)]
    work['fundings'] = [funding('G-1', 'Programme One', 'Project One', 'Example Foundation'),
                        funding('G-2', 'Programme Two', 'Project Two', 'Second Funder')]
    return metadata


def cul_metadata():
    metadata = thoth_metadata()
    work = metadata['data']['work']
    author = work['contributions'][0]
    author['contributor']['orcid'] = ORCID
    author['affiliations'] = [{'institution': {'institutionName': 'University of Examples'}}]
    work['contributions'].append({
        'contributionType': 'EDITOR', 'mainContribution': True, 'firstName': None,
        'lastName': 'Collective', 'fullName': 'Example Editorial Collective',
        'affiliations': [], 'contributor': {'orcid': None}})
    work['fundings'] = [{
        'grantNumber': 'G-1', 'program': 'Programme', 'projectName': 'Project',
        'institution': {'institutionName': 'Example Foundation',
                        'ror': 'https://ror.org/00x0x0x00', 'institutionDoi': None}}]
    return metadata


def with_work(change, metadata=None):
    """Thoth metadata after `change(work)`."""
    metadata = thoth_metadata() if metadata is None else metadata
    change(metadata['data']['work'])
    return metadata


def set_values(**values):
    return lambda work: work.update(values)


def set_subjects(*subjects):
    return set_values(subjects=[{'subjectType': subject_type, 'subjectCode': code}
                                for (subject_type, code) in subjects])


def entry_elements(entry):
    """(local name, text) of each dcterms element in a sword2 entry, in order."""
    root = etree.fromstring(str(entry).encode('utf-8'))
    return [(element.tag[len(DCTERMS):], element.text) for element in root
            if isinstance(element.tag, str) and element.tag.startswith(DCTERMS)]


def build_entry(metadata):
    remote = FakeRemote()
    entry = make_uploader(remote, metadata).parse_metadata()
    assert remote.events == [] and remote.searches == []
    return entry


def validate(metadata, today=TODAY):
    return validate_oapen_work(metadata['data']['work'], WORK_ID, today=today)


class NoNetworkTestCase(unittest.TestCase):
    def setUp(self):
        # Any real HTTP request through `requests` fails the test
        network = patch('requests.sessions.Session.request',
                        side_effect=AssertionError('unexpected network request'))
        network.start()
        self.addCleanup(network.stop)

    def assertHold(self, metadata):
        """The upload exits before any OAPEN, PDF or SWORD request; return the one ERROR line."""
        remote = FakeRemote()
        uploader = make_uploader(remote, metadata)
        with self.assertLogs(level='ERROR') as logs, self.assertRaises(SystemExit) as exited:
            uploader.upload_to_platform()
        self.assertEqual(exited.exception.code, 1)
        self.assertEqual(remote.events, [])
        self.assertEqual(remote.searches, [])
        self.assertEqual(len(logs.records), 1)
        return logs.records[0].getMessage()

    def assertHeldFor(self, metadata, *problems):
        message = self.assertHold(metadata)
        self.assertEqual(message, 'Cannot upload to OAPEN: work {} held for {} metadata problem(s): {}'.format(
            WORK_ID, len(problems), '; '.join(problems)))


class HoldTests(NoNetworkTestCase):
    """One fixture per hold condition (a) to (t), run through upload_to_platform."""

    def test_a_work_status_not_active(self):
        for status in ('FORTHCOMING', 'WITHDRAWN', 'SUPERSEDED', None):
            with self.subTest(status):
                self.assertHeldFor(with_work(set_values(workStatus=status)),
                                   'work status {!r} is not ACTIVE'.format(status))

    def test_b_work_type_outside_the_agreed_list(self):
        for work_type in ('JOURNAL_ISSUE', 'BOOK_SET', 'BOOK_CHAPTER', None):
            with self.subTest(work_type):
                self.assertHeldFor(
                    with_work(set_values(workType=work_type)),
                    'work type {!r} is not MONOGRAPH, EDITED_BOOK or TEXTBOOK'.format(work_type))

    def test_agreed_work_types_are_not_held(self):
        for work_type in ('MONOGRAPH', 'EDITED_BOOK', 'TEXTBOOK'):
            with self.subTest(work_type):
                (_, problems) = validate(with_work(set_values(workType=work_type)))
                self.assertEqual(problems, [])

    def test_c_missing_long_abstract(self):
        for value in (None, '', '   \n', 5):
            with self.subTest(value):
                self.assertHeldFor(with_work(set_values(longAbstract=value)),
                                   'Work must have a Long Abstract')

    def test_d_missing_licence(self):
        for value in (None, '', ' '):
            with self.subTest(value):
                self.assertHeldFor(with_work(set_values(license=value)), 'Work must have a Licence')

    def test_e_missing_landing_page(self):
        for value in (None, '', ' '):
            with self.subTest(value):
                self.assertHeldFor(with_work(set_values(landingPage=value)),
                                   'Work must have a Landing Page')

    def test_f_no_isbn(self):
        for isbns in ((), ('', ' ')):
            with self.subTest(isbns):
                self.assertHeldFor(thoth_metadata(isbns=isbns), 'no ISBNs found')

    def test_g_no_languages(self):
        for languages in ([], None):
            with self.subTest(languages):
                self.assertHeldFor(with_work(set_values(languages=languages)), 'no languages found')

    def test_h_no_thema_code(self):
        for subjects in ([('KEYWORD', 'examples')],
                         [('THEMA', None), ('THEMA', ' '), ('KEYWORD', 'examples')]):
            with self.subTest(subjects):
                self.assertHeldFor(with_work(set_subjects(*subjects)),
                                   'no THEMA Subject Codes found')

    def test_i_no_keyword(self):
        for subjects in ([('THEMA', 'JP')], [('THEMA', 'JP'), ('KEYWORD', '')]):
            with self.subTest(subjects):
                self.assertHeldFor(with_work(set_subjects(*subjects)), 'no Subject Keywords found')

    def test_j_missing_title(self):
        for value in (None, ''):
            with self.subTest(value):
                self.assertHeldFor(with_work(set_values(title=value)), 'Work must have a Title')

    def test_k_missing_publication_date(self):
        for value in (None, '', 20261023):
            with self.subTest(value):
                self.assertHeldFor(with_work(set_values(publicationDate=value)),
                                   'Work must have a Publication Date')

    def test_l_missing_publisher_or_imprint_name(self):
        def no_imprint_name(work):
            work['imprint']['imprintName'] = ''

        def no_publisher_name(work):
            work['imprint']['publisher']['publisherName'] = None

        for change in (no_imprint_name, no_publisher_name, set_values(imprint=None)):
            with self.subTest(change):
                self.assertHeldFor(with_work(change), 'Work must have a Publisher and Imprint name')

    def test_m_no_main_author_or_editor(self):
        cases = {
            'translator only': [contribution('TRANSLATOR', 'Tom', 'Translator', 'Tom Translator')],
            'author not main': [contribution('AUTHOR', 'Ada', 'Example', 'Ada Example', main=False)],
            'author without a name': [contribution('AUTHOR', None, 'Example', None)],
            'none': [],
        }
        for name, contributions in cases.items():
            with self.subTest(name):
                self.assertHeldFor(with_work(set_values(contributions=contributions)),
                                   'no main AUTHOR or EDITOR')

    def test_n_landing_page_not_an_absolute_http_url(self):
        for value in ('publisher.example/books/0001', 'ftp://publisher.example/books/0001',
                      'https:///books/0001', 'https://publisher.example/books 0001',
                      'https://publisher.example/books/\n0001', 'mailto:books@publisher.example'):
            with self.subTest(value):
                self.assertHeldFor(with_work(set_values(landingPage=value)),
                                   'landing page is not an absolute http(s) URL')

    def test_o_publication_date_not_a_valid_date(self):
        for value in ('2026-13-01', '2026-02-30', '2026-1-05', '23/10/2026', '2026-10-23T00:00:00',
                      '2026', '２０２６-10-23'):
            with self.subTest(value):
                self.assertHeldFor(
                    with_work(set_values(publicationDate=value)),
                    'publication date {!r} is not a valid YYYY-MM-DD date'.format(value))

    def test_p_publication_date_in_the_future(self):
        self.assertHeldFor(with_work(set_values(publicationDate='2999-01-01')),
                           'publication date 2999-01-01 is in the future')

    def test_q_language_code_with_no_name(self):
        cases = {
            'unknown': ([{'languageCode': 'XXX'}], "'XXX'"),
            'null': ([{'languageCode': None}], 'None'),
            'empty': ([{'languageCode': ''}], "''"),
            'not a string': ([{'languageCode': 5}], '5'),
            'several, each once': ([{'languageCode': 'XXX'}, {'languageCode': 'ENG'},
                                    {'languageCode': 'YYY'}, {'languageCode': 'XXX'}],
                                   "'XXX', 'YYY'"),
        }
        for name, (languages, codes) in cases.items():
            with self.subTest(name):
                self.assertHeldFor(with_work(set_values(languages=languages)),
                                   'language code(s) with no name: {}'.format(codes))

    def test_r_licence_not_creative_commons(self):
        for value in ('https://opensource.org/licenses/MIT', 'All rights reserved',
                      'https://creativecommons.org/publicdomain/mark/1.0/'):
            with self.subTest(value):
                self.assertHeldFor(
                    with_work(set_values(license=value)),
                    'licence {!r} is not a Creative Commons licence or CC0'.format(value))

    def test_s_thema_code_not_in_the_oapen_list(self):
        cases = {
            'unknown': ([('THEMA', 'ZZZZ')], "'ZZZZ'"),
            'case and spacing are not normalised': ([('THEMA', 'jp'), ('THEMA', ' JP')],
                                                    "'jp', ' JP'"),
            'several, each once': ([('THEMA', 'ZZZZ'), ('THEMA', 'JP'), ('THEMA', 'YYYY'),
                                    ('THEMA', 'ZZZZ')], "'ZZZZ', 'YYYY'"),
        }
        for name, (subjects, codes) in cases.items():
            with self.subTest(name):
                self.assertHeldFor(with_work(set_subjects(*subjects, ('KEYWORD', 'examples'))),
                                   'Thema code(s) not in the OAPEN list: {}'.format(codes))

    def test_t_value_xml_cannot_carry(self):
        cases = {
            'abstract': (set_values(longAbstract='A long\x0b abstract.'), 'dcterms_abstract'),
            'subtitle': (set_values(subtitle='Evidence\x00'), 'dcterms_alternative'),
            'keyword': (set_subjects(('THEMA', 'JP'), ('KEYWORD', 'ex\x1famples')), 'dcterms_other'),
            'non-character': (set_values(place='Cambridge￾'), 'dcterms_place'),
            'surrogate': (set_values(title='A Study\ud800'), 'dcterms_title'),
            'series name': (set_values(issues=[issue('Series\x08', None, 1)]), 'dcterms_seriesId'),
            'funding': (set_values(fundings=[funding('G\x01', None, None, 'Funder')]),
                        'dcterms_grantNumber'),
        }
        for name, (change, field) in cases.items():
            with self.subTest(name):
                self.assertHeldFor(with_work(change),
                                   'field {} contains characters XML cannot carry'.format(field))

    def test_a_field_xml_cannot_carry_is_named_once(self):
        metadata = with_work(set_subjects(('THEMA', 'JP'), ('KEYWORD', 'a\x0b'), ('KEYWORD', 'b\x0c')))

        self.assertHeldFor(metadata, 'field dcterms_other contains characters XML cannot carry')

    def test_every_problem_is_reported_in_one_error_line_in_order(self):
        def change(work):
            work.update(
                workStatus='WITHDRAWN',
                workType='BOOK_CHAPTER',
                longAbstract=' ',
                landingPage='publisher.example/books/0001',
                publicationDate='2026-02-30',
                license='https://opensource.org/licenses/MIT\x0b',
                title='A Study\x01 of Examples',
                subtitle='Evidence\x02',
                languages=[{'languageCode': 'XXX'}, {'languageCode': 'ENG'},
                           {'languageCode': 'XXX'}, {'languageCode': None}],
                subjects=[{'subjectType': 'THEMA', 'subjectCode': 'ZZZZ'},
                          {'subjectType': 'THEMA', 'subjectCode': 'JP'}],
                contributions=[contribution('TRANSLATOR', 'Tom', 'Translator', 'Tom Translator')],
            )
            work['imprint']['imprintName'] = None
            for publication in work['publications']:
                publication['isbn'] = None

        self.assertHeldFor(
            with_work(change),
            "work status 'WITHDRAWN' is not ACTIVE",
            "work type 'BOOK_CHAPTER' is not MONOGRAPH, EDITED_BOOK or TEXTBOOK",
            'Work must have a Long Abstract',
            'no ISBNs found',
            'no Subject Keywords found',
            'Work must have a Publisher and Imprint name',
            'no main AUTHOR or EDITOR',
            'landing page is not an absolute http(s) URL',
            "publication date '2026-02-30' is not a valid YYYY-MM-DD date",
            "language code(s) with no name: 'XXX', None",
            "licence 'https://opensource.org/licenses/MIT' is not a Creative Commons licence or CC0",
            "Thema code(s) not in the OAPEN list: 'ZZZZ'",
            'field dcterms_title contains characters XML cannot carry',
            'field dcterms_alternative contains characters XML cannot carry',
            'field dcterms_rights contains characters XML cannot carry',
        )

    def test_the_legacy_phrase_and_exit_are_kept_for_existing_checks(self):
        message = self.assertHold(with_work(set_values(longAbstract=None)))

        self.assertTrue(message.startswith('Cannot upload to OAPEN: '))
        self.assertIn('Work must have a Long Abstract', message)

    def test_malformed_fields_are_problems_not_exceptions(self):
        work = {'workStatus': 'ACTIVE', 'workType': 'MONOGRAPH', 'imprint': 'Example Press',
                'publications': [None, 'x', {'isbn': 5}], 'languages': {'languageCode': 'ENG'},
                'subjects': [None, {'subjectType': 'THEMA', 'subjectCode': ['JP']}],
                'contributions': [None, {'mainContribution': True, 'contributionType': 'AUTHOR',
                                         'firstName': None, 'fullName': 'Ada Example',
                                         'contributor': 'not a dict'}],
                'issues': [None, {'series': None, 'issueOrdinal': '1'}],
                'fundings': 'none', 'doi': 5, 'pageCount': '12', 'title': ['A title']}
        for value in (work, {}, None, []):
            with self.subTest(value):
                (fields, problems) = validate_oapen_work(value, WORK_ID, today=TODAY)
                self.assertTrue(problems)
                for (key, field_value) in fields:
                    self.assertIsInstance(field_value, str)

    def test_a_complete_work_is_not_held(self):
        remote = FakeRemote()
        uploader = make_uploader(remote, complete_metadata())

        uploader.upload_to_platform()

        self.assertEqual(remote.events.count('create_item'), 1)


class PublicationDateTests(unittest.TestCase):
    def test_today_is_allowed_and_tomorrow_is_not(self):
        cases = {'2026-11-10': [], '2026-11-11': ['publication date 2026-11-11 is in the future']}
        for value, problems in cases.items():
            with self.subTest(value):
                self.assertEqual(validate(with_work(set_values(publicationDate=value)))[1], problems)

    def test_issued_is_the_year(self):
        (fields, _) = validate(thoth_metadata())

        self.assertIn(('dcterms_issued', '2026'), fields)

    def test_validation_is_deterministic(self):
        metadata = complete_metadata()
        work = metadata['data']['work']
        original = copy.deepcopy(work)

        self.assertEqual(validate_oapen_work(work, WORK_ID), validate_oapen_work(work, WORK_ID))
        self.assertEqual(validate(metadata), validate(metadata))
        self.assertEqual(work, original)


class LicenceTests(unittest.TestCase):
    ACCEPTED = (
        'https://creativecommons.org/licenses/by/4.0/',
        'https://creativecommons.org/licenses/by-sa/4.0/',
        'https://creativecommons.org/licenses/by-nd/4.0/',
        'https://creativecommons.org/licenses/by-nc/4.0/',
        'https://creativecommons.org/licenses/by-nc-sa/4.0/',
        'http://creativecommons.org/licenses/by-nc-nd/4.0',
        'https://www.creativecommons.org/licenses/by/2.5/',
        'https://creativecommons.org/licenses/by/3.0/igo/',
        'https://creativecommons.org/licenses/by-nc/3.0/de/',
        'https://creativecommons.org/licenses/by/2.0/uk/legalcode',
        'https://creativecommons.org/licenses/by/4.0/deed.en',
        'https://creativecommons.org/licenses/by/4.0/deed.zh-Hans',
        'https://creativecommons.org/licenses/by/4.0/legalcode.de',
        'https://creativecommons.org/licenses/by-sa/4.0/legalcode.pt_BR',
        'HTTPS://CreativeCommons.ORG/Licenses/BY/4.0/',
        '  https://creativecommons.org/licenses/by/4.0/  ',
        'https://creativecommons.org/licenses/by/4.0/?ref=chooser-v1',
        'https://creativecommons.org/licenses/by/4.0/#legal',
        'https://creativecommons.org/publicdomain/zero/1.0/',
        'https://creativecommons.org/publicdomain/zero/1.0/legalcode',
        'http://creativecommons.org/publicdomain/zero/1.0/deed.fr',
    )
    HELD = (
        'https://creativecommons.org/licenses/',
        'https://creativecommons.org/licenses/by/',
        'https://creativecommons.org/licenses/by/4/',
        'https://creativecommons.org/licenses/by/4.0/legalcode/extra',
        'https://creativecommons.org/licenses/by/4.0/de1/',
        'https://creativecommons.org/licenses/sampling+/1.0/',
        'https://creativecommons.org/licenses/nc/1.0/',
        'https://creativecommons.org/licenses/by-nc-sa/4.0/deed.english',
        'https://creativecommons.org/publicdomain/mark/1.0/',
        'https://creativecommons.org/publicdomain/zero/2.0/',
        'https://creativecommons.org.evil.example/licenses/by/4.0/',
        'https://evil.example/creativecommons.org/licenses/by/4.0/',
        'https://evil.example/?u=https://creativecommons.org/licenses/by/4.0/',
        'https://user:secret@creativecommons.org/licenses/by/4.0/',
        'https://user@creativecommons.org/licenses/by/4.0/',
        'https://creativecommons.org:443/licenses/by/4.0/',
        'https://creativecommons.org:x/licenses/by/4.0/',
        'https://[creativecommons.org/licenses/by/4.0/',
        'ftp://creativecommons.org/licenses/by/4.0/',
        '//creativecommons.org/licenses/by/4.0/',
        'creativecommons.org/licenses/by/4.0/',
        'https://creativecommons.org/licen\nses/by/4.0/',
        'https://creativecommons.org/licenses/by/4.0/ deed.en',
        'https://creativecommons.org/licenses/by/４.0/',
        'CC BY 4.0',
        'https://opensource.org/licenses/MIT',
    )

    def test_accepted_licences_are_sent_unchanged(self):
        for licence in self.ACCEPTED:
            with self.subTest(licence):
                self.assertTrue(is_open_licence(licence))
                (fields, problems) = validate(with_work(set_values(license=licence)))
                self.assertEqual(problems, [])
                self.assertIn(('dcterms_rights', licence), fields)

    def test_other_licences_hold(self):
        for licence in self.HELD:
            with self.subTest(licence):
                self.assertFalse(is_open_licence(licence))
                (_, problems) = validate(with_work(set_values(license=licence)))
                self.assertEqual(len(problems), 1)
                self.assertTrue(problems[0].startswith('licence '))
                self.assertTrue(problems[0].endswith(' is not a Creative Commons licence or CC0'))

    def test_a_licence_that_is_not_a_string_is_missing(self):
        self.assertFalse(is_open_licence(None))
        self.assertEqual(validate(with_work(set_values(license=4)))[1], ['Work must have a Licence'])


class OptionalValueTests(NoNetworkTestCase):
    OPTIONAL = {
        'subtitle': 'alternative',
        'oclc': 'ocn',
        'pageCount': 'pageCount',
        'place': 'place',
        'doi': 'doi',
    }

    def test_each_missing_optional_value_produces_no_element(self):
        for (name, element) in self.OPTIONAL.items():
            for value in {'pageCount': (None,), 'doi': (None, '')}.get(name, (None, '', '  ')):
                with self.subTest(name=name, value=value):
                    metadata = with_work(set_values(**{name: value}), complete_metadata())
                    tags = [tag for (tag, _) in entry_elements(build_entry(metadata))]
                    self.assertNotIn(element, tags)

    def test_complete_work_sends_every_optional_value(self):
        tags = [tag for (tag, _) in entry_elements(build_entry(complete_metadata()))]

        for element in self.OPTIONAL.values():
            self.assertIn(element, tags)

    def test_no_element_has_the_text_none(self):
        def sparse(work):
            work.update(subtitle=None, oclc=None, pageCount=None, place=None, doi=None,
                        edition=None,
                        issues=[issue('Series', None, None)],
                        fundings=[funding(None, None, None, 'Funder')])

        for metadata in (thoth_metadata(), complete_metadata(), with_work(sparse)):
            with self.subTest():
                elements = entry_elements(build_entry(metadata))
                self.assertNotIn('None', [text for (_, text) in elements])
                self.assertNotIn('None', str(build_entry(metadata)).replace(WORK_ID, ''))

    def test_page_count_must_be_a_positive_integer(self):
        for value in (0, -1, True, False, '12', 12.0, None):
            with self.subTest(value):
                tags = [tag for (tag, _) in entry_elements(
                    build_entry(with_work(set_values(pageCount=value))))]
                self.assertNotIn('pageCount', tags)
        self.assertIn(('pageCount', '1'), entry_elements(build_entry(with_work(set_values(pageCount=1)))))

    def test_no_edition_element_is_sent(self):
        elements = entry_elements(build_entry(complete_metadata()))

        self.assertFalse([tag for (tag, _) in elements if 'version' in tag or 'edition' in tag])
        self.assertNotIn('dc:', str(build_entry(complete_metadata())))

    def test_values_are_sent_unchanged(self):
        metadata = with_work(set_values(subtitle='  Evidence  ', place=' Cambridge '))

        elements = entry_elements(build_entry(metadata))

        self.assertIn(('alternative', '  Evidence  '), elements)
        self.assertIn(('place', ' Cambridge '), elements)

    def test_missing_isbn_keyword_and_contributor_entries_are_skipped(self):
        def change(work):
            work['publications'][0]['isbn'] = ' '
            work['subjects'].append({'subjectType': 'KEYWORD', 'subjectCode': ''})
            work['contributions'].append(contribution('EDITOR', None, 'Nobody', ' '))

        elements = entry_elements(build_entry(with_work(change)))

        self.assertEqual([text for (tag, text) in elements if tag == 'isbn'],
                         ['9781999900021', '9781999900038'])
        self.assertEqual([text for (tag, text) in elements if tag == 'other'], ['examples'])
        self.assertNotIn('editor', [tag for (tag, _) in elements])


class SeriesAndFundingTests(NoNetworkTestCase):
    SERIES = ('issn', 'seriesId', 'seriesNumber')
    FUNDING = ('grantNumber', 'program', 'projectName', 'institutionId')

    def group(self, elements, names):
        return [(tag, text) for (tag, text) in elements if tag in names]

    def test_one_series_omits_missing_values(self):
        cases = {
            'no ISSN or number': (issue('Example Series', None, None),
                                  [('seriesId', 'Example Series')]),
            'invalid ISSN': (issue('Example Series', '0378-5956', 3),
                             [('seriesId', 'Example Series'), ('seriesNumber', '3')]),
            'no name': (issue(' ', '0378-5955', 3),
                        [('issn', '0378-5955'), ('seriesNumber', '3')]),
            'ordinal not an integer': (issue('Example Series', '03785955', '3'),
                                       [('issn', '03785955'), ('seriesId', 'Example Series')]),
        }
        for name, (value, expected) in cases.items():
            with self.subTest(name):
                elements = entry_elements(build_entry(with_work(set_values(issues=[value]))))
                self.assertEqual(self.group(elements, self.SERIES), expected)

    def test_one_funding_omits_missing_values(self):
        metadata = with_work(set_values(fundings=[funding(None, '', 'Project', 'Funder')]))

        elements = entry_elements(build_entry(metadata))

        self.assertEqual(self.group(elements, self.FUNDING),
                         [('projectName', 'Project'), ('institutionId', 'Funder')])

    def test_two_series_keep_one_element_per_series_in_order(self):
        metadata = with_work(set_values(issues=[issue('First', None, 1),
                                                issue('Second', '1050-124X', None)]))

        elements = self.group(entry_elements(build_entry(metadata)), self.SERIES)

        self.assertEqual(elements, [('issn', None), ('seriesId', 'First'), ('seriesNumber', '1'),
                                    ('issn', '1050-124X'), ('seriesId', 'Second'),
                                    ('seriesNumber', None)])
        counts = {name: [tag for (tag, _) in elements].count(name) for name in self.SERIES}
        self.assertEqual(counts, {'issn': 2, 'seriesId': 2, 'seriesNumber': 2})

    def test_two_fundings_keep_one_element_per_funding_in_order(self):
        metadata = with_work(set_values(fundings=[funding(None, 'Programme', None, 'First'),
                                                  funding('G-2', None, 'Project', None)]))

        elements = self.group(entry_elements(build_entry(metadata)), self.FUNDING)

        self.assertEqual(elements, [
            ('grantNumber', None), ('program', 'Programme'), ('projectName', None),
            ('institutionId', 'First'),
            ('grantNumber', 'G-2'), ('program', None), ('projectName', 'Project'),
            ('institutionId', None)])

    def test_an_invalid_issn_is_never_sent(self):
        invalid = ('0378-5956', '0378-595x', '1234-567', '0378 5955', ' 0378-5955', '٠٣٧٨-٥٩٥٥', 3785955)
        for value in invalid:
            for issues in ([issue('Series', value, 1)],
                           [issue('Series', value, 1), issue('Other', '0378-5955', 2)]):
                with self.subTest(value=value, groups=len(issues)):
                    self.assertFalse(is_valid_issn(value))
                    elements = entry_elements(build_entry(with_work(set_values(issues=issues))))
                    issns = [text for (tag, text) in elements if tag == 'issn']
                    self.assertEqual(issns, [None, '0378-5955'] if len(issues) == 2 else [])

    def test_valid_issns(self):
        for value in ('0378-5955', '03785955', '1050-124X', '0000-006X'):
            with self.subTest(value):
                self.assertTrue(is_valid_issn(value))


class GoldenEntryTests(NoNetworkTestCase):
    def test_complete_work_matches_the_legacy_profile(self):
        uploader = make_uploader(FakeRemote(), complete_metadata())

        legacy = SwordV2Uploader.profile_oapen(uploader)
        entry = uploader.parse_metadata()

        self.assertEqual(normalise_updated(entry), normalise_updated(legacy))
        elements = entry_elements(entry)
        self.assertEqual([text for (tag, text) in elements if tag in ('creator', 'editor',
                                                                     'contributionsBy')],
                         ['Example, Ada | {}'.format(ORCID), 'Example Editorial Collective',
                          'Translator, Tom'])
        self.assertEqual([text for (tag, text) in elements if tag == 'language'],
                         ['English', 'German'])

    def test_entry_holds_exactly_the_validated_fields(self):
        metadata = complete_metadata()

        (fields, problems) = validate(metadata)

        self.assertEqual(problems, [])
        self.assertEqual([(key[len('dcterms_'):], value) for (key, value) in fields],
                         entry_elements(build_entry(metadata)))

    def test_oapen_no_longer_uses_the_legacy_profile(self):
        self.assertIsNot(OAPENSWORDUploader.parse_metadata, SwordV2Uploader.parse_metadata)
        with patch.object(SwordV2Uploader, 'profile_oapen',
                          side_effect=AssertionError('legacy profile used')):
            build_entry(complete_metadata())


class CULEntryTests(NoNetworkTestCase):
    def test_cul_uses_the_shared_parser(self):
        self.assertIs(CULUploader.parse_metadata, SwordV2Uploader.parse_metadata)

    def test_cul_entry_is_unchanged_since_861238a6(self):
        entry = make_uploader(FakeRemote(), cul_metadata(), uploader_class=CULUploader).parse_metadata()

        self.assertEqual(normalise_updated(entry), CUL_GOLDEN_861238A6)


class Unavailable:
    """Stands in for a module that the validator must not use."""

    def __init__(self, name):
        self.name = name

    def __getattr__(self, attribute):
        raise AssertionError('{}.{} used by validate_oapen_work'.format(self.name, attribute))


class PurityTests(unittest.TestCase):
    def test_validator_makes_no_request_builds_no_entry_and_logs_nothing(self):
        with patch.object(oapensworduploader, 'requests', Unavailable('requests')), \
                patch.object(oapensworduploader, 'sword2', Unavailable('sword2')), \
                patch.object(oapensworduploader, 'logging', Unavailable('logging')), \
                patch('requests.get', side_effect=AssertionError('requests.get')), \
                patch('requests.sessions.Session.request', side_effect=AssertionError('request')), \
                patch('sword2.Entry', side_effect=AssertionError('sword2.Entry')), \
                patch.dict('os.environ', {}, clear=True), \
                self.assertNoLogs(level='DEBUG'):
            for metadata in (complete_metadata(),
                             with_work(set_values(workStatus=None, languages=[{'languageCode': 'X'}],
                                                  license='x', longAbstract='\x0b'))):
                (fields, problems) = validate(metadata)
                self.assertTrue(fields)


class MessageTests(NoNetworkTestCase):
    def test_message_is_one_line_and_quotes_no_url_text_or_name(self):
        def change(work):
            work.update(
                workStatus='ACTIVE\nERROR:spoofed',
                longAbstract='SECRET abstract\x0b',
                title='SECRET title\x01',
                landingPage='SECRET landing\npage',
                license='https://creativecommons.org/licenses/by/4.0/\r\nERROR:spoofed\x85' + 'x' * 300,
                languages=[{'languageCode': 'SECRET\n'}],
                subjects=[{'subjectType': 'THEMA', 'subjectCode': 'ZZ\x1bZZ'},
                          {'subjectType': 'KEYWORD', 'subjectCode': 'SECRET keyword\x0b'}],
                publicationDate='SECRET\n',
                contributions=[contribution('AUTHOR', 'SECRET', 'SECRET', 'SECRET', orcid='SECRET\x0b')],
            )
            work['publications'][0]['locations'][0]['fullTextUrl'] = 'https://x.example/?token=SECRET'

        message = self.assertHold(with_work(change))

        self.assertEqual(message.splitlines(), [message])
        for character in '\r\n\x0b\x0c\x1b\x85 ':
            self.assertNotIn(character, message)
        self.assertEqual(message.count('SECRET'), 2)
        self.assertIn("language code(s) with no name: 'SECRET'", message)
        self.assertIn("publication date 'SECRET' is not a valid YYYY-MM-DD date", message)
        self.assertIn("Thema code(s) not in the OAPEN list: 'ZZZZ'", message)
        self.assertIn("work status 'ACTIVEERROR:spoofed' is not ACTIVE", message)
        quoted_licence = re.search(r"licence ('[^']*') is not", message).group(1)
        self.assertEqual(len(quoted_licence), 102)
        self.assertNotIn('ERROR:spoofed\x85', message)
        for field in ('dcterms_abstract', 'dcterms_title', 'dcterms_other', 'dcterms_creator'):
            self.assertIn('field {} contains characters XML cannot carry'.format(field), message)


class ImportTests(unittest.TestCase):
    def test_only_helpers_are_imported_from_the_preflight_tests(self):
        imported = [value for value in globals().values()
                    if isinstance(value, type) and issubclass(value, unittest.TestCase)
                    and value.__module__ == preflight_tests.__name__]

        self.assertEqual(imported, [])


if __name__ == '__main__':
    unittest.main()
