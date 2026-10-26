#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to
OAPEN DSpace using SWORD protocol
"""

import logging
import re
import time
import unicodedata
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlparse

import requests

from dspaceuploader import DSpaceUploader, MetadataProfile
from errors import DisseminationError
from uploader import Location


OAPEN_SEARCH_URL = 'https://library.oapen.org/rest/search'
# OAPEN blocks unidentified programmatic REST traffic and allowlists this agent
OAPEN_USER_AGENT = 'Thoth-Dissemination/1.0 (+https://thoth.pub)'
OAPEN_REQUEST_TIMEOUT = (10, 30)
OAPEN_REQUEST_INTERVAL_SECONDS = 1
# Search caps each response at 100 items and never reports a total
OAPEN_PAGE_SIZE = 100
OAPEN_MAX_RESULTS = 500
OAPEN_HANDLE_PREFIX = '20.500.12657/'
# DOIs that OAPEN mints for its own records do not identify a publisher's work
OAPEN_DOI_PREFIX = '10.26530/'
THOTH_WORK_ID_MARKER = 'thoth-work-id:'
THOTH_WORK_ID_PATTERN = re.compile(
    r'thoth-work-id:\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})',
    re.IGNORECASE)
DOI_START = re.compile(r'(?<![0-9a-z.])10\.\d{4,9}/')
DOI_BRACKETS = {')': '(', ']': '[', '}': '{', '>': '<'}
# How a longer DOI continues ours, as a chapter DOI such as '10.1234/book.01' does
DOI_EXTENSION = re.compile(r'[./_-][0-9a-z]')

DOI_FIELD = 'oapen.identifier.doi'
ISBN_FIELD = 'oapen.identifier.isbn'
MARKER_FIELD = 'oapen.identifier'
TITLE_FIELD = 'dc.title'

# A query on a field OAPEN does not index, an emptied index and some access
# blocks all answer HTTP 200 with no results. An empty answer only permits a
# create once every searched field has found and read a record known to be
# in the index. (field, value, handle of the record holding that value)
INDEX_CONTROLS = (
    (DOI_FIELD, '10.26530/OAPEN_1004809', '20.500.12657/25287'),
    (ISBN_FIELD, '9789085551201', '20.500.12657/25287'),
    (MARKER_FIELD, 'thoth-work-id:4344e3cc-b92a-4375-816c-56ce99e29c43', '20.500.12657/113008'),
    (TITLE_FIELD, 'The deliverance of open access books', '20.500.12657/25287'),
)


class PreflightDecision(Enum):
    CREATE_ELIGIBLE = 'CREATE_ELIGIBLE'
    ALREADY_PRESENT = 'ALREADY_PRESENT'
    RECONCILE_EXISTING = 'RECONCILE_EXISTING'
    CONFLICT = 'CONFLICT'
    AMBIGUOUS = 'AMBIGUOUS'
    UNKNOWN_REMOTE_STATE = 'UNKNOWN_REMOTE_STATE'
    INELIGIBLE = 'INELIGIBLE'


@dataclass(frozen=True)
class PreflightResult:
    decision: PreflightDecision
    reason: str


@dataclass(frozen=True)
class DepositIdentity:
    work_id: str
    doi: str | None
    isbns: frozenset
    # A phrase search for the short title also finds it within a longer title
    search_title: str
    titles: frozenset
    # Publisher and imprint names
    publishers: frozenset


@dataclass(frozen=True)
class OAPENItem:
    handle: str
    dois: frozenset
    isbns: frozenset
    work_ids: frozenset
    titles: frozenset
    publishers: frozenset
    # Whether the DOI field has values, every one of which holds a DOI
    doi_field_readable: bool

    def distinguishing_dois(self):
        return {doi for doi in self.dois if not doi.startswith(OAPEN_DOI_PREFIX)}


class OAPENLookupError(DisseminationError):
    """The OAPEN Library index could not give a complete, usable answer."""


class OAPENCoverageError(OAPENLookupError):
    """An OAPEN search matched more items than can be checked."""


def find_dois(value):
    """Return every DOI in a value, in order, lower-cased and without surrounding punctuation."""
    if not isinstance(value, str):
        return []
    text = value.lower()
    starts = [match.start() for match in DOI_START.finditer(text)]
    dois = []
    for (start, end) in zip(starts, starts[1:] + [len(text)]):
        words = text[start:end].split()
        doi = words[0] if words else ''
        # Drop trailing punctuation, quotes and unbalanced closing brackets,
        # as around '<10.1234/x>' or between DOIs in '10.1234/x;10.5678/y'
        while doi and (doi[-1] in '.,;:/"\'' or (
                doi[-1] in DOI_BRACKETS
                and doi.count(doi[-1]) > doi.count(DOI_BRACKETS[doi[-1]]))):
            doi = doi[:-1]
        if re.fullmatch(r'10\.\d{4,9}/\S+', doi) and doi not in dois:
            dois.append(doi)
    return dois


def normalise_doi(value):
    """Return the first DOI in a value, or None."""
    dois = find_dois(value)
    return dois[0] if dois else None


def extends_doi(doi, base):
    """Whether a DOI continues base after a separator, which a stray '-' or '_' alone does not."""
    return doi.startswith(base) and DOI_EXTENSION.match(doi, len(base)) is not None


def normalise_isbn(value):
    """Return a valid ISBN as 13 digits (converting ISBN-10), or None."""
    if not isinstance(value, str):
        return None
    isbn = re.sub(r'[\s-]', '', value).upper()
    if re.fullmatch(r'\d{9}[\dX]', isbn):
        total = sum((10 - index) * (10 if char == 'X' else int(char))
                    for index, char in enumerate(isbn))
        if total % 11 != 0:
            return None
        isbn = '978' + isbn[:9]
        isbn += str((10 - sum(int(char) * (3 if index % 2 else 1)
                              for index, char in enumerate(isbn)) % 10) % 10)
        return isbn
    if re.fullmatch(r'97[89]\d{10}', isbn):
        total = sum(int(char) * (3 if index % 2 else 1)
                    for index, char in enumerate(isbn))
        return isbn if total % 10 == 0 else None
    return None


def find_isbns(value):
    """Return every valid ISBN in a value such as '9781234567897 (PDF)'."""
    parts = re.split(r'[^0-9Xx-]+', value) + [value]
    return {isbn for isbn in map(normalise_isbn, parts) if isbn is not None}


def normalise_text(value):
    """Fold case, compatibility forms, punctuation and spacing for comparison."""
    if not isinstance(value, str):
        return None
    text = unicodedata.normalize('NFKC', value).casefold()
    text = ' '.join(re.sub(r'[\W_]+', ' ', text).split())
    return text or None


def names_overlap(first, second):
    """Whether one normalised name contains the other, as 'example press ltd' does 'example press'."""
    return ' {} '.format(first) in ' {} '.format(second) or ' {} '.format(second) in ' {} '.format(first)


def is_oapen_url(url):
    if not isinstance(url, str):
        return False
    parsed = urlparse(url.strip())
    host = parsed.hostname or ''
    return (host == 'oapen.org' or host.endswith('.oapen.org')
            or (host == 'hdl.handle.net' and parsed.path.startswith('/' + OAPEN_HANDLE_PREFIX)))


def parse_oapen_item(item):
    """Extract the identifiers of one OAPEN REST search result."""
    if not isinstance(item, dict):
        raise OAPENLookupError('OAPEN search returned a result that is not an item')
    handle = item.get('handle')
    metadata = item.get('metadata')
    if not isinstance(handle, str) or not handle or not isinstance(metadata, list):
        raise OAPENLookupError('OAPEN search returned an item without a handle or metadata')
    dois, isbns, work_ids, titles, alternatives, publishers = (
        set(), set(), set(), set(), set(), set())
    doi_field_readable = None
    for entry in metadata:
        if not isinstance(entry, dict) or not isinstance(entry.get('key'), str):
            raise OAPENLookupError(
                'OAPEN search returned malformed metadata for item {}'.format(handle))
        key = entry['key']
        value = entry.get('value')
        if not isinstance(value, str):
            continue
        work_ids.update(match.lower() for match in THOTH_WORK_ID_PATTERN.findall(value))
        if key.endswith('.doi') or (key.startswith('dc.identifier') and 'doi' in value.lower()):
            found = find_dois(value)
            dois.update(found)
            if key == DOI_FIELD:
                doi_field_readable = doi_field_readable is not False and bool(found)
        elif key.endswith('.isbn'):
            isbns.update(find_isbns(value))
        elif key == TITLE_FIELD:
            titles.add(value)
        elif key == 'dc.title.alternative':
            alternatives.add(value)
        elif key == 'publisher.name':
            publishers.add(normalise_text(value))
    full_titles = {'{}: {}'.format(title, alternative)
                   for title in titles for alternative in alternatives}
    return OAPENItem(
        handle,
        frozenset(dois),
        frozenset(isbns),
        frozenset(work_ids),
        frozenset(normalise_text(title) for title in titles | full_titles) - {None},
        frozenset(publishers) - {None},
        bool(doi_field_readable),
    )


class OAPENLibraryIndex:
    """Read-only fielded search of the OAPEN Library REST API."""

    def __init__(self, http_get=requests.get, pause=time.sleep):
        self.http_get = http_get
        self.pause = pause
        self.requests_sent = 0

    @staticmethod
    def build_query(field, value):
        # Phrase matching ignores punctuation, so quotes need no escaping
        phrase = ' '.join(value.replace('"', ' ').replace('\\', ' ').split())
        return '{}:"{}"'.format(field, phrase)

    def first_page(self, field, value):
        """Return the first page of items matching a quoted value."""
        return self.request_page(self.build_query(field, value), 0)

    def search(self, field, value):
        """Return every item matching a quoted value, or raise OAPENLookupError."""
        query = self.build_query(field, value)
        items = []
        handles = set()
        # Continue until an empty page: a page shorter than requested does not
        # prove that the server has no more results
        while len(items) <= OAPEN_MAX_RESULTS:
            batch = self.request_page(query, len(items))
            if not batch:
                return items
            for item in batch:
                handle = item.get('handle') if isinstance(item, dict) else None
                if not isinstance(handle, str) or not handle:
                    raise OAPENLookupError('OAPEN search returned an item without a handle')
                # Also stops a server that ignores the offset from paging forever
                if handle in handles:
                    raise OAPENLookupError(
                        'OAPEN search on {} returned item {} twice; results shifted '
                        'while paging'.format(field, handle))
                handles.add(handle)
            items.extend(batch)
        raise OAPENCoverageError(
            'OAPEN search on {} returned more than {} results; coverage incomplete'.format(
                field, OAPEN_MAX_RESULTS))

    def request_page(self, query, offset):
        # Pace requests: OAPEN has blocked the REST API over excessive traffic
        if self.requests_sent > 0:
            self.pause(OAPEN_REQUEST_INTERVAL_SECONDS)
        self.requests_sent += 1
        try:
            response = self.http_get(
                OAPEN_SEARCH_URL,
                params={
                    'query': query,
                    'expand': 'metadata',
                    'limit': OAPEN_PAGE_SIZE,
                    'offset': offset,
                },
                headers={
                    'Accept': 'application/json',
                    'User-Agent': OAPEN_USER_AGENT,
                },
                timeout=OAPEN_REQUEST_TIMEOUT,
            )
        except requests.RequestException as error:
            # Exception text can contain the full request URL; report its type only
            raise OAPENLookupError(
                'OAPEN search request failed ({})'.format(type(error).__name__)) from None
        if response.status_code != 200:
            raise OAPENLookupError(
                'OAPEN search returned HTTP status {}'.format(response.status_code))
        try:
            batch = response.json()
        except ValueError:
            raise OAPENLookupError('OAPEN search response was not valid JSON') from None
        if not isinstance(batch, list) or len(batch) > OAPEN_PAGE_SIZE:
            raise OAPENLookupError('OAPEN search response was not a page of items')
        return batch


class PreflightGatedSwordApi:
    """Expose the SWORD calls an upload makes, allowing one create per cleared preflight."""

    def __init__(self, api, work_id):
        self._api = api
        self._work_id = work_id
        self._create_cleared = False

    def clear_create(self):
        self._create_cleared = True

    def revoke_create(self):
        self._create_cleared = False

    def create_item(self, metadata_entry):
        if not self._create_cleared:
            raise DisseminationError(
                'Refusing to create OAPEN item for work {}: duplicate preflight has '
                'not cleared it'.format(self._work_id))
        # Retrying a create whose outcome is unknown could create a second item
        self._create_cleared = False
        try:
            receipt = self._api.create_item(metadata_entry)
        except Exception as error:
            detail = error if isinstance(error, DisseminationError) else type(error).__name__
            raise DisseminationError(
                'OAPEN SWORD create for work {} was not confirmed ({}). An item may '
                'nevertheless exist at OAPEN as an unfinished deposit, which OAPEN search '
                'cannot show: do not retry until OAPEN has confirmed whether it holds an '
                'item for "{}{}"'.format(self._work_id, detail, THOTH_WORK_ID_MARKER,
                                         self._work_id)) from error
        logging.info('Created OAPEN SWORD item for work {}: {}'.format(
            self._work_id, getattr(receipt, 'edit', None)))
        return receipt

    def upload_metadata(self, edit_media_iri, payload):
        return self._api.upload_metadata(edit_media_iri, payload)

    def upload_pdf(self, edit_media_iri, payload):
        return self._api.upload_pdf(edit_media_iri, payload)

    def complete_deposit(self, se_iri):
        return self._api.complete_deposit(se_iri)

    def delete_item(self, resource_iri):
        try:
            return self._api.delete_item(resource_iri)
        except Exception:
            logging.error(
                'Incomplete OAPEN item {} for work {} may remain in the SWORD workspace: '
                'reconcile with OAPEN before any retry'.format(resource_iri, self._work_id))
            raise


class OAPENSWORDUploader(DSpaceUploader):
    """Dissemination logic for OAPEN SWORD"""

    def __init__(self, work_id, export_url, client_url, version):
        """Set OAPEN-specific parameters and pass them to DSpaceUploader"""
        user_name_string = 'oapen_sword_user'
        user_pass_string = 'oapen_sword_pw'
        service_document_iri = ("https://library.oapen.org/swordv2/servicedocument")
        collection_iri = ("https://library.oapen.org/swordv2/collection/20.500.12657/106523")
        metadata_profile = MetadataProfile.OAPEN
        super().__init__(
            work_id,
            export_url,
            client_url,
            version,
            user_name_string,
            user_pass_string,
            service_document_iri,
            collection_iri,
            metadata_profile)
        self.api = PreflightGatedSwordApi(self.api, work_id)
        self.oapen_index = OAPENLibraryIndex()

    def upload_to_platform(self):
        """Check for an existing OAPEN item, then perform standard upload and OAPEN-specific processing"""
        # Exits for missing mandatory metadata before any OAPEN request is spent
        self.parse_metadata()
        result = self.assess_existing_deposit()
        logging.info('OAPEN duplicate preflight for work {}: {} - {}'.format(
            self.work_id, result.decision.value, result.reason))
        if result.decision != PreflightDecision.CREATE_ELIGIBLE:
            raise DisseminationError('Not creating OAPEN item for work {}: {} - {}'.format(
                self.work_id, result.decision.value, result.reason))
        self.api.clear_create()
        try:
            (publication_id, _, deposit_receipt) = super().upload_to_platform()
        finally:
            self.api.revoke_create()
        # Return details of created upload to be entered as a Thoth Location
        landing_page = deposit_receipt.alternate
        # Receipt only contains SWORDv2 server URL - translate to frontend URL
        bitstream_root = landing_page.replace('/handle', '/bitstream/handle')
        full_text_url = ('{}/{}.pdf?sequence=2&isAllowed=y').format(bitstream_root, self.work_id)
        location_platform = 'OAPEN'
        return [Location(publication_id, location_platform, landing_page,
                         full_text_url, None, None)]

    def assess_existing_deposit(self):
        """
        Decide whether a new OAPEN item may be created for this work.
        Only a complete, verified search that finds no matching item permits it.
        """
        identity = self.get_deposit_identity()
        if isinstance(identity, PreflightResult):
            return identity

        queries = []
        if identity.doi is not None:
            queries.append((DOI_FIELD, identity.doi))
        queries.append((MARKER_FIELD, THOTH_WORK_ID_MARKER + identity.work_id))
        queries.extend((ISBN_FIELD, isbn) for isbn in sorted(identity.isbns))
        queries.append((TITLE_FIELD, identity.search_title))
        items = {}
        found_by = {}
        title_incomplete = False
        try:
            # Bracket the work's searches with controls, so that an index which
            # starts answering again partway through cannot pass as complete
            self.verify_index_controls(INDEX_CONTROLS[:1])
            for (field, value) in queries:
                try:
                    results = self.oapen_index.search(field, value)
                except OAPENCoverageError:
                    if field != TITLE_FIELD:
                        raise
                    title_incomplete = True
                    continue
                for item in map(parse_oapen_item, results):
                    items[item.handle] = item
                    found_by.setdefault(item.handle, set()).add(field)
        except OAPENLookupError as error:
            return PreflightResult(PreflightDecision.UNKNOWN_REMOTE_STATE, str(error))

        matched = []
        unreadable = []
        possible = []
        for item in items.values():
            if ((identity.doi is not None and identity.doi in item.dois)
                    or identity.isbns & item.isbns
                    or identity.work_id in item.work_ids):
                matched.append(item)
            elif self.is_unexplained_hit(identity, item, found_by[item.handle]):
                unreadable.append(item)
            elif self.is_possible_title_match(identity, item):
                possible.append(item)

        candidates = matched + unreadable + possible
        if len(candidates) > 1:
            return PreflightResult(
                PreflightDecision.AMBIGUOUS,
                'several OAPEN items may represent this work: {}'.format(
                    ', '.join(sorted(item.handle for item in candidates))))
        if unreadable:
            return PreflightResult(
                PreflightDecision.AMBIGUOUS,
                'OAPEN item {} was returned by an identifier search, but none of its '
                'identifiers could be read as this work\'s or as a longer DOI; requires '
                'review'.format(unreadable[0].handle))
        if possible:
            return PreflightResult(
                PreflightDecision.AMBIGUOUS,
                'OAPEN item {} has the same title and publisher and no DOI that rules out '
                'a match; requires review'.format(possible[0].handle))
        if matched:
            return self.compare_matched_item(identity, matched[0])
        if title_incomplete:
            return PreflightResult(
                PreflightDecision.AMBIGUOUS,
                'more than {} OAPEN items contain the title "{}", so a record without '
                'identifiers cannot be ruled out; requires review'.format(
                    OAPEN_MAX_RESULTS, identity.search_title))

        try:
            self.verify_index_controls(INDEX_CONTROLS)
        except OAPENLookupError as error:
            return PreflightResult(PreflightDecision.UNKNOWN_REMOTE_STATE, str(error))
        return PreflightResult(
            PreflightDecision.CREATE_ELIGIBLE,
            'no OAPEN item matches DOI, ISBNs, Thoth work ID or title; index controls passed')

    def get_deposit_identity(self):
        """Return the identifiers to search for, or a PreflightResult that stops the upload."""
        work = ((self.metadata or {}).get('data') or {}).get('work') or {}
        work_id = str(work.get('workId') or '').lower()
        if work_id != self.work_id.lower():
            return PreflightResult(
                PreflightDecision.INELIGIBLE,
                'Thoth metadata is for work {!r}, not the requested work'.format(work_id))
        publications = work.get('publications')
        if not isinstance(publications, list):
            return PreflightResult(PreflightDecision.INELIGIBLE,
                                   'Thoth metadata has no publication list')
        pdf_publications = [publication for publication in publications
                            if publication.get('publicationType') == 'PDF']
        if len(pdf_publications) != 1 or not pdf_publications[0].get('publicationId'):
            return PreflightResult(
                PreflightDecision.INELIGIBLE,
                'work needs exactly one PDF publication, found {}'.format(len(pdf_publications)))

        existing = []
        for publication in publications:
            locations = publication.get('locations')
            if not isinstance(locations, list):
                return PreflightResult(
                    PreflightDecision.INELIGIBLE,
                    'Thoth metadata has no location list for publication {}'.format(
                        publication.get('publicationId')))
            existing.extend(
                location.get('landingPage') or location.get('fullTextUrl')
                for location in locations
                if location.get('locationPlatform') == 'OAPEN'
                or is_oapen_url(location.get('landingPage'))
                or is_oapen_url(location.get('fullTextUrl')))
        if existing:
            return PreflightResult(
                PreflightDecision.ALREADY_PRESENT,
                'Thoth already records an OAPEN location: {}'.format(
                    ', '.join(sorted(str(url) for url in existing))))

        doi = None
        if work.get('doi'):
            doi = normalise_doi(work['doi'])
            if doi is None:
                return PreflightResult(PreflightDecision.INELIGIBLE,
                                       'work DOI {!r} is not a valid DOI'.format(work['doi']))
        isbns = set()
        for publication in publications:
            if publication.get('isbn'):
                isbn = normalise_isbn(publication['isbn'])
                if isbn is None:
                    return PreflightResult(
                        PreflightDecision.INELIGIBLE,
                        '{} ISBN {!r} is not a valid ISBN'.format(
                            publication.get('publicationType'), publication['isbn']))
                isbns.add(isbn)
        if doi is None and not isbns:
            return PreflightResult(
                PreflightDecision.INELIGIBLE,
                'work has neither a DOI nor an ISBN to search OAPEN for')
        search_title = work.get('title') or work.get('fullTitle')
        titles = {normalise_text(title) for title in (work.get('title'), work.get('fullTitle'))}
        titles.discard(None)
        if not titles:
            return PreflightResult(PreflightDecision.INELIGIBLE, 'work has no title')
        imprint = work.get('imprint') or {}
        publishers = {normalise_text(imprint.get('imprintName')),
                      normalise_text((imprint.get('publisher') or {}).get('publisherName'))}
        publishers.discard(None)
        return DepositIdentity(work_id, doi, frozenset(isbns), search_title,
                               frozenset(titles), frozenset(publishers))

    @staticmethod
    def is_unexplained_hit(identity, item, fields):
        """
        An unmatched identifier-search hit is unrelated only if the DOI search
        returned it for readable DOIs that begin with ours, and every such DOI
        extends ours, as a chapter DOI does. Exact-value marker and ISBN searches
        have no such explanation.
        """
        if MARKER_FIELD in fields or ISBN_FIELD in fields:
            return True
        if DOI_FIELD in fields:
            longer = [doi for doi in item.dois if doi.startswith(identity.doi)]
            return not (item.doi_field_readable and longer
                        and all(extends_doi(doi, identity.doi) for doi in longer))
        return False

    @staticmethod
    def is_possible_title_match(identity, item):
        """
        Flag a same-title, same-publisher item for review unless it has its own DOI,
        one that does not begin with ours. Disjoint ISBNs do not rule it out: OAPEN
        and Thoth may each hold only some of a book's format ISBNs.
        """
        if not identity.titles & item.titles:
            return False
        if item.publishers and not any(names_overlap(ours, theirs)
                                       for ours in identity.publishers
                                       for theirs in item.publishers):
            return False
        return identity.doi is None or not {
            doi for doi in item.distinguishing_dois() if not doi.startswith(identity.doi)}

    @staticmethod
    def compare_matched_item(identity, item):
        conflicts = []
        other_work_ids = item.work_ids - {identity.work_id}
        if other_work_ids:
            conflicts.append('it was deposited for Thoth work {}'.format(
                ', '.join(sorted(other_work_ids))))
        if identity.doi is not None and item.distinguishing_dois() - {identity.doi}:
            conflicts.append('its DOI {} differs'.format(
                ', '.join(sorted(item.distinguishing_dois()))))
        if identity.isbns and item.isbns and not identity.isbns & item.isbns:
            conflicts.append('none of its ISBNs {} belong to this work'.format(
                ', '.join(sorted(item.isbns))))
        if conflicts:
            return PreflightResult(
                PreflightDecision.CONFLICT,
                'OAPEN item {} shares an identifier with this work, but {}'.format(
                    item.handle, '; '.join(conflicts)))
        origin = ('was deposited by Thoth for this work'
                  if identity.work_id in item.work_ids
                  else 'was not deposited by Thoth (publisher or other route)')
        return PreflightResult(
            PreflightDecision.RECONCILE_EXISTING,
            'OAPEN item {} {} but Thoth has no OAPEN location; reconcile instead of '
            'depositing again'.format(item.handle, origin))

    def verify_index_controls(self, controls):
        """Raise OAPENLookupError unless each control search finds and reads its record."""
        for (field, value, handle) in controls:
            found = [parse_oapen_item(item) for item in self.oapen_index.first_page(field, value)]
            control = [item for item in found if item.handle == handle]
            if field == DOI_FIELD:
                readable = control and normalise_doi(value) in control[0].dois
            elif field == ISBN_FIELD:
                readable = control and normalise_isbn(value) in control[0].isbns
            elif field == MARKER_FIELD:
                readable = control and value[len(THOTH_WORK_ID_MARKER):] in control[0].work_ids
            else:
                readable = control and normalise_text(value) in control[0].titles
            if not readable:
                raise OAPENLookupError(
                    'OAPEN index control for {} did not return record {}; an empty '
                    'search cannot be trusted'.format(field, handle))
