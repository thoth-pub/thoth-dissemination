#!/usr/bin/env python3
"""
Acquire a list of work IDs to be disseminated.
Purpose: automatic dissemination at regular intervals of specified works from selected publishers.
For dissemination to Internet Archive, (Loughborough) Figshare, Zenodo, CUL and Google Play:
find newly-published works for upload.
For dissemination to Crossref: find newly-updated works for metadata deposit (including update).
Based on `iabulkupload/obtain_work_ids.py`.
"""

# Both third-party packages already included in thoth-dissemination/requirements.txt
from internetarchive import search_items
from thothlibrary import errors, ThothClient
import argparse
import json
import logging
from datetime import datetime, timedelta, UTC
from os import environ
import sys


class IDFinder():
    """Common logic for retrieving work IDs for all platforms"""

    def __init__(self):
        """Set up Thoth client instance and variables for use in other methods"""
        self.thoth = ThothClient()
        self.thoth_ids = []
        self.work_statuses = None
        self.work_types = None
        self.publishers = None
        self.order = None
        self.updated_at_with_relations = None

    def run(self):
        """
        Retrieve the required set of work IDs and output them
        (as an array of comma-separated, quote-enclosed strings)
        """
        self.get_publishers()
        self.get_query_parameters()
        self.get_thoth_ids()
        self.remove_exceptions()
        self.post_process()
        logging.info('List of IDs found: {}'.format(self.thoth_ids))
        print(self.thoth_ids)

    def get_publishers(self):
        """"Retrieve IDs for all publishers whose works should be included"""
        # Check that a list of IDs of publishers whose works should be uploaded
        # has been provided as a JSON-formatted environment variable
        try:
            publishers_env = json.loads(environ.get('ENV_PUBLISHERS'))
        except:
            logging.error(
                'Failed to retrieve publisher IDs from environment variable')
            sys.exit(1)

        # Test that list is not empty - if so, the Thoth client call would erroneously
        # retrieve the full list of works from all publishers
        if len(publishers_env) < 1:
            logging.error(
                'No publisher IDs found in environment variable: list is empty')
            sys.exit(1)

        # Test that all supplied publisher IDs are valid - if a mistyped ID was passed to the Thoth
        # client call, it would behave the same as a valid ID for which no relevant works exist
        for publisher in publishers_env:
            try:
                self.thoth.publisher(publisher)
            except errors.ThothError:
                # Don't include full error text as it's lengthy (contains full query/response)
                logging.error('No record found for publisher {}: ID may be incorrect'.format(
                    publisher))
                sys.exit(1)

        self.publishers = json.dumps(publishers_env)

    def get_query_parameters(self):
        """
        Construct Thoth work ID query parameters depending on platform-specific
        requirements
        """
        # Default: all active (published) works listed in Thoth (from the selected publishers).
        self.work_statuses = '[ACTIVE]'
        # Default: all work types included except for chapters (from the selected publishers).
        self.work_types = '[MONOGRAPH, EDITED_BOOK, JOURNAL_ISSUE, TEXTBOOK, BOOK_SET]'
        # Start with the most recent, so that we can disregard everything else
        # as soon as we hit the first work published earlier than the desired date range.
        self.order = '{field: PUBLICATION_DATE, direction: DESC}'
        self.updated_at_with_relations = None

    def get_thoth_ids(self):
        """Query Thoth GraphQL API with relevant parameters to retrieve required work IDs"""
        # `books` query includes Monographs, Edited Books, Textbooks and Journal Issues
        # but excludes Chapters and Book Sets. `bookIds` variant only retrieves their workIds.
        thoth_works = self.thoth.bookIds(
            # The default limit is 100; publishers' back catalogues may be bigger than that
            limit='9999',
            work_statuses=self.work_statuses,
            order=self.order,
            publishers=self.publishers,
            updated_at_with_relations=self.updated_at_with_relations,
        )

        # Extract the Thoth work ID strings from the set of results
        self.thoth_ids = [n.workId for n in thoth_works]

    def get_thoth_ids_iteratively(self, start_date, end_date):
        """
        Query Thoth GraphQL API with relevant parameters to retrieve required work IDs,
        iterating through results to select only those published between the specified dates
        """
        # TODO Once https://github.com/thoth-pub/thoth/issues/486 is completed,
        # we can simply construct a standard query filtering by publication date
        offset = 0
        while True:
            next_batch = self.thoth.works(
                limit=1,
                offset=offset,
                work_statuses=self.work_statuses,
                work_types=self.work_types,
                order=self.order,
                publishers=self.publishers,
                updated_at_with_relations=self.updated_at_with_relations,
            )
            if len(next_batch) < 1:
                # No more works to be found
                break
            offset += 1
            next_work = next_batch[0]
            next_work_pub_date = datetime.strptime(next_work.publicationDate, "%Y-%m-%d").date()
            if next_work_pub_date > end_date:
                # This work will be handled in the next run - don't cause duplication
                continue
            elif next_work_pub_date >= start_date:
                # This work was published in the target period - include it
                self.thoth_ids.append(next_work.workId)
            else:
                # We've reached the first work in the list which was published
                # earlier than the target period - stop
                break

    def remove_exceptions(self):
        """
        If a list of exceptions has been provided, remove these from the results
        (e.g. works that are ineligible for upload due to not being available as PDFs)
        """
        # Omitted exceptions may be represented as None if running locally,
        # or an empty string if passed via GitHub Actions inheritance
        if environ.get('ENV_EXCEPTIONS'):
            try:
                exceptions = json.loads(environ.get('ENV_EXCEPTIONS').lower())
                self.thoth_ids = list(
                    set(self.thoth_ids).difference(exceptions))
            except Exception:
                # Current use case for exceptions list is just to avoid attempting
                # uploads which are expected to fail. However, an exception here
                # would indicate that the list has been entered incorrectly.
                # Early-exit to alert users that it needs to be fixed.
                logging.error(
                    'Failed to retrieve excepted works from environment variable')
                sys.exit(1)

    def post_process(self):
        """
        Amend list of retrieved work IDs depending on platform-specific
        requirements
        """
        # Default: not required - keep full list
        pass


class CrossrefIDFinder(IDFinder):
    """Logic for retrieving work IDs which is specific to Crossref dissemination"""

    def get_query_parameters(self):
        """Construct Thoth work ID query parameters depending on Crossref-specific requirements"""
        # The schedule for finding and depositing updated metadata is once hourly.
        # TODO ideally we could pass this value from the GitHub Action to ensure synchronisation.
        DEPOSIT_INTERVAL_HRS = 1

        # Scheduled GitHub Actions may not start exactly at the specified time.
        # A couple of months of daily runs showed average delay of 10-15 mins.
        # Try to avoid missing any works which were updated in the gap between
        # when the Action should have run and when it actually ran.
        DELAY_BUFFER_HRS = 0.25

        # Target: all works listed in Thoth (from the selected publishers) which are
        # Active, and which have been updated since the last deposit.
        # Use UTC, as GitHub Actions scheduling runs in UTC.
        current_time = datetime.now(UTC)
        last_deposit_time = current_time - \
            timedelta(hours=(DEPOSIT_INTERVAL_HRS + DELAY_BUFFER_HRS))
        last_deposit_time_str = datetime.strftime(
            last_deposit_time, "%Y-%m-%dT%H:%M:%SZ")

        self.work_statuses = '[ACTIVE]'
        # Start with the most recently updated
        self.order = '{field: UPDATED_AT_WITH_RELATIONS, direction: DESC}'
        self.updated_at_with_relations = '{{timestamp: "{}", expression: GREATER_THAN}}'.format(
            last_deposit_time_str)


class InternetArchiveIDFinder(IDFinder):
    """Logic for retrieving work IDs which is specific to Internet Archive dissemination"""

    def get_query_parameters(self):
        """Construct Thoth work ID query parameters depending on Internet Archive-specific requirements"""
        # Target: all active (published) works listed in Thoth (from the selected publishers).
        self.work_statuses = '[ACTIVE]'
        # Start with the earliest, so that the upload is logically ordered
        self.order = '{field: PUBLICATION_DATE, direction: ASC}'
        self.updated_at_with_relations = None

    def post_process(self):
        """Amend list of retrieved work IDs depending on Internet Archive-specific requirements"""

        # Obtain all works listed in the Internet Archive's Thoth Archiving Network collection.
        # We only need the identifier; this matches the Thoth work ID.
        # If the collection later grows to include more publishers, we may want to
        # additionally filter the query to only return works from those selected.
        ia_works = search_items(
            query='collection:thoth-archiving-network', fields=['identifier'])

        # Extract the IA identifiers from the set of results
        ia_ids = [n['identifier'] for n in ia_works]

        # The set of IDs of works that need to be uploaded to the Internet Archive
        # is those which appear as published for the selected publishers in Thoth
        # but do not appear as already uploaded to the IA collection
        # (minus any specified exceptions).
        self.thoth_ids = list(set(self.thoth_ids).difference(ia_ids))


class CatchupIDFinder(IDFinder):
    """
    Logic for retrieving work IDs which is specific to recurring 'catchup'
    dissemination of recent publications to various archiving platforms.
    Currently used for (Loughborough) Figshare, CUL and Zenodo. Internet Archive
    is handled separately, as its API allows a simpler workflow.
    """

    def get_thoth_ids(self):
        """Query Thoth GraphQL API with relevant parameters to retrieve required work IDs"""
        # TODO Once https://github.com/thoth-pub/thoth/issues/486 is completed,
        # we can remove this overriding method and simply construct a standard query
        # filtering by publication date

        # In addition to the conditions of the query parameters, we need to filter the results
        # to obtain only works with a publication date within the previous calendar month.
        # The schedule for finding and depositing newly published works is once monthly
        # (a few days after the start of the month, to allow for delays in updating records).
        current_date = datetime.now(UTC).date()
        current_month_start = current_date.replace(day=1)
        previous_month_end = current_month_start - timedelta(days=1)
        previous_month_start = previous_month_end.replace(day=1)

        self.get_thoth_ids_iteratively(previous_month_start, previous_month_end)


class GooglePlayIDFinder(IDFinder):
    """Logic for retrieving work IDs which is specific to Google Play dissemination"""

    def get_thoth_ids(self):
        """Query Thoth GraphQL API with relevant parameters to retrieve required work IDs"""
        # TODO Once https://github.com/thoth-pub/thoth/issues/486 is completed,
        # we can remove this overriding method and simply construct a standard query
        # filtering by publication date

        # In addition to the conditions of the query parameters, we need to filter the results
        # to obtain only works with a publication date within the previous day.
        # The schedule for finding and depositing newly published works is once daily.
        current_date = datetime.now(UTC).date()
        previous_day = current_date - timedelta(days=1)

        self.get_thoth_ids_iteratively(previous_day, previous_day)


class OapenIDFinder(IDFinder):
    """Logic for retrieving work IDs which is specific to OAPEN dissemination"""

    def get_thoth_ids(self):
        """Query Thoth GraphQL API with relevant parameters to retrieve required work IDs"""
        # TODO Once https://github.com/thoth-pub/thoth/issues/486 is completed,
        # we can remove this overriding method and simply construct a standard query
        # filtering by publication date

        # In addition to the conditions of the query parameters, we need to filter the results
        # to obtain only works with a publication date within the previous week.
        # The schedule for finding and depositing newly published works is once weekly.
        current_date = datetime.now(UTC).date()
        previous_week_end = current_date - timedelta(days=1)
        previous_week_start = previous_week_end - timedelta(days=6)

        self.get_thoth_ids_iteratively(previous_week_start, previous_week_end)


class BKCIIDFinder(IDFinder):
    """Logic for retrieving work IDs which is specific to Clarivate Web of Science Book Citation Index (BKCI) dissemination"""

    def get_query_parameters(self):
        """Construct Thoth work ID query parameters depending on Clarivate BKCI-specific requirements"""
        # Target: all active (published) works listed in Thoth (from the selected publishers), except for textbooks
        self.work_statuses = '[ACTIVE]'
        self.work_types = '[MONOGRAPH, EDITED_BOOK, JOURNAL_ISSUE, BOOK_SET]'
        # Start with the most recent, so that we can disregard everything else
        # as soon as we hit the first work published earlier than the desired date range.
        self.order = '{field: PUBLICATION_DATE, direction: DESC}'
        self.updated_at_with_relations = None

    def get_thoth_ids(self):
        """Query Thoth GraphQL API with relevant parameters to retrieve required work IDs"""
        # TODO Once https://github.com/thoth-pub/thoth/issues/486 is completed,
        # we can remove this overriding method and simply construct a standard query
        # filtering by publication date

        # In addition to the conditions of the query parameters, we need to filter the results
        # to obtain only works with a publication date within the previous calendar month.
        # The schedule for finding and depositing newly published works is once monthly
        # (a few days after the start of the month, to allow for delays in updating records).
        current_date = datetime.now(UTC).date()
        current_month_start = current_date.replace(day=1)
        previous_month_end = current_month_start - timedelta(days=1)
        previous_month_start = previous_month_end.replace(day=1)

        self.get_thoth_ids_iteratively(previous_month_start, previous_month_end)

class OapenLocationsIDFinder(IDFinder):
    """
    Helper class for workflow which updates Thoth records with newly-registered
    OAPEN/DOAB location URLs (by searching their APIs). See obtain_locations.py.
    """

    def get_query_parameters(self):
        """Construct Thoth work ID query parameters based on OAPEN location workflow requirements"""
        # Target: all active (published) works listed in Thoth (from the selected publishers).
        self.work_statuses = '[ACTIVE]'
        # Order doesn't matter: default to publication date descending
        self.order = '{field: PUBLICATION_DATE, direction: DESC}'
        self.updated_at_with_relations = None

    def post_process(self):
        """
        Narrow down the results to works which have a PDF publication but no OAPEN location.
        Note that this returns a list of tuples of publication IDs and DOIs
        (both required in next stage of workflow), rather than a list of work IDs.
        """
        oapen_location_required = []
        for id in self.thoth_ids:
            work = self.thoth.work_by_id(id)
            try:
                pdf_publication = [pub for pub in work.publications
                                   if pub.publicationType == 'PDF'][0]
            except IndexError:
                # No PDF publication, so no OAPEN location - skip
                continue
            try:
                [loc for loc in pdf_publication.locations
                 if loc.locationPlatform == 'OAPEN'][0]
            except IndexError:
                # No existing OAPEN location found - add it to the list to search on
                if work.doi:
                    # If the work doesn't have a DOI, we can't easily search on it - skip
                    doi = work.doi.replace('https://doi.org/', '')
                    publication_id = pdf_publication.publicationId
                    oapen_location_required.append((publication_id, doi))

        self.thoth_ids = oapen_location_required


def get_arguments():
    """Simple argument parsing"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--platform")
    parser.add_argument("--locations", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s:%(asctime)s: %(message)s')

    args = get_arguments()
    platform = args.platform
    locations = args.locations

    if locations:
        match platform:
            case 'OAPEN':
                id_finder = OapenLocationsIDFinder()
            case _:
                logging.error(
                    'Locations option is only supported for OAPEN')
                sys.exit(1)
    else:
        match platform:
            case 'InternetArchive':
                id_finder = InternetArchiveIDFinder()
            case 'Crossref':
                id_finder = CrossrefIDFinder()
            case 'GooglePlay':
                id_finder = GooglePlayIDFinder()
            case 'OAPEN':
                id_finder = OapenIDFinder()
            case 'Figshare' | 'Zenodo' | 'CUL':
                id_finder = CatchupIDFinder()
            case 'BKCI':
                id_finder = BKCIIDFinder()
            case _:
                logging.error(
                    'Platform must be one of InternetArchive, Crossref, Figshare, '
                    'Zenodo, CUL, GooglePlay, BKCI or OAPEN')
                sys.exit(1)

    id_finder.run()
