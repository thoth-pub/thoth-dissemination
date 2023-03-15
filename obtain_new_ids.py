#!/usr/bin/env python3
"""
Acquire a list of work IDs to be disseminated.
Purpose: automatic dissemination at regular intervals of specified works from selected publishers.
For dissemination to Internet Archive: find newly-published works for upload.
For dissemination to Crossref: find newly-updated works for metadata deposit (including update).
Based on `iabulkupload/obtain_work_ids.py`.
"""

# Both third-party packages already included in thoth-dissemination/requirements.txt
from internetarchive import search_items
from thothlibrary import errors, ThothClient
import argparse
import json
import logging
from os import environ
import sys


class IDFinder():

    def __init__(self):
        self.thoth = ThothClient()

    def get_publishers(self):
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

        publishers = json.dumps(publishers_env)
        return publishers

    def get_thoth_ids(self, work_statuses, order, publishers, updated_at_with_relations):
        # `books` query includes Monographs, Edited Books, Textbooks and Journal Issues
        # but excludes Chapters and Book Sets. `bookIds` variant only retrieves their workIds.
        logging.info(work_statuses)
        logging.info(updated_at_with_relations)
        thoth_works = self.thoth.bookIds(
            # The default limit is 100; publishers' back catalogues may be bigger than that
            limit='5',
            # work_statuses=work_statuses,
            # order=order,
            publishers=publishers,
            # updated_at_with_relations=updated_at_with_relations,
        )

        # Extract the Thoth work ID strings from the set of results
        thoth_ids = [n.workId for n in thoth_works]

        return thoth_ids

    def remove_exceptions(self, thoth_ids):
        # If a list of exceptions has been provided, remove these from the results
        # (e.g. works that are ineligible for upload due to not being available as PDFs)
        if environ.get('ENV_EXCEPTIONS') is not None:
            try:
                exceptions = json.loads(environ.get('ENV_EXCEPTIONS'))
                thoth_ids = set(thoth_ids).difference(exceptions)
            except:
                # No need to early-exit; current use case for exceptions list is
                # just to avoid attempting uploads which are expected to fail
                logging.warning(
                    'Failed to retrieve excepted works from environment variable')

        return thoth_ids


class CrossrefIDFinder(IDFinder):

    def get_query_parameters(self):
        from datetime import datetime, timedelta, timezone

        # The schedule for finding and depositing updated metadata is once daily.
        # TODO ideally we could pass this value from the GitHub Action to ensure synchronisation.
        DEPOSIT_INTERVAL_HRS = 24

        # Scheduled GitHub Actions we have run so far have not started until ~2h after specified time.
        # Ensure we don't miss any works which were been updated in the gap between
        # when the Action should have run and when it actually ran.
        DELAY_BUFFER_HRS = 3

        # Obtain all works listed in Thoth from the selected publishers which are
        # either Active or Forthcoming, and which have been updated since the last deposit.
        current_time = datetime.now(timezone.utc)
        last_deposit_time = current_time - \
            timedelta(hours=(DEPOSIT_INTERVAL_HRS + DELAY_BUFFER_HRS))
        last_deposit_time_str = datetime.strftime(
            last_deposit_time, "%Y-%m-%dT%H:%M:%SZ")

        work_statuses = '[ACTIVE, FORTHCOMING]'
        # Start with the most recently updated
        order = '{field: UPDATED_AT_WITH_RELATIONS, direction: DESC}'
        updated_at_with_relations = '{{timestamp: "{}", expression: GREATER_THAN}}'.format(
            last_deposit_time_str)

        return work_statuses, order, updated_at_with_relations

    def post_process(self, thoth_ids):
        return thoth_ids


class InternetArchiveIDFinder(IDFinder):

    def get_query_parameters(self):
        # Obtain all active (published) works listed in Thoth from the selected publishers.
        work_statuses = '[ACTIVE]'
        # Start with the earliest, so that the upload is logically ordered
        order = '{field: PUBLICATION_DATE, direction: ASC}'
        updated_at_with_relations = None

        return work_statuses, order, updated_at_with_relations

    def post_process(self, thoth_ids):
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
        new_ids = list(set(thoth_ids).difference(ia_ids))

        return new_ids


def get_arguments():
    # Simple argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--platform")
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s:%(asctime)s: %(message)s')

    args = get_arguments()
    platform = args.platform

    if platform == 'InternetArchive':
        id_finder = InternetArchiveIDFinder()
    elif platform == 'Crossref':
        id_finder = CrossrefIDFinder()
    else:
        logging.error(
            'Platform must be one of InternetArchive or Crossref')
        sys.exit(1)

    publishers = id_finder.get_publishers()

    work_statuses, order, updated_at_with_relations = id_finder.get_query_parameters()

    thoth_ids = id_finder.get_thoth_ids(work_statuses, order, publishers, updated_at_with_relations)

    thoth_ids = id_finder.remove_exceptions(thoth_ids)

    new_ids = id_finder.post_process(thoth_ids)

    # Output this list (as an array of comma-separated, quote-enclosed strings)
    print(new_ids)
