#!/usr/bin/env python3
"""
Acquire a list of work IDs to have metadata deposited in Crossref.
Purpose: automatic deposit at regular intervals of newly-updated works from selected publishers.
Based on `iabulkupload/obtain_work_ids.py`.
"""

# Third-party package already included in thoth-dissemination/requirements.txt
from thothlibrary import errors, ThothClient
from datetime import datetime, timedelta, timezone
import json
import logging
from os import environ
import sys

# The schedule for finding and depositing updated metadata is once daily.
# TODO ideally we could pass this value from the GitHub Action to ensure synchronisation.
DEPOSIT_INTERVAL_HRS = 24

# Scheduled GitHub Actions we have run so far have not started until ~2h after specified time.
# Ensure we don't miss any works which were been updated in the gap between
# when the Action should have run and when it actually ran.
DELAY_BUFFER_HRS = 3

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:%(asctime)s: %(message)s')
thoth = ThothClient()

# Check that a list of IDs of publishers whose works should be uploaded
# has been provided as a JSON-formatted environment variable
try:
    publishers_env = json.loads(environ.get('ENV_PUBLISHERS_CROSSREF'))
except:
    logging.error('Failed to retrieve publisher IDs from environment variable')
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
        thoth.publisher(publisher)
    except errors.ThothError:
        # Don't include full error text as it's lengthy (contains full query/response)
        logging.error('No record found for publisher {}: ID may be incorrect'.format(
            publisher))
        sys.exit(1)

publishers = json.dumps(publishers_env)

# Obtain all works listed in Thoth from the selected publishers which are
# either Active or Forthcoming, and which have been updated since the last deposit.

current_time = datetime.now(timezone.utc)
last_deposit_time = current_time - \
    timedelta(hours=(DEPOSIT_INTERVAL_HRS + DELAY_BUFFER_HRS))

limit = 50
offset = 0

# Placeholder values
thoth_works = []
earliest_updated_time = current_time

while earliest_updated_time > last_deposit_time:
    # `books` query includes Monographs, Edited Books, Textbooks and Journal Issues
    # but excludes Chapters and Book Sets.
    thoth_works += thoth.books(
        limit=limit,
        offset=offset,
        # TODO: we need both Active and Forthcoming works. Requires change to GraphQL schema
        # c.f. work_types for `works` query. As a workaround, could make two calls.
        work_status='ACTIVE',
        # Start with the most recently updated
        order='{field: UPDATED_AT_WITH_RELATIONS, direction: DESC}',
        publishers=publishers,
    )
    offset += limit
    earliest_updated_time_str = thoth_works.last().get('updatedAtWithRelations')
    earliest_updated_time = datetime.strptime(
        earliest_updated_time_str, "%Y-%m-%dT%H:%M:%S.%f%z")

# Remove any results where last update is earlier than last deposit.
thoth_works = [n for n in thoth_works if datetime.strptime(
    n.get('updatedAtWithRelations'), "%Y-%m-%dT%H:%M:%S.%f%z") > last_deposit_time]

# Extract the Thoth work ID strings from the set of results
thoth_ids = [n.workId for n in thoth_works]

# If a list of exceptions has been provided, remove these from the results
if environ.get('ENV_EXCEPTIONS_CROSSREF') is not None:
    try:
        exceptions = json.loads(environ.get('ENV_EXCEPTIONS_CROSSREF'))
        thoth_ids = set(thoth_ids).difference(exceptions)
    except:
        # No need to early-exit; current use case for exceptions list is
        # just to avoid attempting uploads which are expected to fail
        logging.warning(
            'Failed to retrieve excepted works from environment variable')

# Output this list (as an array of comma-separated, quote-enclosed strings)
print(thoth_ids)
