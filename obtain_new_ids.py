#!/usr/bin/env python3
"""
Acquire a list of work IDs to be uploaded to Internet Archive.
Purpose: automatic upload at regular intervals of newly-published works from selected publishers.
Based on `iabulkupload/obtain_work_ids.py`.
"""

# Both third-party packages already included in thoth-dissemination/requirements.txt
from internetarchive import search_items
from thothlibrary import errors, ThothClient
import json
from os import environ
import sys

thoth = ThothClient()

# Check that a list of IDs of publishers whose works should be uploaded
# has been provided as a JSON-formatted environment variable
try:
    publishers_env = json.loads(environ.get('ENV_PUBLISHERS'))
except:
    print("ERROR: Failed to retrieve publisher IDs from environment variable")
    sys.exit(1)

# Test that list is not empty - if so, the Thoth client call would erroneously
# retrieve the full list of works from all publishers
if len(publishers_env) < 1:
    print("ERROR: No publisher IDs found in environment variable: list is empty")
    sys.exit(1)

# Test that all supplied publisher IDs are valid - if a mistyped ID was passed to the Thoth
# client call, it would behave the same as a valid ID for which no relevant works exist
for publisher in publishers_env:
    try:
        thoth.publisher(publisher)
    except errors.ThothError:
        # Don't include full error text as it's lengthy (contains full query/response)
        print("ERROR: No record found for publisher {}: ID may be incorrect".format(publisher))
        sys.exit(1)

publishers = json.dumps(publishers_env)

# Obtain all active (published) works listed in Thoth from the selected publishers.
# `books` query includes Monographs, Edited Books, Textbooks and Journal Issues
# but excludes Chapters and Book Sets.
thoth_works = thoth.books(
    # The default limit is 100; publishers' back catalogues may be bigger than that
    limit='9999',
    work_status='ACTIVE',
    # Start with the earliest, so that the upload is logically ordered
    order='{field: PUBLICATION_DATE, direction: ASC}',
    publishers=publishers,
)

# Extract the Thoth work IDs from the set of results
thoth_ids = [n.workId for n in thoth_works]

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
# but do not appear as already uploaded to the IA collection.
# Note that if any work has been determined as ineligible for upload
# (e.g. due to not being available as a PDF), it will continue to appear
# in this list every time this process is run.
new_ids = list(set(thoth_ids).difference(ia_ids))

# Output this list (as an array of comma-separated, quote-enclosed strings)
print(new_ids)
