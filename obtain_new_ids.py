#!/usr/bin/env python3
"""
Acquire a list of work IDs to be uploaded to Internet Archive.
Purpose: automatic upload at regular intervals of newly-published works from selected publishers.
Based on `iabulkupload/obtain_work_ids.py`.
"""

# Both third-party packages already included in thoth-dissemination/requirements.txt
from internetarchive import search_items
from thothlibrary import errors, ThothClient
import sys

thoth = ThothClient()

# Obtain IDs of publishers whose works should be uploaded from arguments to script
# Test that all are valid and early-exit if not
for publisher in sys.argv[1:]:
    try:
        thoth.publisher(publisher)
    except errors.ThothError:
        # Don't include full error text as it's lengthy (contains full query/response)
        print("No record found for publisher {}: ID may be incorrect".format(publisher))
        sys.exit(1)

publishers = '["' + '", "'.join(sys.argv[1:]) + '"]'

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
