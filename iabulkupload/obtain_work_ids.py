#!/usr/bin/env python3
"""
Acquire a newline-separated list of work IDs to be uploaded to Internet Archive.
Purpose: bulk upload of OBP and punctum's back catalogue.
"""

from thothlibrary import ThothClient

thoth = ThothClient()

# Short names and Thoth IDs of Open Book Publishers and punctum respectively
# publishers = [("obp", "85fd969a-a16c-480b-b641-cb9adf979c3b"), ("punctum", "9c41b13c-cecc-4f6a-a151-be4682915ef5")]
# NOTE passing these in as arguments would be preferable - cf obtain_new_ids.py
publishers = [("example1", "example-ID-1"), ("example2", "example-ID-2")]

for publisher in publishers:
    # `books` query includes Monographs, Edited Books, Textbooks and Journal Issues
    # but excludes Chapters and Book Sets.
    works_to_upload = thoth.books(
        # The default limit is 100; publishers' back catalogues may be bigger than that
        limit='9999',
        # Only upload published works
        work_status='ACTIVE',
        # Start with the earliest, so that the upload is logically ordered
        order='{field: PUBLICATION_DATE, direction: ASC}',
        # Request one publisher's list of works at a time
        publishers='["{}"]'.format(publisher[1]),
    )

    # Extract the Thoth work IDs from the set of results
    work_ids = [n.workId for n in works_to_upload]

    # Turn them into a newline-separated list
    ids_list = '\n'.join(work_ids)

    # Write the list to a file in the current directory named "[publisher]_list.txt"
    # NOTE outputting a stream would be preferable - cf obtain_new_ids.py
    with open('{}_list.txt'.format(publisher[0]), 'w') as output:
        output.write(f'{ids_list}\n')
