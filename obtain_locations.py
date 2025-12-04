#!/usr/bin/env python3
"""
Acquire a list of locations to be added to Thoth (in same format as output by disseminator.py).
Purpose: automate updating of Thoth records for platforms where location is not immediately
         returned as part of initial dissemination process.
Inputs: string list of tuples containing publication ID and DOI.
Currently only supports OAPEN and DOAB locations. Assumes that works lacking OAPEN location
will also lack DOAB location.
"""
import ast
import logging
import json
from time import sleep
import requests
import sys

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(asctime)s: %(message)s')

works_to_search = ast.literal_eval(sys.stdin.read())

locations = []

for (publication_id, doi) in works_to_search:
    oapen_rsp = requests.get(
        url='https://library.oapen.org/rest/search?query=oapen.identifier.doi:%22{}%22' \
            '&expand=metadata,bitstreams'
            .format(doi),
        headers={'Accept': 'application/json'},
    )
    if oapen_rsp.status_code != 200:
        logging.error('OAPEN API request failed for {} (status code {})'.format(doi, oapen_rsp.status_code))
        # Sleep in case the issue is too many requests
        sleep(1)
        continue
    try:
        oapen_rsp_json = json.loads(oapen_rsp.content)
        if len(oapen_rsp_json) > 1:
            logging.error('More than one OAPEN API result found for {}'.format(doi))
            continue
        oapen_result = oapen_rsp_json[0]
        handle = oapen_result['handle']
        file_name = [bitstream['name'] for bitstream in oapen_result['bitstreams']
                     if bitstream['bundleName'] == 'ORIGINAL'][0]
        oapen_landing_page = 'https://library.oapen.org/handle/{}'.format(handle)
        oapen_full_text_url = 'https://library.oapen.org/bitstream/handle/{}/' \
                              '{}?sequence=1&isAllowed=y'.format(handle, file_name)
        logging.info('{} has OAPEN landing page {} and full text URL {}'.format(doi, oapen_landing_page, oapen_full_text_url))
        locations.append('{} OAPEN {} {}'.format(publication_id, oapen_landing_page, oapen_full_text_url))
    except (IndexError, KeyError, json.JSONDecodeError):
        logging.info('No results found in OAPEN for {} - assume not yet processed'.format(doi))

    # Assume that if OAPEN location is missing, DOAB location is too
    doab_rsp = requests.get(
        url='https://directory.doabooks.org/rest/search?query=oapen.identifier.doi:%22{}%22' \
            '&expand=metadata'
            .format(doi),
        headers={'Accept': 'application/json'},
    )
    if doab_rsp.status_code != 200:
        logging.error('DOAB API request failed for {} (status code {})'.format(doi, doab_rsp.status_code))
        # Sleep in case the issue is too many requests
        sleep(1)
        continue
    try:
        doab_rsp_json = json.loads(doab_rsp.content)
        if len(doab_rsp_json) > 1:
            logging.error('More than one DOAB API result found for {}'.format(doi))
            continue
        handle = doab_rsp_json[0]['handle']
        # DOAB only has landing pages, not full text URLs
        doab_landing_page = 'https://directory.doabooks.org/handle/{}'.format(handle)
        logging.info('{} has DOAB landing page {}'.format(doi, doab_landing_page))
        locations.append('{} DOAB {} {}'.format(publication_id, doab_landing_page, None))
    except (IndexError, KeyError, json.JSONDecodeError):
        logging.info('No results found in DOAB for {} - assume not yet processed'.format(doi))

print(json.dumps(locations))
