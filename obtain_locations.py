#!/usr/bin/env python3
"""
TODO
"""
import ast
import logging
import json
import requests
import sys

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(asctime)s: %(message)s')

works_to_search = ast.literal_eval(sys.stdin.read())

locations = []

for (publication_id, doi) in works_to_search:
    oapen_rsp = requests.get(
        url='https://library.oapen.org/rest/search?query=oapen.identifier.doi:%22{}%22' \
            '&expand=metadata,bitstreams'
            .format(doi),
        headers={'Accept': 'application/json'},
    )
    try:
        oapen_rsp_json = json.loads(oapen_rsp.content)[0]
        handle = oapen_rsp_json.get('handle')
        file_name = [bitstream.get('name') for bitstream in oapen_rsp_json.get('bitstreams')
                     if bitstream.get('bundleName') == 'ORIGINAL'][0]
        oapen_landing_page = 'https://library.oapen.org/handle/{}'.format(handle)
        oapen_full_text_url = 'https://library.oapen.org/bitstream/handle/{}/' \
                              '{}?sequence=1&isAllowed=y'.format(handle, file_name)
        logging.info('{} has OAPEN landing page {} and full text URL {}'.format(doi, oapen_landing_page, oapen_full_text_url))
        locations.append('{} OAPEN {} {}'.format(publication_id, oapen_landing_page, oapen_full_text_url))
    except IndexError:
        logging.info('No results found in OAPEN for {} - assume not yet processed'.format(doi))

    # Assume that if OAPEN location is missing, DOAB location is too
    doab_rsp = requests.get(
        url='https://directory.doabooks.org/rest/search?query=oapen.identifier.doi:%22{}%22' \
            '&expand=metadata'
            .format(doi),
        headers={'Accept': 'application/json'},
    )
    try:
        doab_rsp_json = json.loads(doab_rsp.content)[0]
        handle = doab_rsp_json.get('handle')
        # DOAB only has landing pages, not full text URLs
        doab_landing_page = 'https://directory.doabooks.org/handle/{}'.format(handle)
        logging.info('{} has DOAB landing page {}'.format(doi, doab_landing_page))
        locations.append('{} DOAB {} {}'.format(publication_id, doab_landing_page, None))
    except IndexError:
        logging.info('No results found in DOAB for {} - assume not yet processed'.format(doi))

print(json.dumps(locations))
