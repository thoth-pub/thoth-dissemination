#!/usr/bin/env python3
import csv
import logging
import json
import sys
import requests
from io import StringIO
from thothlibrary import ThothClient, ThothError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

thoth = ThothClient()

MUSE_API_URL = 'https://about.muse.jhu.edu/lib/metadata?no_auth=1&format=kbart&content=book&collection_ids=2652'

file = requests.get(MUSE_API_URL).text

success = True

try:
    data = csv.DictReader(StringIO(file), delimiter="\t")
except (ValueError, NameError):
    raise ValueError('Project MUSE data not found, or not in expected tab-separated string format')

locations = []
for row in data:
    try:
        isbn = str(row['online_identifier']).strip()
        publications = thoth.publications(search=isbn)
        if len(publications) == 0:
            logging.error(f"No publications found for ISBN {isbn}")
            success = False
            continue
        # We may have submitted either PDF or EPUB or both - no way to check
        # Assume that the set of publications remains unchanged since submission
        # and add locations to all relevant publications accordingly
        elif len(publications) > 1:
            pdfs = [n for n in publications if n.publicationType == 'PDF']
            epubs = [n for n in publications if n.publicationType == 'EPUB']
            if not pdfs and not epubs:
                logging.error(f"No PDF or EPUB publications found for {isbn}")
                success = False
                continue
            elif len(pdfs) > 1 or len(epubs) > 1:
                logging.error(f"Multiple publications of same type found for {isbn}")
                success = False
                continue
        landing_page = row['title_url'].strip()
    except KeyError:
        raise ValueError('Project MUSE data missing expected column header')
    except ThothError as e:
        raise ValueError(f'Error connecting to Thoth: {e}')

    for publication in publications:
        if publication.publicationType == 'PDF':
            full_text_url = '{}/pdf/download'.format(landing_page)
        elif publication.publicationType == 'EPUB':
            full_text_url = '{}/epub'.format(landing_page),
        else:
            continue
        try:
            # Check for any existing MUSE location, to detect inconsistencies
            existing_landing_page = [n.landingPage for n in publication.locations
                                     if n.locationPlatform == 'PROJECT_MUSE' and n.landingPage.strip()][0]
            if existing_landing_page != landing_page:
                logging.error(f"Landing page {landing_page} given in data for {isbn}, but record already has " +
                              f"landing page {existing_landing_page}")
            else:
                logging.info(f"Landing page {landing_page} already present in record for {isbn} - skipping")
            continue
        except IndexError:
            pass
        locations.append('{} PROJECT_MUSE {} {}'.format(publication.publicationId, landing_page, full_text_url))

print(json.dumps(locations))

if not success:
    logging.warning("Not all data could be processed. Please review errors logged above.")
    sys.exit(1)
