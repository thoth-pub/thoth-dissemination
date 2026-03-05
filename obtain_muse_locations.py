#!/usr/bin/env python3
from dotenv import load_dotenv
import csv
import logging
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
            raise ValueError(f"No publications found for ISBN {isbn}")
        # We may have submitted either PDF or EPUB or both - no way to check
        # Assume that the set of publications remains unchanged since submission
        # and add locations to all relevant publications accordingly
        elif len(publications) > 1:
            pdfs = [n for n in publications if n.publicationType == 'PDF']
            epubs = [n for n in publications if n.publicationType == 'EPUB']
            if not pdfs and not epubs:
                raise ValueError(f"No PDF or EPUB publications found for {isbn}")
            elif len(pdfs) > 1 or len(epubs) > 1:
                raise ValueError(f"Multiple publications of same type found for {isbn}")
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
        location = {
            'publicationId': publication.publicationId,
            'landingPage': landing_page,
            'fullTextUrl': full_text_url,
            'locationPlatform': 'PROJECT_MUSE',
            'canonical': 'false'
        }
        locations.append(location)

print(locations)
