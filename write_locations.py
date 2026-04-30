#!/usr/bin/env python3
"""
Write one or more sets of publication location information to Thoth.
Input: path to file containing location information, one location per line,
containing publication ID, location platform, landing page and full text URL,
separated by spaces.
Requires: Thoth personal access token as THOTH_PAT env var.
"""

# Third-party package already included in thoth-dissemination/requirements.txt
from thothlibrary import ThothError
from os import environ
import sys
from thothapi import get_thoth_client


def write_thoth_location(publication_id, location_platform, landing_page,
                         full_text_url, checksum):
    thoth = get_thoth_client()
    try:
        token = environ['THOTH_PAT']
    except KeyError as e:
        raise KeyError('No Thoth token provided (THOTH_PAT environment variable not set)') from e

    thoth.set_token(token)

    location = {
        'publicationId': publication_id,
        'landingPage': landing_page,
        'fullTextUrl': full_text_url,
        'locationPlatform': location_platform,
        'canonical': 'false',
        'checksum': checksum
    }
    try:
        location_id = thoth.create_location(location)
    except ThothError as e:
        raise ValueError('Failed to create location in Thoth: token may be incorrect') from e
    print(location_id)


if __name__ == '__main__':
    locations_file = sys.argv[1]
    with open(locations_file, 'r') as locations:
        for location in locations:
            parts = location.rstrip().split(' ')
            try:
                # Handle locations which only have a landing page, no full text URL
                if parts[3] == "None":
                    parts[3] = None
                # Handle locations which don't have a checksum
                if parts[4] == "None":
                    parts[4] = None
                write_thoth_location(parts[0], parts[1], parts[2], parts[3], parts[4])
            except IndexError:
                raise ValueError('Not enough data in entry "{}"'.format(location.rstrip()))
