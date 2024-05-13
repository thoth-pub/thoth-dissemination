#!/usr/bin/env python3
"""
Write one or more sets of publication location information to Thoth.
Input: path to file containing location information, one location per line,
containing publication ID, location platform, landing page and full text URL,
separated by spaces.
Requires: Thoth credentials as THOTH_EMAIL and THOTH_PWD env vars.
"""

# Third-party package already included in thoth-dissemination/requirements.txt
from thothlibrary import ThothClient, ThothError
from os import environ
import sys


def write_thoth_location(publication_id, location_platform, landing_page,
                         full_text_url):
    thoth = ThothClient()
    username = environ.get('THOTH_EMAIL')
    password = environ.get('THOTH_PWD')
    if username is None:
        raise KeyError('No Thoth username provided (THOTH_EMAIL environment '
                       'variable not set)')
    if password is None:
        raise KeyError('No Thoth password provided (THOTH_PWD environment '
                       'variable not set)')
    try:
        thoth.login(username, password)
    except ThothError:
        raise ValueError('Thoth login failed: credentials may be incorrect')

    location = {
        'publicationId': publication_id,
        'landingPage': landing_page,
        'fullTextUrl': full_text_url,
        'locationPlatform': location_platform,
        'canonical': 'false'
    }
    thoth.create_location(location)


if __name__ == '__main__':
    locations_file = sys.argv[1]
    with open(locations_file, 'r') as locations:
        for location in locations:
            parts = location.rstrip().split(' ')
            try:
                write_thoth_location(parts[0], parts[1], parts[2], parts[3])
            except IndexError:
                raise ValueError('Not enough data in entry "{}"'.location)
