#!/usr/bin/env python3
"""
TODO
"""
from thothlibrary import ThothClient, ThothError
from os import environ
import sys

def write_thoth_location(publication_id, location_platform, landing_page, full_text_url):
    thoth = ThothClient()
    username = environ.get('THOTH_EMAIL')
    password = environ.get('THOTH_PWD')
    if username is None:
        raise KeyError('No Thoth username provided (THOTH_EMAIL environment variable not set)')
    if password is None:
        raise KeyError('No Thoth password provided (THOTH_PWD environment variable not set)')
    try:
        self.thoth.login(username, password)
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
    args = sys.argv
    publication_id = args[1]
    location_platform = args[2]
    landing_page = args[3]
    full_text_url = args[4]
    write_thoth_location(publication_id, location_platform, landing_page, full_text_url)
