#!/usr/bin/env python3
"""
Retrieve files and metadata from Thoth for dissemination to any platform
"""

import logging
import sys
import json
import requests
from errors import DisseminationError
from thothlibrary import ThothClient, ThothError


PUB_FORMATS = {
    'PDF': {
        'content_type': 'application/pdf',
        'file_extension': '.pdf',
    },
    # All current Thoth XML publications list a ZIP file for their URL
    # rather than anything in application/xml format
    'XML': {
        'content_type': 'application/zip',
        'file_extension': '.zip',
    },
    # The following have not been fully tested as most current examples
    # in Thoth are paywalled and cannot be accessed by the disseminator
    'EPUB': {
        'content_type': 'application/epub+zip',
        'file_extension': '.epub',
    },
    'AZW3': {
        'content_type': 'application/vnd.amazon.ebook',
        'file_extension': '.azw3',
    },
    'MOBI': {
        'content_type': 'application/x-mobipocket-ebook',
        'file_extension': '.mobi',
    },
    # The following have not been tested as no examples exist in Thoth yet
    'DOCX': {
        'content-type': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'file_extension': '.docx',
    },
    'FICTION_BOOK': {
        'content_type': 'text/xml',
        'file_extension': '.fb2',
    },
    # Not clear how to handle HTML publications: fetching from the Full Text URL
    # is likely to return just the main (TOC) page as the content is under separate
    # links. May also be 'text/html' and '.html' rather than the below.
    # 'HTML': {
    #     'content_type': 'application/xhtml+xml',
    #     'file_extension': '.xhtml',
    # },
}


class Uploader():
    """Generic logic to retrieve and disseminate files and metadata"""

    def __init__(self, work_id, export_url, version):
        """Save argument values and retrieve and store JSON-formatted work metadata"""
        self.work_id = work_id
        self.export_url = export_url
        self.metadata = self.get_thoth_metadata()
        self.version = version

    def run(self):
        """Execute upload logic specific to the selected platform"""
        self.upload_to_platform()

    def get_thoth_metadata(self):
        """Retrieve JSON-formatted work metadata from Thoth GraphQL API via Thoth Client"""
        thoth = ThothClient()
        try:
            metadata_string = thoth.work_by_id(self.work_id, raw=True)
        except ThothError:
            logging.error(
                # Don't include full error text as it's lengthy (contains full query/response)
                'Error retrieving work metadata from GraphQL API: work ID may be incorrect')
            sys.exit(1)

        # Convert string value to JSON value
        try:
            metadata_json = json.loads(metadata_string)
        except json.JSONDecodeError as error:
            logging.error(
                'Error converting GraphQL metadata to JSON: {}'.format(error))
            sys.exit(1)

        return metadata_json

    def get_publication_bytes(self, publication_type):
        """Retrieve canonical work publication from URL specified in work metadata"""
        try:
            # Extract publication URL from Thoth metadata
            publication_url = self.get_publication_url(publication_type)
            # Download publication bytes from publication URL
            return self.get_data_from_url(publication_url, PUB_FORMATS[publication_type]['content_type'])
        except DisseminationError:
            raise

    def get_formatted_metadata(self, format):
        """Retrieve work metadata from Thoth Export API in specified format"""
        metadata_url = self.export_url + '/specifications/' + \
            format + '/work/' + self.work_id
        try:
            return self.get_data_from_url(metadata_url)
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

    def get_cover_image(self):
        """
        Retrieve work cover image from URL specified in work metadata
        (required by some platforms)
        """
        # Extract cover URL from Thoth metadata
        cover_url = self.get_cover_url()

        match cover_url.split('.')[-1].lower():
            case 'jpg':
                expected_format = 'image/jpeg'
            case 'png':
                expected_format = 'image/png'
            case _:
                logging.error(
                    'Format for cover image at URL "{}" is not yet supported'.format(cover_url))
                sys.exit(1)

        try:
            return self.get_data_from_url(cover_url, expected_format)
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

    def get_publication_url(self, publication_type):
        """Extract canonical work publication URL from work metadata"""
        publications = self.metadata.get(
            'data').get('work').get('publications')
        try:
            # There should be a maximum of one publication per type with a maximum of
            # one canonical location; more than one would be a Thoth database error
            publication_locations = [n.get('locations') for n in publications if n.get(
                'publicationType') == publication_type][0]
            publication_url = [n.get('fullTextUrl')
                               for n in publication_locations if n.get('canonical')][0]
            if not publication_url:
                raise ValueError
            return publication_url
        except (IndexError, ValueError):
            raise DisseminationError(
                'No {} Full Text URL found for Work'.format(publication_type))

    def get_cover_url(self):
        """Extract cover URL from work metadata"""
        cover_url = self.metadata.get('data').get('work').get('coverUrl')

        if cover_url is None:
            logging.error('No cover image URL found for Work')
            sys.exit(1)

        return cover_url

    def get_pb_isbn(self):
        """Extract paperback ISBN from work metadata"""
        pb_isbn = None
        publications = self.metadata.get(
            'data').get('work').get('publications')
        for publication in publications:
            # Required ISBN is under paperback publication
            if publication.get('publicationType') == 'PDF':
                if pb_isbn is None:
                    pb_isbn = publication.get('isbn')
                else:
                    logging.error(
                        'Found more than one paperback ISBN - should be unique')
                    sys.exit(1)

        if pb_isbn is None:
            logging.error('No paperback ISBN found for Work')
            sys.exit(1)

        # Remove hyphens from ISBN before returning
        return pb_isbn.replace('-', '')

    def get_publisher_name(self):
        """Extract publisher name from work metadata"""
        publisher = self.metadata.get('data').get('work').get(
            'imprint').get('publisher').get('publisherName')

        return publisher

    @staticmethod
    def get_data_from_url(url, expected_format=None):
        """Download data from specified URL"""

        if expected_format is not None:
            # Attempt a HEAD request to check validity before downloading full data
            # Other request methods follow redirects by default, but we need to
            # set this behaviour explicitly for `head()`
            url_headers = requests.head(url, allow_redirects=True)

            if url_headers.status_code != 200:
                raise DisseminationError('Error retrieving data from "{}": {}'.format(
                    url, url_headers.text))
            elif url_headers.headers.get('Content-Type') != expected_format:
                raise DisseminationError('Data at "{}" is not in format "{}"'.format(
                    url, expected_format))

        url_content = requests.get(url)
        if url_content.status_code == 200:
            # Return downloaded data as bytes
            return url_content.content
        else:
            raise DisseminationError('Error retrieving data from "{}": {}'.format(
                url, url_content.text))
