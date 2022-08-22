#!/usr/bin/env python3
"""
Retrieve files and metadata from Thoth for dissemination to any platform
"""

import logging
import sys
import json
import requests
from thothlibrary import ThothClient


class Uploader():
    """Generic logic to retrieve and disseminate files and metadata"""


    def __init__(self, work_id, export_url):
        """Save argument values and retrieve and store JSON-formatted work metadata"""
        self.work_id = work_id
        self.export_url = export_url
        self.metadata = self.get_thoth_metadata()


    def run(self):
        """Execute upload logic specific to the selected platform"""
        self.upload_to_platform()


    def get_thoth_metadata(self):
        """Retrieve JSON-formatted work metadata from Thoth GraphQL API via Thoth Client"""
        thoth = ThothClient()
        metadata_string = thoth.work_by_id(self.work_id, raw=True)

        # Convert string value to JSON value
        try:
            metadata_json = json.loads(metadata_string)
        except json.JSONDecodeError as error:
            logging.error('Error converting GraphQL metadata to JSON: {}'.format(error))
            sys.exit(1)

        return metadata_json


    def get_pdf_bytes(self):
        """Retrieve canonical work PDF from URL specified in work metadata"""
        # Extract PDF URL from Thoth metadata
        pdf_url = self.get_pdf_url()
        # Download PDF bytes from PDF URL
        return self.get_data_from_url(pdf_url, 'application/pdf')


    def get_formatted_metadata(self, format):
        """Retrieve work metadata from Thoth Export API in specified format"""
        metadata_url = self.export_url + '/specifications/' + format + '/work/' + self.work_id
        return self.get_data_from_url(metadata_url)


    def get_cover_image(self):
        """
        Retrieve work cover image from URL specified in work metadata
        (required by some platforms)
        """
        # Extract cover URL from Thoth metadata
        cover_url = self.get_cover_url()

        match cover_url.split('.')[-1]:
            case 'jpg':
                expected_format = 'image/jpeg'
            case 'png':
                expected_format = 'image/png'
            case _:
                logging.error('Format for cover image at URL "{}" is not yet supported'.format(cover_url))
                sys.exit(1)

        return self.get_data_from_url(cover_url, expected_format)


    def get_pdf_url(self):
        """Extract canonical work PDF URL from work metadata"""
        # Default value
        pdf_url = None
        publications = self.metadata.get('data').get('work').get('publications')
        for publication in publications:
            # Required URL is under PDF publication
            if publication.get('publicationType') == 'PDF':
                locations = publication.get('locations')
                for location in locations:
                    # Required URL is under canonical location
                    if location.get('canonical') == True:
                        if pdf_url is None:
                            pdf_url = location.get('fullTextUrl')
                        else:
                            logging.error('Found more than one PDF Full Text URL - should be unique')
                            sys.exit(1)

        if pdf_url is None:
            logging.error('No PDF Full Text URL found for Work')
            sys.exit(1)

        return pdf_url


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
        publications = self.metadata.get('data').get('work').get('publications')
        for publication in publications:
            # Required ISBN is under paperback publication
            if publication.get('publicationType') == 'PDF':
                if pb_isbn is None:
                    pb_isbn = publication.get('isbn')
                else:
                    logging.error('Found more than one paperback ISBN - should be unique')
                    sys.exit(1)

        if pb_isbn is None:
            logging.error('No paperback ISBN found for Work')
            sys.exit(1)

        # Remove hyphens from ISBN before returning
        return pb_isbn.replace('-', '')


    def get_publisher_name(self):
        """Extract publisher name from work metadata"""
        publisher = self.metadata.get('data').get('work').get('imprint').get('publisher').get('publisherName')

        return publisher


    @staticmethod
    def get_data_from_url(url, expected_format=None):
        """Download data from specified URL"""

        if expected_format is not None:
            # Attempt a HEAD request to check validity before downloading full data
            url_headers = requests.head(url)

            if url_headers.status_code != 200:
                logging.error('Error retrieving data from "{}": {}'.format(url, url_headers.text))
                sys.exit(1)
            elif url_headers.headers.get('Content-Type') != expected_format:
                logging.error('Data at "{}" is not in format "{}"'.format(url, expected_format))
                sys.exit(1)

        url_content = requests.get(url)
        if url_content.status_code == 200:
            # Return downloaded data as bytes
            return url_content.content
        else:
            logging.error('Error retrieving data from "{}": {}'.format(url, url_content.text))
            sys.exit(1)