#!/usr/bin/env python3
"""
Retrieve files and metadata from Thoth for dissemination to any platform
"""

import logging
import sys
import json
import requests
from errors import DisseminationError
from os import environ
from thothapi import get_thoth_client
from thothlibrary import ThothError


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
        'content_type': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
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


class Location():
    def __init__(self, publication_id, location_platform, landing_page,
                 full_text_url, checksum):
        self.publication_id = publication_id
        self.location_platform = location_platform
        self.landing_page = landing_page
        self.full_text_url = full_text_url
        self.checksum = checksum

    def __str__(self):
        return "{} {} {} {} {}".format(
            self.publication_id,
            self.location_platform,
            self.landing_page,
            self.full_text_url,
            self.checksum
        )


class Publication():
    def __init__(self, publication_type, publication_id, publication_bytes,
                 file_extension):
        self.type = publication_type
        self.id = publication_id
        self.bytes = publication_bytes
        self.file_ext = file_extension


class Uploader():
    """Generic logic to retrieve and disseminate files and metadata"""

    def __init__(self, work_id, export_url, client_url, version):
        """Save argument values and retrieve and store JSON-formatted work metadata"""
        self.work_id = work_id
        self.export_url = export_url
        self.metadata = self.get_thoth_metadata(client_url)
        self.version = version

    def run(self):
        """Execute upload logic specific to the selected platform"""
        locations = self.upload_to_platform()
        # Not all platforms will return upload location information
        if locations is not None:
            for location in locations:
                print(location)

    def get_thoth_metadata(self, client_url):
        """Retrieve JSON-formatted work metadata from Thoth GraphQL API via Thoth Client"""
        thoth = get_thoth_client(client_url)
        try:
            metadata_string = thoth.work_by_id(
                work_id=self.work_id,
                raw=True
            )
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

        self.normalise_work_metadata(metadata_json)
        return metadata_json

    @staticmethod
    def get_canonical_metadata_entry(entries, entry_type=None):
        """Return the canonical entry from a list of metadata dictionaries."""
        if not isinstance(entries, list):
            return None

        matching_entries = entries
        if entry_type is not None:
            matching_entries = [
                entry for entry in entries
                if entry.get('abstractType') == entry_type
            ]

        if len(matching_entries) < 1:
            return None

        try:
            return [entry for entry in matching_entries if entry.get('canonical')][0]
        except IndexError:
            return matching_entries[0]

    @classmethod
    def normalise_work_metadata(cls, metadata_json):
        """
        Backfill legacy top-level work fields from the newer multilingual
        schema response so existing uploaders can keep reading the same keys.
        """
        try:
            work = metadata_json['data']['work']
        except (KeyError, TypeError):
            return

        canonical_title = cls.get_canonical_metadata_entry(work.get('titles'))
        if canonical_title is not None:
            title = canonical_title.get('title')
            subtitle = canonical_title.get('subtitle')
            full_title = canonical_title.get('fullTitle')

            if full_title is None and title is not None:
                full_title = title if subtitle is None else '{}: {}'.format(
                    title, subtitle)

            work.setdefault('title', title if title is not None else full_title)
            work.setdefault('subtitle', subtitle)
            work.setdefault('fullTitle', full_title)

        long_abstract = cls.get_canonical_metadata_entry(
            work.get('abstracts'),
            entry_type='LONG'
        )
        short_abstract = cls.get_canonical_metadata_entry(
            work.get('abstracts'),
            entry_type='SHORT'
        )
        fallback_abstract = cls.get_canonical_metadata_entry(work.get('abstracts'))

        work.setdefault(
            'shortAbstract',
            None if short_abstract is None else short_abstract.get('content')
        )
        work.setdefault(
            'longAbstract',
            None if long_abstract is None else long_abstract.get('content')
        )

        if work.get('longAbstract') is None and fallback_abstract is not None:
            work['longAbstract'] = fallback_abstract.get('content')

    def get_formatted_metadata(self, format):
        """Retrieve work metadata from Thoth Export API in specified format"""
        metadata_url = self.export_url + '/specifications/' + \
            format + '/work/' + self.work_id
        try:
            return self.get_data_from_url(metadata_url)
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

    def get_cover_image(self, required_format=None):
        """
        Retrieve work cover image from URL specified in work metadata
        (required by some platforms)
        @param required_format: optional string representing file extension of
                                image type desired by caller (e.g. 'jpg', 'png')
        """
        # Extract cover URL from Thoth metadata
        cover_url = self.get_cover_url()
        cover_ext = cover_url.split('.')[-1].lower()

        if cover_ext == 'jpeg':
            cover_ext = 'jpg'

        if required_format and not required_format == cover_ext:
            logging.error('Work cover image has format "{}" instead of "{}"'.format(cover_ext, required_format))
            sys.exit(1)

        match cover_ext:
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

    def get_publication_details(self, publication_type):
        """
        Retrieve publication details for specified type from work metadata:
        Thoth ID, canonical content file (via location URL) and extension
        """
        publications = self.metadata.get(
            'data').get('work').get('publications')
        # There should be a maximum of one publication per type;
        # more than one would be a Thoth database error
        try:
            publication = [n for n in publications
                           if n['publicationType'] == publication_type][0]
        except (IndexError, KeyError):
            raise DisseminationError(
                'No {} publication found for Work'.format(publication_type))
        publication_id = publication.get('publicationId')
        try:
            publication_url = [n['fullTextUrl']
                               for n in publication['locations']
                               if n['canonical']][0]
        except (IndexError, KeyError):
            raise DisseminationError(
                'No {} Full Text URL found for Work'.format(publication_type))
        try:
            publication_bytes = self.get_data_from_url(
                publication_url, PUB_FORMATS[publication_type]['content_type'])
        except DisseminationError:
            raise

        file_extension = PUB_FORMATS[publication_type]['file_extension']

        return Publication(
            publication_type,
            publication_id,
            publication_bytes,
            file_extension
        )

    def get_cover_url(self):
        """Extract cover URL from work metadata"""
        cover_url = self.metadata.get('data').get('work').get('coverUrl')

        if cover_url is None:
            logging.error('No cover image URL found for Work')
            sys.exit(1)

        return cover_url

    def get_isbn(self, publication_type):
        """Extract ISBN of specified type (e.g. 'PAPERBACK') from work metadata"""
        publications = self.metadata.get('data').get('work').get('publications')
        # There should be a maximum of one publication per type;
        # more than one would be a Thoth database error
        try:
            isbn = [n['isbn'] for n in publications
                    if n['publicationType'] == publication_type][0]
            # Remove hyphens from ISBN before returning
            return isbn.replace('-', '')
        except (IndexError, KeyError):
            logging.error('No ISBN of type {} found for Work'.format(publication_type))
            sys.exit(1)

    def get_publisher_name(self):
        """Extract publisher name from work metadata"""
        return self.metadata.get('data').get('work').get(
            'imprint').get('publisher').get('publisherName')

    def get_publisher_id(self):
        """Extract publisher id from work metadata"""
        return self.metadata.get('data').get('work').get(
            'imprint').get('publisher').get('publisherId')

    def get_title(self):
        """Extract work title from work metadata"""
        work = self.metadata.get('data').get('work')
        return work.get('title') or work.get('fullTitle')

    @staticmethod
    def get_data_from_url(url, expected_format=None):
        """Download data from specified URL"""

        if expected_format is not None:
            # Attempt a HEAD request to check validity before downloading full data
            # Other request methods follow redirects by default, but we need to
            # set this behaviour explicitly for `head()`
            try:
                url_headers = requests.head(url, allow_redirects=True, headers={
                    "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36",
                })
            except requests.exceptions.ConnectTimeout:
                raise DisseminationError(
                    'Connection to "{}" timed out: URL may not be valid'
                    .format(url))

            if url_headers.status_code != 200:
                raise DisseminationError('Error retrieving data from "{}": {} (code {})'.format(
                    url, url_headers.text, url_headers.status_code))
            elif url_headers.headers.get('Content-Type') != expected_format:
                raise DisseminationError('Data at "{}" is not in format "{}"'.format(
                    url, expected_format))

        url_content = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36",
        })
        if url_content.status_code == 200:
            # Return downloaded data as bytes
            return url_content.content
        else:
            raise DisseminationError('Error retrieving data from "{}": {} (code {})'.format(
                url, url_content.text, url_content.status_code))

    @staticmethod
    def get_variable_from_env(variable_name, platform_name):
        """Retrieve specified variable from the environment"""

        variable = environ.get(variable_name)

        if variable is None or len(variable) < 1:
            raise DisseminationError(
                'Error uploading to {}: missing value for {}'.format(platform_name, variable_name))
        else:
            return variable
