#!/usr/bin/env python3
"""
Retrieve and upload files and metadata to dedicated Thoth server
for automated content fetching by Google Play crawler
Based on guide at https://support.google.com/books/partner/answer/2763162
"""

import logging
import sys
from datetime import date
from google.cloud import storage
from errors import DisseminationError
from uploader import Uploader


class GooglePlayUploader(Uploader):
    """Dissemination logic for Google Play crawler Thoth server"""

    def upload_to_platform(self):
        """
        Upload work in required format to Google Play crawler Thoth server.

        Content required: EPUB and/or PDF work file plus JPEG cover file (optional)
        Metadata required: Google Books ONIX 3.0 export
        Naming convention: Use eISBN for all content filename roots
                           Metadata filename must include publisher name and unique timestamp
        Upload directory: `ebooks/<collection-code>` for content, `onix/<collection-code>-full` for metadata
                          (where <collection-code> is per-publisher and assigned by Google Play)
        """
        # Check that Google Play bucket name and collection-code have been provided
        publisher_id = self.get_publisher_id()
        try:
            bucket_name = self.get_variable_from_env('google_play_bucket', 'Google Play')
            collection_code = self.get_variable_from_env(
                'google_play_coll_' + publisher_id.replace('-', '_'), 'Google Play')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)
        # TODO credentials handled automatically in local testing - confirm GitHub Actions workflow

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        content_folder = "ebooks"
        metadata_folder = "onix"
        content_files = []
        filename = None

        # Can't continue if neither PDF nor EPUB file is present
        pdf_error = None
        epub_error = None
        try:
            pdf = self.get_publication_details('PDF')
            filename = self.get_isbn('PDF')
            content_files.append(('{}{}'.format(filename, pdf.file_ext), pdf.bytes))
        except DisseminationError as error:
            pdf_error = error
        try:
            epub = self.get_publication_details('EPUB')
            if not filename:
                filename = self.get_isbn('EPUB')
            content_files.append(('{}{}'.format(filename, epub.file_ext), epub.bytes))
        except DisseminationError as error:
            epub_error = error
        if pdf_error and epub_error:
            logging.error(pdf_error)
            logging.error(epub_error)
            sys.exit(1)

        metadata_bytes = self.get_formatted_metadata('onix_3.0::google_books')
        metadata_filename = '{}_{}_{}.xml'.format(
            self.get_publisher_name().replace(' ', ''),
            filename,
            date.today().isoformat().replace('-', '')
        )

        for file in content_files:
            content_blob = bucket.blob('{}/{}/{}'.format(content_folder, collection_code, file[0]))
            try:
                content_blob.upload_from_string(data=file[1], content_type="application/octet-stream")
            except Exception as error:
                logging.error(
                    'Could not connect to Thoth Google Cloud bucket for Google Play crawl: {}'.format(error))
                sys.exit(1)

        metadata_blob = bucket.blob('{}/{}-full/{}'.format(metadata_folder, collection_code, metadata_filename))
        try:
            metadata_blob.upload_from_string(data=metadata_bytes, content_type="application/octet-stream")
        except Exception as error:
            logging.error(
                'Could not connect to Thoth Google Cloud bucket for Google Play crawl: {}'.format(error))
            sys.exit(1)

        logging.info('Successfully uploaded to Thoth Google Cloud bucket for Google Play crawl')

    def parse_metadata(self):
        """Convert work metadata into Google Play format"""
        # Not required for Google Play - only the metadata file is required
        pass
