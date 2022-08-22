#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Internet Archive
"""

import logging
import sys
from internetarchive import upload
from io import BytesIO
from os import environ
from uploader import Uploader


class IAUploader(Uploader):
    """Dissemination logic for Internet Archive"""

    def upload_to_platform(self):
        """Upload work in required format to Internet Archive"""

        # Identifier and filenames TBD: use paperback ISBN for now
        filename = self.get_pb_isbn()
        # Metadata file format TBD: use CSV for now
        metadata_bytes = self.get_formatted_metadata('csv::thoth')
        pdf_bytes = self.get_pdf_bytes()

        # Convert Thoth work metadata into Internet Archive format
        ia_metadata = self.parse_metadata()

        responses = upload(
            identifier=filename,
            files={
                '{}.pdf'.format(filename): BytesIO(pdf_bytes),
                '{}.csv'.format(filename): BytesIO(metadata_bytes),
            },
            metadata=ia_metadata,
            access_key=environ.get('ia_s3_access'),
            secret_key=environ.get('ia_s3_secret'),
        )

        for response in responses:
            if response.status_code != 200:
                logging.error(
                    'Error uploading to Internet Archive: {}'.format(response.text))
                sys.exit(1)

        logging.info(
            'Successfully uploaded to Internet Archive at archive.org/details/{}'.format(filename))

    def parse_metadata(self):
        """Convert work metadata into Internet Archive format"""
        work_metadata = self.metadata.get('data').get('work')
        ia_metadata = {
            'title': work_metadata.get('title'),
            'publisher': self.get_publisher_name(),
            'creator': work_metadata.get('copyrightHolder'),
            'date': work_metadata.get('publicationDate'),
            'description': work_metadata.get('longAbstract'),
            'imagecount': work_metadata.get('imageCount'),
            'isbn': self.get_pb_isbn(),
            'lccn': work_metadata.get('lccn'),
            'licenseurl': work_metadata.get('license'),
            'mediatype': 'texts',
            'oclc-id': work_metadata.get('oclc'),
        }

        return ia_metadata
