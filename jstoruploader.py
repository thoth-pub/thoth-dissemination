#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to JSTOR
"""

import logging
import sys
import pysftp
from io import BytesIO
from errors import DisseminationError
from uploader import Uploader


class JSTORUploader(Uploader):
    """Dissemination logic for JSTOR"""

    def upload_to_platform(self):
        """
        Upload work in required format to JSTOR.

        Content required: PDF and/or EPUB work file plus JPG cover file
        Metadata required: JSTOR ONIX 3.0 export
        Naming convention: Use PDF ISBN for all filename roots
        Upload directory: per-publisher folder, `books` subfolder
        """

        # Check that JSTOR credentials have been provided for this publisher
        # TODO not yet confirmed whether credentials will be per-publisher
        # or a single Thoth user
        publisher_id = self.get_publisher_id()
        try:
            username = self.get_credential_from_env('jstor_ftp_user', 'JSTOR')
            password = self.get_credential_from_env('jstor_ftp_pw', 'JSTOR')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        publisher = self.get_publisher_name()
        filename = self.get_isbn('PDF')
        publisher_dir = 'TBD' # TODO
        collection_dir = 'books'

        metadata_bytes = self.get_formatted_metadata('onix_3.0::jstor')
        # Only .jpg cover files are supported
        cover_bytes = self.get_cover_image('jpg')
        pdf = self.get_publication_details('PDF').bytes
        files = [
            ('{}.xml'.format(filename), BytesIO(metadata_bytes)),
            ('{}.jpg'.format(filename), BytesIO(cover_bytes)),
            ('{}.pdf'.format(filename), pdf_bytes),
        ]

        try:
            with pysftp.Connection(
                host='ftp.jstor.org',
                username=username,
                password=password,
            ) as sftp:
                try:
                    sftp.cwd(publisher_dir)
                except FileNotFoundError:
                    logging.error(
                        'Could not find publisher folder "{}" on JSTOR SFTP server'.format(publisher_dir))
                    sys.exit(1)
                try:
                    sftp.cwd(collection_dir)
                except FileNotFoundError:
                    logging.error(
                        'Could not find collection folder "books" on JSTOR SFTP server')
                    sys.exit(1)
                for file in files:
                    try:
                        sftp.putfo(flo=file[1], remotepath=file[0])
                    except TypeError as error:
                        logging.error(
                            'Error uploading to JSTOR SFTP server: {}'.format(error))
                        # Attempt to delete any partially-uploaded items
                        # (not confirmed whether JSTOR system automatically begins
                        # processing on upload - cf museuploader)
                        for file in files:
                            try:
                                sftp.remove(file[0])
                            except FileNotFoundError:
                                pass
                        sys.exit(1)
        except pysftp.AuthenticationException as error:
            logging.error(
                'Could not connect to JSTOR SFTP server: {}'.format(error))
            sys.exit(1)

        logging.info('Successfully uploaded to JSTOR SFTP server')

    def parse_metadata(self):
        """Convert work metadata into JSTOR format"""
        # Not required for JSTOR - only the metadata file is required
        pass
