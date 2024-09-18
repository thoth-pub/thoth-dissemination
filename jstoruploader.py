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

        Content required: PDF work file plus JPG cover file
        Metadata required: JSTOR ONIX 3.0 export
        Naming convention: Use PDF ISBN for all filename roots
        Upload directory: per-publisher folder (named ad-hoc), `books` subfolder
        """

        # Check that JSTOR credentials and publisher folder name have been provided
        publisher_id = self.get_publisher_id()
        try:
            username = self.get_variable_from_env('jstor_ftp_user', 'JSTOR')
            password = self.get_variable_from_env('jstor_ftp_pw', 'JSTOR')
            publisher_dir = self.get_variable_from_env(
                'jstor_ftp_folder' + publisher_id.replace('-', '_'), 'JSTOR')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        filename = self.get_isbn('PDF')
        collection_dir = 'books'

        metadata_bytes = self.get_formatted_metadata('onix_3.0::jstor')
        # Only .jpg cover files are supported
        cover_bytes = self.get_cover_image('jpg')
        pdf = self.get_publication_details('PDF')
        files = [
            ('{}.xml'.format(filename), BytesIO(metadata_bytes)),
            ('{}.jpg'.format(filename), BytesIO(cover_bytes)),
            ('{}{}'.format(filename, pdf.file_ext), pdf.bytes),
        ]

        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            with pysftp.Connection(
                host='ftp.jstor.org',
                username=username,
                password=password,
                port=2222,
                cnopts=cnopts,
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
