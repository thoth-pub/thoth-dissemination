#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to OAPEN
"""

import logging
import sys
from ftplib import FTP, error_perm
from io import BytesIO
from os import environ
from uploader import Uploader


class OAPENUploader(Uploader):
    """Dissemination logic for OAPEN"""


    def upload_to_platform(self):
        """
        Upload work in required format to OAPEN.

        Only the OAPEN ONIX file is required, as OAPEN can retrieve
        content files from the links contained within it.

        Separate folders for OAPEN and DOAB: upload one copy to each.
        """

        metadata_bytes = self.get_formatted_metadata('onix_3.0::oapen')
        # Filename TBD: use work ID for now
        filename = self.work_id

        try:
            with FTP(
                host='oapen-ftpserver.org',
                user=environ.get('oapen_ftp_user'),
                passwd=environ.get('oapen_ftp_pw'),
            ) as ftp:
                for dir in ['OAPEN', 'DOAB']:
                    try:
                        ftp.cwd('/{}'.format(dir))
                    except FileNotFoundError:
                        logging.error('Could not find folder "{}" on OAPEN FTP server'.format(dir))
                        sys.exit(1)
                    try:
                        ftp.storbinary('STOR {}.xml'.format(filename), BytesIO(metadata_bytes))
                    except error_perm as error:
                        logging.error('Error uploading to OAPEN FTP server: {}'.format(error))
                        sys.exit(1)
        except error_perm as error:
            logging.error('Could not connect to OAPEN FTP server: {}'.format(error))
            sys.exit(1)

        logging.info('Successfully uploaded to OAPEN FTP server')


    def parse_metadata(self):
        """Convert work metadata into OAPEN format"""
        # Not required for OAPEN - only the ONIX file is required
        pass