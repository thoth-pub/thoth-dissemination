#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to EBSCOHost
"""

import logging
import sys
import pysftp
from io import BytesIO
from errors import DisseminationError
from uploader import Uploader


class EBSCOUploader(Uploader):
    """Dissemination logic for EBSCOHost"""

    def upload_to_platform(self):
        """
        Upload work in required format to EBSCOHost.

        Content required: PDF and/or EPUB work file
        Metadata required: EBSCOHost ONIX 2.1 export
        Naming convention: Use "corresponding eISBN" for all filename roots # TODO check
        Upload directory: TBC # TODO
        """

        # Check that EBSCOHost credentials have been provided
        try:
            username = self.get_credential_from_env('ebsco_ftp_user', 'EBSCOHost')
            password = self.get_credential_from_env('ebsco_ftp_pw', 'EBSCOHost')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        # TODO awaiting confirmation whether different ISBNs should be used
        # for different formats/whether ISBN should be used for metadata file
        # (this will fail if only EPUB exists - see below)
        filename = self.get_isbn('PDF')

        metadata_bytes = self.get_formatted_metadata('onix_2.1::ebsco_host')
        files = [('{}.xml'.format(filename), BytesIO(metadata_bytes))]

        # Can't continue if neither PDF nor EPUB file is present
        pdf_error = None
        epub_error = None
        try:
            pdf = self.get_publication_details('PDF')
            files.append(('{}{}'.format(filename, pdf.file_ext), BytesIO(pdf.bytes)))
        except DisseminationError as error:
            pdf_error = error
        try:
            epub = self.get_publication_details('EPUB')
            files.append(('{}{}'.format(filename, epub.file_ext), BytesIO(epub.bytes)))
        except DisseminationError as error:
            epub_error = error
        if pdf_error and epub_error:
            logging.error(pdf_error)
            logging.error(epub_error)
            sys.exit(1)

        try:
            with pysftp.Connection(
                host='sftp.epnet.com',
                username=username,
                password=password,
            ) as sftp:
                for file in files:
                    try:
                        sftp.putfo(flo=file[1], remotepath=file[0])
                    except TypeError as error:
                        logging.error(
                            'Error uploading to EBSCOHost SFTP server: {}'.format(error))
                        # Attempt to delete any partially-uploaded items
                        # (not confirmed whether EBSCOHost system automatically begins
                        # processing on upload - cf museuploader)
                        for file in files:
                            try:
                                sftp.remove(file[0])
                            except FileNotFoundError:
                                pass
                        sys.exit(1)
        except pysftp.AuthenticationException as error:
            logging.error(
                'Could not connect to EBSCOHost SFTP server: {}'.format(error))
            sys.exit(1)

        logging.info('Successfully uploaded to EBSCOHost SFTP server')

    def parse_metadata(self):
        """Convert work metadata into EBSCOHost format"""
        # Not required for EBSCOHost - only the metadata file is required
        pass
