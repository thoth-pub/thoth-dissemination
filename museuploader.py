#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Project MUSE
"""

import logging
import sys
import pysftp
from io import BytesIO
from errors import DisseminationError
from uploader import Uploader


class MUSEUploader(Uploader):
    """Dissemination logic for Project MUSE"""

    def upload_to_platform(self):
        """
        Upload work in required format to Project MUSE.

        Content required: PDF and/or EPUB work file plus JPG cover file
        Metadata required: Project MUSE ONIX 3.0 export
        Naming convention: Use PDF ISBN for all filename roots
        Upload directory: `uploads`
        """

        # Check that Project MUSE credentials have been provided for this publisher
        publisher_id = self.get_publisher_id()
        try:
            username = self.get_variable_from_env(
                'muse_ftp_user_' + publisher_id.replace('-', '_'), 'Project MUSE')
            password = self.get_variable_from_env(
                'muse_ftp_pw_' + publisher_id.replace('-', '_'), 'Project MUSE')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        filename = self.get_isbn('PDF')
        root_dir = 'uploads'

        metadata_bytes = self.get_formatted_metadata('onix_3.0::project_muse')
        # Only .jpg cover files are supported
        cover_bytes = self.get_cover_image('jpg')
        files = [
            ('{}.xml'.format(filename), BytesIO(metadata_bytes)),
            ('{}.jpg'.format(filename), BytesIO(cover_bytes)),
        ]

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
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            with pysftp.Connection(
                host='ftp.press.jhu.edu',
                username=username,
                password=password,
                cnopts=cnopts,
            ) as sftp:
                try:
                    sftp.cwd(root_dir)
                except FileNotFoundError:
                    logging.error(
                        'Could not find folder "uploads" on Project MUSE SFTP server')
                    sys.exit(1)
                for file in files:
                    try:
                        sftp.putfo(flo=file[1], remotepath=file[0])
                    except TypeError as error:
                        logging.error(
                            'Error uploading to Project MUSE SFTP server: {}'.format(error))
                        # Attempt to delete any partially-uploaded items
                        # However, note that Project MUSE system automatically begins
                        # processing on upload, so this may not help
                        for file in files:
                            try:
                                sftp.remove(file[0])
                            except FileNotFoundError:
                                pass
                        sys.exit(1)
        except pysftp.AuthenticationException as error:
            logging.error(
                'Could not connect to Project MUSE SFTP server: {}'.format(error))
            sys.exit(1)

        logging.info('Successfully uploaded to Project MUSE SFTP server')

    def parse_metadata(self):
        """Convert work metadata into Project MUSE format"""
        # Not required for Project MUSE - only the metadata file is required
        pass
