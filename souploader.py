#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to ScienceOpen
"""

import logging
import sys
import zipfile
from datetime import date
from io import BytesIO
from errors import DisseminationError
from paramiko import ssh_exception, AutoAddPolicy, SSHClient
from uploader import Uploader


class SOUploader(Uploader):
    """Dissemination logic for ScienceOpen"""

    def upload_to_platform(self):
        """
        Upload work in required format to ScienceOpen.

        Standard instructions:
        - Create new directory, named with today's date (YYYY-MM-DD), in 'UPLOAD_TO_THIS_DIRECTORY/books'
        - Add one zip file to this directory for each work in this upload batch
        - Zip file should contain metadata and all relevant content files

        Further details determined in discussion between Thoth and ScienceOpen:
        - Create separate directories within upload directory for each publisher
        - Supply work PDF, cover image, and metadata file (format TBD)
        - Can continue existing pattern of using paperback ISBN for all filenames
        - Chapter files and metadata required (full details TBD; not yet implemented)
        """

        # Fast-fail if credentials for upload are missing
        try:
            username = self.get_variable_from_env(
                'so_ftp_user', 'ScienceOpen')
            password = self.get_variable_from_env('so_ftp_pw', 'ScienceOpen')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        publisher = self.get_publisher_name()
        filename = self.get_isbn('PAPERBACK')
        root_dir = 'UPLOAD_TO_THIS_DIRECTORY'
        collection_dir = 'books'
        new_dir = date.today().isoformat()

        # Metadata file format TBD: use CSV for now
        metadata_bytes = self.get_formatted_metadata('csv::thoth')
        cover_bytes = self.get_cover_image()
        # Can't continue if no PDF file is present
        try:
            pdf_bytes = self.get_publication_details('PDF').bytes
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        # Both .jpg and .png cover files are supported
        cover_file_ext = self.get_cover_url().split('.')[-1]

        files = [
            ('{}.csv'.format(filename), metadata_bytes),
            ('{}.{}'.format(filename, cover_file_ext), cover_bytes),
            ('{}.pdf'.format(filename), pdf_bytes),
        ]

        zipped_files = BytesIO()
        with zipfile.ZipFile(file=zipped_files, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            for f in files:
                zf.writestr(zinfo_or_arcname=f[0], data=f[1])
        # Reset buffer position to start of stream so that it can be fully read in upload
        zipped_files.seek(0)

        try:
            client = SSHClient()
            client.set_missing_host_key_policy(AutoAddPolicy)
            client.connect(
                hostname='ftp.scienceopen.com',
                username=username,
                password=password,
            )
            with client.open_sftp() as sftp:
                try:
                    sftp.chdir(root_dir)
                except FileNotFoundError:
                    logging.error(
                        'Could not find folder "UPLOAD_TO_THIS_DIRECTORY" on ScienceOpen SFTP server')
                    sys.exit(1)
                try:
                    sftp.chdir(collection_dir)
                except FileNotFoundError:
                    logging.error(
                        'Could not find collection folder "books" on ScienceOpen SFTP server')
                    sys.exit(1)
                try:
                    sftp.chdir(publisher)
                except FileNotFoundError:
                    logging.error(
                        'Could not find folder for publisher "{}" on ScienceOpen SFTP server'.format(publisher))
                    sys.exit(1)
                sftp.mkdir(new_dir)
                sftp.chdir(new_dir)
                try:
                    sftp.putfo(fl=zipped_files,
                               remotepath='{}.zip'.format(filename))
                except TypeError as error:
                    logging.error(
                        'Error uploading to ScienceOpen SFTP server: {}'.format(error))
                    sys.exit(1)
        except ssh_exception.AuthenticationException as error:
            logging.error(
                'Could not connect to ScienceOpen SFTP server: {}'.format(error))
            sys.exit(1)

        logging.info('Successfully uploaded to ScienceOpen SFTP server')

    def parse_metadata(self):
        """Convert work metadata into ScienceOpen format"""
        # Not required for ScienceOpen - only the metadata file is required
        pass
