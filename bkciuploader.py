#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Clarivate Web of Science Book Citation Index (BKCI)
"""

import logging
import sys
from datetime import date
from ftplib import FTP, error_perm
from io import BytesIO, TextIOWrapper
from errors import DisseminationError
from uploader import Uploader


class BKCIUploader(Uploader):
    """Dissemination logic for Clarivate Web of Science Book Citation Index (BKCI)"""

    def upload_to_platform(self):
        """
        Upload work in required format to Clarivate Web of Science Book Citation Index (BKCI).
        """

        # Check that BKCI credentials have been provided for this publisher
        publisher_id = self.get_publisher_id()
        try:
            user = self.get_variable_from_env('bkci_ftp_user_' + publisher_id.replace('-', '_'), 'BKCI')
            passwd = self.get_variable_from_env('bkci_ftp_pw_' + publisher_id.replace('-', '_'), 'BKCI')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        metadata_csv = self.parse_metadata()
        filename = self.get_isbn('PDF')
        folder_name = date.today().isoformat().replace('-', '')
        pdf = self.get_publication_details('PDF')

        try:
            with FTP(
                host='ftp.isinet.com',
                user=user,
                passwd=passwd,
            ) as ftp:
                try:
                    ftp.cwd('/INCOMING-BOOKS')
                except FileNotFoundError:
                    logging.error(
                        'Could not find folder "INCOMING-BOOKS" on BKCI FTP server')
                    sys.exit(1)
                try:
                    ftp.mkd(folder_name)
                except Exception:
                    logging.error(
                        'Could not create folder "{}" on BKCI FTP server'.format(folder_name))
                    sys.exit(1)
                try:
                    ftp.cwd(folder_name)
                except FileNotFoundError:
                    logging.error(
                        'Could not find folder "{}" on BKCI FTP server'.format(folder_name))
                    sys.exit(1)
                try:
                    ftp.storbinary('STOR {}{}'.format(filename, pdf.file_ext), BytesIO(pdf.bytes))
                except error_perm as error:
                    logging.error(
                        'Error uploading PDF to BKCI FTP server: {}'.format(error))
                    sys.exit(1)
                try:
                    ftp.storbinary('STOR {}.csv'.format(folder_name), metadata_csv)
                except error_perm as error:
                    logging.error(
                        'Error uploading metadata to BKCI FTP server: {}'.format(error))
                    sys.exit(1)
        except error_perm as error:
            logging.error(
                'Could not connect to BKCI FTP server: {}'.format(error))
            sys.exit(1)

        logging.info('Successfully uploaded to BKCI FTP server')

    def parse_metadata(self):
        """Convert work metadata into Clarivate Web of Science Book Citation Index (BKCI) format"""
        title = self.get_title()
        isbn = self.get_isbn('PDF')
        filename = '{}.pdf'.format(isbn)
        pub_date = self.metadata.get('data').get('work').get('publicationDate')
        rows = [
            "Title, ISBN, Publication date, Filename\n",
            "{}, {}, {}, {}\n".format(title, isbn, pub_date, filename)
        ]

        metadata_bytes = BytesIO()
        metadata_text = TextIOWrapper(metadata_bytes)
        metadata_text.writelines(rows)
        metadata_text.detach()
        metadata_bytes.seek(0)

        return metadata_bytes
