#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to ProQuest Ebook Central
"""

import logging
import sys
from datetime import date
from io import BytesIO
from errors import DisseminationError
from sftpclient import SFTPClient, SFTPAuthError
from uploader import Uploader


class ProquestUploader(Uploader):
    """Dissemination logic for ProQuest Ebook Central"""

    def upload_to_platform(self):
        """
        Upload work in required format to ProQuest Ebook Central.

        Content required: PDF and/or EPUB work file plus cover file
        Metadata required: ProQuest Ebook Central ONIX 2.1 export
        Naming convention: Use "associated eISBN" for all content filename roots # TODO check
                           Metadata filename must start with publisher name (no spaces)
                           and include current date in YYYYMMDD format # TODO check
        Upload directory: `upload`
        """

        # Check that ProQuest Ebook Central credentials have been provided
        # TODO not yet confirmed whether credentials will be per-publisher
        # or a single Thoth user
        try:
            username = self.get_variable_from_env('proquest_ftp_user', 'ProQuest Ebook Central')
            password = self.get_variable_from_env('proquest_ftp_pw', 'ProQuest Ebook Central')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        # TODO awaiting confirmation whether different ISBNs should be used
        # for different formats/whether ISBN should be used for metadata file
        # (this will fail if only EPUB exists - see below)
        filename = self.get_isbn('PDF')
        root_dir = 'upload'

        metadata_bytes = self.get_formatted_metadata('onix_2.1::proquest_ebrary')
        # TODO unclear whether there are additional filename requirements
        metadata_filename = '{}_{}_{}.xml'.format(
            self.get_publisher_name().replace(' ', ''),
            filename,
            date.today().isoformat().replace('-', '')
        )

        cover_bytes = self.get_cover_image()
        # No restriction on cover file format
        cover_file_ext = self.get_cover_url().split('.')[-1]

        files = [
            (metadata_filename, BytesIO(metadata_bytes)),
            ('{}.{}'.format(filename, cover_file_ext), BytesIO(cover_bytes)),
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
            with SFTPClient(
                host='ftp.ebookcentral.proquest.com',
                username=username,
                password=password,
            ) as sftp:
                try:
                    sftp.cwd(root_dir)
                except FileNotFoundError:
                    logging.error(
                        'Could not find folder "upload" on ProQuest Ebook Central SFTP server')
                    sys.exit(1)
                for file in files:
                    try:
                        sftp.putfo(flo=file[1], remotepath=file[0])
                    except TypeError as error:
                        logging.error(
                            'Error uploading to ProQuest Ebook Central SFTP server: {}'.format(error))
                        # Attempt to delete any partially-uploaded items
                        # However, note that ProQuest Ebook Central system automatically
                        # begins processing on upload, so this may not help
                        for file in files:
                            try:
                                sftp.remove(file[0])
                            except FileNotFoundError:
                                pass
                        sys.exit(1)
        except SFTPClient as error:
            logging.error(
                'Could not connect to ProQuest Ebook Central SFTP server: {}'.format(error))
            sys.exit(1)

        logging.info('Successfully uploaded to ProQuest Ebook Central SFTP server')

    def parse_metadata(self):
        """Convert work metadata into ProQuest Ebook Central format"""
        # Not required for ProQuest Ebook Central - only the metadata file is required
        pass
