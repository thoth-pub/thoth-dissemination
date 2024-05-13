#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Internet Archive
"""

import logging
import sys
from internetarchive import get_item, upload, exceptions as ia_except
from io import BytesIO
from requests import exceptions as req_except
from errors import DisseminationError
from uploader import Uploader


class IAUploader(Uploader):
    """Dissemination logic for Internet Archive"""

    def upload_to_platform(self):
        """Upload work in required format to Internet Archive"""

        # Fast-fail if credentials for upload are missing
        try:
            access_key = self.get_credential_from_env(
                'ia_s3_access', 'Internet Archive')
            secret_key = self.get_credential_from_env(
                'ia_s3_secret', 'Internet Archive')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        # Use Thoth ID as unique identifier (URL will be in format `archive.org/details/[identifier]`)
        filename = self.work_id

        # Ensure that this identifier is available in the Archive
        # (this will also check that the identifier is in a valid format,
        # although all Thoth IDs should be acceptable)
        if not get_item(filename).identifier_available():
            logging.error(
                'Cannot upload to Internet Archive: an item with this identifier already exists')
            sys.exit(1)

        # Include full work metadata file in JSON format,
        # as a supplement to filling out Internet Archive metadata fields
        metadata_bytes = self.get_formatted_metadata('json::thoth')
        # Can't continue if no PDF file is present
        try:
            pdf_bytes = self.get_publication_bytes('PDF')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        # Convert Thoth work metadata into Internet Archive format
        # (not expected to fail, as "required" metadata is minimal)
        ia_metadata = self.parse_metadata()

        try:
            responses = upload(
                identifier=filename,
                files={
                    '{}.pdf'.format(filename): BytesIO(pdf_bytes),
                    '{}.json'.format(filename): BytesIO(metadata_bytes),
                },
                metadata=ia_metadata,
                access_key=access_key,
                secret_key=secret_key,
                retries=2,
                retries_sleep=30,
                verify=True,
            )
        # Empty access_key and/or secret_key triggers an AuthenticationError.
        # Incorrect access_key and/or secret_key triggers an HTTPError.
        except ia_except.AuthenticationError:
            # The fast-fail above ought to prevent us from hitting this
            logging.error(
                'Error uploading to Internet Archive: credentials missing')
            sys.exit(1)
        except req_except.HTTPError:
            # internetarchive module outputs its own ERROR log before we catch this exception,
            # so no need to repeat the error text. As a future enhancement,
            # we could filter out these third-party logs (along with the INFO logs
            # which are output during the upload process) and update this message.
            logging.error(
                'Error uploading to Internet Archive: credentials may be incorrect')
            sys.exit(1)

        if len(responses) < 1:
            logging.error(
                'Error uploading to Internet Archive: no response received from server')
            sys.exit(1)

        for response in responses:
            if response.status_code != 200:
                logging.error(
                    'Error uploading to Internet Archive: {}'.format(response.text))
                sys.exit(1)

        logging.info(
            'Successfully uploaded to Internet Archive at https://archive.org/details/{}'.format(filename))

    def parse_metadata(self):
        """Convert work metadata into Internet Archive format"""
        work_metadata = self.metadata.get('data').get('work')
        # Repeatable fields such as 'creator', 'isbn', 'subject'
        # can be set by submitting a list of values
        creators = [n.get('fullName')
                    for n in work_metadata.get('contributions')
                    if n.get('mainContribution') is True]
        # IA metadata schema suggests hyphens should be omitted,
        # although including them does not cause any errors
        isbns = [n.get('isbn').replace(
            '-', '') for n in work_metadata.get('publications') if n.get('isbn') is not None]
        # We may want to mark BIC, BISAC, Thema etc subject codes as such;
        # IA doesn't set a standard so representations vary across the archive
        subjects = [n.get('subjectCode')
                    for n in work_metadata.get('subjects')]
        languages = [n.get('languageCode')
                     for n in work_metadata.get('languages')]
        issns = [n.get('series').get(key) for n in work_metadata.get(
            'issues') for key in ['issnPrint', 'issnDigital']]
        # IA only accepts a single volume number
        volume = next(iter([str(n.get('issueOrdinal'))
                      for n in work_metadata.get('issues')]), None)
        ia_metadata = {
            # All fields are non-mandatory
            # Any None values or empty lists are ignored by IA on ingest
            'collection': 'thoth-archiving-network',
            'title': work_metadata.get('fullTitle'),
            'publisher': self.get_publisher_name(),
            'creator': creators,
            # IA requires date in YYYY-MM-DD format, as output by Thoth
            'date': work_metadata.get('publicationDate'),
            'description': work_metadata.get('longAbstract'),
            # Field name is misleading; displayed in IA as 'Pages'
            'imagecount': work_metadata.get('pageCount'),
            'isbn': isbns,
            'lccn': work_metadata.get('lccn'),
            'licenseurl': work_metadata.get('license'),
            'mediatype': 'texts',
            'oclc-id': work_metadata.get('oclc'),
            # IA has no dedicated DOI field but 'source' is
            # "[u]sed to signify where a piece of media originated"
            'source': work_metadata.get('doi'),
            # https://help.archive.org/help/uploading-a-basic-guide/ requests no more than
            # 10 subject tags, but additional tags appear to be accepted without error
            'subject': subjects,
            'language': languages,
            'issn': issns,
            'volume': volume,
            # Custom field: this data should already be included in the formatted
            # metadata file, but including it here may be beneficial for searching
            'thoth-work-id': self.work_id,
            # Custom field helping future users determine what logic was used to create an upload
            'thoth-dissemination-service': self.version,
        }

        return ia_metadata
