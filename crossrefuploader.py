#!/usr/bin/env python3
"""
Retrieve and disseminate DOI deposit metadata files to Crossref
Based on guide at https://www.crossref.org/documentation/register-maintain-records/direct-deposit-xml/https-post/
"""

import logging
import sys
from os import environ
import requests
from uploader import Uploader


class CrossrefUploader(Uploader):
    """Dissemination logic for Crossref"""

    def upload_to_platform(self):
        """
        Submit work metadata in required format to Crossref.

        Only the Crossref DOI deposit file is required.
        """

        # Check that Crossref credentials have been provided for this publisher
        publisher_id = self.get_publisher_id()
        login_id = environ.get('crossref_user_' + publisher_id)
        login_passwd = environ.get('crossref_pw_' + publisher_id)

        if login_id is None:
            logging.error('Error uploading to Crossref: no user ID supplied for publisher of this work')
            sys.exit(1)

        if login_passwd is None:
            logging.error('Error uploading to Crossref: no password supplied for publisher of this work')
            sys.exit(1)

        CROSSREF_ENDPOINT = 'https://doi.crossref.org/servlet/deposit'
        # The Crossref API is minimal and will not necessarily return errors if
        # requests are malformed, so check the response text for confirmation
        SUCCESS_MSG = 'Your batch submission was successfully received.'

        metadata_bytes = self.get_formatted_metadata('doideposit::crossref')
        # No specifications for filename given in Crossref guide, and it seems
        # not to impact success/failure of upload. Use work ID for simplicity.
        filename = '{}.xml'.format(self.work_id)

        crossref_rsp = requests.post(
            url=CROSSREF_ENDPOINT,
            files={filename: metadata_bytes},
            params={
                'operation': 'doMDUpload',
                'login_id': login_id,
                'login_passwd': login_passwd,
            },
        )

        if crossref_rsp.status_code != 200:
            logging.error(
                'Failed to deposit file in Crossref database: {}'.format(crossref_rsp.text))
            sys.exit(1)

        if not SUCCESS_MSG in crossref_rsp.text:
            logging.error(
                'Failed to deposit file in Crossref database')
            sys.exit(1)

        logging.info('Successfully deposited file in Crossref database')

    def parse_metadata(self):
        """Convert work metadata into Crossref format"""
        # Not required for Crossref - only the XML file is required
        pass
