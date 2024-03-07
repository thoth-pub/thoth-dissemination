#!/usr/bin/env python3
"""
Retrieve and disseminate DOI deposit metadata files to Crossref
Based on guide at https://www.crossref.org/documentation/register-maintain-records/direct-deposit-xml/https-post/
"""

import logging
import sys
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
        login_id = self.get_credential_from_env(
            'crossref_user_' + publisher_id.replace('-', '_'), 'Crossref')
        login_passwd = self.get_credential_from_env(
            'crossref_pw_' + publisher_id.replace('-', '_'), 'Crossref')

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

        if crossref_rsp.status_code != 200 or not SUCCESS_MSG in crossref_rsp.text:
            # The Crossref API does not return succinct error messages so it isn't
            # useful to display the response text; the status code/reason may help
            logging.error('Failed to submit DOI file to Crossref database (status code: {} {})'.format(
                crossref_rsp.status_code, crossref_rsp.reason)
            )
            sys.exit(1)

        # Note that the Crossref API does not do any validity checks during the submission process.
        # Success/failure of deposit is reported separately via an email to the address in the file.
        # At this point we can only report that the file was safely received by Crossref.
        logging.info('Successfully submitted DOI file to Crossref database')

    def parse_metadata(self):
        """Convert work metadata into Crossref format"""
        # Not required for Crossref - only the XML file is required
        pass
