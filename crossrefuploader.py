#!/usr/bin/env python3
"""
Retrieve and disseminate DOI deposit metadata files to Crossref
Based on guide at https://www.crossref.org/documentation/register-maintain-records/direct-deposit-xml/https-post/
"""

import logging
import sys
import requests
from errors import DisseminationError
from uploader import Uploader


class CrossrefUploader(Uploader):
    """Dissemination logic for Crossref"""

    def upload_to_platform(self):
        """
        Submit work metadata in required format to Crossref.

        Only the Crossref DOI deposit file is required.
        """

        CR_PREFIX_ENDPOINT = 'https://api.crossref.org/prefixes'
        CR_DEPOSIT_ENDPOINT = 'https://doi.crossref.org/servlet/deposit'
        # The deposit API is minimal and will not necessarily return errors if
        # requests are malformed, so check the response text for confirmation
        SUCCESS_MSG = 'Your batch submission was successfully received.'

        # Check that Crossref credentials have been provided for this publisher
        publisher_id = self.get_publisher_id()
        try:
            login_id = self.get_variable_from_env(
                'crossref_user_' + publisher_id.replace('-', '_'), 'Crossref')
            login_passwd = self.get_variable_from_env(
                'crossref_pw_' + publisher_id.replace('-', '_'), 'Crossref')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        metadata_bytes = self.get_formatted_metadata('doideposit::crossref')

        # Check that the provided DOI prefix is a valid Crossref prefix, as
        # this is not checked by Crossref at point of submission
        doi = self.metadata.get('data').get('work').get('doi')
        # DOI must not be None or deposit file request above would have failed
        # (Thoth database guarantees consistent DOI URL format)
        doi_prefix = doi.replace('https://doi.org/', '').split('/')[0]
        doi_rsp = requests.get(
            url='{}/{}'.format(CR_PREFIX_ENDPOINT, doi_prefix),
            # Crossref REST API requests containing a mailto header get preferentially load-balanced
            # (https://www.crossref.org/blog/rebalancing-our-rest-api-traffic/)
            headers={'mailto': 'distribution@thoth.pub'},
        )
        if doi_rsp.status_code != 200:
            logging.error(
                'Not a valid Crossref DOI prefix: {}'.format(doi_prefix)
            )
            sys.exit(1)

        # No specifications for filename given in Crossref guide, and it seems
        # not to impact success/failure of upload. Use work ID for simplicity.
        filename = '{}.xml'.format(self.work_id)

        crossref_rsp = requests.post(
            url=CR_DEPOSIT_ENDPOINT,
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
