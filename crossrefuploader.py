#!/usr/bin/env python3
"""
Retrieve and disseminate DOI deposit metadata files to Crossref
Based on guide at https://www.crossref.org/documentation/register-maintain-records/direct-deposit-xml/https-post/
"""

import logging
import requests
from errors import DisseminationError
from uploader import Uploader


CR_PREFIX_ENDPOINT = 'https://api.crossref.org/prefixes'
CR_DEPOSIT_ENDPOINT = 'https://doi.crossref.org/servlet/deposit'

# The deposit API is minimal and will not necessarily return errors if
# requests are malformed, so check the response text for confirmation
SUCCESS_MSG = 'Your batch submission was successfully received.'

# Explicit bounds for every Crossref call. The durable-job worker holds a
# non-renewable lease, so an unbounded provider call is never acceptable.
CR_CONNECT_TIMEOUT_SECONDS = 10
CR_PREFIX_READ_TIMEOUT_SECONDS = 30
CR_DEPOSIT_READ_TIMEOUT_SECONDS = 90

# The only prefix-lookup responses that carry proof. 200 proves the prefix
# exists and 404 proves Crossref does not know it; every other status - a 429
# rate limit, a 5xx outage, a redirect, an authorization problem - reports on
# the lookup service rather than on the prefix, and must never terminalize a
# durable job as though the DOI prefix had been confirmed invalid.
CR_PREFIX_FOUND_STATUS = 200
CR_PREFIX_NOT_FOUND_STATUS = 404


class CrossrefError(DisseminationError):
    """A Crossref dissemination failure classified by the phase it reached.

    These subclass `DisseminationError` so the legacy `disseminator.py` CLI
    keeps exiting non-zero, while the durable-job runner can map each phase to
    a stable classification instead of inferring one from an exit status.
    """


class CrossrefCredentialsMissingError(CrossrefError):
    """No Crossref credentials are configured for this work's publisher."""


class CrossrefMetadataError(CrossrefError):
    """The work lacks the root DOI the Crossref deposit path requires."""


class CrossrefPrefixInvalidError(CrossrefError):
    """Crossref confirmed the work's DOI prefix is invalid or not found."""


class CrossrefPrefixLookupError(CrossrefError):
    """The DOI prefix could not be checked; no deposit was attempted."""


class CrossrefDepositRejectedError(CrossrefError):
    """A completed deposit request returned a known non-acceptance result."""


class CrossrefDepositIndeterminateError(CrossrefError):
    """The deposit may have reached Crossref but acceptance is unproven."""


class CrossrefUploader(Uploader):
    """Dissemination logic for Crossref"""

    def upload_to_platform(self):
        """
        Submit work metadata in required format to Crossref.

        Only the Crossref DOI deposit file is required.
        """

        # Check that Crossref credentials have been provided for this publisher
        publisher_id = self.get_publisher_id()
        try:
            login_id = self.get_variable_from_env(
                'crossref_user_' + publisher_id.replace('-', '_'), 'Crossref')
            login_passwd = self.get_variable_from_env(
                'crossref_pw_' + publisher_id.replace('-', '_'), 'Crossref')
        except DisseminationError as error:
            # Raised rather than exiting from inside Crossref-specific logic,
            # so a long-lived caller is not terminated by a per-work condition.
            raise CrossrefCredentialsMissingError(str(error)) from None

        metadata_bytes = self.get_formatted_metadata('doideposit::crossref')

        # Check that the provided DOI prefix is a valid Crossref prefix, as
        # this is not checked by Crossref at point of submission
        doi = self.metadata.get('data').get('work').get('doi')
        if not isinstance(doi, str) or len(doi.strip()) < 1:
            raise CrossrefMetadataError(
                'No work-level DOI found for Work: cannot derive a Crossref '
                'DOI prefix')
        doi_prefix = doi.replace('https://doi.org/', '').split('/')[0]
        try:
            doi_rsp = requests.get(
                url='{}/{}'.format(CR_PREFIX_ENDPOINT, doi_prefix),
                # Crossref REST API requests containing a mailto header get preferentially load-balanced
                # (https://www.crossref.org/blog/rebalancing-our-rest-api-traffic/)
                headers={'mailto': 'distribution@thoth.pub'},
                timeout=(CR_CONNECT_TIMEOUT_SECONDS,
                         CR_PREFIX_READ_TIMEOUT_SECONDS),
            )
        except requests.exceptions.RequestException as error:
            # Only the exception class is reported: request exception text can
            # quote the effective URL, and no Crossref transport diagnostic is
            # ever copied raw into a log or a durable job detail.
            raise CrossrefPrefixLookupError(
                'Could not check Crossref DOI prefix before deposit: '
                '{}'.format(type(error).__name__)) from None

        if doi_rsp.status_code == CR_PREFIX_NOT_FOUND_STATUS:
            raise CrossrefPrefixInvalidError(
                'Not a valid Crossref DOI prefix: {}'.format(doi_prefix))
        if doi_rsp.status_code != CR_PREFIX_FOUND_STATUS:
            # Only the status code is reported. It happened before the deposit
            # POST, so no provider write has started and the pre-write
            # retryable path is the truthful classification.
            raise CrossrefPrefixLookupError(
                'Crossref prefix lookup did not confirm the prefix before '
                'deposit (status code: {})'.format(doi_rsp.status_code))

        # No specifications for filename given in Crossref guide, and it seems
        # not to impact success/failure of upload. Use work ID for simplicity.
        filename = '{}.xml'.format(self.work_id)

        try:
            crossref_rsp = requests.post(
                url=CR_DEPOSIT_ENDPOINT,
                files={filename: metadata_bytes},
                params={
                    'operation': 'doMDUpload',
                    'login_id': login_id,
                    'login_passwd': login_passwd,
                },
                timeout=(CR_CONNECT_TIMEOUT_SECONDS,
                         CR_DEPOSIT_READ_TIMEOUT_SECONDS),
            )
        except requests.exceptions.RequestException as error:
            # This request carries the credentials as query parameters, so its
            # exception text may contain them and is never reported. The
            # outcome is treated as unproven even when the failure looks like
            # it preceded the write: safety outranks automatic replay.
            raise CrossrefDepositIndeterminateError(
                'Crossref deposit outcome could not be determined: '
                '{}'.format(type(error).__name__)) from None

        if crossref_rsp.status_code != 200 or SUCCESS_MSG not in crossref_rsp.text:
            # The Crossref API does not return succinct error messages so it isn't
            # useful to display the response text; the status code/reason may help
            raise CrossrefDepositRejectedError(
                'Failed to submit DOI file to Crossref database (status code: '
                '{} {})'.format(crossref_rsp.status_code, crossref_rsp.reason))

        # Note that the Crossref API does not do any validity checks during the submission process.
        # Success/failure of deposit is reported separately via an email to the address in the file.
        # At this point we can only report that the file was safely received by Crossref.
        logging.info('Successfully submitted DOI file to Crossref database')

    def parse_metadata(self):
        """Convert work metadata into Crossref format"""
        # Not required for Crossref - only the XML file is required
        pass
