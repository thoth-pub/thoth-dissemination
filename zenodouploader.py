#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Zenodo
"""

import logging
import sys
import requests
from io import BytesIO
from errors import DisseminationError
from uploader import Uploader


class ZenodoUploader(Uploader):
    """Dissemination logic for Zenodo"""

    def __init__(self, work_id, export_url, version):
        """Instantiate class for accessing Zenodo API."""
        super().__init__(work_id, export_url, version)
        try:
            api_token = self.get_credential_from_env(
                'zenodo_token', 'Zenodo')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)
        self.api = ZenodoApi(api_token)

    def upload_to_platform(self):
        """Upload work in required format to Zenodo."""

        # Include full work metadata file in JSON format,
        # as a supplement to filling out Zenodo metadata fields.
        metadata_bytes = self.get_formatted_metadata('json::thoth')
        # Can't continue if no PDF file is present.
        # TODO do we want to also upload any other formats?
        try:
            pdf_bytes = self.get_publication_bytes('PDF')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        # Create a deposition to represent the Work.
        zenodo_metadata = self.parse_metadata()
        (deposition_id, api_bucket) = self.api.create_deposition(zenodo_metadata)

        try:
            filename = self.work_id
            self.api.upload_file(
                pdf_bytes, '{}.pdf'.format(filename), api_bucket)
            self.api.upload_file(
                metadata_bytes, '{}.json'.format(filename), api_bucket)
            published_url = self.api.publish_deposition(deposition_id)
        except DisseminationError as error:
            # Report failure, and remove any partially-created items from Zenodo storage.
            logging.error(error)
            self.api.clean_up(deposition_id)
            sys.exit(1)
        except:
            # Unexpected failure. Let program crash, but still need to tidy Zenodo storage.
            self.api.clean_up(deposition_id)
            raise

        logging.info(
            'Successfully uploaded to Zenodo at {}'.format(published_url))

    def parse_metadata(self):
        """Convert work metadata into Zenodo format."""
        work_metadata = self.metadata.get('data').get('work')
        long_abstract = work_metadata.get('longAbstract')
        if long_abstract is None:
            logging.error(
                'Cannot upload to Zenodo: Work must have a Long Abstract')
            sys.exit(1)
        zenodo_metadata = {
            'metadata': {
                # Mandatory fields for publication:
                'title': work_metadata['fullTitle'],  # mandatory in Thoth
                'upload_type': 'publication',
                'publication_type': 'book',  # mandatory when upload_type is publication
                'description': long_abstract,
                'creators': self.get_zenodo_creators(work_metadata),
            }
        }
        return zenodo_metadata

    @staticmethod
    def get_zenodo_creators(metadata):
        """
        Create a list of main contributors in the format required by Zenodo.
        """
        authors = []
        for author in metadata.get('contributions'):
            if author['mainContribution'] is True:
                first_name = author.get('firstName')
                # Zenodo requests author names in `Family name, Given name(s)` format
                # But if we only have full name, supply that as a workaround
                if first_name is not None:
                    name = '{}, {}'.format(author['lastName'], first_name)
                else:
                    name = author['fullName']
                authors.append({'name': name})
        if len(authors) < 1:
            logging.error(
                'Cannot upload to Zenodo: Work must have at least one Main Contribution')
            sys.exit(1)
        return authors


class ZenodoApi:
    """
    Methods for interacting with Zenodo API.
    See documentation at https://developers.zenodo.org/#rest-api.
    """

    # Production instance. Test instance is 'https://sandbox.zenodo.org/api'
    API_ROOT = 'https://zenodo.org/api'

    def __init__(self, api_token):
        """Set up API connection."""
        self.api_token = api_token

    def issue_request(self, method, url, expected_status, data_body=None, json_body=None):
        """
        Issue a request to the API, with optional request body, and handle the response.
        @param expected_status: HTTP status code expected for response.
        @param data_body: Optional request body, as bytes.
        @param json_body: Optional request body, as JSON.
        """
        headers = {'Authorization': 'Bearer ' + self.api_token}
        response = requests.request(
            method, url, headers=headers, data=data_body, json=json_body)
        if response.status_code != expected_status:
            error_message = 'Zenodo API error (HTTP status {})'.format(
                response.status_code)
            raise DisseminationError(error_message)
        return response

    def create_deposition(self, metadata):
        """Create a deposition with the specified metadata."""
        url = '{}/deposit/depositions'.format(self.API_ROOT)
        try:
            response = self.issue_request('POST', url, 201, json_body=metadata)
        except DisseminationError as error:
            logging.error('Creating deposition failed: {}'.format(error))
            sys.exit(1)
        try:
            return (response.json()['id'], response.json()['links']['bucket'])
        # If JSON response body is empty, calling .json() will trigger a JSONDecodeError
        except (KeyError, requests.exceptions.JSONDecodeError):
            logging.error('Creating deposition failed: Zenodo API returned unexpected response')
            sys.exit(1)

    def upload_file(self, file_bytes, file_name, api_bucket):
        """Upload the supplied file under the specified API bucket."""
        url = '{}/{}'.format(api_bucket, file_name)
        try:
            return self.issue_request('PUT', url, 201, data_body=file_bytes)
        except DisseminationError as error:
            raise DisseminationError('Uploading file failed: {}'.format(error))

    def publish_deposition(self, deposition_id):
        """Publish the specified deposition."""
        url = '{}/deposit/depositions/{}/actions/publish'.format(
            self.API_ROOT, deposition_id)
        try:
            response = self.issue_request('POST', url, 202)
        except DisseminationError as error:
            raise DisseminationError(
                'Publishing deposition failed: {}'.format(error))
        try:
            return response.json()['links']['html']
        # If JSON response body is empty, calling .json() will trigger a JSONDecodeError
        except (KeyError, requests.exceptions.JSONDecodeError):
            raise DisseminationError(
                'Publishing deposition failed: Zenodo API returned unexpected response')

    def clean_up(self, deposition_id):
        """
        Remove any items created during the upload process if it fails partway through.

        Deleting a deposition should delete any files under it.
        This will fail with a 403 error if the deposition is already published.
        """
        url = '{}/deposit/depositions/{}'.format(
            self.API_ROOT, deposition_id)
        try:
            self.issue_request('DELETE', url, 204)
        except DisseminationError as error:
            # Can't do anything about this. Calling function will exit.
            logging.error(
                'Failed to delete incomplete deposition {}: {}'.format(deposition_id, error))
