#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Figshare
"""

import logging
import sys
import json
import requests
import hashlib
from io import BytesIO
from os import environ
from uploader import Uploader


class FigshareUploader(Uploader):
    """Dissemination logic for Figshare"""

    def upload_to_platform(self):
        """Upload work in required format to Figshare"""
        # Required steps:
        # 1) create Project (not Collection) to represent Thoth Work
        # 2) create one Article (within Project) to represent each (digital) Thoth Publication
        # 3) Add relevant files to Articles
        # 4) Publish the Project/Articles (i.e. copy from Private to Public version)
        # Design questions:
        # - Where to upload full formatted (JSON) metadata file? As a separate Article,
        # or duplicated under each Publication-Article?
        # - Which Publications to upload? Just PDF? Just OA versions? Or all?
        # Must correctly replicate manual upload "embargo" logic if uploading paywalled EPUBs/MOBIs etc.
        api = FigshareApi()
        (project_metadata, article_metadata) = self.parse_metadata()

        # Include full work metadata file in JSON format,
        # as a supplement to filling out Figshare metadata fields
        metadata_bytes = self.get_formatted_metadata('json::thoth')
        pdf_bytes = self.get_pdf_bytes()
        # Filename TBD: use work ID for now
        filename = self.work_id

        # Minimal test with single article and no files
        project_id = api.create_project(project_metadata)
        article_id = api.create_article(article_metadata, project_id)

        # Add files
        api.upload_file(pdf_bytes, '{}.pdf'.format(filename), article_id)
        api.upload_file(metadata_bytes, '{}.json'.format(filename), article_id)

        # Placeholder message
        logging.info(
            'Successfully uploaded to Figshare: article id {}'.format(article_id))

    def parse_metadata(self):
        """Convert work metadata into Figshare format"""
        work_metadata = self.metadata.get('data').get('work')
        project_metadata = {
            # Minimal metadata for testing
            # The only other supported fields are description and funding
            'title': work_metadata.get('title'),
        }
        article_metadata = {
            # Minimal metadata for testing
            # Note manual upload testing was done with minimal metadata -
            # can view json representation of manual uploads for pointers.
            'title': work_metadata.get('title'),
            # resource_title = text value for hyperlink to resource_doi
        }

        # TODO check whether article metadata will need to vary depending on publication type
        return (project_metadata, article_metadata)


class FigshareApi:
    """Methods for interacting with Figshare API"""

    # Test instance. Production instance is 'https://api.figshare.com/v2'
    API_ROOT = 'https://api.figsh.com/v2'

    def __init__(self):
        self.api_token = environ.get('figshare_token')

    # TODO commonise these two methods
    def create_project(self, metadata):
        url = '{}/account/projects'.format(self.API_ROOT)
        response = requests.post(url=url, json=metadata, headers={
                                 'Authorization': 'token {}'.format(self.api_token)})
        response_json = json.loads(response.text)
        # print(response_json)
        if response.status_code == 201:
            # Return project id - response is { entity_id, location }
            # TODO better error checking e.g. ensure id field is actually present
            return response_json.get('entity_id')
        else:
            # Retrieve the error message from { code, message } response
            logging.error('Error creating Figshare project: {}'.format(
                response_json.get('message')))
            sys.exit(1)

    def create_article(self, metadata, project_id):
        url = '{}/account/projects/{}/articles'.format(
            self.API_ROOT, project_id)
        response = requests.post(url=url, json=metadata, headers={
                                 'Authorization': 'token {}'.format(self.api_token)})
        response_json = json.loads(response.text)
        # print(response_json)
        if response.status_code == 201:
            # Return article id - response is { entity_id, location, warnings }
            # TODO better error checking e.g. ensure id field is actually present
            return response_json.get('entity_id')
        else:
            # Retrieve the error message from { code, message } response
            logging.error('Error creating Figshare article: {}'.format(
                response_json.get('message')))
            sys.exit(1)

    def upload_file(self, file_bytes, file_name, article_id):
        upload_api = FigshareUploadApi(
            file_bytes, file_name, article_id, self.API_ROOT, self.api_token)
        upload_api.run()


class FigshareUploadApi:
    """Methods for interacting with Figshare upload service API"""

    def __init__(self, file_bytes, file_name, article_id, api_root, api_token):
        self.file_stream = BytesIO(file_bytes)
        self.file_name = file_name
        self.article_id = article_id
        self.api_root = api_root
        self.api_token = api_token

    def issue_request(self, method, url, expected_status, expected_key=None, data_body=None, json_body=None):
        headers = {'Authorization': 'token ' + self.api_token}
        response = requests.request(
            method, url, headers=headers, data=data_body, json=json_body)

        if response.status_code != expected_status:
            logging.error('Error contacting Figshare API (status code {})'.format(
                response.status_code))
            sys.exit(1)

        if expected_key is not None:
            try:
                response_json = json.loads(response.content)
                key_value = response_json[expected_key]
                return key_value
            except ValueError:
                logging.error(
                    'Unexpected response from Figshare API: {}'.format(response.text))
                sys.exit(1)
            except KeyError:
                logging.error(
                    'No data found in Figshare API for requested item {}'.format(expected_key))
                sys.exit(1)

    def construct_file_info(self):
        md5 = hashlib.md5()
        md5.update(self.file_stream.getvalue())
        file_info = {
            'name': self.file_name,
            'md5': md5.hexdigest(),
            'size': len(self.file_stream.getvalue())
        }
        return file_info

    def initiate_new_upload(self):
        url = '{}/account/articles/{}/files'.format(
            self.api_root, self.article_id)
        file_info = self.construct_file_info()
        file_url = self.issue_request(
            'POST', url, 201, 'location', json_body=file_info)
        return file_url

    def get_upload_url(self, file_url):
        upload_url = self.issue_request(
            'GET', file_url, 200, 'upload_url')
        return upload_url

    def upload_part(self, upload_url, part):
        url = '{}/{}'.format(upload_url, part['partNo'])
        self.file_stream.seek(part['startOffset'])
        data = self.file_stream.read(
            part['endOffset'] - part['startOffset'] + 1)
        self.issue_request('PUT', url, 200, data_body=data)

    def upload_data(self, upload_url):
        # Upload service API may require the data to be submitted in multiple parts.
        parts = self.issue_request('GET', upload_url, 200, 'parts')
        for part in parts:
            self.upload_part(upload_url, part)

    def complete_upload(self, file_url):
        self.issue_request('POST', file_url, 202)

    def check_upload_status(self, file_url):
        status = self.issue_request('GET', file_url, 200, 'status')
        # Status may be 'available' if upload has been processed successfully,
        # or 'moving_to_final' if processing is still in progress
        # (likely errors should be caught early in processing).
        if status not in {'moving_to_final', 'available'}:
            logging.info('Error checking uploaded file: status is {}'.status)
            sys.exit(1)

    def run(self):
        # Request a URL (in the form articles/{id}/files/{id}) for a new file upload.
        file_url = self.initiate_new_upload()
        # File data needs to be uploaded to a separate URL at the Figshare upload service API.
        upload_url = self.get_upload_url(file_url)
        self.upload_data(upload_url)
        # Contact main Figshare API again to confirm we've finished uploading data.
        self.complete_upload(file_url)
        # Check that the data was processed successfully.
        self.check_upload_status(file_url)
