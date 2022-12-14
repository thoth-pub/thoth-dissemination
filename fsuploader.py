#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Figshare
"""

import logging
import sys
import json
import requests
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
        # Minimal test with single article and no files
        project_id = api.create_project(project_metadata)
        article_id = api.create_article(article_metadata, project_id)

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
