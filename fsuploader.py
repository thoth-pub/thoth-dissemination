#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Figshare
"""

import logging
import sys
import json
import requests
import hashlib
import re
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

        # Test that no record associated with this work already exists in Figshare repository
        # TODO first check that the custom field containing the Thoth Work ID exists
        search_results = api.search_articles(self.work_id)
        if len(search_results) > 0:
            logging.error(
                'Cannot upload to Figshare: an item with this Work ID already exists')
            sys.exit(1)

        # Obtain the current set of available licences from the Figshare API
        licence_list = api.get_licence_list()

        (project_metadata, article_metadata) = self.parse_metadata(licence_list)

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

        # Publish article and project
        api.publish_article(article_id)
        api.publish_project(project_id)

        # Placeholder message
        logging.info(
            'Successfully uploaded to Figshare: article id {}'.format(article_id))

    def parse_metadata(self, licence_list):
        """Convert work metadata into Figshare format"""
        work_metadata = self.metadata.get('data').get('work')
        try:
            long_abstract = work_metadata.get('longAbstract')
        except KeyError:
            logging.error('Cannot upload to Figshare: work must have a Long Abstract')
            sys.exit(1)
        project_metadata = {
            # Only title is mandatory
            'title': work_metadata['title'], # mandatory in Thoth
            'description': long_abstract,
            # Must submit a group ID for project to be created under "group" storage
            # rather than "individual" - allows use of group-specific custom fields
            # Thoth Archiving Network has group ID 49106 on Loughborough repository
            # (TODO this should be abstracted out e.g. in case we add other Figshare repositories)
            'group_id': 49106,
            # Required by us for tracking uploads:
            'custom_fields': {
                'Thoth Work ID': self.work_id,
            },
            # The only other supported field is funding
        }
        article_metadata = {
            # Note manual upload testing was done with minimal metadata -
            # can view json representation of manual uploads for pointers.
            # Mandatory fields for creation:
            'title': work_metadata['title'], # mandatory in Thoth
            # Mandatory fields for publication:
            'description': long_abstract,
            'defined_type': self.get_figshare_type(work_metadata),
            'license': self.get_figshare_licence(work_metadata, licence_list),
            'authors': self.get_figshare_authors(work_metadata),
            'tags': self.get_figshare_tags(work_metadata),
            # Required by us for tracking uploads:
            'custom_fields': {
                'Thoth Work ID': self.work_id,
            },
            # Optional fields:
            # resource_title = text value for hyperlink to resource_doi
        }

        # TODO check whether article metadata will need to vary depending on publication type
        return (project_metadata, article_metadata)

    @staticmethod
    def get_figshare_type(metadata):
        # Options as listed in documentation are:
        # figure | online resource | preprint | book | conference contribution
        # media | dataset | poster | journal contribution | presentation | thesis | software
        # However, options from ArticleSearch item_type full list also seem to be accepted:
        # 1 - Figure, 2 - Media, 3 - Dataset, 5 - Poster, 6 - Journal contribution, 7 - Presentation,
        # 8 - Thesis, 9 - Software, 11 - Online resource, 12 - Preprint, 13 - Book, 14 - Conference contribution,
        # 15 - Chapter, 16 - Peer review, 17 - Educational resource, 18 - Report, 19 - Standard, 20 - Composition,
        # 21 - Funding, 22 - Physical object, 23 - Data management plan, 24 - Workflow, 25 - Monograph,
        # 26 - Performance, 27 - Event, 28 - Service, 29 - Model
        match metadata.get('workType'):
            case 'MONOGRAPH':
                return 'monograph'
            case 'TEXTBOOK':
                return 'educational resource'
            case 'BOOK_CHAPTER':
                return 'chapter'
            case 'EDITED_BOOK' | 'BOOK_SET' | 'JOURNAL_ISSUE':
                return 'book'
            case other:
                logging.error('Unsupported value for workType metadata field: {}'.format(other))
                sys.exit(1)

    @staticmethod
    def get_figshare_licence(metadata, licence_list):
        # Find the Figshare licence object corresponding to the Thoth licence URL.
        # Note URLs must match exactly, barring http(s) and www prefixes -
        # e.g. "creativecommons.org/licenses/by/4.0/legalcode" will not match to "creativecommons.org/licenses/by/4.0/".
        # If multiple Figshare licence objects have the same URL, the lowest numbered will be used.
        # If Thoth licence URL field is empty or no Figshare licence exists for it, raise an error.
        thoth_licence = metadata.get('license')
        if thoth_licence is None:
            logging.error('Cannot upload to Figshare: work must have a Licence')
            sys.exit(1)
        # Thoth licence field is unchecked free text - try to ensure it matches
        # Figshare licence options by normalising to remove http(s) and www prefixes.
        # (IGNORECASE may be redundant if Thoth licences are lowercased on entry into database)
        regex = re.compile('^(?:https?://)?(?:www\\.)?', re.IGNORECASE)
        thoth_licence = regex.sub('', thoth_licence)
        # Figshare requires licence information to be submitted as the integer representing the licence object.
        licence_int = None
        for fs_licence in licence_list:
            if thoth_licence == fs_licence.get('url'):
                licence_int = fs_licence.get('value')
                break
        if licence_int == None:
            logging.error('Licence {} not supported by Figshare'.format(thoth_licence))
            sys.exit(1)
        return licence_int

    @staticmethod
    def get_figshare_authors(metadata):
        # TBD which contributors should be submitted - assume only
        # main contributors, in line with Internet Archive requirements.
        # Figshare also accepts other author details such as ORCIDs, however,
        # this can lead to rejected submissions if a record already exists
        # within Figshare for the author with that ORCID (Thoth doesn't track this).
        # fullName is mandatory so we do not expect KeyErrors
        authors = [{'name': n['fullName']} for n in metadata.get('contributions')
            if n.get('mainContribution') == True]
        if len(authors) < 1:
            logging.error('Cannot upload to Figshare: work must have at least one Main Contribution')
            sys.exit(1)
        return authors

    @staticmethod
    def get_figshare_tags(metadata):
        # subjectCode is mandatory so we do not expect KeyErrors
        tags = [n['subjectCode'] for n in metadata.get
            ('subjects') if n.get('subjectType') == 'KEYWORD']
        if len(tags) < 1:
            logging.error('Cannot upload to Figshare: work must have at least one Subject of type Keyword')
            sys.exit(1)
        return tags


class FigshareApi:
    """Methods for interacting with Figshare API"""

    # Test instance. Production instance is 'https://api.figshare.com/v2'
    API_ROOT = 'https://api.figsh.com/v2'

    def __init__(self):
        self.api_token = environ.get('figshare_token')

    def get_licence_list(self):
        url = '{}/account/licenses'.format(self.API_ROOT)
        # We need the whole response from issue_request, not just a specific JSON key value
        # - TODO this could be handled better to avoid repeating the json.loads() call etc
        licence_list_bytes = self.issue_request('GET', url, 200)
        try:
            licence_list = json.loads(licence_list_bytes)
        except ValueError:
            logging.error(
                'Could not read licence list from Figshare API - invalid JSON')
            sys.exit(1)
        # Figshare licences format is not strongly policed -
        # normalise them all to remove http(s) and www prefixes
        regex = re.compile('^(?:https?://)?(?:www\\.)?', re.IGNORECASE)
        for licence in licence_list:
            try:
                licence.update(url=regex.sub('', licence['url']))
            except KeyError:
                logging.error(
                    'No URL found in licence info from Figshare API')
                sys.exit(1)
        return licence_list

    def create_project(self, metadata):
        url = '{}/account/projects'.format(self.API_ROOT)
        project_id = self.issue_request('POST', url, 201, 'entity_id', json_body=metadata)
        return project_id

    def create_article(self, metadata, project_id):
        url = '{}/account/projects/{}/articles'.format(
            self.API_ROOT, project_id)
        article_url = self.issue_request('POST', url, 201, 'location', json_body=metadata)
        article_id = article_url.split('/')[-1]
        return article_id

    def publish_project(self, project_id):
        url = '{}/account/projects/{}/publish'.format(self.API_ROOT, project_id)
        self.issue_request('POST', url, 200)

    def publish_article(self, article_id):
        url = '{}/account/articles/{}/publish'.format(self.API_ROOT, article_id)
        self.issue_request('POST', url, 201)

    def search_articles(self, thoth_work_id):
        # Ideally we would be searching for projects containing the work ID,
        # not articles - however, Figshare project search apparently fails to
        # find results in custom fields (while Figshare article search succeeds)
        url = '{}/account/articles/search'.format(self.API_ROOT)
        query = {
            'search_for': thoth_work_id,
        }
        # TODO same issue with handling response as in get_licence_list
        results = self.issue_request('POST', url, 200, json_body=query)
        try:
            results_array = json.loads(results)
        except ValueError:
            logging.error(
                'Could not read search response from Figshare API - invalid JSON')
            sys.exit(1)
        return results_array

    def issue_request(self, method, url, expected_status, expected_key=None, data_body=None, json_body=None):
        headers = {'Authorization': 'token ' + self.api_token}
        response = requests.request(
            method, url, headers=headers, data=data_body, json=json_body)

        if response.status_code != expected_status:
            # TODO this isn't enough information
            # Error responses sometimes include { code, message } json
            # but the message can be unwieldy and the code is not user-friendly
            # Print calling function, and/or add more INFO messages during process?
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

        # If no expected key was specified, return the whole response
        # (this is to accommodate get_licence_list requirements - TODO improve?)
        return response.content

    def construct_file_info(self, file_bytes, file_name):
        md5 = hashlib.md5()
        md5.update(file_bytes)
        file_info = {
            'name': file_name,
            'md5': md5.hexdigest(),
            'size': len(file_bytes)
        }
        return file_info

    def initiate_new_upload(self, article_id, file_bytes, file_name):
        url = '{}/account/articles/{}/files'.format(
            self.API_ROOT, article_id)
        file_info = self.construct_file_info(file_bytes, file_name)
        file_url = self.issue_request(
            'POST', url, 201, 'location', json_body=file_info)
        return file_url

    def get_upload_url(self, file_url):
        upload_url = self.issue_request(
            'GET', file_url, 200, 'upload_url')
        return upload_url

    def upload_part(self, upload_url, file_stream, part):
        url = '{}/{}'.format(upload_url, part['partNo'])
        file_stream.seek(part['startOffset'])
        data = file_stream.read(
            part['endOffset'] - part['startOffset'] + 1)
        self.issue_request('PUT', url, 200, data_body=data)

    def upload_data(self, upload_url, file_bytes):
        # Upload service API may require the data to be submitted in multiple parts.
        parts = self.issue_request('GET', upload_url, 200, 'parts')
        file_stream = BytesIO(file_bytes)
        for part in parts:
            self.upload_part(upload_url, file_stream, part)

    def complete_upload(self, file_url):
        self.issue_request('POST', file_url, 202)

    def check_upload_status(self, file_url):
        status = self.issue_request('GET', file_url, 200, 'status')
        # Status may be 'available' if upload has been processed successfully,
        # or 'moving_to_final' if processing is still in progress
        # (likely errors should be caught early in processing).
        if status not in {'moving_to_final', 'available'}:
            logging.info('Error checking uploaded file: status is {}'.format(status))
            sys.exit(1)

    def upload_file(self, file_bytes, file_name, article_id):
        # Request a URL (in the form articles/{id}/files/{id}) for a new file upload.
        file_url = self.initiate_new_upload(article_id, file_bytes, file_name)
        # File data needs to be uploaded to a separate URL at the Figshare upload service API.
        upload_url = self.get_upload_url(file_url)
        self.upload_data(upload_url, file_bytes)
        # Contact main Figshare API again to confirm we've finished uploading data.
        self.complete_upload(file_url)
        # Check that the data was processed successfully.
        self.check_upload_status(file_url)