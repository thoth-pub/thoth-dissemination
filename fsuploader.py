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
from time import sleep
from errors import DisseminationError
from uploader import Uploader, PUB_FORMATS


class FigshareUploader(Uploader):
    """Dissemination logic for Figshare"""

    def __init__(self, work_id, export_url, version):
        super().__init__(work_id, export_url, version)
        self.api = FigshareApi()

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

        # Test that no record associated with this work already exists in Figshare repository
        search_results = self.api.search_articles(self.work_id)
        if len(search_results) > 0:
            logging.error(
                'Cannot upload to Figshare: an item with this Work ID already exists')
            sys.exit(1)

        (project_metadata, article_metadata) = self.parse_metadata()

        # Include full work metadata file in JSON format,
        # as a supplement to filling out Figshare metadata fields
        metadata_bytes = self.get_formatted_metadata('json::thoth')

        # Include all available publication files. Don't fail if
        # one is missing, but do fail if none are found at all.
        # (Any paywalled publications will not be retrieved.)
        publications = {}
        for format in PUB_FORMATS:
            try:
                publication_bytes = self.get_publication_bytes(format)
                publications[format] = publication_bytes
            except DisseminationError as error:
                pass
        if len(publications) < 1:
            logging.error('No uploadable publication files found')
            sys.exit(1)

        filename = self.work_id

        # Create a project to represent the Work
        project_id = self.api.create_project(project_metadata)

        # Any failure after this point will leave incomplete data in
        # Figshare storage which will need to be removed.
        try:
            # Create an article to represent each publication,
            # add the publication file and full JSON metadata file to it,
            # then publish it
            # All current Thoth XML publications list a ZIP file for their URL
            # rather than anything in application/xml format
            # Upload as-is rather than extracting and uploading individually -
            # the upload will lack previews, but display structure more readably
            for pub_format, pub_bytes in publications.items():
                # Append publication type to article title, to tell them apart
                article_id = self.api.create_article(dict(article_metadata,
                    title='{} ({})'.format(article_metadata['title'], pub_format)), project_id)
                self.api.upload_file(pub_bytes, '{}{}'.format(
                    filename, PUB_FORMATS[pub_format]['file_extension']), article_id)
                self.api.upload_file(metadata_bytes, '{}.json'.format(filename), article_id)
                self.api.publish_article(article_id)
        except DisseminationError as error:
            # Report failure, and remove any partially-created items from Figshare storage
            logging.error(error)
            self.api.clean_up(project_id)
            sys.exit(1)
        except:
            # Unexpected failure. Let program crash, but still need to tidy Figshare storage.
            self.api.clean_up(project_id)
            raise

        # Publish project
        # Don't do this within the try block for Loughborough repository
        # as it's configured to require review before publishing -
        # articles will therefore be "pending" at this stage so
        # publishing project itself would always fail and trigger cleanup.
        # (TBD whether this will have any effect or if a manual publication step is required)
        try:
            self.api.publish_project(project_id)
        except DisseminationError as error:
            # Assume that publishing has failed due to review requirement.
            # Don't clean up, but warn and continue. TBD how to handle long-term.
            logging.warning(error)

        # The public project URL would be more useful than the project ID, but
        # the API doesn't return it as part of the workflow (and it won't be created
        # until the review stage is completed). We could obtain it by calling
        # the project details endpoint and extracting the "figshare_url" (if any).
        logging.info(
            'Successfully uploaded to Figshare: project ID {}'.format(project_id))

    def parse_metadata(self):
        """Convert work metadata into Figshare format"""
        work_metadata = self.metadata.get('data').get('work')
        try:
            long_abstract = work_metadata.get('longAbstract')
        except KeyError:
            logging.error('Cannot upload to Figshare: work must have a Long Abstract')
            sys.exit(1)
        project_metadata = {
            # Only title is mandatory
            'title': work_metadata['fullTitle'], # mandatory in Thoth
            'description': long_abstract,
            # Must submit a group ID for project to be created under "group" storage
            # rather than "individual" - allows use of group-specific custom fields
            'group_id': self.api.group_id,
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
            'title': work_metadata['fullTitle'], # mandatory in Thoth
            # Mandatory fields for publication:
            'description': long_abstract,
            'defined_type': self.get_figshare_type(work_metadata),
            'license': self.get_figshare_licence(work_metadata),
            'authors': self.get_figshare_authors(work_metadata),
            'tags': self.get_figshare_tags(work_metadata),
            # Required by us for tracking uploads:
            'custom_fields': {
                'Thoth Work ID': self.work_id,
            },
        }
        # Optional fields (must not be None):
        pub_date = work_metadata.get('publicationDate')
        if pub_date is not None:
            article_metadata.update({
                'timeline': {
                    'publisherPublication': pub_date,
                    'firstOnline': pub_date,
                },
            })
        doi = work_metadata.get('doi')
        if doi is not None:
            # Must be DOI only, without URL domain
            # Thoth database guarantees consistent URL format
            doi = doi.replace('https://doi.org/', '')
            article_metadata.update({
                'resource_doi': doi,
                # resource_title = display text for hyperlink to resource_doi
                # Mandatory if resource_doi is supplied; use publisher name as not displayed elsewhere
                'resource_title': work_metadata['imprint']['publisher']['publisherName'], # all mandatory in Thoth
            })
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

    def get_figshare_licence(self, metadata):
        # Find the Figshare licence object corresponding to the Thoth licence URL.
        # Note URLs must match exactly, barring http(s) and www prefixes and final '/' -
        # e.g. "creativecommons.org/licenses/by/4.0/legalcode" will not match to "creativecommons.org/licenses/by/4.0/".
        # If multiple Figshare licence objects have the same URL, the first in the list will be used.
        # If Thoth licence URL field is empty or no Figshare licence exists for it, raise an error.
        thoth_licence_raw = metadata.get('license')
        if thoth_licence_raw is None:
            logging.error('Cannot upload to Figshare: work must have a Licence')
            sys.exit(1)
        # Obtain the current set of available licences from the Figshare API
        licence_list = self.api.get_licence_list()
        # Thoth licence field is unchecked free text and Figshare licences format
        # is not strongly policed. When checking for matches, we therefore want to
        # disregard http(s) and www prefixes, and optional final '/'.
        match_pattern = '^(?:https?://)?(?:www\.)?{}/?$'
        # Retrieve a normalised version of the Thoth licence, without prefixes.
        # We'll re-insert this into the match pattern for comparison with the non-normalised Figshare licences.
        # (IGNORECASE may be redundant here if Thoth licences are lowercased on entry into database)
        try:
            thoth_licence = re.fullmatch(match_pattern.format('(.*)'), thoth_licence_raw, re.IGNORECASE).group(1)
        except AttributeError:
            logging.error('Work licence {} not in expected URL format'.format(thoth_licence))
            sys.exit(1)
        # Figshare requires licence information to be submitted as the integer representing the licence object.
        licence_int = next((fs_licence.get('value') for fs_licence in licence_list
            if re.fullmatch(match_pattern.format(thoth_licence), fs_licence.get('url'), re.IGNORECASE) is not None), None)
        if licence_int == None:
            logging.error('Work licence {} not supported by Figshare'.format(thoth_licence))
            sys.exit(1)
        return licence_int

    @staticmethod
    def get_figshare_authors(metadata):
        # TBD which contributors should be submitted - assume only
        # main contributors, in line with Internet Archive requirements.
        # Note they will be displayed as "Authored by:" irrespective of contribution type.
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
        [self.user_id, self.group_id] = self.get_account_details()

    def get_account_details(self):
        url = '{}/account'.format(self.API_ROOT)
        try:
            return self.issue_request('GET', url, 200, expected_keys=['user_id', 'group_id'])
        except DisseminationError as error:
            logging.error('Getting account details failed: {}'.format(error))
            sys.exit(1)

    def get_licence_list(self):
        url = '{}/account/licenses'.format(self.API_ROOT)
        try:
            return self.issue_request('GET', url, 200, expected_keys=[])
        except DisseminationError as error:
            logging.error('Getting licence list failed: {}'.format(error))
            sys.exit(1)

    def create_project(self, metadata):
        url = '{}/account/projects'.format(self.API_ROOT)
        try:
            return self.issue_request('POST', url, 201, expected_keys=['entity_id'], json_body=metadata)
        except DisseminationError as error:
            logging.error('Creating project failed: {}'.format(error))
            sys.exit(1)

    def create_article(self, metadata, project_id):
        url = '{}/account/projects/{}/articles'.format(
            self.API_ROOT, project_id)
        try:
            article_url = self.issue_request('POST', url, 201, expected_keys=['location'], json_body=metadata)
        except DisseminationError as error:
            raise DisseminationError('Creating article failed: {}'.format(error))
        article_id = article_url.split('/')[-1]
        # Figshare default behaviour (confirmed under support ticket #438719)
        # is to always add the logged-in user as an author. Work around this.
        # If the user hasn't been added, this will return 404 - it would be fine
        # to continue in this case, but it would be unexpected API behaviour.
        try:
            self.remove_article_author(article_id, self.user_id)
        except DisseminationError as error:
            raise DisseminationError(
                'Failed to remove user account from author list: {}'.format(error))
        return article_id

    def remove_article_author(self, article_id, author_id):
        url = '{}/account/articles/{}/authors/{}'.format(
            self.API_ROOT, article_id, author_id)
        try:
            self.issue_request('DELETE', url, 204)
        except DisseminationError:
            raise

    def publish_project(self, project_id):
        url = '{}/account/projects/{}/publish'.format(self.API_ROOT, project_id)
        try:
            self.issue_request('POST', url, 200)
        except DisseminationError as error:
            raise DisseminationError('Publishing project failed: {}'.format(error))

    def publish_article(self, article_id):
        url = '{}/account/articles/{}/publish'.format(self.API_ROOT, article_id)
        try:
            self.issue_request('POST', url, 201)
        except DisseminationError as error:
            raise DisseminationError('Publishing article failed: {}'.format(error))

    def search_articles(self, thoth_work_id):
        # Repository needs to be set up with a custom field to hold
        # the work ID in order for us to validly search on it.
        self.check_for_custom_field('Thoth Work ID')
        # Ideally we would be searching for projects containing the work ID,
        # not articles - however, Figshare project search apparently fails to
        # find results in custom fields (while Figshare article search succeeds)
        url = '{}/account/articles/search'.format(self.API_ROOT)
        query = {
            'search_for': thoth_work_id,
        }
        try:
            return self.issue_request('POST', url, 200, expected_keys=[], json_body=query)
        except DisseminationError as error:
            logging.error('Article search failed: {}'.format(error))
            sys.exit(1)

    def check_for_custom_field(self, field_name):
        url = '{}/account/institution/custom_fields'.format(self.API_ROOT)
        try:
            custom_fields = self.issue_request('GET', url, 200, expected_keys=[])
        except DisseminationError as error:
            logging.error('Getting custom fields failed: {}'.format(error))
            sys.exit(1)
        if next((field for field in custom_fields if field.get('name') == field_name), None) is None:
            logging.error('Cannot upload to Figshare: no {} field found in repository'.format(field_name))
            sys.exit(1)

    def clean_up(self, project_id=None):
        # Deleting a project should delete any articles/files under it (if under "group" storage).
        # This will fail if the project or any of its articles is already published.
        if project_id is not None:
            url = '{}/account/projects/{}'.format(self.API_ROOT, project_id)
            try:
                self.issue_request('DELETE', url, 204)
            except DisseminationError as error:
                # Can't do anything about this. Calling function will exit.
                logging.error('Failed to delete incomplete project {}: {}'.format(project_id, error))

    def issue_request(self, method, url, expected_status, expected_keys=None, data_body=None, json_body=None):
        headers = {'Authorization': 'token ' + self.api_token}
        response = requests.request(
            method, url, headers=headers, data=data_body, json=json_body)

        try:
            # Body is often JSON, whether detailing success or error
            response_json = json.loads(response.content)
        except ValueError:
            if expected_keys is not None:
                # We wanted JSON and didn't get it - something's failed, regardless of status code
                raise DisseminationError(
                    'Figshare API returned unexpected response "{}"'.format(response.text))
            else:
                # We don't need the response - doesn't matter that it isn't JSON
                response_json = None
                pass

        if response.status_code != expected_status:
            error_message = 'Figshare API error (HTTP status {})'.format(
                response.status_code)
            if response_json is not None:
                figshare_message = response_json.get('message')
                if figshare_message is not None:
                    # Error message is occasionally very lengthy; only include first line
                    # (this will always include full message if there are no line breaks)
                    figshare_message_short = figshare_message.splitlines()[0]
                    error_message += ' "{}"'.format(figshare_message_short)
                logging.debug(figshare_message)
            raise DisseminationError(error_message)

        if expected_keys is not None:
            # Some or all of the JSON response is required
            if len(expected_keys) < 1:
                # The full response is required
                return response_json
            else:
                key_values = []
                for key in expected_keys:
                    try:
                        key_value = response_json[key]
                        key_values.append(key_value)
                    except KeyError:
                        raise DisseminationError(
                            'No data found in Figshare API for requested item {}'.format(key))
                if len(key_values) == 1:
                    return key_values[0]
                else:
                    return key_values

    @staticmethod
    def construct_file_info(file_bytes, file_name):
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
        try:
            return self.issue_request(
                'POST', url, 201, expected_keys=['location'], json_body=file_info)
        except DisseminationError:
            raise

    def get_upload_url(self, file_url):
        try:
            return self.issue_request(
                'GET', file_url, 200, expected_keys=['upload_url'])
        except DisseminationError:
            raise

    def upload_part(self, upload_url, file_stream, part):
        url = '{}/{}'.format(upload_url, part['partNo'])
        file_stream.seek(part['startOffset'])
        data = file_stream.read(
            part['endOffset'] - part['startOffset'] + 1)
        try:
            self.issue_request('PUT', url, 200, data_body=data)
        except DisseminationError:
            raise

    def upload_data(self, upload_url, file_bytes):
        # Upload service API may require the data to be submitted in multiple parts.
        try:
            parts = self.issue_request('GET', upload_url, 200, expected_keys=['parts'])
        except DisseminationError:
            raise
        with BytesIO(file_bytes) as file_stream:
            for part in parts:
                try:
                    self.upload_part(upload_url, file_stream, part)
                except DisseminationError:
                    raise

    def complete_upload(self, file_url):
        try:
            self.issue_request('POST', file_url, 202)
        except DisseminationError:
            raise

    def check_upload_status(self, file_url):
        # Possible statuses are not documented, but include:
        # - created: API is still awaiting more content, i.e. complete_upload has not succeeded.
        # - ic_failure: an error was found in the upload, e.g. MD5 hash mismatch.
        # - ic_checking: the upload is still being checked.
        # - ic_success: this presumably means checking has been completed.
        # - moving_to_final: ditto.
        # - available: upload has been successfully finalised.
        # Upload checking time appears to vary widely, and it may not be practical
        # to keep re-checking until 'available' is reached.
        # If status is still 'ic_checking' after three tries, assume (hope!) it will succeed.
        tries = 0
        while True:
            try:
                status = self.issue_request('GET', file_url, 200, expected_keys=['status'])
            except DisseminationError:
                raise
            match status:
                case 'available' | 'moving_to_final' | 'ic_success':
                    break
                case 'ic_checking':
                    tries += 1
                    if tries <= 3:
                        sleep(5)
                    else:
                        logging.debug('Uploaded file still being processed; could not confirm success')
                        break
                case 'created' | 'ic_failure' | _ :
                    raise DisseminationError(
                        'Error checking uploaded file: status is {}'.format(status))

    def upload_file(self, file_bytes, file_name, article_id):
        try:
            # Request a URL (in the form articles/{id}/files/{id}) for a new file upload.
            file_url = self.initiate_new_upload(article_id, file_bytes, file_name)
            # File data needs to be uploaded to a separate URL at the Figshare upload service API.
            upload_url = self.get_upload_url(file_url)
            self.upload_data(upload_url, file_bytes)
            # Contact main Figshare API again to confirm we've finished uploading data.
            self.complete_upload(file_url)
            # Check that the data was processed successfully.
            self.check_upload_status(file_url)
        except DisseminationError as error:
            raise DisseminationError('Uploading file failed: {}'.format(error))
