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
from time import sleep
from errors import DisseminationError
from uploader import Uploader, PUB_FORMATS, Location


class FigshareUploader(Uploader):
    """Dissemination logic for Figshare"""

    # Figshare dissemination is currently only to Loughborough instance
    REPO_ROOT = 'https://repository.lboro.ac.uk'
    # Loughborough handle: https://hdl.handle.net/2134
    HANDLE_PREFIX = '2134'

    def __init__(self, work_id, export_url, client_url, version):
        """Instantiate class for accessing Figshare API."""
        super().__init__(work_id, export_url, client_url, version)
        try:
            api_token = self.get_credential_from_env(
                'figshare_token', 'Figshare')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)
        self.api = FigshareApi(api_token)

    def upload_to_platform(self):
        """
        Upload work in required format to Figshare.

        Required steps:
        - Create Project (not Collection) to represent Thoth Work
        - Create one Article (within Project) to represent each (digital, OA) Thoth Publication
        - Add the relevant files (Publication and JSON metadata) to each Article
        - Publish the Articles, then the Project (i.e. copy from Private to Public version)

        Note: all current Thoth XML publications list a ZIP file for their
        Full Text URL, rather than anything in application/xml format.
        We'll upload the ZIP (if any) as-is, rather than extracting and uploading files
        individually - the upload will lack previews, but display structure more readably.
        """

        # Test that no record associated with this Work already exists in Figshare repository.
        search_results = self.api.search_articles(self.work_id)
        if len(search_results) > 0:
            logging.error(
                'Cannot upload to Figshare: an item with this Work ID already exists')
            sys.exit(1)

        # If any required metadata is missing, this step will fail, so do it
        # before attempting large file downloads.
        (project_metadata, article_metadata) = self.parse_metadata()

        # Include full work metadata file in JSON format,
        # as a supplement to filling out Figshare metadata fields.
        metadata_bytes = self.get_formatted_metadata('json::thoth')

        # Include all available publication files. Don't fail if
        # one is missing, but do fail if none are found at all.
        # (Any paywalled publications will not be retrieved.)
        publications = []
        for format in PUB_FORMATS:
            try:
                publication = self.get_publication_details(format)
                publications.append(publication)
            except DisseminationError as error:
                pass
        if len(publications) < 1:
            logging.error(
                'Cannot upload to Figshare: no suitable publication files found')
            sys.exit(1)

        # Create a project to represent the Work.
        project_id = self.api.create_project(project_metadata)

        locations = []
        # Uploads to any Figshare-backed institutional repository create
        # mirrored records both in figshare.com and under the repository's
        # home URL. Repository version is listed as canonical in HTML, so use
        # that and treat it as an institution-specific location i.e. 'OTHER'.
        # In future we may also add the figshare.com version under 'FIGSHARE'.
        location_platform = 'OTHER'

        # Any failure after this point will leave incomplete data in
        # Figshare storage which will need to be removed.
        try:
            filename = self.work_id
            for publication in publications:
                # Create an article to represent the Publication.
                # Append publication type to article title, to tell them apart.
                article_id = self.api.create_article(
                    dict(article_metadata,
                         title='{} ({})'.format(article_metadata['title'],
                                                publication.type)),
                    project_id)
                # Add the publication file and full JSON metadata file to it.
                pub_file_id = self.api.upload_file(
                    publication.bytes,
                    '{}{}'.format(filename, publication.file_ext),
                    article_id)
                self.api.upload_file(
                    metadata_bytes, '{}.json'.format(filename), article_id)
                # Publish the article.
                self.api.publish_article(article_id)
                # We expect Figshare to assign a handle to every article,
                # using a standard pattern of repo-specific prefix plus article ID
                # (prefix doesn't appear to be obtainable from API)
                landing_page = 'https://hdl.handle.net/{}/{}'.format(self.HANDLE_PREFIX, article_id)
                # API only returns figshare.com URLs - construct repo URL
                full_text_url = '{}/ndownloader/files/{}'.format(
                    self.REPO_ROOT, pub_file_id)
                locations.append(Location(publication.id, location_platform,
                                          landing_page, full_text_url))
            # Publish project.
            self.api.publish_project(project_id)
        except DisseminationError as error:
            # Report failure, and remove any partially-created items from Figshare storage.
            logging.error(error)
            self.api.clean_up(project_id)
            sys.exit(1)
        except:
            # Unexpected failure. Let program crash, but still need to tidy Figshare storage.
            self.api.clean_up(project_id)
            raise

        # The public project URL would be more useful than the project ID, but
        # the API doesn't return it as part of the workflow. We could obtain it
        # by calling the project details endpoint and extracting the
        # "figshare_url" (if any).
        logging.info(
            'Successfully uploaded to Figshare: project ID {}'.format(project_id))

        # Return details of created uploads to be entered as Thoth Locations
        return locations

    def parse_metadata(self):
        """Convert work metadata into Figshare format."""
        work_metadata = self.metadata.get('data').get('work')
        long_abstract = work_metadata.get('longAbstract')
        if long_abstract is None:
            logging.error(
                'Cannot upload to Figshare: Work must have a Long Abstract')
            sys.exit(1)
        project_metadata = {
            # Only title is mandatory
            'title': work_metadata['fullTitle'],  # mandatory in Thoth
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
            # Mandatory fields for creation:
            'title': work_metadata['fullTitle'],  # mandatory in Thoth
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
                # (imprint, publisher, publisherName all mandatory in Thoth)
                'resource_title': work_metadata['imprint']['publisher']['publisherName'],
            })
        return (project_metadata, article_metadata)

    @staticmethod
    def get_figshare_type(metadata):
        """
        Convert Thoth Work Type into appropriate Figshare equivalent.

        Options as listed in documentation are:
        figure | online resource | preprint | book | conference contribution
        media | dataset | poster | journal contribution | presentation | thesis | software

        However, options from ArticleSearch item_type full list also seem to be accepted:
        1 - Figure, 2 - Media, 3 - Dataset, 5 - Poster, 6 - Journal contribution, 7 - Presentation,
        8 - Thesis, 9 - Software, 11 - Online resource, 12 - Preprint, 13 - Book, 14 - Conference contribution,
        15 - Chapter, 16 - Peer review, 17 - Educational resource, 18 - Report, 19 - Standard, 20 - Composition,
        21 - Funding, 22 - Physical object, 23 - Data management plan, 24 - Workflow, 25 - Monograph,
        26 - Performance, 27 - Event, 28 - Service, 29 - Model
        """
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
                logging.error(
                    'Unsupported value for workType metadata field: {}'.format(other))
                sys.exit(1)

    def get_figshare_licence(self, metadata):
        """
        Find the Figshare licence object corresponding to the Thoth licence URL.

        Note URLs must match exactly, barring http(s) and www prefixes and final '/' -
        e.g. "creativecommons.org/licenses/by/4.0/legalcode" will not match to "creativecommons.org/licenses/by/4.0/".
        If multiple Figshare licence objects have the same URL, the first in the list will be used.
        """
        thoth_licence_raw = metadata.get('license')
        if thoth_licence_raw is None:
            logging.error(
                'Cannot upload to Figshare: Work must have a Licence')
            sys.exit(1)
        # Obtain the current set of defined licence objects from the Figshare API.
        licence_list = self.api.get_licence_list()
        # Thoth licence field is unchecked free text and Figshare licences format
        # is not strongly policed. When checking for matches, we therefore want to
        # disregard http(s) and www prefixes, and optional final '/'.
        match_pattern = r'^(?:https?://)?(?:www\.)?{}/?$'
        # Retrieve a normalised version of the Thoth licence, without prefixes.
        # We'll re-insert this into the match pattern for comparison with the non-normalised Figshare licences.
        # (IGNORECASE may be redundant here if Thoth licences are lowercased on entry into database)
        try:
            thoth_licence = re.fullmatch(match_pattern.format(
                '(.*)'), thoth_licence_raw, re.IGNORECASE).group(1)
        except AttributeError:
            logging.error(
                'Work Licence {} not in expected URL format'
                .format(thoth_licence_raw))
            sys.exit(1)
        # Figshare requires licence information to be submitted as the integer representing the licence object.
        licence_int = next((fs_licence.get('value') for fs_licence in licence_list
                            if re.fullmatch(match_pattern.format(thoth_licence), fs_licence.get('url'), re.IGNORECASE) is not None), None)
        if licence_int == None:
            logging.error(
                'Work Licence {} not supported by Figshare'
                .format(thoth_licence_raw))
            sys.exit(1)
        return licence_int

    @staticmethod
    def get_figshare_authors(metadata):
        """
        Create a list of main contributors in the format required by Figshare.

        Note they will be displayed as "Authored by:" irrespective of contribution type.
        Figshare also accepts other author details such as ORCIDs, however,
        this can lead to rejected submissions if a record already exists
        within Figshare for the author with that ORCID (Thoth doesn't track this).
        """
        # fullName is mandatory so we do not expect KeyErrors
        authors = [{'name': n['fullName']} for n in metadata.get('contributions')
                   if n.get('mainContribution') is True]
        if len(authors) < 1:
            logging.error(
                'Cannot upload to Figshare: Work must have at least one Main Contribution')
            sys.exit(1)
        return authors

    @staticmethod
    def get_figshare_tags(metadata):
        """Create a list of subject keywords in the format required by Figshare."""
        # subjectCode is mandatory so we do not expect KeyErrors
        tags = [n['subjectCode'] for n in metadata.get
                ('subjects') if n.get('subjectType') == 'KEYWORD']
        if len(tags) < 1:
            logging.error(
                'Cannot upload to Figshare: Work must have at least one Subject of type Keyword')
            sys.exit(1)
        return tags


class FigshareApi:
    """
    Methods for interacting with Figshare API.
    See documentation at https://docs.figshare.com/.
    """

    # Production instance. Test instance is 'https://api.figsh.com/v2'
    API_ROOT = 'https://api.figshare.com/v2'

    def __init__(self, api_token):
        """Connect to API and retrieve account details which will be needed for upload."""
        self.api_token = api_token
        [self.user_id, self.group_id] = self.get_account_details()

    def issue_request(self, method, url, expected_status, expected_keys=None, data_body=None, json_body=None):
        """
        Issue a request to the API, with optional request body, and handle the response.
        @param expected_status: HTTP status code expected for response.
        @param expected_keys: Array of keys expected to be found within JSON response body (if any).
                              If None, no value will be returned.
                              If '[]', full JSON response body (usually an array) will be returned.
                              If array contains only one key, its corresponding string value will be returned.
                              If array contains more than one key, an array of the corresponding values will be returned.
        @param data_body: Optional request body, as bytes.
        @param json_body: Optional request body, as JSON.
        """
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
                # JSON may contain an error message - include it if so
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
                        if len(str(key_value)) < 1:
                            raise ValueError
                        key_values.append(key_value)
                    except (KeyError, ValueError):
                        raise DisseminationError(
                            'No data found in Figshare API for requested item {}'.format(key))
                if len(key_values) == 1:
                    return key_values[0]
                else:
                    return key_values

    def get_account_details(self):
        """Retrieve logged-in user's ID (= author ID) and group (for project creation)."""
        url = '{}/account'.format(self.API_ROOT)
        try:
            return self.issue_request('GET', url, 200, expected_keys=['user_id', 'group_id'])
        except DisseminationError as error:
            logging.error('Getting account details failed: {}'.format(error))
            sys.exit(1)

    def search_articles(self, thoth_work_id):
        """Search the repository for Articles containing the supplied Thoth ID."""
        # Repository needs to be set up with a custom field to hold
        # the work ID in order for us to validly search on it.
        self.check_custom_field_exists('Thoth Work ID')
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

    def check_custom_field_exists(self, field_name):
        """
        Check that the specified custom field is defined for
        the repository group to which the logged-in user belongs.
        """
        url = '{}/account/institution/custom_fields'.format(self.API_ROOT)
        try:
            custom_fields = self.issue_request(
                'GET', url, 200, expected_keys=[])
        except DisseminationError as error:
            logging.error('Getting custom fields failed: {}'.format(error))
            sys.exit(1)
        if next((field for field in custom_fields if field.get('name') == field_name), None) is None:
            logging.error(
                'Cannot upload to Figshare: no {} field found in repository'.format(field_name))
            sys.exit(1)

    def get_licence_list(self):
        """Retrieve the list of licences which are defined within this repository."""
        url = '{}/account/licenses'.format(self.API_ROOT)
        try:
            return self.issue_request('GET', url, 200, expected_keys=[])
        except DisseminationError as error:
            logging.error('Getting licence list failed: {}'.format(error))
            sys.exit(1)

    def create_project(self, metadata):
        """Create a Project with the specified metadata."""
        url = '{}/account/projects'.format(self.API_ROOT)
        try:
            return self.issue_request('POST', url, 201, expected_keys=['entity_id'], json_body=metadata)
        except DisseminationError as error:
            logging.error('Creating project failed: {}'.format(error))
            sys.exit(1)

    def create_article(self, metadata, project_id):
        """Create an Article with the specified metadata, under the specified Project."""
        url = '{}/account/projects/{}/articles'.format(
            self.API_ROOT, project_id)
        try:
            # Documentation states that both 'location' and 'entity_id'
            # should be returned, but only 'location' is (support ticket #437956)
            article_url = self.issue_request('POST', url, 201, expected_keys=[
                                             'location'], json_body=metadata)
        except DisseminationError as error:
            raise DisseminationError(
                'Creating article failed: {} ({})'.format(error, metadata.get('title')))
        # Derive 'entity_id' from 'location'
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
        """Remove the specified Author from the specified Article."""
        url = '{}/account/articles/{}/authors/{}'.format(
            self.API_ROOT, article_id, author_id)
        try:
            self.issue_request('DELETE', url, 204)
        except DisseminationError:
            raise

    def publish_project(self, project_id):
        """Publish the supplied Project."""
        url = '{}/account/projects/{}/publish'.format(
            self.API_ROOT, project_id)
        try:
            self.issue_request('POST', url, 200)
        except DisseminationError as error:
            raise DisseminationError(
                'Publishing project failed: {}'.format(error))

    def publish_article(self, article_id):
        """Publish the supplied Article."""
        url = '{}/account/articles/{}/publish'.format(
            self.API_ROOT, article_id)
        try:
            self.issue_request('POST', url, 201)
        except DisseminationError as error:
            raise DisseminationError(
                'Publishing article {} failed: {}'.format(article_id, error))

    def clean_up(self, project_id):
        """
        Remove any items created during the upload process if it fails partway through.

        Deleting a project should delete any articles/files under it (if under "group" storage).
        This will fail if the project or any of its articles is already published.
        """
        url = '{}/account/projects/{}'.format(self.API_ROOT, project_id)
        try:
            self.issue_request('DELETE', url, 204)
        except DisseminationError as error:
            # Can't do anything about this. Calling function will exit.
            logging.error(
                'Failed to delete incomplete project {}: {}'.format(project_id, error))

    def upload_file(self, file_bytes, file_name, article_id):
        """
        Upload the supplied file under the specified Article.

        This is a multi-stage process involving both the main
        Figshare API, and the separate Figshare upload service API.
        """
        try:
            # Request a URL (in the form articles/{id}/files/{id}) for a new file upload.
            file_url = self.initiate_new_upload(
                article_id, file_bytes, file_name)
            # File data needs to be uploaded to a separate URL at the Figshare upload service API.
            upload_url = self.get_upload_url(file_url)
            self.upload_data(upload_url, file_bytes)
            # Contact main Figshare API again to confirm we've finished uploading data.
            self.complete_upload(file_url)
            # Check that the data was processed successfully.
            return self.check_upload_status(file_url)
        except DisseminationError as error:
            raise DisseminationError('Uploading file failed: {} ({})'.format(error, file_name))

    def initiate_new_upload(self, article_id, file_bytes, file_name):
        """
        Create a new file details object under the specified Article.
        This will include a link out to the Figshare upload service API
        where the file bytes themselves can be uploaded.
        """
        url = '{}/account/articles/{}/files'.format(
            self.API_ROOT, article_id)
        file_info = self.construct_file_info(file_bytes, file_name)
        try:
            return self.issue_request(
                'POST', url, 201, expected_keys=['location'], json_body=file_info)
        except DisseminationError:
            raise

    @staticmethod
    def construct_file_info(file_bytes, file_name):
        """Extract file details and return them in the format required by Figshare."""
        md5 = hashlib.md5()
        md5.update(file_bytes)
        file_info = {
            'name': file_name,
            'md5': md5.hexdigest(),
            'size': len(file_bytes)
        }
        return file_info

    def get_upload_url(self, file_url):
        """
        Retrieve the file details object for the pending upload from the Figshare main API,
        and return the Figshare upload service API URL where the bytes can be uploaded.
        """
        try:
            return self.issue_request(
                'GET', file_url, 200, expected_keys=['upload_url'])
        except DisseminationError:
            raise

    def upload_data(self, upload_url, file_bytes):
        """
        Upload the file to the Figshare upload service API.
        The data may need to be submitted in multiple parts.
        """
        try:
            parts = self.issue_request(
                'GET', upload_url, 200, expected_keys=['parts'])
        except DisseminationError:
            raise
        with BytesIO(file_bytes) as file_stream:
            for part in parts:
                try:
                    self.upload_part(upload_url, file_stream, part)
                except DisseminationError:
                    raise

    def upload_part(self, upload_url, file_stream, part):
        """Upload the specified part of the file."""
        url = '{}/{}'.format(upload_url, part['partNo'])
        file_stream.seek(part['startOffset'])
        data = file_stream.read(
            part['endOffset'] - part['startOffset'] + 1)
        try:
            self.issue_request('PUT', url, 200, data_body=data)
        except DisseminationError:
            raise

    def complete_upload(self, file_url):
        """
        Inform the Figshare main API that we have finished uploading
        the specified file to the Figshare upload service API.

        A success response just means the request was received -
        this will trigger processing of the uploaded data
        (which may itself fail).
        """
        try:
            self.issue_request('POST', file_url, 202)
        except DisseminationError:
            raise

    def check_upload_status(self, file_url):
        """
        Check the status of the uploaded file.

        Possible statuses are not documented, but include:
        - created: API is still awaiting more content, i.e. complete_upload has not succeeded.
        - ic_failure: an error was found in the upload, e.g. MD5 hash mismatch.
        - ic_checking: the upload is still being checked.
        - ic_success: this presumably means checking has been completed.
        - moving_to_final: ditto.
        - available: upload has been successfully finalised.

        Upload checking time appears to vary widely, and it may not be practical
        to keep re-checking until 'available' is reached.
        If status is still 'ic_checking' after three tries, assume (hope!) it will succeed.
        """
        tries = 0
        while True:
            try:
                [status, file_id] = self.issue_request(
                    'GET', file_url, 200, expected_keys=['status', 'id'])
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
                        logging.debug(
                            'Uploaded file still being processed; could not confirm success')
                        break
                case 'created' | 'ic_failure' | _:
                    raise DisseminationError(
                        'Error checking uploaded file: status is {}'.format(status))
        return file_id
