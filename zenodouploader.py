#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Zenodo
"""

import logging
import sys
import re
import requests
from io import BytesIO
from errors import DisseminationError
from uploader import Uploader


class ZenodoUploader(Uploader):
    """Dissemination logic for Zenodo"""

    def __init__(self, work_id, export_url, client_url, version):
        """Instantiate class for accessing Zenodo API."""
        super().__init__(work_id, export_url, client_url, version)
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
        doi = work_metadata.get('doi')
        if doi is None:
            logging.error(
                'Cannot upload to Zenodo: Work must have a DOI')
            sys.exit(1)
        zenodo_metadata = {
            'metadata': {
                # Mandatory fields which will prevent publication if not set explicitly:
                'title': work_metadata['fullTitle'],  # mandatory in Thoth
                'upload_type': 'publication',
                'publication_type': 'book',  # mandatory when upload_type is publication
                'description': long_abstract,
                'creators': self.get_zenodo_creators(work_metadata),
                # Mandatory fields which will be defaulted if not set explicitly:
                # Zenodo requires date in YYYY-MM-DD format, as output by Thoth
                'date': work_metadata.get('publicationDate'),
                'access_right': 'open',
                'license': self.get_zenodo_licence(work_metadata),  # mandatory when access_right is open
                # Optional fields:
                # If own DOI is not supplied, Zenodo will register one
                'doi': doi,
                'prereserve_doi': False,
                # Will be safely ignored if empty
                'keywords': [n['subjectCode'] for n in work_metadata.get
                             ('subjects') if n.get('subjectType') == 'KEYWORD'],
                # Will be safely ignored if empty
                'related_identifiers': self.get_zenodo_relations(work_metadata),
                # Will be safely ignored if empty
                'references': [n['unstructuredCitation'] for n in work_metadata.get
                               ('references') if n.get('unstructuredCitation') is not None],
                'communities': [{'identifier': 'thoth'}],
                'imprint_publisher': self.get_publisher_name(),
                # Will be safely ignored if None
                'imprint_isbn': next((n.get('isbn') for n in work_metadata.get('publications')
                     if n.get('isbn') is not None and n['publicationType'] == 'PDF'), None),
                # Requested in format `city, country` but seems not to be checked
                'imprint_place': work_metadata.get('place'),
                'notes': 'thoth-work-id:{}'.format(self.work_id)
            }
        }

        # Only one language can be supplied, and must not be None
        language = next((n['languageCode'] for n in work_metadata.get('languages')), None)
        if language is not None:
            zenodo_metadata['metadata'].update({'language': language.lower()})

        return zenodo_metadata

    def get_zenodo_licence(self, metadata):
        """
        Find the Zenodo licence string corresponding to the Thoth licence URL.
        """
        thoth_licence_raw = metadata.get('license')
        if thoth_licence_raw is None:
            logging.error(
                'Cannot upload to Zenodo: Work must have a Licence')
            sys.exit(1)
        # Thoth licence field is unchecked free text. Retrieve a normalised version
        # of the Thoth licence, without http(s) or www prefixes, optional final '/',
        # or the `deed`/`legalcode` suffixes sometimes given with CC licences.
        # (IGNORECASE may be redundant here if Thoth licences are lowercased on entry into database)
        try:
            thoth_licence = re.fullmatch(
                '^(?:https?://)?(?:www\.)?(.*?)/?(?:(?:deed|legalcode)(?:\.[a-zA-Z]{2})?)?$',
                thoth_licence_raw, re.IGNORECASE).group(1)
        except AttributeError:
            logging.error(
                'Work Licence {} not in expected URL format'.format(thoth_licence_raw))
            sys.exit(1)
        zenodo_licence = self.api.search_licences(thoth_licence)
        if zenodo_licence is None:
            logging.error(
                'Work Licence {} not supported by Zenodo'.format(thoth_licence_raw))
            sys.exit(1)
        return zenodo_licence

    @staticmethod
    def get_zenodo_creators(metadata):
        """
        Create a list of main contributors in the format required by Zenodo.
        """
        zenodo_creators = []
        for contribution in [n for n in metadata.get('contributions')
                             if n['mainContribution'] is True]:
            first_name = contribution.get('firstName')
            # Zenodo requests author names in `Family name, Given name(s)` format
            # But if we only have full name, supply that as a workaround
            if first_name is not None:
                name = '{}, {}'.format(contribution['lastName'], first_name)
            else:
                name = contribution['fullName']
            # OK to submit in URL format - Zenodo will convert to ID-only format
            # (will also validate ORCID and prevent publication if invalid)
            # Will be safely ignored if None
            orcid = contribution.get('contributor').get('orcid')
            affiliations = contribution.get('affiliations')
            # Will be safely ignored if None
            first_institution = next((a.get('institution').get(
                'institutionName') for a in affiliations if affiliations), None)
            zenodo_creators.append({
                'name': name,
                'orcid': orcid,
                'affiliation': first_institution})
        if len(zenodo_creators) < 1:
            logging.error(
                'Cannot upload to Zenodo: Work must have at least one Main Contribution')
            sys.exit(1)
        return zenodo_creators

    def get_zenodo_relations(self, metadata):
        """
        Create a list of work relations in the format required by Zenodo.
        Relations must have a standard identifier (e.g. ISBN, DOI).
        Can be used to represent alternative format ISBNs, references,
        Thoth work relations (e.g. child, parent), and series.
        """
        zenodo_relations = []

        for isbn in [n.get('isbn') for n in metadata.get('publications')
                     if n.get('isbn') is not None and n['publicationType'] != 'PDF']:
            zenodo_relations.append({
                'relation': 'isVariantFormOf',
                'identifier': isbn,
                # Resource type is optional but can be guaranteed here
                'resource_type': 'publication-book',
                # Scheme will be auto-detected if not submitted
                'scheme': 'isbn'})

        for reference in [n['doi'] for n in metadata.get('references')
                          if n.get('doi') is not None]:
            zenodo_relations.append({
                'relation': 'cites',
                'identifier': reference,
                'scheme': 'doi'})

        for (relation_type, relation_doi) in [(n.get('relationType'), n.get(
                'relatedWork').get('doi')) for n in metadata.get('relations')
                if n.get('relatedWork').get('doi') is not None]:
            resource_type = 'publication-book'
            if relation_type == 'HAS_PART' or relation_type == 'HAS_CHILD':
                zenodo_type = 'hasPart'
                # `section` in API displays as "Book chapter" in UI
                resource_type = 'publication-section'
            elif relation_type == 'IS_PART_OF' or relation_type == 'IS_CHILD_OF':
                zenodo_type = 'isPartOf'
            elif relation_type == 'HAS_TRANSLATION':
                zenodo_type = 'isSourceOf'
            elif relation_type == 'IS_TRANSLATION_OF':
                zenodo_type = 'isDerivedFrom'
            elif relation_type == 'REPLACES':
                zenodo_type = 'obsoletes'
            elif relation_type == 'IS_REPLACED_BY':
                zenodo_type = 'isObsoletedBy'
            else:
                raise NotImplementedError
            zenodo_relations.append({
                'relation': zenodo_type,
                'identifier': relation_doi,
                'resource_type': resource_type,
                'scheme': 'doi'})

        for issn in [n.get('series').get('issnPrint') for n in metadata.get('issues')
                     if n.get('series').get('issnPrint') is not None]:
            zenodo_relations.append({
                'relation': 'isPartOf',
                'identifier': issn,
                # No appropriate resource type for book series
                'scheme': 'issn'})

        for issn in [n.get('series').get('issnDigital') for n in metadata.get('issues')
                     if n.get('series').get('issnDigital') is not None]:
            zenodo_relations.append({
                'relation': 'isPartOf',
                'identifier': issn,
                # No appropriate resource type for book series
                'scheme': 'eissn'})

        # Only one "alternate identifier" per scheme is permitted
        zenodo_relations.append({
            'relation': 'isAlternateIdentifier',
            'identifier': 'urn:uuid:{}'.format(self.work_id),
            # Resource type is ignored for type `isAlternateIdentifier``
            'scheme': 'urn'})

        landing_page = metadata.get('landingPage')
        if landing_page is not None:
            zenodo_relations.append({
                'relation': 'isAlternateIdentifier',
                'identifier': landing_page,
                # Resource type is ignored for type `isAlternateIdentifier``
                'scheme': 'url'})

        return(zenodo_relations)

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

    def issue_request(self, method, url, expected_status, data_body=None,
                      json_body=None, return_json=False):
        """
        Issue a request to the API, with optional request body, and handle the response.
        @param expected_status: HTTP status code expected for response.
        @param data_body: Optional request body, as bytes.
        @param json_body: Optional request body, as JSON.
        @param return_json: True if caller expects JSON in the response and wants it returned.
        """
        headers = {'Authorization': 'Bearer ' + self.api_token}
        response = requests.request(
            method, url, headers=headers, data=data_body, json=json_body)
        if response.status_code != expected_status:
            error_message = 'Zenodo API error {}'.format(
                response.status_code)
            try:
                json = response.json()
                try:
                    # Per-error messages are the most useful, but only provide
                    # the first one so as not to overload the user
                    error_message += ' - {}'.format(json['errors'][0]['messages'][0])
                except (KeyError, IndexError):
                    # Fall back to main message if no per-error messages
                    error_message += ' - {}'.format(json['message'])
            except (requests.exceptions.JSONDecodeError, KeyError):
                # If JSON response body is empty, calling .json() will trigger a JSONDecodeError -
                # this just means no additional error details are available
                pass
            raise DisseminationError(error_message)

        if return_json:
            try:
                return response.json()
            # If JSON response body is empty, calling .json() will trigger a JSONDecodeError
            except requests.exceptions.JSONDecodeError:
                raise DisseminationError('Zenodo API returned unexpected response')

    def search_licences(self, licence_url):
        """
        Search the Zenodo licences endpoint for ones matching the specified URL.
        @param licence_url: normalised licence URL, without prefixes/suffixes.
        """
        url = '{}/licenses/?q="{}"'.format(self.API_ROOT, licence_url)
        try:
            response = self.issue_request('GET', url, 200, return_json=True)
        except DisseminationError as error:
            logging.error('Searching for licence failed: {}'.format(error))
            sys.exit(1)
        try:
            hits = response['hits']
            if hits['total'] == 1:
                licence_id = hits['hits'][0]['id']
            else:
                # If there are multiple matches, it might be because the specified
                # URL also appears as a substring of other licence URLs (e.g.
                # CC `by/3.0/` will also match `by/3.0/us/`). Zenodo lists CC URLs
                # in their `https://[...]/legalcode` format, so see if any of the
                # matches has the exact URL we're looking for (in this format).
                licence_id = next((n['id'] for n in hits['hits']
                    if n['props']['url'] == 'https://{}/legalcode'.format(licence_url)),
                    None)
            return licence_id
        except KeyError:
            logging.error('Searching for licence failed: Zenodo API returned unexpected response')
            sys.exit(1)

    def create_deposition(self, metadata):
        """Create a deposition with the specified metadata."""
        url = '{}/deposit/depositions'.format(self.API_ROOT)
        try:
            response = self.issue_request('POST', url, 201, json_body=metadata,
                                          return_json=True)
        except DisseminationError as error:
            logging.error('Creating deposition failed: {}'.format(error))
            sys.exit(1)
        try:
            return (response['id'], response['links']['bucket'])
        except KeyError:
            logging.error('Creating deposition failed: Zenodo API returned unexpected response')
            sys.exit(1)

    def upload_file(self, file_bytes, file_name, api_bucket):
        """Upload the supplied file under the specified API bucket."""
        url = '{}/{}'.format(api_bucket, file_name)
        try:
            self.issue_request('PUT', url, 201, data_body=file_bytes)
        except DisseminationError as error:
            raise DisseminationError('Uploading file failed: {}'.format(error))

    def publish_deposition(self, deposition_id):
        """Publish the specified deposition."""
        url = '{}/deposit/depositions/{}/actions/publish'.format(
            self.API_ROOT, deposition_id)
        try:
            response = self.issue_request('POST', url, 202, return_json=True)
        except DisseminationError as error:
            raise DisseminationError(
                'Publishing deposition failed: {}'.format(error))
        try:
            return response['links']['html']
        except KeyError:
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
