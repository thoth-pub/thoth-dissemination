#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Internet Archive
"""

import hashlib
import logging
from io import BytesIO
from time import sleep

from internetarchive import get_item, upload, exceptions as ia_except
from requests import exceptions as req_except

from errors import (
    DisseminationError,
    InternetArchiveIdentifierCollisionError,
    InternetArchiveVerificationError,
)
from uploader import Uploader, Location


class IAUploader(Uploader):
    """Dissemination logic for Internet Archive."""

    THOTH_COLLECTION = 'thoth-archiving-network'
    VERIFICATION_ATTEMPTS = 10
    VERIFICATION_SLEEP_SECONDS = 20
    REPEATABLE_METADATA_FIELDS = {
        'collection', 'creator', 'isbn', 'subject', 'language', 'issn'
    }
    MANAGED_METADATA_FIELDS = {
        'collection',
        'title',
        'publisher',
        'creator',
        'date',
        'description',
        'imagecount',
        'isbn',
        'lccn',
        'licenseurl',
        'mediatype',
        'oclc-id',
        'source',
        'subject',
        'language',
        'issn',
        'volume',
        'thoth-work-id',
        'thoth-dissemination-service',
    }

    def upload_to_platform(self):
        """Create or idempotently update a work in Internet Archive."""
        access_key = self.get_variable_from_env(
            'ia_s3_access', 'Internet Archive')
        secret_key = self.get_variable_from_env(
            'ia_s3_secret', 'Internet Archive')

        identifier = self.work_id
        try:
            item = get_item(identifier)
        except req_except.RequestException as error:
            raise DisseminationError(
                'Error inspecting Internet Archive item {}: {}'.format(
                    identifier, error)) from error

        item_exists = item.exists
        if item_exists:
            self._assert_item_owned_by_thoth(item)

        metadata_bytes = self.get_formatted_metadata('json::thoth')
        publication = self.get_publication_details('PDF')
        pdf_bytes = publication.bytes
        ia_metadata = self.parse_metadata()
        absent_metadata_fields = (
            self.MANAGED_METADATA_FIELDS - ia_metadata.keys())

        file_bytes = {
            '{}.pdf'.format(identifier): pdf_bytes,
            '{}.json'.format(identifier): metadata_bytes,
        }
        expected_md5s = {
            name: hashlib.md5(contents).hexdigest()
            for name, contents in file_bytes.items()
        }
        files_to_upload = self._files_requiring_upload(
            item, file_bytes, expected_md5s)

        metadata_patch = {}
        if item_exists:
            metadata_patch = self._managed_metadata_patch(
                item.metadata, ia_metadata)

        if files_to_upload:
            self._upload_files(
                identifier,
                files_to_upload,
                ia_metadata if not item_exists else None,
                access_key,
                secret_key,
            )

        if item_exists and metadata_patch:
            self._modify_metadata(
                item, metadata_patch, access_key, secret_key)

        verified_md5s = self._verify_final_state(
            item,
            expected_md5s,
            ia_metadata,
            absent_metadata_fields,
        )
        landing_page = 'https://archive.org/details/{}'.format(identifier)
        full_text_url = 'https://archive.org/download/{}/{}.pdf'.format(
            identifier, identifier)

        logging.info(
            'Successfully verified Internet Archive item at {}'.format(
                landing_page))

        return [Location(
            publication.id,
            'INTERNET_ARCHIVE',
            landing_page,
            full_text_url,
            verified_md5s['{}.pdf'.format(identifier)],
            'MD5',
        )]

    def _assert_item_owned_by_thoth(self, item):
        """Raise when an existing identifier cannot safely be linked to Thoth."""
        marker_values = self._as_metadata_list(
            item.metadata.get('thoth-work-id'))
        if marker_values:
            if all(value == self.work_id for value in marker_values):
                return
            raise InternetArchiveIdentifierCollisionError(
                'Internet Archive identifier collision for {}: existing '
                'thoth-work-id metadata is {!r}; refusing to modify the item'
                .format(self.work_id, marker_values))

        collections = self._as_metadata_list(
            item.metadata.get('collection'))
        if (item.identifier == self.work_id
                and self.THOTH_COLLECTION in collections):
            logging.warning(
                'Internet Archive item %s is a legacy Thoth collection item '
                'without thoth-work-id metadata; adding the ownership marker',
                self.work_id)
            return

        raise InternetArchiveIdentifierCollisionError(
            'Internet Archive identifier collision for {}: the existing item '
            'has no matching thoth-work-id metadata and is not an identifiable '
            'legacy member of the {} collection; refusing to modify it'.format(
                self.work_id, self.THOTH_COLLECTION))

    @classmethod
    def _files_requiring_upload(cls, item, file_bytes, expected_md5s):
        originals = cls._original_files(item.files)
        return {
            name: BytesIO(contents)
            for name, contents in file_bytes.items()
            if name not in originals
            or cls._file_value(originals[name], 'md5') != expected_md5s[name]
        }

    @staticmethod
    def _file_value(file_metadata, key):
        if isinstance(file_metadata, dict):
            return file_metadata.get(key)
        return getattr(file_metadata, key, None)

    @classmethod
    def _original_files(cls, files):
        originals = {}
        for file_metadata in files or []:
            if cls._file_value(file_metadata, 'source') != 'original':
                continue
            name = cls._file_value(file_metadata, 'name')
            if name is not None:
                originals[name] = file_metadata
        return originals

    def _upload_files(self, identifier, files, metadata, access_key, secret_key):
        try:
            responses = upload(
                identifier=identifier,
                files=files,
                metadata=metadata,
                access_key=access_key,
                secret_key=secret_key,
                queue_derive=True,
                retries=2,
                retries_sleep=30,
                verify=True,
                checksum=True,
            )
        except ia_except.AuthenticationError as error:
            raise DisseminationError(
                'Error uploading to Internet Archive: credentials missing') \
                from error
        except (ia_except.InvalidChecksumError,
                ia_except.ItemLocateError) as error:
            raise DisseminationError(
                'Internet Archive file upload failed for {}: {}'.format(
                    identifier, error)) from error
        except req_except.HTTPError as error:
            raise DisseminationError(
                'Internet Archive file upload failed for {}: {}'.format(
                    identifier, error)) from error
        except req_except.RequestException as error:
            raise DisseminationError(
                'Internet Archive file upload request failed for {}: {}'.format(
                    identifier, error)) from error

        if not responses:
            raise DisseminationError(
                'Error uploading to Internet Archive: no response received '
                'for requested files')

        for response in responses:
            self._validate_response(
                response, 'Internet Archive file upload', allow_empty=True)

    def _modify_metadata(self, item, metadata_patch, access_key, secret_key):
        try:
            response = item.modify_metadata(
                metadata_patch,
                access_key=access_key,
                secret_key=secret_key,
                refresh=False,
            )
        except ia_except.AuthenticationError as error:
            raise DisseminationError(
                'Error updating Internet Archive metadata: credentials missing') \
                from error
        except (ia_except.ItemLocateError,
                req_except.RequestException) as error:
            raise DisseminationError(
                'Internet Archive metadata update failed for {}: {}'.format(
                    self.work_id, error)) from error

        self._validate_response(
            response, 'Internet Archive metadata update', allow_empty=False)

    @staticmethod
    def _validate_response(response, action, allow_empty):
        status_code = getattr(response, 'status_code', None)
        if status_code is None:
            if allow_empty:
                return
            raise DisseminationError(
                '{} failed: no HTTP status was returned'.format(action))

        if not 200 <= status_code < 300:
            response_text = getattr(response, 'text', '')
            raise DisseminationError(
                '{} failed with HTTP status {}: {}'.format(
                    action, status_code, response_text))

        try:
            response_body = response.json()
        except (AttributeError, ValueError):
            response_body = {}
        if isinstance(response_body, dict) \
                and response_body.get('success') is False:
            raise DisseminationError(
                '{} failed: {}'.format(
                    action, response_body.get('error', response_body)))

    def _managed_metadata_patch(self, current_metadata, desired_metadata):
        current_metadata = current_metadata or {}
        desired_metadata = desired_metadata.copy()

        current_collections = self._as_metadata_list(
            current_metadata.get('collection'))
        extra_collections = [
            collection for collection in current_collections
            if collection != self.THOTH_COLLECTION
        ]
        if extra_collections:
            desired_metadata['collection'] = current_collections.copy()
            if self.THOTH_COLLECTION not in current_collections:
                desired_metadata['collection'].append(self.THOTH_COLLECTION)

        patch = {}
        for field in self.MANAGED_METADATA_FIELDS:
            if field not in desired_metadata:
                if field in current_metadata:
                    patch[field] = 'REMOVE_TAG'
                continue
            if not self._metadata_values_equal(
                    field, current_metadata.get(field),
                    desired_metadata[field]):
                patch[field] = desired_metadata[field]
        return patch

    def _metadata_verification_problems(
            self, current_metadata, desired_metadata, absent_fields):
        current_metadata = current_metadata or {}
        problems = []
        for field, desired_value in desired_metadata.items():
            current_value = current_metadata.get(field)
            if field == 'collection':
                if self.THOTH_COLLECTION not in self._as_metadata_list(
                        current_value):
                    problems.append(
                        '{} is {!r}, expected to include {!r}'.format(
                            field, current_value, self.THOTH_COLLECTION))
            elif not self._metadata_values_equal(
                    field, current_value, desired_value):
                problems.append(
                    '{} is {!r}, expected {!r}'.format(
                        field, current_value, desired_value))

        for field in sorted(absent_fields):
            if field in current_metadata:
                problems.append(
                    '{} is still present with value {!r}, expected it to be '
                    'absent'.format(field, current_metadata[field]))
        return problems

    @classmethod
    def _metadata_values_equal(cls, field, current_value, desired_value):
        if field in cls.REPEATABLE_METADATA_FIELDS:
            return cls._as_metadata_list(current_value) \
                == cls._as_metadata_list(desired_value)
        if field == 'thoth-work-id':
            return cls._as_metadata_list(current_value) \
                == cls._as_metadata_list(desired_value)
        return cls._clean_metadata_value(current_value) \
            == cls._clean_metadata_value(desired_value)

    @classmethod
    def _as_metadata_list(cls, value):
        if isinstance(value, (list, tuple, set)):
            values = value
        elif value is None:
            values = []
        else:
            values = [value]
        return [
            cleaned for cleaned in (
                cls._clean_metadata_value(entry) for entry in values)
            if cleaned is not None
        ]

    @classmethod
    def _clean_metadata_value(cls, value):
        if value is None:
            return None
        if isinstance(value, (list, tuple, set)):
            cleaned_values = [
                cleaned for cleaned in (
                    cls._clean_metadata_value(entry) for entry in value)
                if cleaned is not None
            ]
            return cleaned_values or None
        if isinstance(value, str) \
                and (not value.strip() or value == 'None'):
            return None
        return value

    def _verify_final_state(
            self, item, expected_md5s, desired_metadata, absent_fields):
        last_file_problems = ['item file metadata was not available']
        last_metadata_problems = ['item metadata was not available']
        last_refresh_problem = None
        for attempt in range(1, self.VERIFICATION_ATTEMPTS + 1):
            try:
                item.refresh()
                originals = self._original_files(item.files)
                file_problems = []
                for name, expected_md5 in expected_md5s.items():
                    if name not in originals:
                        file_problems.append(
                            '{} is missing as an original file'.format(name))
                        continue
                    remote_md5 = self._file_value(originals[name], 'md5')
                    if remote_md5 != expected_md5:
                        file_problems.append(
                            '{} has MD5 {!r}, expected {!r}'.format(
                                name, remote_md5, expected_md5))
                metadata_problems = self._metadata_verification_problems(
                    item.metadata, desired_metadata, absent_fields)
                last_file_problems = file_problems
                last_metadata_problems = metadata_problems
                last_refresh_problem = None
                if not file_problems and not metadata_problems:
                    return {
                        name: self._file_value(originals[name], 'md5')
                        for name in expected_md5s
                    }
            except req_except.RequestException as error:
                last_refresh_problem = 'refresh failed: {}'.format(error)

            problem_groups = []
            if last_file_problems:
                problem_groups.append(
                    'file discrepancies: {}'.format(
                        '; '.join(last_file_problems)))
            if last_metadata_problems:
                problem_groups.append(
                    'metadata discrepancies: {}'.format(
                        '; '.join(last_metadata_problems)))
            if last_refresh_problem:
                problem_groups.append(last_refresh_problem)
            last_problem = '; '.join(problem_groups)

            if attempt < self.VERIFICATION_ATTEMPTS:
                logging.debug(
                    'Internet Archive verification incomplete on attempt %s: %s',
                    attempt, last_problem)
                sleep(self.VERIFICATION_SLEEP_SECONDS)

        raise InternetArchiveVerificationError(
            'Timed out verifying Internet Archive item {} after {} attempts: {}'
            .format(self.work_id, self.VERIFICATION_ATTEMPTS, last_problem))

    def parse_metadata(self):
        """Convert work metadata into Internet Archive format."""
        work_metadata = self.metadata.get('data', {}).get('work', {})
        contributions = work_metadata.get('contributions') or []
        publications = work_metadata.get('publications') or []
        subjects_metadata = work_metadata.get('subjects') or []
        languages_metadata = work_metadata.get('languages') or []
        issues = work_metadata.get('issues') or []

        creators = [
            contribution.get('fullName')
            for contribution in contributions
            if contribution.get('mainContribution') is True
        ]
        isbns = [
            publication.get('isbn').replace('-', '')
            for publication in publications
            if publication.get('isbn') is not None
        ]
        subjects = [
            subject.get('subjectCode') for subject in subjects_metadata
        ]
        languages = [
            language.get('languageCode') for language in languages_metadata
        ]
        issns = [
            (issue.get('series') or {}).get(key)
            for issue in issues
            for key in ['issnPrint', 'issnDigital']
        ]
        volume = next(iter([
            str(issue.get('issueOrdinal'))
            for issue in issues
            if issue.get('issueOrdinal') is not None
        ]), None)
        page_count = work_metadata.get('pageCount')

        ia_metadata = {
            'collection': self.THOTH_COLLECTION,
            'title': work_metadata.get('fullTitle'),
            'publisher': self.get_publisher_name(),
            'creator': creators,
            'date': work_metadata.get('publicationDate'),
            'description': work_metadata.get('longAbstract'),
            'imagecount': None if page_count is None else str(page_count),
            'isbn': isbns,
            'lccn': work_metadata.get('lccn'),
            'licenseurl': work_metadata.get('license'),
            'mediatype': 'texts',
            'oclc-id': work_metadata.get('oclc'),
            'source': work_metadata.get('doi'),
            'subject': subjects,
            'language': languages,
            'issn': issns,
            'volume': volume,
            'thoth-work-id': self.work_id,
            'thoth-dissemination-service': self.version,
        }

        return {
            field: cleaned
            for field, value in ia_metadata.items()
            if (cleaned := self._clean_metadata_value(value)) is not None
        }
