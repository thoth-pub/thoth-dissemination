#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Internet Archive
"""

import hashlib
import logging
from dataclasses import dataclass
from io import BytesIO
from time import sleep

from internetarchive import get_item, upload, exceptions as ia_except
from requests import exceptions as req_except

from errors import (
    DisseminationError,
    InternetArchiveDesiredStateError,
    InternetArchiveIdentifierCollisionError,
    InternetArchiveImmutableMetadataError,
    InternetArchiveVerificationError,
)
from uploader import Uploader, Location


@dataclass(frozen=True)
class IADesiredState:
    """Complete Thoth-managed state expected for one Archive item."""

    identifier: str
    publication_id: str
    source_url: str
    file_bytes: dict
    expected_md5s: dict
    metadata: dict
    absent_metadata_fields: frozenset
    location: Location


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
    INITIAL_ONLY_METADATA_FIELDS = {
        'mediatype',
    }
    MUTABLE_MANAGED_METADATA_FIELDS = (
        MANAGED_METADATA_FIELDS - INITIAL_ONLY_METADATA_FIELDS
    )

    def upload_to_platform(self):
        """Create or idempotently update a work in Internet Archive."""
        identifier = self.work_id
        try:
            item = get_item(identifier)
        except req_except.RequestException as error:
            raise DisseminationError(
                'Error inspecting Internet Archive item {}: {}'.format(
                    identifier, error)) from error

        ownership = self.classify_item_ownership(item)
        self._assert_item_owned_by_thoth(item, ownership=ownership)

        desired = self.build_desired_state()
        inspection = self.inspect_item(item, desired, ownership=ownership)
        verified_md5s = self.apply_archive_repairs(
            item,
            desired,
            inspection=inspection,
        )

        logging.info(
            'Successfully verified Internet Archive item at {}'.format(
                desired.location.landing_page))

        location = desired.location
        return [Location(
            location.publication_id,
            location.location_platform,
            location.landing_page,
            location.full_text_url,
            verified_md5s['{}.pdf'.format(identifier)],
            location.checksum_algorithm,
        )]

    def build_desired_state(self):
        """Construct Archive and Thoth location state without mutating either."""
        try:
            metadata_bytes = self.get_formatted_metadata('json::thoth')
        except Exception as error:
            raise InternetArchiveDesiredStateError(
                'json',
                'JSON export unavailable for {}: {}'.format(
                    self.work_id, error),
            ) from error

        try:
            publication = self.get_publication_details('PDF')
        except Exception as error:
            raise InternetArchiveDesiredStateError(
                'pdf',
                'PDF source unavailable for {}: {}'.format(
                    self.work_id, error),
            ) from error

        if not isinstance(metadata_bytes, bytes):
            raise InternetArchiveDesiredStateError(
                'json',
                'JSON export unavailable for {}: response was not bytes'.format(
                    self.work_id),
            )
        if not isinstance(publication.bytes, bytes) or not publication.bytes:
            raise InternetArchiveDesiredStateError(
                'pdf',
                'PDF source unavailable for {}: response was empty or not bytes'
                .format(self.work_id),
            )

        try:
            ia_metadata = self.parse_metadata()
            missing_metadata = [
                field for field in ('title', 'publisher', 'mediatype',
                                    'collection', 'thoth-work-id')
                if field not in ia_metadata
            ]
            if missing_metadata:
                raise ValueError(
                    'missing required fields {}'.format(
                        ', '.join(missing_metadata)))
        except Exception as error:
            raise InternetArchiveDesiredStateError(
                'metadata',
                'Malformed Thoth metadata for {}: {}'.format(
                    self.work_id, error),
            ) from error

        identifier = self.work_id
        file_bytes = {
            '{}.pdf'.format(identifier): publication.bytes,
            '{}.json'.format(identifier): metadata_bytes,
        }
        expected_md5s = {
            name: hashlib.md5(contents).hexdigest()
            for name, contents in file_bytes.items()
        }
        pdf_name = '{}.pdf'.format(identifier)
        landing_page = 'https://archive.org/details/{}'.format(identifier)
        full_text_url = 'https://archive.org/download/{}/{}'.format(
            identifier, pdf_name)
        location = Location(
            publication.id,
            'INTERNET_ARCHIVE',
            landing_page,
            full_text_url,
            expected_md5s[pdf_name],
            'MD5',
        )
        return IADesiredState(
            identifier=identifier,
            publication_id=publication.id,
            source_url=publication.source_url,
            file_bytes=file_bytes,
            expected_md5s=expected_md5s,
            metadata=ia_metadata,
            absent_metadata_fields=frozenset(
                self.MANAGED_METADATA_FIELDS - ia_metadata.keys()),
            location=location,
        )

    def classify_item_ownership(self, item):
        """Return an ownership classification shared by inspect and apply paths."""
        if not item.exists:
            try:
                available = item.identifier_available()
            except Exception as error:
                raise DisseminationError(
                    'Unable to check Internet Archive identifier availability '
                    'for {} after no public item metadata was available: {}; '
                    'the item will not be created or modified'.format(
                        self.work_id, error)
                ) from error
            if type(available) is not bool:
                raise DisseminationError(
                    'Internet Archive identifier availability returned an '
                    'invalid response {!r} for {} after no public item '
                    'metadata was available; the item will not be created or '
                    'modified'.format(available, self.work_id)
                )
            if available:
                return {
                    'status': 'missing',
                    'reason': None,
                    'identifier_available': True,
                }
            return {
                'status': 'collision',
                'reason': (
                    'no public item metadata was available, but the identifier '
                    'availability API reported the identifier unavailable; the '
                    'item will not be created or modified'
                ),
                'identifier_available': False,
            }

        marker_values = self._as_metadata_list(
            item.metadata.get('thoth-work-id'))
        if marker_values:
            if all(value == self.work_id for value in marker_values):
                return {
                    'status': 'owned',
                    'reason': None,
                    'identifier_available': None,
                }
            return {
                'status': 'collision',
                'reason': (
                    'existing thoth-work-id metadata is {!r}'.format(
                        marker_values)
                ),
                'identifier_available': None,
            }

        collections = self._as_metadata_list(
            item.metadata.get('collection'))
        if (item.identifier == self.work_id
                and self.THOTH_COLLECTION in collections):
            return {
                'status': 'legacy',
                'reason': 'legacy Thoth collection item has no ownership marker',
                'identifier_available': None,
            }

        return {
            'status': 'collision',
            'reason': (
                'item has no matching thoth-work-id metadata and is not an '
                'identifiable legacy member of the {} collection'.format(
                    self.THOTH_COLLECTION)
            ),
            'identifier_available': None,
        }

    def inspect_item(self, item, desired, ownership=None):
        """Compare one Archive item with desired state without mutating it."""
        ownership = ownership or self.classify_item_ownership(item)
        originals = self._original_files(item.files)
        files = self.compare_original_files(
            originals, desired.expected_md5s)
        metadata_patch = {}
        mutable_metadata_problems = []
        immutable_metadata_problems = []
        if item.exists:
            metadata_patch = self._managed_metadata_patch(
                item.metadata, desired.metadata)
            mutable_metadata_problems = self._metadata_verification_problems(
                item.metadata,
                desired.metadata,
                desired.absent_metadata_fields,
                fields=self.MUTABLE_MANAGED_METADATA_FIELDS,
            )
            immutable_metadata_problems = \
                self._metadata_verification_problems(
                    item.metadata,
                    desired.metadata,
                    desired.absent_metadata_fields,
                    fields=self.INITIAL_ONLY_METADATA_FIELDS,
                )
        metadata_problems = (
            mutable_metadata_problems + immutable_metadata_problems
        )
        return {
            'exists': bool(item.exists),
            'ownership': ownership['status'],
            'ownership_reason': ownership['reason'],
            'identifier_available': ownership['identifier_available'],
            'legacy': ownership['status'] == 'legacy',
            'files': files,
            'metadata_current': bool(item.exists) and not metadata_problems,
            'metadata_problems': metadata_problems,
            'mutable_metadata_problems': mutable_metadata_problems,
            'immutable_metadata_problems': immutable_metadata_problems,
            'metadata_patch': metadata_patch,
        }

    def _assert_initial_only_metadata_current(self, item, desired):
        if not item.exists:
            return

        current_metadata = item.metadata or {}
        conflicts = []
        for field in sorted(self.INITIAL_ONLY_METADATA_FIELDS):
            if field not in desired.metadata:
                continue
            current_value = current_metadata.get(field)
            required_value = desired.metadata[field]
            if not self._metadata_values_equal(
                    field, current_value, required_value):
                conflicts.append(
                    '{} is {!r}, required {!r}'.format(
                        field, current_value, required_value)
                )

        if conflicts:
            raise InternetArchiveImmutableMetadataError(
                'Internet Archive item {} has incompatible initial-only '
                'metadata: {}. These fields can only be set during item '
                'creation; no automatic mutation was attempted.'.format(
                    desired.identifier, '; '.join(conflicts))
            )

    def apply_archive_repairs(
            self, item, desired, inspection=None, access_key=None,
            secret_key=None, progress=None):
        """Apply only the file and metadata differences found by inspection."""
        inspection = inspection or self.inspect_item(item, desired)
        if inspection['ownership'] == 'collision':
            self._raise_item_collision(inspection['ownership_reason'])
        self._assert_initial_only_metadata_current(item, desired)

        files_to_upload = [
            name
            for name in (
                '{}.pdf'.format(desired.identifier),
                '{}.json'.format(desired.identifier),
            )
            if not inspection['files'][name]['current']
        ]
        creating_item = not inspection['exists']
        metadata_update_required = (
            inspection['exists'] and bool(inspection['metadata_patch'])
        )
        if files_to_upload or metadata_update_required:
            access_key = access_key or self.get_variable_from_env(
                'ia_s3_access', 'Internet Archive')
            secret_key = secret_key or self.get_variable_from_env(
                'ia_s3_secret', 'Internet Archive')

        for index, name in enumerate(files_to_upload):
            action = (
                'upload_pdf_original' if name.endswith('.pdf')
                else 'upload_json_original'
            )
            if progress is not None and creating_item and index == 0:
                progress('create_archive_item', 'attempted')
            if progress is not None:
                progress(action, 'attempted')
            self._upload_files(
                desired.identifier,
                {name: BytesIO(desired.file_bytes[name])},
                desired.metadata if creating_item and index == 0 else None,
                access_key,
                secret_key,
            )
            if progress is not None and creating_item and index == 0:
                progress('create_archive_item', 'completed')
            if progress is not None:
                progress(action, 'completed')

        if metadata_update_required:
            if progress is not None:
                progress('update_archive_metadata', 'attempted')
            self._modify_metadata(
                item,
                inspection['metadata_patch'],
                access_key,
                secret_key,
            )
            if progress is not None:
                progress('update_archive_metadata', 'completed')

        return self._verify_final_state(
            item,
            desired.expected_md5s,
            desired.metadata,
            desired.absent_metadata_fields,
        )

    def _assert_item_owned_by_thoth(self, item, ownership=None):
        """Raise when an existing identifier cannot safely be linked to Thoth."""
        ownership = ownership or self.classify_item_ownership(item)
        if ownership['status'] in {'owned', 'missing'}:
            return
        if ownership['status'] == 'legacy':
            logging.warning(
                'Internet Archive item %s is an accepted legacy Thoth '
                'collection item without thoth-work-id metadata',
                self.work_id)
            return

        self._raise_item_collision(ownership['reason'])

    def _raise_item_collision(self, reason):
        raise InternetArchiveIdentifierCollisionError(
            'Internet Archive identifier collision for {}: {}; refusing to '
            'modify the item'.format(self.work_id, reason))

    @classmethod
    def _files_requiring_upload(cls, item, file_bytes, expected_md5s):
        comparisons = cls.compare_original_files(
            cls._original_files(item.files), expected_md5s)
        return {
            name: BytesIO(contents)
            for name, contents in file_bytes.items()
            if not comparisons[name]['current']
        }

    @classmethod
    def compare_original_files(cls, originals, expected_md5s):
        """Return deterministic managed-original comparisons by filename."""
        comparisons = {}
        for name in sorted(expected_md5s):
            file_metadata = originals.get(name)
            remote_md5 = None if file_metadata is None else cls._file_value(
                file_metadata, 'md5')
            comparisons[name] = {
                'present': file_metadata is not None,
                'remote_md5': remote_md5,
                'expected_md5': expected_md5s[name],
                'current': (
                    file_metadata is not None
                    and remote_md5 == expected_md5s[name]
                ),
            }
        return comparisons

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
        for field in self.MUTABLE_MANAGED_METADATA_FIELDS:
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
            self, current_metadata, desired_metadata, absent_fields,
            fields=None):
        current_metadata = current_metadata or {}
        fields = self.MANAGED_METADATA_FIELDS if fields is None else fields
        problems = []
        for field, desired_value in desired_metadata.items():
            if field not in fields:
                continue
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
            if field in fields and field in current_metadata:
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
