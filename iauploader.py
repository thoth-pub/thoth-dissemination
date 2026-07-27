#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Internet Archive
"""

import hashlib
import json
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
    InternetArchiveRestrictedMetadataError,
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
    # Phase 1 (ordinary) verification: metadata + all expected originals.
    VERIFICATION_ATTEMPTS = 10
    VERIFICATION_SLEEP_SECONDS = 20
    # Phase 2 (extended) verification: only for originals successfully uploaded
    # during this invocation whose new bytes Internet Archive has accepted but
    # not yet exposed (eventual consistency). Bounded backoff, no re-upload.
    UPLOAD_PROPAGATION_ATTEMPTS = 8
    UPLOAD_PROPAGATION_INITIAL_SLEEP_SECONDS = 30
    UPLOAD_PROPAGATION_MAX_SLEEP_SECONDS = 180
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
    ADMIN_ONLY_METADATA_FIELDS = {
        'collection',
    }
    # Fields that Internet Archive derives and owns after upload. IA's derive
    # process recomputes `imagecount` from the deposited PDF and overwrites any
    # value we send. We may seed such fields on initial creation, but must
    # never treat IA's derived value as stale or re-patch it: doing so makes
    # every reconciliation re-queue an update that IA immediately overwrites,
    # so the item never converges to `current` (and bulk-dissemination
    # verification times out on the same mismatch). These are excluded from the
    # auto-mutable set but are NOT restricted (initial-only/admin-only), so a
    # divergence is silently accepted rather than raised as a manual conflict.
    DERIVED_METADATA_FIELDS = {
        'imagecount',
    }
    NON_AUTOMUTABLE_METADATA_FIELDS = (
        INITIAL_ONLY_METADATA_FIELDS
        | ADMIN_ONLY_METADATA_FIELDS
        | DERIVED_METADATA_FIELDS
    )
    MUTABLE_MANAGED_METADATA_FIELDS = (
        MANAGED_METADATA_FIELDS - NON_AUTOMUTABLE_METADATA_FIELDS
    )
    # Managed fields whose final remote state we can verify. Internet
    # Archive-derived fields are excluded because IA owns their value after
    # upload (e.g. it recomputes `imagecount` from the deposited PDF), so
    # neither a divergent value nor its presence when we sent none is a
    # verification failure. This keeps final-state verification consistent with
    # `inspect_item`, which already scopes its checks to the non-derived subsets.
    FINAL_VERIFICATION_METADATA_FIELDS = (
        MANAGED_METADATA_FIELDS - DERIVED_METADATA_FIELDS
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

    @staticmethod
    def _normalise_json_sidecar(metadata_bytes):
        """Return canonical, deterministic bytes for a ``json::thoth`` sidecar.

        Thoth's ``json::thoth`` export is generated per request and begins with
        a top-level ``jsonGeneratedAt`` wall-clock timestamp, so uploading the
        raw export byte-for-byte gives an unchanged work a different expected
        MD5 on every run, making a stable ``current`` state structurally
        impossible for managed JSON originals. This removes only that single
        volatile top-level field and re-serialises the remaining semantic
        payload canonically (sorted keys, no insignificant whitespace, a single
        trailing newline), so two exports that differ only by ``jsonGeneratedAt``
        (or by whitespace or key ordering) yield identical bytes while any real
        metadata change still yields different bytes.

        Failures raise ``InternetArchiveDesiredStateError`` for the ``json``
        component with a concise reason and never expose the full payload; the
        caller adds the work UUID. The raw response is never used as a silent
        fallback.
        """
        if not isinstance(metadata_bytes, bytes):
            raise InternetArchiveDesiredStateError(
                'json',
                'response was not bytes (got {})'.format(
                    type(metadata_bytes).__name__),
            )
        try:
            decoded = metadata_bytes.decode('utf-8')
        except UnicodeDecodeError as error:
            raise InternetArchiveDesiredStateError(
                'json', 'response was not valid UTF-8: {}'.format(error),
            ) from error
        try:
            payload = json.loads(decoded)
        except ValueError as error:
            raise InternetArchiveDesiredStateError(
                'json', 'response was not valid JSON: {}'.format(error),
            ) from error
        if not isinstance(payload, dict):
            raise InternetArchiveDesiredStateError(
                'json',
                'JSON root value is {}, expected a JSON object'.format(
                    type(payload).__name__),
            )
        # Remove only the top-level volatile generation timestamp; any nested
        # field of the same name is deliberately left untouched.
        payload.pop('jsonGeneratedAt', None)
        try:
            canonical = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':'),
                allow_nan=False,
            )
        except ValueError as error:
            raise InternetArchiveDesiredStateError(
                'json',
                'response contained values that canonical JSON serialisation '
                'refuses: {}'.format(error),
            ) from error
        return canonical.encode('utf-8') + b'\n'

    def build_desired_state(self):
        """Construct Archive and Thoth location state without mutating either."""
        try:
            raw_metadata_bytes = self.get_formatted_metadata('json::thoth')
        except Exception as error:
            raise InternetArchiveDesiredStateError(
                'json',
                'JSON export unavailable for {}: {}'.format(
                    self.work_id, error),
            ) from error

        try:
            metadata_bytes = self._normalise_json_sidecar(raw_metadata_bytes)
        except InternetArchiveDesiredStateError as error:
            raise InternetArchiveDesiredStateError(
                'json',
                'JSON sidecar normalisation failed for {}: {}'.format(
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
        initial_only_metadata_problems = []
        admin_only_metadata_problems = []
        if item.exists:
            metadata_patch = self._managed_metadata_patch(
                item.metadata, desired.metadata)
            mutable_metadata_problems = self._metadata_verification_problems(
                item.metadata,
                desired.metadata,
                desired.absent_metadata_fields,
                fields=self.MUTABLE_MANAGED_METADATA_FIELDS,
            )
            initial_only_metadata_problems = \
                self._metadata_verification_problems(
                    item.metadata,
                    desired.metadata,
                    desired.absent_metadata_fields,
                    fields=self.INITIAL_ONLY_METADATA_FIELDS,
                )
            admin_only_metadata_problems = \
                self._metadata_verification_problems(
                    item.metadata,
                    desired.metadata,
                    desired.absent_metadata_fields,
                    fields=self.ADMIN_ONLY_METADATA_FIELDS,
                )
        restricted_metadata_problems = (
            initial_only_metadata_problems + admin_only_metadata_problems
        )
        metadata_problems = (
            mutable_metadata_problems + restricted_metadata_problems
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
            'initial_only_metadata_problems':
                initial_only_metadata_problems,
            'admin_only_metadata_problems': admin_only_metadata_problems,
            'restricted_metadata_problems': restricted_metadata_problems,
            # Compatibility for existing report consumers: immutable metadata
            # remains the initial-only subset.
            'immutable_metadata_problems':
                initial_only_metadata_problems,
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

    def _assert_admin_only_metadata_current(self, item, desired):
        if not item.exists:
            return

        current_metadata = item.metadata or {}
        if 'collection' not in desired.metadata:
            return
        current_collections = self._as_metadata_list(
            current_metadata.get('collection'))
        if self.THOTH_COLLECTION in current_collections:
            return

        raise InternetArchiveRestrictedMetadataError(
            'Internet Archive item {} has incompatible collection membership '
            '{!r}; it must include {!r}. Collection membership changes require '
            'Internet Archive administrator intervention; no automatic '
            'mutation was attempted.'.format(
                desired.identifier,
                current_collections,
                self.THOTH_COLLECTION,
            )
        )

    def _assert_restricted_metadata_current(self, item, desired):
        self._assert_initial_only_metadata_current(item, desired)
        self._assert_admin_only_metadata_current(item, desired)

    def apply_archive_repairs(
            self, item, desired, inspection=None, access_key=None,
            secret_key=None, progress=None):
        """Apply only the file and metadata differences found by inspection."""
        inspection = inspection or self.inspect_item(item, desired)
        if inspection['ownership'] == 'collision':
            self._raise_item_collision(inspection['ownership_reason'])
        self._assert_restricted_metadata_current(item, desired)

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
            try:
                item.refresh()
            except req_except.RequestException as error:
                raise DisseminationError(
                    'Unable to refresh Internet Archive item {} immediately '
                    'before mutation: {}'.format(
                        desired.identifier, error)
                ) from error

            current_ownership = self.classify_item_ownership(item)
            self._assert_item_owned_by_thoth(
                item, ownership=current_ownership, warn_legacy=False)
            inspection = self.inspect_item(
                item, desired, ownership=current_ownership)
            self._assert_restricted_metadata_current(item, desired)
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

        uploaded_file_names = set()
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
            # Only record the file as uploaded after the S3 request was
            # accepted (i.e. _upload_files returned without raising). Internet
            # Archive may still take minutes to expose the new original.
            uploaded_file_names.add(name)
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
            uploaded_file_names=frozenset(uploaded_file_names),
        )

    def _assert_item_owned_by_thoth(
            self, item, ownership=None, warn_legacy=True):
        """Raise when an existing identifier cannot safely be linked to Thoth."""
        ownership = ownership or self.classify_item_ownership(item)
        if ownership['status'] in {'owned', 'missing'}:
            return
        if ownership['status'] == 'legacy':
            if warn_legacy:
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

    @classmethod
    def _upload_propagation_sleeps(cls):
        """Bounded backoff schedule (seconds) for extended propagation waits."""
        sleeps = []
        seconds = cls.UPLOAD_PROPAGATION_INITIAL_SLEEP_SECONDS
        for _ in range(cls.UPLOAD_PROPAGATION_ATTEMPTS):
            sleeps.append(min(seconds, cls.UPLOAD_PROPAGATION_MAX_SLEEP_SECONDS))
            seconds = int(seconds * 1.5)
        return sleeps

    @staticmethod
    def _describe_file_problem(problem):
        if problem['kind'] == 'missing':
            return '{} is missing as an original file'.format(problem['name'])
        return '{} has MD5 {!r}, expected {!r}'.format(
            problem['name'], problem['remote_md5'], problem['expected_md5'])

    def _check_final_state(
            self, item, expected_md5s, desired_metadata, absent_fields):
        """Refresh once and compare originals + metadata.

        Returns a dict: ``verified`` (name->md5 map or None), ``file_problems``
        (structured list or None), ``metadata_problems`` (list or None) and
        ``refresh_error`` (str or None).
        """
        try:
            item.refresh()
        except req_except.RequestException as error:
            return {'verified': None, 'file_problems': None,
                    'metadata_problems': None,
                    'refresh_error': 'refresh failed: {}'.format(error)}
        originals = self._original_files(item.files)
        file_problems = []
        for name, expected_md5 in expected_md5s.items():
            if name not in originals:
                file_problems.append({
                    'name': name, 'kind': 'missing',
                    'remote_md5': None, 'expected_md5': expected_md5})
                continue
            remote_md5 = self._file_value(originals[name], 'md5')
            if remote_md5 != expected_md5:
                file_problems.append({
                    'name': name, 'kind': 'stale',
                    'remote_md5': remote_md5, 'expected_md5': expected_md5})
        metadata_problems = self._metadata_verification_problems(
            item.metadata, desired_metadata, absent_fields,
            fields=self.FINAL_VERIFICATION_METADATA_FIELDS)
        verified = None
        if not file_problems and not metadata_problems:
            verified = {name: self._file_value(originals[name], 'md5')
                        for name in expected_md5s}
        return {'verified': verified, 'file_problems': file_problems,
                'metadata_problems': metadata_problems, 'refresh_error': None}

    @staticmethod
    def _is_upload_propagation_only(result, uploaded_file_names):
        """True when the only remaining discrepancies are originals uploaded in
        this invocation that Internet Archive has accepted but not yet exposed.

        Deliberately conservative: any refresh error, metadata discrepancy, or
        a file discrepancy for a file not uploaded in this invocation disqualifies
        the extended phase.
        """
        if result.get('refresh_error'):
            return False
        if result.get('metadata_problems'):
            return False
        file_problems = result.get('file_problems')
        if not file_problems:
            return False
        for problem in file_problems:
            if problem['name'] not in uploaded_file_names:
                return False
            if problem['kind'] not in ('missing', 'stale'):
                return False
        return True

    @staticmethod
    def _problem_summary(result):
        groups = []
        if result.get('file_problems'):
            groups.append('file discrepancies: {}'.format('; '.join(
                IAUploader._describe_file_problem(p)
                for p in result['file_problems'])))
        if result.get('metadata_problems'):
            groups.append('metadata discrepancies: {}'.format(
                '; '.join(result['metadata_problems'])))
        if result.get('refresh_error'):
            groups.append(result['refresh_error'])
        return '; '.join(groups) or 'no item state was available'

    def _verify_final_state(
            self, item, expected_md5s, desired_metadata, absent_fields,
            uploaded_file_names=frozenset()):
        # --- Phase 1: ordinary verification (metadata + all originals) ---
        result = {'file_problems': None, 'metadata_problems': None,
                  'refresh_error': 'item state was not available',
                  'verified': None}
        for attempt in range(1, self.VERIFICATION_ATTEMPTS + 1):
            result = self._check_final_state(
                item, expected_md5s, desired_metadata, absent_fields)
            if result['verified'] is not None:
                return result['verified']
            if attempt < self.VERIFICATION_ATTEMPTS:
                logging.debug(
                    'Internet Archive verification incomplete on attempt %s: %s',
                    attempt, self._problem_summary(result))
                sleep(self.VERIFICATION_SLEEP_SECONDS)

        # --- Phase 2 gate ---
        if self._is_upload_propagation_only(result, uploaded_file_names):
            return self._verify_uploaded_file_propagation(
                item, expected_md5s, desired_metadata, absent_fields,
                uploaded_file_names, result)

        raise InternetArchiveVerificationError(
            'Timed out verifying Internet Archive item {} after {} attempts: {}'
            .format(self.work_id, self.VERIFICATION_ATTEMPTS,
                    self._problem_summary(result)))

    def _verify_uploaded_file_propagation(
            self, item, expected_md5s, desired_metadata, absent_fields,
            uploaded_file_names, last_result):
        """Extended, bounded wait for accepted-but-pending original uploads.

        Never re-uploads. Only re-reads the item state on a backoff schedule.
        """
        result = last_result
        sleeps = self._upload_propagation_sleeps()
        completed_attempts = 0
        elapsed_seconds = 0
        stopped_early = False
        for attempt, sleep_seconds in enumerate(sleeps, start=1):
            logging.info(
                'Waiting %ss for Internet Archive to expose accepted upload(s) '
                'for %s (extended attempt %s/%s)',
                sleep_seconds, self.work_id, attempt, len(sleeps))
            sleep(sleep_seconds)
            completed_attempts = attempt
            elapsed_seconds += sleep_seconds
            result = self._check_final_state(
                item, expected_md5s, desired_metadata, absent_fields)
            if result['verified'] is not None:
                return result['verified']
            # If a new non-propagation problem surfaces (metadata drift, refresh
            # error, or a file we did not upload), stop waiting immediately and
            # report the transition accurately -- do not claim the full
            # propagation deadline expired.
            if not self._is_upload_propagation_only(result, uploaded_file_names):
                stopped_early = True
                break

        if stopped_early:
            raise InternetArchiveVerificationError(
                'Internet Archive verification stopped during upload '
                'propagation for item {} after {} extended attempts (~{}s): '
                '{}'.format(
                    self.work_id, completed_attempts, elapsed_seconds,
                    self._problem_summary(result)))

        raise InternetArchiveVerificationError(
            'Timed out waiting for accepted Internet Archive upload propagation '
            'for item {} after {} extended attempts (~{}s): {}'.format(
                self.work_id, completed_attempts, elapsed_seconds,
                self._problem_summary(result)))

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
