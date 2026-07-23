#!/usr/bin/env python3
"""Inspect and optionally reconcile Internet Archive dissemination state."""

import argparse
from copy import deepcopy
import json
import logging
from os import environ
from pathlib import Path
import sys
from uuid import UUID

from dotenv import load_dotenv
from internetarchive import get_item
from thothlibrary import ThothError

from errors import (
    DisseminationError,
    InternetArchiveDesiredStateError,
    InternetArchiveVerificationError,
)
from iauploader import IAUploader
from thothapi import get_thoth_client
from uploader import Uploader
from version import __version__
from write_locations import (
    DuplicateLocationsError,
    LocationInput,
    decide_location_action,
    retrieve_existing_locations,
    upsert_location,
)


DEFAULT_LIMIT = 100
DEFAULT_EXPORT_URL = 'https://export.thoth.pub'
SUPPORTED_WORK_TYPES = (
    'MONOGRAPH',
    'EDITED_BOOK',
    'JOURNAL_ISSUE',
    'TEXTBOOK',
    'BOOK_SET',
)
ACTIVE_WORK_STATUSES = ('ACTIVE',)
ARCHIVE_ACTIONS = {
    'create_archive_item',
    'upload_pdf_original',
    'upload_json_original',
    'update_archive_metadata',
}
LOCATION_ACTIONS = {
    'create_thoth_location',
    'update_thoth_location',
}

ISSUE_ORDER = (
    'work_not_found',
    'thoth_work_lookup_failed',
    'ineligible_status',
    'unsupported_work_type',
    'no_pdf_publication',
    'no_pdf_source',
    'pdf_source_unavailable',
    'json_export_unavailable',
    'malformed_metadata',
    'archive_request_failed',
    'identifier_collision',
    'item_missing',
    'missing_pdf_original',
    'missing_json_original',
    'stale_pdf_original',
    'stale_json_original',
    'archive_collection_membership_conflict',
    'archive_immutable_metadata_conflict',
    'archive_metadata_stale',
    'thoth_location_lookup_failed',
    'location_missing',
    'location_stale',
    'duplicate_locations',
    'archive_mutation_failed',
    'thoth_location_mutation_failed',
    'verification_failed',
)

ACTION_ORDER = (
    'resolve_identifier_collision',
    'resolve_archive_collection_membership',
    'resolve_archive_immutable_metadata',
    'restore_pdf_source',
    'restore_json_export',
    'fix_work_eligibility',
    'create_archive_item',
    'upload_pdf_original',
    'upload_json_original',
    'update_archive_metadata',
    'create_thoth_location',
    'update_thoth_location',
    'resolve_duplicate_locations',
)

STATUS_ORDER = (
    'identifier_collision',
    'metadata_conflict',
    'error',
    'source_unavailable',
    'duplicate_locations',
    'item_missing',
    'item_incomplete',
    'files_stale',
    'metadata_stale',
    'location_missing',
    'location_stale',
    'ineligible',
    'current',
)

ISSUE_STATUS = {
    'work_not_found': 'error',
    'thoth_work_lookup_failed': 'error',
    'ineligible_status': 'ineligible',
    'unsupported_work_type': 'ineligible',
    'no_pdf_publication': 'ineligible',
    'no_pdf_source': 'ineligible',
    'pdf_source_unavailable': 'source_unavailable',
    'json_export_unavailable': 'source_unavailable',
    'malformed_metadata': 'error',
    'archive_request_failed': 'error',
    'identifier_collision': 'identifier_collision',
    'item_missing': 'item_missing',
    'missing_pdf_original': 'item_incomplete',
    'missing_json_original': 'item_incomplete',
    'stale_pdf_original': 'files_stale',
    'stale_json_original': 'files_stale',
    'archive_collection_membership_conflict': 'metadata_conflict',
    'archive_immutable_metadata_conflict': 'metadata_conflict',
    'archive_metadata_stale': 'metadata_stale',
    'thoth_location_lookup_failed': 'error',
    'location_missing': 'location_missing',
    'location_stale': 'location_stale',
    'duplicate_locations': 'duplicate_locations',
    'archive_mutation_failed': 'error',
    'thoth_location_mutation_failed': 'error',
    'verification_failed': 'error',
}


class ReconciliationConfigurationError(RuntimeError):
    """The requested batch cannot be configured safely."""


class WorkLookupError(RuntimeError):
    """One requested Thoth work could not be loaded."""


def _value(value, key, default=None):
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _ordered_unique(values, ordering):
    order = {value: index for index, value in enumerate(ordering)}
    return sorted(
        set(values),
        key=lambda value: (order.get(value, len(order)), value),
    )


def _status_for_issues(issues):
    statuses = {ISSUE_STATUS.get(issue, 'error') for issue in issues}
    if not statuses:
        return 'current'
    return next(status for status in STATUS_ORDER if status in statuses)


def _base_result(work_id):
    return {
        'work_id': work_id,
        'selection': {
            'explicit': True,
            'publisher': False,
        },
        'publisher_id': None,
        'title': None,
        'publication_id': None,
        'pdf_source_url': None,
        'eligible': False,
        'status': 'error',
        'issues': [],
        'recommended_actions': [],
        'auto_applicable_actions': [],
        'attempted_actions': [],
        'applied_actions': [],
        'uncertain_actions': [],
        'internet_archive': None,
        'thoth_location': None,
        'error': None,
    }


def _location_input(location):
    return LocationInput(
        publication_id=location.publication_id,
        location_platform=location.location_platform,
        landing_page=location.landing_page,
        full_text_url=location.full_text_url,
        checksum=location.checksum,
        checksum_algorithm=location.checksum_algorithm,
    )


def _location_report(locations, location_input, plan=None):
    matches = sorted([
        location for location in locations
        if location['locationPlatform'] == location_input.location_platform
    ], key=lambda location: location['locationId'])
    return {
        'count': len(matches),
        'state': (
            'duplicate' if len(matches) > 1
            else 'missing' if not matches
            else 'current' if plan is not None and plan.action == 'noop'
            else 'stale'
        ),
        'location_id': matches[0]['locationId'] if len(matches) == 1 else None,
        'canonical': matches[0]['canonical'] if len(matches) == 1 else None,
        'locations': [
            {
                'location_id': location['locationId'],
                'publication_id': location['publicationId'],
                'platform': location['locationPlatform'],
                'landing_page': location['landingPage'],
                'full_text_url': location['fullTextUrl'],
                'canonical': location['canonical'],
                'checksum': location['checksum'],
                'checksum_algorithm': location['checksumAlgorithm'],
            }
            for location in matches
        ],
        'other_platform_location_count': len(locations) - len(matches),
        'expected': {
            'platform': location_input.location_platform,
            'landing_page': location_input.landing_page,
            'full_text_url': location_input.full_text_url,
            'checksum': location_input.checksum,
            'checksum_algorithm': location_input.checksum_algorithm,
        },
    }


def _archive_report(item, desired, inspection):
    expected_names = set(desired.expected_md5s)
    originals = IAUploader._original_files(item.files)
    unmanaged_originals = sorted(set(originals) - expected_names)
    all_file_names = {
        IAUploader._file_value(file_metadata, 'name')
        for file_metadata in item.files or []
    }
    all_file_names.discard(None)
    unmanaged_metadata = sorted(
        set((item.metadata or {}).keys()) - IAUploader.MANAGED_METADATA_FIELDS
    )
    return {
        'identifier': desired.identifier,
        'exists': inspection['exists'],
        'ownership': inspection['ownership'],
        'ownership_reason': inspection['ownership_reason'],
        'identifier_available': inspection['identifier_available'],
        'accepted_legacy_item': inspection['legacy'],
        'warnings': (
            ['legacy_item_missing_ownership_marker']
            if inspection['legacy'] else []
        ),
        'files': inspection['files'],
        'metadata': {
            'current': inspection['metadata_current'],
            'problems': inspection['metadata_problems'],
            'mutable_problems': inspection['mutable_metadata_problems'],
            'initial_only_problems': inspection[
                'initial_only_metadata_problems'],
            'admin_only_problems': inspection[
                'admin_only_metadata_problems'],
            'restricted_problems': inspection[
                'restricted_metadata_problems'],
            # Compatibility for existing report consumers.
            'immutable_problems': inspection[
                'immutable_metadata_problems'],
            'patch_fields': sorted(inspection['metadata_patch']),
        },
        'unrelated': {
            'files': sorted(all_file_names - expected_names),
            'original_files': unmanaged_originals,
            'metadata_fields': unmanaged_metadata,
            'collections': sorted([
                collection
                for collection in IAUploader._as_metadata_list(
                    (item.metadata or {}).get('collection'))
                if collection != IAUploader.THOTH_COLLECTION
            ]),
        },
        'expected': {
            'pdf_filename': '{}.pdf'.format(desired.identifier),
            'json_filename': '{}.json'.format(desired.identifier),
            'pdf_md5': desired.expected_md5s[
                '{}.pdf'.format(desired.identifier)],
            'json_md5': desired.expected_md5s[
                '{}.json'.format(desired.identifier)],
            'managed_metadata': desired.metadata,
            'absent_managed_metadata_fields': sorted(
                desired.absent_metadata_fields),
            'landing_page': desired.location.landing_page,
            'full_text_url': desired.location.full_text_url,
        },
    }


def _archive_inventory_report(item, uploader, ownership=None):
    """Report safely discoverable Archive state without desired source state."""
    ownership = ownership or uploader.classify_item_ownership(item)
    all_files = sorted(filter(None, (
        IAUploader._file_value(file_metadata, 'name')
        for file_metadata in item.files or []
    )))
    originals = sorted(IAUploader._original_files(item.files))
    metadata = item.metadata or {}
    return {
        'identifier': uploader.work_id,
        'exists': bool(item.exists),
        'ownership': ownership['status'],
        'ownership_reason': ownership['reason'],
        'identifier_available': ownership['identifier_available'],
        'accepted_legacy_item': ownership['status'] == 'legacy',
        'warnings': (
            ['legacy_item_missing_ownership_marker']
            if ownership['status'] == 'legacy' else []
        ),
        'files': {
            'all_names': all_files,
            'original_names': originals,
        },
        'metadata': None,
        'unrelated': {
            'files': all_files,
            'original_files': originals,
            'metadata_fields': sorted(metadata),
            'collections': sorted(
                IAUploader._as_metadata_list(metadata.get('collection'))),
        },
        'expected': None,
    }, ownership


def _location_inventory_report(locations):
    """Report observed IA locations when desired source state is unavailable."""
    matches = sorted([
        location for location in locations
        if location['locationPlatform'] == 'INTERNET_ARCHIVE'
    ], key=lambda location: location['locationId'])
    return {
        'count': len(matches),
        'state': (
            'duplicate' if len(matches) > 1
            else 'missing' if not matches
            else 'observed'
        ),
        'location_id': matches[0]['locationId'] if len(matches) == 1 else None,
        'canonical': matches[0]['canonical'] if len(matches) == 1 else None,
        'locations': [
            {
                'location_id': location['locationId'],
                'publication_id': location['publicationId'],
                'platform': location['locationPlatform'],
                'landing_page': location['landingPage'],
                'full_text_url': location['fullTextUrl'],
                'canonical': location['canonical'],
                'checksum': location['checksum'],
                'checksum_algorithm': location['checksumAlgorithm'],
            }
            for location in matches
        ],
        'other_platform_location_count': len(locations) - len(matches),
        'expected': None,
    }


class InternetArchiveReconciler:
    """Coordinate read-only inspection and guarded idempotent repairs."""

    def __init__(self, thoth=None, export_url=DEFAULT_EXPORT_URL):
        self.thoth = thoth or get_thoth_client()
        self.export_url = export_url.rstrip('/')
        self.selection_by_work_id = {}

    def publisher_work_ids(self, publisher_id, limit, offset):
        """Return a stable eligible publisher slice without downloading files."""
        try:
            publisher = self.thoth.publisher(publisher_id=publisher_id)
        except Exception as error:
            raise ReconciliationConfigurationError(
                'Publisher {} was not found: {}'.format(publisher_id, error)
            ) from error
        if publisher is None:
            raise ReconciliationConfigurationError(
                'Publisher {} was not found'.format(publisher_id)
            )

        page_size = 100
        page_offset = 0
        work_ids = []
        while True:
            try:
                page = self.thoth.works(
                    limit=page_size,
                    offset=page_offset,
                    publishers=json.dumps([publisher_id]),
                    work_statuses='[{}]'.format(
                        ', '.join(ACTIVE_WORK_STATUSES)),
                    work_types='[{}]'.format(', '.join(SUPPORTED_WORK_TYPES)),
                )
            except Exception as error:
                raise ReconciliationConfigurationError(
                    'Unable to select works for publisher {}: {}'.format(
                        publisher_id, error)
                ) from error

            for work in page:
                publications = _value(work, 'publications', []) or []
                if any(
                        _value(publication, 'publicationType') == 'PDF'
                        for publication in publications):
                    work_ids.append(str(_value(work, 'workId')))
            if len(page) < page_size:
                break
            page_offset += page_size

        return sorted(set(work_ids))[offset:offset + limit]

    def select_work_ids(
            self, publisher_id=None, explicit_work_ids=None,
            limit=DEFAULT_LIMIT, offset=0):
        explicit = set(explicit_work_ids or [])
        publisher = set()
        if publisher_id is not None:
            publisher.update(
                self.publisher_work_ids(publisher_id, limit, offset))
        selected = explicit | publisher
        self.selection_by_work_id = {
            work_id: {
                'explicit': work_id in explicit,
                'publisher': work_id in publisher,
            }
            for work_id in selected
        }
        return sorted(selected)

    def _load_work_metadata(self, work_id):
        try:
            raw = self.thoth.work_by_id(work_id=work_id, raw=True)
        except ThothError as error:
            error_text = str(error)
            if any(marker in error_text.lower() for marker in (
                    'not found', 'no record was found', 'could not find')):
                raise WorkLookupError('work not found') from error
            raise WorkLookupError(
                'Thoth GraphQL lookup failed: {}'.format(error_text)
            ) from error
        except Exception as error:
            raise WorkLookupError(str(error)) from error

        try:
            payload = json.loads(raw) if isinstance(raw, str) else raw
            work = payload['data']['work']
        except (KeyError, TypeError, ValueError) as error:
            raise WorkLookupError(
                'Thoth returned malformed work metadata: {}'.format(error)
            ) from error
        if work is None:
            raise WorkLookupError('work not found')
        Uploader.normalise_work_metadata(payload)
        return payload

    def _uploader(self, work_id, metadata):
        uploader = IAUploader.__new__(IAUploader)
        uploader.work_id = work_id
        uploader.export_url = self.export_url
        uploader.version = __version__
        uploader.metadata = metadata
        return uploader

    @staticmethod
    def _eligibility_issues(work):
        issues = []
        if work.get('workStatus') not in ACTIVE_WORK_STATUSES:
            issues.append('ineligible_status')
        if work.get('workType') not in SUPPORTED_WORK_TYPES:
            issues.append('unsupported_work_type')
        publications = work.get('publications') or []
        if not any(
                publication.get('publicationType') == 'PDF'
                for publication in publications):
            issues.append('no_pdf_publication')
        return issues

    def inspect_work(self, work_id, selection=None):
        """Preflight Archive ownership, then inspect source and remote state."""
        result = _base_result(work_id)
        if selection is not None:
            result['selection'] = {
                'explicit': bool(selection.get('explicit')),
                'publisher': bool(selection.get('publisher')),
            }
        context = {}
        try:
            metadata = self._load_work_metadata(work_id)
        except WorkLookupError as error:
            issue = (
                'work_not_found' if str(error) == 'work not found'
                else 'thoth_work_lookup_failed'
            )
            result['issues'] = [issue]
            result['status'] = _status_for_issues(result['issues'])
            result['error'] = str(error)
            if result['selection']['explicit']:
                uploader = self._uploader(
                    work_id, {'data': {'work': {}}})
                return self._inspect_discoverable_state(
                    result, uploader, context), context
            return result, context

        work = metadata['data']['work']
        uploader = self._uploader(work_id, metadata)
        context['uploader'] = uploader
        publisher = ((work.get('imprint') or {}).get('publisher') or {})
        pdf_publication = next((
            publication for publication in work.get('publications') or []
            if publication.get('publicationType') == 'PDF'
        ), None)
        result.update({
            'publisher_id': publisher.get('publisherId'),
            'title': work.get('title') or work.get('fullTitle'),
            'publication_id': (
                pdf_publication.get('publicationId')
                if pdf_publication is not None else None
            ),
        })
        eligibility_issues = self._eligibility_issues(work)
        result['issues'] = _ordered_unique(eligibility_issues, ISSUE_ORDER)
        result['eligible'] = not eligibility_issues
        if eligibility_issues:
            result['recommended_actions'] = ['fix_work_eligibility']

        result = self._preflight_archive(result, uploader, context)
        if {'archive_request_failed', 'identifier_collision'}.intersection(
                result['issues']):
            return self._inspect_discoverable_state(
                result, uploader, context), context
        if eligibility_issues and not result['selection']['explicit']:
            return self._inspect_discoverable_state(
                result, uploader, context), context

        try:
            source = uploader.get_publication_source('PDF')
        except Exception as error:
            result['issues'] = _ordered_unique(
                result['issues'] + ['no_pdf_source'], ISSUE_ORDER)
            result['recommended_actions'] = _ordered_unique(
                result['recommended_actions'] + ['fix_work_eligibility'],
                ACTION_ORDER,
            )
            result['error'] = str(error)
            return self._inspect_discoverable_state(
                result, uploader, context), context

        result.update({
            'publication_id': source.id,
            'pdf_source_url': source.url,
        })
        try:
            desired = uploader.build_desired_state()
        except InternetArchiveDesiredStateError as error:
            issue = {
                'pdf': 'pdf_source_unavailable',
                'json': 'json_export_unavailable',
                'metadata': 'malformed_metadata',
            }.get(error.source, 'malformed_metadata')
            result['issues'] = _ordered_unique(
                result['issues'] + [issue], ISSUE_ORDER)
            result['recommended_actions'] = _ordered_unique(
                result['recommended_actions'] + [{
                'pdf_source_unavailable': 'restore_pdf_source',
                'json_export_unavailable': 'restore_json_export',
                'malformed_metadata': 'fix_work_eligibility',
                }[issue]], ACTION_ORDER)
            result['error'] = str(error)
            return self._inspect_discoverable_state(
                result, uploader, context), context

        context['desired'] = desired
        result = self._inspect_remote(result, uploader, desired, context)
        return result, context

    def _preflight_archive(self, initial_result, uploader, context):
        """Retrieve and classify one Archive item before source requests."""
        result = deepcopy(initial_result)
        issues = list(result['issues'])
        actions = list(result['recommended_actions'])
        errors = [result['error']] if result['error'] else []
        context['archive_preflight_complete'] = True

        try:
            item = get_item(uploader.work_id)
            ownership = uploader.classify_item_ownership(item)
            archive_report, _ = _archive_inventory_report(
                item, uploader, ownership=ownership)
            context.update({
                'item': item,
                'archive_ownership': ownership,
            })
            result['internet_archive'] = archive_report
            if ownership['status'] == 'collision':
                issues.append('identifier_collision')
                actions.append('resolve_identifier_collision')
        except Exception as error:
            issues.append('archive_request_failed')
            errors.append('Internet Archive request failed: {}'.format(error))

        result['issues'] = _ordered_unique(issues, ISSUE_ORDER)
        result['recommended_actions'] = _ordered_unique(actions, ACTION_ORDER)
        result['status'] = _status_for_issues(result['issues'])
        result['error'] = '; '.join(_ordered_unique(errors, ())) or None
        return result

    def _inspect_discoverable_state(self, initial_result, uploader, context):
        """Inspect ownership and observed locations without expected state."""
        result = deepcopy(initial_result)
        if not context.get('archive_preflight_complete'):
            result = self._preflight_archive(result, uploader, context)
        issues = list(result['issues'])
        actions = list(result['recommended_actions'])
        errors = [result['error']] if result['error'] else []

        ownership = context.get('archive_ownership')
        if ownership is not None:
            if ownership['status'] == 'collision':
                issues.append('identifier_collision')
                actions.append('resolve_identifier_collision')
            elif ownership['status'] == 'missing':
                issues.append('item_missing')
                actions.append('create_archive_item')

        if result['publication_id'] is not None:
            try:
                locations = retrieve_existing_locations(
                    self.thoth, result['publication_id'])
                context['locations'] = locations
                result['thoth_location'] = _location_inventory_report(locations)
                count = result['thoth_location']['count']
                if count == 0:
                    issues.append('location_missing')
                    actions.append('create_thoth_location')
                elif count > 1:
                    issues.append('duplicate_locations')
                    actions.append('resolve_duplicate_locations')
            except Exception as error:
                issues.append('thoth_location_lookup_failed')
                errors.append('Thoth location lookup failed: {}'.format(error))

        result['issues'] = _ordered_unique(issues, ISSUE_ORDER)
        result['recommended_actions'] = _ordered_unique(actions, ACTION_ORDER)
        result['auto_applicable_actions'] = []
        result['status'] = _status_for_issues(result['issues'])
        result['error'] = '; '.join(_ordered_unique(errors, ())) or None
        return result

    def _inspect_remote(self, initial_result, uploader, desired, context):
        result = deepcopy(initial_result)
        issues = list(result['issues'])
        actions = list(result['recommended_actions'])
        errors = [result['error']] if result['error'] else []
        result['internet_archive'] = initial_result.get('internet_archive')
        result['thoth_location'] = None

        try:
            item = context.get('item')
            if item is None:
                item = get_item(desired.identifier)
                context['item'] = item
            ownership = context.get('archive_ownership')
            if ownership is None:
                ownership = uploader.classify_item_ownership(item)
                context['archive_ownership'] = ownership
            inspection = uploader.inspect_item(
                item, desired, ownership=ownership)
            context['archive_inspection'] = inspection
            result['internet_archive'] = _archive_report(
                item, desired, inspection)
            if inspection['legacy']:
                logging.warning(
                    'Internet Archive item %s is an accepted legacy Thoth '
                    'collection item without an ownership marker',
                    desired.identifier,
                )
            if inspection['ownership'] == 'collision':
                issues.append('identifier_collision')
                actions.append('resolve_identifier_collision')
            elif not inspection['exists']:
                issues.append('item_missing')
                actions.extend([
                    'create_archive_item',
                    'upload_pdf_original',
                    'upload_json_original',
                ])
            else:
                pdf_name = '{}.pdf'.format(desired.identifier)
                json_name = '{}.json'.format(desired.identifier)
                for name, missing_issue, stale_issue, action in (
                    (pdf_name, 'missing_pdf_original', 'stale_pdf_original',
                     'upload_pdf_original'),
                    (json_name, 'missing_json_original', 'stale_json_original',
                     'upload_json_original'),
                ):
                    state = inspection['files'][name]
                    if not state['present']:
                        issues.append(missing_issue)
                        actions.append(action)
                    elif not state['current']:
                        issues.append(stale_issue)
                        actions.append(action)
                if inspection['immutable_metadata_problems']:
                    issues.append('archive_immutable_metadata_conflict')
                    actions.append('resolve_archive_immutable_metadata')
                if inspection['admin_only_metadata_problems']:
                    issues.append('archive_collection_membership_conflict')
                    actions.append(
                        'resolve_archive_collection_membership')
                if inspection['mutable_metadata_problems']:
                    issues.append('archive_metadata_stale')
                    actions.append('update_archive_metadata')
        except Exception as error:
            issues.append('archive_request_failed')
            errors.append('Internet Archive request failed: {}'.format(error))

        location_input = _location_input(desired.location)
        context['location_input'] = location_input
        try:
            locations = retrieve_existing_locations(
                self.thoth, desired.publication_id)
            try:
                plan = decide_location_action(location_input, locations)
            except DuplicateLocationsError:
                plan = None
                issues.append('duplicate_locations')
                actions.append('resolve_duplicate_locations')
            context.update({'locations': locations, 'location_plan': plan})
            result['thoth_location'] = _location_report(
                locations, location_input, plan)
            if plan is not None and plan.action == 'create':
                issues.append('location_missing')
                actions.append('create_thoth_location')
            elif plan is not None and plan.action == 'update':
                issues.append('location_stale')
                actions.append('update_thoth_location')
        except Exception as error:
            issues.append('thoth_location_lookup_failed')
            errors.append('Thoth location lookup failed: {}'.format(error))

        issues = _ordered_unique(issues, ISSUE_ORDER)
        actions = _ordered_unique(actions, ACTION_ORDER)
        result['issues'] = issues
        result['recommended_actions'] = actions
        result['status'] = _status_for_issues(issues)
        result['error'] = '; '.join(_ordered_unique(errors, ())) or None
        result['auto_applicable_actions'] = self._auto_applicable_actions(
            result)
        return result

    @staticmethod
    def _auto_applicable_actions(result):
        blocking_issues = {
            'work_not_found',
            'thoth_work_lookup_failed',
            'pdf_source_unavailable',
            'json_export_unavailable',
            'malformed_metadata',
            'archive_request_failed',
            'identifier_collision',
            'archive_collection_membership_conflict',
            'archive_immutable_metadata_conflict',
            'thoth_location_lookup_failed',
            'duplicate_locations',
        }
        if (not result['eligible'] or result['error'] is not None
                or blocking_issues.intersection(result['issues'])):
            return []
        return _ordered_unique([
            action for action in result['recommended_actions']
            if action in ARCHIVE_ACTIONS | LOCATION_ACTIONS
        ], ACTION_ORDER)

    @staticmethod
    def _safe_to_apply(result):
        return bool(result['auto_applicable_actions'])

    def reconcile_one(
            self, work_id, apply=False, credentials=None, selection=None):
        before, context = self.inspect_work(work_id, selection=selection)
        if not apply or not self._safe_to_apply(before):
            return before

        attempted_actions = []
        applied_actions = []
        uncertain_actions = []

        def record_progress(action, state):
            if state == 'attempted':
                attempted_actions.append(action)
            elif state == 'completed':
                applied_actions.append(action)

        archive_actions = [
            action for action in before['auto_applicable_actions']
            if action in ARCHIVE_ACTIONS
        ]
        location_actions = [
            action for action in before['auto_applicable_actions']
            if action in LOCATION_ACTIONS
        ]

        try:
            if archive_actions:
                context['uploader'].apply_archive_repairs(
                    context['item'],
                    context['desired'],
                    inspection=context['archive_inspection'],
                    access_key=credentials['ia_s3_access'],
                    secret_key=credentials['ia_s3_secret'],
                    progress=record_progress,
                )
                # Preserve compatibility with test doubles or alternate
                # uploaders that return success without progress callbacks.
                for action in archive_actions:
                    if action not in attempted_actions:
                        attempted_actions.append(action)
                    if action not in applied_actions:
                        applied_actions.append(action)
        except InternetArchiveVerificationError as error:
            uncertain_actions = [
                action for action in applied_actions
                if action in ARCHIVE_ACTIONS
            ]
            applied_actions = [
                action for action in applied_actions
                if action not in uncertain_actions
            ]
            return self._failed_apply_result(
                before, attempted_actions, applied_actions,
                uncertain_actions, 'verification_failed', str(error))
        except DisseminationError as error:
            return self._failed_apply_result(
                before, attempted_actions, applied_actions,
                uncertain_actions, 'archive_mutation_failed', str(error))
        except Exception as error:
            return self._failed_apply_result(
                before, attempted_actions, applied_actions,
                uncertain_actions, 'archive_mutation_failed', str(error))

        try:
            if location_actions:
                location_result = upsert_location(
                    self.thoth,
                    context['location_input'],
                    progress=record_progress,
                    emit_location_id=False,
                )
                # A successful alternate implementation without progress
                # callbacks still indicates a mutation when it returns an ID.
                if location_result is not None:
                    for action in location_actions:
                        if action not in attempted_actions:
                            attempted_actions.append(action)
                        if action not in applied_actions:
                            applied_actions.append(action)
        except Exception as error:
            return self._failed_apply_result(
                before, attempted_actions, applied_actions,
                uncertain_actions, 'thoth_location_mutation_failed',
                str(error))

        verification_base = deepcopy(before)
        verification_base.update({
            'issues': [],
            'recommended_actions': [],
            'auto_applicable_actions': [],
            'attempted_actions': [],
            'applied_actions': [],
            'uncertain_actions': [],
            'internet_archive': None,
            'thoth_location': None,
            'error': None,
        })
        verification_context = {
            'uploader': context['uploader'],
            'desired': context['desired'],
            'item': context['item'],
        }
        final = self._inspect_remote(
            verification_base,
            context['uploader'],
            context['desired'],
            verification_context,
        )
        final['before'] = before
        final['attempted_actions'] = _ordered_unique(
            attempted_actions, ACTION_ORDER)
        final['applied_actions'] = _ordered_unique(
            applied_actions, ACTION_ORDER)
        final['uncertain_actions'] = _ordered_unique(
            uncertain_actions, ACTION_ORDER)
        if final['status'] != 'current':
            final['issues'] = _ordered_unique(
                final['issues'] + ['verification_failed'], ISSUE_ORDER)
            final['status'] = 'error'
            message = 'Final verification did not converge to current state'
            final['error'] = (
                '{}; {}'.format(final['error'], message)
                if final['error'] else message
            )
        return final

    @staticmethod
    def _failed_apply_result(
            before, attempted_actions, applied_actions, uncertain_actions,
            issue, error):
        result = deepcopy(before)
        result['before'] = before
        result['issues'] = _ordered_unique(
            result['issues'] + [issue], ISSUE_ORDER)
        result['status'] = 'error'
        result['attempted_actions'] = _ordered_unique(
            attempted_actions, ACTION_ORDER)
        result['applied_actions'] = _ordered_unique(
            applied_actions, ACTION_ORDER)
        result['uncertain_actions'] = _ordered_unique(
            uncertain_actions, ACTION_ORDER)
        result['error'] = error
        return result

    def reconcile(self, work_ids, apply=False, credentials=None):
        results = []
        for work_id in work_ids:
            logging.info('Inspecting Internet Archive state for %s', work_id)
            try:
                result = self.reconcile_one(
                    work_id,
                    apply=apply,
                    credentials=credentials,
                    selection=self.selection_by_work_id.get(work_id),
                )
            except Exception as error:
                result = _base_result(work_id)
                selection = self.selection_by_work_id.get(work_id)
                if selection is not None:
                    result['selection'] = selection
                result.update({
                    'status': 'error',
                    'issues': ['thoth_work_lookup_failed'],
                    'error': 'Unexpected reconciliation failure: {}'.format(
                        error),
                })
            results.append(result)
        return results


def validate_apply_credentials(environment=None):
    values = environ if environment is None else environment
    required = ('ia_s3_access', 'ia_s3_secret', 'THOTH_PAT')
    missing = [name for name in required if not values.get(name)]
    if missing:
        raise ReconciliationConfigurationError(
            'Apply mode requires: {}'.format(', '.join(missing)))
    return {name: values[name] for name in required}


def load_local_environment(path=Path('./config.env')):
    """Load local CLI configuration without overriding process values."""
    load_dotenv(dotenv_path=path, override=False)


def summarise(results):
    ambiguous_statuses = {
        'identifier_collision',
        'metadata_conflict',
        'duplicate_locations',
    }
    failed_statuses = {'error', 'source_unavailable', 'ineligible'}
    return {
        'inspected': len(results),
        'current': sum(result['status'] == 'current' for result in results),
        'repairable': sum(
            result['eligible']
            and result['status'] not in ambiguous_statuses | failed_statuses
            and bool(result['auto_applicable_actions'])
            for result in results
        ),
        'ambiguous': sum(
            result['eligible'] and result['status'] in ambiguous_statuses
            for result in results),
        'failed': sum(
            not result['eligible'] or result['status'] in failed_statuses
            for result in results),
        'repaired': sum(
            result['status'] == 'current' and bool(result['applied_actions'])
            for result in results
        ),
        'by_status': {
            status: sum(result['status'] == status for result in results)
            for status in STATUS_ORDER
            if any(result['status'] == status for result in results)
        },
    }


def _redact(value, secrets):
    if isinstance(value, dict):
        return {key: _redact(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, secrets) for item in value]
    if isinstance(value, str):
        redacted = value
        for secret in secrets:
            if secret:
                redacted = redacted.replace(secret, '[REDACTED]')
        return redacted
    return value


def render_report(results, output_format='json', secrets=()):
    safe_results = _redact(results, secrets)
    summary = summarise(safe_results)
    if output_format == 'json':
        return json.dumps(
            {'results': safe_results, 'summary': summary},
            indent=2,
            sort_keys=True,
        ) + '\n'
    lines = [
        json.dumps(result, sort_keys=True, separators=(',', ':'))
        for result in safe_results
    ]
    lines.append(json.dumps(
        {'summary': summary}, sort_keys=True, separators=(',', ':')))
    return '\n'.join(lines) + '\n'


def write_report(report, output=None):
    if output is None:
        sys.stdout.write(report)
        return
    Path(output).write_text(report, encoding='utf-8')


def uuid_value(value):
    try:
        return str(UUID(value))
    except (ValueError, AttributeError) as error:
        raise argparse.ArgumentTypeError(
            '{} is not a valid UUID'.format(value)) from error


def positive_integer(value):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError('must be an integer') from error
    if parsed < 1:
        raise argparse.ArgumentTypeError('must be at least 1')
    return parsed


def nonnegative_integer(value):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError('must be an integer') from error
    if parsed < 0:
        raise argparse.ArgumentTypeError('must be at least 0')
    return parsed


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(
        description='Inspect and safely reconcile Internet Archive state')
    parser.add_argument('--publisher-id', type=uuid_value)
    parser.add_argument('--work-id', action='append', type=uuid_value,
                        default=[])
    parser.add_argument('--limit', type=positive_integer,
                        default=DEFAULT_LIMIT)
    parser.add_argument('--offset', type=nonnegative_integer, default=0)
    parser.add_argument('--output')
    parser.add_argument('--format', choices=('json', 'jsonl'), default='json')
    parser.add_argument('--apply', action='store_true')
    arguments = parser.parse_args(argv)
    if arguments.publisher_id is None and not arguments.work_id:
        parser.error('at least one --publisher-id or --work-id is required')
    return arguments


def main(argv=None):
    arguments = parse_arguments(argv)
    credentials = None
    try:
        load_local_environment()
        if arguments.apply:
            credentials = validate_apply_credentials()
        reconciler = InternetArchiveReconciler()
        if arguments.apply:
            reconciler.thoth.set_token(credentials['THOTH_PAT'])
        work_ids = reconciler.select_work_ids(
            publisher_id=arguments.publisher_id,
            explicit_work_ids=arguments.work_id,
            limit=arguments.limit,
            offset=arguments.offset,
        )
        results = reconciler.reconcile(
            work_ids, apply=arguments.apply, credentials=credentials)
        secret_values = [
            environ.get(name)
            for name in ('ia_s3_access', 'ia_s3_secret', 'THOTH_PAT')
        ]
        write_report(
            render_report(results, arguments.format, secret_values),
            arguments.output,
        )
    except (ReconciliationConfigurationError, OSError) as error:
        logging.error('%s', error)
        return 2

    summary = summarise(results)
    if arguments.apply:
        return 1 if any(
            result['status'] != 'current' for result in results) else 0
    return 1 if summary['failed'] else 0


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(asctime)s: %(message)s',
    )
    logging.getLogger('urllib3').setLevel(logging.INFO)
    sys.exit(main())
