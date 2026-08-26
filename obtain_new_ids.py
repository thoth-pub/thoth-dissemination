#!/usr/bin/env python3
"""
Acquire a list of work IDs to be disseminated.
Purpose: automatic dissemination at regular intervals of specified works from selected publishers.
For dissemination to (Loughborough) Figshare, Zenodo, CUL and Google Play:
find newly-published works for upload. Internet Archive selection is based on
recent relation-aware updates and usable canonical PDF sources.
For dissemination to Crossref: find newly-updated works for metadata deposit (including update).
Based on `iabulkupload/obtain_work_ids.py`.
"""

# Both third-party packages already included in thoth-dissemination/requirements.txt
from thothlibrary import errors
import argparse
import json
import logging
import math
from datetime import datetime, timedelta, UTC
from os import environ
from pathlib import Path
import sys
from uuid import UUID

from internet_archive_policy import SUPPORTED_WORK_TYPES
from publisher_source import (
    MODE_API,
    MODE_COMPARE,
    MODE_ENV,
    PublisherDiscoveryError,
    PublisherSourceConfigurationError,
    build_comparison_report,
    comparison_report_error,
    discover_api_publisher_ids,
    resolve_source_mode,
    sanitise_detail,
    summarise_comparison,
    write_comparison_report,
)
from thothapi import (
    get_internet_archive_selection_works,
    get_thoth_client,
)


DEFAULT_IA_LOOKBACK_HOURS = 30
MAX_IA_LOOKBACK_HOURS = 168
DEFAULT_IA_MAX_IDS = 200
MAX_IA_MAX_IDS = 200
IA_QUERY_PAGE_SIZE = 100


class InternetArchiveSelectionError(RuntimeError):
    """Internet Archive selection could not complete safely."""


def canonical_utc_timestamp(value):
    """Render a timezone-aware datetime as canonical UTC ISO 8601."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('timestamp is timezone-naive')
    return value.astimezone(UTC).isoformat().replace('+00:00', 'Z')


def parse_api_timestamp(value):
    """Parse an API timestamp without guessing a missing timezone."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError('timestamp is missing')
    normalised = value.strip()
    if normalised.endswith(('Z', 'z')):
        normalised = normalised[:-1] + '+00:00'
    parsed = datetime.fromisoformat(normalised)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('timestamp is timezone-naive')
    return parsed.astimezone(UTC)


def lookback_hours_type(value):
    """Argparse validator for the bounded IA overlap window."""
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError(
            'lookback hours must be a number') from error
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError(
            'lookback hours must be a positive finite number')
    if number > MAX_IA_LOOKBACK_HOURS:
        raise argparse.ArgumentTypeError(
            'lookback hours may not exceed {}'.format(
                MAX_IA_LOOKBACK_HOURS))
    return int(number) if number.is_integer() else number


def max_ids_type(value):
    """Argparse validator for the IA matrix hard cap."""
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError(
            'max IDs must be an integer') from error
    if number < 1 or number > MAX_IA_MAX_IDS:
        raise argparse.ArgumentTypeError(
            'max IDs must be between 1 and {}'.format(MAX_IA_MAX_IDS))
    return number


class IDFinder():
    """Common logic for retrieving work IDs for all platforms"""

    def __init__(self, platform=None, source_mode=MODE_ENV,
                 comparison_report_path=None, thoth=None):
        """Set up Thoth client instance and variables for use in other methods"""
        self.thoth = get_thoth_client() if thoth is None else thoth
        self.thoth_ids = []
        self.work_statuses = None
        self.work_types = None
        self.publishers = None
        self.order = None
        self.updated_at_with_relations = None
        self.platform = platform
        self.source_mode = source_mode
        self.comparison_report_path = comparison_report_path
        self.comparison_report = None
        self.legacy_publisher_ids = None
        self.api_publisher_ids = None

    def run(self):
        """
        Retrieve the required set of work IDs and output them
        (as an array of comma-separated, quote-enclosed strings)
        """
        self.get_publishers()
        self.get_query_parameters()
        if self.publisher_selection_is_empty():
            # A reconciled empty API assignment set is a successful no-op.
            # An empty publisher filter must never reach an existing work
            # query, where it would mean "every publisher".
            self.thoth_ids = []
        else:
            self.get_thoth_ids()
        self.remove_exceptions()
        self.post_process()
        logging.info('List of IDs found: {}'.format(self.thoth_ids))
        print(json.dumps(self.thoth_ids, separators=(',', ':')))

    def publisher_selection_is_empty(self):
        """Whether API-authoritative discovery resolved to no publishers."""
        return self.source_mode == MODE_API and not self.api_publisher_ids

    def get_publishers(self):
        """Retrieve IDs for all publishers whose works should be included"""
        if self.source_mode == MODE_API:
            self.get_api_publishers()
            return
        self.get_legacy_publishers()
        if self.source_mode == MODE_COMPARE:
            self.compare_publishers(self.legacy_publisher_ids)

    def get_api_publishers(self):
        """Resolve publishers solely from Publisher Services assignments"""
        # Fails closed: there is deliberately no ENV_PUBLISHERS fallback.
        self.api_publisher_ids = discover_api_publisher_ids(
            self.thoth, self.platform)
        self.publishers = json.dumps(self.api_publisher_ids)

    def compare_publishers(self, legacy_publisher_ids):
        """
        Compare the authoritative legacy publisher set against Publisher
        Services observationally.

        The legacy selection has already succeeded when this runs. No failure
        here may alter the selected works, contaminate the work-ID stdout
        contract or change the process exit status.
        """
        try:
            report = build_comparison_report(
                self.thoth, self.platform, legacy_publisher_ids)
        except Exception as error:
            report = comparison_report_error(self.platform, error)
        self.comparison_report = report

        if self.comparison_report_path:
            try:
                write_comparison_report(self.comparison_report_path, report)
            except Exception as error:
                logging.error(
                    'Unable to write the publisher comparison report: %s: %s',
                    type(error).__name__, sanitise_detail(error))
        # Comparison evidence never reaches stdout, which remains the
        # work-ID contract, and an ERROR here never fails the selection.
        log = (
            logging.error if report['status'] == 'ERROR' else logging.info)
        log('Publisher comparison (%s):\n%s',
            report['status'], summarise_comparison(report))

    def get_legacy_publishers(self):
        """Retrieve IDs for all publishers configured in the environment"""
        # Check that a list of IDs of publishers whose works should be uploaded
        # has been provided as a JSON-formatted environment variable
        try:
            publishers_env = json.loads(environ.get('ENV_PUBLISHERS'))
        except:
            logging.error(
                'Failed to retrieve publisher IDs from environment variable')
            sys.exit(1)

        # Test that list is not empty - if so, the Thoth client call would erroneously
        # retrieve the full list of works from all publishers
        if len(publishers_env) < 1:
            logging.error(
                'No publisher IDs found in environment variable: list is empty')
            sys.exit(1)

        # Test that all supplied publisher IDs are valid - if a mistyped ID was passed to the Thoth
        # client call, it would behave the same as a valid ID for which no relevant works exist
        for publisher in publishers_env:
            try:
                self.thoth.publisher(publisher_id=publisher)
            except errors.ThothError:
                # Don't include full error text as it's lengthy (contains full query/response)
                logging.error('No record found for publisher {}: ID may be incorrect'.format(
                    publisher))
                sys.exit(1)

        self.legacy_publisher_ids = publishers_env
        self.publishers = json.dumps(publishers_env)

    def get_query_parameters(self):
        """
        Construct Thoth work ID query parameters depending on platform-specific
        requirements
        """
        # Default: all active (published) works listed in Thoth (from the selected publishers).
        self.work_statuses = '[ACTIVE]'
        # Default: all work types included except for chapters (from the selected publishers).
        self.work_types = '[MONOGRAPH, EDITED_BOOK, JOURNAL_ISSUE, TEXTBOOK, BOOK_SET]'
        # Start with the most recent, so that we can disregard everything else
        # as soon as we hit the first work published earlier than the desired date range.
        self.order = '{field: PUBLICATION_DATE, direction: DESC}'
        self.updated_at_with_relations = None

    def get_thoth_ids(self):
        """Query Thoth GraphQL API with relevant parameters to retrieve required work IDs"""
        # `books` query includes Monographs, Edited Books, Textbooks and Journal Issues
        # but excludes Chapters and Book Sets. `bookIds` variant only retrieves their workIds.
        thoth_works = self.thoth.bookIds(
            # The default limit is 100; publishers' back catalogues may be bigger than that
            limit='9999',
            work_statuses=self.work_statuses,
            order=self.order,
            publishers=self.publishers,
            updated_at_with_relations=self.updated_at_with_relations,
        )

        # Extract the Thoth work ID strings from the set of results
        self.thoth_ids = [n.workId for n in thoth_works]

    def get_thoth_ids_iteratively(self, start_date, end_date):
        """
        Query Thoth GraphQL API with relevant parameters to retrieve required work IDs,
        iterating through results to select only those published between the specified dates
        """
        # TODO Once https://github.com/thoth-pub/thoth/issues/486 is completed,
        # we can simply construct a standard query filtering by publication date
        offset = 0
        while True:
            next_batch = self.thoth.works(
                limit=1,
                offset=offset,
                work_statuses=self.work_statuses,
                work_types=self.work_types,
                order=self.order,
                publishers=self.publishers,
                updated_at_with_relations=self.updated_at_with_relations,
            )
            if len(next_batch) < 1:
                # No more works to be found
                break
            offset += 1
            next_work = next_batch[0]
            next_work_pub_date = datetime.strptime(next_work.publicationDate, "%Y-%m-%d").date()
            if next_work_pub_date > end_date:
                # This work will be handled in the next run - don't cause duplication
                continue
            elif next_work_pub_date >= start_date:
                # This work was published in the target period - include it
                self.thoth_ids.append(next_work.workId)
            else:
                # We've reached the first work in the list which was published
                # earlier than the target period - stop
                break

    def remove_exceptions(self):
        """
        If a list of exceptions has been provided, remove these from the results
        (e.g. works that are ineligible for upload due to not being available as PDFs)
        """
        # Omitted exceptions may be represented as None if running locally,
        # or an empty string if passed via GitHub Actions inheritance
        if environ.get('ENV_EXCEPTIONS'):
            try:
                exceptions = json.loads(environ.get('ENV_EXCEPTIONS').lower())
                self.thoth_ids = list(
                    set(self.thoth_ids).difference(exceptions))
            except Exception:
                # Current use case for exceptions list is just to avoid attempting
                # uploads which are expected to fail. However, an exception here
                # would indicate that the list has been entered incorrectly.
                # Early-exit to alert users that it needs to be fixed.
                logging.error(
                    'Failed to retrieve excepted works from environment variable')
                sys.exit(1)

    def post_process(self):
        """
        Amend list of retrieved work IDs depending on platform-specific
        requirements
        """
        # Default: not required - keep full list
        pass


class MonthlyIDFinder(IDFinder):
    """Logic for retrieving work IDs for monthly catchup dissemination"""

    def get_thoth_ids(self):
        """Query Thoth GraphQL API with relevant parameters to retrieve required work IDs"""
        # TODO Once https://github.com/thoth-pub/thoth/issues/486 is completed,
        # we can remove this overriding method and simply construct a standard query
        # filtering by publication date

        # In addition to the conditions of the query parameters, we need to filter the results
        # to obtain only works with a publication date within the previous calendar month.
        # The schedule for finding and depositing newly published works is once monthly
        # (a few days after the start of the month, to allow for delays in updating records).
        current_date = datetime.now(UTC).date()
        current_month_start = current_date.replace(day=1)
        previous_month_end = current_month_start - timedelta(days=1)
        previous_month_start = previous_month_end.replace(day=1)

        self.get_thoth_ids_iteratively(previous_month_start, previous_month_end)


class InternetArchiveIDFinder(IDFinder):
    """Select recently updated IA-eligible works without retrieving sources."""

    def __init__(
            self, lookback_hours=DEFAULT_IA_LOOKBACK_HOURS,
            max_ids=DEFAULT_IA_MAX_IDS, report_path=None, now_provider=None,
            thoth=None, platform='InternetArchive', source_mode=MODE_ENV,
            comparison_report_path=None):
        super().__init__(
            platform=platform,
            source_mode=source_mode,
            comparison_report_path=comparison_report_path,
            thoth=thoth,
        )
        self.lookback_hours = lookback_hours
        self.max_ids = max_ids
        self.report_path = Path(report_path) if report_path else None
        self.now_provider = now_provider or (lambda: datetime.now(UTC))
        self.publisher_ids = []
        self.exception_ids = []
        self.report = None
        self.window_start = None
        self.window_end = None

    @staticmethod
    def _normalise_uuid_list(raw_value, variable_name, required):
        if raw_value is None or raw_value == '':
            if required:
                raise InternetArchiveSelectionError(
                    '{} must be a non-empty JSON array'.format(variable_name))
            return []
        try:
            values = json.loads(raw_value)
        except (TypeError, ValueError) as error:
            raise InternetArchiveSelectionError(
                '{} must be a JSON array'.format(variable_name)) from error
        if not isinstance(values, list) or (required and not values):
            qualifier = 'non-empty ' if required else ''
            raise InternetArchiveSelectionError(
                '{} must be a {}JSON array'.format(variable_name, qualifier))

        normalised = []
        for value in values:
            if not isinstance(value, str):
                raise InternetArchiveSelectionError(
                    '{} contains a non-string UUID'.format(variable_name))
            try:
                normalised.append(str(UUID(value)))
            except (ValueError, AttributeError) as error:
                raise InternetArchiveSelectionError(
                    '{} contains malformed UUID {}'.format(
                        variable_name, value)
                ) from error
        return sorted(set(normalised))

    def get_publishers(self):
        """Validate, normalise and confirm every configured publisher."""
        if self.source_mode == MODE_API:
            # Fails closed: there is no ENV_PUBLISHERS fallback.
            self.publisher_ids = discover_api_publisher_ids(
                self.thoth, self.platform)
            self.api_publisher_ids = self.publisher_ids
            return

        self.publisher_ids = self._normalise_uuid_list(
            environ.get('ENV_PUBLISHERS'), 'ENV_PUBLISHERS', required=True)
        for publisher_id in self.publisher_ids:
            try:
                publisher = self.thoth.publisher(publisher_id=publisher_id)
            except Exception as error:
                raise InternetArchiveSelectionError(
                    'Publisher {} could not be confirmed'.format(
                        publisher_id)
                ) from error
            if publisher is None:
                raise InternetArchiveSelectionError(
                    'Publisher {} was not found'.format(publisher_id))
        self.legacy_publisher_ids = list(self.publisher_ids)
        if self.source_mode == MODE_COMPARE:
            self.compare_publishers(self.legacy_publisher_ids)

    def get_exceptions(self):
        """Validate and normalise the optional configured work exceptions."""
        self.exception_ids = self._normalise_uuid_list(
            environ.get('ENV_EXCEPTIONS'), 'ENV_EXCEPTIONS', required=False)

    @staticmethod
    def _work_value(work, key, default=None):
        if isinstance(work, dict):
            return work.get(key, default)
        return getattr(work, key, default)

    @classmethod
    def _exclusion(cls, work, reason, timestamp=None):
        work_id = cls._work_value(work, 'workId')
        raw_timestamp = cls._work_value(work, 'updatedAtWithRelations')
        return {
            'work_id': None if work_id is None else str(work_id),
            'updated_at_with_relations': (
                canonical_utc_timestamp(timestamp)
                if timestamp is not None
                else raw_timestamp
            ),
            'reason': reason,
        }

    @classmethod
    def _source_exclusion_reason(cls, work):
        publications = cls._work_value(work, 'publications', []) or []
        pdf_publications = [
            publication for publication in publications
            if cls._work_value(publication, 'publicationType') == 'PDF'
        ]
        if not pdf_publications:
            return 'no_pdf_publication'

        locations = cls._work_value(
            pdf_publications[0], 'locations', []) or []
        canonical_locations = [
            location for location in locations
            if cls._work_value(location, 'canonical') is True
        ]
        if not canonical_locations:
            return 'no_canonical_pdf_location'
        if not any(
                isinstance(cls._work_value(location, 'fullTextUrl'), str)
                and cls._work_value(location, 'fullTextUrl').strip()
                for location in canonical_locations):
            return 'canonical_pdf_location_missing_full_text_url'
        return None

    @staticmethod
    def _excluded_sort_key(entry):
        timestamp = entry.get('updated_at_with_relations')
        try:
            timestamp_key = parse_api_timestamp(timestamp)
        except (TypeError, ValueError):
            timestamp_key = datetime.max.replace(tzinfo=UTC)
        return (
            timestamp_key,
            entry.get('work_id') or '',
            entry.get('reason') or '',
            '' if timestamp is None else str(timestamp),
        )

    def _write_report(self, report):
        if self.report_path is None:
            return
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        serialised = json.dumps(
            report, indent=2, sort_keys=True, ensure_ascii=True) + '\n'
        self.report_path.write_text(serialised, encoding='utf-8')

    def _base_report(self, generated_at):
        return {
            'platform': 'InternetArchive',
            'generated_at': canonical_utc_timestamp(generated_at),
            'window': {
                'start': canonical_utc_timestamp(self.window_start),
                'end': canonical_utc_timestamp(self.window_end),
                'lookback_hours': self.lookback_hours,
            },
            'publisher_ids': self.publisher_ids,
            'exception_ids': self.exception_ids,
            'queried_count': 0,
            'eligible_count': 0,
            'selected_count': 0,
            'omitted_count': 0,
            'truncated': False,
            'selection_limit': self.max_ids,
            'selected': [],
            'omitted': [],
            'excluded_counts': {},
            'excluded': [],
        }

    def write_failure_report(self, error):
        """Write a sanitised report for configuration/query failures."""
        if self.window_end is None:
            now = self.now_provider()
            if now.tzinfo is None or now.utcoffset() is None:
                now = datetime.now(UTC)
            self.window_end = now.astimezone(UTC)
            self.window_start = self.window_end - timedelta(
                hours=self.lookback_hours)
        report = self._base_report(self.window_end)
        report['error'] = sanitise_detail(
            '{}: {}'.format(type(error).__name__, error))
        report['status'] = 'failed'
        self.report = report
        self._write_report(report)

    def select(self):
        """Build the deterministic selected, omitted and excluded records."""
        captured_now = self.now_provider()
        if captured_now.tzinfo is None or captured_now.utcoffset() is None:
            raise InternetArchiveSelectionError(
                'Current time must be timezone-aware')
        self.window_end = captured_now.astimezone(UTC)
        self.window_start = self.window_end - timedelta(
            hours=self.lookback_hours)

        self.get_publishers()
        self.get_exceptions()
        report = self._base_report(self.window_end)
        if self.publisher_selection_is_empty():
            # A reconciled empty API assignment set selects nothing; an empty
            # publisher filter must never reach the selection query.
            works = []
        else:
            works = get_internet_archive_selection_works(
                self.thoth,
                self.publisher_ids,
                SUPPORTED_WORK_TYPES,
                canonical_utc_timestamp(self.window_start),
                page_size=IA_QUERY_PAGE_SIZE,
            )
        report['queried_count'] = len(works)

        newest_by_work_id = {}
        excluded = []
        for work in works:
            work_id = self._work_value(work, 'workId')
            raw_timestamp = self._work_value(
                work, 'updatedAtWithRelations')
            if raw_timestamp is None or raw_timestamp == '':
                excluded.append(self._exclusion(
                    work, 'missing_update_timestamp'))
                continue
            try:
                timestamp = parse_api_timestamp(raw_timestamp)
            except (TypeError, ValueError):
                excluded.append(self._exclusion(
                    work, 'malformed_update_timestamp'))
                continue
            if work_id is None:
                excluded.append(self._exclusion(
                    work, 'missing_work_id', timestamp))
                continue
            work_id = str(work_id)
            current = newest_by_work_id.get(work_id)
            if current is None or timestamp > current[0]:
                newest_by_work_id[work_id] = (timestamp, work)

        eligible = []
        exception_ids = set(self.exception_ids)
        for work_id, (timestamp, work) in newest_by_work_id.items():
            reason = None
            if timestamp <= self.window_start:
                reason = 'outside_window'
            elif timestamp > self.window_end:
                reason = 'after_window_end'
            elif work_id in exception_ids:
                reason = 'configured_exception'
            elif self._work_value(work, 'workStatus') != 'ACTIVE':
                reason = 'inactive'
            elif self._work_value(work, 'workType') not in SUPPORTED_WORK_TYPES:
                reason = 'unsupported_work_type'
            else:
                reason = self._source_exclusion_reason(work)

            if reason is not None:
                excluded.append(self._exclusion(work, reason, timestamp))
                continue
            eligible.append({
                'work_id': work_id,
                'updated_at_with_relations': canonical_utc_timestamp(timestamp),
                '_timestamp': timestamp,
            })

        eligible.sort(key=lambda entry: (
            entry['_timestamp'], entry['work_id']))
        selected = eligible[:self.max_ids]
        omitted = eligible[self.max_ids:]
        for entry in selected + omitted:
            entry.pop('_timestamp')

        excluded.sort(key=self._excluded_sort_key)
        excluded_counts = {}
        for entry in excluded:
            reason = entry['reason']
            excluded_counts[reason] = excluded_counts.get(reason, 0) + 1

        report.update({
            'eligible_count': len(eligible),
            'selected_count': len(selected),
            'omitted_count': len(omitted),
            'truncated': bool(omitted),
            'selected': selected,
            'omitted': omitted,
            'excluded_counts': dict(sorted(excluded_counts.items())),
            'excluded': excluded,
        })
        self.thoth_ids = [entry['work_id'] for entry in selected]
        self.report = report
        return report

    def run(self):
        """Select, report, then emit only the compact JSON work-ID array."""
        report = self.select()
        self._write_report(report)
        if report['truncated']:
            logging.warning(
                '%s eligible works exceeded the %s-work limit; %s omitted. '
                'Inspect the selection artifact and use bounded manual '
                'reconciliation or another reviewed bounded operation.',
                report['eligible_count'],
                report['selection_limit'],
                report['omitted_count'],
            )
        logging.info('List of IDs found: %s', self.thoth_ids)
        print(json.dumps(self.thoth_ids, separators=(',', ':')))


class WeeklyIDFinder(IDFinder):
    """Logic for retrieving work IDs for weekly catchup dissemination"""

    def get_thoth_ids(self):
        """Query Thoth GraphQL API with relevant parameters to retrieve required work IDs"""
        # TODO Once https://github.com/thoth-pub/thoth/issues/486 is completed,
        # we can remove this overriding method and simply construct a standard query
        # filtering by publication date

        # In addition to the conditions of the query parameters, we need to filter the results
        # to obtain only works with a publication date within the previous week.
        # The schedule for finding and depositing newly published works is once weekly.
        current_date = datetime.now(UTC).date()
        previous_week_end = current_date - timedelta(days=1)
        previous_week_start = previous_week_end - timedelta(days=6)

        self.get_thoth_ids_iteratively(previous_week_start, previous_week_end)


class CrossrefIDFinder(IDFinder):
    """Logic for retrieving work IDs which is specific to Crossref dissemination"""

    def get_query_parameters(self):
        """Construct Thoth work ID query parameters depending on Crossref-specific requirements"""
        # The schedule for finding and depositing updated metadata is once hourly.
        # TODO ideally we could pass this value from the GitHub Action to ensure synchronisation.
        DEPOSIT_INTERVAL_HRS = 1

        # Scheduled GitHub Actions may not start exactly at the specified time.
        # A couple of months of daily runs showed average delay of 10-15 mins.
        # Try to avoid missing any works which were updated in the gap between
        # when the Action should have run and when it actually ran.
        DELAY_BUFFER_HRS = 0.25

        # Target: all works listed in Thoth (from the selected publishers) which are
        # Active or Forthcoming, and which have been updated since the last deposit.
        # Use UTC, as GitHub Actions scheduling runs in UTC.
        current_time = datetime.now(UTC)
        last_deposit_time = current_time - \
            timedelta(hours=(DEPOSIT_INTERVAL_HRS + DELAY_BUFFER_HRS))
        last_deposit_time_str = datetime.strftime(
            last_deposit_time, "%Y-%m-%dT%H:%M:%SZ")

        self.work_statuses = '[ACTIVE, FORTHCOMING]'
        # Start with the most recently updated
        self.order = '{field: UPDATED_AT_WITH_RELATIONS, direction: DESC}'
        self.updated_at_with_relations = '{{timestamp: "{}", expression: GREATER_THAN}}'.format(
            last_deposit_time_str)

    def post_process(self):
        """
        Exclude from the results any Forthcoming works which don't yet have
        both a DOI and publication date.
        """
        for id in reversed(self.thoth_ids):
            work = self.thoth.work_by_id(work_id=id)
            if work.workStatus == 'FORTHCOMING':
                if not work.doi or not work.publicationDate:
                    self.thoth_ids.remove(id)


class GooglePlayIDFinder(IDFinder):
    """Logic for retrieving work IDs which is specific to Google Play dissemination"""

    def get_thoth_ids(self):
        """Query Thoth GraphQL API with relevant parameters to retrieve required work IDs"""
        # TODO Once https://github.com/thoth-pub/thoth/issues/486 is completed,
        # we can remove this overriding method and simply construct a standard query
        # filtering by publication date

        # In addition to the conditions of the query parameters, we need to filter the results
        # to obtain only works with a publication date within the previous day.
        # The schedule for finding and depositing newly published works is once daily.
        current_date = datetime.now(UTC).date()
        previous_day = current_date - timedelta(days=1)

        self.get_thoth_ids_iteratively(previous_day, previous_day)


class BKCIIDFinder(IDFinder):
    """Logic for retrieving work IDs which is specific to Clarivate Web of Science Book Citation Index (BKCI) dissemination"""

    def get_query_parameters(self):
        """Construct Thoth work ID query parameters depending on Clarivate BKCI-specific requirements"""
        # Target: all active (published) works listed in Thoth (from the selected publishers), except for textbooks
        self.work_statuses = '[ACTIVE]'
        self.work_types = '[MONOGRAPH, EDITED_BOOK, JOURNAL_ISSUE, BOOK_SET]'
        # Start with the most recent, so that we can disregard everything else
        # as soon as we hit the first work published earlier than the desired date range.
        self.order = '{field: PUBLICATION_DATE, direction: DESC}'
        self.updated_at_with_relations = None

    def get_thoth_ids(self):
        """Query Thoth GraphQL API with relevant parameters to retrieve required work IDs"""
        # TODO Once https://github.com/thoth-pub/thoth/issues/486 is completed,
        # we can remove this overriding method and simply construct a standard query
        # filtering by publication date

        # In addition to the conditions of the query parameters, we need to filter the results
        # to obtain only works with a publication date within the previous calendar month.
        # The schedule for finding and depositing newly published works is once monthly
        # (a few days after the start of the month, to allow for delays in updating records).
        current_date = datetime.now(UTC).date()
        current_month_start = current_date.replace(day=1)
        previous_month_end = current_month_start - timedelta(days=1)
        previous_month_start = previous_month_end.replace(day=1)

        self.get_thoth_ids_iteratively(previous_month_start, previous_month_end)


class OapenLocationsIDFinder(IDFinder):
    """
    Helper class for workflow which updates Thoth records with newly-registered
    OAPEN/DOAB location URLs (by searching their APIs). See obtain_oapen_locations.py.
    """

    def get_query_parameters(self):
        """Construct Thoth work ID query parameters based on OAPEN location workflow requirements"""
        # Target: all active (published) works listed in Thoth (from the selected publishers).
        self.work_statuses = '[ACTIVE]'
        # Order doesn't matter: default to publication date descending
        self.order = '{field: PUBLICATION_DATE, direction: DESC}'
        self.updated_at_with_relations = None

    def post_process(self):
        """
        Narrow down the results to works which have a PDF publication but are
        missing an OAPEN and/or DOAB location.
        Returns a list of 3-tuples (publication_id, doi, missing_platforms)
        where missing_platforms is a list of platform names that are missing
        (e.g. ["OAPEN"], ["DOAB"], or ["OAPEN", "DOAB"]).
        """
        oapen_location_required = []
        for id in self.thoth_ids:
            work = self.thoth.work_by_id(work_id=id)
            try:
                pdf_publication = [pub for pub in work.publications
                                   if pub.publicationType == 'PDF'][0]
            except IndexError:
                continue
            has_oapen = any(
                loc.locationPlatform == 'OAPEN' for loc in pdf_publication.locations
            )
            has_doab = any(
                loc.locationPlatform == 'DOAB' for loc in pdf_publication.locations
            )
            missing_platforms = []
            if not has_oapen:
                missing_platforms.append("OAPEN")
            if not has_doab:
                missing_platforms.append("DOAB")
            if missing_platforms and work.doi:
                doi = work.doi.replace('https://doi.org/', '')
                publication_id = pdf_publication.publicationId
                oapen_location_required.append(
                    (publication_id, doi, missing_platforms)
                )

        self.thoth_ids = oapen_location_required


def get_arguments(argv=None):
    """Simple argument parsing"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--platform")
    parser.add_argument("--locations", action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--lookback-hours',
        type=lookback_hours_type,
        default=DEFAULT_IA_LOOKBACK_HOURS,
        help='IA UTC lookback window (positive hours, maximum 168; default 30)',
    )
    parser.add_argument(
        '--max-ids',
        type=max_ids_type,
        default=DEFAULT_IA_MAX_IDS,
        help='IA selection limit (1-200; default 200)',
    )
    parser.add_argument(
        '--report',
        help='write the Internet Archive selection report to this path',
    )
    parser.add_argument(
        '--comparison-report',
        help=(
            'write the compare-mode publisher comparison report to this '
            'path (never written to stdout)'
        ),
    )
    args = parser.parse_args(argv)
    return args


def get_id_finder(args, now_provider=None, thoth=None):
    """Map parsed CLI arguments to the platform-specific finder."""
    platform = args.platform
    if args.locations:
        # The OAPEN/DOAB location catch-up is a Thoth write-back pathway, not
        # scheduled dissemination publisher discovery. It always retains
        # legacy env publisher authority: PUBLISHER_SOURCE_MODES is not
        # resolved and no Publisher Services discovery call is made.
        if platform == 'OAPEN':
            return OapenLocationsIDFinder(
                platform=platform, source_mode=MODE_ENV, thoth=thoth)
        raise InternetArchiveSelectionError(
            'Locations option is only supported for OAPEN')

    source_mode = resolve_source_mode(
        platform, environ.get('PUBLISHER_SOURCE_MODES'))
    comparison_report_path = (
        args.comparison_report if source_mode == MODE_COMPARE else None)

    finder_classes = {
        'Crossref': CrossrefIDFinder,
        'GooglePlay': GooglePlayIDFinder,
        'OAPEN': WeeklyIDFinder,
        'EBSCOHost': WeeklyIDFinder,
        'JSTOR': WeeklyIDFinder,
        'ProjectMUSE': WeeklyIDFinder,
        'ProQuest': WeeklyIDFinder,
        'Figshare': MonthlyIDFinder,
        'Zenodo': MonthlyIDFinder,
        'CUL': MonthlyIDFinder,
        'BKCI': BKCIIDFinder,
    }
    if platform == 'InternetArchive':
        return InternetArchiveIDFinder(
            lookback_hours=args.lookback_hours,
            max_ids=args.max_ids,
            report_path=args.report,
            now_provider=now_provider,
            thoth=thoth,
            platform=platform,
            source_mode=source_mode,
            comparison_report_path=comparison_report_path,
        )
    finder_class = finder_classes.get(platform)
    if finder_class is None:
        raise InternetArchiveSelectionError(
            'Platform must be one of InternetArchive, Crossref, Figshare, '
            'Zenodo, CUL, GooglePlay, BKCI, OAPEN, EBSCOHost, JSTOR, '
            'ProjectMUSE or ProQuest')
    return finder_class(
        platform=platform,
        source_mode=source_mode,
        comparison_report_path=comparison_report_path,
        thoth=thoth,
    )


def main(argv=None, now_provider=None, thoth=None):
    """Run selection and return a process status."""
    args = get_arguments(argv)
    finder = None
    try:
        finder = get_id_finder(args, now_provider=now_provider, thoth=thoth)
        finder.run()
    except InternetArchiveSelectionError as error:
        if isinstance(finder, InternetArchiveIDFinder):
            try:
                finder.write_failure_report(error)
            except Exception as report_error:
                logging.error(
                    'Unable to write Internet Archive failure report: %s',
                    report_error)
        logging.error('%s', error)
        return 1
    except (PublisherSourceConfigurationError,
            PublisherDiscoveryError) as error:
        # Publisher-source configuration and API-authoritative discovery both
        # fail closed: no legacy fallback and no broadened selection.
        if isinstance(finder, InternetArchiveIDFinder):
            try:
                finder.write_failure_report(error)
            except Exception as report_error:
                logging.error(
                    'Unable to write Internet Archive failure report: %s',
                    report_error)
        logging.error(
            'Publisher source resolution failed: %s: %s',
            type(error).__name__, sanitise_detail(error))
        return 1
    except Exception as error:
        if isinstance(finder, InternetArchiveIDFinder):
            try:
                finder.write_failure_report(error)
            except Exception as report_error:
                logging.error(
                    'Unable to write Internet Archive failure report: %s',
                    report_error)
            logging.error(
                'Internet Archive selection failed: %s: %s',
                type(error).__name__, error)
            return 1
        raise
    return 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s:%(asctime)s: %(message)s')
    sys.exit(main())
