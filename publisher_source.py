#!/usr/bin/env python3
"""
Publisher-source resolution and observational publisher comparison.

This module implements the DIS-01 publisher-source mechanism approved in
`thoth-pub/thoth-dissemination#97`. It resolves which authority supplies the
publisher list for one scheduled dissemination pathway:

- `env`:     the existing environment publisher configuration is authoritative
             and no Publisher Services discovery call is made;
- `compare`: the environment configuration remains authoritative, and the
             Publisher Services assignment state is read observationally and
             compared against it;
- `api`:     Publisher Services assignments are authoritative and fail closed.

Nothing here disseminates, uploads, writes a Thoth location, sends email,
creates or mutates a distribution job, or mutates Publisher Services
configuration. Discovery consumes only the public, anonymous read surfaces of
the pinned Thoth v1.7.0 contract and requires no credential.

Modes are activated only by the repository-level `PUBLISHER_SOURCE_MODES`
variable. When that variable is absent - which is its state at merge - every
pathway resolves to `env`.
"""

import argparse
import json
from os import environ
from pathlib import Path
import re
import sys

from thothapi import (
    DISTRIBUTION_PLATFORM_PAGE_SIZE,
    get_distribution_platform_publisher_ids,
)


MODE_ENV = 'env'
MODE_COMPARE = 'compare'
MODE_API = 'api'
SOURCE_MODES = (MODE_ENV, MODE_COMPARE, MODE_API)

PUBLISHER_SOURCE_MODES_VARIABLE = 'PUBLISHER_SOURCE_MODES'

# Keys that would express a default or catch-all mode. None of them may exist,
# because no wildcard may activate non-legacy publisher authority.
WILDCARD_MODE_KEYS = ('*', '', 'all', 'ALL', 'default', 'DEFAULT')

COMPARISON_SCHEMA_VERSION = 'thoth-dissemination-publisher-comparison/1'

STATUS_MATCH = 'MATCH'
STATUS_DIFF = 'DIFF'
STATUS_ERROR = 'ERROR'

# How an upstream destination is served by this repository.
CLASSIFICATION_UPLOADER = 'uploader'
CLASSIFICATION_SHARED_ADAPTER = 'shared_adapter'
CLASSIFICATION_MANUAL = 'manual'
CLASSIFICATION_PULL_FEED = 'pull_feed'
CLASSIFICATION_INACTIVE = 'inactive'

# Classifications that have an automated publisher-discovery execution
# pathway in this repository. Manual, pull-feed and inactive destinations
# must never become synthetic dissemination pathways.
AUTOMATED_CLASSIFICATIONS = (
    CLASSIFICATION_UPLOADER, CLASSIFICATION_SHARED_ADAPTER)

LINKED_GROUP_OAPEN_DOAB = 'OAPEN_DOAB'

DETAIL_LENGTH_LIMIT = 300
SUMMARY_PUBLISHER_LIMIT = 10


class PublisherSourceConfigurationError(RuntimeError):
    """The publisher-source configuration could not be resolved safely."""


class PublisherDiscoveryError(RuntimeError):
    """Publisher Services assignments could not be discovered or reconciled."""


class LinkedPlatformMismatchError(PublisherDiscoveryError):
    """Linked upstream platforms reported different publisher assignments."""

    def __init__(self, message, platforms, differing_publisher_ids):
        super().__init__(message)
        self.platforms = tuple(platforms)
        self.differing_publisher_ids = tuple(differing_publisher_ids)


class _Destination:
    """One pinned upstream platform and how this repository serves it."""

    def __init__(
            self, api_platform, classification, dissemination_platform=None,
            linked_group=None):
        self.api_platform = api_platform
        self.classification = classification
        self.dissemination_platform = dissemination_platform
        self.linked_group = linked_group

    @property
    def supports_publisher_discovery(self):
        return (
            self.classification in AUTOMATED_CLASSIFICATIONS
            and self.dissemination_platform is not None
        )


# Exhaustive classification of the 17 pinned `DistributionPlatform` values of
# thoth-pub/thoth v1.7.0 (commit 40e9c06d4ab76217c3ef277dd539d3b5580e2bb8).
# The order is the canonical upstream declaration order. There is deliberately
# no wildcard, `OTHER` or nearest-match entry: an upstream platform absent from
# this table is a contract incompatibility and fails closed.
DESTINATIONS = (
    _Destination(
        'INTERNET_ARCHIVE', CLASSIFICATION_UPLOADER, 'InternetArchive'),
    _Destination(
        'OAPEN', CLASSIFICATION_SHARED_ADAPTER, 'OAPEN',
        LINKED_GROUP_OAPEN_DOAB),
    # DOAB shares the single OAPEN/DOAB execution adapter and is never a
    # second upload, so it has no dissemination platform of its own.
    _Destination(
        'DOAB', CLASSIFICATION_SHARED_ADAPTER, None, LINKED_GROUP_OAPEN_DOAB),
    _Destination('SCIENCE_OPEN', CLASSIFICATION_MANUAL, 'ScienceOpen'),
    _Destination(
        'CAMBRIDGE_UNIVERSITY_LIBRARY', CLASSIFICATION_UPLOADER, 'CUL'),
    _Destination('CROSSREF', CLASSIFICATION_UPLOADER, 'Crossref'),
    _Destination('FIGSHARE', CLASSIFICATION_UPLOADER, 'Figshare'),
    _Destination('ZENODO', CLASSIFICATION_UPLOADER, 'Zenodo'),
    _Destination('PROJECT_MUSE', CLASSIFICATION_UPLOADER, 'ProjectMUSE'),
    _Destination('JSTOR', CLASSIFICATION_UPLOADER, 'JSTOR'),
    _Destination('EBSCO_HOST', CLASSIFICATION_UPLOADER, 'EBSCOHost'),
    _Destination(
        'PROQUEST_EBOOK_CENTRAL', CLASSIFICATION_UPLOADER, 'ProQuest'),
    _Destination('GOOGLE_PLAY', CLASSIFICATION_UPLOADER, 'GooglePlay'),
    _Destination('BKCI', CLASSIFICATION_UPLOADER, 'BKCI'),
    _Destination('OCLC_KB', CLASSIFICATION_PULL_FEED),
    _Destination('EX_LIBRIS_KB', CLASSIFICATION_PULL_FEED),
    _Destination('JISC_NBK', CLASSIFICATION_INACTIVE),
)

DESTINATIONS_BY_API_PLATFORM = {
    destination.api_platform: destination for destination in DESTINATIONS
}

# The dissemination-platform vocabulary accepted by this repository, which is
# deliberately distinct from the upstream API platform vocabulary.
DISSEMINATION_PLATFORMS = (
    'InternetArchive',
    'OAPEN',
    'ScienceOpen',
    'CUL',
    'Crossref',
    'Figshare',
    'Zenodo',
    'ProjectMUSE',
    'JSTOR',
    'EBSCOHost',
    'ProQuest',
    'GooglePlay',
    'BKCI',
)

_UUID_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')

_USERINFO_PATTERN = re.compile(r'://[^/\s@]+@')
_CREDENTIAL_PATTERN = re.compile(
    r'(?i)(token|secret|password|passwd|api[_-]?key|access[_-]?key|'
    r'authorization)\s*[=:]\s*\S+')


def sanitise_detail(value, limit=DETAIL_LENGTH_LIMIT):
    """Reduce a diagnostic to bounded, single-line, non-sensitive text."""
    text = _USERINFO_PATTERN.sub('://[redacted]@', str(value))
    text = _CREDENTIAL_PATTERN.sub(
        lambda match: '{}=[redacted]'.format(match.group(1)), text)
    text = ' '.join(text.split())
    if len(text) > limit:
        text = text[:limit] + '...(truncated)'
    return text


def destination_for_api_platform(api_platform):
    """Return the pinned classification of one upstream platform value."""
    destination = DESTINATIONS_BY_API_PLATFORM.get(api_platform)
    if destination is None:
        raise PublisherSourceConfigurationError(
            'Unknown distribution platform {}'.format(api_platform))
    return destination


def api_platforms_for(dissemination_platform):
    """
    Return the upstream platform values that describe one dissemination
    platform, in canonical upstream order.

    OAPEN and DOAB are two upstream platform values projected onto the single
    existing OAPEN/DOAB execution adapter, so both are returned for OAPEN.
    """
    destination = None
    for candidate in DESTINATIONS:
        if (candidate.dissemination_platform is not None
                and candidate.dissemination_platform
                == dissemination_platform):
            destination = candidate
            break
    if destination is None:
        raise PublisherSourceConfigurationError(
            'Dissemination platform {} has no distribution platform '
            'mapping'.format(dissemination_platform))
    if not destination.supports_publisher_discovery:
        raise PublisherSourceConfigurationError(
            'Dissemination platform {} is classified {} and has no automated '
            'publisher-discovery pathway'.format(
                dissemination_platform, destination.classification))
    if destination.linked_group is None:
        return (destination.api_platform,)
    return tuple(
        candidate.api_platform for candidate in DESTINATIONS
        if candidate.linked_group == destination.linked_group
    )


def supports_publisher_discovery(dissemination_platform):
    """Whether a dissemination platform may use `compare` or `api`."""
    try:
        api_platforms_for(dissemination_platform)
    except PublisherSourceConfigurationError:
        return False
    return True


def parse_source_modes(raw_value):
    """
    Parse the repository-level `PUBLISHER_SOURCE_MODES` value.

    A missing or empty value means every pathway keeps legacy `env`
    behaviour. Anything else must be a JSON object keyed by this
    repository's dissemination-platform names whose values are exactly
    `env`, `compare` or `api`.
    """
    if raw_value is None or not str(raw_value).strip():
        return {}

    try:
        modes = json.loads(raw_value)
    except (TypeError, ValueError) as error:
        raise PublisherSourceConfigurationError(
            '{} must be a JSON object: {}'.format(
                PUBLISHER_SOURCE_MODES_VARIABLE, sanitise_detail(error))
        ) from error

    if not isinstance(modes, dict):
        raise PublisherSourceConfigurationError(
            '{} must be a JSON object'.format(
                PUBLISHER_SOURCE_MODES_VARIABLE))

    for platform, mode in modes.items():
        if platform in WILDCARD_MODE_KEYS:
            raise PublisherSourceConfigurationError(
                '{} must not contain the wildcard key {!r}'.format(
                    PUBLISHER_SOURCE_MODES_VARIABLE, platform))
        if platform not in DISSEMINATION_PLATFORMS:
            raise PublisherSourceConfigurationError(
                '{} contains unknown dissemination platform {!r}'.format(
                    PUBLISHER_SOURCE_MODES_VARIABLE, platform))
        if not isinstance(mode, str):
            raise PublisherSourceConfigurationError(
                '{} value for {} must be a string'.format(
                    PUBLISHER_SOURCE_MODES_VARIABLE, platform))
        if mode not in SOURCE_MODES:
            raise PublisherSourceConfigurationError(
                '{} value for {} must be one of {}'.format(
                    PUBLISHER_SOURCE_MODES_VARIABLE, platform,
                    ', '.join(SOURCE_MODES)))
    return modes


def resolve_source_mode(dissemination_platform, raw_value=None):
    """
    Resolve the publisher-source mode for one dissemination platform.

    Missing configuration, an empty configuration and a missing platform key
    all resolve to `env`. A non-legacy mode is accepted only for a platform
    with an automated publisher-discovery pathway.
    """
    if raw_value is None:
        raw_value = environ.get(PUBLISHER_SOURCE_MODES_VARIABLE)
    modes = parse_source_modes(raw_value)
    mode = modes.get(dissemination_platform, MODE_ENV)
    if mode != MODE_ENV:
        # Raises for manual-only, pull-feed, inactive and unmapped platforms.
        api_platforms_for(dissemination_platform)
    return mode


def discover_platform_publisher_sets(
        thoth, dissemination_platform,
        page_size=DISTRIBUTION_PLATFORM_PAGE_SIZE):
    """
    Return the reconciled publisher set of each mapped upstream platform.

    Each set is completely paged, count-reconciled and normalized by the
    Thoth API helper. Every failure is raised as a discovery error rather
    than reported as a smaller publisher set.
    """
    publisher_sets = {}
    for api_platform in api_platforms_for(dissemination_platform):
        try:
            publisher_sets[api_platform] = (
                get_distribution_platform_publisher_ids(
                    thoth, api_platform, page_size=page_size))
        except Exception as error:
            raise PublisherDiscoveryError(
                'Publisher discovery failed for {}: {}: {}'.format(
                    api_platform, type(error).__name__,
                    sanitise_detail(error))
            ) from error
    return publisher_sets


def reconcile_linked_publisher_sets(dissemination_platform, publisher_sets):
    """
    Project the mapped upstream platforms onto one publisher set.

    Linked platforms - currently OAPEN and DOAB - are queried and reconciled
    independently and must agree exactly. A disagreement is an invariant
    violation, never a silent union or intersection.
    """
    api_platforms = api_platforms_for(dissemination_platform)
    missing = [
        api_platform for api_platform in api_platforms
        if api_platform not in publisher_sets
    ]
    if missing:
        raise PublisherDiscoveryError(
            'Publisher assignments are missing for {}'.format(
                ', '.join(sorted(missing))))

    resolved = sorted(set(publisher_sets[api_platforms[0]]))
    for api_platform in api_platforms[1:]:
        other = sorted(set(publisher_sets[api_platform]))
        if other != resolved:
            differing = sorted(
                set(resolved).symmetric_difference(other))
            raise LinkedPlatformMismatchError(
                'Linked platforms {} reported different publisher '
                'assignments'.format(', '.join(sorted(api_platforms))),
                sorted(api_platforms),
                differing,
            )
    return resolved


def discover_api_publisher_ids(
        thoth, dissemination_platform,
        page_size=DISTRIBUTION_PLATFORM_PAGE_SIZE):
    """Return the authoritative API publisher set for one platform."""
    publisher_sets = discover_platform_publisher_sets(
        thoth, dissemination_platform, page_size=page_size)
    return reconcile_linked_publisher_sets(
        dissemination_platform, publisher_sets)


def normalise_publisher_ids(publisher_ids):
    """
    Normalize configured publisher IDs for comparison only.

    Legacy publisher semantics are never altered by this function: it
    returns a sorted, deduplicated, lower-cased view plus any value that
    could not be read as a UUID.
    """
    normalised = set()
    invalid = []
    for publisher_id in publisher_ids or []:
        candidate = str(publisher_id).strip().lower()
        if _UUID_PATTERN.match(candidate):
            normalised.add(candidate)
        else:
            invalid.append(str(publisher_id))
    return sorted(normalised), sorted(set(invalid))


def _empty_comparison_report(mode, dissemination_platform):
    return {
        'schemaVersion': COMPARISON_SCHEMA_VERSION,
        'mode': mode,
        'disseminationPlatform': dissemination_platform,
        'apiPlatforms': [],
        'destinationClassifications': {},
        'apiPlatformPublisherCounts': {},
        'legacyPublisherIds': [],
        'apiPublisherIds': [],
        'legacyOnly': [],
        'apiOnly': [],
        'linkedPlatformIssues': [],
        'contractIssues': [],
        'status': STATUS_ERROR,
    }


def build_comparison_report(
        thoth, dissemination_platform, legacy_publisher_ids,
        mode=MODE_COMPARE, page_size=DISTRIBUTION_PLATFORM_PAGE_SIZE):
    """
    Build the deterministic publisher comparison report.

    The legacy publisher set remains authoritative for work selection: this
    function is observational, never raises, and never alters the caller's
    selection. Publisher arrays are sorted lexicographically, keys are
    stable, and no timestamp participates in the canonical content.
    """
    report = _empty_comparison_report(mode, dissemination_platform)
    contract_issues = []
    linked_issues = []

    legacy_publisher_ids, invalid_legacy = normalise_publisher_ids(
        legacy_publisher_ids)
    report['legacyPublisherIds'] = legacy_publisher_ids
    if invalid_legacy:
        contract_issues.append({
            'issue': 'legacy_publisher_id_not_a_uuid',
            'platform': dissemination_platform,
            'detail': sanitise_detail(
                'unusable configured publisher IDs: {}'.format(
                    ', '.join(invalid_legacy))),
        })

    api_platforms = ()
    try:
        api_platforms = api_platforms_for(dissemination_platform)
    except PublisherSourceConfigurationError as error:
        contract_issues.append({
            'issue': 'unsupported_dissemination_platform',
            'platform': dissemination_platform,
            'detail': sanitise_detail(error),
        })

    report['apiPlatforms'] = list(api_platforms)
    report['destinationClassifications'] = {
        api_platform: destination_for_api_platform(api_platform).classification
        for api_platform in api_platforms
    }

    api_publisher_ids = []
    if api_platforms:
        publisher_sets = {}
        try:
            publisher_sets = discover_platform_publisher_sets(
                thoth, dissemination_platform, page_size=page_size)
            api_publisher_ids = reconcile_linked_publisher_sets(
                dissemination_platform, publisher_sets)
        except LinkedPlatformMismatchError as error:
            linked_issues.append({
                'issue': 'linked_platform_publisher_set_mismatch',
                'platforms': list(error.platforms),
                'publisherIds': list(error.differing_publisher_ids),
            })
        except Exception as error:
            contract_issues.append({
                'issue': 'api_discovery_failed',
                'platform': dissemination_platform,
                'detail': sanitise_detail(error),
            })
        report['apiPlatformPublisherCounts'] = {
            api_platform: len(publisher_sets[api_platform])
            for api_platform in sorted(publisher_sets)
        }

    report['contractIssues'] = sorted(
        contract_issues, key=lambda issue: json.dumps(issue, sort_keys=True))
    report['linkedPlatformIssues'] = sorted(
        linked_issues, key=lambda issue: json.dumps(issue, sort_keys=True))

    if report['contractIssues'] or report['linkedPlatformIssues']:
        # An error is never presented as a reconciled difference, and never
        # counts as clean comparison evidence.
        report['status'] = STATUS_ERROR
        return report

    report['apiPublisherIds'] = list(api_publisher_ids)
    report['legacyOnly'] = sorted(
        set(legacy_publisher_ids).difference(api_publisher_ids))
    report['apiOnly'] = sorted(
        set(api_publisher_ids).difference(legacy_publisher_ids))
    report['status'] = (
        STATUS_MATCH
        if not report['legacyOnly'] and not report['apiOnly']
        else STATUS_DIFF
    )
    return report


def comparison_report_error(dissemination_platform, error, mode=MODE_COMPARE):
    """Build an `ERROR` report for a failure outside publisher discovery."""
    report = _empty_comparison_report(mode, dissemination_platform)
    report['contractIssues'] = [{
        'issue': 'comparison_failed',
        'platform': dissemination_platform,
        'detail': sanitise_detail(
            '{}: {}'.format(type(error).__name__, error)),
    }]
    return report


def serialise_comparison_report(report):
    """Render the canonical comparison report deterministically."""
    return json.dumps(
        report, indent=2, sort_keys=True, ensure_ascii=True) + '\n'


def write_comparison_report(report_path, report):
    """Write the canonical comparison report to its own file."""
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(serialise_comparison_report(report), encoding='utf-8')
    return path


def summarise_comparison(report):
    """Render one concise, sanitized, human-readable comparison summary."""
    lines = [
        '- Status: `{}`'.format(report.get('status')),
        '- Mode: `{}`'.format(report.get('mode')),
        '- API platforms: `{}`'.format(
            ', '.join(report.get('apiPlatforms') or []) or 'none'),
        '- Legacy publishers: `{}`'.format(
            len(report.get('legacyPublisherIds') or [])),
        '- API publishers: `{}`'.format(
            len(report.get('apiPublisherIds') or [])),
        '- Legacy-only publishers: `{}`'.format(
            len(report.get('legacyOnly') or [])),
        '- API-only publishers: `{}`'.format(
            len(report.get('apiOnly') or [])),
    ]
    for key, label in (
            ('legacyOnly', 'Legacy-only'), ('apiOnly', 'API-only')):
        publisher_ids = report.get(key) or []
        if publisher_ids:
            shown = publisher_ids[:SUMMARY_PUBLISHER_LIMIT]
            suffix = (
                ' (+{} more)'.format(len(publisher_ids) - len(shown))
                if len(publisher_ids) > len(shown) else '')
            lines.append('- {}: {}{}'.format(
                label,
                ', '.join('`{}`'.format(value) for value in shown),
                suffix,
            ))
    for issue in report.get('linkedPlatformIssues') or []:
        lines.append('- Linked platform issue: `{}` ({})'.format(
            issue.get('issue'), ', '.join(issue.get('platforms') or [])))
    for issue in report.get('contractIssues') or []:
        lines.append('- Contract issue: `{}`: {}'.format(
            issue.get('issue'), issue.get('detail')))
    return '\n'.join(lines)


def _write_step_summary(text):
    """Append to the workflow step summary, or fall back to stdout."""
    summary_path = environ.get('GITHUB_STEP_SUMMARY')
    if summary_path:
        with open(summary_path, 'a', encoding='utf-8') as summary_file:
            summary_file.write(text + '\n')
    else:
        print(text)


def _summary_command(args):
    """
    Render publisher-source evidence for one workflow run.

    This is reporting only. It never fails a dissemination selection that has
    already succeeded, so it always returns zero.
    """
    heading = '### Publisher source ({})'.format(args.platform)
    try:
        mode = resolve_source_mode(
            args.platform, environ.get(PUBLISHER_SOURCE_MODES_VARIABLE))
    except PublisherSourceConfigurationError as error:
        _write_step_summary('{}\n\n- Mode: `unresolved`\n- {}'.format(
            heading, sanitise_detail(error)))
        return 0

    if mode == MODE_ENV:
        # Legacy-authoritative runs produce no comparison evidence.
        return 0

    if mode == MODE_API:
        _write_step_summary(
            '{}\n\n- Mode: `api`\n- Publisher discovery was '
            'API-authoritative.'.format(heading))
        return 0

    report_path = Path(args.report)
    try:
        report = json.loads(report_path.read_text(encoding='utf-8'))
        if not isinstance(report, dict):
            raise ValueError('comparison report was not an object')
    except Exception as error:
        _write_step_summary(
            '{}\n\n- Mode: `compare`\n- Comparison evidence: '
            '`UNAVAILABLE`\n- {}'.format(heading, sanitise_detail(error)))
        return 0

    _write_step_summary('{}\n\n{}'.format(heading, summarise_comparison(report)))
    return 0


def get_arguments(argv=None):
    """Parse the reporting-only command-line interface."""
    parser = argparse.ArgumentParser(
        description='Publisher-source reporting helper (no dissemination).')
    subparsers = parser.add_subparsers(dest='command', required=True)
    summary = subparsers.add_parser(
        'summary', help='render publisher-source evidence for a workflow run')
    summary.add_argument('--platform', required=True)
    summary.add_argument('--report', required=True)
    return parser.parse_args(argv)


def main(argv=None):
    """Run the reporting-only command line and return a process status."""
    args = get_arguments(argv)
    if args.command == 'summary':
        return _summary_command(args)
    return 1


if __name__ == '__main__':
    sys.exit(main())
