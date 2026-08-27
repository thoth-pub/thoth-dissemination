#!/usr/bin/env python3
"""
Per-work Crossref runner for the DIS-02A durable-job worker.

The parent worker must not call legacy uploader code in-process: that code
still contains `SystemExit` paths and assumes one short-lived CLI process per
work. This module is that isolation boundary. It deposits exactly one work to
Crossref, on behalf of exactly one expected publisher, and reports a single
bounded structured result on stdout.

Crossref is fixed. There is deliberately no `--platform` argument, so no other
adapter can be reached through this entrypoint.
"""

import argparse
import json
import logging
import sys
from uuid import UUID

from crossrefuploader import (
    CrossrefCredentialsMissingError,
    CrossrefDepositIndeterminateError,
    CrossrefDepositRejectedError,
    CrossrefMetadataError,
    CrossrefPrefixInvalidError,
    CrossrefPrefixLookupError,
    CrossrefUploader,
)
from errors import DisseminationError
from version import __version__


DEFAULT_EXPORT_URL = 'https://export.thoth.pub'

SCHEMA_VERSION = 1
STATUS_ACCEPTED = 'ACCEPTED'
STATUS_FAILED = 'FAILED'
STATUS_INDETERMINATE = 'INDETERMINATE'

# Classifications shared with the parent worker's closed taxonomy.
CATALOGUE_PUBLISHER_MISMATCH = 'CATALOGUE_PUBLISHER_MISMATCH'
CROSSREF_CREDENTIAL_MISSING = 'CROSSREF_CREDENTIAL_MISSING'
CROSSREF_METADATA_INVALID = 'CROSSREF_METADATA_INVALID'
CROSSREF_EXPORT_FAILED = 'CROSSREF_EXPORT_FAILED'
CROSSREF_PREFIX_INVALID = 'CROSSREF_PREFIX_INVALID'
CROSSREF_PREFIX_LOOKUP_FAILED = 'CROSSREF_PREFIX_LOOKUP_FAILED'
CROSSREF_DEPOSIT_REJECTED = 'CROSSREF_DEPOSIT_REJECTED'
CROSSREF_DEPOSIT_INDETERMINATE = 'CROSSREF_DEPOSIT_INDETERMINATE'
INTERNAL_WORKER_ERROR = 'INTERNAL_WORKER_ERROR'

MAX_DETAIL_CHARS = 512

# Every deposit-phase failure whose provider phase the runner can prove.
DEPOSIT_PHASE_FAILURES = (
    (CrossrefCredentialsMissingError, CROSSREF_CREDENTIAL_MISSING, False),
    (CrossrefMetadataError, CROSSREF_METADATA_INVALID, False),
    (CrossrefPrefixInvalidError, CROSSREF_PREFIX_INVALID, False),
    (CrossrefPrefixLookupError, CROSSREF_PREFIX_LOOKUP_FAILED, False),
    (CrossrefDepositRejectedError, CROSSREF_DEPOSIT_REJECTED, True),
    (CrossrefDepositIndeterminateError, CROSSREF_DEPOSIT_INDETERMINATE, True),
)


def bounded_detail(detail):
    """Return a bounded single-line diagnostic safe for durable storage."""
    if not isinstance(detail, str):
        return None
    collapsed = ' '.join(detail.split())
    return collapsed[:MAX_DETAIL_CHARS] if collapsed else None


def build_result(status, code=None, detail=None, external_write_started=False):
    """Build the versioned structured result the parent worker consumes."""
    return {
        'schemaVersion': SCHEMA_VERSION,
        'status': status,
        'code': code,
        'detail': bounded_detail(detail),
        'externalWriteStarted': bool(external_write_started),
    }


def canonical_uuid(value):
    """Return `value` when it is already a canonical UUID string, else None."""
    if not isinstance(value, str):
        return None
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError, TypeError):
        return None
    return value if str(parsed) == value else None


def run(work_id, expected_publisher_id, uploader_factory=None):
    """Deposit one work to Crossref and return one structured result.

    The publisher is proven twice against the claim: the parent proved it from
    the catalogue row, and this runner proves it again from the metadata the
    uploader actually loaded, immediately before the deposit path. There is no
    cross-publisher credential fallback.
    """
    if canonical_uuid(work_id) is None or canonical_uuid(
            expected_publisher_id) is None:
        return build_result(
            STATUS_FAILED, INTERNAL_WORKER_ERROR,
            'Runner arguments were not canonical UUIDs; nothing was submitted')

    factory = uploader_factory or _default_uploader_factory

    # Loading metadata is provably before any provider write. The legacy path
    # can still call `sys.exit`, which must not escape this boundary.
    try:
        uploader = factory(work_id)
    except SystemExit:
        return build_result(
            STATUS_FAILED, CROSSREF_METADATA_INVALID,
            'Work metadata could not be loaded from Thoth; nothing was '
            'submitted to Crossref')
    except DisseminationError as error:
        return build_result(
            STATUS_FAILED, CROSSREF_EXPORT_FAILED,
            'Work metadata could not be loaded: {}'.format(
                type(error).__name__))
    except Exception as error:
        return build_result(
            STATUS_FAILED, INTERNAL_WORKER_ERROR,
            'Work metadata could not be loaded: {}'.format(
                type(error).__name__))

    try:
        actual_publisher_id = uploader.get_publisher_id()
    except Exception as error:
        return build_result(
            STATUS_FAILED, CROSSREF_METADATA_INVALID,
            'Work metadata carried no readable publisher identity: {}'.format(
                type(error).__name__))

    if actual_publisher_id != expected_publisher_id:
        return build_result(
            STATUS_FAILED, CATALOGUE_PUBLISHER_MISMATCH,
            'Loaded work belongs to a publisher other than the claimed one; '
            'nothing was submitted to Crossref')

    try:
        work = uploader.metadata.get('data', {}).get('work') or {}
        doi = work.get('doi')
    except AttributeError:
        doi = None
    if not isinstance(doi, str) or len(doi.strip()) < 1:
        return build_result(
            STATUS_FAILED, CROSSREF_METADATA_INVALID,
            'Work has no usable root DOI; nothing was submitted to Crossref')

    return _deposit(uploader)


def _deposit(uploader):
    """Run the Crossref deposit path and classify exactly one outcome."""
    try:
        uploader.upload_to_platform()
    except DisseminationError as error:
        for error_type, code, external in DEPOSIT_PHASE_FAILURES:
            if isinstance(error, error_type):
                status = (STATUS_INDETERMINATE
                          if code == CROSSREF_DEPOSIT_INDETERMINATE
                          else STATUS_FAILED)
                return build_result(status, code, str(error), external)
        # A generic dissemination error on this path is the export/metadata
        # retrieval failing before the deposit request is built.
        return build_result(
            STATUS_FAILED, CROSSREF_EXPORT_FAILED,
            'Pre-deposit Crossref operation failed: {}'.format(
                type(error).__name__))
    except SystemExit:
        # A legacy exit inside the deposit path proves nothing about how far
        # it got, so the outcome is unprovable rather than assumed pre-write.
        return build_result(
            STATUS_INDETERMINATE, CROSSREF_DEPOSIT_INDETERMINATE,
            'Crossref deposit path exited without a classified result, so the '
            'deposit outcome is unprovable', True)
    except Exception as error:
        return build_result(
            STATUS_INDETERMINATE, CROSSREF_DEPOSIT_INDETERMINATE,
            'Crossref deposit path raised an unexpected {}, so the deposit '
            'outcome is unprovable'.format(type(error).__name__), True)

    return build_result(STATUS_ACCEPTED, external_write_started=True)


def _default_uploader_factory(work_id):
    """Instantiate the existing Crossref execution path for one work."""
    return CrossrefUploader(work_id, DEFAULT_EXPORT_URL, None, __version__)


def get_arguments(argv=None):
    """Parse the runner's deliberately minimal argument set."""
    parser = argparse.ArgumentParser(
        description='Deposit one Thoth work to Crossref for the '
                    'distribution job worker')
    parser.add_argument('--work', dest='work_id', required=True,
                        help='Thoth Work ID of the work to deposit')
    parser.add_argument('--expected-publisher-id', dest='expected_publisher_id',
                        required=True,
                        help='Thoth Publisher ID the work must belong to')
    return parser.parse_args(argv)


def main(argv=None):
    """Emit exactly one structured result on stdout and exit accordingly."""
    arguments = get_arguments(argv)
    result = run(arguments.work_id, arguments.expected_publisher_id)
    # Operational logs go to stderr; stdout carries the result and nothing else.
    print(json.dumps(result, sort_keys=True))
    return 0 if result['status'] == STATUS_ACCEPTED else 1


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format='%(levelname)s:%(asctime)s: %(message)s')
    # DEBUG level urllib3 logs may contain sensitive information such as
    # passwords sent as URL query parameters, and must never be emitted.
    logging.getLogger("urllib3").setLevel(logging.INFO)
    sys.exit(main())
