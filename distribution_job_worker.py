#!/usr/bin/env python3
"""
DIS-02A durable-job worker substrate.

Claims at most one BE-04 `PUBLISHER_BACK_CATALOGUE` job per invocation and
executes it against Crossref, the only target this stage permits. Thoth
remains the sole durable owner of job, attempt, retry, lease and claim-token
state: this worker keeps no durable state of its own, renews no lease and
never replays external work to discover whether a transition committed.
"""

import json
import logging
import os.path
import subprocess
import sys
from datetime import datetime, timezone
from os import environ
from uuid import UUID

import thothapi


# The worker is inert unless this repository Actions variable is set to the
# exact ASCII string below. The comparison is case-sensitive and performs no
# trimming: a GitHub Actions `if:` expression may additionally skip the job,
# but it is not the authority for these semantics, so the guard lives here.
WORKER_ENABLED_VARIABLE = 'DISTRIBUTION_JOB_WORKER_ENABLED'
WORKER_ENABLED_VALUE = 'ON'


# The only durable job kind and the only executable target of this stage.
SUPPORTED_JOB_KIND = 'PUBLISHER_BACK_CATALOGUE'
SUPPORTED_TARGET = 'CROSSREF'

# BE-04 clamps these rather than rejecting them, but the worker always asks for
# exactly one job and the full 3600-second lease it budgets against.
CLAIM_LIMIT = 1
CLAIM_LEASE_SECONDS = 3600

# BE-04 attempt results, used only to interpret the bounded attempt history.
ATTEMPT_RESULT_ABANDONED = 'ABANDONED'
ATTEMPT_RESULTS = frozenset(
    {'SUCCEEDED', 'FAILED', 'CANCELLED', ATTEMPT_RESULT_ABANDONED})

# Closed DIS-02A failure taxonomy. `retryable` is decided here, from the code
# alone, and never from anything a runner subprocess asserts about itself.
UNSUPPORTED_JOB_KIND = 'UNSUPPORTED_JOB_KIND'
UNSUPPORTED_TARGET_SET = 'UNSUPPORTED_TARGET_SET'
CATALOGUE_QUERY_FAILED = 'CATALOGUE_QUERY_FAILED'
CATALOGUE_CONTRACT_INVALID = 'CATALOGUE_CONTRACT_INVALID'
CATALOGUE_PUBLISHER_MISMATCH = 'CATALOGUE_PUBLISHER_MISMATCH'
CATALOGUE_TOO_LARGE = 'CATALOGUE_TOO_LARGE'
CROSSREF_CREDENTIAL_MISSING = 'CROSSREF_CREDENTIAL_MISSING'
CROSSREF_METADATA_INVALID = 'CROSSREF_METADATA_INVALID'
CROSSREF_EXPORT_FAILED = 'CROSSREF_EXPORT_FAILED'
CROSSREF_PREFIX_INVALID = 'CROSSREF_PREFIX_INVALID'
CROSSREF_PREFIX_LOOKUP_FAILED = 'CROSSREF_PREFIX_LOOKUP_FAILED'
CROSSREF_DEPOSIT_REJECTED = 'CROSSREF_DEPOSIT_REJECTED'
CROSSREF_DEPOSIT_INDETERMINATE = 'CROSSREF_DEPOSIT_INDETERMINATE'
CROSSREF_RUNNER_FAILED = 'CROSSREF_RUNNER_FAILED'
LEASE_BUDGET_EXHAUSTED = 'LEASE_BUDGET_EXHAUSTED'
INTERNAL_WORKER_ERROR = 'INTERNAL_WORKER_ERROR'

# The only codes that may ever be reported as retryable. Each names a
# condition the worker can prove happened before the relevant provider write.
RETRYABLE_CODES = frozenset({
    CATALOGUE_QUERY_FAILED,
    CROSSREF_EXPORT_FAILED,
    CROSSREF_PREFIX_LOOKUP_FAILED,
    INTERNAL_WORKER_ERROR,
})

WORKER_ERROR_CODES = frozenset({
    UNSUPPORTED_JOB_KIND, UNSUPPORTED_TARGET_SET, CATALOGUE_QUERY_FAILED,
    CATALOGUE_CONTRACT_INVALID, CATALOGUE_PUBLISHER_MISMATCH,
    CATALOGUE_TOO_LARGE, CROSSREF_CREDENTIAL_MISSING,
    CROSSREF_METADATA_INVALID, CROSSREF_EXPORT_FAILED,
    CROSSREF_PREFIX_INVALID, CROSSREF_PREFIX_LOOKUP_FAILED,
    CROSSREF_DEPOSIT_REJECTED, CROSSREF_DEPOSIT_INDETERMINATE,
    CROSSREF_RUNNER_FAILED, LEASE_BUDGET_EXHAUSTED, INTERNAL_WORKER_ERROR,
})

# The pilot fence. The raw ACTIVE/FORTHCOMING book count is the pre-write
# bound, not the executable count: a publisher with more raw candidates is out
# of scope for this stage regardless of how many of them are depositable.
MAX_PILOT_CANDIDATES = 10
CATALOGUE_PAGE_SIZE = 10
CATALOGUE_WORK_STATUSES = ['ACTIVE', 'FORTHCOMING']

# Every executable work runs in its own subprocess under this hard deadline.
RUNNER_DEADLINE_SECONDS = 240

# No runner starts unless the remaining lease covers a whole runner deadline
# plus a safety margin for result reporting.
LEASE_SAFETY_MARGIN_SECONDS = 300
LEASE_REQUIRED_SECONDS = LEASE_SAFETY_MARGIN_SECONDS + RUNNER_DEADLINE_SECONDS

# Result reporting may repeat only the identical mutation, and only because
# the previous attempt produced no usable response. No provider action ever
# happens between these attempts.
RESULT_REPORT_MAX_ATTEMPTS = 3

# Pinned Thoth v1.7.0 terminal-success contract. Recognition is exact: a
# changed message or extension type is contract drift requiring review, never
# something to match heuristically.
TERMINAL_ERROR_TYPE = 'DISTRIBUTION_JOB_TERMINAL'
TERMINAL_SUCCEEDED_MESSAGE = (
    'The distribution job is already in the terminal state SUCCEEDED.')

# BE-04 truncates `errorDetail` at 2048 characters; the worker never relies on
# that and bounds its own sanitised detail well below it.
MAX_ERROR_DETAIL_CHARS = 512

# Invocation outcomes. These describe what this process did, not durable state.
OUTCOME_DISABLED = 'DISABLED'
OUTCOME_NO_JOB = 'NO_JOB'
OUTCOME_COMPLETED = 'COMPLETED'
OUTCOME_FAILED = 'FAILED'
OUTCOME_HOLD = 'HOLD'

CLAIM_DISTRIBUTION_JOBS_MUTATION = """
mutation ClaimDistributionJobs($data: ClaimDistributionJobsInput!) {
  claimDistributionJobs(data: $data) {
    claimToken
    leaseExpiresAt
    attemptNumber
    job {
      distributionJobId
      kind
      publisherId
      targets {
        platform
      }
      attempts {
        attemptNumber
        result
      }
    }
  }
}
"""

COMPLETE_DISTRIBUTION_JOB_MUTATION = """
mutation CompleteDistributionJob($data: CompleteDistributionJobInput!) {
  completeDistributionJob(data: $data) {
    distributionJobId
    status
  }
}
"""

FAIL_DISTRIBUTION_JOB_MUTATION = """
mutation FailDistributionJob($data: FailDistributionJobInput!) {
  failDistributionJob(data: $data) {
    distributionJobId
    status
  }
}
"""


BACK_CATALOGUE_BOOK_COUNT_QUERY = """
query BackCatalogueBookCount($publishers: [Uuid!]!, $workStatuses: [WorkStatus!]!) {
  bookCount(publishers: $publishers, workStatuses: $workStatuses)
}
"""

BACK_CATALOGUE_BOOKS_QUERY = """
query BackCatalogueBooks(
  $limit: Int!
  $offset: Int!
  $publishers: [Uuid!]!
  $workStatuses: [WorkStatus!]!
) {
  books(
    limit: $limit
    offset: $offset
    publishers: $publishers
    workStatuses: $workStatuses
    order: {field: WORK_ID, direction: ASC}
  ) {
    workId
    workStatus
    doi
    publicationDate
    imprint {
      publisher {
        publisherId
      }
    }
  }
}
"""


# The versioned runner result contract. A runner speaking any other version
# is not understood and is therefore not trusted.
RUNNER_SCRIPT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'crossref_worker_runner.py')

RUNNER_SCHEMA_VERSION = 1
RUNNER_STATUS_ACCEPTED = 'ACCEPTED'
RUNNER_STATUS_FAILED = 'FAILED'
RUNNER_STATUS_INDETERMINATE = 'INDETERMINATE'
RUNNER_STATUSES = frozenset({
    RUNNER_STATUS_ACCEPTED, RUNNER_STATUS_FAILED, RUNNER_STATUS_INDETERMINATE})


class RunnerDeadlineExceeded(Exception):
    """The per-work runner did not terminate inside its hard deadline."""


def launch_crossref_runner(work_id, expected_publisher_id, deadline_seconds):
    """Run one work in an isolated subprocess and return its parsed result.

    The legacy adapter is never called in-process: it still contains
    `SystemExit` paths and assumes one short-lived CLI process per work.
    Returning `None` for output that cannot be parsed keeps the "untrustworthy
    result" decision with the caller rather than guessing an outcome here.
    """
    try:
        completed = subprocess.run(
            [sys.executable, RUNNER_SCRIPT,
             '--work', work_id,
             '--expected-publisher-id', expected_publisher_id],
            capture_output=True,
            text=True,
            timeout=deadline_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise RunnerDeadlineExceeded(
            'Runner exceeded its deadline') from None

    # stderr carries the runner's operational logging only; stdout carries the
    # single structured result and nothing else.
    try:
        parsed = json.loads(completed.stdout)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


class WorkerFailure(Exception):
    """A classified durable-job failure, ready to report to BE-04.

    `retryable` is derived from the closed taxonomy rather than supplied, so
    no call site can accidentally mark a post-write outcome retryable.
    """

    def __init__(self, code, detail):
        if code not in WORKER_ERROR_CODES:
            raise ValueError('unknown worker error code')
        self.code = code
        self.detail = bounded_detail(detail)
        self.retryable = code in RETRYABLE_CODES
        super().__init__('{}: {}'.format(code, self.detail))


class WorkerResult():
    """What one invocation did, for logging and process exit status."""

    def __init__(self, outcome, error_code=None, retryable=None, detail=None,
                 accepted_works=()):
        self.outcome = outcome
        self.error_code = error_code
        self.retryable = retryable
        self.detail = detail
        self.accepted_works = tuple(accepted_works)


def bounded_detail(detail):
    """Return a bounded single-line diagnostic safe for durable storage."""
    if not isinstance(detail, str):
        return ''
    collapsed = ' '.join(detail.split())
    return collapsed[:MAX_ERROR_DETAIL_CHARS]


def parse_timestamp(value):
    """Parse a Thoth RFC 3339 timestamp into an aware UTC datetime."""
    if not isinstance(value, str):
        raise ValueError('timestamp was not a string')
    normalised = value.strip()
    if normalised.endswith('Z'):
        normalised = normalised[:-1] + '+00:00'
    parsed = datetime.fromisoformat(normalised)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def canonical_uuid(value):
    """Return `value` when it is already a canonical UUID string, else None."""
    if not isinstance(value, str):
        return None
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError, TypeError):
        return None
    return value if str(parsed) == value else None


def worker_enabled(env):
    """Return whether the activation variable is exactly ASCII `ON`."""
    value = env.get(WORKER_ENABLED_VARIABLE)
    return isinstance(value, str) and value == WORKER_ENABLED_VALUE


class DistributionJobWorker():
    """Claims and executes at most one Crossref back-catalogue job."""

    def __init__(self, transport, runner, clock):
        self._transport = transport
        self._runner = runner
        self._clock = clock

    def run_once(self):
        """Claim one job, execute it, and report exactly one durable result."""
        claim = self._claim_one_job()
        if claim is None:
            logging.info('No distribution job was available to claim')
            return WorkerResult(OUTCOME_NO_JOB)

        job_id, claim_token = self._claim_identity(claim)
        try:
            return self._execute_claim(claim, job_id, claim_token)
        except WorkerFailure as failure:
            return self._report_failure(job_id, claim_token, failure)

    def _claim_one_job(self):
        """Ask BE-04 for exactly one job of the only supported kind."""
        data = self._transport.execute(
            CLAIM_DISTRIBUTION_JOBS_MUTATION,
            {'data': {
                'limit': CLAIM_LIMIT,
                'leaseSeconds': CLAIM_LEASE_SECONDS,
                'kinds': [SUPPORTED_JOB_KIND],
            }},
        )
        claims = data.get('claimDistributionJobs')
        if not isinstance(claims, list):
            raise thothapi.ThothWorkerTransportError(
                'Thoth returned a malformed claim list')
        if not claims:
            return None
        # At most one job is processed per invocation even if BE-04 ever
        # returned more than the requested limit.
        return claims[0]

    @staticmethod
    def _claim_identity(claim):
        """Extract the job ID and claim token, or fail the invocation.

        A claim whose identity cannot be trusted cannot be reported against,
        so this is an invocation failure rather than a fabricated job error.
        """
        if not isinstance(claim, dict):
            raise thothapi.ThothWorkerTransportError(
                'Thoth returned a malformed claim')
        job = claim.get('job')
        if not isinstance(job, dict):
            raise thothapi.ThothWorkerTransportError(
                'Thoth returned a claim with no job')
        job_id = canonical_uuid(job.get('distributionJobId'))
        claim_token = canonical_uuid(claim.get('claimToken'))
        if job_id is None or claim_token is None:
            raise thothapi.ThothWorkerTransportError(
                'Thoth returned a claim without a usable identity')
        return job_id, claim_token

    def _execute_claim(self, claim, job_id, claim_token):
        """Validate, execute and complete one claimed job."""
        job = claim['job']
        publisher_id = self._validate_supported_job(job)
        # The fence is evaluated immediately after claim validation and before
        # any catalogue query, runner launch or provider write.
        self._fence_abandoned_predecessor(claim, job)
        logging.info('Claimed distribution job %s for publisher %s',
                     job_id, publisher_id)

        candidates = self._select_candidates(publisher_id)
        executable = self._executable_work_ids(candidates)
        logging.info('Job %s has %d candidate works, %d executable',
                     job_id, len(candidates), len(executable))

        accepted = self._execute_works(claim, publisher_id, executable)
        return self._complete(job_id, claim_token, accepted)

    @staticmethod
    def _validate_supported_job(job):
        """Prove the claim is exactly one Crossref back-catalogue job."""
        if job.get('kind') != SUPPORTED_JOB_KIND:
            raise WorkerFailure(
                UNSUPPORTED_JOB_KIND,
                'Claimed job kind is not the supported back-catalogue kind')

        publisher_id = canonical_uuid(job.get('publisherId'))
        if publisher_id is None:
            raise WorkerFailure(
                INTERNAL_WORKER_ERROR,
                'Claimed job carried no canonical publisher identity; no '
                'catalogue or provider work was attempted')

        targets = job.get('targets')
        if not isinstance(targets, list) or len(targets) != 1:
            raise WorkerFailure(
                UNSUPPORTED_TARGET_SET,
                'Claimed job did not carry exactly one immutable target')
        target = targets[0]
        if not isinstance(target, dict) or target.get('platform') != SUPPORTED_TARGET:
            raise WorkerFailure(
                UNSUPPORTED_TARGET_SET,
                'Claimed job target is not exactly {}'.format(SUPPORTED_TARGET))

        return publisher_id

    @staticmethod
    def _fence_abandoned_predecessor(claim, job):
        """Refuse to replay provider work after an untrustworthy predecessor.

        BE-04 closes a lease-expired attempt as `ABANDONED` and may return the
        job to `PENDING`, but that transition proves nothing about how far the
        dead worker got: the durable model records no provider phase and no
        per-work progress. Crossref redeposit support authorises reviewed
        retries of a *proven pre-write* failure; it never authorises blind
        replay here.

        The predecessor is located by exact ordinal `attemptNumber - 1`. List
        position is never used: the attempt collection is ordered most recent
        first, and reading position 0 would silently inspect the wrong attempt
        the moment ordering or the inclusion of the current open attempt
        changed. Anything that prevents identifying exactly one trustworthy
        predecessor is itself fenced, because an unverifiable history is
        indistinguishable from an abandoned one.
        """
        attempt_number = claim.get('attemptNumber')
        if isinstance(attempt_number, bool) or not isinstance(
                attempt_number, int) or attempt_number < 1:
            raise WorkerFailure(
                CROSSREF_DEPOSIT_INDETERMINATE,
                'Claim carried no trustworthy attempt ordinal, so a possible '
                'abandoned predecessor could not be excluded; no catalogue or '
                'provider work was attempted')

        if attempt_number == 1:
            # A first attempt has no predecessor to fence against.
            return

        attempts = job.get('attempts')
        if not isinstance(attempts, list):
            raise WorkerFailure(
                CROSSREF_DEPOSIT_INDETERMINATE,
                'Claim carried no usable attempt history for a repeated '
                'attempt; no catalogue or provider work was attempted')

        predecessor_ordinal = attempt_number - 1
        matches = []
        for attempt in attempts:
            if not isinstance(attempt, dict):
                raise WorkerFailure(
                    CROSSREF_DEPOSIT_INDETERMINATE,
                    'Claim carried a malformed attempt record; no catalogue '
                    'or provider work was attempted')
            ordinal = attempt.get('attemptNumber')
            if isinstance(ordinal, bool) or not isinstance(ordinal, int):
                raise WorkerFailure(
                    CROSSREF_DEPOSIT_INDETERMINATE,
                    'Claim carried an attempt without a usable ordinal; no '
                    'catalogue or provider work was attempted')
            if ordinal == predecessor_ordinal:
                matches.append(attempt)

        if len(matches) != 1:
            raise WorkerFailure(
                CROSSREF_DEPOSIT_INDETERMINATE,
                'Immediately preceding attempt {} was not uniquely present, '
                'so a possible abandoned predecessor could not be excluded; '
                'no catalogue or provider work was attempted'.format(
                    predecessor_ordinal))

        result = matches[0].get('result')
        if result not in ATTEMPT_RESULTS:
            raise WorkerFailure(
                CROSSREF_DEPOSIT_INDETERMINATE,
                'Immediately preceding attempt {} carried no trustworthy '
                'result, so a possible abandoned predecessor could not be '
                'excluded; no catalogue or provider work was '
                'attempted'.format(predecessor_ordinal))

        if result == ATTEMPT_RESULT_ABANDONED:
            raise WorkerFailure(
                CROSSREF_DEPOSIT_INDETERMINATE,
                'Immediately preceding attempt {} was abandoned, so the '
                'Crossref provider outcome of that attempt is unprovable; no '
                'catalogue or provider work was attempted and operator '
                'reconciliation is required'.format(predecessor_ordinal))

    def _catalogue_query(self, query, variables):
        """Run one bounded catalogue read before any provider write."""
        try:
            return self._transport.execute(query, variables)
        except (thothapi.ThothWorkerTransportError,
                thothapi.ThothWorkerResponseError) as error:
            raise WorkerFailure(
                CATALOGUE_QUERY_FAILED,
                'Bounded Thoth catalogue read failed before any provider '
                'write: {}'.format(type(error).__name__)) from None

    def _select_candidates(self, publisher_id):
        """Return the complete reconciled candidate population.

        The raw `bookCount` is read first and is the pilot size fence. Pages
        are then consumed until termination rather than until the count has
        been collected, so an extra row or page is detected instead of being
        silently truncated away.
        """
        count_data = self._catalogue_query(
            BACK_CATALOGUE_BOOK_COUNT_QUERY,
            {
                'publishers': [publisher_id],
                'workStatuses': list(CATALOGUE_WORK_STATUSES),
            },
        )
        count = count_data.get('bookCount')
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise WorkerFailure(
                CATALOGUE_CONTRACT_INVALID,
                'Thoth reported a malformed candidate count')

        if count > MAX_PILOT_CANDIDATES:
            raise WorkerFailure(
                CATALOGUE_TOO_LARGE,
                'Publisher has {} raw ACTIVE/FORTHCOMING candidates, above '
                'the {}-book pilot bound; no Crossref write was '
                'attempted'.format(count, MAX_PILOT_CANDIDATES))

        if count == 0:
            return []

        rows = []
        seen = set()
        offset = 0
        while True:
            page_data = self._catalogue_query(
                BACK_CATALOGUE_BOOKS_QUERY,
                {
                    'limit': CATALOGUE_PAGE_SIZE,
                    'offset': offset,
                    'publishers': [publisher_id],
                    'workStatuses': list(CATALOGUE_WORK_STATUSES),
                },
            )
            page = page_data.get('books')
            if not isinstance(page, list):
                raise WorkerFailure(
                    CATALOGUE_CONTRACT_INVALID,
                    'Thoth returned a malformed catalogue page')
            if len(page) > CATALOGUE_PAGE_SIZE:
                raise WorkerFailure(
                    CATALOGUE_CONTRACT_INVALID,
                    'Thoth returned more rows than the requested page size')

            for row in page:
                rows.append(self._validated_row(row, publisher_id, seen))

            if len(rows) > count:
                raise WorkerFailure(
                    CATALOGUE_CONTRACT_INVALID,
                    'Thoth returned more catalogue rows than the reported '
                    'candidate count')

            if len(page) < CATALOGUE_PAGE_SIZE:
                break
            # A full page never terminates the population: the next page must
            # be requested and proven empty.
            offset += CATALOGUE_PAGE_SIZE

        if len(rows) != count:
            raise WorkerFailure(
                CATALOGUE_CONTRACT_INVALID,
                'Thoth returned {} catalogue rows but reported {} '
                'candidates'.format(len(rows), count))
        return rows

    @staticmethod
    def _validated_row(row, publisher_id, seen):
        """Validate one catalogue row and record its work ID as seen."""
        if not isinstance(row, dict):
            raise WorkerFailure(
                CATALOGUE_CONTRACT_INVALID,
                'Thoth returned a malformed catalogue row')

        work_id = canonical_uuid(row.get('workId'))
        if work_id is None:
            raise WorkerFailure(
                CATALOGUE_CONTRACT_INVALID,
                'Thoth returned a catalogue row without a canonical work ID')
        if work_id in seen:
            raise WorkerFailure(
                CATALOGUE_CONTRACT_INVALID,
                'Thoth returned work {} more than once'.format(work_id))
        seen.add(work_id)

        if row.get('workStatus') not in CATALOGUE_WORK_STATUSES:
            raise WorkerFailure(
                CATALOGUE_CONTRACT_INVALID,
                'Thoth returned work {} with a status outside the requested '
                'filter'.format(work_id))

        imprint = row.get('imprint')
        publisher = imprint.get('publisher') if isinstance(imprint, dict) else None
        row_publisher_id = publisher.get('publisherId') if isinstance(
            publisher, dict) else None
        if not isinstance(row_publisher_id, str) or canonical_uuid(
                row_publisher_id) is None:
            raise WorkerFailure(
                CATALOGUE_CONTRACT_INVALID,
                'Thoth returned work {} without a usable publisher '
                'identity'.format(work_id))
        if row_publisher_id != publisher_id:
            raise WorkerFailure(
                CATALOGUE_PUBLISHER_MISMATCH,
                'Thoth returned work {} belonging to a publisher other than '
                'the claimed one'.format(work_id))

        return row

    @staticmethod
    def _executable_work_ids(rows):
        """Return the sorted work IDs this stage may deposit to Crossref.

        A root work DOI and a publication date are both required. Roots whose
        only depositable records are chapter DOIs are deliberately outside
        this pilot: the Crossref adapter derives its prefix from the root DOI,
        so such a root is excluded here rather than repaired or broadened.
        """
        executable = []
        for row in rows:
            doi = row.get('doi')
            publication_date = row.get('publicationDate')
            if not isinstance(doi, str) or len(doi.strip()) < 1:
                continue
            if not isinstance(publication_date, str) or len(
                    publication_date.strip()) < 1:
                continue
            executable.append(row['workId'])
        return sorted(executable)

    def _execute_works(self, claim, publisher_id, executable):
        """Run each executable work sequentially behind the lease guard.

        The first result that is not `ACCEPTED` stops the attempt: no later
        runner is started and no later provider write is attempted.
        """
        accepted = []
        for work_id in executable:
            self._require_lease_budget(claim, accepted)
            logging.info('Executing Crossref deposit for work %s', work_id)
            result = self._launch_runner(work_id, publisher_id)
            self._require_accepted(work_id, result)
            accepted.append(work_id)
        return accepted

    def _launch_runner(self, work_id, publisher_id):
        """Run one work in its own subprocess and return its raw result."""
        try:
            return self._runner(work_id, publisher_id, RUNNER_DEADLINE_SECONDS)
        except RunnerDeadlineExceeded:
            # The runner was killed at its deadline. The parent has no
            # trustworthy evidence of which phase it reached, so the Crossref
            # outcome is unprovable rather than assumed pre-write.
            raise WorkerFailure(
                CROSSREF_DEPOSIT_INDETERMINATE,
                'Runner for work {} exceeded its {}-second deadline, so the '
                'Crossref deposit outcome is unprovable; operator '
                'reconciliation is required'.format(
                    work_id, RUNNER_DEADLINE_SECONDS)) from None
        except Exception as error:
            raise WorkerFailure(
                CROSSREF_RUNNER_FAILED,
                'Runner for work {} could not be run to a classified '
                'result: {}'.format(
                    work_id, type(error).__name__)) from None

    @staticmethod
    def _require_accepted(work_id, result):
        """Validate one runner result and stop unless it is `ACCEPTED`.

        A result that cannot be parsed, is of an unknown schema version, or
        carries a code outside the closed taxonomy is untrustworthy and stops
        the attempt exactly as an explicit failure does.
        """
        if not isinstance(result, dict):
            raise WorkerFailure(
                CROSSREF_RUNNER_FAILED,
                'Runner for work {} produced no structured result'.format(
                    work_id))
        if result.get('schemaVersion') != RUNNER_SCHEMA_VERSION:
            raise WorkerFailure(
                CROSSREF_RUNNER_FAILED,
                'Runner for work {} produced an unsupported result schema '
                'version'.format(work_id))

        status = result.get('status')
        if status not in RUNNER_STATUSES:
            raise WorkerFailure(
                CROSSREF_RUNNER_FAILED,
                'Runner for work {} produced an unknown result status'.format(
                    work_id))

        external_write_started = result.get('externalWriteStarted')
        if not isinstance(external_write_started, bool):
            raise WorkerFailure(
                CROSSREF_RUNNER_FAILED,
                'Runner for work {} did not report whether an external write '
                'started'.format(work_id))

        code = result.get('code')
        if status == RUNNER_STATUS_ACCEPTED:
            if code is not None:
                raise WorkerFailure(
                    CROSSREF_RUNNER_FAILED,
                    'Runner for work {} reported acceptance with a failure '
                    'classification'.format(work_id))
            return

        if code not in WORKER_ERROR_CODES:
            raise WorkerFailure(
                CROSSREF_RUNNER_FAILED,
                'Runner for work {} produced a classification outside the '
                'worker taxonomy'.format(work_id))

        detail = result.get('detail')
        detail = detail if isinstance(detail, str) else ''
        if code in RETRYABLE_CODES and external_write_started:
            # A retryable classification is only ever accurate before the
            # relevant provider write. Once a write may have started the
            # outcome is unprovable and must not invite an automatic replay.
            raise WorkerFailure(
                CROSSREF_DEPOSIT_INDETERMINATE,
                'Runner for work {} reported a pre-write classification after '
                'an external write may have started, so the Crossref outcome '
                'is unprovable; operator reconciliation is required'.format(
                    work_id))

        raise WorkerFailure(
            code, 'Work {}: {}'.format(work_id, detail))

    def _require_lease_budget(self, claim, accepted):
        """Refuse to start another runner without enough remaining lease.

        The server-returned `leaseExpiresAt` is the only authority; the lease
        is never renewed and no heartbeat exists.
        """
        try:
            lease_expires_at = parse_timestamp(claim.get('leaseExpiresAt'))
        except (ValueError, TypeError):
            raise WorkerFailure(
                INTERNAL_WORKER_ERROR,
                'Claim carried no usable lease expiry, so no runner was '
                'started and no provider write was attempted') from None

        remaining = (lease_expires_at - self._clock()).total_seconds()
        if remaining >= LEASE_REQUIRED_SECONDS:
            return

        if accepted:
            raise WorkerFailure(
                LEASE_BUDGET_EXHAUSTED,
                'Remaining lease is below the {}-second guard after {} '
                'accepted deposit(s), so no further runner was started; '
                'operator reconciliation is required'.format(
                    LEASE_REQUIRED_SECONDS, len(accepted)))

        raise WorkerFailure(
            INTERNAL_WORKER_ERROR,
            'Remaining lease is below the {}-second guard before any '
            'provider write, so no runner was started'.format(
                LEASE_REQUIRED_SECONDS))

    def _complete(self, job_id, claim_token, accepted_works):
        """Report whole-job success under the exact claim token.

        A lost response is resolved by repeating the identical mutation, never
        by repeating provider work to discover what committed.
        """
        logging.info('Completing distribution job %s', job_id)
        variables = {'data': {
            'distributionJobId': job_id,
            'claimToken': claim_token,
        }}
        ambiguous = False
        for _ in range(RESULT_REPORT_MAX_ATTEMPTS):
            try:
                self._transport.execute(
                    COMPLETE_DISTRIBUTION_JOB_MUTATION, variables)
                return WorkerResult(OUTCOME_COMPLETED,
                                    accepted_works=accepted_works)
            except thothapi.ThothWorkerTransportError:
                # No usable response arrived, so whether the transition
                # committed is unknown. Repeat exactly this mutation.
                ambiguous = True
                continue
            except thothapi.ThothWorkerResponseError as error:
                if ambiguous and self._is_terminal_succeeded(error.errors):
                    logging.info(
                        'Job %s was already SUCCEEDED; the ambiguous earlier '
                        'completion committed', job_id)
                    return WorkerResult(OUTCOME_COMPLETED,
                                        accepted_works=accepted_works)
                return self._hold(
                    job_id,
                    'Completion reporting returned an unreconcilable '
                    'response',
                    accepted_works)

        return self._hold(
            job_id,
            'Completion reporting did not resolve within its bounded retry '
            'budget',
            accepted_works)

    @staticmethod
    def _is_terminal_succeeded(errors):
        """Match the pinned v1.7.0 terminal-SUCCEEDED contract exactly."""
        return any(
            isinstance(error, dict)
            and error.get('type') == TERMINAL_ERROR_TYPE
            and error.get('message') == TERMINAL_SUCCEEDED_MESSAGE
            for error in errors
        )

    def _report_failure(self, job_id, claim_token, failure):
        """Report one classified failure under the exact claim token.

        Any retry repeats the identical job, token, code, detail and
        retryability. No provider action occurs between attempts.
        """
        logging.error('Failing distribution job %s as %s (retryable=%s)',
                      job_id, failure.code, failure.retryable)
        variables = {'data': {
            'distributionJobId': job_id,
            'claimToken': claim_token,
            'errorCode': failure.code,
            'errorDetail': failure.detail,
            'retryable': failure.retryable,
        }}
        for _ in range(RESULT_REPORT_MAX_ATTEMPTS):
            try:
                self._transport.execute(
                    FAIL_DISTRIBUTION_JOB_MUTATION, variables)
                return WorkerResult(OUTCOME_FAILED, error_code=failure.code,
                                    retryable=failure.retryable,
                                    detail=failure.detail)
            except thothapi.ThothWorkerTransportError:
                continue
            except thothapi.ThothWorkerResponseError:
                # A stale claim or a terminal state means this worker cannot
                # record the outcome it observed. That is a reconciliation
                # question, never a licence to repeat external work.
                return self._hold(
                    job_id,
                    'Failure reporting for {} returned an unreconcilable '
                    'response'.format(failure.code))

        return self._hold(
            job_id,
            'Failure reporting for {} did not resolve within its bounded '
            'retry budget'.format(failure.code))

    @staticmethod
    def _hold(job_id, detail, accepted_works=()):
        """Record that durable state could not be reconciled from here."""
        logging.error('Job %s requires operator reconciliation: %s',
                      job_id, detail)
        return WorkerResult(OUTCOME_HOLD, detail=bounded_detail(detail),
                            accepted_works=accepted_works)


def main():
    """Run at most one bounded claim/execute/report cycle."""
    if not worker_enabled(environ):
        logging.info(
            'Distribution job worker is not enabled; no claim attempted')
        return 0

    try:
        transport = thothapi.ThothWorkerTransport()
    except thothapi.ThothWorkerAuthError as error:
        # No job has been claimed, so there is nothing to report against and
        # no durable failure may be fabricated. This is an invocation failure.
        logging.error('Worker could not authenticate: %s', error)
        return 1

    job_worker = DistributionJobWorker(
        transport=transport,
        runner=launch_crossref_runner,
        clock=lambda: datetime.now(timezone.utc),
    )
    try:
        result = job_worker.run_once()
    except (thothapi.ThothWorkerTransportError,
            thothapi.ThothWorkerResponseError) as error:
        logging.error('Worker invocation failed before or during claim: %s',
                      type(error).__name__)
        return 1

    return 0 if result.outcome in (OUTCOME_COMPLETED, OUTCOME_NO_JOB) else 1


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s:%(asctime)s: %(message)s')
    # DEBUG level urllib3 logs may contain sensitive information such as
    # bearer tokens and credential-bearing URLs, and must never be emitted.
    logging.getLogger("urllib3").setLevel(logging.INFO)
    sys.exit(main())
