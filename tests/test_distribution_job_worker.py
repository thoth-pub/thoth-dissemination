"""Unit tests for the DIS-02A durable-job worker substrate.

Every external boundary (Thoth GraphQL, the Crossref runner subprocess) is
faked. No test contacts a real Thoth API, a real provider, or requires any
production credential.
"""

import json
import unittest
from unittest.mock import MagicMock, patch

import requests

import distribution_job_worker as worker
import thothapi


class TestWorkerTransportConstruction(unittest.TestCase):
    """The worker transport takes its bearer token from the environment."""

    def test_transport_requires_worker_token(self):
        with patch.dict(thothapi.environ, {}, clear=True):
            with self.assertRaises(thothapi.ThothWorkerAuthError):
                thothapi.ThothWorkerTransport()

    def test_transport_rejects_empty_worker_token(self):
        with patch.dict(thothapi.environ,
                        {'THOTH_WORKER_TOKEN': '   '}, clear=True):
            with self.assertRaises(thothapi.ThothWorkerAuthError):
                thothapi.ThothWorkerTransport()


def _worker_env(**overrides):
    env = {
        'THOTH_WORKER_TOKEN': 'super-secret-worker-token',
        'THOTH_API_URL': 'https://api.example',
    }
    env.update(overrides)
    return env


def _fake_session(payload=None, status_code=200, exception=None, text=None):
    """A stand-in for `requests` exposing only the `post` the transport uses."""
    session = MagicMock()
    if exception is not None:
        session.post.side_effect = exception
        return session
    response = MagicMock()
    response.status_code = status_code
    response.text = text if text is not None else json.dumps(payload or {})
    response.json.return_value = payload if payload is not None else {}
    session.post.return_value = response
    return session


class TestWorkerTransportRequests(unittest.TestCase):
    """Every worker call is bearer-authenticated and explicitly bounded."""

    def _transport(self, session):
        with patch.dict(thothapi.environ, _worker_env(), clear=True):
            return thothapi.ThothWorkerTransport(session=session)

    def test_execute_posts_bearer_token_and_explicit_timeouts(self):
        session = _fake_session({'data': {'bookCount': 3}})
        transport = self._transport(session)

        data = transport.execute('query Q { bookCount }', {'a': 1})

        self.assertEqual(data, {'bookCount': 3})
        session.post.assert_called_once()
        _, kwargs = session.post.call_args
        self.assertEqual(kwargs['url'], 'https://api.example/graphql')
        self.assertEqual(
            kwargs['headers']['Authorization'],
            'Bearer super-secret-worker-token')
        self.assertEqual(kwargs['timeout'], (10, 60))
        self.assertEqual(
            kwargs['json'],
            {'query': 'query Q { bookCount }', 'variables': {'a': 1}})

    def test_execute_wraps_transport_exception_without_token(self):
        session = _fake_session(
            exception=requests.exceptions.ConnectTimeout(
                'failed talking to https://api.example with '
                'super-secret-worker-token'))
        transport = self._transport(session)

        with self.assertRaises(thothapi.ThothWorkerTransportError) as caught:
            transport.execute('query Q { bookCount }', {})

        self.assertNotIn('super-secret-worker-token', str(caught.exception))

    def test_execute_rejects_non_success_status(self):
        session = _fake_session({'data': {}}, status_code=500)
        transport = self._transport(session)

        with self.assertRaises(thothapi.ThothWorkerTransportError):
            transport.execute('query Q { bookCount }', {})

    def test_execute_rejects_unparseable_body(self):
        session = _fake_session(text='<html>not json</html>')
        session.post.return_value.json.side_effect = ValueError('no json')
        transport = self._transport(session)

        with self.assertRaises(thothapi.ThothWorkerTransportError):
            transport.execute('query Q { bookCount }', {})

    def test_execute_rejects_oversized_body(self):
        oversized = 'x' * (thothapi.WORKER_MAX_RESPONSE_BYTES + 1)
        session = _fake_session(text=oversized)
        transport = self._transport(session)

        with self.assertRaises(thothapi.ThothWorkerTransportError):
            transport.execute('query Q { bookCount }', {})

    def test_graphql_errors_are_reduced_to_bounded_sanitised_fields(self):
        session = _fake_session({
            'data': None,
            'errors': [{
                'message': 'The distribution job claim is no longer valid.',
                'path': ['completeDistributionJob'],
                'locations': [{'line': 2, 'column': 3}],
                'extensions': {
                    'type': 'STALE_DISTRIBUTION_JOB_CLAIM',
                    'internalTrace': 'super-secret-worker-token',
                },
            }],
        })
        transport = self._transport(session)

        with self.assertRaises(thothapi.ThothWorkerResponseError) as caught:
            transport.execute('mutation M { completeDistributionJob }', {})

        self.assertEqual(caught.exception.errors, [{
            'message': 'The distribution job claim is no longer valid.',
            'path': ['completeDistributionJob'],
            'type': 'STALE_DISTRIBUTION_JOB_CLAIM',
        }])
        self.assertNotIn('super-secret-worker-token', str(caught.exception))
        self.assertNotIn('internalTrace', str(caught.exception))


class TestWorkerActivationGuard(unittest.TestCase):
    """Only the exact ASCII value `ON` permits the worker to proceed."""

    def test_exact_on_is_enabled(self):
        self.assertTrue(worker.worker_enabled({
            worker.WORKER_ENABLED_VARIABLE: 'ON'}))

    def test_every_other_value_is_disabled(self):
        for value in ['on', 'On', 'oN', ' ON', 'ON ', '', 'OFF', 'off',
                      'true', 'TRUE', '1', 'yes', 'ENABLED', 'ON\n',
                      '\u041e\u041d']:
            with self.subTest(value=value):
                self.assertFalse(worker.worker_enabled({
                    worker.WORKER_ENABLED_VARIABLE: value}))

    def test_absent_variable_is_disabled(self):
        self.assertFalse(worker.worker_enabled({}))

    def test_disabled_worker_never_builds_a_transport_or_claims(self):
        with patch.object(thothapi, 'ThothWorkerTransport') as transport_class:
            with patch.dict(worker.environ, {}, clear=True):
                exit_code = worker.main()

        self.assertEqual(exit_code, 0)
        transport_class.assert_not_called()


JOB_ID = '00000000-0000-4000-8000-000000000001'
CLAIM_TOKEN = '00000000-0000-4000-8000-0000000000ff'
PUBLISHER_ID = '11111111-1111-4111-8111-111111111111'
OTHER_PUBLISHER_ID = '22222222-2222-4222-8222-222222222222'
LEASE_EXPIRES_AT = '2026-08-27T18:00:00Z'


def _claim(attempt_number=1, attempts=None, targets=('CROSSREF',),
           kind='PUBLISHER_BACK_CATALOGUE', publisher_id=PUBLISHER_ID,
           lease_expires_at=LEASE_EXPIRES_AT, claim_token=CLAIM_TOKEN):
    return {
        'claimToken': claim_token,
        'leaseExpiresAt': lease_expires_at,
        'attemptNumber': attempt_number,
        'job': {
            'distributionJobId': JOB_ID,
            'kind': kind,
            'publisherId': publisher_id,
            'targets': [{'platform': p} for p in targets],
            'attempts': [] if attempts is None else attempts,
        },
    }


class FakeTransport():
    """Records every worker GraphQL call and replays scripted responses."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def execute(self, query, variables):
        self.calls.append((query, variables))
        if not self.responses:
            raise AssertionError('unexpected extra GraphQL call')
        outcome = self.responses.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def operations(self):
        """Return the operation name of each call, in order."""
        names = []
        for query, _ in self.calls:
            if 'claimDistributionJobs' in query:
                names.append('claim')
            elif 'completeDistributionJob' in query:
                names.append('complete')
            elif 'failDistributionJob' in query:
                names.append('fail')
            elif 'bookCount' in query:
                names.append('bookCount')
            elif 'books(' in query:
                names.append('books')
            else:
                names.append('unknown')
        return names


class RecordingRunner():
    """Stands in for the per-work Crossref subprocess boundary."""

    def __init__(self, results=None):
        self.results = list(results or [])
        self.launched = []

    def __call__(self, work_id, expected_publisher_id, deadline_seconds):
        self.launched.append(
            (work_id, expected_publisher_id, deadline_seconds))
        if not self.results:
            raise AssertionError('unexpected extra runner launch')
        outcome = self.results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _accepted():
    return {'schemaVersion': 1, 'status': 'ACCEPTED', 'code': None,
            'detail': None, 'externalWriteStarted': True}


def _build_worker(transport, runner, now=None):
    return worker.DistributionJobWorker(
        transport=transport,
        runner=runner,
        clock=lambda: now or worker.parse_timestamp('2026-08-27T17:00:00Z'),
    )


class TestClaimContract(unittest.TestCase):
    """The claim request shape is exact and one job is processed per run."""

    def test_claim_request_uses_exact_bounded_input(self):
        transport = FakeTransport([{'claimDistributionJobs': []}])
        result = _build_worker(transport, RecordingRunner()).run_once()

        self.assertEqual(result.outcome, worker.OUTCOME_NO_JOB)
        self.assertEqual(transport.operations(), ['claim'])
        _, variables = transport.calls[0]
        self.assertEqual(variables, {'data': {
            'limit': 1,
            'leaseSeconds': 3600,
            'kinds': ['PUBLISHER_BACK_CATALOGUE'],
        }})

    def test_empty_claim_is_a_successful_no_job_invocation(self):
        transport = FakeTransport([{'claimDistributionJobs': []}])
        runner = RecordingRunner()
        result = _build_worker(transport, runner).run_once()

        self.assertEqual(result.outcome, worker.OUTCOME_NO_JOB)
        self.assertEqual(runner.launched, [])

    def test_only_the_first_returned_claim_is_processed(self):
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim(), _claim()]},
            {'bookCount': 0},
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        result = _build_worker(transport, RecordingRunner()).run_once()

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual(transport.operations(),
                         ['claim', 'bookCount', 'complete'])


class TestUnsupportedJobsAndTargets(unittest.TestCase):
    """Unsupported kinds and target sets fail closed before any adapter call."""

    def _run_rejected(self, claim):
        transport = FakeTransport([
            {'claimDistributionJobs': [claim]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        runner = RecordingRunner()
        result = _build_worker(transport, runner).run_once()
        return result, transport, runner

    def test_unsupported_kind_fails_before_catalogue(self):
        result, transport, runner = self._run_rejected(_claim(kind='OTHER'))

        self.assertEqual(result.error_code, worker.UNSUPPORTED_JOB_KIND)
        self.assertFalse(result.retryable)
        self.assertEqual(transport.operations(), ['claim', 'fail'])
        self.assertEqual(runner.launched, [])

    def test_empty_target_set_is_unsupported(self):
        result, transport, runner = self._run_rejected(_claim(targets=()))

        self.assertEqual(result.error_code, worker.UNSUPPORTED_TARGET_SET)
        self.assertEqual(runner.launched, [])

    def test_multi_target_set_is_unsupported(self):
        result, _, runner = self._run_rejected(
            _claim(targets=('CROSSREF', 'INTERNET_ARCHIVE')))

        self.assertEqual(result.error_code, worker.UNSUPPORTED_TARGET_SET)
        self.assertEqual(runner.launched, [])

    def test_non_crossref_target_is_unsupported(self):
        result, _, runner = self._run_rejected(
            _claim(targets=('INTERNET_ARCHIVE',)))

        self.assertEqual(result.error_code, worker.UNSUPPORTED_TARGET_SET)
        self.assertEqual(runner.launched, [])

    def test_malformed_publisher_uuid_fails_closed(self):
        result, transport, runner = self._run_rejected(
            _claim(publisher_id='not-a-uuid'))

        # A claim whose publisher scope cannot be established performs no
        # catalogue read and no provider write. It is reported under the only
        # taxonomy entry that is truthful here: an internal failure the worker
        # can prove began no provider write.
        self.assertEqual(result.error_code, worker.INTERNAL_WORKER_ERROR)
        self.assertEqual(transport.operations(), ['claim', 'fail'])
        self.assertEqual(runner.launched, [])

    def test_failure_report_uses_the_exact_claim_token(self):
        _, transport, _ = self._run_rejected(_claim(kind='OTHER'))

        _, variables = transport.calls[-1]
        self.assertEqual(variables['data']['distributionJobId'], JOB_ID)
        self.assertEqual(variables['data']['claimToken'], CLAIM_TOKEN)
        self.assertEqual(variables['data']['errorCode'],
                         worker.UNSUPPORTED_JOB_KIND)
        self.assertIs(variables['data']['retryable'], False)


def _attempt(number, result='FAILED'):
    return {'attemptNumber': number, 'result': result}


class TestAbandonedPredecessorFence(unittest.TestCase):
    """A reclaimed attempt is fenced before any catalogue or provider work."""

    def _run(self, claim, extra_responses=()):
        responses = [{'claimDistributionJobs': [claim]}]
        responses.extend(extra_responses)
        responses.append({'failDistributionJob': {'distributionJobId': JOB_ID}})
        transport = FakeTransport(responses)
        runner = RecordingRunner()
        result = _build_worker(transport, runner).run_once()
        return result, transport, runner

    def test_first_attempt_is_not_fenced(self):
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim(attempt_number=1, attempts=[])]},
            {'bookCount': 0},
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        result = _build_worker(transport, RecordingRunner()).run_once()

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual(transport.operations(),
                         ['claim', 'bookCount', 'complete'])

    def test_abandoned_predecessor_performs_zero_catalogue_and_runner_work(self):
        result, transport, runner = self._run(_claim(
            attempt_number=2,
            attempts=[_attempt(2, None), _attempt(1, 'ABANDONED')]))

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertIs(result.retryable, False)
        # Exactly claim then fail: no bookCount, no books, no runner.
        self.assertEqual(transport.operations(), ['claim', 'fail'])
        self.assertEqual(runner.launched, [])

    def test_fenced_claim_reports_only_the_new_claim_token(self):
        new_token = '00000000-0000-4000-8000-0000000000ab'
        _, transport, _ = self._run(_claim(
            attempt_number=3,
            claim_token=new_token,
            attempts=[_attempt(2, 'ABANDONED'), _attempt(1, 'FAILED')]))

        _, variables = transport.calls[-1]
        self.assertEqual(variables['data']['claimToken'], new_token)
        self.assertNotEqual(variables['data']['claimToken'], CLAIM_TOKEN)
        self.assertEqual(variables['data']['errorCode'],
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertIs(variables['data']['retryable'], False)

    def test_predecessor_is_located_by_ordinal_not_list_position(self):
        # The immediately preceding ordinal is 2 and it FAILED, so the job
        # proceeds. Attempt 1 is ABANDONED but is not the predecessor, and
        # list position 0 holds an unrelated attempt.
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim(
                attempt_number=3,
                attempts=[_attempt(1, 'ABANDONED'), _attempt(3, None),
                          _attempt(2, 'FAILED')])]},
            {'bookCount': 0},
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        result = _build_worker(transport, RecordingRunner()).run_once()

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)

    def test_abandoned_at_wrong_ordinal_in_first_list_position_still_fences(self):
        # Ordinal 2 is the predecessor and is ABANDONED even though a
        # SUCCEEDED attempt occupies the first list position.
        result, transport, runner = self._run(_claim(
            attempt_number=3,
            attempts=[_attempt(1, 'SUCCEEDED'), _attempt(2, 'ABANDONED'),
                      _attempt(3, None)]))

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertEqual(transport.operations(), ['claim', 'fail'])
        self.assertEqual(runner.launched, [])

    def test_failed_predecessor_is_not_confused_with_abandoned(self):
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim(
                attempt_number=2,
                attempts=[_attempt(1, 'FAILED')])]},
            {'bookCount': 0},
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        result = _build_worker(transport, RecordingRunner()).run_once()

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)

    def test_cancelled_predecessor_is_not_confused_with_abandoned(self):
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim(
                attempt_number=2, attempts=[_attempt(1, 'CANCELLED')])]},
            {'bookCount': 0},
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        result = _build_worker(transport, RecordingRunner()).run_once()

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)

    def test_missing_predecessor_ordinal_fails_closed(self):
        result, transport, runner = self._run(_claim(
            attempt_number=3, attempts=[_attempt(1, 'FAILED')]))

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertIs(result.retryable, False)
        self.assertEqual(transport.operations(), ['claim', 'fail'])
        self.assertEqual(runner.launched, [])

    def test_duplicated_predecessor_ordinal_fails_closed(self):
        result, transport, runner = self._run(_claim(
            attempt_number=2,
            attempts=[_attempt(1, 'FAILED'), _attempt(1, 'FAILED')]))

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertEqual(transport.operations(), ['claim', 'fail'])
        self.assertEqual(runner.launched, [])

    def test_open_predecessor_attempt_fails_closed(self):
        result, transport, runner = self._run(_claim(
            attempt_number=2, attempts=[_attempt(1, None)]))

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertEqual(runner.launched, [])

    def test_unknown_predecessor_result_fails_closed(self):
        result, _, runner = self._run(_claim(
            attempt_number=2, attempts=[_attempt(1, 'PROBABLY_FINE')]))

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertEqual(runner.launched, [])

    def test_malformed_attempt_history_fails_closed(self):
        for attempts in [
            'not-a-list',
            [None],
            ['nope'],
            [{'attemptNumber': '1', 'result': 'FAILED'}],
            [{'attemptNumber': True, 'result': 'FAILED'}],
            [{'result': 'FAILED'}],
        ]:
            with self.subTest(attempts=attempts):
                result, transport, runner = self._run(
                    _claim(attempt_number=2, attempts=attempts))
                self.assertEqual(result.error_code,
                                 worker.CROSSREF_DEPOSIT_INDETERMINATE)
                self.assertEqual(transport.operations(), ['claim', 'fail'])
                self.assertEqual(runner.launched, [])

    def test_malformed_attempt_number_fails_closed(self):
        for attempt_number in ['2', None, True, 0, -1]:
            with self.subTest(attempt_number=attempt_number):
                result, transport, runner = self._run(_claim(
                    attempt_number=attempt_number,
                    attempts=[_attempt(1, 'FAILED')]))
                self.assertEqual(result.error_code,
                                 worker.CROSSREF_DEPOSIT_INDETERMINATE)
                self.assertEqual(transport.operations(), ['claim', 'fail'])
                self.assertEqual(runner.launched, [])

    def test_fence_precedes_target_validation_free_catalogue_access(self):
        """Redeposit support cannot bypass the fence for a valid Crossref job."""
        result, transport, runner = self._run(_claim(
            attempt_number=2, targets=('CROSSREF',),
            attempts=[_attempt(1, 'ABANDONED')]))

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertNotIn('bookCount', transport.operations())
        self.assertNotIn('books', transport.operations())
        self.assertEqual(runner.launched, [])

    def test_fenced_detail_is_bounded_and_sanitised(self):
        result, _, _ = self._run(_claim(
            attempt_number=2, attempts=[_attempt(1, 'ABANDONED')]))

        self.assertLessEqual(len(result.detail), worker.MAX_ERROR_DETAIL_CHARS)
        self.assertIn('abandoned', result.detail.lower())
        self.assertNotIn('\n', result.detail)


def _row(work_id, status='ACTIVE', doi='https://doi.org/10.1000/x',
         publication_date='2020-01-01', publisher_id=PUBLISHER_ID):
    return {
        'workId': work_id,
        'workStatus': status,
        'doi': doi,
        'publicationDate': publication_date,
        'imprint': {'publisher': {'publisherId': publisher_id}},
    }


def _work_id(n):
    return '3333333{}-3333-4333-8333-333333333333'.format(n)


class TestCatalogueContract(unittest.TestCase):
    """Candidate selection is bounded, reconciled and publisher-fenced."""

    def _run(self, responses, runner=None):
        transport = FakeTransport(
            [{'claimDistributionJobs': [_claim()]}] + list(responses))
        runner = runner if runner is not None else RecordingRunner()
        result = _build_worker(transport, runner).run_once()
        return result, transport, runner

    def test_book_count_uses_exact_publisher_and_status_filters(self):
        _, transport, _ = self._run([
            {'bookCount': 0},
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        query, variables = transport.calls[1]
        self.assertIn('bookCount', query)
        self.assertEqual(variables, {
            'publishers': [PUBLISHER_ID],
            'workStatuses': ['ACTIVE', 'FORTHCOMING'],
        })
        self.assertNotIn('updatedAtWithRelations', query)

    def test_zero_candidates_is_a_successful_no_op(self):
        result, transport, runner = self._run([
            {'bookCount': 0},
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual(transport.operations(),
                         ['claim', 'bookCount', 'complete'])
        self.assertEqual(runner.launched, [])

    def test_count_above_pilot_cap_fails_before_any_provider_write(self):
        result, transport, runner = self._run([
            {'bookCount': 11},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.error_code, worker.CATALOGUE_TOO_LARGE)
        self.assertIs(result.retryable, False)
        self.assertEqual(transport.operations(), ['claim', 'bookCount', 'fail'])
        self.assertEqual(runner.launched, [])

    def test_books_page_uses_work_id_ascending_and_page_size_ten(self):
        _, transport, _ = self._run([
            {'bookCount': 1},
            {'books': [_row(_work_id(1))]},
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ], runner=RecordingRunner([_accepted()]))

        query, variables = transport.calls[2]
        self.assertIn('order: {field: WORK_ID, direction: ASC}', query)
        self.assertEqual(variables, {
            'limit': 10,
            'offset': 0,
            'publishers': [PUBLISHER_ID],
            'workStatuses': ['ACTIVE', 'FORTHCOMING'],
        })

    def test_full_first_page_requires_a_proven_empty_second_page(self):
        rows = [_row(_work_id(n)) for n in range(10)]
        result, transport, runner = self._run([
            {'bookCount': 10},
            {'books': rows},
            {'books': []},
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ], runner=RecordingRunner([_accepted() for _ in range(10)]))

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual(transport.operations(),
                         ['claim', 'bookCount', 'books', 'books', 'complete'])
        self.assertEqual(transport.calls[3][1]['offset'], 10)
        self.assertEqual(len(runner.launched), 10)

    def test_unexpected_eleventh_row_is_contract_invalid(self):
        rows = [_row(_work_id(n)) for n in range(10)]
        result, transport, runner = self._run([
            {'bookCount': 10},
            {'books': rows},
            {'books': [_row('44444444-4444-4444-8444-444444444444')]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.error_code, worker.CATALOGUE_CONTRACT_INVALID)
        self.assertIs(result.retryable, False)
        self.assertEqual(runner.launched, [])

    def test_page_larger_than_page_size_is_contract_invalid(self):
        rows = [_row(_work_id(n)) for n in range(11)]
        result, _, runner = self._run([
            {'bookCount': 10},
            {'books': rows},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.error_code, worker.CATALOGUE_CONTRACT_INVALID)
        self.assertEqual(runner.launched, [])

    def test_duplicate_work_id_is_contract_invalid(self):
        result, _, runner = self._run([
            {'bookCount': 2},
            {'books': [_row(_work_id(1)), _row(_work_id(1))]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.error_code, worker.CATALOGUE_CONTRACT_INVALID)
        self.assertEqual(runner.launched, [])

    def test_count_mismatch_is_contract_invalid(self):
        result, _, runner = self._run([
            {'bookCount': 3},
            {'books': [_row(_work_id(1))]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.error_code, worker.CATALOGUE_CONTRACT_INVALID)
        self.assertEqual(runner.launched, [])

    def test_non_canonical_work_id_is_contract_invalid(self):
        for work_id in ['3333333A-3333-4333-8333-333333333333',
                        'not-a-uuid', None, 12345,
                        '{33333331-3333-4333-8333-333333333333}']:
            with self.subTest(work_id=work_id):
                result, _, runner = self._run([
                    {'bookCount': 1},
                    {'books': [_row(work_id)]},
                    {'failDistributionJob': {'distributionJobId': JOB_ID}},
                ])
                self.assertEqual(result.error_code,
                                 worker.CATALOGUE_CONTRACT_INVALID)
                self.assertEqual(runner.launched, [])

    def test_status_outside_the_requested_filter_is_contract_invalid(self):
        result, _, runner = self._run([
            {'bookCount': 1},
            {'books': [_row(_work_id(1), status='SUPERSEDED')]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.error_code, worker.CATALOGUE_CONTRACT_INVALID)
        self.assertEqual(runner.launched, [])

    def test_foreign_publisher_row_is_a_publisher_mismatch(self):
        result, transport, runner = self._run([
            {'bookCount': 1},
            {'books': [_row(_work_id(1), publisher_id=OTHER_PUBLISHER_ID)]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.error_code,
                         worker.CATALOGUE_PUBLISHER_MISMATCH)
        self.assertIs(result.retryable, False)
        self.assertEqual(runner.launched, [])

    def test_malformed_row_shape_is_contract_invalid(self):
        for rows in ['nope', [None], ['row'], [{'workId': _work_id(1)}],
                     [{'workId': _work_id(1), 'workStatus': 'ACTIVE',
                       'imprint': {}}]]:
            with self.subTest(rows=rows):
                result, _, runner = self._run([
                    {'bookCount': 1},
                    {'books': rows},
                    {'failDistributionJob': {'distributionJobId': JOB_ID}},
                ])
                self.assertEqual(result.error_code,
                                 worker.CATALOGUE_CONTRACT_INVALID)
                self.assertEqual(runner.launched, [])

    def test_malformed_book_count_is_contract_invalid(self):
        for count in ['3', None, True, -1, 3.0]:
            with self.subTest(count=count):
                result, _, runner = self._run([
                    {'bookCount': count},
                    {'failDistributionJob': {'distributionJobId': JOB_ID}},
                ])
                self.assertEqual(result.error_code,
                                 worker.CATALOGUE_CONTRACT_INVALID)
                self.assertEqual(runner.launched, [])

    def test_catalogue_transport_failure_is_retryable_before_any_write(self):
        result, transport, runner = self._run([
            thothapi.ThothWorkerTransportError('Thoth worker request failed'),
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.error_code, worker.CATALOGUE_QUERY_FAILED)
        self.assertIs(result.retryable, True)
        self.assertEqual(runner.launched, [])


class TestExecutableWorkPredicate(unittest.TestCase):
    """Only works with a usable root DOI and a publication date execute."""

    def _run(self, rows, runner_results=()):
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim()]},
            {'bookCount': len(rows)},
            {'books': rows},
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        runner = RecordingRunner(runner_results)
        result = _build_worker(transport, runner).run_once()
        return result, transport, runner

    def test_chapter_doi_only_root_is_excluded_and_never_reaches_a_runner(self):
        result, _, runner = self._run([_row(_work_id(1), doi=None)])

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual(runner.launched, [])

    def test_missing_publication_date_is_excluded(self):
        result, _, runner = self._run([_row(_work_id(1),
                                            publication_date=None)])

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual(runner.launched, [])

    def test_blank_doi_is_excluded(self):
        result, _, runner = self._run([_row(_work_id(1), doi='   ')])

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual(runner.launched, [])

    def test_empty_executable_set_is_a_successful_no_op(self):
        result, transport, runner = self._run([
            _row(_work_id(1), doi=None),
            _row(_work_id(2), publication_date=None),
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual(runner.launched, [])

    def test_executable_works_run_sequentially_in_lexicographic_order(self):
        rows = [_row(_work_id(3)), _row(_work_id(1)), _row(_work_id(2))]
        result, _, runner = self._run(
            rows, runner_results=[_accepted() for _ in range(3)])

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual([call[0] for call in runner.launched],
                         [_work_id(1), _work_id(2), _work_id(3)])

    def test_runner_is_fenced_to_the_claimed_publisher_and_deadline(self):
        _, _, runner = self._run([_row(_work_id(1))],
                                 runner_results=[_accepted()])

        self.assertEqual(runner.launched,
                         [(_work_id(1), PUBLISHER_ID, 240)])


def _runner_result(status, code=None, detail=None, external=False,
                   schema_version=1):
    return {'schemaVersion': schema_version, 'status': status, 'code': code,
            'detail': detail, 'externalWriteStarted': external}


class TestRunnerResultHandling(unittest.TestCase):
    """The parent trusts only a well-formed, classified runner result."""

    def _run(self, rows, runner_results, expect_fail=True):
        responses = [
            {'claimDistributionJobs': [_claim()]},
            {'bookCount': len(rows)},
            {'books': rows},
        ]
        responses.append({'failDistributionJob': {'distributionJobId': JOB_ID}}
                         if expect_fail else
                         {'completeDistributionJob': {'distributionJobId': JOB_ID}})
        transport = FakeTransport(responses)
        runner = RecordingRunner(runner_results)
        result = _build_worker(transport, runner).run_once()
        return result, transport, runner

    def test_first_failed_result_stops_every_later_runner(self):
        rows = [_row(_work_id(n)) for n in range(1, 4)]
        result, transport, runner = self._run(rows, [
            _accepted(),
            _runner_result('FAILED', worker.CROSSREF_PREFIX_INVALID,
                           'prefix rejected'),
        ])

        self.assertEqual(result.error_code, worker.CROSSREF_PREFIX_INVALID)
        self.assertIs(result.retryable, False)
        # Only two runners ran: the third work was never started.
        self.assertEqual(len(runner.launched), 2)
        self.assertEqual(transport.operations(),
                         ['claim', 'bookCount', 'books', 'fail'])

    def test_first_indeterminate_result_stops_every_later_runner(self):
        rows = [_row(_work_id(n)) for n in range(1, 4)]
        result, _, runner = self._run(rows, [
            _runner_result('INDETERMINATE',
                           worker.CROSSREF_DEPOSIT_INDETERMINATE,
                           'deposit outcome unproven', external=True),
        ])

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertIs(result.retryable, False)
        self.assertEqual(len(runner.launched), 1)

    def test_partial_success_never_completes_the_job(self):
        rows = [_row(_work_id(n)) for n in range(1, 3)]
        result, _, _ = self._run(rows, [
            _accepted(),
            _runner_result('FAILED', worker.CROSSREF_DEPOSIT_REJECTED,
                           'deposit not accepted', external=True),
        ])

        self.assertNotEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual(result.outcome, worker.OUTCOME_FAILED)

    def test_named_pre_write_failure_after_accepted_work_stays_retryable(self):
        rows = [_row(_work_id(n)) for n in range(1, 3)]
        result, _, _ = self._run(rows, [
            _accepted(),
            _runner_result('FAILED', worker.CROSSREF_EXPORT_FAILED,
                           'export unavailable', external=False),
        ])

        self.assertEqual(result.error_code, worker.CROSSREF_EXPORT_FAILED)
        self.assertIs(result.retryable, True)

    def test_generic_code_after_a_possible_write_is_escalated(self):
        """A retryable classification is forbidden once a write may have run."""
        rows = [_row(_work_id(1))]
        result, _, _ = self._run(rows, [
            _runner_result('FAILED', worker.INTERNAL_WORKER_ERROR,
                           'unclassified', external=True),
        ])

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertIs(result.retryable, False)

    def test_malformed_runner_output_stops_work_and_is_not_trusted(self):
        rows = [_row(_work_id(n)) for n in range(1, 3)]
        for bad in [None, 'nope', {}, {'status': 'ACCEPTED'},
                    _runner_result('ACCEPTED', schema_version=2),
                    _runner_result('MAYBE'),
                    _runner_result('FAILED', 'NOT_A_TAXONOMY_CODE'),
                    _runner_result('FAILED', None),
                    _runner_result('ACCEPTED', 'CROSSREF_PREFIX_INVALID'),
                    {'schemaVersion': 1, 'status': 'FAILED',
                     'code': worker.CROSSREF_PREFIX_INVALID, 'detail': None,
                     'externalWriteStarted': 'yes'}]:
            with self.subTest(bad=bad):
                result, _, runner = self._run(rows, [bad])
                self.assertEqual(result.error_code,
                                 worker.CROSSREF_RUNNER_FAILED)
                self.assertIs(result.retryable, False)
                self.assertEqual(len(runner.launched), 1)

    def test_runner_timeout_is_indeterminate(self):
        rows = [_row(_work_id(1))]
        result, _, _ = self._run(
            rows, [worker.RunnerDeadlineExceeded('runner exceeded deadline')])

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertIs(result.retryable, False)

    def test_runner_launch_failure_is_runner_failed(self):
        rows = [_row(_work_id(1))]
        result, _, _ = self._run(rows, [OSError('cannot spawn')])

        self.assertEqual(result.error_code, worker.CROSSREF_RUNNER_FAILED)
        self.assertIs(result.retryable, False)

    def test_runner_detail_is_bounded_and_sanitised(self):
        rows = [_row(_work_id(1))]
        result, _, _ = self._run(rows, [
            _runner_result('FAILED', worker.CROSSREF_PREFIX_INVALID,
                           'x' * 5000 + '\nsecond line'),
        ])

        self.assertLessEqual(len(result.detail), worker.MAX_ERROR_DETAIL_CHARS)
        self.assertNotIn('\n', result.detail)


class TestLeaseBudget(unittest.TestCase):
    """No runner starts without a whole runner deadline plus safety margin."""

    def _run(self, rows, runner_results, lease_expires_at, now):
        transport = FakeTransport([
            {'claimDistributionJobs': [
                _claim(lease_expires_at=lease_expires_at)]},
            {'bookCount': len(rows)},
            {'books': rows},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        runner = RecordingRunner(runner_results)
        result = worker.DistributionJobWorker(
            transport=transport, runner=runner,
            clock=lambda: worker.parse_timestamp(now)).run_once()
        return result, transport, runner

    def test_required_budget_is_the_approved_guard(self):
        self.assertEqual(worker.LEASE_REQUIRED_SECONDS, 540)
        self.assertEqual(worker.RUNNER_DEADLINE_SECONDS, 240)
        self.assertEqual(worker.LEASE_SAFETY_MARGIN_SECONDS, 300)

    def test_guard_before_the_first_runner_performs_no_provider_write(self):
        result, _, runner = self._run(
            [_row(_work_id(1))], [],
            lease_expires_at='2026-08-27T17:05:00Z',
            now='2026-08-27T17:00:00Z')

        self.assertEqual(runner.launched, [])
        self.assertEqual(result.error_code, worker.INTERNAL_WORKER_ERROR)
        self.assertIs(result.retryable, True)

    def test_guard_after_accepted_work_is_terminal_lease_budget_exhausted(self):
        # Enough budget for the first runner only; the clock is read again
        # before the second and has moved past the guard.
        clock_values = iter([
            worker.parse_timestamp('2026-08-27T17:00:00Z'),
            worker.parse_timestamp('2026-08-27T17:06:00Z'),
        ])
        transport = FakeTransport([
            {'claimDistributionJobs': [
                _claim(lease_expires_at='2026-08-27T17:10:00Z')]},
            {'bookCount': 2},
            {'books': [_row(_work_id(1)), _row(_work_id(2))]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        runner = RecordingRunner([_accepted()])
        result = worker.DistributionJobWorker(
            transport=transport, runner=runner,
            clock=lambda: next(clock_values)).run_once()

        self.assertEqual(len(runner.launched), 1)
        self.assertEqual(result.error_code, worker.LEASE_BUDGET_EXHAUSTED)
        self.assertIs(result.retryable, False)

    def test_unparseable_lease_expiry_fails_before_any_provider_write(self):
        result, _, runner = self._run(
            [_row(_work_id(1))], [],
            lease_expires_at='not-a-timestamp',
            now='2026-08-27T17:00:00Z')

        self.assertEqual(runner.launched, [])
        self.assertEqual(result.error_code, worker.INTERNAL_WORKER_ERROR)


def _graphql_error(message, error_type, path=None):
    return thothapi.ThothWorkerResponseError([{
        'message': message,
        'path': path or ['completeDistributionJob'],
        'type': error_type,
    }])


def _transport_error():
    return thothapi.ThothWorkerTransportError('Thoth worker request failed')


class TestCompletionReconciliation(unittest.TestCase):
    """A lost completion response never causes provider work to repeat."""

    def _run(self, report_responses, runner_results=(_accepted(),)):
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim()]},
            {'bookCount': 1},
            {'books': [_row(_work_id(1))]},
        ] + list(report_responses))
        runner = RecordingRunner(list(runner_results))
        result = _build_worker(transport, runner).run_once()
        return result, transport, runner

    def test_completion_uses_the_exact_job_id_and_claim_token(self):
        _, transport, _ = self._run([
            {'completeDistributionJob': {'distributionJobId': JOB_ID}}])

        query, variables = transport.calls[-1]
        self.assertIn('completeDistributionJob', query)
        self.assertEqual(variables, {'data': {
            'distributionJobId': JOB_ID, 'claimToken': CLAIM_TOKEN}})

    def test_transport_ambiguity_retries_only_the_identical_mutation(self):
        result, transport, runner = self._run([
            _transport_error(),
            {'completeDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        # Exactly one runner ran: the retry invoked no provider work.
        self.assertEqual(len(runner.launched), 1)
        first = transport.calls[-2]
        second = transport.calls[-1]
        self.assertEqual(first, second)

    def test_pinned_terminal_succeeded_reconciles_an_ambiguous_completion(self):
        result, transport, runner = self._run([
            _transport_error(),
            _graphql_error(
                'The distribution job is already in the terminal state '
                'SUCCEEDED.',
                'DISTRIBUTION_JOB_TERMINAL'),
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_COMPLETED)
        self.assertEqual(len(runner.launched), 1)

    def test_terminal_succeeded_without_prior_ambiguity_is_hold(self):
        result, _, runner = self._run([
            _graphql_error(
                'The distribution job is already in the terminal state '
                'SUCCEEDED.',
                'DISTRIBUTION_JOB_TERMINAL'),
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_HOLD)
        self.assertEqual(len(runner.launched), 1)

    def test_other_terminal_state_is_hold(self):
        result, _, _ = self._run([
            _transport_error(),
            _graphql_error(
                'The distribution job is already in the terminal state '
                'FAILED.',
                'DISTRIBUTION_JOB_TERMINAL'),
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_HOLD)

    def test_drifted_terminal_message_is_hold_not_heuristic_match(self):
        result, _, _ = self._run([
            _transport_error(),
            _graphql_error(
                'The distribution job is already in terminal state SUCCEEDED',
                'DISTRIBUTION_JOB_TERMINAL'),
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_HOLD)

    def test_drifted_extension_type_is_hold(self):
        result, _, _ = self._run([
            _transport_error(),
            _graphql_error(
                'The distribution job is already in the terminal state '
                'SUCCEEDED.',
                'JOB_TERMINAL'),
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_HOLD)

    def test_stale_claim_on_completion_is_hold(self):
        result, _, _ = self._run([
            _graphql_error('The distribution job claim is no longer valid.',
                           'STALE_DISTRIBUTION_JOB_CLAIM'),
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_HOLD)

    def test_unresolved_transport_ambiguity_is_hold_and_never_replays(self):
        result, transport, runner = self._run(
            [_transport_error()] * worker.RESULT_REPORT_MAX_ATTEMPTS)

        self.assertEqual(result.outcome, worker.OUTCOME_HOLD)
        self.assertEqual(len(runner.launched), 1)
        completions = [c for c in transport.calls
                       if 'completeDistributionJob' in c[0]]
        self.assertEqual(len(completions), worker.RESULT_REPORT_MAX_ATTEMPTS)
        self.assertEqual(len(set(json.dumps(c, sort_keys=True)
                                 for c in completions)), 1)


class TestFailureReconciliation(unittest.TestCase):
    """A failure report may only ever repeat the identical mutation."""

    def _run(self, report_responses):
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim(kind='OTHER')]},
        ] + list(report_responses))
        runner = RecordingRunner()
        result = _build_worker(transport, runner).run_once()
        return result, transport, runner

    def test_failure_retry_repeats_the_identical_mutation(self):
        result, transport, runner = self._run([
            _transport_error(),
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_FAILED)
        self.assertEqual(runner.launched, [])
        failures = [c for c in transport.calls
                    if 'failDistributionJob' in c[0]]
        self.assertEqual(len(failures), 2)
        self.assertEqual(failures[0], failures[1])

    def test_unresolved_failure_report_is_hold_without_provider_work(self):
        result, transport, runner = self._run(
            [_transport_error()] * worker.RESULT_REPORT_MAX_ATTEMPTS)

        self.assertEqual(result.outcome, worker.OUTCOME_HOLD)
        self.assertEqual(runner.launched, [])

    def test_stale_claim_on_failure_report_is_hold(self):
        result, _, runner = self._run([
            _graphql_error('The distribution job claim is no longer valid.',
                           'STALE_DISTRIBUTION_JOB_CLAIM',
                           path=['failDistributionJob']),
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_HOLD)
        self.assertEqual(runner.launched, [])

    def test_terminal_response_on_failure_report_is_hold(self):
        result, _, _ = self._run([
            _graphql_error(
                'The distribution job is already in the terminal state '
                'FAILED.',
                'DISTRIBUTION_JOB_TERMINAL', path=['failDistributionJob']),
        ])

        self.assertEqual(result.outcome, worker.OUTCOME_HOLD)


class TestSubprocessRunnerLauncher(unittest.TestCase):
    """The parent reaches the legacy adapter only through a subprocess."""

    def test_parent_module_never_imports_legacy_uploader_code(self):
        source = open('distribution_job_worker.py').read()
        for forbidden in ['from crossrefuploader', 'import crossrefuploader',
                          'from uploader', 'import uploader',
                          'CrossrefUploader', 'disseminator']:
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)

    def test_launcher_invokes_the_runner_with_only_bounded_arguments(self):
        completed = MagicMock()
        completed.stdout = json.dumps(_accepted())
        completed.stderr = ''
        completed.returncode = 0

        with patch.object(worker.subprocess, 'run',
                          return_value=completed) as run:
            result = worker.launch_crossref_runner(
                _work_id(1), PUBLISHER_ID, 240)

        self.assertEqual(result, _accepted())
        argv = run.call_args.args[0]
        self.assertEqual(argv[1:], [
            worker.RUNNER_SCRIPT, '--work', _work_id(1),
            '--expected-publisher-id', PUBLISHER_ID])
        self.assertNotIn('--platform', argv)
        self.assertEqual(run.call_args.kwargs['timeout'], 240)
        self.assertTrue(run.call_args.kwargs['capture_output'])

    def test_launcher_converts_a_timeout_into_a_deadline_error(self):
        with patch.object(
                worker.subprocess, 'run',
                side_effect=worker.subprocess.TimeoutExpired('cmd', 240)):
            with self.assertRaises(worker.RunnerDeadlineExceeded):
                worker.launch_crossref_runner(_work_id(1), PUBLISHER_ID, 240)

    def test_launcher_returns_none_for_unparseable_stdout(self):
        completed = MagicMock()
        completed.stdout = 'not json at all'
        completed.stderr = ''
        completed.returncode = 1

        with patch.object(worker.subprocess, 'run', return_value=completed):
            result = worker.launch_crossref_runner(
                _work_id(1), PUBLISHER_ID, 240)

        # An untrustworthy result is surfaced as such, not guessed at.
        self.assertIsNone(result)

    def test_launcher_ignores_runner_stderr_logs(self):
        completed = MagicMock()
        completed.stdout = json.dumps(_accepted())
        completed.stderr = 'INFO: chatty operational log\n'
        completed.returncode = 0

        with patch.object(worker.subprocess, 'run', return_value=completed):
            result = worker.launch_crossref_runner(
                _work_id(1), PUBLISHER_ID, 240)

        self.assertEqual(result, _accepted())


class TestWorkerEntrypoint(unittest.TestCase):
    """`main` gates on activation and never fabricates a durable failure."""

    def test_enabled_worker_claims_exactly_once(self):
        transport = MagicMock()
        instance = MagicMock()
        instance.run_once.return_value = worker.WorkerResult(
            worker.OUTCOME_NO_JOB)

        with patch.dict(worker.environ,
                        {worker.WORKER_ENABLED_VARIABLE: 'ON'}, clear=True), \
                patch.object(thothapi, 'ThothWorkerTransport',
                             return_value=transport) as transport_class, \
                patch.object(worker, 'DistributionJobWorker',
                             return_value=instance):
            exit_code = worker.main()

        self.assertEqual(exit_code, 0)
        transport_class.assert_called_once_with()
        instance.run_once.assert_called_once_with()

    def test_authentication_failure_before_a_claim_is_an_invocation_failure(self):
        with patch.dict(worker.environ,
                        {worker.WORKER_ENABLED_VARIABLE: 'ON'}, clear=True), \
                patch.object(thothapi, 'ThothWorkerTransport',
                             side_effect=thothapi.ThothWorkerAuthError(
                                 'Missing value for THOTH_WORKER_TOKEN')), \
                patch.object(worker, 'DistributionJobWorker') as worker_class:
            exit_code = worker.main()

        # No job exists yet, so nothing may be reported against BE-04.
        self.assertEqual(exit_code, 1)
        worker_class.assert_not_called()

    def test_completed_job_exits_zero_and_hold_exits_non_zero(self):
        for outcome, expected in [(worker.OUTCOME_COMPLETED, 0),
                                  (worker.OUTCOME_NO_JOB, 0),
                                  (worker.OUTCOME_FAILED, 1),
                                  (worker.OUTCOME_HOLD, 1)]:
            with self.subTest(outcome=outcome):
                instance = MagicMock()
                instance.run_once.return_value = worker.WorkerResult(outcome)
                with patch.dict(
                        worker.environ,
                        {worker.WORKER_ENABLED_VARIABLE: 'ON'}, clear=True), \
                        patch.object(thothapi, 'ThothWorkerTransport'), \
                        patch.object(worker, 'DistributionJobWorker',
                                     return_value=instance):
                    self.assertEqual(worker.main(), expected)

    def test_transport_failure_during_a_claim_is_an_invocation_failure(self):
        instance = MagicMock()
        instance.run_once.side_effect = thothapi.ThothWorkerTransportError(
            'Thoth worker request failed')

        with patch.dict(worker.environ,
                        {worker.WORKER_ENABLED_VARIABLE: 'ON'}, clear=True), \
                patch.object(thothapi, 'ThothWorkerTransport'), \
                patch.object(worker, 'DistributionJobWorker',
                             return_value=instance):
            self.assertEqual(worker.main(), 1)

    def test_default_worker_uses_the_subprocess_launcher(self):
        transport = MagicMock()
        with patch.dict(worker.environ,
                        {worker.WORKER_ENABLED_VARIABLE: 'ON'}, clear=True), \
                patch.object(thothapi, 'ThothWorkerTransport',
                             return_value=transport), \
                patch.object(worker.DistributionJobWorker, 'run_once',
                             return_value=worker.WorkerResult(
                                 worker.OUTCOME_NO_JOB)) as run_once:
            worker.main()

        run_once.assert_called_once_with()


class TestWorkerActivationWorkflow(unittest.TestCase):
    """The activation workflow is bounded, serialised and inert by default."""

    WORKFLOW_PATH = '.github/workflows/distribution_job_worker.yml'

    @classmethod
    def setUpClass(cls):
        with open(cls.WORKFLOW_PATH) as workflow_file:
            cls.workflow = workflow_file.read()

    def test_concurrency_is_globally_serialised_without_cancellation(self):
        self.assertIn('group: distribution-job-worker', self.workflow)
        self.assertIn('cancel-in-progress: false', self.workflow)
        self.assertNotIn('cancel-in-progress: true', self.workflow)

    def test_workflow_runs_the_worker_entrypoint_once(self):
        self.assertIn('python distribution_job_worker.py', self.workflow)
        self.assertNotIn('matrix', self.workflow)

    def test_workflow_passes_the_activation_variable_to_python(self):
        # The executable guard is authoritative, so the value must reach it.
        self.assertIn(
            'DISTRIBUTION_JOB_WORKER_ENABLED: ${{ '
            'vars.DISTRIBUTION_JOB_WORKER_ENABLED }}',
            self.workflow)

    def test_workflow_never_disseminates_or_writes_locations(self):
        for forbidden in ['disseminator.py', 'write_locations.py',
                          'obtain_new_ids.py', 'reconcile_internet_archive.py',
                          'workflow_call', 'send-email']:
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, self.workflow)

    def test_workflow_requests_only_read_permission(self):
        self.assertIn('permissions:', self.workflow)
        self.assertIn('contents: read', self.workflow)
        self.assertNotIn('contents: write', self.workflow)

    def test_workflow_credential_exposure_is_scoped_to_crossref(self):
        # Publisher Crossref credentials only; no blanket secret export.
        self.assertNotIn('secrets: inherit', self.workflow)
        self.assertIn('include: ^CROSSREF_USER_, ^CROSSREF_PW_',
                      self.workflow)

    def test_no_existing_workflow_is_referenced_or_reused(self):
        self.assertNotIn('uses: ./.github/workflows/', self.workflow)


class TestRemainingSpecificationMatrix(unittest.TestCase):
    """Cases the specification names explicitly and that no other test owns."""

    def test_wrong_role_on_claim_is_an_invocation_failure(self):
        """A worker without DISSEMINATION_WORKER claims nothing to report on."""
        transport = FakeTransport([
            thothapi.ThothWorkerResponseError(
                [{'message': 'Unauthorized', 'type': 'NO_ACCESS'}])])
        runner = RecordingRunner()

        with self.assertRaises(thothapi.ThothWorkerResponseError):
            _build_worker(transport, runner).run_once()

        self.assertEqual(transport.operations(), ['claim'])
        self.assertEqual(runner.launched, [])

    def test_zero_candidate_no_op_only_after_the_fence_is_evaluated(self):
        """An empty catalogue is not a shortcut past the fence."""
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim(
                attempt_number=2, attempts=[_attempt(1, 'ABANDONED')])]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        runner = RecordingRunner()
        result = _build_worker(transport, runner).run_once()

        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertNotIn('bookCount', transport.operations())

    def test_crash_and_reclaim_never_redeposits_automatically(self):
        """A worker that died mid-attempt is fenced, not replayed.

        Attempt 1 accepted one deposit and then died, so BE-04 closed it as
        ABANDONED and granted attempt 2. The new attempt must reach neither
        the catalogue nor Crossref.
        """
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim(
                attempt_number=2,
                claim_token='00000000-0000-4000-8000-0000000000cd',
                attempts=[_attempt(2, None), _attempt(1, 'ABANDONED')])]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        runner = RecordingRunner()
        result = _build_worker(transport, runner).run_once()

        self.assertEqual(result.outcome, worker.OUTCOME_FAILED)
        self.assertEqual(result.error_code,
                         worker.CROSSREF_DEPOSIT_INDETERMINATE)
        self.assertIs(result.retryable, False)
        self.assertEqual(runner.launched, [])
        self.assertEqual(transport.operations(), ['claim', 'fail'])

    def test_runner_reported_publisher_mismatch_is_honoured_by_the_parent(self):
        """The execution-boundary publisher fence terminates the job."""
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim()]},
            {'bookCount': 2},
            {'books': [_row(_work_id(1)), _row(_work_id(2))]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        runner = RecordingRunner([_runner_result(
            'FAILED', worker.CATALOGUE_PUBLISHER_MISMATCH,
            'work belongs to another publisher')])
        result = _build_worker(transport, runner).run_once()

        self.assertEqual(result.error_code,
                         worker.CATALOGUE_PUBLISHER_MISMATCH)
        self.assertIs(result.retryable, False)
        self.assertEqual(len(runner.launched), 1)

    def test_runner_reported_missing_root_doi_is_metadata_invalid(self):
        transport = FakeTransport([
            {'claimDistributionJobs': [_claim()]},
            {'bookCount': 1},
            {'books': [_row(_work_id(1))]},
            {'failDistributionJob': {'distributionJobId': JOB_ID}},
        ])
        runner = RecordingRunner([_runner_result(
            'FAILED', worker.CROSSREF_METADATA_INVALID, 'no root DOI')])
        result = _build_worker(transport, runner).run_once()

        self.assertEqual(result.error_code, worker.CROSSREF_METADATA_INVALID)
        self.assertIs(result.retryable, False)

    def test_every_taxonomy_code_matches_the_upstream_shape(self):
        """BE-04 rejects any code outside ^[A-Z][A-Z0-9_]*$ up to 64 chars."""
        import re
        pattern = re.compile(r'^[A-Z][A-Z0-9_]*$')
        for code in worker.WORKER_ERROR_CODES:
            with self.subTest(code=code):
                self.assertRegex(code, pattern)
                self.assertLessEqual(len(code), 64)

    def test_retryable_codes_are_a_subset_of_the_taxonomy(self):
        self.assertTrue(
            worker.RETRYABLE_CODES.issubset(worker.WORKER_ERROR_CODES))

    def test_post_write_codes_are_never_retryable(self):
        for code in [worker.CROSSREF_DEPOSIT_REJECTED,
                     worker.CROSSREF_DEPOSIT_INDETERMINATE,
                     worker.CROSSREF_RUNNER_FAILED,
                     worker.LEASE_BUDGET_EXHAUSTED,
                     worker.CATALOGUE_TOO_LARGE,
                     worker.CATALOGUE_CONTRACT_INVALID,
                     worker.CATALOGUE_PUBLISHER_MISMATCH,
                     worker.UNSUPPORTED_JOB_KIND,
                     worker.UNSUPPORTED_TARGET_SET,
                     worker.CROSSREF_CREDENTIAL_MISSING,
                     worker.CROSSREF_METADATA_INVALID,
                     worker.CROSSREF_PREFIX_INVALID]:
            with self.subTest(code=code):
                self.assertNotIn(code, worker.RETRYABLE_CODES)
                self.assertIs(worker.WorkerFailure(code, 'x').retryable, False)

    def test_the_only_durable_operations_are_the_three_worker_mutations(self):
        """No lease renewal or heartbeat operation exists to be called."""
        documents = [
            worker.CLAIM_DISTRIBUTION_JOBS_MUTATION,
            worker.COMPLETE_DISTRIBUTION_JOB_MUTATION,
            worker.FAIL_DISTRIBUTION_JOB_MUTATION,
            worker.BACK_CATALOGUE_BOOK_COUNT_QUERY,
            worker.BACK_CATALOGUE_BOOKS_QUERY,
        ]
        mutations = [d for d in documents if d.lstrip().startswith('mutation')]
        self.assertEqual(len(mutations), 3)
        for document in documents:
            with self.subTest(document=document.split('(')[0].strip()):
                for forbidden in ['renewDistributionJob', 'heartbeat',
                                  'extendLease', 'cancelDistributionJob']:
                    self.assertNotIn(forbidden, document)

    def test_the_claim_request_never_supplies_a_worker_identity(self):
        """`claimedBy` is derived upstream and never submitted."""
        transport = FakeTransport([{'claimDistributionJobs': []}])
        _build_worker(transport, RecordingRunner()).run_once()

        query, variables = transport.calls[0]
        self.assertNotIn('claimedBy', query)
        self.assertNotIn('claimedBy', json.dumps(variables))
        self.assertEqual(sorted(variables['data']),
                         ['kinds', 'leaseSeconds', 'limit'])

    def test_the_worker_imports_no_persistence_layer(self):
        """There is no second durable store: BE-04 owns all job state."""
        import distribution_job_worker
        for forbidden in ['sqlite3', 'shelve', 'pickle', 'dbm', 'json.dump']:
            with self.subTest(forbidden=forbidden):
                self.assertFalse(
                    hasattr(distribution_job_worker, forbidden.split('.')[0])
                    and forbidden.split('.')[0] in
                    ('sqlite3', 'shelve', 'pickle', 'dbm'))
