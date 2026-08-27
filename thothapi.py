#!/usr/bin/env python3
"""Helpers for configuring and patching Thoth client access."""

import json
import requests
from os import environ
from uuid import UUID


PUBLICATION_LOCATIONS_QUERY = """
query PublicationLocations($publicationId: Uuid!, $limit: Int!, $offset: Int!) {
  publication(publicationId: $publicationId) {
    publicationId
    locations(limit: $limit, offset: $offset) {
      locationId
      publicationId
      locationPlatform
      landingPage
      fullTextUrl
      canonical
      checksum
      checksumAlgorithm
    }
  }
}
"""

INTERNET_ARCHIVE_SELECTION_QUERY = """
query InternetArchiveSelection(
  $limit: Int!
  $offset: Int!
  $publishers: [Uuid!]!
  $workTypes: [WorkType!]!
  $workStatuses: [WorkStatus!]!
  $updatedAtWithRelations: TimeExpression!
) {
  works(
    limit: $limit
    offset: $offset
    publishers: $publishers
    workTypes: $workTypes
    workStatuses: $workStatuses
    order: {field: UPDATED_AT_WITH_RELATIONS, direction: ASC}
    updatedAtWithRelations: $updatedAtWithRelations
  ) {
    workId
    updatedAtWithRelations
    workStatus
    workType
    publications {
      publicationType
      locations {
        canonical
        fullTextUrl
      }
    }
  }
}
"""


DISTRIBUTION_PLATFORM_OPTIONS_QUERY = """
query DistributionPlatformOptions {
  distributionPlatformOptions {
    platform
    displayLabel
    linkedGroup
    backCatalogueBehaviour
    assignable
  }
}
"""

DISTRIBUTION_PLATFORM_PUBLISHER_COUNT_QUERY = """
query PublisherCountByDistributionPlatform($platform: DistributionPlatform!) {
  publisherCountByDistributionPlatform(platform: $platform)
}
"""

DISTRIBUTION_PLATFORM_PUBLISHERS_QUERY = """
query PublishersByDistributionPlatform(
  $platform: DistributionPlatform!
  $limit: Int!
  $offset: Int!
) {
  publishersByDistributionPlatform(
    platform: $platform
    limit: $limit
    offset: $offset
    order: {field: PUBLISHER_ID, direction: ASC}
  ) {
    publisherId
  }
}
"""

# Publisher Services pages are explicitly bounded: the public contract accepts
# a maximum of 100 and an explicit null or zero limit is never sent.
DISTRIBUTION_PLATFORM_PAGE_SIZE = 100

# The DIS-02A worker's bearer token is provisioned separately from every other
# credential in this repository and carries only the DISSEMINATION_WORKER role.
WORKER_TOKEN_VARIABLE = 'THOTH_WORKER_TOKEN'

# Explicit bounds for every worker GraphQL call. The worker holds a
# non-renewable lease, so an unbounded read is never acceptable.
WORKER_CONNECT_TIMEOUT_SECONDS = 10
WORKER_READ_TIMEOUT_SECONDS = 60

# A worker response is a small bounded document: one claim, one count, at most
# ten catalogue rows, or one mutation acknowledgement. Anything materially
# larger is refused unparsed rather than loaded into memory.
WORKER_MAX_RESPONSE_BYTES = 1048576

# Only these GraphQL error fields ever reach a log or a durable diagnostic.
WORKER_SAFE_ERROR_FIELDS = ('message', 'path')


class ThothGraphQLTransportError(RuntimeError):
    """The Thoth GraphQL endpoint could not return a usable response."""


class ThothGraphQLResponseError(RuntimeError):
    """The Thoth GraphQL endpoint returned one or more GraphQL errors."""


class ThothPublisherDiscoveryError(RuntimeError):
    """Publisher assignments could not be reconciled from the Thoth API."""


class ThothWorkerTransportError(RuntimeError):
    """A worker GraphQL call produced no usable response."""


class ThothWorkerResponseError(RuntimeError):
    """A worker GraphQL call returned sanitised GraphQL errors.

    `errors` carries only bounded `message`, `path` and `extensions.type`
    values, which is what the result-report reconciliation protocol matches
    on. No other part of the response is retained.
    """

    def __init__(self, errors):
        self.errors = errors
        super().__init__(json.dumps(errors, sort_keys=True))


class ThothWorkerAuthError(RuntimeError):
    """The worker has no usable bearer token, so it cannot authenticate.

    Raised before any job is claimed. It is an invocation failure and must
    never be converted into a fabricated durable-job failure report.
    """


class _ExplicitGraphQLNull:
    """Sentinel for nullable mutation fields that must be explicitly cleared."""


EXPLICIT_GRAPHQL_NULL = _ExplicitGraphQLNull()


def get_thoth_client_url(client_url=None):
    """
    Return the Thoth API base URL expected by thothlibrary.

    Accept either a base URL such as `https://api.thoth.pub` or a GraphQL URL
    such as `https://api.thoth.pub/graphql`.
    """
    resolved_url = client_url or environ.get('THOTH_API_URL')
    if resolved_url is None:
        return None

    stripped_url = resolved_url.rstrip('/')
    if stripped_url.endswith('/graphql'):
        return stripped_url[:-8]
    return stripped_url


def patch_thoth_client_mutations():
    """Render native Python booleans as valid GraphQL boolean literals."""
    from thothlibrary.mutation import ThothMutation

    if getattr(ThothMutation, '_thoth_dissemination_boolean_patch', False):
        return

    original_statement = ThothMutation._statement

    @staticmethod
    def statement(key, value, enclose):
        if value is EXPLICIT_GRAPHQL_NULL:
            return '{}: null'.format(key)
        if isinstance(value, bool):
            return '{}: {}'.format(key, str(value).lower())
        return original_statement(key, value, enclose)

    ThothMutation._statement = statement
    ThothMutation._thoth_dissemination_boolean_patch = True


def sanitised_graphql_errors(payload_errors):
    """Reduce GraphQL errors to bounded, useful, non-sensitive fields."""
    useful_errors = []
    for error in payload_errors:
        if isinstance(error, dict):
            useful_errors.append({
                key: error[key] for key in ('message', 'path')
                if key in error
            })
        else:
            useful_errors.append(str(error))
    return json.dumps(useful_errors, sort_keys=True)


def sanitised_worker_graphql_errors(payload_errors):
    """Reduce worker GraphQL errors to message, path and extensions.type.

    `extensions.type` is retained because the result-report reconciliation
    protocol is pinned to it. Every other extension key is discarded unread:
    an upstream diagnostic must never become a durable job detail.
    """
    if not isinstance(payload_errors, list):
        return [{'message': 'Thoth returned a malformed GraphQL error list'}]

    sanitised = []
    for error in payload_errors:
        if not isinstance(error, dict):
            sanitised.append({'message': 'Thoth returned a malformed GraphQL error'})
            continue
        useful = {
            key: error[key] for key in WORKER_SAFE_ERROR_FIELDS if key in error
        }
        extensions = error.get('extensions')
        if isinstance(extensions, dict) and isinstance(
                extensions.get('type'), str):
            useful['type'] = extensions['type']
        sanitised.append(useful)
    return sanitised


def get_publication_locations(thoth, publication_id):
    """Return complete location data for one publication."""
    page_size = 100
    offset = 0
    all_locations = []

    while True:
        try:
            response = thoth.client.execute(
                PUBLICATION_LOCATIONS_QUERY,
                {
                    'publicationId': publication_id,
                    'limit': page_size,
                    'offset': offset,
                },
            )
        except Exception as error:
            raise ThothGraphQLTransportError(str(error)) from error

        try:
            payload = json.loads(response)
        except (TypeError, ValueError) as error:
            raise ThothGraphQLTransportError(
                'Invalid JSON response from Thoth: {}'.format(error)
            ) from error

        if payload.get('errors'):
            raise ThothGraphQLResponseError(
                json.dumps(payload['errors'], sort_keys=True)
            )

        try:
            publication = payload['data']['publication']
        except (KeyError, TypeError) as error:
            raise ThothGraphQLTransportError(
                'Thoth response did not contain publication data'
            ) from error

        if publication is None:
            raise ThothGraphQLResponseError(
                'Publication {} was not found'.format(publication_id)
            )
        if publication.get('publicationId') != publication_id:
            raise ThothGraphQLTransportError(
                'Thoth returned publication {} while looking up {}'.format(
                    publication.get('publicationId'), publication_id
                )
            )
        page = publication.get('locations')
        if not isinstance(page, list):
            raise ThothGraphQLTransportError(
                'Thoth response did not contain a locations list for publication {}'.format(
                    publication_id
                )
            )

        all_locations.extend(page)
        if len(page) < page_size:
            return all_locations
        offset += page_size


def get_internet_archive_selection_works(
        thoth, publisher_ids, work_types, updated_after, page_size=100):
    """Return every IA selection candidate using a bounded GraphQL query."""
    if not isinstance(page_size, int) or page_size < 1 or page_size > 100:
        raise ValueError('page_size must be between 1 and 100')

    offset = 0
    all_works = []
    while True:
        variables = {
            'limit': page_size,
            'offset': offset,
            'publishers': list(publisher_ids),
            'workTypes': list(work_types),
            'workStatuses': ['ACTIVE'],
            'updatedAtWithRelations': {
                'timestamp': updated_after,
                'expression': 'GREATER_THAN',
            },
        }
        try:
            response = thoth.client.execute(
                INTERNET_ARCHIVE_SELECTION_QUERY, variables)
        except Exception as error:
            raise ThothGraphQLTransportError(
                'Unable to query Internet Archive selection candidates: {}'.format(
                    error)
            ) from error

        try:
            payload = json.loads(response) if isinstance(response, str) else response
        except (TypeError, ValueError) as error:
            raise ThothGraphQLTransportError(
                'Invalid JSON response from Thoth: {}'.format(error)
            ) from error

        if not isinstance(payload, dict):
            raise ThothGraphQLTransportError(
                'Thoth selection response was not an object')
        if payload.get('errors'):
            raise ThothGraphQLResponseError(
                sanitised_graphql_errors(payload['errors']))

        try:
            page = payload['data']['works']
        except (KeyError, TypeError) as error:
            raise ThothGraphQLTransportError(
                'Thoth selection response did not contain a works list'
            ) from error
        if not isinstance(page, list):
            raise ThothGraphQLTransportError(
                'Thoth selection response did not contain a works list')

        all_works.extend(page)
        if len(page) < page_size:
            return all_works
        offset += page_size


def _execute_publisher_discovery_query(
        thoth, query, variables, description='publisher assignments'):
    """Execute one read-only publisher-discovery query and return its data."""
    try:
        response = thoth.client.execute(query, variables)
    except Exception as error:
        raise ThothGraphQLTransportError(
            'Unable to query {}: {}'.format(description, error)
        ) from error

    try:
        payload = json.loads(response) if isinstance(response, str) else response
    except (TypeError, ValueError) as error:
        raise ThothGraphQLTransportError(
            'Invalid JSON response from Thoth: {}'.format(error)
        ) from error

    if not isinstance(payload, dict):
        raise ThothGraphQLTransportError(
            'Thoth {} response was not an object'.format(description))
    if payload.get('errors'):
        raise ThothGraphQLResponseError(
            sanitised_graphql_errors(payload['errors']))

    data = payload.get('data')
    if not isinstance(data, dict):
        raise ThothGraphQLTransportError(
            'Thoth {} response did not contain data'.format(description))
    return data


def get_distribution_platform_options(thoth):
    """
    Return the running `distributionPlatformOptions` contract descriptors.

    This is a public, anonymous, read-only surface of the pinned Thoth v1.7.0
    contract. The rows are returned exactly as the API ordered them, because
    that order is itself part of the contract that the caller validates.
    """
    data = _execute_publisher_discovery_query(
        thoth,
        DISTRIBUTION_PLATFORM_OPTIONS_QUERY,
        {},
        description='distribution platform options',
    )
    options = data.get('distributionPlatformOptions')
    if not isinstance(options, list):
        raise ThothPublisherDiscoveryError(
            'Thoth did not return a distribution platform option list')
    return options


def get_distribution_platform_publisher_count(thoth, platform):
    """Return the reported number of publishers enabled for one platform."""
    data = _execute_publisher_discovery_query(
        thoth,
        DISTRIBUTION_PLATFORM_PUBLISHER_COUNT_QUERY,
        {'platform': platform},
    )
    count = data.get('publisherCountByDistributionPlatform')
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ThothPublisherDiscoveryError(
            'Thoth reported an invalid publisher count for {}'.format(platform))
    return count


def get_distribution_platform_publisher_ids(
        thoth, platform, page_size=DISTRIBUTION_PLATFORM_PAGE_SIZE):
    """
    Return every publisher ID enabled for one distribution platform.

    The result is complete, deterministically ordered, free of duplicate or
    malformed identities, and reconciled against
    `publisherCountByDistributionPlatform`. Any anomaly is an error rather
    than a smaller publisher set: an empty result is legitimate only when the
    reported count is zero and the fully consumed result is also empty.
    """
    if (isinstance(page_size, bool) or not isinstance(page_size, int)
            or page_size < 1
            or page_size > DISTRIBUTION_PLATFORM_PAGE_SIZE):
        raise ValueError('page_size must be between 1 and {}'.format(
            DISTRIBUTION_PLATFORM_PAGE_SIZE))

    expected_count = get_distribution_platform_publisher_count(thoth, platform)

    publisher_ids = []
    seen = set()
    offset = 0
    while True:
        data = _execute_publisher_discovery_query(
            thoth,
            DISTRIBUTION_PLATFORM_PUBLISHERS_QUERY,
            {'platform': platform, 'limit': page_size, 'offset': offset},
        )
        page = data.get('publishersByDistributionPlatform')
        if not isinstance(page, list):
            raise ThothPublisherDiscoveryError(
                'Thoth did not return a publisher list for {}'.format(platform))
        if len(page) > page_size:
            raise ThothPublisherDiscoveryError(
                'Thoth returned {} publishers for {} on a page of {}'.format(
                    len(page), platform, page_size))

        for entry in page:
            if not isinstance(entry, dict):
                raise ThothPublisherDiscoveryError(
                    'Thoth returned a malformed publisher record for {}'.format(
                        platform))
            raw_publisher_id = entry.get('publisherId')
            if not isinstance(raw_publisher_id, str):
                raise ThothPublisherDiscoveryError(
                    'Thoth returned a publisher without a usable ID for {}'.format(
                        platform))
            try:
                publisher_id = str(UUID(raw_publisher_id))
            except (ValueError, AttributeError) as error:
                raise ThothPublisherDiscoveryError(
                    'Thoth returned a malformed publisher ID for {}'.format(
                        platform)
                ) from error
            if publisher_id in seen:
                raise ThothPublisherDiscoveryError(
                    'Thoth returned publisher {} more than once for {}'.format(
                        publisher_id, platform))
            seen.add(publisher_id)
            publisher_ids.append(publisher_id)

        if len(publisher_ids) > expected_count:
            raise ThothPublisherDiscoveryError(
                'Thoth returned more than the reported {} publishers for {}'.format(
                    expected_count, platform))
        if len(page) < page_size:
            break
        offset += page_size

    if len(publisher_ids) != expected_count:
        raise ThothPublisherDiscoveryError(
            'Thoth returned {} publishers for {} but reported {}'.format(
                len(publisher_ids), platform, expected_count))
    return sorted(publisher_ids)


def get_thoth_client(client_url=None):
    """Instantiate a Thoth client using an optional endpoint override."""
    from thothlibrary import ThothClient

    patch_thoth_client_mutations()
    resolved_url = get_thoth_client_url(client_url)
    return ThothClient(resolved_url) if resolved_url else ThothClient()


class ThothWorkerTransport():
    """Bounded GraphQL transport for the DIS-02A durable-job worker.

    `thothlibrary`'s HTTP transport applies no request timeout, which is
    unusable while holding a non-renewable BE-04 lease. This transport is
    deliberately narrow: it serves only the worker's claim, catalogue and
    result-report operations, always with explicit timeouts.

    The bearer token is read once from `THOTH_WORKER_TOKEN` and is never
    logged, stringified into an error, or reflected into any diagnostic.
    """

    def __init__(self, client_url=None, session=None):
        token = environ.get(WORKER_TOKEN_VARIABLE)
        if token is None or len(token.strip()) < 1:
            raise ThothWorkerAuthError(
                'Missing value for {}'.format(WORKER_TOKEN_VARIABLE))
        self._token = token.strip()
        self._session = session if session is not None else requests
        base_url = get_thoth_client_url(client_url)
        if base_url is None:
            raise ThothWorkerAuthError(
                'Missing Thoth API URL: set THOTH_API_URL')
        self._endpoint = '{}/graphql'.format(base_url)

    def execute(self, query, variables):
        """Run one bounded worker GraphQL operation and return its `data`.

        Raises `ThothWorkerTransportError` when no usable response arrived and
        `ThothWorkerResponseError` when Thoth answered with GraphQL errors.
        Neither carries the bearer token, the request body or the raw
        response: upstream exception text is reduced to its exception class.
        """
        try:
            response = self._session.post(
                url=self._endpoint,
                json={'query': query, 'variables': variables},
                headers={
                    'Authorization': 'Bearer {}'.format(self._token),
                    'Content-Type': 'application/json',
                },
                timeout=(WORKER_CONNECT_TIMEOUT_SECONDS,
                         WORKER_READ_TIMEOUT_SECONDS),
            )
        except Exception as error:
            # The request carries the bearer token, so `str(error)` may quote
            # it back. Only the exception class is ever reported.
            raise ThothWorkerTransportError(
                'Thoth worker request failed: {}'.format(
                    type(error).__name__)) from None

        status_code = getattr(response, 'status_code', None)
        if status_code != 200:
            raise ThothWorkerTransportError(
                'Thoth worker request returned status {}'.format(status_code))

        body = getattr(response, 'text', '')
        if isinstance(body, str) and len(body) > WORKER_MAX_RESPONSE_BYTES:
            raise ThothWorkerTransportError(
                'Thoth worker response exceeded the bounded size limit')

        try:
            payload = response.json()
        except Exception as error:
            raise ThothWorkerTransportError(
                'Thoth worker response was not JSON: {}'.format(
                    type(error).__name__)) from None

        if not isinstance(payload, dict):
            raise ThothWorkerTransportError(
                'Thoth worker response was not an object')

        if payload.get('errors'):
            raise ThothWorkerResponseError(
                sanitised_worker_graphql_errors(payload['errors']))

        data = payload.get('data')
        if not isinstance(data, dict):
            raise ThothWorkerTransportError(
                'Thoth worker response contained no data object')
        return data
