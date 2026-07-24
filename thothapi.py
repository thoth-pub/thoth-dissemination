#!/usr/bin/env python3
"""Helpers for configuring and patching Thoth client access."""

import json
from os import environ


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


class ThothGraphQLTransportError(RuntimeError):
    """The Thoth GraphQL endpoint could not return a usable response."""


class ThothGraphQLResponseError(RuntimeError):
    """The Thoth GraphQL endpoint returned one or more GraphQL errors."""


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


def patch_thoth_client_queries():
    """
    Patch the thothlibrary work queries for thoth-dissemination's needs.

    Two adjustments are applied to the `Work` query field selections:

    1. The released client still requests `workFeaturedVideos` on `Work`, but the
       launch schema exposes a singular `featuredVideo` field. thoth-dissemination
       does not consume featured-video data, so removing that selection is safe.

    2. The client hardcodes `markupFormat: JATS_XML` on the canonical `titles`
       and `abstracts` selections. Internet Archive (and other plain-text
       consumers such as the BKCI CSV and the SWORD Dublin Core profiles) strip
       or cannot render that markup, which prevents dissemination from ever
       converging. We request `PLAIN_TEXT` for the work title/abstract instead
       of stripping the markup ourselves downstream. Only the top-level
       `titles`/`abstracts` selections are rewritten; other JATS_XML selections
       (biographies, relations, awards, etc.) are left untouched as they are not
       consumed as the work title/abstract.
    """
    from thothlibrary import ThothClient

    def _patch_field(field):
        stripped = field.lstrip()
        if stripped.startswith('titles(') or stripped.startswith('abstracts('):
            return field.replace(
                'markupFormat: JATS_XML', 'markupFormat: PLAIN_TEXT')
        return field

    for query_name in [
        'work',
        'workByDoi',
        'bookByDoi',
        'chapterByDoi',
        'works',
        'books',
        'chapters',
    ]:
        query_spec = ThothClient.QUERIES.get(query_name)
        if query_spec is None or 'fields' not in query_spec:
            continue

        query_spec['fields'] = [
            _patch_field(field)
            for field in query_spec['fields']
            if not field.lstrip().startswith('workFeaturedVideos ')
        ]


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
            useful_errors = []
            for error in payload['errors']:
                if isinstance(error, dict):
                    useful_errors.append({
                        key: error[key] for key in ('message', 'path')
                        if key in error
                    })
                else:
                    useful_errors.append(str(error))
            raise ThothGraphQLResponseError(
                json.dumps(useful_errors, sort_keys=True)
            )

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


def get_thoth_client(client_url=None):
    """Instantiate a patched Thoth client using an optional endpoint override."""
    from thothlibrary import ThothClient

    patch_thoth_client_queries()
    patch_thoth_client_mutations()
    resolved_url = get_thoth_client_url(client_url)
    return ThothClient(resolved_url) if resolved_url else ThothClient()
