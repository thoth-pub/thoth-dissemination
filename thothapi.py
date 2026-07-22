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
    Patch known query mismatches between thothlibrary 1.0.0 and the launch schema.

    The released client still requests `workFeaturedVideos` on `Work`, but the
    launch schema exposes a singular `featuredVideo` field. thoth-dissemination
    does not consume featured-video data, so removing that selection is safe.
    """
    from thothlibrary import ThothClient

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
            field for field in query_spec['fields']
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


def get_thoth_client(client_url=None):
    """Instantiate a patched Thoth client using an optional endpoint override."""
    from thothlibrary import ThothClient

    patch_thoth_client_queries()
    patch_thoth_client_mutations()
    resolved_url = get_thoth_client_url(client_url)
    return ThothClient(resolved_url) if resolved_url else ThothClient()
