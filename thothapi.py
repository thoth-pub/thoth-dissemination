#!/usr/bin/env python3
"""Helpers for configuring and patching Thoth client access."""

from os import environ


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


def get_thoth_client(client_url=None):
    """Instantiate a patched Thoth client using an optional endpoint override."""
    from thothlibrary import ThothClient

    patch_thoth_client_queries()
    resolved_url = get_thoth_client_url(client_url)
    return ThothClient(resolved_url) if resolved_url else ThothClient()
