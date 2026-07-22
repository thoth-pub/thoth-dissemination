#!/usr/bin/env python3
"""Converge Thoth publication locations from dissemination output."""

from dataclasses import dataclass
import logging
from os import environ
import sys

from thothapi import (
    EXPLICIT_GRAPHQL_NULL,
    ThothGraphQLResponseError,
    ThothGraphQLTransportError,
    get_publication_locations,
    get_thoth_client,
)


logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(asctime)s: %(message)s')


class LocationWriteError(RuntimeError):
    """A requested location could not be safely converged."""


class MissingTokenError(LocationWriteError):
    """The required Thoth personal access token is missing."""


class AuthenticationError(LocationWriteError):
    """Thoth rejected the request as unauthenticated or unauthorised."""


class PublicationNotFoundError(LocationWriteError):
    """The requested publication does not exist in Thoth."""


class DuplicateLocationsError(LocationWriteError):
    """Multiple existing locations make an update ambiguous."""


class LocationLookupError(LocationWriteError):
    """Existing Thoth locations could not be retrieved safely."""


class LocationMutationError(LocationWriteError):
    """A Thoth location create or update mutation failed."""


class MalformedLocationError(ValueError, LocationWriteError):
    """A dissemination location line is malformed."""


@dataclass(frozen=True)
class LocationInput:
    publication_id: str
    location_platform: str
    landing_page: str
    full_text_url: str | None
    checksum: str | None
    checksum_algorithm: str | None


@dataclass(frozen=True)
class LocationPlan:
    action: str
    data: dict


MANAGED_FIELDS = (
    'landingPage',
    'fullTextUrl',
    'locationPlatform',
    'checksum',
    'checksumAlgorithm',
)

REQUIRED_EXISTING_FIELDS = (
    'locationId',
    'publicationId',
    'locationPlatform',
    'landingPage',
    'fullTextUrl',
    'canonical',
    'checksum',
    'checksumAlgorithm',
)


def validate_checksum_pair(checksum, checksum_algorithm, description):
    if (checksum is None) != (checksum_algorithm is None):
        raise MalformedLocationError(
            '{} must provide both checksum and checksum algorithm, or neither'.format(
                description
            )
        )


def parse_location_line(line, line_number):
    """Parse one six-field ``Location.__str__()`` output line."""
    stripped = line.strip()
    if not stripped:
        return None

    parts = stripped.split()
    if len(parts) != 6:
        raise MalformedLocationError(
            'Line {}: expected 6 fields, found {} in {!r}'.format(
                line_number, len(parts), stripped
            )
        )

    for index in (3, 4, 5):
        if parts[index] == 'None':
            parts[index] = None

    parsed = LocationInput(*parts)
    try:
        validate_checksum_pair(
            parsed.checksum,
            parsed.checksum_algorithm,
            'Line {} incoming location'.format(line_number),
        )
    except MalformedLocationError as error:
        raise MalformedLocationError(str(error)) from error
    return parsed


def configure_thoth_client():
    """Return a PAT-authenticated Thoth client."""
    token = environ.get('THOTH_PAT')
    if not token:
        raise MissingTokenError(
            'No Thoth token provided (THOTH_PAT environment variable not set)'
        )
    thoth = get_thoth_client()
    thoth.set_token(token)
    return thoth


def _is_authentication_error(error):
    text = str(error).lower()
    return any(marker in text for marker in (
        'authentication', 'unauthenticated', 'unauthorized', 'unauthorised',
        'authorization', 'authorisation', 'not authorized', 'not authorised',
        'forbidden', 'permission denied',
        'status code 401', 'status code 403',
    ))


def _is_publication_not_found_error(error):
    text = str(error).lower()
    return 'publication' in text and any(marker in text for marker in (
        'not found', 'no record was found', 'could not find',
    ))


def retrieve_existing_locations(thoth, publication_id):
    """Retrieve complete existing location state for a publication."""
    try:
        locations = get_publication_locations(thoth, publication_id)
    except ThothGraphQLResponseError as error:
        if _is_authentication_error(error):
            raise AuthenticationError(
                'Thoth authentication or authorisation failed: {}'.format(error)
            ) from error
        if _is_publication_not_found_error(error):
            raise PublicationNotFoundError(
                'Publication {} was not found: {}'.format(publication_id, error)
            ) from error
        raise LocationLookupError(
            'Thoth GraphQL lookup failed for publication {}: {}'.format(
                publication_id, error
            )
        ) from error
    except ThothGraphQLTransportError as error:
        raise LocationLookupError(
            'Thoth GraphQL transport failed for publication {}: {}'.format(
                publication_id, error
            )
        ) from error

    for location in locations:
        missing = [
            field for field in REQUIRED_EXISTING_FIELDS
            if field not in location
        ]
        if missing:
            raise LocationLookupError(
                'Location {} for publication {} omitted required fields: {}'.format(
                    location.get('locationId', '<unknown>'),
                    publication_id,
                    ', '.join(missing),
                )
            )
        if location['publicationId'] != publication_id:
            raise LocationLookupError(
                'Location {} belongs to publication {}, expected {}'.format(
                    location['locationId'],
                    location['publicationId'],
                    publication_id,
                )
            )
    return locations


def construct_desired_location(location_input, existing=None):
    """Build complete create/update state while preserving unmanaged values."""
    validate_checksum_pair(
        location_input.checksum,
        location_input.checksum_algorithm,
        'Incoming location',
    )

    checksum = location_input.checksum
    checksum_algorithm = location_input.checksum_algorithm
    if existing is not None and checksum is None:
        validate_checksum_pair(
            existing['checksum'],
            existing['checksumAlgorithm'],
            'Existing location {}'.format(existing['locationId']),
        )
        checksum = existing['checksum']
        checksum_algorithm = existing['checksumAlgorithm']

    desired = {
        'publicationId': location_input.publication_id,
        'landingPage': location_input.landing_page,
        'fullTextUrl': location_input.full_text_url,
        'locationPlatform': location_input.location_platform,
        'canonical': existing['canonical'] if existing is not None else False,
        'checksum': checksum,
        'checksumAlgorithm': checksum_algorithm,
    }
    if existing is not None:
        desired['locationId'] = existing['locationId']
    return desired


def decide_location_action(location_input, existing_locations):
    """Return a create, update, or no-op plan for one input location."""
    matches = [
        location for location in existing_locations
        if location['locationPlatform'] == location_input.location_platform
    ]
    if len(matches) > 1:
        location_ids = [location['locationId'] for location in matches]
        raise DuplicateLocationsError(
            'Publication {} has multiple {} locations: {}'.format(
                location_input.publication_id,
                location_input.location_platform,
                ', '.join(location_ids),
            )
        )

    if not matches:
        return LocationPlan(
            'create', construct_desired_location(location_input)
        )

    existing = matches[0]
    desired = construct_desired_location(location_input, existing)
    if all(existing[field] == desired[field] for field in MANAGED_FIELDS):
        return LocationPlan('noop', desired)
    return LocationPlan('update', desired)


def perform_location_plan(thoth, plan, progress=None):
    """Perform the planned mutation, if any, and return its location ID."""
    if plan.action == 'noop':
        logging.info(
            'Location %s for publication %s on %s is already current; no mutation needed',
            plan.data['locationId'],
            plan.data['publicationId'],
            plan.data['locationPlatform'],
        )
        return None

    mutation = (
        thoth.create_location if plan.action == 'create'
        else thoth.update_location
    )
    mutation_data = plan.data
    if plan.action == 'update' and plan.data['fullTextUrl'] is None:
        mutation_data = dict(plan.data)
        mutation_data['fullTextUrl'] = EXPLICIT_GRAPHQL_NULL
    action = '{}_thoth_location'.format(plan.action)
    if progress is not None:
        progress(action, 'attempted')
    try:
        location_id = mutation(mutation_data)
    except Exception as error:
        if _is_authentication_error(error):
            raise AuthenticationError(
                'Thoth authentication or authorisation failed during {}: {}'.format(
                    plan.action, error
                )
            ) from error
        raise LocationMutationError(
            'Thoth {} location mutation failed for publication {} on {}: {}'.format(
                plan.action,
                plan.data['publicationId'],
                plan.data['locationPlatform'],
                error,
            )
        ) from error

    if progress is not None:
        progress(action, 'completed')
    print(location_id)
    return location_id


def upsert_location(thoth, location_input, progress=None):
    existing = retrieve_existing_locations(thoth, location_input.publication_id)
    plan = decide_location_action(location_input, existing)
    return perform_location_plan(thoth, plan, progress=progress)


def write_thoth_location(publication_id, location_platform, landing_page,
                         full_text_url, checksum, checksum_algorithm,
                         thoth=None):
    """Compatibility entrypoint for converging one location."""
    location_input = LocationInput(
        publication_id,
        location_platform,
        landing_page,
        full_text_url,
        checksum,
        checksum_algorithm,
    )
    return upsert_location(thoth or configure_thoth_client(), location_input)


def process_locations_file(locations_file):
    thoth = None
    with open(locations_file, 'r') as locations:
        for line_number, line in enumerate(locations, start=1):
            location_input = parse_location_line(line, line_number)
            if location_input is None:
                continue
            if thoth is None:
                thoth = configure_thoth_client()
            upsert_location(thoth, location_input)


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1:
        logging.error('Usage: python write_locations.py <locations-file>')
        return 2
    try:
        process_locations_file(arguments[0])
    except (LocationWriteError, OSError) as error:
        logging.error('%s', error)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
