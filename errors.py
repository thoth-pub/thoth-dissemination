#!/usr/bin/env python3
"""
Custom errors for Dissemination Service
"""


class DisseminationError(Exception):
    """Exception to report generic errors"""

    def __init__(self, message):
        super().__init__(message)


class InternetArchiveIdentifierCollisionError(DisseminationError):
    """Refuse to modify an Internet Archive item not known to belong to Thoth."""


class InternetArchiveVerificationError(DisseminationError):
    """Report an Internet Archive upload which could not be verified in time."""


class InternetArchiveDesiredStateError(DisseminationError):
    """Report which source failed while constructing Archive desired state."""

    def __init__(self, source, message):
        self.source = source
        super().__init__(message)
