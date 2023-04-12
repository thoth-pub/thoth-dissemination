#!/usr/bin/env python3
"""
Custom errors for Dissemination Service
"""

class DisseminationError(Exception):
    """Exception to report generic errors"""
    def __init__(self, message):
        super().__init__(message)
