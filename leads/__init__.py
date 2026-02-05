"""
Lead management module for Outbound Engine.

Handles importing, validating, and deduplicating leads from external sources.
"""

from .importer import (
    LeadImporter,
    ValidationError,
    ImportResult
)

__all__ = [
    "LeadImporter",
    "ValidationError",
    "ImportResult"
]
