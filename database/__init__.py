"""
Database module for Outbound Engine.

Provides Supabase integration for secure, hosted data storage.
"""

from .supabase_client import (
    SupabaseClient,
    DatabaseConfig,
    get_client
)

__all__ = [
    "SupabaseClient",
    "DatabaseConfig",
    "get_client"
]
