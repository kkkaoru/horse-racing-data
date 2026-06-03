"""Defensive ``NEON_DATABASE_URL`` normaliser.

The Cloudflare Workers ``wrangler secret put`` flow accepts arbitrary bytes on
stdin, including wrapping single / double quotes if the operator copy-pasted the
URL with surrounding shell quoting. psycopg's ``connect()`` parses the conninfo
as a URI only when it starts with ``postgres://`` / ``postgresql://``; a leading
quote makes psycopg fall back to keyword parsing and raise

    psycopg.ProgrammingError: invalid connection option "'postgresql://..."

This module strips matching wrapping quotes and surrounding whitespace before the
URL reaches psycopg. Pure + unit-tested so future paste mistakes can never cost
us another silent container crash.
"""

from __future__ import annotations

from typing import Final

_QUOTE_CHARS: Final[tuple[str, ...]] = ("'", '"')


def _strip_matching_wrapping_quote(value: str) -> str:
    if len(value) < 2:
        return value
    first = value[0]
    if first not in _QUOTE_CHARS:
        return value
    if value[-1] != first:
        return value
    return value[1:-1]


def normalise_database_url(raw: str) -> str:
    """Strip wrapping quotes + surrounding whitespace from a Postgres URL.

    Idempotent: applying it twice yields the same result as once.
    """
    stripped = raw.strip()
    return _strip_matching_wrapping_quote(stripped).strip()


def resolve_source_url(raw: str | None, default_url: str) -> str:
    """Resolve the SOURCE_DATABASE_URL with NEON_DATABASE_URL as fallback.

    The feature-build subprocess reads SOURCE; predictions UPSERT goes to NEON.
    When SOURCE is unset / empty / whitespace-only, fall back to ``default_url``
    so existing deployments (single NEON URL) keep working unchanged. When
    SOURCE is provided, it is run through ``normalise_database_url`` to strip
    accidental wrapping quotes the same way the NEON URL is.
    """
    if not raw or not raw.strip():
        return default_url
    return normalise_database_url(raw)
