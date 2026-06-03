"""Unit tests for ``predict_lib.conn_url.normalise_database_url``.

NOT DRY by design (per ``feedback_typescript_rules_strict`` rule 40, applied to
Python tests in this repo): every case is its own ``test_*`` function with literal
expected strings, no shared fixtures.
"""

from __future__ import annotations

from predict_lib.conn_url import normalise_database_url


def test_clean_url_unchanged() -> None:
    assert (
        normalise_database_url("postgresql://user:pw@host.example.com/db?sslmode=require")
        == "postgresql://user:pw@host.example.com/db?sslmode=require"
    )


def test_wrapping_single_quotes_stripped() -> None:
    assert (
        normalise_database_url("'postgresql://user:pw@host.example.com/db?sslmode=require'")
        == "postgresql://user:pw@host.example.com/db?sslmode=require"
    )


def test_wrapping_double_quotes_stripped() -> None:
    assert (
        normalise_database_url('"postgresql://user:pw@host.example.com/db?sslmode=require"')
        == "postgresql://user:pw@host.example.com/db?sslmode=require"
    )


def test_leading_and_trailing_whitespace_stripped() -> None:
    assert (
        normalise_database_url("   postgresql://user:pw@host.example.com/db?sslmode=require\n")
        == "postgresql://user:pw@host.example.com/db?sslmode=require"
    )


def test_outer_quotes_with_padding_stripped() -> None:
    assert (
        normalise_database_url("  'postgresql://user:pw@host.example.com/db?sslmode=require'  ")
        == "postgresql://user:pw@host.example.com/db?sslmode=require"
    )


def test_unmatched_leading_quote_preserved() -> None:
    # Asymmetric quoting is not stripped — preserve the user-supplied value so
    # the downstream parser surfaces the real shape rather than silently mutating.
    assert (
        normalise_database_url("'postgresql://user:pw@host.example.com/db?sslmode=require")
        == "'postgresql://user:pw@host.example.com/db?sslmode=require"
    )


def test_unmatched_trailing_quote_preserved() -> None:
    assert (
        normalise_database_url("postgresql://user:pw@host.example.com/db?sslmode=require'")
        == "postgresql://user:pw@host.example.com/db?sslmode=require'"
    )


def test_inner_quotes_not_touched() -> None:
    assert (
        normalise_database_url("postgresql://user:pw'with'quote@host.example.com/db")
        == "postgresql://user:pw'with'quote@host.example.com/db"
    )


def test_empty_string_returned_as_is() -> None:
    assert normalise_database_url("") == ""


def test_single_character_returned_as_is() -> None:
    assert normalise_database_url("'") == "'"


def test_idempotent_on_already_clean() -> None:
    once = normalise_database_url("postgresql://h/d?sslmode=require")
    twice = normalise_database_url(once)
    assert twice == "postgresql://h/d?sslmode=require"


def test_idempotent_on_quoted_input() -> None:
    once = normalise_database_url("'postgresql://h/d?sslmode=require'")
    twice = normalise_database_url(once)
    assert twice == "postgresql://h/d?sslmode=require"


def test_only_whitespace_returns_empty() -> None:
    assert normalise_database_url("   \n\t  ") == ""
