"""Raw-clock examples shared with the race-detail decoder contract."""

from __future__ import annotations

import duckdb
import pytest
from learning.race_time import (
    encoded_race_time_seconds,
    encoded_race_time_seconds_sql,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, None),
        ("", None),
        ("  ", None),
        ("0000", None),
        ("0", None),
        ("1234", 83.4),
        (" 1234 ", 83.4),
        ("1599", 119.9),
        ("2000", 120.0),
        ("1000", 60.0),
        ("599", 59.9),
        ("1", 0.1),
        ("12", 1.2),
        ("9599", 599.9),
        ("1600", None),
        ("600", None),
        ("9999", None),
        ("01234", None),
        ("83.4", None),
        ("1:23.4", None),
        ("-123", None),
        ("+123", None),
        ("１２３４", None),
        ("NaN", None),
    ],
)
def test_scalar_decoder(raw: str | None, expected: float | None) -> None:
    assert encoded_race_time_seconds(raw) == expected


def test_sql_decoder_matches_minute_boundaries_and_rejects_invalid_values() -> None:
    with duckdb.connect() as con:
        result = con.execute(
            f"select {encoded_race_time_seconds_sql('raw')} from "
            "(values ('1234'), ('1599'), ('2000'), ('599'), ('1'), "
            "('0000'), ('1600'), ('83.4'), ('01234'), ('１２３４'), "
            "(NULL), (' 1234 '), ('-123'), ('+123'), ('')) t(raw)"
        ).fetchall()
    assert result == [
        (83.4,),
        (119.9,),
        (120.0,),
        (59.9,),
        (0.1,),
        (None,),
        (None,),
        (None,),
        (None,),
        (None,),
        (None,),
        (83.4,),
        (None,),
        (None,),
        (None,),
    ]


def test_sql_decoder_accepts_integer_encoded_catalog_values() -> None:
    with duckdb.connect() as con:
        result = con.execute(
            f"select {encoded_race_time_seconds_sql('raw')} "
            "from (values (1234), (599), (0), (1600)) t(raw)"
        ).fetchall()
    assert result == [(83.4,), (59.9,), (None,), (None,)]
