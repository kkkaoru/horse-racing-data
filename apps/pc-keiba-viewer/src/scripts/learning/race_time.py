"""Decode raw MSSd race clocks using the race-detail page's contract.

Do not apply this decoder to already normalized seconds or sectional times.
Changing a historical feature's units requires retraining and serving parity.
"""

from __future__ import annotations

import re
from typing import Final

_ENCODED_PATTERN: Final[re.Pattern[str]] = re.compile(r"[0-9]{1,4}")
_SECONDS_PER_MINUTE: Final[int] = 60
_TENTHS_PER_SECOND: Final[int] = 10
_MINUTE_ENCODING: Final[int] = 1000


def encoded_race_time_seconds(value: str | None) -> float | None:
    """Mirror ``parseEncodedRaceTimeTenths`` and return seconds, not tenths."""
    if value is None:
        return None
    cleaned = value.strip()
    if _ENCODED_PATTERN.fullmatch(cleaned) is None:
        return None
    encoded = int(cleaned)
    minutes, remainder = divmod(encoded, _MINUTE_ENCODING)
    seconds, tenths = divmod(remainder, _TENTHS_PER_SECOND)
    if encoded == 0 or seconds >= _SECONDS_PER_MINUTE:
        return None
    return (minutes * 600 + seconds * 10 + tenths) / _TENTHS_PER_SECOND


def encoded_race_time_seconds_sql(column: str) -> str:
    """Build DuckDB SQL for a trusted raw-clock column/expression.

    The regex prevents decimal, signed, oversized and non-ASCII inputs from
    becoming valid clocks through permissive numeric casts. ``try_cast`` keeps
    invalid rows safe regardless of expression evaluation order.
    """
    cleaned = f"trim(cast({column} as varchar))"
    encoded = f"try_cast({cleaned} as integer)"
    return (
        f"case when regexp_full_match({cleaned}, '[0-9]{{1,4}}') "
        f"and {encoded} > 0 and ({encoded} % 1000) // 10 < 60 "
        f"then (({encoded} // 1000) * 600 + {encoded} % 1000) / 10.0 "
        "else null end"
    )
