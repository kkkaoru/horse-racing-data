"""SQL expression helpers for PC-KEIBA packed ``MSSd`` race clocks.

``soha_time`` is not elapsed tenths.  Its decimal positions are minutes,
two-digit seconds, and tenths, so ``1008`` means 1:00.8 (608 tenths).
"""

from __future__ import annotations


def encoded_race_time_tenths_sql(expression: str) -> str:
    """Return a DuckDB expression decoding a packed ``MSSd`` value."""
    raw = f"try_cast(nullif(trim(cast({expression} as varchar)), '0000') as bigint)"
    seconds = f"(({raw} // 10) % 100)"
    return (
        "case "
        f"when {raw} is null or {raw} <= 0 or {seconds} >= 60 then null "
        f"else cast(({raw} // 1000) * 600 + {seconds} * 10 + ({raw} % 10) as bigint) "
        "end"
    )
