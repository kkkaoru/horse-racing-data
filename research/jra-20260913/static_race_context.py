"""Explicit race descriptors for broad-history teachers with different conditions."""

from __future__ import annotations

import math
import re

STATIC_CONTEXT_NAMES: tuple[str, ...] = ("keibajo_code", "track_code", "kyoso_joken_code")
ALPHANUMERIC_CODE_OFFSET: int = 1000
CODE_PATTERN: re.Pattern[str] = re.compile(r"[0-9A-Z]{1,3}")


def numeric_race_code(value: str | None) -> float:
    """Keep decimal codes unchanged; encode foreign codes separately; retain missingness.

    This is a category identifier, not a measured magnitude. The live JRA route
    uses decimal strings, which the existing scorer converts without an adapter.
    """
    if value is None or not value.strip():
        return math.nan
    code = value.strip().upper()
    if CODE_PATTERN.fullmatch(code) is None:
        raise ValueError("Race code must contain one to three ASCII letters or digits")
    if code.isdecimal():
        return float(int(code))
    return float(ALPHANUMERIC_CODE_OFFSET + int(code, 36))
