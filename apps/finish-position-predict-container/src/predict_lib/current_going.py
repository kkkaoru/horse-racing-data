"""Race-day going (馬場状態) side record -> RA-shaped baba codes.

Pre-race RA rows carry ``babajotai_code_* = '0'``, so serving would leave every
baba x pedigree feature NULL. ``sync-realtime-data`` records the going shown on
the race-day 出馬表 page (``GET /api/race-going/{raceKey}``); this module maps
it to the same JV code training reads from results RA (良=1 稍重=2 重=3 不良=4)
and lays it out RA-shaped (turf -> shiba column, dirt -> dirt column) so
``add-baba-pedigree-affinity-features.py`` resolves it through the exact
``coalesce(shiba, dirt)`` expression training uses. Anything else (blank,
ban-ei moisture, unknown text) maps to nothing: a going is never invented.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Final

GOING_CODE_BY_GRADE: Final[Mapping[str, str]] = {"良": "1", "稍重": "2", "重": "3", "不良": "4"}
"""JV 馬場状態コード for the four going grades."""

RA_COLUMN_BY_SURFACE: Final[Mapping[str, str]] = {
    "turf": "babajotai_code_shiba",
    "dirt": "babajotai_code_dirt",
}
NO_GOING_CODE: Final[str] = "0"
GOING_CATEGORIES: Final[frozenset[str]] = frozenset({"jra", "nar"})
"""Ban-ei reports moisture, not a going grade, so it stays NULL for now."""

CURRENT_BABA_FIELD: Final[str] = "current_baba_condition"


def ra_going_pair(goings: Iterable[Mapping[str, object]]) -> tuple[str, str] | None:
    """Return ``(babajotai_code_shiba, babajotai_code_dirt)`` or ``None``.

    ``goings`` are the worker's ``{"surface", "going"}`` observations. A
    surface without a recognised grade stays ``'0'`` like a results RA row.
    """
    codes = {
        RA_COLUMN_BY_SURFACE["turf"]: NO_GOING_CODE,
        RA_COLUMN_BY_SURFACE["dirt"]: NO_GOING_CODE,
    }
    for going in goings:
        column = RA_COLUMN_BY_SURFACE.get(str(going.get("surface")))
        code = GOING_CODE_BY_GRADE.get(str(going.get("going")))
        if column is not None and code is not None:
            codes[column] = code
    pair = (codes[RA_COLUMN_BY_SURFACE["turf"]], codes[RA_COLUMN_BY_SURFACE["dirt"]])
    return None if pair == (NO_GOING_CODE, NO_GOING_CODE) else pair


def baba_condition(pair: tuple[str, str]) -> int:
    """Mirror ``baba_condition_sql``: the first non-'0' code, shiba first."""
    shiba, dirt = pair
    return int(shiba if shiba != NO_GOING_CODE else dirt)


def baba_refresh_needed(entries: Iterable[Mapping[str, object]], code: int) -> bool:
    """True when any cached entry's ``current_baba_condition`` differs from ``code``."""
    return any(_as_code(entry.get(CURRENT_BABA_FIELD)) != code for entry in entries)


def _as_code(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return None if math.isnan(value) else int(value)
