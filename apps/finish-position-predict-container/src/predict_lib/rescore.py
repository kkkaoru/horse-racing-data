"""Pure race-scope filtering + fresh-snapshot application for Stage-2 rescore.

This module is the side-effect-free core of the per-race rescore path.  The
caller (``predict_upcoming.py``) loads the cached feature parquet into a
``race_id -> entries`` map, fetches the latest realtime odds / bataiju via
``realtime_odds_fetcher`` (the only I/O), and passes both into these pure
functions to (a) overwrite the 5 late-binding columns from the fresh snapshot
and (b) restrict the run to a single race (or keibajo) when a scope is given.

Race-scope normalization
-------------------------
The cached parquet stores ``keibajo_code`` / ``race_bango`` as zero-padded
two-digit strings (e.g. ``"44"`` / ``"01"``), matching the ``nvd_se`` /
``jvd_se`` source columns and the canonical ``race_id``.  Worker-supplied scope
values may arrive un-padded (e.g. ``"1"`` for race 1), so both the parsed
``race_id`` value and the scope value are normalized with the same
zero-pad-to-2 helper before comparison.  This keeps ``raceBango=1`` and
``raceBango=01`` equivalent without weakening any other check.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from predict_lib.late_binding import (
    UMABAN_FIELD,
    OddsSnapshot,
    WeightSnapshot,
    apply_late_binding_to_entry,
    coerce_optional_int,
)
from predict_lib.model_meta import Category
from predict_lib.race_id import parse_race_id

KEIBAJO_CODE_FIELD: Final[str] = "keibajo_code"
RACE_BANGO_FIELD: Final[str] = "race_bango"
RACE_KEY_PAD_WIDTH: Final[int] = 2
"""Zero-pad width for keibajo_code / race_bango scope comparison."""

Entry = dict[str, object]
Races = Mapping[str, list[Entry]]


@dataclass(frozen=True)
class RaceScope:
    """Optional race-level filter: keibajo_code and/or race_bango.

    Both ``None`` means "all races in the cache" (the full-day rescore).  When
    only ``keibajo_code`` is set, every race at that track matches; when both
    are set, exactly one race matches.  Scope values are compared after
    zero-pad normalization so un-padded worker input still matches.
    """

    keibajo_code: str | None = None
    race_bango: str | None = None


@dataclass(frozen=True)
class RaceFreshSnapshot:
    """Per-race fresh realtime snapshot keyed by umaban.

    ``odds_by_umaban`` and ``bataiju_by_umaban`` carry the latest odds and
    bataiju; a horse absent from either map gets the builder's median / NULL
    fallback (the caller passes a ``None``-valued snapshot for it).
    """

    odds_by_umaban: Mapping[int, OddsSnapshot]
    bataiju_by_umaban: Mapping[int, float]


def _normalize_race_key_part(value: str | None) -> str | None:
    """Zero-pad a keibajo_code / race_bango to width 2 (None -> None).

    Empty / whitespace-only values collapse to ``None`` so an absent scope side
    never spuriously matches.
    """
    if value is None:
        return None
    text = value.strip()
    if text == "":
        return None
    return text.zfill(RACE_KEY_PAD_WIDTH)


def _side_matches(actual: str, scope_value: str | None) -> bool:
    """Return True when ``actual`` matches a (possibly wildcard) scope side.

    ``scope_value`` of ``None`` is a wildcard.  Both sides are zero-pad-normalized
    so un-padded scope input still matches the padded cache key.
    """
    scope_normalized = _normalize_race_key_part(scope_value)
    if scope_normalized is None:
        return True
    return _normalize_race_key_part(actual) == scope_normalized


def race_matches_scope(race_id: str, scope: RaceScope) -> bool:
    """Return True when ``race_id`` satisfies every set side of ``scope``.

    A ``None`` scope side is a wildcard.  The ``keibajo_code`` / ``race_bango``
    decoded from ``race_id`` and the scope values are both zero-pad-normalized
    so un-padded scope input still matches the padded cache key.
    """
    parts = parse_race_id(race_id)
    return _side_matches(parts.keibajo_code, scope.keibajo_code) and _side_matches(
        parts.race_bango, scope.race_bango
    )


def filter_races_by_scope(races: Races, scope: RaceScope) -> dict[str, list[Entry]]:
    """Return the subset of ``races`` whose ``race_id`` matches ``scope``.

    An empty result is returned (not an error) when no race matches — the caller
    then writes zero predictions for the request.
    """
    return {
        race_id: entries for race_id, entries in races.items() if race_matches_scope(race_id, scope)
    }


def _snapshot_for_entry(
    entry: Entry,
    snapshot: RaceFreshSnapshot | None,
) -> tuple[OddsSnapshot, WeightSnapshot]:
    """Resolve the (odds, weight) snapshot for one entry by its umaban.

    A missing per-race snapshot or a horse absent from the umaban maps yields a
    ``None``-valued odds/weight snapshot, which the late-binding recompute turns
    into the builder's median / NULL fallback.
    """
    if snapshot is None:
        return OddsSnapshot(None, None), WeightSnapshot(None)
    umaban = coerce_optional_int(entry.get(UMABAN_FIELD))
    if umaban is None:
        return OddsSnapshot(None, None), WeightSnapshot(None)
    odds = snapshot.odds_by_umaban.get(umaban, OddsSnapshot(None, None))
    bataiju = snapshot.bataiju_by_umaban.get(umaban)
    return odds, WeightSnapshot(bataiju)


def apply_fresh_snapshots(
    races: Races,
    snapshots_by_race_key: Mapping[tuple[str, str], RaceFreshSnapshot],
    category: Category,
) -> dict[str, list[Entry]]:
    """Return a new races map with the 5 late-binding columns recomputed.

    For each entry the per-race snapshot is looked up by the entry's
    ``(keibajo_code, race_bango)`` (zero-pad-normalized to match the snapshot
    keys) and the per-horse odds / bataiju by ``umaban``.  Races / horses with
    no fresh snapshot keep the builder's median / NULL fallback.  Early-binding
    columns are preserved verbatim.
    """
    updated: dict[str, list[Entry]] = {}
    for race_id, entries in races.items():
        updated[race_id] = [
            apply_late_binding_to_entry(
                entry,
                *_snapshot_for_entry(
                    entry,
                    _lookup_race_snapshot(entry, snapshots_by_race_key),
                ),
                category,
            )
            for entry in entries
        ]
    return updated


def _lookup_race_snapshot(
    entry: Entry,
    snapshots_by_race_key: Mapping[tuple[str, str], RaceFreshSnapshot],
) -> RaceFreshSnapshot | None:
    """Resolve the per-race snapshot for ``entry`` by normalized race key.

    Returns ``None`` when the entry carries no usable keibajo_code / race_bango
    or no snapshot exists for that race (median / NULL fallback path).
    """
    keibajo = _normalize_race_key_part(_entry_str(entry.get(KEIBAJO_CODE_FIELD)))
    bango = _normalize_race_key_part(_entry_str(entry.get(RACE_BANGO_FIELD)))
    if keibajo is None or bango is None:
        return None
    return snapshots_by_race_key.get((keibajo, bango))


def _entry_str(value: object) -> str | None:
    """Stringify a cache cell for race-key normalization (None stays None)."""
    if value is None:
        return None
    return str(value)
