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

import math
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


class PostWeightValidationError(RuntimeError):
    """A post-weight snapshot does not exactly cover the active race runners."""


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


def race_scope_from_target_race(target_race: str) -> RaceScope:
    """Parse a ``keibajo:bango`` target-race string into a single-race scope.

    This is the container's focused-scope shape (DuckDB ``--target-race``,
    ``predict_category(target_race=...)``). Padding is applied later by
    :func:`filter_races_by_scope`, so unpadded worker input still matches.
    """
    keibajo_code, race_bango = target_race.split(":", 1)
    return RaceScope(keibajo_code=keibajo_code, race_bango=race_bango)


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


def filter_post_weight_active_runners(
    races: Races,
    active_horse_numbers: tuple[int, ...],
    excluded_horse_numbers: tuple[int, ...],
) -> dict[str, list[Entry]]:
    """Validate one entry snapshot against cached rows and remove exclusions.

    The weight-trigger entry snapshot is authoritative for late scratches, but
    the attested feature cache remains authoritative for which runners belong
    to the race. Requiring the cache set to equal ``active + excluded`` makes a
    missing active runner, an unknown exclusion, and a stale extra cache row
    fail closed. Only the explicitly active rows can reach scoring/persistence.
    """
    if len(races) != 1:
        raise PostWeightValidationError(
            "post-weight entry snapshot validation requires exactly one race"
        )
    active = set(active_horse_numbers)
    excluded = set(excluded_horse_numbers)
    if not active or active & excluded:
        raise PostWeightValidationError("post-weight entry snapshot runner set invalid")

    race_id, entries = next(iter(races.items()))
    parts = parse_race_id(race_id)
    race_label = (
        f"{parts.keibajo_code.zfill(RACE_KEY_PAD_WIDTH)}:"
        f"{parts.race_bango.zfill(RACE_KEY_PAD_WIDTH)}"
    )
    cached: set[int] = set()
    active_entries: list[Entry] = []
    for entry in entries:
        umaban = coerce_optional_int(entry.get(UMABAN_FIELD))
        if umaban is None or umaban <= 0 or umaban in cached:
            raise PostWeightValidationError(
                f"post-weight cached runner set invalid: race={race_label}"
            )
        cached.add(umaban)
        if umaban in active:
            active_entries.append(entry)
    expected = active | excluded
    if cached != expected:
        missing = sorted(expected - cached)
        unexpected = sorted(cached - expected)
        raise PostWeightValidationError(
            f"post-weight entry snapshot mismatch: race={race_label} "
            f"missing={missing} unexpected={unexpected}"
        )
    return {race_id: active_entries}


def validate_post_weight_snapshots(
    races: Races,
    snapshots_by_race_key: Mapping[tuple[str, str], RaceFreshSnapshot],
) -> None:
    """Require exact, positive horse weights for every active cached runner.

    The watermark-attested cache contains only active runners: source rows with
    JRA/NAR ``ijo_kubun_code`` 1 (scratch) or 2 (exclude) are removed by the
    feature pipeline and its entry-list gate. Therefore its per-race umaban set
    is authoritative for post-weight validation, including late scratches.
    """
    expected_by_race: dict[tuple[str, str], set[int]] = {}
    for race_id, entries in races.items():
        parts = parse_race_id(race_id)
        race_key = (
            parts.keibajo_code.zfill(RACE_KEY_PAD_WIDTH),
            parts.race_bango.zfill(RACE_KEY_PAD_WIDTH),
        )
        expected: set[int] = set()
        for entry in entries:
            umaban = coerce_optional_int(entry.get(UMABAN_FIELD))
            if umaban is None or umaban <= 0:
                raise PostWeightValidationError(
                    f"post-weight runner set invalid: race={race_key[0]}:{race_key[1]}"
                )
            expected.add(umaban)
        if not expected:
            raise PostWeightValidationError(
                f"post-weight runner set empty: race={race_key[0]}:{race_key[1]}"
            )
        expected_by_race[race_key] = expected

    expected_race_keys = set(expected_by_race)
    actual_race_keys = set(snapshots_by_race_key)
    if actual_race_keys != expected_race_keys:
        missing = sorted(expected_race_keys - actual_race_keys)
        unexpected = sorted(actual_race_keys - expected_race_keys)
        raise PostWeightValidationError(
            f"post-weight race set mismatch: missing={missing} unexpected={unexpected}"
        )

    for race_key, expected in expected_by_race.items():
        weights = snapshots_by_race_key[race_key].bataiju_by_umaban
        actual = set(weights)
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        if missing or unexpected:
            raise PostWeightValidationError(
                f"post-weight runner set mismatch: race={race_key[0]}:{race_key[1]} "
                f"missing={missing} unexpected={unexpected}"
            )
        if any(
            isinstance(weight, bool) or not math.isfinite(float(weight)) or float(weight) <= 0
            for weight in weights.values()
        ):
            raise PostWeightValidationError(
                f"post-weight value invalid: race={race_key[0]}:{race_key[1]}"
            )


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
