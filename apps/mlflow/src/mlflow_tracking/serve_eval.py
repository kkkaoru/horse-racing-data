"""Query + join + aggregate library for production-served prediction accuracy.

This module has no CLI of its own and is never invoked directly -- it is a
pure library imported by two sibling modules built separately (out of scope
for this file): `sync_production.py` (pulls fresh rows from the racing Neon
database and stores/ingests them) and `champion_cell_eval.py` (scores the
currently-deployed champion model per CELL, see cells.py). Both need the same
underlying join: production-served predictions (from Neon) against finalized
race results/metadata (from the local PostgreSQL replica), so the query/join/
aggregate logic lives here once instead of being duplicated in each caller.

Two evaluation granularities, not one:

  * Finish-position (FP) evaluation is PER-RACE: one model run produces a
    full ranking (predicted_rank 1..N) for every horse in a race, and that
    ranking is scored as a single unit against the race's actual finish
    order (top1/place2/place3/place4/place5/place6/fukusho_2p/top3_box --
    see `_compute_race_hits`). Ranks 4/5/6 carry a per-metric small-field
    exclusion (None, not 0, when a race's total starter count is below that
    rank -- see `_compute_race_hits`'s own docstring for why ranks 2/3 do
    not need the same treatment) that every consumer of
    `aggregate_fp_day_metrics`'s output must respect. One race, one
    `FpRaceEvalRow`.
  * Running-style (RS) evaluation is PER-HORSE: the running-style model
    predicts one class (nige/senkou/sashi/oikomi) per horse independently --
    it is not a ranking, so there is no race-level aggregate hit/miss the
    way there is for FP. One horse, one `RsHorseEvalRow`.

Every query function below takes an INJECTED `conn: db.ConnectionLike`,
never constructing one itself -- this mirrors db.py's own module-docstring
rationale exactly: it keeps this module hermetically testable (every test
passes a fake connection/cursor, never touches Neon or the local replica)
and keeps connection lifecycle (open/close, readonly enforcement) entirely
the caller's responsibility.

`GEN_LAG_TOLERANCE_DAYS = 3` exists to separate genuinely-served predictions
(rows written within a few days of the race itself -- pre-race publishing,
or same-day/next-day) from offline walk-forward backfill re-predictions,
which can carry a `prediction_generated_at` timestamp decades away from the
race date (the model was trained/evaluated long after or long before the
actual race). `backfill_serve_timeline.py` applies this same discipline at a
whole-script, whole-date-range level (it only ever re-invokes
serve_accuracy_report.py for historical dates, never mixes eras within one
call); `is_genuine` applies it here at ROW granularity, since a single fetch
across a date range can otherwise mix genuinely-served rows with backfill
noise for the same race.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final, cast

import pandas as pd

from mlflow_tracking import cells, db

GEN_LAG_TOLERANCE_DAYS: Final[int] = 3
BANEI_KEIBAJO_CODE: Final[str] = "83"

# Running-style class thresholds/labels, hand-replicated from
# apps/pc-keiba-viewer/src/scripts/serve_accuracy_report.py (this package
# never imports across apps -- see cells.py's own module docstring for the
# same rationale). Any change to the source thresholds must be mirrored here.
RS_SENKOU_THRESHOLD: Final[float] = 0.3
RS_SASHI_THRESHOLD: Final[float] = 0.7
RS_CLASS_NIGE: Final[int] = 0
RS_CLASS_SENKOU: Final[int] = 1
RS_CLASS_SASHI: Final[int] = 2
RS_CLASS_OIKOMI: Final[int] = 3
RS_CLASS_LABELS: Final[tuple[str, str, str, str]] = ("nige", "senkou", "sashi", "oikomi")

# JRA results live in jvd_se/jvd_ra. NAR and Ban-ei share nvd_se/nvd_ra --
# Ban-ei is simply the subset of NAR rows with keibajo_code ==
# BANEI_KEIBAJO_CODE, not a separately-exported table (same convention
# cells.py's CATEGORY_BANEI already follows).
_JRA_RESULT_TABLES: Final[tuple[str, str]] = ("jvd_se", "jvd_ra")
_NAR_RESULT_TABLES: Final[tuple[str, str]] = ("nvd_se", "nvd_ra")


@dataclass(frozen=True)
class FpRaceEvalRow:
    """One race's finish-position serve-accuracy evaluation.

    `place4_hit`/`place5_hit`/`place6_hit` are `int | None`, NOT the same
    always-0-or-1 shape as `top1_hit`/`place2_hit`/`place3_hit` -- see
    `_compute_race_hits`'s own docstring for the full rationale. In short:
    a race with fewer than N starters cannot possibly produce a legitimate
    rank-N finisher, so `place{N}_hit` is None (excluded from that specific
    metric's population entirely -- not a hit, not a miss) whenever this
    race's true total starter count (every horse that ran, not just the
    ones a prediction matched -- see `build_fp_race_eval_rows`) is below N.
    """

    category: str
    race_date: str  # YYYYMMDD
    venue: str  # == keibajo_code
    race_bango: str
    model_version: str
    distance_band: str | None
    field_size_band: str | None
    season_band: str | None
    class_code: str | None
    surface: str | None
    predicted_top1_ketto: str | None
    actual_top1_ketto: str | None
    matched_horses: int
    top1_hit: int
    place2_hit: int
    place3_hit: int
    place4_hit: int | None
    place5_hit: int | None
    place6_hit: int | None
    fukusho_2p_hit: int
    top3_box_hit: int


@dataclass(frozen=True)
class RsHorseEvalRow:
    """One horse's running-style serve-accuracy evaluation."""

    category: str
    race_date: str
    venue: str
    race_bango: str
    ketto_toroku_bango: str
    model_version: str
    distance_band: str | None
    field_size_band: str | None
    season_band: str | None
    class_code: str | None
    surface: str | None
    predicted_class: int
    predicted_label: str
    actual_class: int
    actual_label: str
    hit: int


# ── Pure helpers (no I/O) ────────────────────────────────────────────────────


def compute_corner1_norm(corner1_raw: str, shusso_tosu: int) -> float | None:
    """Normalize a raw corner-1 passing-position string to [0, 1].

    Mirrors serve_accuracy_report.py's function of the same name exactly:
    "00" (or any non-digit string, or a digit string worth 0) means "no
    corner-1 data" (straight-course races have no corner) and returns None,
    as does a field of 1 or fewer horses (division by zero in the formula).
    """
    if not corner1_raw or corner1_raw == "00":
        return None
    if not corner1_raw.isdigit():
        return None
    c1 = int(corner1_raw)
    if c1 == 0:
        return None
    if shusso_tosu <= 1:
        return None
    return (c1 - 1) / (shusso_tosu - 1)


def classify_running_style(corner1_norm: float | None) -> int | None:
    """Assign a running-style class from a normalized corner-1 position.

    Returns None for straight-course races (corner1_norm is None), which
    have no ground truth to compare a running-style prediction against.
    """
    if corner1_norm is None:
        return None
    if corner1_norm == 0.0:
        return RS_CLASS_NIGE
    if corner1_norm <= RS_SENKOU_THRESHOLD:
        return RS_CLASS_SENKOU
    if corner1_norm <= RS_SASHI_THRESHOLD:
        return RS_CLASS_SASHI
    return RS_CLASS_OIKOMI


def is_genuine(
    race_date: str,
    generated_at: datetime | None,
    *,
    tolerance_days: int = GEN_LAG_TOLERANCE_DAYS,
) -> bool:
    """True when `generated_at` falls within `tolerance_days` of `race_date`.

    `race_date` is an 8-char YYYYMMDD string. A missing `generated_at` (None)
    is never genuine -- there is nothing to compare it against, so it is
    conservatively treated the same as a backfill row of unknown provenance.
    """
    if generated_at is None:
        return False
    parsed_race_date = date(int(race_date[:4]), int(race_date[4:6]), int(race_date[6:8]))
    return abs((generated_at.date() - parsed_race_date).days) <= tolerance_days


LIVE_LAG_TOLERANCE_DAYS: Final[int] = 1


def classify_serving_kind(
    race_date: str, generated_at: datetime, *, live_lag_days: int = LIVE_LAG_TOLERANCE_DAYS
) -> str:
    """Classify an already-genuine row (see `is_genuine`) as `"live"` or
    `"backfill"` serving.

    `"live"` covers pre-race publishing (a negative lag) through same-day/
    next-day reporting (`lag_days <= live_lag_days`); anything generated
    later -- still within `is_genuine`'s broader `GEN_LAG_TOLERANCE_DAYS`
    window, so not decades-old WF-era noise, but clearly not the live serving
    path -- is `"backfill"`: e.g. a backfill script re-predicting a handful
    of historical races a few days after the fact. Distinct from (and
    narrower than) `is_genuine`'s own genuine/not-genuine split: this
    function is only ever meaningful on a row that already passed that
    check, and assumes `generated_at` is not None (unlike `is_genuine`,
    which must handle that case for a row it hasn't filtered yet).
    """
    parsed_race_date = date(int(race_date[:4]), int(race_date[4:6]), int(race_date[6:8]))
    lag_days = (generated_at.date() - parsed_race_date).days
    return "live" if lag_days <= live_lag_days else "backfill"


def resolve_result_tables(category: str) -> tuple[str, str]:
    """Return (se_table, ra_table) for `category`; raises ValueError otherwise."""
    if category == cells.CATEGORY_JRA:
        return _JRA_RESULT_TABLES
    if category in (cells.CATEGORY_NAR, cells.CATEGORY_BANEI):
        return _NAR_RESULT_TABLES
    raise ValueError(f"unknown category: {category!r}")


def resolve_source(category: str) -> str:
    """Return the `source` column value used in the production-prediction
    tables for `category`.

    Ban-ei predictions are stored under source="nar" (Ban-ei is a keibajo_code
    subset of NAR, not a distinct upstream source), mirroring
    `resolve_result_tables`'s nvd_se/nvd_ra sharing above.
    """
    if category == cells.CATEGORY_JRA:
        return cells.CATEGORY_JRA
    if category in (cells.CATEGORY_NAR, cells.CATEGORY_BANEI):
        return cells.CATEGORY_NAR
    raise ValueError(f"unknown category: {category!r}")


def partition_by_banei(
    rows: Sequence[Mapping[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Split `rows` into (non_banei_rows, banei_rows) by keibajo_code.

    Every surviving row is converted to a plain `dict` (input rows may be any
    `Mapping`, e.g. rows already built elsewhere in this module).
    """
    non_banei_rows: list[dict[str, object]] = []
    banei_rows: list[dict[str, object]] = []
    for row in rows:
        if row["keibajo_code"] == BANEI_KEIBAJO_CODE:
            banei_rows.append(dict(row))
        else:
            non_banei_rows.append(dict(row))
    return non_banei_rows, banei_rows


def filter_genuine_rows(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Keep only rows whose `prediction_generated_at` is genuine (see `is_genuine`).

    Each row must carry `kaisai_nen`, `kaisai_tsukihi`, and
    `prediction_generated_at` -- the shape `fetch_fp_prediction_rows`/
    `fetch_rs_prediction_rows` produce.
    """
    result: list[dict[str, object]] = []
    for row in rows:
        race_date = str(row["kaisai_nen"]) + str(row["kaisai_tsukihi"])
        generated_at = cast("datetime | None", row["prediction_generated_at"])
        if is_genuine(race_date, generated_at):
            result.append(dict(row))
    return result


def partition_live_backfill(
    rows: Sequence[Mapping[str, object]], *, live_lag_days: int = LIVE_LAG_TOLERANCE_DAYS
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Split already-genuine `rows` (see `filter_genuine_rows`) into
    (live_rows, backfill_rows) via `classify_serving_kind`.

    Every row must carry `kaisai_nen`, `kaisai_tsukihi`, and a non-None
    `prediction_generated_at` -- the exact postcondition `filter_genuine_rows`
    already guarantees for whatever it lets through, and the only shape this
    function is ever called with in this package (always downstream of that
    filter, never on raw unfiltered rows).
    """
    live_rows: list[dict[str, object]] = []
    backfill_rows: list[dict[str, object]] = []
    for row in rows:
        race_date = str(row["kaisai_nen"]) + str(row["kaisai_tsukihi"])
        generated_at = cast("datetime", row["prediction_generated_at"])
        kind = classify_serving_kind(race_date, generated_at, live_lag_days=live_lag_days)
        (live_rows if kind == "live" else backfill_rows).append(dict(row))
    return live_rows, backfill_rows


def group_by_model_version(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, list[dict[str, object]]]:
    """Group `rows` by their `model_version` value, preserving encounter order."""
    result: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        model_version = str(row["model_version"])
        result.setdefault(model_version, []).append(dict(row))
    return result


def _compute_race_hits(pairs: list[tuple[int, int]], total_starters: int) -> dict[str, int | None]:
    """Compute the FP hit flags for ONE race from its (pred_rank, actual_rank)
    pairs plus that race's TRUE total starter count (every horse that ran,
    not just the ones `pairs` matched -- see `build_fp_race_eval_rows`'s own
    docstring for how `total_starters` is derived).

    `pred1_actual` stays None when no row in `pairs` has `pred_rank == 1`
    (e.g. that horse was excluded because it had no matching result) -- in
    that case top1_hit/place2_hit/place3_hit/place4_hit/place5_hit/place6_hit
    are all 0 (or None, per the small-field exclusion below), but
    fukusho_2p_hit and top3_box_hit are computed independently of
    `pred1_actual` and may still be nonzero based on the remaining pairs.

    ★ Ranks 2/3 vs. ranks 4/5/6 are DELIBERATELY asymmetric here, not an
    oversight:

    `place2_hit`/`place3_hit` use the ORIGINAL, UNGATED formula -- no
    small-field guard at all, and a race's `total_starters` never enters
    their computation. This has never been a correctness bug in practice:
    real jra/nar/banei races essentially never run a field of 1 or 2 horses,
    so `pred1_actual <= 2`/`<= 3` has never been trivially satisfied by a
    too-small field the way rank 4/5/6 genuinely can be (see below). Changing
    this established, already-shipped behavior is explicitly out of scope --
    it would be a regression to existing `finish-position/production-usage`
    metric history for zero corrective benefit.

    `place4_hit`/`place5_hit`/`place6_hit` DO need the guard: NAR/Ban-ei
    races realistically run fields as small as 5-8 starters (occasionally
    fewer), so a race with `total_starters < N` cannot possibly produce a
    legitimate rank-N finisher at all -- e.g. in a 5-horse field,
    `pred1_actual` (which must be 1..5) is ALWAYS `<= 6`, so a naive
    `place6_hit = 1 if pred1_actual is not None and pred1_actual <= 6 else 0`
    would be trivially 1 for every such race, silently inflating
    `place6_pct`. Each of these three is None (excluded from that metric's
    population entirely -- not a hit, not a miss) whenever
    `total_starters < N`, mirroring `aggregate_rs_day_metrics`'s own
    None-for-"not applicable" precedent (see `aggregate_fp_day_metrics`'s
    docstring for how the per-metric denominator this produces is
    aggregated).
    """
    pred1_actual: int | None = None
    any_top2_in_top2 = False
    top3_in_actual_top3 = 0
    for pred_rank, actual_rank in pairs:
        if pred_rank == 1:
            pred1_actual = actual_rank
        if pred_rank <= 2 and actual_rank <= 2:
            any_top2_in_top2 = True
        if pred_rank <= 3 and actual_rank <= 3:
            top3_in_actual_top3 += 1

    def _rank_n_hit(n: int) -> int | None:
        if total_starters < n:
            return None
        return 1 if (pred1_actual is not None and pred1_actual <= n) else 0

    return {
        "top1_hit": 1 if pred1_actual == 1 else 0,
        "place2_hit": 1 if (pred1_actual is not None and pred1_actual <= 2) else 0,
        "place3_hit": 1 if (pred1_actual is not None and pred1_actual <= 3) else 0,
        "place4_hit": _rank_n_hit(4),
        "place5_hit": _rank_n_hit(5),
        "place6_hit": _rank_n_hit(6),
        "fukusho_2p_hit": 1 if any_top2_in_top2 else 0,
        "top3_box_hit": 1 if top3_in_actual_top3 == 3 else 0,
    }


def _parse_actual_rank(kakutei_chakujun: object) -> int | None:
    """Parse a raw kakutei_chakujun DB value into a positive finish rank, or None.

    Non-finishers/DNF rows encode kakutei_chakujun as non-numeric text (e.g.
    "中止", "取消") in the source tables -- `re.fullmatch` rejects anything
    that isn't purely digits, and a parsed value of 0 (a defensive case, not
    known to occur in real data) is also treated as "no rank".
    """
    text = str(kakutei_chakujun or "")
    if not re.fullmatch(r"[0-9]+", text):
        return None
    value = int(text)
    return value if value > 0 else None


def _safe_int(value: object) -> int | None:
    """Parse `value` as an int, returning None instead of raising on failure."""
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


# ── Query functions (take an injected connection) ───────────────────────────

_FP_PREDICTION_COLUMNS: Final[tuple[str, ...]] = (
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "model_version",
    "predicted_rank",
    "prediction_generated_at",
    "distance_band",
    "field_size_band",
    "season_band",
    "class_code",
    "surface",
    "kaisai_nen",
    "kaisai_tsukihi",
)

_FP_PREDICTION_SQL: Final[str] = f"""
    SELECT {", ".join(_FP_PREDICTION_COLUMNS)}
    FROM race_finish_position_model_predictions
    WHERE source = %s AND to_date(kaisai_nen || kaisai_tsukihi, 'YYYYMMDD')
          BETWEEN to_date(%s, 'YYYYMMDD') AND to_date(%s, 'YYYYMMDD')
"""

_RS_PREDICTION_COLUMNS: Final[tuple[str, ...]] = (
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "model_version",
    "predicted_class",
    "predicted_label",
    "prediction_generated_at",
    "kaisai_nen",
    "kaisai_tsukihi",
)

_RS_PREDICTION_SQL: Final[str] = f"""
    SELECT {", ".join(_RS_PREDICTION_COLUMNS)}
    FROM race_running_style_model_predictions
    WHERE source = %s AND to_date(kaisai_nen || kaisai_tsukihi, 'YYYYMMDD')
          BETWEEN to_date(%s, 'YYYYMMDD') AND to_date(%s, 'YYYYMMDD')
"""

_RACE_RESULTS_COLUMNS: Final[tuple[str, ...]] = (
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "kakutei_chakujun",
    "corner_1",
)

_RACE_METADATA_COLUMNS: Final[tuple[str, ...]] = (
    "keibajo_code",
    "race_bango",
    "kyori",
    "shusso_tosu",
    "track_code",
    "kyoso_joken_code",
)


def fetch_fp_prediction_rows(
    conn: db.ConnectionLike, source: str, date_from: str, date_to: str
) -> list[dict[str, object]]:
    """Fetch race_finish_position_model_predictions rows for `source` in [date_from, date_to]."""
    cursor = conn.cursor()
    cursor.execute(_FP_PREDICTION_SQL, (source, date_from, date_to))
    return [dict(zip(_FP_PREDICTION_COLUMNS, row, strict=True)) for row in cursor.fetchall()]


def fetch_rs_prediction_rows(
    conn: db.ConnectionLike, source: str, date_from: str, date_to: str
) -> list[dict[str, object]]:
    """Fetch race_running_style_model_predictions rows for `source` in [date_from, date_to]."""
    cursor = conn.cursor()
    cursor.execute(_RS_PREDICTION_SQL, (source, date_from, date_to))
    return [dict(zip(_RS_PREDICTION_COLUMNS, row, strict=True)) for row in cursor.fetchall()]


def fetch_race_results(
    conn: db.ConnectionLike, category: str, date_str: str
) -> dict[tuple[str, str, str], dict[str, object]]:
    """Fetch finalized per-horse results for `category` on `date_str`.

    Returns a dict keyed by (keibajo_code, race_bango, ketto_toroku_bango) so
    join functions below can resolve one prediction row's actual outcome by
    a single dict lookup rather than a linear scan.
    """
    se_table, _ = resolve_result_tables(category)
    # se_table is interpolated via f-string, which is safe here ONLY because
    # it always comes from the small hardcoded allow-list resolve_result_tables
    # returns ("jvd_se" / "nvd_se"), never from unsanitized external input --
    # mirrors the exact pattern serve_accuracy_report.py already uses for the
    # same reason.
    sql = f"""
        SELECT {", ".join(_RACE_RESULTS_COLUMNS)}
        FROM {se_table}
        WHERE kaisai_nen = %s AND kaisai_tsukihi = %s
    """
    cursor = conn.cursor()
    cursor.execute(sql, (date_str[:4], date_str[4:]))
    result: dict[tuple[str, str, str], dict[str, object]] = {}
    for raw_row in cursor.fetchall():
        row = dict(zip(_RACE_RESULTS_COLUMNS, raw_row, strict=True))
        key = (
            str(row["keibajo_code"]),
            str(row["race_bango"]),
            str(row["ketto_toroku_bango"]),
        )
        result[key] = {
            "actual_rank": _parse_actual_rank(row["kakutei_chakujun"]),
            "corner_1": row["corner_1"],
        }
    return result


def fetch_race_metadata(
    conn: db.ConnectionLike, category: str, date_str: str
) -> dict[tuple[str, str], dict[str, object]]:
    """Fetch race-level metadata for `category` on `date_str`.

    Returns a dict keyed by (keibajo_code, race_bango). Every numeric field
    is parsed defensively via `_safe_int` (never raises on a malformed value).
    """
    _, ra_table = resolve_result_tables(category)
    # Same narrow, allow-list-only f-string interpolation rationale as
    # fetch_race_results above.
    sql = f"""
        SELECT {", ".join(_RACE_METADATA_COLUMNS)}
        FROM {ra_table}
        WHERE kaisai_nen = %s AND kaisai_tsukihi = %s
    """
    cursor = conn.cursor()
    cursor.execute(sql, (date_str[:4], date_str[4:]))
    result: dict[tuple[str, str], dict[str, object]] = {}
    for raw_row in cursor.fetchall():
        row = dict(zip(_RACE_METADATA_COLUMNS, raw_row, strict=True))
        key = (str(row["keibajo_code"]), str(row["race_bango"]))
        track_code = row["track_code"]
        kyoso_joken_code = row["kyoso_joken_code"]
        result[key] = {
            "kyori": _safe_int(row["kyori"]),
            "shusso_tosu": _safe_int(row["shusso_tosu"]),
            "track_code": str(track_code) if track_code is not None else None,
            "kyoso_joken_code": str(kyoso_joken_code) if kyoso_joken_code is not None else None,
        }
    return result


_BANEI_RACE_CALENDAR_SQL: Final[str] = """
    SELECT race_bango FROM nvd_ra
    WHERE kaisai_nen = %s AND kaisai_tsukihi = %s AND keibajo_code = %s
"""


def fetch_banei_race_count(conn: db.ConnectionLike, date_str: str) -> int:
    """Return the count of Ban-ei races scheduled in the local replica's own
    nvd_ra race calendar for `date_str`, independent of whether anything was
    ever served for them.

    This is the "races expected" oracle `sync_production.py`'s serving-gap
    check uses for category="banei" specifically: running-style has no
    Ban-ei model at all (see `RS_CATEGORIES` in sync_production.py), so
    there is no RS-served count to compare a Ban-ei fp_races==0 day against
    the way jra/nar can. nvd_ra is the same table `fetch_race_metadata`
    already reads race-level metadata from -- filtered here to just the
    Ban-ei keibajo_code subset (`BANEI_KEIBAJO_CODE`), since nvd_ra
    otherwise spans every NAR venue for the day, not just Ban-ei's.
    """
    cursor = conn.cursor()
    cursor.execute(_BANEI_RACE_CALENDAR_SQL, (date_str[:4], date_str[4:], BANEI_KEIBAJO_CODE))
    return len(cursor.fetchall())


# ── Join / build functions (pure, no I/O) ───────────────────────────────────


def build_fp_race_eval_rows(
    category: str,
    model_version: str,
    pred_rows: list[dict[str, object]],
    results: dict[tuple[str, str, str], dict[str, object]],
) -> list[FpRaceEvalRow]:
    """Join FP prediction rows against `results` to build one row per race.

    A race is emitted only when at least one of its horses resolves to a
    matched, ranked result (`matched_horses >= 1`); a race with zero matches
    is skipped entirely. `predicted_top1_ketto` comes directly from
    `pred_rows` (the predicted_rank==1 horse) regardless of whether that
    horse itself matched a result -- it is independent of `pairs`.

    `total_starters` (fed to `_compute_race_hits` for the rank-4/5/6
    small-field exclusion, see that function's own docstring) is derived
    from `results` alone, with ZERO new queries: `results` is built by
    `fetch_race_results` for EVERY horse that ran in EVERY race on this
    date, not just the ones a prediction happens to match (see that
    function's own docstring) -- so the count of DISTINCT
    `ketto_toroku_bango` keys sharing one race's (keibajo_code, race_bango)
    in `results` IS that race's true total starter count, computed once
    up front here (`_starter_counts`) rather than re-scanned per race.
    """
    groups: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in pred_rows:
        key = (str(row["keibajo_code"]), str(row["race_bango"]))
        groups.setdefault(key, []).append(row)

    starter_counts: dict[tuple[str, str], int] = {}
    for keibajo_code_key, race_bango_key, _ketto_toroku_bango in results:
        count_key = (keibajo_code_key, race_bango_key)
        starter_counts[count_key] = starter_counts.get(count_key, 0) + 1

    eval_rows: list[FpRaceEvalRow] = []
    for (keibajo_code, race_bango), group_rows in groups.items():
        pairs: list[tuple[int, int]] = []
        matched_actual_ranks: dict[str, int] = {}
        for row in group_rows:
            ketto_toroku_bango = str(row["ketto_toroku_bango"])
            result = results.get((keibajo_code, race_bango, ketto_toroku_bango))
            if result is None:
                continue
            actual_rank_raw = result["actual_rank"]
            if actual_rank_raw is None:
                continue
            actual_rank = cast(int, actual_rank_raw)
            predicted_rank = cast(int, row["predicted_rank"])
            pairs.append((predicted_rank, actual_rank))
            matched_actual_ranks[ketto_toroku_bango] = actual_rank
        if not pairs:
            continue

        total_starters = starter_counts.get((keibajo_code, race_bango), 0)
        hits = _compute_race_hits(pairs, total_starters)

        predicted_top1_ketto: str | None = None
        for row in group_rows:
            if cast(int, row["predicted_rank"]) == 1:
                predicted_top1_ketto = str(row["ketto_toroku_bango"])
                break

        actual_top1_ketto: str | None = None
        for ketto_toroku_bango, actual_rank in matched_actual_ranks.items():
            if actual_rank == 1:
                actual_top1_ketto = ketto_toroku_bango
                break

        first_row = group_rows[0]
        race_date = str(first_row["kaisai_nen"]) + str(first_row["kaisai_tsukihi"])
        eval_rows.append(
            FpRaceEvalRow(
                category=category,
                race_date=race_date,
                venue=keibajo_code,
                race_bango=race_bango,
                model_version=model_version,
                distance_band=cast("str | None", first_row["distance_band"]),
                field_size_band=cast("str | None", first_row["field_size_band"]),
                season_band=cast("str | None", first_row["season_band"]),
                class_code=cast("str | None", first_row["class_code"]),
                surface=cast("str | None", first_row["surface"]),
                predicted_top1_ketto=predicted_top1_ketto,
                actual_top1_ketto=actual_top1_ketto,
                matched_horses=len(pairs),
                top1_hit=cast(int, hits["top1_hit"]),
                place2_hit=cast(int, hits["place2_hit"]),
                place3_hit=cast(int, hits["place3_hit"]),
                place4_hit=hits["place4_hit"],
                place5_hit=hits["place5_hit"],
                place6_hit=hits["place6_hit"],
                fukusho_2p_hit=cast(int, hits["fukusho_2p_hit"]),
                top3_box_hit=cast(int, hits["top3_box_hit"]),
            )
        )
    return eval_rows


def build_rs_horse_eval_rows(
    category: str,
    model_version: str,
    pred_rows: list[dict[str, object]],
    results: dict[tuple[str, str, str], dict[str, object]],
    race_meta: dict[tuple[str, str], dict[str, object]],
) -> list[RsHorseEvalRow]:
    """Join RS prediction rows against `results`/`race_meta` to build one row per horse.

    A horse is skipped when: it has no matching result; its race has no
    metadata entry, or the metadata's `shusso_tosu` is None (needed to
    normalize corner_1); or the resulting corner1_norm classifies to None
    (straight-course race, no running-style ground truth).
    """
    eval_rows: list[RsHorseEvalRow] = []
    for row in pred_rows:
        keibajo_code = str(row["keibajo_code"])
        race_bango = str(row["race_bango"])
        ketto_toroku_bango = str(row["ketto_toroku_bango"])
        result = results.get((keibajo_code, race_bango, ketto_toroku_bango))
        if result is None:
            continue

        meta = race_meta.get((keibajo_code, race_bango))
        meta_shusso_tosu = meta["shusso_tosu"] if meta is not None else None
        if meta is None or meta_shusso_tosu is None:
            continue
        shusso_tosu = cast(int, meta_shusso_tosu)

        corner1_raw = str(result["corner_1"] or "")
        corner1_norm = compute_corner1_norm(corner1_raw, shusso_tosu)
        actual_class = classify_running_style(corner1_norm)
        if actual_class is None:
            continue

        kaisai_tsukihi = str(row["kaisai_tsukihi"])
        predicted_class = cast(int, row["predicted_class"])
        eval_rows.append(
            RsHorseEvalRow(
                category=category,
                race_date=str(row["kaisai_nen"]) + kaisai_tsukihi,
                venue=keibajo_code,
                race_bango=race_bango,
                ketto_toroku_bango=ketto_toroku_bango,
                model_version=model_version,
                distance_band=cells.classify_distance_band(cast("int | None", meta["kyori"])),
                field_size_band=cells.classify_field_size_band(shusso_tosu),
                season_band=cells.classify_season_band(kaisai_tsukihi),
                class_code=cast("str | None", meta["kyoso_joken_code"]),
                surface=cells.classify_surface(cast("str | None", meta["track_code"])),
                predicted_class=predicted_class,
                predicted_label=RS_CLASS_LABELS[predicted_class],
                actual_class=actual_class,
                actual_label=RS_CLASS_LABELS[actual_class],
                hit=1 if predicted_class == actual_class else 0,
            )
        )
    return eval_rows


# ── Day-level aggregation (feeds timeline.py extractors) ────────────────────


def compute_rank_pct(hits: Sequence[int | None]) -> float | None:
    """Compute a hit-rate percentage over the SUBSET of `hits` that are not
    None, i.e. a PER-METRIC denominator distinct from `len(hits)`.

    Returns None when every value is None -- this population (e.g. every
    race sharing a cell/day has `total_starters < 4`) has literally nothing
    to compute a place4_pct/place5_pct/place6_pct from, mirroring
    `aggregate_rs_day_metrics`'s own "None means not applicable, never a
    fabricated 0.0" precedent. Shared by `aggregate_fp_day_metrics` (day
    granularity) and `champion_cell_eval._aggregate_fp_cells` (cell
    granularity) so this exact denominator logic lives once, per this
    module's own docstring rationale for why the query/join/aggregate
    library lives here instead of being re-derived per caller.
    """
    eligible = [hit for hit in hits if hit is not None]
    if not eligible:
        return None
    return 100.0 * sum(eligible) / len(eligible)


def aggregate_fp_day_metrics(rows: Sequence[FpRaceEvalRow]) -> dict[str, float | None]:
    """Aggregate one day's FP race-eval rows into the shape
    `timeline.fp_metrics_for_timeline` expects (unprefixed `*_pct` keys plus
    `races`). Never raises on an empty `rows` -- returns an all-zero/all-None
    shape.

    `place4_pct`/`place5_pct`/`place6_pct` are `float | None`, computed via
    `compute_rank_pct` over each metric's OWN eligible-race subset (races
    where that specific `place{N}_hit` is not None -- see
    `_compute_race_hits`'s docstring for why ranks 4/5/6 need this and ranks
    1/2/3 do not) rather than the shared `total`/`races` denominator every
    other key here uses. This mirrors `aggregate_rs_day_metrics`'s own
    None-handling precedent: the key is ALWAYS present in the returned dict
    (even when every race in `rows` is too-small-a-field for that rank, or
    `rows` itself is empty), but its VALUE may be None -- callers
    (`sync_production._sync_fp_eval`, `timeline.fp_metrics_for_timeline`,
    `champion_cell_eval`) must skip logging a None value as a metric rather
    than blindly `float()`-casting it, the same discipline
    `aggregate_rs_day_metrics`'s own consumers already apply to
    `macro_f1_pct`.
    """
    total = len(rows)
    if total == 0:
        return {
            "races": 0.0,
            "top1_pct": 0.0,
            "place2_pct": 0.0,
            "place3_pct": 0.0,
            "place4_pct": None,
            "place5_pct": None,
            "place6_pct": None,
            "fukusho_2p_pct": 0.0,
            "top3_box_pct": 0.0,
        }
    return {
        "races": float(total),
        "top1_pct": 100.0 * sum(r.top1_hit for r in rows) / total,
        "place2_pct": 100.0 * sum(r.place2_hit for r in rows) / total,
        "place3_pct": 100.0 * sum(r.place3_hit for r in rows) / total,
        "place4_pct": compute_rank_pct([r.place4_hit for r in rows]),
        "place5_pct": compute_rank_pct([r.place5_hit for r in rows]),
        "place6_pct": compute_rank_pct([r.place6_hit for r in rows]),
        "fukusho_2p_pct": 100.0 * sum(r.fukusho_2p_hit for r in rows) / total,
        "top3_box_pct": 100.0 * sum(r.top3_box_hit for r in rows) / total,
    }


def aggregate_rs_day_metrics(rows: Sequence[RsHorseEvalRow]) -> dict[str, object]:
    """Aggregate one day's RS horse-eval rows into the shape
    `timeline.rs_metrics_for_timeline` expects: `overall_accuracy_pct`,
    `macro_f1_pct`, and a `per_class` list of {"label", "f1_pct"} dicts for
    all 4 RS_CLASS_LABELS (even when `rows` is empty).

    Precision/recall/F1 replicate serve_accuracy_report.py's
    `RunningStyleClassMetrics` formulas exactly: precision is None when no
    horse was predicted into a class; recall is None when no horse actually
    belonged to it; F1 is None whenever either is None, or when both are 0
    (their sum is 0, avoiding a spurious division by zero). All 3 values are
    stored on the 0-100 scale (`_pct` naming), matching
    `aggregate_fp_day_metrics`'s convention.
    """
    total = len(rows)
    per_class: list[dict[str, object]] = []
    f1_values: list[float] = []
    for cls_idx, label in enumerate(RS_CLASS_LABELS):
        pred_count = sum(1 for r in rows if r.predicted_class == cls_idx)
        actual_count = sum(1 for r in rows if r.actual_class == cls_idx)
        tp = sum(1 for r in rows if r.predicted_class == cls_idx and r.actual_class == cls_idx)
        precision = tp / pred_count if pred_count > 0 else None
        recall = tp / actual_count if actual_count > 0 else None
        f1_pct: float | None = None
        if precision is not None and recall is not None and (precision + recall) != 0:
            f1_pct = 100.0 * 2 * precision * recall / (precision + recall)
            f1_values.append(f1_pct)
        per_class.append({"label": label, "f1_pct": f1_pct})

    macro_f1_pct = sum(f1_values) / len(f1_values) if f1_values else None
    overall_accuracy_pct = 100.0 * sum(r.hit for r in rows) / total if total > 0 else 0.0

    return {
        "total_horses": total,
        "overall_accuracy_pct": overall_accuracy_pct,
        "macro_f1_pct": macro_f1_pct,
        "per_class": per_class,
    }


# ── DataFrame adapters ───────────────────────────────────────────────────────

_FP_EVAL_ROW_FIELD_NAMES: Final[tuple[str, ...]] = tuple(
    f.name for f in dataclasses.fields(FpRaceEvalRow)
)
_RS_EVAL_ROW_FIELD_NAMES: Final[tuple[str, ...]] = tuple(
    f.name for f in dataclasses.fields(RsHorseEvalRow)
)


def fp_eval_rows_to_dataframe(rows: Sequence[FpRaceEvalRow]) -> pd.DataFrame:
    """Convert FP race-eval rows to a DataFrame; an empty input still yields
    the correct (empty) columns rather than a columnless empty frame."""
    if not rows:
        return pd.DataFrame(columns=list(_FP_EVAL_ROW_FIELD_NAMES))
    return pd.DataFrame([dataclasses.asdict(r) for r in rows])


def rs_eval_rows_to_dataframe(rows: Sequence[RsHorseEvalRow]) -> pd.DataFrame:
    """Convert RS horse-eval rows to a DataFrame; an empty input still yields
    the correct (empty) columns rather than a columnless empty frame."""
    if not rows:
        return pd.DataFrame(columns=list(_RS_EVAL_ROW_FIELD_NAMES))
    return pd.DataFrame([dataclasses.asdict(r) for r in rows])
