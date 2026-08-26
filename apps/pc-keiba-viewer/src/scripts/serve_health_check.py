"""Production read-only serve-health diagnostic for finish-position predictions.

Detects the defect patterns characterized by
docs/probes/jra-serving-audit-jun-jul-2026-07-17.md's investigation of the
2026-07-12 "Cluster B" incident (a 23-47 second write-burst that scored 72
JRA races off degraded/stale features, collapsing within-race
``predicted_score`` differentiation to ~1/11th of healthy variance). This is
a general-purpose ongoing health check -- it must work correctly against ANY
date, not just 2026-07-12 -- covering six independent checks:

  1. Coverage:      confirmed, already-run races with zero prediction rows.
  2. Quality:        within-(race, model_version) predicted_score stddev
                     collapse (the Cluster-B signature).
  3. Routing parity: confirmed races missing their cell-routing-expected
                     model_version row.
  4. Burst detection: minute-buckets with a suspicious number of races
                     written at once.
  5. D1 self-heal activity: informational count only, never affects the
                     exit code (self-heal firing can be genuine repair or
                     the known false-positive re-trigger pattern documented
                     as the audit's Defect C).
  6. R2 shard presence: whether the running-style prediction shard
                     (add-pacestyle-features.py's raw-iceberg-v1 R2 input)
                     exists for each of the last R2_SHARD_LOOKBACK_DAYS
                     days, checked for both jra and nar regardless of
                     --category. A day still in progress (today JST) is
                     always reported but never counted as a gap -- a
                     same-day shard can legitimately not exist yet (see
                     add-pacestyle-features.py's create_empty_rs_predictions
                     fallback); a settled day (strictly before today) with
                     no shard is a genuine gap and DOES flip the exit code.
                     R2 access itself being unavailable (no credentials,
                     network failure) is reported as N/A, distinct from a
                     real gap, and never flips the exit code -- mirrors
                     check 3's routing_supported=False treatment.

Run with:
    uv run python src/scripts/serve_health_check.py --date 20260712 --category jra
    uv run python src/scripts/serve_health_check.py --date 20260712 --category jra --json

``--date`` defaults to today JST. ``--category`` defaults to "jra" --
this tool's checks/thresholds (the 0.3 quality stddev cutoff, the >10
burst-bucket cutoff) were calibrated against JRA incident data specifically;
other category values are accepted (not restricted via argparse ``choices``)
but their thresholds may not transfer, and check 3 (routing parity) reports
itself as unsupported (not a mismatch) for any category absent from
cell_routing.json (currently only "jra" and "ban-ei" have rules -- "nar"
does not).

Deliberately self-contained: this module does not import
``serve_accuracy_report`` (a sibling script) or cross-import
``predict_lib`` (the container's separate package) even though both host
near-identical logic (``parse_post_time_jst``, the cell-routing
first-match-wins rule engine) -- a diagnostic tool that audits a pipeline
should not share runtime code with the pipeline it audits, so
``parse_post_time_jst`` and the cell-routing dimension-derivation formulas
are reimplemented locally, matching their source's behavior exactly (see
each function's docstring for the precedent it mirrors).

Connection: Neon only (``NEON_PRIMARY_URL``), read-only/autocommit session.
No local PG mirror is used -- Neon alone carries jvd_ra/jvd_se plus the
predictions table (confirmed by the audit doc), so a single connection
covers every check. The DSN value is never printed or logged anywhere in
this module.

Exit code: 0 if checks 1-4 and 6 all found zero anomalies (check 5/D1 never
affects this); 1 if any of checks 1-4 or 6 found at least one anomaly; 2 if
the tool itself failed (e.g. a Neon connection error, a missing
NEON_PRIMARY_URL, or an unhandled bug) -- this distinction is enforced by
main()'s outer catch-all, not just by the explicit connection-error
handling in run(), so exit code 1 is never accidentally produced by a code
bug rather than a genuine finding.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import traceback
from collections.abc import Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final, Protocol, SupportsFloat, TypedDict, cast

import boto3
import psycopg
from botocore.exceptions import BotoCoreError, ClientError

# ── Constants ─────────────────────────────────────────────────────────────────

# JST (UTC+9, no DST in Japan) -- mirrors serve_accuracy_report.py's constant
# of the same name/value (reimplemented locally, not imported -- see module
# docstring).
JST: Final[timezone] = timezone(timedelta(hours=9))

NEON_ENV_VAR: Final[str] = "NEON_PRIMARY_URL"
DEFAULT_CATEGORY: Final[str] = "jra"

# Check 2 threshold: sits comfortably between the documented degraded range
# (0.05-0.15, Cluster B) and healthy range (0.72-1.57, Cluster A), with wide
# margin on both sides.
QUALITY_DEGRADED_STDDEV_THRESHOLD: Final[float] = 0.3

# Check 4 threshold: flag a minute-bucket with MORE THAN this many distinct
# races written in it.
BURST_RACE_COUNT_THRESHOLD: Final[int] = 10

RACE_STATUS_FULLY_HEALTHY: Final[str] = "fully_healthy"
RACE_STATUS_PARTIALLY_DEGRADED: Final[str] = "partially_degraded"
RACE_STATUS_FULLY_DEGRADED: Final[str] = "fully_degraded"

D1_DATABASE_NAME: Final[str] = "finish-position-cron-db"
D1_QUERY_TIMEOUT_SECONDS: Final[int] = 30
D1_UNAVAILABLE_LABEL: Final[str] = "N/A (wrangler unavailable)"

# Check 6 constants: R2 raw-iceberg-v1 RS-prediction shard presence over a
# trailing window. RUNNING_STYLE_CATALOG_GENERATION / R2_PREDICTIONS_PREFIX
# and the R2_* env var names mirror add-pacestyle-features.py's own
# constants of the same name/value exactly (redefined locally rather than
# imported -- see module docstring on self-containment). Both categories
# are always checked regardless of --category: add-pacestyle-features.py's
# R2 path only ever serves jra/nar, and shard landing is a pipeline-wide
# signal, not scoped to whichever category this invocation happens to be
# diagnosing via checks 1-5.
R2_SHARD_LOOKBACK_DAYS: Final[int] = 7
R2_SHARD_CATEGORIES: Final[tuple[str, ...]] = ("jra", "nar")
ROUTING_HEALTH_SUPPORTED_CATEGORIES: Final[frozenset[str]] = frozenset({"jra", "ban-ei"})
RUNNING_STYLE_CATALOG_GENERATION: Final[str] = "raw-iceberg-v1"
R2_PREDICTIONS_PREFIX: Final[str] = (
    f"running-style/predictions/by-day/{RUNNING_STYLE_CATALOG_GENERATION}"
)
R2_BUCKET_ENV_VAR: Final[str] = "R2_BUCKET"
R2_BUCKET_DEFAULT: Final[str] = "pc-keiba-features-archive"
R2_ACCOUNT_ID_ENV_VAR: Final[str] = "R2_ACCOUNT_ID"
R2_ACCESS_KEY_ID_ENV_VAR: Final[str] = "R2_ACCESS_KEY_ID"
R2_SECRET_ACCESS_KEY_ENV_VAR: Final[str] = "R2_SECRET_ACCESS_KEY"
R2_UNAVAILABLE_LABEL: Final[str] = "N/A (R2 access unavailable)"

# apps/pc-keiba-viewer/src/scripts/serve_health_check.py -> apps/
_APPS_DIR: Final[Path] = Path(__file__).resolve().parents[3]
CELL_ROUTING_CONFIG_PATH: Final[Path] = (
    _APPS_DIR / "finish-position-predict-container" / "src" / "predict_lib" / "cell_routing.json"
)
FINISH_POSITION_CRON_DIR: Final[Path] = _APPS_DIR / "finish-position-cron"

EXIT_OK: Final[int] = 0
EXIT_ANOMALIES_FOUND: Final[int] = 1
EXIT_TOOL_FAILURE: Final[int] = 2

# (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
RaceKey = tuple[str, str, str, str]

# ── Row type aliases (SQL fetch shapes) ─────────────────────────────────────

# (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, hasso_jikoku,
#  track_code, kyori, shusso_tosu, grade_code, kyoso_joken_code)
ConfirmedRaceRow = tuple[str, str, str, str, str | None, str, int, int, str, str]

# (keibajo_code, race_bango, model_version, predicted_score)
PredictionRow = tuple[str, str, str, float | None]

# (minute_bucket_utc, race_count)
BurstRow = tuple[datetime, int]

# (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, model_version)
QualityGroupKey = tuple[str, str, str, str, str]

# ── I/O Protocol (mirrors serve_accuracy_report.py's own) ──────────────────


class CursorLike(Protocol):
    def execute(self, query: str, params: object = None) -> object: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...


class ConnectionLike(Protocol):
    def cursor(self) -> CursorLike: ...

    def close(self) -> None: ...


class ListObjectsClient(Protocol):
    """The one boto3 S3-client method check 6 needs -- narrowed to a
    structural Protocol (mirrors ConnectionLike/CursorLike above) so tests
    can pass a plain mock instead of a real boto3 client."""

    def list_objects_v2(self, **kwargs: object) -> Mapping[str, object]: ...


# ── Data classes ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class QualityGroupResult:
    """Within-(race, model_version) predicted_score stddev result (check 2)."""

    kaisai_nen: str
    kaisai_tsukihi: str
    keibajo_code: str
    race_bango: str
    model_version: str
    sample_count: int
    stddev: float
    degraded: bool


@dataclass(frozen=True)
class RaceQualityRollup:
    """Per-race roll-up of check 2's (race, model_version) groups."""

    kaisai_nen: str
    kaisai_tsukihi: str
    keibajo_code: str
    race_bango: str
    status: str  # RACE_STATUS_FULLY_HEALTHY | _PARTIALLY_DEGRADED | _FULLY_DEGRADED


@dataclass(frozen=True)
class RoutingMismatch:
    """A confirmed race missing its cell-routing-expected model_version row."""

    kaisai_nen: str
    kaisai_tsukihi: str
    keibajo_code: str
    race_bango: str
    expected_model_version: str


@dataclass(frozen=True)
class BurstBucket:
    """A minute-bucket flagged by check 4 for writing more than the threshold."""

    minute_utc: datetime
    race_count: int

    @property
    def minute_jst(self) -> datetime:
        return self.minute_utc.astimezone(JST)


@dataclass(frozen=True)
class R2ShardCheck:
    """One (day, category) R2 raw-iceberg-v1 shard-presence result (check 6).

    ``shard_found`` is a tri-state: True/False when the presence check
    actually ran against R2, None when the check mechanism itself could not
    run at all (missing credentials, a client/network error) -- mirrors
    check 5's None-means-N/A contract. A None entry is never treated as a
    gap (see find_r2_shard_gaps).
    """

    day: str  # YYYYMMDD
    category: str
    shard_found: bool | None


@dataclass(frozen=True)
class RoutingCondition:
    dimension: str
    values: frozenset[str]


@dataclass(frozen=True)
class RoutingRule:
    conditions: tuple[RoutingCondition, ...]
    variant: str


@dataclass(frozen=True)
class CategoryRoutingConfig:
    default_variant: str
    variant_model_versions: dict[str, str]
    rules: tuple[RoutingRule, ...]


@dataclass(frozen=True)
class HealthCheckReport:
    """Full assembled report for one (date, category) invocation."""

    date_str: str
    category: str
    checked_race_count: int
    coverage_gaps: tuple[RaceKey, ...]
    quality_groups: tuple[QualityGroupResult, ...]
    quality_rollup: tuple[RaceQualityRollup, ...]
    routing_supported: bool
    routing_mismatches: tuple[RoutingMismatch, ...]
    burst_buckets: tuple[BurstBucket, ...]
    d1_gap_event_count: int | None
    r2_shard_checks: tuple[R2ShardCheck, ...]
    r2_shard_gaps: tuple[R2ShardCheck, ...]
    r2_shard_access_available: bool

    @property
    def degraded_quality_groups(self) -> tuple[QualityGroupResult, ...]:
        return tuple(g for g in self.quality_groups if g.degraded)

    @property
    def exit_code(self) -> int:
        return determine_exit_code(
            coverage_gap_count=len(self.coverage_gaps),
            degraded_group_count=len(self.degraded_quality_groups),
            routing_mismatch_count=len(self.routing_mismatches),
            burst_bucket_count=len(self.burst_buckets),
            r2_shard_gap_count=len(self.r2_shard_gaps),
        )


# ── Pure helpers: race-id / post-time (check 1) ─────────────────────────────


def format_race_id(category: str, race_key: RaceKey) -> str:
    """Build the repo-wide ``race_id`` string:
    ``{category}:{kaisai_nen}:{kaisai_tsukihi}:{keibajo_code}:{race_bango}``,
    matching ``predict_lib/race_id.py``'s ``format_race_id`` convention.
    """
    kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango = race_key
    return f"{category}:{kaisai_nen}:{kaisai_tsukihi}:{keibajo_code}:{race_bango}"


def parse_post_time_jst(
    kaisai_nen: str,
    kaisai_tsukihi: str,
    hasso_jikoku: str | None,
) -> datetime | None:
    """Parse jvd_ra/nvd_ra's ``hasso_jikoku`` (post time, JST wall-clock
    ``HHMM``) into an aware JST datetime.

    Mirrors ``serve_accuracy_report.py``'s function of the same name and
    signature (reimplemented locally rather than imported -- see this
    module's docstring). Returns None when the value cannot be used as a
    serve-time cutoff: missing/blank, the jvd ``'0000'`` not-yet-determined
    placeholder (the same ``'00'``-family trap documented for
    kakutei_chakujun/shusso_tosu), out-of-range digits, or an otherwise
    invalid calendar date.
    """
    if not hasso_jikoku:
        return None
    hhmm = hasso_jikoku.strip()
    if len(hhmm) != 4 or not hhmm.isdigit() or hhmm == "0000":
        return None
    hour, minute = int(hhmm[:2]), int(hhmm[2:])
    if hour > 23 or minute > 59:
        return None
    try:
        return datetime(
            int(kaisai_nen), int(kaisai_tsukihi[:2]), int(kaisai_tsukihi[2:]),
            hour, minute, tzinfo=JST,
        )
    except ValueError:
        return None


def is_post_time_passed(post_time: datetime | None, now: datetime) -> bool:
    """A race counts as "already gone off" only when its post time is known
    and <= now.

    An unparseable/placeholder ``hasso_jikoku`` (``post_time=None``) fails
    closed to False: this function cannot confirm the race has gone off, so
    it is never flagged as a coverage gap purely from a ``hasso_jikoku``
    data-quality issue -- independent of whether ``shusso_tosu`` already
    confirms the race as settled (settlement implies the race has run, but a
    separately-corrupt ``hasso_jikoku`` on that same row should not by
    itself manufacture a false-positive gap).
    """
    if post_time is None:
        return False
    return post_time <= now


def find_coverage_gaps(
    confirmed_races: Sequence[ConfirmedRaceRow],
    predicted_race_keys: AbstractSet[RaceKey],
    now: datetime,
) -> list[RaceKey]:
    """Check 1: confirmed races whose post time has passed but which have
    zero prediction rows of ANY model_version.

    ``confirmed_races`` must already be filtered to the placeholder-safe
    predicate (``trim(shusso_tosu) NOT IN ('', '00')`` -- see
    ``query_confirmed_races``); this function only adds the "post time has
    already passed" filter, so a later-today race that hasn't gone off yet
    is never flagged (it is simply not-yet-served, not a gap).
    """
    gaps: list[RaceKey] = []
    for row in confirmed_races:
        kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, hasso_jikoku = row[:5]
        post_time = parse_post_time_jst(kaisai_nen, kaisai_tsukihi, hasso_jikoku)
        if not is_post_time_passed(post_time, now):
            continue
        key = (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        if key not in predicted_race_keys:
            gaps.append(key)
    return gaps


def distinct_race_keys(
    rows: Sequence[PredictionRow], kaisai_nen: str, kaisai_tsukihi: str,
) -> set[RaceKey]:
    """Race identities with at least one prediction row, of ANY
    model_version -- the population check 1 diffs confirmed races against."""
    return {
        (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        for keibajo_code, race_bango, _model_version, _predicted_score in rows
    }


# ── Pure helpers: quality / Cluster-B signature (check 2) ──────────────────


def group_quality_rows(
    rows: Sequence[PredictionRow], kaisai_nen: str, kaisai_tsukihi: str,
) -> dict[QualityGroupKey, list[float]]:
    """Group ``predicted_score`` values by (kaisai_nen, kaisai_tsukihi,
    keibajo_code, race_bango, model_version) -- NOT by race alone.

    This exact grouping is the single most important design detail of this
    tool: on 2026-07-11, the SAME race can carry both a healthy row-set
    (Cluster A, default model_version) and a degraded row-set (Cluster B
    backfill, a routed-variant model_version) simultaneously, because the
    predictions table's primary key includes model_version. Grouping by race
    alone would blend the two and hide the signal.

    Rows with ``predicted_score is None`` are skipped defensively: the
    column is not expected to ever be NULL for a genuinely-written
    prediction row, but a stray NULL must not crash stddev computation for
    its whole group.
    """
    groups: dict[QualityGroupKey, list[float]] = {}
    for keibajo_code, race_bango, model_version, predicted_score in rows:
        if predicted_score is None:
            continue
        key = (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, model_version)
        groups.setdefault(key, []).append(predicted_score)
    return groups


def compute_quality_groups(
    groups: Mapping[QualityGroupKey, Sequence[float]],
) -> list[QualityGroupResult]:
    """Compute within-group ``predicted_score`` stddev and flag DEGRADED
    groups (stddev < QUALITY_DEGRADED_STDDEV_THRESHOLD).

    Uses population standard deviation (``statistics.pstdev``, ddof=0)
    rather than sample stddev (``statistics.stdev``, ddof=1): the two agree
    to several decimal places at the field sizes involved here (8-18
    horses) relative to the threshold's wide margin from both the
    documented degraded range (0.05-0.15) and healthy range (0.72-1.57), and
    ``pstdev`` handles a (defensive-only) single-row group by returning 0.0
    instead of raising -- ``stdev`` raises ``StatisticsError`` for n<2.
    """
    results: list[QualityGroupResult] = []
    for (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, model_version), scores in groups.items():
        stddev = statistics.pstdev(scores)
        results.append(QualityGroupResult(
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
            model_version=model_version,
            sample_count=len(scores),
            stddev=stddev,
            degraded=stddev < QUALITY_DEGRADED_STDDEV_THRESHOLD,
        ))
    return results


def rollup_quality_by_race(groups: Sequence[QualityGroupResult]) -> list[RaceQualityRollup]:
    """Roll up check 2's (race, model_version) groups to one status per race:
    "fully_healthy" if every model_version group present is >=threshold,
    "fully_degraded" if all present groups are below it, else
    "partially_degraded" (the Cluster A/B mixed case)."""
    degraded_flags_by_race: dict[RaceKey, list[bool]] = {}
    for group in groups:
        key = (group.kaisai_nen, group.kaisai_tsukihi, group.keibajo_code, group.race_bango)
        degraded_flags_by_race.setdefault(key, []).append(group.degraded)
    rollups: list[RaceQualityRollup] = []
    for (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango), flags in degraded_flags_by_race.items():
        status = RACE_STATUS_PARTIALLY_DEGRADED
        if all(flags):
            status = RACE_STATUS_FULLY_DEGRADED
        elif not any(flags):
            status = RACE_STATUS_FULLY_HEALTHY
        rollups.append(RaceQualityRollup(
            kaisai_nen=kaisai_nen,
            kaisai_tsukihi=kaisai_tsukihi,
            keibajo_code=keibajo_code,
            race_bango=race_bango,
            status=status,
        ))
    return rollups


# ── Pure helpers: cell-routing reimplementation (check 3) ──────────────────
#
# Reimplements apps/finish-position-predict-container/src/predict_lib/
# cell_router.py's derive_surface/derive_distance_band/derive_field_band/
# derive_season/derive_class/resolve_dimension/all_conditions_match/
# CellRouter.resolve_variant in pure Python, matching their behavior
# exactly (including the dimension-derivation formulas) without
# cross-importing predict_lib -- see module docstring for why. Precedent:
# the audit doc that specified this tool did the same reimplementation for
# the same reason ("ルーティング期待値は cell_routing.json + cell_router.py
# の first-match-wins ロジックを Python で再実装").
#
# is_final_race is intentionally NOT special-cased in
# resolve_routing_dimension below: no rule in the live cell_routing.json
# uses it, this tool has no card-level context (card_max_race_bango) to
# resolve it correctly, and the generic raw-column fallback already
# reproduces the real resolve_dimension's fail-closed behavior for it
# (this tool's entry dict never has an "is_final_race" key, so the fallback
# returns None -> the condition never matches), the same outcome the real
# function has whenever card_max_race_bango is not supplied.


def derive_surface(track_code: str, category: str) -> str:
    if category != "jra":
        return "dirt"
    if track_code.startswith("1"):
        return "turf"
    if track_code.startswith("2"):
        return "dirt"
    return "other"


def derive_distance_band(kyori: int) -> str:
    if kyori < 1200:
        return "sprint"
    if kyori < 1600:
        return "mile"
    if kyori < 2000:
        return "intermediate"
    if kyori < 2400:
        return "long"
    return "extended"


def derive_field_band(shusso_tosu: int) -> str:
    if shusso_tosu <= 10:
        return "f_le10"
    if shusso_tosu <= 13:
        return "f11_13"
    if shusso_tosu <= 15:
        return "f14_15"
    return "f16p"


def derive_season(month: int) -> str:
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    if month in {9, 10, 11}:
        return "autumn"
    return "winter"


def derive_class(grade_code: str) -> str:
    return grade_code if grade_code else "unknown"


def resolve_routing_dimension(
    entry: Mapping[str, object], dimension: str, category: str,
) -> str | None:
    """Resolve one cell-routing dimension's value for a race entry.

    ``entry`` is built from ``ConfirmedRaceRow`` fields (see
    ``find_routing_mismatches``): ``keibajo_code``, ``track_code``,
    ``kyori``, ``shusso_tosu``, ``kaisai_tsukihi``, ``grade_code``, and the
    raw passthrough columns (e.g. ``kyoso_joken_code``) that the live
    cell_routing.json rules key on directly. field_band is derived from
    jvd_ra's own (confirmed, post-settlement) ``shusso_tosu`` here rather
    than a declared-runner count (``len(entries)`` in the real
    ``resolve_variant``, unavailable to this retrospective/read-only tool) --
    a known, accepted divergence from live serving in the rare case of a
    late scratch near a field_band boundary, the same divergence
    ``cell_router.py``'s own field_band branch already documents as an
    "accepted, unavoidable predict-time reality."
    """
    if dimension == "venue":
        raw = entry.get("keibajo_code")
        return str(raw).strip() if raw is not None else None
    if dimension == "surface":
        track_code = entry.get("track_code")
        if track_code is None:
            return None
        return derive_surface(str(track_code), category)
    if dimension == "distance_band":
        kyori = entry.get("kyori")
        if kyori is None:
            return None
        return derive_distance_band(int(float(str(kyori))))
    if dimension == "field_band":
        shusso_tosu = entry.get("shusso_tosu")
        if shusso_tosu is None:
            return None
        return derive_field_band(int(float(str(shusso_tosu))))
    if dimension == "season":
        tsukihi = entry.get("kaisai_tsukihi")
        if tsukihi is None:
            return None
        month_str = str(tsukihi).strip()[:2]
        if not month_str.isdigit():
            return None
        return derive_season(int(month_str))
    if dimension == "class":
        grade_code = entry.get("grade_code")
        if grade_code is None:
            return None
        return derive_class(str(grade_code).strip())
    raw = entry.get(dimension)
    return str(raw).strip() if raw is not None else None


def all_routing_conditions_match(
    entry: Mapping[str, object],
    conditions: tuple[RoutingCondition, ...],
    category: str,
) -> bool:
    for condition in conditions:
        value = resolve_routing_dimension(entry, condition.dimension, category)
        if value is None or value not in condition.values:
            return False
    return True


def resolve_expected_model_version(
    config: CategoryRoutingConfig, entry: Mapping[str, object], category: str,
) -> str:
    """First-match-wins rule resolution, mirroring
    ``CellRouter.resolve_variant``: the first rule (in file order) whose
    conditions all match wins; no match falls back to ``default_variant``.
    """
    for rule in config.rules:
        if all_routing_conditions_match(entry, rule.conditions, category):
            return config.variant_model_versions[rule.variant]
    return config.variant_model_versions[config.default_variant]


def parse_cell_routing_config(
    payload: Mapping[str, object], category: str,
) -> CategoryRoutingConfig | None:
    """Parse the subset of cell_routing.json needed to resolve an expected
    model_version for ``category``. Returns None if the category has no
    routing entry or when this retrospective checker lacks the dimensions
    needed to reproduce that category's live route. NAR is intentionally
    unsupported here because ``ConfirmedRaceRow`` does not carry
    ``nar_subclass`` or ``current_baba_condition``; treating its config as
    checkable would fabricate routing mismatches. Only the current
    "variants" object format is supported (both live jra/ban-ei entries use
    it) -- the legacy flat sim_model_version/base_model_version format
    ``cell_router.py`` also accepts is not reimplemented here since no live
    file uses it.
    """
    if category not in ROUTING_HEALTH_SUPPORTED_CATEGORIES or category not in payload:
        return None
    category_payload = cast(Mapping[str, object], payload[category])
    variants_payload = cast(Mapping[str, object], category_payload["variants"])
    variant_model_versions = {
        name: str(cast(Mapping[str, object], spec)["model_version"])
        for name, spec in variants_payload.items()
    }
    rules: list[RoutingRule] = []
    for rule_payload in cast(list[object], category_payload["rules"]):
        rule_map = cast(Mapping[str, object], rule_payload)
        conditions = tuple(
            RoutingCondition(
                dimension=str(cast(Mapping[str, object], condition)["dimension"]),
                values=frozenset(
                    str(v) for v in cast(list[object], cast(Mapping[str, object], condition)["values"])
                ),
            )
            for condition in cast(list[object], rule_map["conditions"])
        )
        rules.append(RoutingRule(conditions=conditions, variant=str(rule_map["variant"])))
    return CategoryRoutingConfig(
        default_variant=str(category_payload["default_variant"]),
        variant_model_versions=variant_model_versions,
        rules=tuple(rules),
    )


def model_versions_by_race_key(
    rows: Sequence[PredictionRow], kaisai_nen: str, kaisai_tsukihi: str,
) -> dict[RaceKey, set[str]]:
    result: dict[RaceKey, set[str]] = {}
    for keibajo_code, race_bango, model_version, _predicted_score in rows:
        key = (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        result.setdefault(key, set()).add(model_version)
    return result


def find_routing_mismatches(
    confirmed_races: Sequence[ConfirmedRaceRow],
    model_versions_by_race: Mapping[RaceKey, AbstractSet[str]],
    routing_config: CategoryRoutingConfig | None,
    category: str,
) -> list[RoutingMismatch]:
    """Check 3: confirmed races missing a prediction row under their
    cell-routing-expected model_version. Returns [] (not an error) when
    ``routing_config`` is None -- the category simply has no routing rules
    to check against (see ``HealthCheckReport.routing_supported``)."""
    if routing_config is None:
        return []
    mismatches: list[RoutingMismatch] = []
    for (
        kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, _hasso_jikoku,
        track_code, kyori, shusso_tosu, grade_code, kyoso_joken_code,
    ) in confirmed_races:
        entry: dict[str, object] = {
            "keibajo_code": keibajo_code,
            "track_code": track_code,
            "kyori": kyori,
            "shusso_tosu": shusso_tosu,
            "kaisai_tsukihi": kaisai_tsukihi,
            "grade_code": grade_code,
            "kyoso_joken_code": kyoso_joken_code,
        }
        expected_model_version = resolve_expected_model_version(routing_config, entry, category)
        key = (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        existing_model_versions = model_versions_by_race.get(key, frozenset())
        if expected_model_version not in existing_model_versions:
            mismatches.append(RoutingMismatch(
                kaisai_nen=kaisai_nen,
                kaisai_tsukihi=kaisai_tsukihi,
                keibajo_code=keibajo_code,
                race_bango=race_bango,
                expected_model_version=expected_model_version,
            ))
    return mismatches


# ── Pure helpers: burst detection (check 4) ─────────────────────────────────


def flag_burst_buckets(buckets: Sequence[BurstRow]) -> list[BurstBucket]:
    """Flag any minute-bucket with strictly more than
    BURST_RACE_COUNT_THRESHOLD distinct races written in it. ``buckets`` is
    already grouped in SQL (``date_trunc('minute', prediction_generated_at)``,
    ``COUNT(DISTINCT (keibajo_code, race_bango))``) -- see
    ``query_burst_buckets``."""
    return [
        BurstBucket(minute_utc=minute, race_count=race_count)
        for minute, race_count in buckets
        if race_count > BURST_RACE_COUNT_THRESHOLD
    ]


# ── Pure helper: exit-code determination ────────────────────────────────────


def determine_exit_code(
    coverage_gap_count: int,
    degraded_group_count: int,
    routing_mismatch_count: int,
    burst_bucket_count: int,
    r2_shard_gap_count: int,
) -> int:
    """Pure 0/1 decision for checks 1-4 and 6 (check 5/D1 never affects this
    -- self-heal firing can be either genuine repair or the known
    false-positive re-trigger pattern documented as the audit's Defect C, so
    a raw count alone is not a clean pass/fail signal). check 6 (R2 shard
    presence) DOES flip the exit code for a genuine gap: r2_shard_gap_count
    only ever counts settled days (strictly before today JST) with a
    confirmed-missing shard (see find_r2_shard_gaps) -- a day still in
    progress or an R2-access failure never contributes to this count, so
    neither ever flips the exit code through this parameter. The distinct
    "tool itself failed" exit code (2) is produced by run()/main()'s
    exception handling, not by this function.
    """
    any_anomaly = (
        coverage_gap_count > 0
        or degraded_group_count > 0
        or routing_mismatch_count > 0
        or burst_bucket_count > 0
        or r2_shard_gap_count > 0
    )
    return EXIT_ANOMALIES_FOUND if any_anomaly else EXIT_OK


# ── Pure helpers: D1 gap-events JSON parsing ────────────────────────────────


def build_d1_gap_events_query(date_str: str, category: str) -> str:
    """Build the SELECT-only SQL for check 5. ``date_str`` is always an
    8-digit validated YYYYMMDD by the time this is called (validate_date_arg
    runs before any check), matching ``finish_position_coverage_gap_events``
    .run_ymd's own storage format exactly (verified live against D1:
    ``run_ymd = '20260712'``). ``category`` is defensively quote-escaped
    even though it is not attacker-controlled in this CLI tool's threat
    model."""
    escaped_category = category.replace("'", "''")
    return (
        "SELECT COUNT(*) AS gap_event_count FROM finish_position_coverage_gap_events "
        f"WHERE run_ymd = '{date_str}' AND category = '{escaped_category}'"
    )


def parse_d1_gap_events_count(stdout: str) -> int | None:
    """Parse the ``gap_event_count`` column out of
    ``wrangler d1 execute --json`` output (verified live shape: a JSON array
    of one statement-result object, each with a "results" row-object list).
    Returns None -- never raises -- for any unexpected shape, matching
    ``query_d1_gap_event_count``'s own "never crash the tool" contract."""
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, list) or not payload:
        return None
    first_statement = payload[0]
    if not isinstance(first_statement, dict):
        return None
    results = first_statement.get("results")
    if not isinstance(results, list) or not results:
        return None
    first_row = results[0]
    if not isinstance(first_row, dict):
        return None
    count = first_row.get("gap_event_count")
    if not isinstance(count, int):
        return None
    return count


# ── Pure helpers: R2 shard presence (check 6) ───────────────────────────────


def trailing_day_strs(end_date_str: str, lookback_days: int) -> list[str]:
    """Return ``lookback_days`` YYYYMMDD strings ending at (and including)
    ``end_date_str``, oldest first -- e.g.
    ``trailing_day_strs("20260718", 3) == ["20260716", "20260717", "20260718"]``.
    Builds check 6's R2 shard-presence lookback window.
    """
    end = date(int(end_date_str[:4]), int(end_date_str[4:6]), int(end_date_str[6:8]))
    return [
        (end - timedelta(days=offset)).strftime("%Y%m%d")
        for offset in range(lookback_days - 1, -1, -1)
    ]


def r2_shard_prefix(day_str: str, category: str) -> str:
    """R2 key prefix for one day/category's RS-prediction shard directory,
    matching add-pacestyle-features.py's ``stage_rs_predictions_from_r2``
    glob shape (see ``R2_PREDICTIONS_PREFIX``'s comment for why this is a
    local redefinition rather than an import)."""
    yyyy, mm, dd = day_str[:4], day_str[4:6], day_str[6:8]
    return f"{R2_PREDICTIONS_PREFIX}/{yyyy}/{mm}/{dd}/{category}/"


def find_r2_shard_gaps(
    checks: Sequence[R2ShardCheck], today_str: str,
) -> list[R2ShardCheck]:
    """Check 6: (day, category) pairs with a confirmed-missing shard on a
    day strictly before today -- a genuine gap.

    A day still in progress (``day >= today_str``) is never flagged even
    when its shard has not landed yet, mirroring ``is_post_time_passed``'s
    fail-closed "not yet due" treatment for check 1: a same-day shard can
    legitimately not exist yet early in the race day (see
    add-pacestyle-features.py's ``create_empty_rs_predictions`` fallback),
    so today alone is exempt regardless of what date_str was requested.
    ``shard_found=None`` ("couldn't even check") is never a gap either --
    only a confirmed False counts.
    """
    return [c for c in checks if c.shard_found is False and c.day < today_str]


def r2_shard_access_available(checks: Sequence[R2ShardCheck]) -> bool:
    """True once at least one check resolved to True/False rather than the
    couldn't-even-check None -- mirrors ``routing_supported``'s role for
    check 3: an empty ``r2_shard_gaps`` list only means something once the
    checking mechanism itself produced at least one real answer."""
    return any(c.shard_found is not None for c in checks)


# ── Pure orchestration: assemble the full report from fetched data ─────────


def build_health_check_report(
    date_str: str,
    category: str,
    confirmed_races: Sequence[ConfirmedRaceRow],
    prediction_rows: Sequence[PredictionRow],
    burst_rows: Sequence[BurstRow],
    routing_payload: Mapping[str, object],
    d1_gap_event_count: int | None,
    r2_shard_checks: Sequence[R2ShardCheck],
    now: datetime,
) -> HealthCheckReport:
    """Pure assembly of a full report from already-fetched data -- no I/O.
    Thin wrapper functions (query_*, connect_neon, query_d1_gap_event_count,
    query_r2_shard_presence) do the fetching; this function and everything
    it calls is directly unit-testable without mocks."""
    kaisai_nen = date_str[:4]
    kaisai_tsukihi = date_str[4:]

    predicted_race_keys = distinct_race_keys(prediction_rows, kaisai_nen, kaisai_tsukihi)
    coverage_gaps = find_coverage_gaps(confirmed_races, predicted_race_keys, now)

    quality_groups = compute_quality_groups(
        group_quality_rows(prediction_rows, kaisai_nen, kaisai_tsukihi),
    )
    quality_rollup = rollup_quality_by_race(quality_groups)

    routing_config = parse_cell_routing_config(routing_payload, category)
    model_versions_by_race = model_versions_by_race_key(prediction_rows, kaisai_nen, kaisai_tsukihi)
    routing_mismatches = find_routing_mismatches(
        confirmed_races, model_versions_by_race, routing_config, category,
    )

    burst_buckets = flag_burst_buckets(burst_rows)

    today_str = now.strftime("%Y%m%d")
    r2_shard_gaps = find_r2_shard_gaps(r2_shard_checks, today_str)

    return HealthCheckReport(
        date_str=date_str,
        category=category,
        checked_race_count=len(confirmed_races),
        coverage_gaps=tuple(coverage_gaps),
        quality_groups=tuple(quality_groups),
        quality_rollup=tuple(quality_rollup),
        routing_supported=routing_config is not None,
        routing_mismatches=tuple(routing_mismatches),
        burst_buckets=tuple(burst_buckets),
        d1_gap_event_count=d1_gap_event_count,
        r2_shard_checks=tuple(r2_shard_checks),
        r2_shard_gaps=tuple(r2_shard_gaps),
        r2_shard_access_available=r2_shard_access_available(r2_shard_checks),
    )


# ── Database queries (I/O wrappers -- thin, no logic) ───────────────────────


def connect_neon() -> ConnectionLike:
    """Open a READ-ONLY, autocommit session against the racing Neon
    database. Resolves the DSN from NEON_PRIMARY_URL only -- this tool is
    Neon-only, no local-PG fallback (see module docstring). The DSN value is
    never printed/logged anywhere in this module; callers must render
    connection failures using only the exception TYPE
    (``type(exc).__name__``), never ``str(exc)``, since libpq
    connection-failure messages can echo back non-password DSN components
    (host/port/dbname/user) that this tool avoids surfacing as an extra
    precaution.
    """
    conn = psycopg.connect(os.environ[NEON_ENV_VAR])
    conn.read_only = True
    conn.autocommit = True
    return cast(ConnectionLike, conn)


def query_confirmed_races(
    conn: ConnectionLike, date_str: str, category: str,
) -> list[ConfirmedRaceRow]:
    """Fetch jvd_ra/nvd_ra rows for ``date_str``, filtered to confirmed
    (settled) races via the placeholder-safe predicate
    ``trim(shusso_tosu) NOT IN ('', '00')`` -- ``IS NOT NULL``/``!= ''``
    would silently pass every unconfirmed row as a false positive (see
    module docstring / reference_jvd_placeholder_semantics). Table choice
    mirrors ``serve_accuracy_report.py``'s own category scoping: JRA reads
    jvd_ra, every other category (nar, ban-ei) reads nvd_ra -- ban-ei shares
    nvd_ra with nar (keyed by keibajo_code='83') and is not separately
    scoped here, matching that script's own category handling exactly,
    since this tool's checks/thresholds are documented as JRA-calibrated.
    """
    kaisai_nen = date_str[:4]
    kaisai_tsukihi = date_str[4:]
    ra_table = "jvd_ra" if category == "jra" else "nvd_ra"
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, hasso_jikoku,
               track_code, CAST(kyori AS int) AS kyori, CAST(shusso_tosu AS int) AS shusso_tosu,
               grade_code, kyoso_joken_code
        FROM {ra_table}
        WHERE kaisai_nen = %s AND kaisai_tsukihi = %s
          AND trim(shusso_tosu) NOT IN ('', '00')
        ORDER BY keibajo_code, race_bango
        """,
        (kaisai_nen, kaisai_tsukihi),
    )
    return cast(list[ConfirmedRaceRow], cur.fetchall())


def _coerce_prediction_row(row: tuple[object, ...]) -> PredictionRow:
    """Coerce one raw fetched row into a genuine ``PredictionRow``.

    psycopg returns a Postgres ``NUMERIC`` column (``predicted_score``) as
    ``decimal.Decimal``, not ``float``. The row type alias's declared
    ``float | None`` was previously enforced only by ``cast()`` -- a
    static-only type assertion, a no-op at runtime -- so every downstream
    consumer actually received ``Decimal`` values. ``statistics.pstdev``
    preserves that type (its result is ``Decimal`` when its inputs are), so
    ``QualityGroupResult.stddev`` ended up ``Decimal`` too, and
    ``json.dumps`` cannot serialize ``Decimal`` -- ``--json`` mode crashed
    the instant any quality-degraded row actually carried a real
    predicted_score, i.e. exactly when this tool is most needed. Converting
    once, here, at the single fetch boundary is the root-cause fix: every
    function downstream of :func:`query_predictions` already declares (and
    now genuinely receives) ``float``.
    """
    keibajo_code, race_bango, model_version, predicted_score = row
    coerced_score = (
        None if predicted_score is None else float(cast("SupportsFloat | str", predicted_score))
    )
    return (
        cast(str, keibajo_code),
        cast(str, race_bango),
        cast(str, model_version),
        coerced_score,
    )


def query_predictions(conn: ConnectionLike, date_str: str, category: str) -> list[PredictionRow]:
    """Fetch raw (unaggregated) prediction rows for checks 1, 2, and 3.
    Every candidate row is returned -- including multiple model_versions per
    race -- so the pure grouping functions (distinct_race_keys,
    group_quality_rows, model_versions_by_race_key) can each apply their own
    granularity."""
    kaisai_nen = date_str[:4]
    kaisai_tsukihi = date_str[4:]
    cur = conn.cursor()
    cur.execute(
        """
        SELECT keibajo_code, race_bango, model_version, predicted_score
        FROM race_finish_position_model_predictions
        WHERE source = %s AND kaisai_nen = %s AND kaisai_tsukihi = %s
        ORDER BY keibajo_code, race_bango, model_version
        """,
        (category, kaisai_nen, kaisai_tsukihi),
    )
    return [_coerce_prediction_row(row) for row in cur.fetchall()]


def query_burst_buckets(conn: ConnectionLike, date_str: str, category: str) -> list[BurstRow]:
    """Group prediction writes into per-minute buckets in SQL
    (``date_trunc('minute', prediction_generated_at)``), matching the audit
    doc's own burst-detection method exactly. ``COUNT(DISTINCT
    (keibajo_code, race_bango))`` -- not ``COUNT(*)`` -- so a single race's
    many horse-rows don't inflate the count; a genuine burst is measured in
    races written, not raw rows.
    """
    kaisai_nen = date_str[:4]
    kaisai_tsukihi = date_str[4:]
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date_trunc('minute', prediction_generated_at) AS minute_bucket,
               COUNT(DISTINCT (keibajo_code, race_bango)) AS race_count
        FROM race_finish_position_model_predictions
        WHERE source = %s AND kaisai_nen = %s AND kaisai_tsukihi = %s
        GROUP BY minute_bucket
        ORDER BY minute_bucket
        """,
        (category, kaisai_nen, kaisai_tsukihi),
    )
    return cast(list[BurstRow], cur.fetchall())


def load_cell_routing_json(path: Path | None = None) -> Mapping[str, object]:
    """Load and parse cell_routing.json (defaults to the real repo path).
    Raises on a missing/malformed file -- callers treat this as a tool
    failure (exit 2), same as a Neon connection error, since check 3 cannot
    run at all without it (unlike D1/check 5, this is a checked-in
    repo-internal file with no live network/auth dependency, so its absence
    signals a broken checkout/working directory, not a transient external
    failure worth a soft N/A fallback)."""
    resolved_path = path if path is not None else CELL_ROUTING_CONFIG_PATH
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    return cast(Mapping[str, object], payload)


def query_d1_gap_event_count(date_str: str, category: str) -> int | None:
    """Best-effort D1 self-heal-activity count (check 5) via a
    ``wrangler d1 execute --remote`` subprocess (SELECT-only). Returns None
    -- never raises -- for ANY failure mode (wrangler missing,
    unauthenticated, network error, timeout, unexpected output shape):
    callers render this as the pre-approved "N/A (wrangler unavailable)"
    fallback rather than crashing the whole tool. This check is purely
    informational and never affects the exit code (see
    determine_exit_code)."""
    sql = build_d1_gap_events_query(date_str, category)
    try:
        completed = subprocess.run(
            [
                "bunx", "wrangler", "d1", "execute", D1_DATABASE_NAME, "--remote",
                "--command", sql, "--json",
            ],
            cwd=FINISH_POSITION_CRON_DIR,
            capture_output=True,
            text=True,
            timeout=D1_QUERY_TIMEOUT_SECONDS,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return parse_d1_gap_events_count(completed.stdout)


def _build_r2_client() -> ListObjectsClient:
    """Construct a boto3 S3-compatible client against R2's endpoint,
    reusing the exact env var names add-pacestyle-features.py's
    ``setup_r2_duckdb_secret`` reads (R2_ACCOUNT_ID / R2_ACCESS_KEY_ID /
    R2_SECRET_ACCESS_KEY). Raises KeyError if any is unset, matching
    ``connect_neon``'s own env-lookup contract so callers can catch it the
    same way."""
    account_id = os.environ[R2_ACCOUNT_ID_ENV_VAR]
    access_key = os.environ[R2_ACCESS_KEY_ID_ENV_VAR]
    secret_key = os.environ[R2_SECRET_ACCESS_KEY_ENV_VAR]
    return cast(
        ListObjectsClient,
        boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        ),
    )


def _prefix_has_object(client: ListObjectsClient, bucket: str, prefix: str) -> bool:
    """True if at least one object exists under ``prefix`` in ``bucket``."""
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(response.get("Contents"))


def query_r2_shard_presence(
    day_strs: Sequence[str], categories: Sequence[str], bucket: str,
) -> list[R2ShardCheck]:
    """Best-effort per-(day, category) R2 shard-presence check (check 6).

    Returns ``shard_found=None`` for every (day, category) -- without
    attempting any network calls -- when an R2 client cannot even be
    constructed (a missing credential env var): one fast fail instead of
    ``len(day_strs) * len(categories)`` redundant failures. Once a client
    exists, each (day, category) is checked independently; a per-call
    botocore/network error degrades only that one entry to None rather than
    aborting the remaining checks or crashing the tool -- never raises,
    mirroring ``query_d1_gap_event_count``'s "never crash the tool, never a
    false positive" contract.
    """
    try:
        client = _build_r2_client()
    except KeyError:
        return [
            R2ShardCheck(day=day, category=category, shard_found=None)
            for day in day_strs
            for category in categories
        ]
    checks: list[R2ShardCheck] = []
    for day in day_strs:
        for category in categories:
            try:
                found = _prefix_has_object(client, bucket, r2_shard_prefix(day, category))
            except (BotoCoreError, ClientError):
                found = None
            checks.append(R2ShardCheck(day=day, category=category, shard_found=found))
    return checks


# ── Output formatting (human-readable) ──────────────────────────────────────


def _format_coverage_section(report: HealthCheckReport) -> list[str]:
    if not report.coverage_gaps:
        return ["[1] Coverage: OK (0 gaps)"]
    lines = [f"[1] Coverage: {len(report.coverage_gaps)} gap(s) found"]
    lines += [f"      - {format_race_id(report.category, key)}" for key in report.coverage_gaps]
    return lines


def _format_quality_section(report: HealthCheckReport) -> list[str]:
    degraded = report.degraded_quality_groups
    if not degraded:
        lines = ["[2] Quality (Cluster-B signature): OK (0 degraded groups)"]
    else:
        lines = [
            f"[2] Quality (Cluster-B signature): {len(degraded)} degraded (race, model_version) "
            f"group(s) found (stddev < {QUALITY_DEGRADED_STDDEV_THRESHOLD})",
        ]
        lines += [
            f"      - {format_race_id(report.category, (g.kaisai_nen, g.kaisai_tsukihi, g.keibajo_code, g.race_bango))} "
            f"model_version={g.model_version} stddev={g.stddev:.3f} n={g.sample_count}"
            for g in degraded
        ]
    healthy = sum(1 for r in report.quality_rollup if r.status == RACE_STATUS_FULLY_HEALTHY)
    partial = sum(1 for r in report.quality_rollup if r.status == RACE_STATUS_PARTIALLY_DEGRADED)
    fully_degraded = sum(1 for r in report.quality_rollup if r.status == RACE_STATUS_FULLY_DEGRADED)
    lines.append(
        f"      Per-race rollup: fully_healthy={healthy} partially_degraded={partial} "
        f"fully_degraded={fully_degraded}",
    )
    return lines


def _format_routing_section(report: HealthCheckReport) -> list[str]:
    if not report.routing_supported:
        return [f"[3] Routing parity: N/A (no cell-routing rules for category={report.category})"]
    if not report.routing_mismatches:
        return ["[3] Routing parity: OK (0 mismatches)"]
    lines = [f"[3] Routing parity: {len(report.routing_mismatches)} mismatch(es) found"]
    lines += [
        f"      - {format_race_id(report.category, (m.kaisai_nen, m.kaisai_tsukihi, m.keibajo_code, m.race_bango))} "
        f"expected={m.expected_model_version}"
        for m in report.routing_mismatches
    ]
    return lines


def _format_burst_section(report: HealthCheckReport) -> list[str]:
    if not report.burst_buckets:
        return [f"[4] Burst detection: OK (0 minute-buckets > {BURST_RACE_COUNT_THRESHOLD} races)"]
    lines = [f"[4] Burst detection: {len(report.burst_buckets)} minute-bucket(s) flagged"]
    lines += [
        f"      - {b.minute_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC "
        f"({b.minute_jst.strftime('%Y-%m-%d %H:%M:%S')} JST): {b.race_count} races"
        for b in report.burst_buckets
    ]
    return lines


def _format_d1_section(report: HealthCheckReport) -> list[str]:
    if report.d1_gap_event_count is None:
        return [f"[5] D1 self-heal activity (informational): {D1_UNAVAILABLE_LABEL}"]
    return [f"[5] D1 self-heal activity (informational): {report.d1_gap_event_count} event(s)"]


def _r2_shard_status_label(shard_found: bool | None) -> str:
    if shard_found is None:
        return "N/A"
    return "found" if shard_found else "not found yet"


def _format_r2_shard_section(report: HealthCheckReport) -> list[str]:
    header = (
        f"[6] R2 shard presence (trailing {R2_SHARD_LOOKBACK_DAYS}d, "
        f"{'/'.join(R2_SHARD_CATEGORIES)})"
    )
    if not report.r2_shard_access_available:
        return [f"{header}: {R2_UNAVAILABLE_LABEL}"]
    if not report.r2_shard_gaps:
        lines = [f"{header}: OK (0 gaps)"]
    else:
        lines = [f"{header}: {len(report.r2_shard_gaps)} gap(s) found"]
        lines += [
            f"      - {g.day} category={g.category}: shard not found"
            for g in report.r2_shard_gaps
        ]
    requested_day_checks = [c for c in report.r2_shard_checks if c.day == report.date_str]
    if requested_day_checks:
        statuses = ", ".join(
            f"{c.category}={_r2_shard_status_label(c.shard_found)}"
            for c in sorted(requested_day_checks, key=lambda c: c.category)
        )
        lines.append(f"      Requested date ({report.date_str}): {statuses}")
    return lines


def format_report(report: HealthCheckReport) -> str:
    """Format a full report as a human-readable string."""
    lines = [
        f"=== Serve Health Check: {report.date_str} ({report.category.upper()}) ===",
        f"  Confirmed races checked: {report.checked_race_count}",
        "",
    ]
    lines += _format_coverage_section(report)
    lines += _format_quality_section(report)
    lines += _format_routing_section(report)
    lines += _format_burst_section(report)
    lines += _format_d1_section(report)
    lines += _format_r2_shard_section(report)
    verdict = "anomalies found" if report.exit_code == EXIT_ANOMALIES_FOUND else "OK"
    lines += ["", f"Exit code: {report.exit_code} ({verdict})"]
    return "\n".join(lines)


# ── Output formatting (JSON) ─────────────────────────────────────────────────


class QualityGroupDict(TypedDict):
    kaisai_nen: str
    kaisai_tsukihi: str
    keibajo_code: str
    race_bango: str
    model_version: str
    sample_count: int
    stddev: float


class RaceQualityRollupDict(TypedDict):
    kaisai_nen: str
    kaisai_tsukihi: str
    keibajo_code: str
    race_bango: str
    status: str


class RoutingMismatchDict(TypedDict):
    kaisai_nen: str
    kaisai_tsukihi: str
    keibajo_code: str
    race_bango: str
    expected_model_version: str


class BurstBucketDict(TypedDict):
    minute_utc: str
    minute_jst: str
    race_count: int


class R2ShardCheckDict(TypedDict):
    day: str
    category: str
    shard_found: bool | None


class HealthCheckReportDict(TypedDict):
    date_str: str
    category: str
    checked_race_count: int
    coverage_gap_count: int
    coverage_gaps: list[str]
    quality_degraded_count: int
    quality_degraded_groups: list[QualityGroupDict]
    quality_rollup: list[RaceQualityRollupDict]
    routing_supported: bool
    routing_mismatch_count: int
    routing_mismatches: list[RoutingMismatchDict]
    burst_bucket_count: int
    burst_buckets: list[BurstBucketDict]
    d1_gap_event_count: int | None
    r2_shard_access_available: bool
    r2_shard_gap_count: int
    r2_shard_gaps: list[R2ShardCheckDict]
    r2_shard_checks: list[R2ShardCheckDict]
    exit_code: int


def report_to_dict(report: HealthCheckReport) -> HealthCheckReportDict:
    """Convert a HealthCheckReport to a JSON-serializable typed dict,
    mirroring serve_accuracy_report.py's ``metrics_to_dict`` --json
    convention."""
    return {
        "date_str": report.date_str,
        "category": report.category,
        "checked_race_count": report.checked_race_count,
        "coverage_gap_count": len(report.coverage_gaps),
        "coverage_gaps": [format_race_id(report.category, key) for key in report.coverage_gaps],
        "quality_degraded_count": len(report.degraded_quality_groups),
        "quality_degraded_groups": [
            {
                "kaisai_nen": g.kaisai_nen,
                "kaisai_tsukihi": g.kaisai_tsukihi,
                "keibajo_code": g.keibajo_code,
                "race_bango": g.race_bango,
                "model_version": g.model_version,
                "sample_count": g.sample_count,
                "stddev": g.stddev,
            }
            for g in report.degraded_quality_groups
        ],
        "quality_rollup": [
            {
                "kaisai_nen": r.kaisai_nen,
                "kaisai_tsukihi": r.kaisai_tsukihi,
                "keibajo_code": r.keibajo_code,
                "race_bango": r.race_bango,
                "status": r.status,
            }
            for r in report.quality_rollup
        ],
        "routing_supported": report.routing_supported,
        "routing_mismatch_count": len(report.routing_mismatches),
        "routing_mismatches": [
            {
                "kaisai_nen": m.kaisai_nen,
                "kaisai_tsukihi": m.kaisai_tsukihi,
                "keibajo_code": m.keibajo_code,
                "race_bango": m.race_bango,
                "expected_model_version": m.expected_model_version,
            }
            for m in report.routing_mismatches
        ],
        "burst_bucket_count": len(report.burst_buckets),
        "burst_buckets": [
            {
                "minute_utc": b.minute_utc.isoformat(),
                "minute_jst": b.minute_jst.isoformat(),
                "race_count": b.race_count,
            }
            for b in report.burst_buckets
        ],
        "d1_gap_event_count": report.d1_gap_event_count,
        "r2_shard_access_available": report.r2_shard_access_available,
        "r2_shard_gap_count": len(report.r2_shard_gaps),
        "r2_shard_gaps": [
            {"day": g.day, "category": g.category, "shard_found": g.shard_found}
            for g in report.r2_shard_gaps
        ],
        "r2_shard_checks": [
            {"day": c.day, "category": c.category, "shard_found": c.shard_found}
            for c in report.r2_shard_checks
        ],
        "exit_code": report.exit_code,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments. ``--date`` defaults to None here (resolved to
    today JST later, via resolve_date_arg) so this function stays a pure,
    deterministic parse with no ``datetime.now()`` call of its own."""
    parser = argparse.ArgumentParser(
        description="Production read-only serve-health diagnostic for finish-position predictions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Race date in YYYYMMDD format (e.g. 20260712); defaults to today JST",
    )
    parser.add_argument(
        "--category",
        default=DEFAULT_CATEGORY,
        help=(
            "Race category (default: jra). This tool's checks/thresholds are "
            "JRA-calibrated; other values are accepted but may not transfer."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output JSON instead of human-readable text",
    )
    return parser.parse_args(argv)


def validate_date_arg(date_str: str) -> None:
    """Raise ValueError if date_str is not YYYYMMDD."""
    if len(date_str) != 8 or not date_str.isdigit():
        raise ValueError(f"date must be YYYYMMDD, got: {date_str!r}")
    try:
        date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    except ValueError as exc:
        raise ValueError(f"Invalid date: {date_str!r}") from exc


def resolve_date_arg(date_arg: str | None, now: datetime) -> str:
    """Resolve ``--date`` to an 8-digit YYYYMMDD string, defaulting to
    ``now`` (today JST when called with ``datetime.now(JST)``) when omitted.
    """
    if date_arg is not None:
        return date_arg
    return now.strftime("%Y%m%d")


def _fail(message: str, json_output: bool) -> int:
    if json_output:
        print(json.dumps({"error": "tool_failure", "message": message}))
    else:
        print(f"ERROR: {message}", file=sys.stderr)
    return EXIT_TOOL_FAILURE


def run(
    date_str: str,
    category: str,
    json_output: bool = False,
    now: datetime | None = None,
) -> int:
    """Run all 6 checks and print the report. Returns the process exit code
    (0/1/2 -- see module docstring). ``now`` defaults to ``datetime.now(JST)``
    when omitted, and is otherwise injectable for deterministic testing of
    check 1's post-time filter (and check 6's today-JST gap-exemption
    boundary).
    """
    validate_date_arg(date_str)
    effective_now = now if now is not None else datetime.now(JST)

    try:
        conn = connect_neon()
    except KeyError as exc:
        return _fail(f"environment variable {exc} is not set", json_output)
    except (psycopg.Error, OSError) as exc:
        return _fail(f"Neon connection failed ({type(exc).__name__})", json_output)

    try:
        confirmed_races = query_confirmed_races(conn, date_str, category)
        prediction_rows = query_predictions(conn, date_str, category)
        burst_rows = query_burst_buckets(conn, date_str, category)
    except psycopg.Error as exc:
        return _fail(f"Neon query failed: {exc}", json_output)
    finally:
        conn.close()

    try:
        routing_payload = load_cell_routing_json()
    except (OSError, json.JSONDecodeError) as exc:
        return _fail(f"failed to load cell_routing.json: {exc}", json_output)

    d1_gap_event_count = query_d1_gap_event_count(date_str, category)

    r2_bucket = os.environ.get(R2_BUCKET_ENV_VAR, R2_BUCKET_DEFAULT)
    r2_shard_day_strs = trailing_day_strs(date_str, R2_SHARD_LOOKBACK_DAYS)
    r2_shard_checks = query_r2_shard_presence(r2_shard_day_strs, R2_SHARD_CATEGORIES, r2_bucket)

    report = build_health_check_report(
        date_str, category, confirmed_races, prediction_rows, burst_rows,
        routing_payload, d1_gap_event_count, r2_shard_checks, effective_now,
    )

    if json_output:
        print(json.dumps(report_to_dict(report), indent=2))
    else:
        print(format_report(report))

    return report.exit_code


def main() -> None:
    args = parse_args()
    date_str = resolve_date_arg(args.date, datetime.now(JST))
    try:
        exit_code = run(date_str=date_str, category=args.category, json_output=args.json_output)
    except Exception:
        # Outer safety net: ANY unhandled exception (including a malformed
        # --date's ValueError from validate_date_arg) must still map to exit
        # code 2, never Python's default uncaught-exception exit code 1 --
        # exit 1 is reserved exclusively for "checks 1-4 found a real
        # anomaly" (see determine_exit_code). The traceback is still printed
        # so nothing is silently swallowed.
        traceback.print_exc()
        exit_code = EXIT_TOOL_FAILURE
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
