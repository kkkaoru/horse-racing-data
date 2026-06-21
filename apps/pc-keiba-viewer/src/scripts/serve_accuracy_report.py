"""Serve-accuracy validation harness for JRA/NAR finish-position and running-style predictions.

Joins production-served predictions (race_finish_position_model_predictions /
race_running_style_model_predictions) against actual race results (jvd_se / nvd_se)
for a given date, and reports:

  * Finish-position serve accuracy: top1 / place2 / place3 / fukusho_2p / top3_box
  * Running-style serve accuracy (where predictions exist): per-class P/R/F1 + label share
  * Era label: DEGRADED (pre-09:30-fix) vs POST_FIX (>=2026-06-11, 09:30 cron live)

Run with:
    uv run python src/scripts/serve_accuracy_report.py --date 20260614 --category jra
    uv run python src/scripts/serve_accuracy_report.py --date 20260606 --category jra
    uv run python src/scripts/serve_accuracy_report.py --date 20260606 --category jra --json

Population-scale reference baselines (from serve-condition-baseline-population.md, n=11703 JRA):
    DEGRADED (OOD median odds): top1=31.78%  place2=15.25%  place3=9.19%  fukusho_2p=57.76%
    FULL (real odds):           top1=44.71%  place2=24.51%  place3=15.48%  fukusho_2p=74.79%

WF holdout reference (I4 simulation on 687k rows):
    JRA WF real odds:           top1=47.43%  place2=85.98%  place3=97.18%
    JRA WF median fallback:     top1=38.78%  place2=76.35%  place3=92.46%

Era definition:
    DEGRADED: predictions generated before 2026-06-11 JST (no 09:30 cron, OOD-median odds)
    POST_FIX: predictions generated on or after 2026-06-11 JST (09:30 cron, realtime odds)
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Final, NotRequired, Protocol, TypedDict, cast

import psycopg

# ── Constants ─────────────────────────────────────────────────────────────────

# 09:30 fix went live on 2026-06-11 JST = 2026-06-11 00:30:00 UTC (commit fe871a6)
ERA_POSTFIX_CUTOFF_JST: Final[datetime] = datetime(2026, 6, 11, 0, 30, 0,
                                                    tzinfo=timezone.utc)

# Running-style class thresholds (must match running-style-feature-sql.ts)
RS_SENKOU_THRESHOLD: Final[float] = 0.3
RS_SASHI_THRESHOLD: Final[float] = 0.7
RS_CLASS_NIGE: Final[int] = 0
RS_CLASS_SENKOU: Final[int] = 1
RS_CLASS_SASHI: Final[int] = 2
RS_CLASS_OIKOMI: Final[int] = 3
RS_CLASS_LABELS: Final[tuple[str, str, str, str]] = ("nige", "senkou", "sashi", "oikomi")

# Population-scale baselines (serve-condition-baseline-population.md)
JRA_BASELINE_DEGRADED: Final[dict[str, float]] = {
    "top1": 31.778, "place2": 15.252, "place3": 9.194, "fukusho_2p": 57.763,
}
JRA_BASELINE_FULL: Final[dict[str, float]] = {
    "top1": 44.706, "place2": 24.506, "place3": 15.475, "fukusho_2p": 74.793,
}

VALID_CATEGORIES: Final[tuple[str, str]] = ("jra", "nar")

DEFAULT_PG_URL: Final[str] = (
    "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"
)

# ── Data classes ──────────────────────────────────────────────────────────────


@dataclass
class SubgroupAccuracy:
    """Finish-position serve accuracy for one subgroup band of one dimension."""

    dimension: str  # "distance_band" | "field_size_band" | "season_band" | "venue"
    band: str
    races: int
    top1_hits: int
    place2_hits: int
    place3_hits: int
    fukusho_2p_hits: int
    top3_box_hits: int

    @property
    def top1_pct(self) -> float:
        return 100.0 * self.top1_hits / self.races if self.races > 0 else 0.0

    @property
    def place2_pct(self) -> float:
        return 100.0 * self.place2_hits / self.races if self.races > 0 else 0.0

    @property
    def place3_pct(self) -> float:
        return 100.0 * self.place3_hits / self.races if self.races > 0 else 0.0

    @property
    def fukusho_2p_pct(self) -> float:
        return 100.0 * self.fukusho_2p_hits / self.races if self.races > 0 else 0.0

    @property
    def top3_box_pct(self) -> float:
        return 100.0 * self.top3_box_hits / self.races if self.races > 0 else 0.0


@dataclass
class FinishPositionMetrics:
    """Per-date finish-position serve accuracy metrics."""

    date_str: str
    category: str
    era: str  # "DEGRADED" | "POST_FIX" | "UNKNOWN"
    races: int
    horses: int
    top1_hits: int
    place2_hits: int
    place3_hits: int
    fukusho_2p_hits: int
    top3_box_hits: int
    prediction_generated_at_jst: str
    model_version_counts: dict[str, int] = field(default_factory=dict)
    subgroups: list[SubgroupAccuracy] = field(default_factory=list)

    @property
    def top1_pct(self) -> float:
        return 100.0 * self.top1_hits / self.races if self.races > 0 else 0.0

    @property
    def place2_pct(self) -> float:
        return 100.0 * self.place2_hits / self.races if self.races > 0 else 0.0

    @property
    def place3_pct(self) -> float:
        return 100.0 * self.place3_hits / self.races if self.races > 0 else 0.0

    @property
    def fukusho_2p_pct(self) -> float:
        return 100.0 * self.fukusho_2p_hits / self.races if self.races > 0 else 0.0

    @property
    def top3_box_pct(self) -> float:
        return 100.0 * self.top3_box_hits / self.races if self.races > 0 else 0.0


@dataclass
class RunningStyleClassMetrics:
    """Per-class running-style metrics."""

    label: str
    cls_idx: int
    pred_count: int
    actual_count: int
    tp: int

    @property
    def precision(self) -> float | None:
        if self.pred_count == 0:
            return None
        return self.tp / self.pred_count

    @property
    def recall(self) -> float | None:
        if self.actual_count == 0:
            return None
        return self.tp / self.actual_count

    @property
    def f1(self) -> float | None:
        p = self.precision
        r = self.recall
        if p is None or r is None or (p + r) == 0:
            return None
        return 2 * p * r / (p + r)


@dataclass
class RunningStyleMetrics:
    """Running-style serve accuracy for a date."""

    date_str: str
    category: str
    era: str
    total_horses: int
    overall_accuracy: float
    per_class: list[RunningStyleClassMetrics] = field(default_factory=list)
    macro_f1: float | None = None
    model_version: str = ""
    prediction_generated_at_jst: str = ""

    @property
    def pred_label_share(self) -> dict[str, float]:
        total = sum(c.pred_count for c in self.per_class)
        if total == 0:
            return {}
        return {c.label: c.pred_count / total for c in self.per_class}

    @property
    def actual_label_share(self) -> dict[str, float]:
        total = sum(c.actual_count for c in self.per_class)
        if total == 0:
            return {}
        return {c.label: c.actual_count / total for c in self.per_class}


# ── Row type aliases ─────────────────────────────────────────────────────────

# (keibajo_code, race_bango, predicted_rank, actual_rank, model_version, gen_at,
#  kyori, shusso_tosu)
FpRow = tuple[str, str, int, int, str, datetime | None, int, int]

# (keibajo, race_bango, ketto, predicted_label, predicted_class,
#  p_nige, p_senkou, p_sashi, p_oikomi, model_version, gen_at, corner_1, shusso_tosu)
RsRow = tuple[str, str, str, str, int, float, float, float, float, str, datetime | None, str, int]

# ── I/O Protocol ─────────────────────────────────────────────────────────────


class CursorLike(Protocol):
    def execute(self, query: str, params: object = None) -> object: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...


class ConnectionLike(Protocol):
    def cursor(self) -> CursorLike: ...

    def close(self) -> None: ...


# ── Core computation (pure helpers — fully testable) ──────────────────────────


def infer_era(gen_at_utc: datetime | None) -> str:
    """Return 'POST_FIX', 'DEGRADED', or 'UNKNOWN' based on generation timestamp."""
    if gen_at_utc is None:
        return "UNKNOWN"
    if gen_at_utc >= ERA_POSTFIX_CUTOFF_JST:
        return "POST_FIX"
    return "DEGRADED"


def compute_corner1_norm(corner1_raw: str, shusso_tosu: int) -> float | None:
    """Compute corner1_norm from raw JVD string and field size.

    Mirrors the formula in build-corner-feature-table.ts:
      when nullif(corner_1, '00') is not null and shusso_tosu > 1
        then (corner_1 - 1) / (shusso_tosu - 1)
      else null
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
    """Assign running-style class from corner1_norm.

    Returns None for straight tracks (corner1_norm is None = no corner 1 data).
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


def classify_distance_band(kyori: int) -> str:
    """Map race distance (meters) to a band label.

    Mirrors the CASE semantics used across the finish-position feature SQL:
      <=1400 sprint / <=1800 mile / <=2200 intermediate / <=2800 long / else extended.
    """
    if kyori <= 1400:
        return "sprint"
    if kyori <= 1800:
        return "mile"
    if kyori <= 2200:
        return "intermediate"
    if kyori <= 2800:
        return "long"
    return "extended"


def classify_field_size_band(shusso_tosu: int) -> str:
    """Map field size to a band label: <=8 small / <=14 medium / else large."""
    if shusso_tosu <= 8:
        return "small"
    if shusso_tosu <= 14:
        return "medium"
    return "large"


def classify_season_band(kaisai_tsukihi: str) -> str:
    """Map a race date's month (first 2 chars of MMDD) to a season label.

    Callers pass a validated 4-char MMDD string sourced from the DB, so the
    month is always present.
    """
    month = int(kaisai_tsukihi[:2])
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    if month in (9, 10, 11):
        return "autumn"
    return "winter"


def aggregate_fp_metrics(
    race_rows: list[list[tuple[int, int]]],
) -> tuple[int, int, int, int, int]:
    """Aggregate hit counts across races.

    race_rows: list of per-race lists of (pred_rank, actual_rank).
    Returns (top1_hits, place2_hits, place3_hits, fukusho_2p_hits, top3_box_hits).
    fukusho_2p: whether *any* of the predicted top-2 horses finished <=2.
    top3_box: whether predicted ranks 1, 2, and 3 all finished in the top 3.
    """
    top1 = place2 = place3 = fukusho_2p = top3_box = 0
    for race in race_rows:
        pred1_actual: int | None = None
        any_top2_in_top2 = False
        top3_in_actual_top3 = 0
        for pred_rank, actual_rank in race:
            if pred_rank == 1:
                pred1_actual = actual_rank
            if pred_rank <= 2 and actual_rank <= 2:
                any_top2_in_top2 = True
            if pred_rank <= 3 and actual_rank <= 3:
                top3_in_actual_top3 += 1
        if pred1_actual is not None:
            if pred1_actual == 1:
                top1 += 1
            if pred1_actual <= 2:
                place2 += 1
            if pred1_actual <= 3:
                place3 += 1
        if any_top2_in_top2:
            fukusho_2p += 1
        if top3_in_actual_top3 == 3:
            top3_box += 1
    return top1, place2, place3, fukusho_2p, top3_box


def compute_subgroup_accuracies(
    race_partitions: list[tuple[str, str, str, str, list[tuple[int, int]]]],
) -> list[SubgroupAccuracy]:
    """Compute per-subgroup finish-position accuracy across four dimensions.

    Each entry in ``race_partitions`` is one race:
        (distance_band, field_size_band, season_band, venue, race_pairs)
    where ``race_pairs`` is that race's list of (pred_rank, actual_rank).

    Returns a flat list of SubgroupAccuracy ordered by dimension then band.
    Pure: reuses ``aggregate_fp_metrics`` per partition and performs no I/O.
    """
    dimensions: tuple[str, str, str, str] = (
        "distance_band",
        "field_size_band",
        "season_band",
        "venue",
    )
    result: list[SubgroupAccuracy] = []
    for dimension in dimensions:
        buckets: dict[str, list[list[tuple[int, int]]]] = {}
        for distance, field_size, season, venue, race_pairs in race_partitions:
            band_by_dim: dict[str, str] = {
                "distance_band": distance,
                "field_size_band": field_size,
                "season_band": season,
                "venue": venue,
            }
            band = band_by_dim[dimension]
            if band not in buckets:
                buckets[band] = []
            buckets[band].append(race_pairs)
        for band in sorted(buckets):
            partition = buckets[band]
            top1, place2, place3, fukusho_2p, top3_box = aggregate_fp_metrics(partition)
            result.append(SubgroupAccuracy(
                dimension=dimension,
                band=band,
                races=len(partition),
                top1_hits=top1,
                place2_hits=place2,
                place3_hits=place3,
                fukusho_2p_hits=fukusho_2p,
                top3_box_hits=top3_box,
            ))
    return result


def compute_rs_per_class(
    pred_labels: list[int],
    actual_labels: list[int],
) -> list[RunningStyleClassMetrics]:
    """Compute per-class precision/recall/F1 for running-style predictions."""
    result: list[RunningStyleClassMetrics] = []
    for cls_idx, label in enumerate(RS_CLASS_LABELS):
        pred_mask = [p == cls_idx for p in pred_labels]
        actual_mask = [a == cls_idx for a in actual_labels]
        tp = sum(1 for p, a in zip(pred_mask, actual_mask) if p and a)
        result.append(RunningStyleClassMetrics(
            label=label,
            cls_idx=cls_idx,
            pred_count=sum(pred_mask),
            actual_count=sum(actual_mask),
            tp=tp,
        ))
    return result


def compute_macro_f1(per_class: list[RunningStyleClassMetrics]) -> float | None:
    """Compute macro-averaged F1 from per-class metrics."""
    f1_values = [c.f1 for c in per_class if c.f1 is not None]
    if not f1_values:
        return None
    return sum(f1_values) / len(f1_values)


# ── Database queries ──────────────────────────────────────────────────────────


def query_finish_position_metrics(
    conn: ConnectionLike,
    date_str: str,
    category: str,
) -> FinishPositionMetrics | None:
    """Query served predictions and results for a date, return metrics.

    Uses DISTINCT ON (keibajo_code, race_bango, ketto_toroku_bango) to pick the
    latest-generated prediction per horse per race (handles multiple model versions / re-runs).
    """
    kaisai_nen = date_str[:4]
    kaisai_tsukihi = date_str[4:]

    # JRA uses jvd_se / jvd_ra; NAR uses nvd_se / nvd_ra
    result_table = "jvd_se" if category == "jra" else "nvd_se"
    result_ra_table = "jvd_ra" if category == "jra" else "nvd_ra"

    cur = conn.cursor()

    # Step 1: Get all served predictions per race (latest prediction per horse),
    # joined with per-race dims (kyori, shusso_tosu) from the race table.
    cur.execute(
        f"""
        WITH served AS (
            SELECT DISTINCT ON (keibajo_code, race_bango, ketto_toroku_bango)
                keibajo_code, race_bango, ketto_toroku_bango, predicted_rank,
                model_version, prediction_generated_at
            FROM race_finish_position_model_predictions
            WHERE source = %s AND kaisai_nen = %s AND kaisai_tsukihi = %s
            ORDER BY keibajo_code, race_bango, ketto_toroku_bango, prediction_generated_at DESC
        ),
        results AS (
            SELECT keibajo_code, race_bango, ketto_toroku_bango,
                CAST(kakutei_chakujun AS int) as actual_rank
            FROM {result_table}
            WHERE kaisai_nen = %s AND kaisai_tsukihi = %s
              AND kakutei_chakujun IS NOT NULL
              AND kakutei_chakujun ~ '^[0-9]+$'
              AND CAST(kakutei_chakujun AS int) > 0
        )
        SELECT
            s.keibajo_code, s.race_bango,
            s.predicted_rank, r.actual_rank,
            s.model_version, s.prediction_generated_at,
            CAST(ra.kyori AS int) as kyori,
            CAST(ra.shusso_tosu AS int) as shusso_tosu
        FROM served s
        JOIN results r ON
            s.keibajo_code = r.keibajo_code AND
            s.race_bango = r.race_bango AND
            s.ketto_toroku_bango = r.ketto_toroku_bango
        JOIN {result_ra_table} ra ON
            ra.kaisai_nen = %s AND ra.kaisai_tsukihi = %s AND
            ra.keibajo_code = s.keibajo_code AND
            ra.race_bango = s.race_bango
        ORDER BY s.keibajo_code, s.race_bango, s.predicted_rank
        """,
        (
            category, kaisai_nen, kaisai_tsukihi,
            kaisai_nen, kaisai_tsukihi,
            kaisai_nen, kaisai_tsukihi,
        ),
    )
    rows: list[FpRow] = cast(list[FpRow], cur.fetchall())

    if not rows:
        return None

    # Group by race
    races_dict: dict[tuple[str, str], list[tuple[int, int]]] = {}
    race_dims: dict[tuple[str, str], tuple[str, int, int]] = {}
    gen_ats: list[datetime] = []
    for keibajo, race_bango, pred_rank, actual_rank, _model_ver, gen_at, kyori, tosu in rows:
        key = (keibajo, race_bango)
        if key not in races_dict:
            races_dict[key] = []
            race_dims[key] = (keibajo, kyori, tosu)
        races_dict[key].append((pred_rank, actual_rank))
        if gen_at is not None:
            gen_ats.append(gen_at)

    race_rows = list(races_dict.values())
    top1, place2, place3, fukusho_2p, top3_box = aggregate_fp_metrics(race_rows)

    # Build pure per-race partitions for subgroup breakdown
    race_partitions: list[tuple[str, str, str, str, list[tuple[int, int]]]] = []
    for key, pairs in races_dict.items():
        venue, kyori, tosu = race_dims[key]
        race_partitions.append((
            classify_distance_band(kyori),
            classify_field_size_band(tosu),
            classify_season_band(kaisai_tsukihi),
            venue,
            pairs,
        ))
    subgroups = compute_subgroup_accuracies(race_partitions)

    # Determine era from latest gen_at (most recent prediction determines data availability)
    latest_gen = max(gen_ats) if gen_ats else None
    era = infer_era(latest_gen)

    # JST display
    gen_jst = ""
    if latest_gen:
        jst_dt = latest_gen.astimezone(timezone(timedelta(hours=9)))
        gen_jst = jst_dt.strftime("%Y-%m-%d %H:%M:%S JST")

    # Count model versions
    model_version_counts: dict[str, int] = {}
    for _, _, _, _, mv, _, _, _ in rows:
        model_version_counts[mv] = model_version_counts.get(mv, 0) + 1

    return FinishPositionMetrics(
        date_str=date_str,
        category=category,
        era=era,
        races=len(race_rows),
        horses=len(rows),
        top1_hits=top1,
        place2_hits=place2,
        place3_hits=place3,
        fukusho_2p_hits=fukusho_2p,
        top3_box_hits=top3_box,
        prediction_generated_at_jst=gen_jst,
        model_version_counts=model_version_counts,
        subgroups=subgroups,
    )


def query_running_style_metrics(
    conn: ConnectionLike,
    date_str: str,
    category: str,
) -> RunningStyleMetrics | None:
    """Query running-style serve predictions and derive actual labels from corner data."""
    kaisai_nen = date_str[:4]
    kaisai_tsukihi = date_str[4:]
    result_table = "jvd_se" if category == "jra" else "nvd_se"
    result_ra_table = "jvd_ra" if category == "jra" else "nvd_ra"

    cur = conn.cursor()

    # Get latest RS model predictions for the date joined with corner data
    cur.execute(
        f"""
        WITH latest_rs AS (
            SELECT DISTINCT ON (keibajo_code, race_bango, ketto_toroku_bango)
                keibajo_code, race_bango, ketto_toroku_bango, umaban,
                predicted_label, predicted_class,
                p_nige, p_senkou, p_sashi, p_oikomi,
                model_version, prediction_generated_at
            FROM race_running_style_model_predictions
            WHERE source = %s AND kaisai_nen = %s AND kaisai_tsukihi = %s
            ORDER BY keibajo_code, race_bango, ketto_toroku_bango,
                     prediction_generated_at DESC
        )
        SELECT
            rs.keibajo_code, rs.race_bango, rs.ketto_toroku_bango,
            rs.predicted_label, rs.predicted_class,
            rs.p_nige, rs.p_senkou, rs.p_sashi, rs.p_oikomi,
            rs.model_version, rs.prediction_generated_at,
            se.corner_1,
            ra.shusso_tosu
        FROM latest_rs rs
        JOIN {result_table} se ON
            se.kaisai_nen = %s AND se.kaisai_tsukihi = %s AND
            se.keibajo_code = rs.keibajo_code AND
            se.race_bango = rs.race_bango AND
            se.ketto_toroku_bango = rs.ketto_toroku_bango
            AND se.kakutei_chakujun IS NOT NULL
        JOIN {result_ra_table} ra ON
            ra.kaisai_nen = %s AND ra.kaisai_tsukihi = %s AND
            ra.keibajo_code = rs.keibajo_code AND
            ra.race_bango = rs.race_bango
        ORDER BY rs.keibajo_code, rs.race_bango, rs.ketto_toroku_bango
        """,
        (
            category, kaisai_nen, kaisai_tsukihi,
            kaisai_nen, kaisai_tsukihi,
            kaisai_nen, kaisai_tsukihi,
        ),
    )
    rs_rows: list[RsRow] = cast(list[RsRow], cur.fetchall())

    if not rs_rows:
        return None

    pred_labels: list[int] = []
    actual_labels: list[int] = []
    gen_ats: list[datetime] = []
    model_versions: list[str] = []

    for row in rs_rows:
        _, _, _, _, predicted_class, _, _, _, _, model_ver, gen_at, corner1_raw, tosu = row
        c1_norm = compute_corner1_norm(str(corner1_raw), int(tosu) if tosu else 0)
        actual_cls = classify_running_style(c1_norm)
        if actual_cls is None:
            # Straight track — skip (corner1 not meaningful)
            continue
        pred_labels.append(int(predicted_class))
        actual_labels.append(actual_cls)
        if gen_at is not None:
            gen_ats.append(gen_at)
        model_versions.append(str(model_ver))

    if not pred_labels:
        return None

    overall_acc = sum(1 for p, a in zip(pred_labels, actual_labels) if p == a) / len(pred_labels)
    per_class = compute_rs_per_class(pred_labels, actual_labels)
    macro_f1 = compute_macro_f1(per_class)

    latest_gen = max(gen_ats) if gen_ats else None
    era = infer_era(latest_gen)

    gen_jst = ""
    if latest_gen:
        jst_dt = latest_gen.astimezone(timezone(timedelta(hours=9)))
        gen_jst = jst_dt.strftime("%Y-%m-%d %H:%M:%S JST")

    model_version = model_versions[0] if model_versions else ""

    return RunningStyleMetrics(
        date_str=date_str,
        category=category,
        era=era,
        total_horses=len(pred_labels),
        overall_accuracy=overall_acc,
        per_class=per_class,
        macro_f1=macro_f1,
        model_version=model_version,
        prediction_generated_at_jst=gen_jst,
    )


# ── Output formatting ─────────────────────────────────────────────────────────


def format_subgroup_report(subgroups: list[SubgroupAccuracy]) -> str:
    """Format per-subgroup finish-position accuracy grouped by dimension then band."""
    lines = [
        "  Subgroup breakdown:",
        f"  {'Dim/Band':<28} {'Races':>5} {'top1':>7} {'plc2':>7} "
        f"{'plc3':>7} {'fk2p':>7} {'t3box':>7}",
        f"  {'-' * 72}",
    ]
    current_dim = ""
    for sg in subgroups:
        if sg.dimension != current_dim:
            current_dim = sg.dimension
            lines.append(f"  [{sg.dimension}]")
        lines.append(
            f"    {sg.band:<26} {sg.races:>5} "
            f"{sg.top1_pct:6.2f}% {sg.place2_pct:6.2f}% {sg.place3_pct:6.2f}% "
            f"{sg.fukusho_2p_pct:6.2f}% {sg.top3_box_pct:6.2f}%"
        )
    return "\n".join(lines)


def format_fp_report(m: FinishPositionMetrics) -> str:
    """Format finish-position metrics as a human-readable string."""
    lines = [
        f"=== Finish-Position Serve Accuracy: {m.date_str} ({m.category.upper()}) ===",
        f"  Era:           {m.era}",
        f"  Generated:     {m.prediction_generated_at_jst}",
        f"  Races:         {m.races}  |  Horses matched: {m.horses}",
        "",
        f"  top1:          {m.top1_pct:6.2f}%  ({m.top1_hits}/{m.races})",
        f"  place2:        {m.place2_pct:6.2f}%  ({m.place2_hits}/{m.races})",
        f"  place3:        {m.place3_pct:6.2f}%  ({m.place3_hits}/{m.races})",
        f"  fukusho_2p:    {m.fukusho_2p_pct:6.2f}%  ({m.fukusho_2p_hits}/{m.races})",
        f"  top3_box:      {m.top3_box_pct:6.2f}%  ({m.top3_box_hits}/{m.races})",
    ]
    if m.category == "jra":
        lines += [
            "",
            "  Baselines (population n=11703):",
            "    DEGRADED top1= 31.78%  place2= 15.25%  place3=  9.19%",
            "    FULL     top1= 44.71%  place2= 24.51%  place3= 15.48%",
        ]
    if m.model_version_counts:
        lines += ["", "  Models served:"]
        for mv, cnt in sorted(m.model_version_counts.items(), key=lambda x: -x[1]):
            lines.append(f"    {mv}: {cnt} horses")
    if m.subgroups:
        lines += [f"", format_subgroup_report(m.subgroups)]
    return "\n".join(lines)


def format_rs_report(m: RunningStyleMetrics) -> str:
    """Format running-style metrics as a human-readable string."""
    lines = [
        f"=== Running-Style Serve Accuracy: {m.date_str} ({m.category.upper()}) ===",
        f"  Era:           {m.era}",
        f"  Model:         {m.model_version}",
        f"  Generated:     {m.prediction_generated_at_jst}",
        f"  Horses (cornered tracks): {m.total_horses}",
        f"  Overall acc:   {m.overall_accuracy * 100:.2f}%",
        f"  Macro-F1:      {m.macro_f1 * 100:.2f}%" if m.macro_f1 is not None else "  Macro-F1:      N/A",
        "",
        f"  {'Class':<10} {'Pred%':>6} {'Act%':>6} {'Prec':>6} {'Rec':>6} {'F1':>6}",
        f"  {'-'*48}",
    ]
    pred_share = m.pred_label_share
    actual_share = m.actual_label_share
    for c in m.per_class:
        prec = f"{c.precision * 100:.1f}%" if c.precision is not None else "  N/A"
        rec = f"{c.recall * 100:.1f}%" if c.recall is not None else "  N/A"
        f1 = f"{c.f1 * 100:.1f}%" if c.f1 is not None else "  N/A"
        ps = f"{pred_share.get(c.label, 0) * 100:.1f}%"
        as_ = f"{actual_share.get(c.label, 0) * 100:.1f}%"
        lines.append(f"  {c.label:<10} {ps:>6} {as_:>6} {prec:>6} {rec:>6} {f1:>6}")
    return "\n".join(lines)


class SubgroupDict(TypedDict):
    """Serialized SubgroupAccuracy (JSON-friendly)."""

    dimension: str
    band: str
    races: int
    top1_pct: float
    place2_pct: float
    place3_pct: float
    fukusho_2p_pct: float
    top3_box_pct: float


class RsPerClassDict(TypedDict):
    """Serialized RunningStyleClassMetrics (JSON-friendly)."""

    label: str
    pred_count: int
    actual_count: int
    tp: int
    precision_pct: float | None
    recall_pct: float | None
    f1_pct: float | None


class FinishPositionDict(TypedDict):
    """Serialized FinishPositionMetrics (JSON-friendly)."""

    date_str: str
    category: str
    era: str
    races: int
    horses: int
    top1_pct: float
    place2_pct: float
    place3_pct: float
    fukusho_2p_pct: float
    top3_box_pct: float
    prediction_generated_at_jst: str
    model_version_counts: dict[str, int]
    subgroups: list[SubgroupDict]


class RunningStyleDict(TypedDict):
    """Serialized RunningStyleMetrics (JSON-friendly)."""

    date_str: str
    category: str
    era: str
    total_horses: int
    overall_accuracy_pct: float
    macro_f1_pct: float | None
    model_version: str
    prediction_generated_at_jst: str
    per_class: list[RsPerClassDict]


class MetricsDict(TypedDict):
    """Top-level serialized metrics; each section is present only when computed."""

    finish_position: NotRequired[FinishPositionDict]
    running_style: NotRequired[RunningStyleDict]


def metrics_to_dict(
    fp: FinishPositionMetrics | None,
    rs: RunningStyleMetrics | None,
) -> MetricsDict:
    """Convert metrics objects to a JSON-serializable typed dict."""
    result: MetricsDict = {}
    if fp is not None:
        result["finish_position"] = {
            "date_str": fp.date_str,
            "category": fp.category,
            "era": fp.era,
            "races": fp.races,
            "horses": fp.horses,
            "top1_pct": fp.top1_pct,
            "place2_pct": fp.place2_pct,
            "place3_pct": fp.place3_pct,
            "fukusho_2p_pct": fp.fukusho_2p_pct,
            "top3_box_pct": fp.top3_box_pct,
            "prediction_generated_at_jst": fp.prediction_generated_at_jst,
            "model_version_counts": fp.model_version_counts,
            "subgroups": [
                {
                    "dimension": sg.dimension,
                    "band": sg.band,
                    "races": sg.races,
                    "top1_pct": sg.top1_pct,
                    "place2_pct": sg.place2_pct,
                    "place3_pct": sg.place3_pct,
                    "fukusho_2p_pct": sg.fukusho_2p_pct,
                    "top3_box_pct": sg.top3_box_pct,
                }
                for sg in fp.subgroups
            ],
        }
    if rs is not None:
        result["running_style"] = {
            "date_str": rs.date_str,
            "category": rs.category,
            "era": rs.era,
            "total_horses": rs.total_horses,
            "overall_accuracy_pct": rs.overall_accuracy * 100,
            "macro_f1_pct": rs.macro_f1 * 100 if rs.macro_f1 is not None else None,
            "model_version": rs.model_version,
            "prediction_generated_at_jst": rs.prediction_generated_at_jst,
            "per_class": [
                {
                    "label": c.label,
                    "pred_count": c.pred_count,
                    "actual_count": c.actual_count,
                    "tp": c.tp,
                    "precision_pct": c.precision * 100 if c.precision is not None else None,
                    "recall_pct": c.recall * 100 if c.recall is not None else None,
                    "f1_pct": c.f1 * 100 if c.f1 is not None else None,
                }
                for c in rs.per_class
            ],
        }
    return result


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Serve-accuracy validation harness for JRA/NAR predictions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Race date in YYYYMMDD format (e.g. 20260614)",
    )
    parser.add_argument(
        "--category",
        choices=list(VALID_CATEGORIES),
        required=True,
        help="Race category: jra or nar",
    )
    parser.add_argument(
        "--pg-url",
        default=DEFAULT_PG_URL,
        help="PostgreSQL connection URL",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output JSON instead of human-readable text",
    )
    parser.add_argument(
        "--no-rs",
        action="store_true",
        help="Skip running-style accuracy (finish-position only)",
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


def run(
    date_str: str,
    category: str,
    pg_url: str,
    json_output: bool = False,
    no_rs: bool = False,
) -> int:
    """Run the harness. Returns exit code (0=success, 1=no data)."""
    validate_date_arg(date_str)

    conn: ConnectionLike = cast(ConnectionLike, psycopg.connect(pg_url))
    try:
        fp = query_finish_position_metrics(conn, date_str, category)
        rs = query_running_style_metrics(conn, date_str, category) if not no_rs else None
    finally:
        conn.close()

    if fp is None and rs is None:
        if json_output:
            print(json.dumps({"error": "no_data", "date": date_str, "category": category}))
        else:
            print(f"No served predictions found for {date_str} ({category}).")
            print("Check that the cron ran and upserted to race_finish_position_model_predictions.")
        return 1

    if json_output:
        print(json.dumps(metrics_to_dict(fp, rs), indent=2))
    else:
        if fp is not None:
            print(format_fp_report(fp))
        if rs is not None:
            print()
            print(format_rs_report(rs))
    return 0


def main() -> None:
    args = parse_args()
    sys.exit(run(
        date_str=args.date,
        category=args.category,
        pg_url=args.pg_url,
        json_output=args.json_output,
        no_rs=args.no_rs,
    ))


if __name__ == "__main__":
    main()
