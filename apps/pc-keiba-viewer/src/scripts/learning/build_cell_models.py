"""Build a cell-routing config from per-cell training evaluations.

Reads cell-level Walk-Forward evaluations written by ``continuous_learner`` into
the PostgreSQL table ``cell_training_evaluations`` and, for each cell, compares a
candidate feature set against the production baseline through a multi-metric
adoption gate (sample size, multi-metric improvement, no-regression, bootstrap
LB95, freshness, baseline-exists). Cells that adopt a candidate are grouped by
feature-set hash into routing *variants* and emitted as the N-variant
``cell_routing.json`` consumed by ``predict_lib.cell_router`` /
``predict_upcoming``.

The module is importable without side effects: nothing connects to PostgreSQL or
touches the filesystem at import time.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Final, LiteralString, SupportsFloat, SupportsInt, cast

import numpy as np

if TYPE_CHECKING:
    import psycopg

_logger = logging.getLogger(__name__)

VARIANT_SIM: Final[str] = "sim"

DEFAULT_MIN_RACES: Final[int] = 200
DEFAULT_FRESHNESS_DAYS: Final[int] = 14
# +0.08pp expressed as a fraction: a cell evaluation stores accuracies in [0, 1].
DEFAULT_MIN_DELTA: Final[float] = 0.0008
# -0.05pp: a metric is a regression once it drops by this much or more.
DEFAULT_NO_REG_THRESHOLD: Final[float] = -0.0005
DEFAULT_N_BOOT: Final[int] = 2000
_BOOTSTRAP_SEED: Final[int] = 12345

_PRIMARY_METRICS: Final[tuple[str, ...]] = ("top1", "place2", "place3")
_PLACE_PRIMARY_METRICS: Final[frozenset[str]] = frozenset({"place2", "place3"})
_NO_REGRESSION_METRICS: Final[tuple[str, ...]] = (
    "top1",
    "place2",
    "place3",
    "place4",
    "place5",
    "place6",
    "top3_box",
)

# CellKey field -> the router dimension name the field routes on. ``subgroup``
# carries the distance band and ``racetrack`` carries the venue code, both of
# which map onto dimensions ``cell_router`` resolves on the fly.
_CELL_DIMENSIONS: Final[tuple[tuple[str, str], ...]] = (
    ("class", "class_label"),
    ("distance_band", "subgroup"),
    ("season", "season"),
    ("surface", "surface"),
    ("venue", "racetrack"),
)

_ARCHITECTURE_BY_CATEGORY: Final[dict[str, str]] = {
    "jra": "catboost",
    "nar": "xgboost",
    "ban-ei": "catboost",
}

_VALID_CATEGORIES: Final[tuple[str, ...]] = ("jra", "nar", "ban-ei")

_SELECT_CELLS: Final[LiteralString] = """
SELECT category, class_label, distance_band, venue, season, surface,
       feature_set_hash, race_count,
       top1_accuracy, place2_accuracy, place3_accuracy,
       place4_accuracy, place5_accuracy, place6_accuracy,
       top3_box_accuracy, evaluated_at, feature_names_array
FROM cell_training_evaluations
WHERE category = %s
"""


@dataclass(frozen=True)
class CellKey:
    category: str
    class_label: str
    subgroup: str
    racetrack: str
    season: str
    surface: str


@dataclass(frozen=True)
class CellMetrics:
    race_count: int
    top1: float
    place2: float
    place3: float
    place4: float
    place5: float
    place6: float
    top3_box: float
    evaluated_at: datetime
    feature_set_hash: str
    feature_names: list[str]


@dataclass(frozen=True)
class AdoptionResult:
    cell: CellKey
    candidate: CellMetrics
    baseline: CellMetrics
    deltas: dict[str, float]
    adopted: bool
    rejection_reasons: list[str]


def _metric_value(metrics: CellMetrics, name: str) -> float:
    return {
        "top1": metrics.top1,
        "place2": metrics.place2,
        "place3": metrics.place3,
        "place4": metrics.place4,
        "place5": metrics.place5,
        "place6": metrics.place6,
        "top3_box": metrics.top3_box,
    }[name]


def compute_deltas(baseline: CellMetrics, candidate: CellMetrics) -> dict[str, float]:
    """Compute metric deltas (candidate - baseline) for every gated metric."""
    return {
        name: _metric_value(candidate, name) - _metric_value(baseline, name)
        for name in _NO_REGRESSION_METRICS
    }


def check_multi_metric_gate(
    deltas: Mapping[str, float], min_delta: float = DEFAULT_MIN_DELTA
) -> tuple[bool, list[str]]:
    """Require >=2 primary metrics improved by >=min_delta, >=1 being place2/place3."""
    improved = [name for name in _PRIMARY_METRICS if deltas.get(name, 0.0) >= min_delta]
    reasons: list[str] = []
    if len(improved) < 2:
        reasons.append(
            f"only {len(improved)} primary metric(s) improved by >= {min_delta}; need >= 2"
        )
    if not any(name in _PLACE_PRIMARY_METRICS for name in improved):
        reasons.append("no place2/place3 among improved primary metrics")
    return not reasons, reasons


def check_no_regression(
    deltas: Mapping[str, float], threshold: float = DEFAULT_NO_REG_THRESHOLD
) -> tuple[bool, list[str]]:
    """Require every rank metric (top1-top6) and top3_box to stay above threshold."""
    reasons = [
        f"{name} regressed by {deltas.get(name, 0.0):+.5f} (<= {threshold})"
        for name in _NO_REGRESSION_METRICS
        if deltas.get(name, 0.0) <= threshold
    ]
    return not reasons, reasons


def synthesize_hit_vector(accuracy: float, race_count: int) -> list[float]:
    """Reconstruct an approximate per-race 0/1 hit vector from an aggregate accuracy.

    ``cell_training_evaluations`` stores only the aggregate accuracy and race
    count, not the per-race hit indicators a true bootstrap needs, so this builds
    a Bernoulli vector with ``round(accuracy * race_count)`` hits. It has the right
    mean and binomial variance, which is the input ``bootstrap_lb95`` resamples.
    """
    hits = max(0, min(race_count, round(accuracy * race_count)))
    return [1.0] * hits + [0.0] * (race_count - hits)


def bootstrap_lb95(
    baseline_values: Sequence[float],
    candidate_values: Sequence[float],
    n_boot: int = DEFAULT_N_BOOT,
    seed: int = _BOOTSTRAP_SEED,
) -> float:
    """Bootstrap 95% CI lower bound for the mean delta (candidate - baseline).

    TODO: the per-race hit vectors fed here are synthesized from aggregate cell
    accuracies via :func:`synthesize_hit_vector` because the raw per-race
    indicators are not persisted in ``cell_training_evaluations``. Replace the
    synthesized inputs with real per-race values (a dedicated PG query) for an
    exact resampled interval rather than a binomial-variance approximation.
    """
    base = np.asarray(list(baseline_values), dtype=float)
    cand = np.asarray(list(candidate_values), dtype=float)
    if base.size == 0 or cand.size == 0:
        return 0.0
    rng = np.random.default_rng(seed)
    base_means = rng.choice(base, size=(n_boot, base.size), replace=True).mean(axis=1)
    cand_means = rng.choice(cand, size=(n_boot, cand.size), replace=True).mean(axis=1)
    diffs = cand_means - base_means
    return float(np.percentile(diffs, 2.5))


def evaluate_cell(
    cell: CellKey,
    baseline: CellMetrics,
    candidate: CellMetrics,
    min_races: int = DEFAULT_MIN_RACES,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    min_delta: float = DEFAULT_MIN_DELTA,
    no_reg_threshold: float = DEFAULT_NO_REG_THRESHOLD,
    n_boot: int = DEFAULT_N_BOOT,
    now: datetime | None = None,
) -> AdoptionResult:
    """Apply the full multi-metric adoption gate to one candidate of a cell."""
    deltas = compute_deltas(baseline, candidate)
    reasons: list[str] = []

    if candidate.race_count < min_races:
        reasons.append(f"race_count {candidate.race_count} < {min_races}")

    reference_now = now if now is not None else datetime.now(timezone.utc)
    if reference_now - candidate.evaluated_at > timedelta(days=freshness_days):
        reasons.append(
            f"evaluated_at {candidate.evaluated_at.isoformat()} older than "
            f"{freshness_days} days"
        )

    _, multi_metric_reasons = check_multi_metric_gate(deltas, min_delta)
    reasons.extend(multi_metric_reasons)

    _, no_regression_reasons = check_no_regression(deltas, no_reg_threshold)
    reasons.extend(no_regression_reasons)

    for name in _PRIMARY_METRICS:
        if deltas[name] < min_delta:
            continue
        lb95 = bootstrap_lb95(
            synthesize_hit_vector(_metric_value(baseline, name), baseline.race_count),
            synthesize_hit_vector(_metric_value(candidate, name), candidate.race_count),
            n_boot=n_boot,
        )
        if lb95 <= 0.0:
            reasons.append(f"{name} bootstrap LB95 {lb95:+.5f} <= 0")

    return AdoptionResult(
        cell=cell,
        candidate=candidate,
        baseline=baseline,
        deltas=deltas,
        adopted=not reasons,
        rejection_reasons=reasons,
    )


def _find_baseline(
    metrics_list: Sequence[CellMetrics], baseline_hash: str
) -> CellMetrics | None:
    for metrics in metrics_list:
        if metrics.feature_set_hash == baseline_hash:
            return metrics
    return None


def evaluate_category(
    grouped: Mapping[CellKey, Sequence[CellMetrics]],
    baseline_hash: str,
    min_races: int = DEFAULT_MIN_RACES,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    now: datetime | None = None,
) -> list[AdoptionResult]:
    """Evaluate every candidate against its cell's baseline; skip baseline-less cells."""
    results: list[AdoptionResult] = []
    for cell, metrics_list in grouped.items():
        baseline = _find_baseline(metrics_list, baseline_hash)
        if baseline is None:
            continue
        for candidate in metrics_list:
            if candidate.feature_set_hash == baseline_hash:
                continue
            results.append(
                evaluate_cell(
                    cell,
                    baseline,
                    candidate,
                    min_races=min_races,
                    freshness_days=freshness_days,
                    now=now,
                )
            )
    return results


def variant_name_for_hash(feature_set_hash: str) -> str:
    return f"cell-{feature_set_hash[:8]}"


def group_variants(
    adopted: Sequence[AdoptionResult],
) -> dict[str, list[AdoptionResult]]:
    """Group adopted cells by candidate feature-set hash -> variant name."""
    groups: dict[str, list[AdoptionResult]] = {}
    for result in adopted:
        variant = variant_name_for_hash(result.candidate.feature_set_hash)
        groups.setdefault(variant, []).append(result)
    return groups


def _cell_sort_key(cell: CellKey) -> tuple[str, str, str, str, str]:
    return (cell.class_label, cell.subgroup, cell.season, cell.surface, cell.racetrack)


def _cell_conditions(cell: CellKey) -> list[dict[str, object]]:
    pairs = (
        ("class", cell.class_label),
        ("distance_band", cell.subgroup),
        ("season", cell.season),
        ("surface", cell.surface),
        ("venue", cell.racetrack),
    )
    return [
        {"dimension": dimension, "values": [value]}
        for dimension, value in pairs
        if value
    ]


def generate_routing_json(
    category: str,
    default_model_version: str,
    default_feature_count: int,
    default_architecture: str,
    variants: Mapping[str, Sequence[AdoptionResult]],
) -> dict[str, object]:
    """Render the N-variant ``cell_routing.json`` body for one category."""
    variant_specs: dict[str, object] = {
        VARIANT_SIM: {
            "model_version": default_model_version,
            "feature_count": default_feature_count,
            "architecture": default_architecture,
        }
    }
    rules: list[dict[str, object]] = []
    for variant_name in sorted(variants):
        results = variants[variant_name]
        variant_specs[variant_name] = {
            "model_version": variant_name,
            "feature_count": len(results[0].candidate.feature_names),
            "architecture": default_architecture,
        }
        for result in sorted(results, key=lambda r: _cell_sort_key(r.cell)):
            rules.append(
                {"conditions": _cell_conditions(result.cell), "variant": variant_name}
            )
    return {
        category: {
            "default_variant": VARIANT_SIM,
            "variants": variant_specs,
            "rules": rules,
        }
    }


def parse_row(row: Sequence[object]) -> tuple[CellKey, CellMetrics]:
    """Map one ``_SELECT_CELLS`` row to its CellKey and CellMetrics."""
    cell = CellKey(
        category=str(row[0]),
        class_label=str(row[1]),
        subgroup=str(row[2]),
        racetrack=str(row[3]),
        season=str(row[4]),
        surface=str(row[5]),
    )
    feature_names = [str(name) for name in cast("Sequence[object]", row[16])]
    metrics = CellMetrics(
        race_count=int(cast("SupportsInt", row[7])),
        top1=float(cast("SupportsFloat", row[8])),
        place2=float(cast("SupportsFloat", row[9])),
        place3=float(cast("SupportsFloat", row[10])),
        place4=float(cast("SupportsFloat", row[11])),
        place5=float(cast("SupportsFloat", row[12])),
        place6=float(cast("SupportsFloat", row[13])),
        top3_box=float(cast("SupportsFloat", row[14])),
        evaluated_at=cast("datetime", row[15]),
        feature_set_hash=str(row[6]),
        feature_names=feature_names,
    )
    return cell, metrics


def load_cell_metrics(
    conn: psycopg.Connection[tuple[object, ...]], category: str
) -> dict[CellKey, list[CellMetrics]]:
    """Read every evaluation row for ``category`` and group it by cell."""
    with conn.cursor() as cur:
        cur.execute(_SELECT_CELLS, (category,))
        rows = cur.fetchall()
    grouped: dict[CellKey, list[CellMetrics]] = {}
    for row in rows:
        cell, metrics = parse_row(row)
        grouped.setdefault(cell, []).append(metrics)
    return grouped


def _connect(pg_url: str) -> psycopg.Connection[tuple[object, ...]]:
    psycopg_mod = __import__("psycopg")
    return psycopg_mod.connect(pg_url)


def _infer_default_feature_count(
    grouped: Mapping[CellKey, Sequence[CellMetrics]], baseline_hash: str
) -> int:
    for metrics_list in grouped.values():
        baseline = _find_baseline(metrics_list, baseline_hash)
        if baseline is not None:
            return len(baseline.feature_names)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read cell_training_evaluations and emit an N-variant cell_routing.json "
            "for cells whose candidate feature set clears the multi-metric gate."
        )
    )
    parser.add_argument("--pg-url", type=str, required=True)
    parser.add_argument(
        "--category", type=str, required=True, choices=list(_VALID_CATEGORIES)
    )
    parser.add_argument("--baseline-hash", type=str, required=True)
    parser.add_argument("--output-path", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--min-races", type=int, default=DEFAULT_MIN_RACES)
    parser.add_argument("--freshness-days", type=int, default=DEFAULT_FRESHNESS_DAYS)
    parser.add_argument("--default-model-version", type=str, default=None)
    parser.add_argument("--default-feature-count", type=int, default=None)
    parser.add_argument("--default-architecture", type=str, default=None)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        stream=sys.stdout,
    )

    category = str(args.category)
    baseline_hash = str(args.baseline_hash)

    conn = _connect(str(args.pg_url))
    try:
        grouped = load_cell_metrics(conn, category)
    finally:
        conn.close()

    default_model_version = (
        str(args.default_model_version)
        if args.default_model_version is not None
        else f"{category}-production"
    )
    default_feature_count = (
        int(args.default_feature_count)
        if args.default_feature_count is not None
        else _infer_default_feature_count(grouped, baseline_hash)
    )
    default_architecture = (
        str(args.default_architecture)
        if args.default_architecture is not None
        else _ARCHITECTURE_BY_CATEGORY.get(category, "catboost")
    )

    results = evaluate_category(
        grouped,
        baseline_hash,
        min_races=int(args.min_races),
        freshness_days=int(args.freshness_days),
    )
    adopted = [result for result in results if result.adopted]
    variants = group_variants(adopted)
    config = generate_routing_json(
        category,
        default_model_version,
        default_feature_count,
        default_architecture,
        variants,
    )
    payload = json.dumps(config, ensure_ascii=False, indent=2)

    _logger.info(
        "category=%s cells=%d candidates=%d adopted=%d variants=%d",
        category,
        len(grouped),
        len(results),
        len(adopted),
        len(variants),
    )

    if args.output_path is not None and not args.dry_run:
        output_path = Path(args.output_path)
        output_path.write_text(payload + "\n", encoding="utf-8")
        _logger.info("wrote routing config to %s", output_path)
    else:
        sys.stdout.write(payload + "\n")


if __name__ == "__main__":
    main()
