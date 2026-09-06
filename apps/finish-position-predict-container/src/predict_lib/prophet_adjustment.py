"""Lightweight score adjustment from day-precomputed Prophet entity trends.

Prophet itself never runs in the prediction Container. A day/category feature
layer joins frozen daily forecasts onto each runner; this module only performs
an O(field-size) centered score adjustment after the normal production routing
has selected its final model rows.
"""

from __future__ import annotations

import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from .model_meta import Category
from .prophet_cell_policy import (
    ProphetCellPolicy,
    load_prophet_cell_policy,
)

PROPHET_ENABLED_ENV: Final[str] = "PROPHET_SCORE_ADJUSTMENT_ENABLED"
PROPHET_WEIGHT_ENV: Final[str] = "PROPHET_SCORE_ADJUSTMENT_WEIGHT"
PROPHET_TREND_FIELD: Final[str] = "prophet_entity_performance_mean"
PROPHET_COVERAGE_FIELD: Final[str] = "prophet_entity_coverage"
KETTO_FIELD: Final[str] = "ketto_toroku_bango"
ROW_KETTO_INDEX: Final[int] = 6
ROW_SCORE_INDEX: Final[int] = 8
ROW_RANK_INDEX: Final[int] = 9
MINIMUM_ELIGIBLE_RUNNERS: Final[int] = 2
MINIMUM_ELIGIBLE_FRACTION: Final[float] = 0.5
MAXIMUM_ADJUSTMENT_WEIGHT: Final[float] = 1.0
MINIMUM_STANDARD_DEVIATION: Final[float] = 1e-12
DISABLED_ENV_VALUES: Final[frozenset[str]] = frozenset({"0", "false", "off", "disabled"})
DEFAULT_PROPHET_CELL_POLICY: Final[ProphetCellPolicy] = load_prophet_cell_policy()


@dataclass(frozen=True, slots=True)
class ProphetAdjustmentResult:
    """Adjusted prediction rows plus an auditable no-op/application reason."""

    rows: list[list[object]]
    applied: bool
    reason: str


def _category_enabled(category: Category, values: Mapping[str, str]) -> bool:
    raw_enabled = values.get(PROPHET_ENABLED_ENV)
    if raw_enabled is None or not raw_enabled.strip():
        return True
    normalized = raw_enabled.strip().lower()
    if normalized in DISABLED_ENV_VALUES:
        return False
    enabled = {token.strip() for token in raw_enabled.split(",")}
    return category in enabled


def configured_prophet_weight(
    category: Category,
    environment: Mapping[str, str] | None = None,
    *,
    cell_variant: str = "sim",
    branch_variant: str | None = None,
    served_signature: str | None = None,
    policy: ProphetCellPolicy = DEFAULT_PROPHET_CELL_POLICY,
) -> float | None:
    """Resolve the cell policy, with environment values as emergency overrides."""
    values = os.environ if environment is None else environment
    decision = policy.resolve(category, cell_variant, branch_variant, served_signature)
    if not decision.enabled or not _category_enabled(category, values):
        return None
    raw_weight = values.get(PROPHET_WEIGHT_ENV)
    if raw_weight is None or not raw_weight.strip():
        return decision.weight
    try:
        weight = float(raw_weight)
    except ValueError:
        return None
    if not math.isfinite(weight) or weight <= 0.0 or weight > MAXIMUM_ADJUSTMENT_WEIGHT:
        return None
    return weight


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _population_standard_deviation(values: Sequence[float], mean: float) -> float:
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def _adjusted_row_sort_key(row: Sequence[object]) -> tuple[float, str]:
    score = _finite_number(row[ROW_SCORE_INDEX])
    return (-(score if score is not None else -math.inf), str(row[ROW_KETTO_INDEX]))


def adjust_prediction_rows_with_prophet(
    rows: Sequence[Sequence[object]],
    entries: Sequence[Mapping[str, object]],
    category: Category,
    environment: Mapping[str, str] | None = None,
    *,
    cell_variant: str = "sim",
    branch_variant: str | None = None,
    served_signature: str | None = None,
    policy: ProphetCellPolicy = DEFAULT_PROPHET_CELL_POLICY,
) -> ProphetAdjustmentResult:
    """Apply a scale-preserving Prophet trend adjustment to final routed rows.

    Missing runner trends receive the race mean, hence a zero adjustment. The
    normal model rows are returned unchanged unless at least half the field and
    at least two runners carry a finite precomputed trend. Invalid configuration
    or degenerate score/trend spreads also fail safely to the original rows.
    """
    copied_rows = [list(row) for row in rows]
    weight = configured_prophet_weight(
        category,
        environment,
        cell_variant=cell_variant,
        branch_variant=branch_variant,
        served_signature=served_signature,
        policy=policy,
    )
    if weight is None:
        return ProphetAdjustmentResult(copied_rows, False, "disabled")
    if not copied_rows or len(copied_rows) != len(entries):
        return ProphetAdjustmentResult(copied_rows, False, "row-entry-mismatch")

    trends_by_horse: dict[str, float] = {}
    for entry in entries:
        coverage = _finite_number(entry.get(PROPHET_COVERAGE_FIELD))
        trend = _finite_number(entry.get(PROPHET_TREND_FIELD))
        horse_id = str(entry.get(KETTO_FIELD, ""))
        if coverage is not None and coverage >= 1.0 and trend is not None and horse_id:
            trends_by_horse[horse_id] = trend

    required = max(
        MINIMUM_ELIGIBLE_RUNNERS, math.ceil(len(copied_rows) * MINIMUM_ELIGIBLE_FRACTION)
    )
    if len(trends_by_horse) < required:
        return ProphetAdjustmentResult(copied_rows, False, "insufficient-coverage")

    available_trends = list(trends_by_horse.values())
    trend_mean = sum(available_trends) / len(available_trends)
    aligned_trends = [
        trends_by_horse.get(str(row[ROW_KETTO_INDEX]), trend_mean) for row in copied_rows
    ]
    scores = [_finite_number(row[ROW_SCORE_INDEX]) for row in copied_rows]
    if any(score is None for score in scores):
        return ProphetAdjustmentResult(copied_rows, False, "invalid-score")
    finite_scores = [score for score in scores if score is not None]
    score_mean = sum(finite_scores) / len(finite_scores)
    score_std = _population_standard_deviation(finite_scores, score_mean)
    trend_std = _population_standard_deviation(aligned_trends, trend_mean)
    if score_std <= MINIMUM_STANDARD_DEVIATION or trend_std <= MINIMUM_STANDARD_DEVIATION:
        return ProphetAdjustmentResult(copied_rows, False, "degenerate-spread")

    for row, score, trend in zip(copied_rows, finite_scores, aligned_trends, strict=True):
        row[ROW_SCORE_INDEX] = score + weight * score_std * (trend - trend_mean) / trend_std
    copied_rows.sort(key=_adjusted_row_sort_key)
    for rank, row in enumerate(copied_rows, start=1):
        row[ROW_RANK_INDEX] = rank
    return ProphetAdjustmentResult(copied_rows, True, "applied")
