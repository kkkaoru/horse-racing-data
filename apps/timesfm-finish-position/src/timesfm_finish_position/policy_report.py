"""Typed report contracts and validation for offline policy evaluation."""

from __future__ import annotations

from typing import Literal, NotRequired, TypedDict

Category = Literal["jra", "nar", "ban-ei"]


class MetricReport(TypedDict):
    baseline_hits: int
    adjusted_hits: int
    delta_hits: int
    baseline_rate: float
    adjusted_rate: float
    delta_pp: float


class BranchReport(TypedDict):
    served_model_version: NotRequired[str]
    enabled: bool
    weight: float
    effective_weight: float
    races: int
    years: dict[str, int]
    adjustment_fallbacks: dict[str, int]
    components: list[dict[str, str]]
    component_weight_contract: str
    metrics: dict[str, MetricReport]


class CellReport(TypedDict):
    branches: dict[str, BranchReport]


class SignatureCellReport(TypedDict):
    signatures: dict[str, BranchReport]


class CategoryReport(TypedDict):
    cells: dict[str, CellReport]
    summary: dict[str, object]
    configured_routes_without_observed_top_level_branch: list[str]
    folded_component_routes: list[str]
    routes_without_replay_support: list[str]
    configured_route_inventory: dict[str, dict[str, str]]


class SignatureCategoryReport(TypedDict):
    cells: dict[str, SignatureCellReport]
    summary: dict[str, object]
    configured_routes_without_observed_top_level_branch: list[str]
    folded_component_routes: list[str]
    routes_without_replay_support: list[str]
    configured_route_inventory: dict[str, dict[str, str]]


class EvaluationReport(TypedDict):
    contract: dict[str, object]
    categories: dict[str, CategoryReport]
    skipped: dict[str, int]


class SignatureEvaluationReport(TypedDict):
    contract: dict[str, object]
    categories: dict[str, SignatureCategoryReport]
    skipped: dict[str, int]


class OptimizedCellReport(TypedDict):
    enabled: bool
    weight: float
    effective_weight: float
    selected_weight: float | None
    races: int
    years: dict[str, int]
    support: str
    metrics: dict[str, object]


class OptimizedCategoryReport(TypedDict):
    default_enabled: bool
    default_weight: float
    cells: dict[str, OptimizedCellReport]


def require_category(value: str) -> Category:
    """Reject categories unsupported by production score adjustment."""
    if value not in ("jra", "nar", "ban-ei"):
        raise ValueError(f"Unsupported prediction category: {value}")
    return value


def require_float(value: object) -> float:
    """Validate numeric values crossing untyped row and JSON boundaries."""
    if not isinstance(value, (int, float, str)) or isinstance(value, bool):
        raise ValueError(f"Expected a numeric value, got {value!r}")
    return float(value)


def require_int(value: object) -> int:
    """Preserve integer conversion semantics while rejecting missing values."""
    if not isinstance(value, (int, float, str)) or isinstance(value, bool):
        raise ValueError(f"Expected an integer value, got {value!r}")
    return int(value)
