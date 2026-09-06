"""Validated cell-level rollout policy for Prophet score adjustment."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final

from .model_meta import Category

POLICY_FILE_NAME: Final[str] = "prophet_cell_policy.json"
DEFAULT_POLICY_PATH: Final[Path] = Path(__file__).with_name(POLICY_FILE_NAME)
MAXIMUM_WEIGHT: Final[float] = 1.0


@dataclass(frozen=True, slots=True)
class ProphetCellDecision:
    """Resolved adjustment switch and scale for one production cell."""

    enabled: bool
    weight: float


@dataclass(frozen=True, slots=True)
class ProphetCellPolicy:
    """Category/cell/served-branch overrides with an ON-by-default fallback."""

    version: str
    default_decision: ProphetCellDecision
    cells: Mapping[Category, Mapping[str, ProphetCellDecision]]
    branches: Mapping[Category, Mapping[str, Mapping[str, ProphetCellDecision]]]
    signatures: Mapping[Category, Mapping[str, Mapping[str, ProphetCellDecision]]]

    def resolve(
        self,
        category: Category,
        cell: str,
        branch: str | None = None,
        signature: str | None = None,
    ) -> ProphetCellDecision:
        if signature is not None:
            category_signatures = self.signatures.get(category)
            if category_signatures is not None:
                cell_signatures = category_signatures.get(cell)
                if cell_signatures is not None and signature in cell_signatures:
                    return cell_signatures[signature]
        if branch is not None:
            category_branches = self.branches.get(category)
            if category_branches is not None:
                cell_branches = category_branches.get(cell)
                if cell_branches is not None and branch in cell_branches:
                    return cell_branches[branch]
        category_cells = self.cells.get(category)
        if category_cells is None:
            return self.default_decision
        return category_cells.get(cell, self.default_decision)


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"Prophet cell policy {field} must be an object with string keys")
    return {key: item for key, item in value.items() if isinstance(key, str)}


def _weight(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Prophet cell policy {field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result <= 0.0 or result > MAXIMUM_WEIGHT:
        raise ValueError(f"Prophet cell policy {field} must be in (0, 1]")
    return result


def _decision(value: object, default_weight: float, field: str) -> ProphetCellDecision:
    data = _mapping(value, field)
    enabled = data.get("enabled")
    if not isinstance(enabled, bool):
        raise ValueError(f"Prophet cell policy {field}.enabled must be boolean")
    raw_weight = data.get("weight", default_weight)
    return ProphetCellDecision(enabled=enabled, weight=_weight(raw_weight, f"{field}.weight"))


def parse_prophet_cell_policy(value: object) -> ProphetCellPolicy:
    """Validate a decoded policy without accepting partial malformed data."""
    root = _mapping(value, "root")
    version = root.get("version")
    if not isinstance(version, str) or not version.strip():
        raise ValueError("Prophet cell policy version must be a non-empty string")
    default_enabled = root.get("default_enabled")
    if not isinstance(default_enabled, bool):
        raise ValueError("Prophet cell policy default_enabled must be boolean")
    default_weight = _weight(root.get("default_weight"), "default_weight")
    categories = _mapping(root.get("categories"), "categories")
    parsed_categories: dict[Category, Mapping[str, ProphetCellDecision]] = {}
    parsed_branches: dict[Category, Mapping[str, Mapping[str, ProphetCellDecision]]] = {}
    parsed_signatures: dict[Category, Mapping[str, Mapping[str, ProphetCellDecision]]] = {}
    for category in ("jra", "nar", "ban-ei"):
        raw_category = categories.get(category, {})
        category_data = _mapping(raw_category, f"categories.{category}")
        raw_cells = _mapping(category_data.get("cells", {}), f"categories.{category}.cells")
        parsed_cells = {
            cell: _decision(raw_decision, default_weight, f"categories.{category}.cells.{cell}")
            for cell, raw_decision in raw_cells.items()
        }
        raw_branch_cells = _mapping(
            category_data.get("branches", {}), f"categories.{category}.branches"
        )
        parsed_branch_cells: dict[str, Mapping[str, ProphetCellDecision]] = {}
        for cell, raw_cell_branches in raw_branch_cells.items():
            branch_data = _mapping(raw_cell_branches, f"categories.{category}.branches.{cell}")
            parsed_branch_cells[cell] = MappingProxyType(
                {
                    branch: _decision(
                        raw_decision,
                        default_weight,
                        f"categories.{category}.branches.{cell}.{branch}",
                    )
                    for branch, raw_decision in branch_data.items()
                }
            )
        raw_signature_cells = _mapping(
            category_data.get("signatures", {}), f"categories.{category}.signatures"
        )
        parsed_signature_cells: dict[str, Mapping[str, ProphetCellDecision]] = {}
        for cell, raw_cell_signatures in raw_signature_cells.items():
            signature_data = _mapping(
                raw_cell_signatures, f"categories.{category}.signatures.{cell}"
            )
            parsed_signature_cells[cell] = MappingProxyType(
                {
                    signature: _decision(
                        raw_decision,
                        default_weight,
                        f"categories.{category}.signatures.{cell}.{signature}",
                    )
                    for signature, raw_decision in signature_data.items()
                }
            )
        parsed_categories[category] = MappingProxyType(parsed_cells)
        parsed_branches[category] = MappingProxyType(parsed_branch_cells)
        parsed_signatures[category] = MappingProxyType(parsed_signature_cells)
    return ProphetCellPolicy(
        version=version,
        default_decision=ProphetCellDecision(default_enabled, default_weight),
        cells=MappingProxyType(parsed_categories),
        branches=MappingProxyType(parsed_branches),
        signatures=MappingProxyType(parsed_signatures),
    )


def load_prophet_cell_policy(path: Path = DEFAULT_POLICY_PATH) -> ProphetCellPolicy:
    """Load the baked policy once during predictor process initialization."""
    try:
        decoded: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to load Prophet cell policy: {path}") from exc
    return parse_prophet_cell_policy(decoded)
