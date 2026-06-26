"""Cell-level model routing for finish-position predictions.

Some categories benefit from scoring different cells (category x class x
subgroup x racetrack x season x surface) with different baked models. Ban-ei
routes class=E races to the v8 base model (111 features) while everything else
uses the v9 sim model (130 features); JRA and NAR have no routing and always
use their single ``MODEL_VERSION_BY_CATEGORY`` model.

The routing table is data-driven (``cell_routing.json``) so adding a cell rule
does not require touching the serve loop.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from .model_meta import (
    METADATA_FILE_NAME,
    R2_KEY_PREFIX,
)

CONFIG_FILE_NAME: Final[str] = "cell_routing.json"

VARIANT_SIM: Final[str] = "sim"
VARIANT_BASE: Final[str] = "base"


@dataclass(frozen=True)
class CellRouteRule:
    dimension: str
    values: frozenset[str]
    variant: str


@dataclass(frozen=True)
class CategoryRouting:
    sim_model_version: str
    base_model_version: str
    base_feature_count: int
    base_architecture: str
    default_variant: str
    rules: tuple[CellRouteRule, ...]


class CellRouter:
    _routing: dict[str, CategoryRouting]

    def __init__(self, routing: dict[str, CategoryRouting]) -> None:
        self._routing = routing

    def has_routing(self, category: str) -> bool:
        return category in self._routing

    def routing_for(self, category: str) -> CategoryRouting:
        return self._routing[category]

    def resolve_variant(
        self, category: str, entries: Sequence[Mapping[str, object]]
    ) -> str:
        if category not in self._routing:
            return VARIANT_SIM
        routing = self._routing[category]
        if not entries:
            return routing.default_variant
        first = entries[0]
        for rule in routing.rules:
            value = first.get(rule.dimension)
            if isinstance(value, str) and value in rule.values:
                return rule.variant
        return routing.default_variant


def _as_mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"cell_routing.json: '{field}' must be an object")
    return {str(key): val for key, val in value.items()}


def _as_sequence(value: object, field: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise ValueError(f"cell_routing.json: '{field}' must be an array")
    return value


def _parse_rule(value: object) -> CellRouteRule:
    rule = _as_mapping(value, "rule")
    return CellRouteRule(
        dimension=str(rule["dimension"]),
        values=frozenset(str(v) for v in _as_sequence(rule["values"], "values")),
        variant=str(rule["variant"]),
    )


def _parse_category_routing(payload: Mapping[str, object]) -> CategoryRouting:
    rules = tuple(_parse_rule(rule) for rule in _as_sequence(payload["rules"], "rules"))
    return CategoryRouting(
        sim_model_version=str(payload["sim_model_version"]),
        base_model_version=str(payload["base_model_version"]),
        base_feature_count=int(str(payload["base_feature_count"])),
        base_architecture=str(payload["base_architecture"]),
        default_variant=str(payload["default_variant"]),
        rules=rules,
    )


def load_cell_router(config_path: Path | None = None) -> CellRouter:
    path = config_path if config_path is not None else Path(__file__).parent / CONFIG_FILE_NAME
    if not path.exists():
        return CellRouter({})
    payload = _as_mapping(json.loads(path.read_text(encoding="utf-8")), "root")
    routing = {
        category: _parse_category_routing(_as_mapping(entry, category))
        for category, entry in payload.items()
    }
    return CellRouter(routing)


def build_base_model_r2_key(
    category: str, base_model_version: str, file_name: str
) -> str:
    return f"{R2_KEY_PREFIX}/{category}/{base_model_version}/{file_name}"


def build_base_metadata_r2_key(category: str, base_model_version: str) -> str:
    return build_base_model_r2_key(category, base_model_version, METADATA_FILE_NAME)
