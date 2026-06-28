"""Cell-level model routing for finish-position predictions.

Some categories benefit from scoring different cells (category x class x
subgroup x racetrack x season x surface) with different baked models. Ban-ei
routes class=E races to the v8 base model (111 features) while everything else
uses the v9 sim model (130 features); JRA and NAR have no routing and always
use their single ``MODEL_VERSION_BY_CATEGORY`` model.

A rule matches a race when *all* of its conditions hold (logical AND), so a
single rule can target a multi-dimensional cell such as ``venue=03`` AND
``surface=turf`` AND ``season=summer``. Dimensions are resolved from the raw
entry columns, deriving ``surface`` / ``distance_band`` / ``season`` / ``class``
on the fly; any dimension that is not one of the derived names falls back to the
raw column of the same name.

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
class CellCondition:
    dimension: str
    values: frozenset[str]


@dataclass(frozen=True)
class CellRouteRule:
    conditions: tuple[CellCondition, ...]
    variant: str


@dataclass(frozen=True)
class VariantSpec:
    model_version: str
    feature_count: int
    architecture: str


@dataclass(frozen=True)
class CategoryRouting:
    default_variant: str
    variants: dict[str, VariantSpec]
    rules: tuple[CellRouteRule, ...]

    @property
    def sim_model_version(self) -> str:
        return self.variants[VARIANT_SIM].model_version

    @property
    def base_model_version(self) -> str:
        return self.variants[VARIANT_BASE].model_version

    @property
    def base_feature_count(self) -> int:
        return self.variants[VARIANT_BASE].feature_count

    @property
    def base_architecture(self) -> str:
        return self.variants[VARIANT_BASE].architecture


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


def resolve_dimension(entry: Mapping[str, object], dimension: str, category: str) -> str | None:
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
    if dimension == "season":
        tsukihi = entry.get("kaisai_tsukihi")
        if tsukihi is not None:
            month_str = str(tsukihi).strip()[:2]
            if month_str.isdigit():
                return derive_season(int(month_str))
        race_id = entry.get("race_id")
        if race_id is not None:
            parts = str(race_id).split(":")
            if len(parts) >= 3:
                tsukihi_part = parts[2]
                if len(tsukihi_part) >= 2 and tsukihi_part[:2].isdigit():
                    return derive_season(int(tsukihi_part[:2]))
        return None
    if dimension == "class":
        grade_code = entry.get("grade_code")
        if grade_code is None:
            return None
        return derive_class(str(grade_code).strip())
    raw = entry.get(dimension)
    return str(raw).strip() if raw is not None else None


def all_conditions_match(
    entry: Mapping[str, object],
    conditions: tuple[CellCondition, ...],
    category: str,
) -> bool:
    for condition in conditions:
        value = resolve_dimension(entry, condition.dimension, category)
        if value is None or value not in condition.values:
            return False
    return True


class CellRouter:
    _routing: dict[str, CategoryRouting]

    def __init__(self, routing: dict[str, CategoryRouting]) -> None:
        self._routing = routing

    def has_routing(self, category: str) -> bool:
        return category in self._routing

    def routing_for(self, category: str) -> CategoryRouting:
        return self._routing[category]

    def resolve_variant(self, category: str, entries: Sequence[Mapping[str, object]]) -> str:
        if category not in self._routing:
            return VARIANT_SIM
        routing = self._routing[category]
        if not entries:
            return routing.default_variant
        first = entries[0]
        for rule in routing.rules:
            if all_conditions_match(first, rule.conditions, category):
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


def _parse_condition(value: object) -> CellCondition:
    condition = _as_mapping(value, "condition")
    return CellCondition(
        dimension=str(condition["dimension"]),
        values=frozenset(str(v) for v in _as_sequence(condition["values"], "values")),
    )


def _parse_rule(value: object) -> CellRouteRule:
    rule = _as_mapping(value, "rule")
    conditions = tuple(
        _parse_condition(condition) for condition in _as_sequence(rule["conditions"], "conditions")
    )
    return CellRouteRule(conditions=conditions, variant=str(rule["variant"]))


def _parse_variant_spec(value: object) -> VariantSpec:
    spec = _as_mapping(value, "variant")
    return VariantSpec(
        model_version=str(spec["model_version"]),
        feature_count=int(str(spec["feature_count"])),
        architecture=str(spec["architecture"]),
    )


def _parse_variants(payload: Mapping[str, object]) -> dict[str, VariantSpec]:
    """Parse the per-category variant table, auto-detecting old vs new format.

    New format carries an explicit ``variants`` object keyed by variant name. The
    legacy flat format only records ``base_feature_count`` / ``base_architecture``
    for the base variant, so the sim variant's feature count is unknown and stored
    as ``0`` (the serve path reads it from ``model_meta`` for the default variant).
    """
    if "variants" in payload:
        variants = _as_mapping(payload["variants"], "variants")
        return {name: _parse_variant_spec(spec) for name, spec in variants.items()}
    base_architecture = str(payload["base_architecture"])
    return {
        VARIANT_SIM: VariantSpec(
            model_version=str(payload["sim_model_version"]),
            feature_count=0,
            architecture=base_architecture,
        ),
        VARIANT_BASE: VariantSpec(
            model_version=str(payload["base_model_version"]),
            feature_count=int(str(payload["base_feature_count"])),
            architecture=base_architecture,
        ),
    }


def _parse_category_routing(payload: Mapping[str, object]) -> CategoryRouting:
    rules = tuple(_parse_rule(rule) for rule in _as_sequence(payload["rules"], "rules"))
    return CategoryRouting(
        default_variant=str(payload["default_variant"]),
        variants=_parse_variants(payload),
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


def build_base_model_r2_key(category: str, base_model_version: str, file_name: str) -> str:
    return f"{R2_KEY_PREFIX}/{category}/{base_model_version}/{file_name}"


def build_base_metadata_r2_key(category: str, base_model_version: str) -> str:
    return build_base_model_r2_key(category, base_model_version, METADATA_FILE_NAME)
