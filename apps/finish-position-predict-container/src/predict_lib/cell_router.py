"""Cell-level model routing for finish-position predictions.

Some categories benefit from scoring different cells (category x class x
subgroup x racetrack x season x surface) with different baked models. Ban-ei
routes class=E races to the v8 base model (111 features) while everything else
uses the v9 sim model (130 features). NAR can route narrow cells, such as
``dirt / mile / E / summer / venue 54``, to a focused cell model while every
unmatched race falls back to the category default model.

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
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from .model_meta import (
    METADATA_FILE_NAME,
    R2_KEY_PREFIX,
)
from .race_id import parse_race_id

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
    feature_set_hash: str | None = None
    feature_names: tuple[str, ...] | None = None


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


def resolve_dimension(
    entry: Mapping[str, object],
    dimension: str,
    category: str,
    field_size: int | None = None,
    card_max_race_bango: int | None = None,
) -> str | None:
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
        # entry["shusso_tosu"] is unconditionally NULL on every row that
        # passes through the near-miss layer (add-near-miss-features.py's
        # append_features_sql intentionally re-emits it as
        # ``cast(null as bigint)`` to reproduce a trained NAR CatBoost split
        # -- confirmed 100% NULL, JRA and NAR alike, completed or upcoming,
        # against real R2 feature parquets). field_band could therefore never
        # resolve and every field_band-gated cell rule was structurally dead
        # at serve. ``field_size`` -- the count of entries actually being
        # scored for this race, passed in by resolve_variant as
        # ``len(entries)`` -- is the declared-runner count with zero
        # dependency on that (or any other) parquet column, so it is used in
        # preference to the entry's own (poisoned) field whenever supplied.
        #
        # SEMANTIC NOTE: len(entries) is the count of DECLARED runners at
        # predict time (one row per entered horse). The offline WF cell
        # analysis that validated field_band-gated rules (e.g.
        # prior_corner_dirt_smallfield_005) gated on ACTUAL STARTERS --
        # post-race shusso_tosu, which can differ from the declaration count
        # when a horse scratches between entry and post. Near a field_band
        # boundary (<=10 vs 11-13 etc.) a late scratch can therefore make
        # live serving route a race the WF analysis would not have (or the
        # reverse). This is an accepted, unavoidable predict-time reality
        # (the actual starter count is never known before the race runs),
        # not a bug in this fix.
        shusso_tosu = field_size if field_size is not None else entry.get("shusso_tosu")
        if shusso_tosu is None:
            return None
        return derive_field_band(int(float(str(shusso_tosu))))
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
    if dimension == "is_final_race":
        # A single race's own entries can never answer "is this the day's
        # last race" -- that requires knowing every race_bango registered for
        # the same (kaisai_nen, kaisai_tsukihi, keibajo_code) card, which
        # lives outside this one race. ``card_max_race_bango`` is therefore a
        # caller-supplied value (the highest *registered* race_bango on the
        # card, not the highest one that has actually run -- see
        # tmp/kochi-final/cell_design.md for why registered-card-max is the
        # only choice that matches what serving can know before the race
        # runs) rather than something derivable from ``entry`` alone, mirroring
        # how ``field_size`` is threaded in over ``entry["shusso_tosu"]``
        # above. No value supplied (card size unknown/not yet discovered) or
        # an unparseable ``race_id`` both fail closed to None -- the condition
        # simply never matches and routing falls through to the category
        # default, never to a guess.
        if card_max_race_bango is None:
            return None
        race_id = entry.get("race_id")
        if race_id is None:
            return None
        try:
            race_bango = parse_race_id(str(race_id)).race_bango
        except ValueError:
            return None
        race_bango = race_bango.strip()
        if not race_bango.isdigit():
            return None
        return "true" if int(race_bango) == card_max_race_bango else "false"
    raw = entry.get(dimension)
    return str(raw).strip() if raw is not None else None


def all_conditions_match(
    entry: Mapping[str, object],
    conditions: tuple[CellCondition, ...],
    category: str,
    field_size: int | None = None,
    card_max_race_bango: int | None = None,
) -> bool:
    for condition in conditions:
        value = resolve_dimension(
            entry,
            condition.dimension,
            category,
            field_size=field_size,
            card_max_race_bango=card_max_race_bango,
        )
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

    def resolve_variant(
        self,
        category: str,
        entries: Sequence[Mapping[str, object]],
        card_max_race_bango: int | None = None,
    ) -> str:
        if category not in self._routing:
            return VARIANT_SIM
        routing = self._routing[category]
        if not entries:
            return routing.default_variant
        first = entries[0]
        # len(entries) is the count of rows actually being scored for this
        # race -- i.e. the real declared-runner count -- independent of any
        # feature-parquet column. See resolve_dimension's field_band branch
        # for why the entry's own "shusso_tosu" can never be trusted here.
        field_size = len(entries)
        # card_max_race_bango cannot be derived from this race's own entries
        # (see resolve_dimension's is_final_race branch) -- it is an optional
        # caller-supplied value, threaded through the same way field_size is,
        # for whichever caller has whole-card context. Omitting it (the
        # default) fails every is_final_race condition closed to None.
        for rule in routing.rules:
            if all_conditions_match(
                first,
                rule.conditions,
                category,
                field_size=field_size,
                card_max_race_bango=card_max_race_bango,
            ):
                return rule.variant
        return routing.default_variant


def derive_card_max_race_bango_by_card(
    race_ids: Iterable[str],
) -> dict[tuple[str, str, str], int]:
    """Derive each card's registered-max race_bango from a batch already being
    scored together -- the batch-wide analog of ``resolve_variant``'s
    ``field_size = len(entries)`` (zero external dependency, trust only what
    this request is already processing). Correct ONLY when the batch
    genuinely contains every race registered for each card it touches (the
    whole-category ``mode=full`` / whole-category ``mode=rescore`` request
    shapes): a request scoped to a single race must NOT call this, since a
    lone race would trivially compute itself as its own card's only (hence
    "final") race. Such callers -- the per-race rescore and focused-full-race
    request shapes -- receive an explicit ``card_max_race_bango`` from the
    caller instead (sourced from discovery; see
    tmp/kochi-final/cell_design.md), which ``score_races`` prefers over this
    derivation whenever supplied.

    Keyed by ``(kaisai_nen, kaisai_tsukihi, keibajo_code)`` decoded from each
    ``race_id`` (``predict_lib.race_id.parse_race_id``). A malformed or
    non-numeric ``race_bango`` is skipped rather than raising, matching every
    other fail-closed dimension resolver in this module -- a batch containing
    one bad id still yields a usable map for every well-formed one.
    """
    result: dict[tuple[str, str, str], int] = {}
    for race_id in race_ids:
        try:
            parts = parse_race_id(race_id)
        except ValueError:
            continue
        race_bango = parts.race_bango.strip()
        if not race_bango.isdigit():
            continue
        key = (parts.kaisai_nen, parts.kaisai_tsukihi, parts.keibajo_code)
        result[key] = max(result.get(key, 0), int(race_bango))
    return result


def card_max_race_bango_for_race_id(
    race_id: str,
    card_max_race_bango_by_card: Mapping[tuple[str, str, str], int],
) -> int | None:
    """Look up one race's own card's max race_bango from a pre-derived batch map.

    Returns ``None`` (fail-closed) when ``race_id`` is malformed or this
    race's card is absent from the map -- never raises.
    """
    try:
        parts = parse_race_id(race_id)
    except ValueError:
        return None
    key = (parts.kaisai_nen, parts.kaisai_tsukihi, parts.keibajo_code)
    return card_max_race_bango_by_card.get(key)


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
    feature_names: tuple[str, ...] | None = None
    if "feature_names" in spec:
        feature_names = tuple(
            str(name) for name in _as_sequence(spec["feature_names"], "feature_names")
        )
    return VariantSpec(
        model_version=str(spec["model_version"]),
        feature_count=int(str(spec["feature_count"])),
        architecture=str(spec["architecture"]),
        feature_set_hash=(str(spec["feature_set_hash"]) if "feature_set_hash" in spec else None),
        feature_names=feature_names,
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
