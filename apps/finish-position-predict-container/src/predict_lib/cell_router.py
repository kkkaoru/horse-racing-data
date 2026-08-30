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
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Final

from .model_meta import (
    METADATA_FILE_NAME,
    R2_KEY_PREFIX,
)
from .race_id import RaceIdParts, parse_race_id

CONFIG_FILE_NAME: Final[str] = "cell_routing.json"
NAMED_RACE_CELLS_FILE_NAME: Final[str] = "named_race_cells.json"

VARIANT_SIM: Final[str] = "sim"
VARIANT_BASE: Final[str] = "base"
LOCK1_RERANK_REST_ROUTING_MODE: Final[str] = "jra_lock1_rerank_rest"
NAMED_RACE_RERANK_VARIANT_SUFFIX: Final[str] = "_rerank"
NAMED_RACE_PRERACE_DIMENSIONS: Final[frozenset[str]] = frozenset({"kyori", "month"})


@dataclass(frozen=True)
class CellCondition:
    dimension: str
    values: frozenset[str]


@dataclass(frozen=True)
class CellRouteRule:
    conditions: tuple[CellCondition, ...]
    variant: str
    effective_after: str | None = None


@dataclass(frozen=True)
class VariantSpec:
    model_version: str
    feature_count: int
    architecture: str
    feature_set_hash: str | None = None
    feature_names: tuple[str, ...] | None = None
    routing_mode: str = "direct"
    base_variant: str | None = None
    minimum_candidate_margin: float | None = None
    minimum_candidate_top_z: float | None = None
    maximum_candidate_v2_rank: int | None = None
    consensus_variants: tuple[str, ...] = ()
    consensus_required_votes: int | None = None
    rerank_variant: str | None = None


@dataclass(frozen=True)
class NamedRacePreraceWhen:
    kyori: frozenset[str] | None = None
    month: frozenset[str] | None = None


@dataclass(frozen=True)
class NamedRacePreraceRoute:
    when: NamedRacePreraceWhen
    model_version: str | None = None
    variant: str | None = None


@dataclass(frozen=True)
class NamedRacePreraceRouter:
    routes: tuple[NamedRacePreraceRoute, ...]


@dataclass(frozen=True)
class NamedRaceCell:
    variant: str
    venue: str
    race_name_token: str
    base_variant: str
    model_version: str | None = None
    feature_count: int | None = None
    architecture: str | None = None
    effective_after: str | None = None
    rerank_feature_count: int | None = None
    rerank_model_version: str | None = None
    routing_mode: str | None = None
    prerace_router: NamedRacePreraceRouter | None = None


@dataclass(frozen=True)
class CategoryRouting:
    default_variant: str
    variants: dict[str, VariantSpec]
    rules: tuple[CellRouteRule, ...]
    named_race_index: Mapping[tuple[str, str], NamedRaceCell] = field(default_factory=dict)

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


# JRA track_code (トラックコード): 10-22 are all turf course configurations,
# 23-29 are all dirt. A track_code "starts with 2" prefix check (the previous
# implementation here) incorrectly bucketed 20/21/22 as dirt -- those three
# codes are turf-course configurations reserved for specific long-distance
# graded races run over an extended/outer turf layout (e.g. track_code=20 is
# 天皇賞(春) at Kyoto, 3200m turf; track_code=21 is スポーツニッポン賞
# ステイヤーズステークス at Nakayama, 3600m turf -- confirmed against the
# local PG mirror's actual race names/babajotai columns, 2026-07-17
# bug-regression-test audit item K: every observed row has a populated
# babajotai_code_shiba and a placeholder babajotai_code_dirt, the turf-race
# signature). track_code >= 30 (steeplechase/jump courses, e.g. 51/52) falls
# through to "other", matching this function's pre-existing behaviour and
# subgroup_diagnostics.get_surface_label's JRA_TURF_CODES/JRA_DIRT_CODES sets
# (apps/pc-keiba-viewer/src/scripts/learning/subgroup_diagnostics.py) exactly
# -- see test_cell_router.py's cross-package parity test for the two
# implementations being kept in sync going forward.
_JRA_TURF_TRACK_CODES: Final[frozenset[str]] = frozenset(str(code) for code in range(10, 23))
_JRA_DIRT_TRACK_CODES: Final[frozenset[str]] = frozenset(str(code) for code in range(23, 30))

# NAR canonical distance bands (meters). Sprint is exclusive of 1400 so that
# 1400-1500 is a first-class extended_sprint cell rather than a leftover hole.
CANONICAL_DISTANCE_BAND_SPRINT: Final[str] = "sprint"
CANONICAL_DISTANCE_BAND_EXTENDED_SPRINT: Final[str] = "extended_sprint"
CANONICAL_DISTANCE_BAND_MILE: Final[str] = "mile"
CANONICAL_DISTANCE_BAND_INTERMEDIATE: Final[str] = "intermediate"
CANONICAL_DISTANCE_BAND_LONG: Final[str] = "long"
CANONICAL_DISTANCE_BAND_EXTENDED: Final[str] = "extended"
CANONICAL_SPRINT_MAX_EXCLUSIVE_METERS: Final[int] = 1400
CANONICAL_EXTENDED_SPRINT_MAX_METERS: Final[int] = 1500
CANONICAL_MILE_MAX_METERS: Final[int] = 1800
CANONICAL_INTERMEDIATE_MAX_METERS: Final[int] = 2200
CANONICAL_LONG_MAX_METERS: Final[int] = 2800
_RACE_NAME_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"[\w\u30fc\u30fb\uff0d-]+(?:杯|賞|記念|ステークス|カップ)",
    re.UNICODE,
)
# U+30FC (ー) is a katakana vowel mark, not a dash; keep it so ステークス matches.
_DASH_MARKS: Final[frozenset[str]] = frozenset({"\uff0d", "\u2015", "\u2010"})
_JOCKEYS_CUP_TOKEN: Final[str] = "ジョッキーズカップ"
_FULLWIDTH_OFFSET: Final[int] = 0xFEE0
RACE_NAME_FIELD_NAMES: Final[tuple[str, ...]] = (
    "kyosomei_hondai",
    "kyosomei_norm",
    "kyosomei_fukudai",
    "kyosomei_kakkonai",
)
_RACE_NAME_TABLE_BY_SOURCE: Final[Mapping[str, str]] = MappingProxyType(
    {
        "jra": "jvd_ra",
        "nar": "nvd_ra",
        "ban-ei": "nvd_ra",
    }
)
_BLANK_RACE_NAME_TOKENS: Final[frozenset[str]] = frozenset({"", "nan", "none", "<na>"})


def derive_surface(track_code: str, category: str) -> str:
    if category != "jra":
        return "dirt"
    if track_code in _JRA_TURF_TRACK_CODES:
        return "turf"
    if track_code in _JRA_DIRT_TRACK_CODES:
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


def derive_canonical_distance_band(kyori: int) -> str:
    if kyori < CANONICAL_SPRINT_MAX_EXCLUSIVE_METERS:
        return CANONICAL_DISTANCE_BAND_SPRINT
    if kyori <= CANONICAL_EXTENDED_SPRINT_MAX_METERS:
        return CANONICAL_DISTANCE_BAND_EXTENDED_SPRINT
    if kyori <= CANONICAL_MILE_MAX_METERS:
        return CANONICAL_DISTANCE_BAND_MILE
    if kyori <= CANONICAL_INTERMEDIATE_MAX_METERS:
        return CANONICAL_DISTANCE_BAND_INTERMEDIATE
    if kyori <= CANONICAL_LONG_MAX_METERS:
        return CANONICAL_DISTANCE_BAND_LONG
    return CANONICAL_DISTANCE_BAND_EXTENDED


def derive_canonical_field_size_band(field_size: int) -> str:
    if field_size <= 8:
        return "small"
    if field_size <= 14:
        return "medium"
    return "large"


def derive_season(month: int) -> str:
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    if month in {9, 10, 11}:
        return "autumn"
    return "winter"


def _calendar_month_token(entry: Mapping[str, object]) -> str | None:
    tsukihi = entry.get("kaisai_tsukihi")
    if tsukihi is not None:
        month_str = str(tsukihi).strip()[:2]
        if month_str.isdigit():
            return month_str.zfill(2)
    race_date = entry.get("race_date")
    if race_date is not None:
        date_str = str(race_date).strip().replace("-", "")
        if len(date_str) >= 6 and date_str[4:6].isdigit():
            return date_str[4:6]
    race_id = entry.get("race_id")
    if race_id is not None:
        parts = str(race_id).split(":")
        if len(parts) >= 3:
            tsukihi_part = parts[2]
            if len(tsukihi_part) >= 2 and tsukihi_part[:2].isdigit():
                return tsukihi_part[:2]
    return None


def _kyori_token(value: object) -> str | None:
    try:
        return str(int(float(str(value).strip())))
    except ValueError:
        return None


def _to_half_width_alnum(value: str) -> str:
    converted: list[str] = []
    for char in value:
        code = ord(char)
        if 0xFF01 <= code <= 0xFF5E:
            converted.append(chr(code - _FULLWIDTH_OFFSET))
        elif char in _DASH_MARKS:
            converted.append("-")
        else:
            converted.append(char)
    return " ".join("".join(converted).split())


def _last_race_name_token(value: str) -> str | None:
    matches = _RACE_NAME_TOKEN_PATTERN.findall(value)
    if not matches:
        return None
    return matches[-1]


def derive_race_name_token(entry: Mapping[str, object]) -> str | None:
    hondai_raw = entry.get("kyosomei_hondai")
    if hondai_raw is None:
        hondai_raw = entry.get("kyosomei_norm")
    fukudai_raw = entry.get("kyosomei_fukudai")
    kakkonai_raw = entry.get("kyosomei_kakkonai")
    hondai = _to_half_width_alnum("" if hondai_raw is None else str(hondai_raw).strip())
    fukudai = _to_half_width_alnum("" if fukudai_raw is None else str(fukudai_raw).strip())
    kakkonai = _to_half_width_alnum("" if kakkonai_raw is None else str(kakkonai_raw).strip())
    subtitle = f"{fukudai} {kakkonai}".strip()
    combined = f"{hondai} {subtitle}".strip()
    if _JOCKEYS_CUP_TOKEN in combined:
        return _JOCKEYS_CUP_TOKEN
    subtitle_token = _last_race_name_token(subtitle)
    if subtitle_token is not None:
        return subtitle_token
    return _last_race_name_token(hondai)


def is_blank_race_name_value(value: object) -> bool:
    """Return True when a kyosomei field cannot contribute a race-name token."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return str(value).strip().lower() in _BLANK_RACE_NAME_TOKENS


def entry_has_race_name(entry: Mapping[str, object]) -> bool:
    """Return True when any kyosomei field on ``entry`` is usable for routing."""
    return any(not is_blank_race_name_value(entry.get(field)) for field in RACE_NAME_FIELD_NAMES)


def overlay_race_name_onto_entry(
    entry: Mapping[str, object],
    race_name: Mapping[str, object] | None,
) -> dict[str, object]:
    """Copy blank kyosomei fields from ``race_name`` onto a shallow entry copy.

    Existing non-blank values win so a parquet that already carries official
    names is never overwritten. Blank / NaN parquet cells are treated as
    missing so a catalog overlay can still attach ``jvd_ra`` / ``nvd_ra``
    names before ``resolve_variant``.
    """
    overlaid = dict(entry)
    if race_name is None:
        return overlaid
    for field_name in RACE_NAME_FIELD_NAMES:
        if not is_blank_race_name_value(overlaid.get(field_name)):
            continue
        incoming = race_name.get(field_name)
        if is_blank_race_name_value(incoming):
            continue
        overlaid[field_name] = incoming
    return overlaid


def overlay_race_names_on_races(
    races: Mapping[str, Sequence[Mapping[str, object]]],
    race_names_by_race_id: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, list[dict[str, object]]]:
    """Attach per-race kyosomei fields onto every entry before cell routing."""
    names = {} if race_names_by_race_id is None else race_names_by_race_id
    return {
        race_id: [overlay_race_name_onto_entry(entry, names.get(race_id)) for entry in entries]
        for race_id, entries in races.items()
    }


def build_race_name_catalog_query(parts: RaceIdParts) -> tuple[str, tuple[object, ...]]:
    """Return ``(sql, params)`` that reads official names from ``jvd_ra``/``nvd_ra``.

    The table name is taken from a fixed source map so caller-supplied race ids
    cannot inject SQL.
    """
    table = _RACE_NAME_TABLE_BY_SOURCE.get(parts.source)
    if table is None:
        message = f"unsupported race_id source for race-name lookup: {parts.source}"
        raise ValueError(message)
    sql = (
        f"select kyosomei_hondai, kyosomei_fukudai, kyosomei_kakkonai "
        f"from pg.{table} "
        "where kaisai_nen = ? and kaisai_tsukihi = ? "
        "and keibajo_code = ? and race_bango = ? "
        "limit 1"
    )
    return (
        sql,
        (parts.kaisai_nen, parts.kaisai_tsukihi, parts.keibajo_code, parts.race_bango),
    )


def derive_class(grade_code: str, kyoso_joken_code: str = "") -> str:
    cleaned_grade = grade_code.strip()
    if cleaned_grade:
        return cleaned_grade
    cleaned_condition = kyoso_joken_code.strip()
    if cleaned_condition:
        return f"joken-{cleaned_condition}"
    return "unknown"


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
    if dimension == "canonical_distance_band":
        kyori = entry.get("kyori")
        if kyori is None:
            return None
        return derive_canonical_distance_band(int(float(str(kyori))))
    if dimension == "canonical_field_size_band":
        resolved_field_size = field_size if field_size is not None else entry.get("shusso_tosu")
        if resolved_field_size is None:
            return None
        return derive_canonical_field_size_band(int(float(str(resolved_field_size))))
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
        month_token = _calendar_month_token(entry)
        if month_token is None:
            return None
        return derive_season(int(month_token))
    if dimension == "month":
        return _calendar_month_token(entry)
    if dimension == "kyori":
        raw_kyori = entry.get("kyori")
        if raw_kyori is None:
            return None
        return _kyori_token(raw_kyori)
    if dimension == "race_name_token":
        return derive_race_name_token(entry)
    if dimension == "class":
        grade_code = entry.get("grade_code")
        condition_code = entry.get("kyoso_joken_code")
        if grade_code is None and condition_code is None:
            return None
        return derive_class(
            "" if grade_code is None else str(grade_code),
            "" if condition_code is None else str(condition_code),
        )
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


def rule_is_effective(entry: Mapping[str, object], effective_after: str | None) -> bool:
    if effective_after is None:
        return True
    threshold = effective_after.replace("-", "")
    if len(threshold) != 8 or not threshold.isdigit():
        return False
    year = entry.get("kaisai_nen")
    month_day = entry.get("kaisai_tsukihi")
    if year is not None and month_day is not None:
        date = f"{str(year).strip():0>4}{str(month_day).strip():0>4}"
        return len(date) == 8 and date.isdigit() and date > threshold
    race_id = entry.get("race_id")
    if race_id is None:
        return False
    try:
        parts = parse_race_id(str(race_id))
    except ValueError:
        return False
    date = f"{parts.kaisai_nen}{parts.kaisai_tsukihi}"
    return len(date) == 8 and date.isdigit() and date > threshold


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
        named_variant = _lookup_named_race_variant(
            entry=first, routing=routing, category=category
        )
        if named_variant is not None:
            return named_variant
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
            if not rule_is_effective(first, rule.effective_after):
                continue
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
    return CellRouteRule(
        conditions=conditions,
        variant=str(rule["variant"]),
        effective_after=(str(rule["effective_after"]) if "effective_after" in rule else None),
    )


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
        routing_mode=str(spec.get("routing_mode", "direct")),
        base_variant=(str(spec["base_variant"]) if "base_variant" in spec else None),
        minimum_candidate_margin=(
            float(str(spec["minimum_candidate_margin"]))
            if "minimum_candidate_margin" in spec
            else None
        ),
        minimum_candidate_top_z=(
            float(str(spec["minimum_candidate_top_z"]))
            if "minimum_candidate_top_z" in spec
            else None
        ),
        maximum_candidate_v2_rank=(
            int(str(spec["maximum_candidate_v2_rank"]))
            if "maximum_candidate_v2_rank" in spec
            else None
        ),
        consensus_variants=(
            tuple(
                str(name) for name in _as_sequence(spec["consensus_variants"], "consensus_variants")
            )
            if "consensus_variants" in spec
            else ()
        ),
        consensus_required_votes=(
            int(str(spec["consensus_required_votes"]))
            if "consensus_required_votes" in spec
            else None
        ),
        rerank_variant=(str(spec["rerank_variant"]) if "rerank_variant" in spec else None),
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


def _required_str(*, payload: Mapping[str, object], field: str, source: str) -> str:
    if field not in payload:
        raise ValueError(f"{source}: '{field}' is required")
    value = str(payload[field]).strip()
    if value == "":
        raise ValueError(f"{source}: '{field}' must be a non-empty string")
    return value


def _optional_str(*, payload: Mapping[str, object], field: str) -> str | None:
    if field not in payload:
        return None
    value = str(payload[field]).strip()
    return value if value != "" else None


def _optional_int(*, payload: Mapping[str, object], field: str) -> int | None:
    if field not in payload:
        return None
    return int(str(payload[field]))


def _normalize_prerace_kyori(value: object) -> str:
    token = _kyori_token(value)
    if token is None:
        raise ValueError("named_race_cells.json: prerace_router.when.kyori must be numeric")
    return token


def _normalize_prerace_month(value: object) -> str:
    text = str(value).strip()
    if not text.isdigit():
        raise ValueError("named_race_cells.json: prerace_router.when.month must be numeric")
    return text.zfill(2)


def _optional_prerace_value_set(
    payload: Mapping[str, object], *, field: str
) -> frozenset[str] | None:
    if field not in payload:
        return None
    raw = payload[field]
    values = raw if isinstance(raw, list) else [raw]
    if not values:
        raise ValueError(f"named_race_cells.json: prerace_router.when.{field} must be non-empty")
    if field == "kyori":
        return frozenset(_normalize_prerace_kyori(item) for item in values)
    return frozenset(_normalize_prerace_month(item) for item in values)


def _parse_named_race_prerace_when(value: object) -> NamedRacePreraceWhen:
    payload = _as_mapping(value, "prerace_router.when")
    extra = set(payload) - NAMED_RACE_PRERACE_DIMENSIONS
    if extra:
        raise ValueError(
            "named_race_cells.json: prerace_router.when only allows kyori and month, "
            f"got {sorted(extra)}"
        )
    kyori = _optional_prerace_value_set(payload, field="kyori")
    month = _optional_prerace_value_set(payload, field="month")
    if kyori is None and month is None:
        raise ValueError("named_race_cells.json: prerace_router.when requires kyori or month")
    return NamedRacePreraceWhen(kyori=kyori, month=month)


def _parse_named_race_prerace_route(value: object) -> NamedRacePreraceRoute:
    payload = _as_mapping(value, "prerace_router.route")
    extra = set(payload) - {"when", "model_version", "variant"}
    if extra:
        raise ValueError(
            "named_race_cells.json: prerace_router route unknown keys "
            f"{sorted(extra)}"
        )
    if "when" not in payload:
        raise ValueError("named_race_cells.json: prerace_router route requires when")
    model_version = _optional_str(payload=payload, field="model_version")
    variant = _optional_str(payload=payload, field="variant")
    if (model_version is None) != (variant is None):
        raise ValueError(
            "named_race_cells.json: prerace_router route requires both "
            "model_version and variant, or neither"
        )
    return NamedRacePreraceRoute(
        when=_parse_named_race_prerace_when(payload["when"]),
        model_version=model_version,
        variant=variant,
    )


def _parse_named_race_prerace_router(value: object) -> NamedRacePreraceRouter:
    payload = _as_mapping(value, "prerace_router")
    extra = set(payload) - {"routes"}
    if extra:
        raise ValueError(
            f"named_race_cells.json: prerace_router unknown keys {sorted(extra)}"
        )
    if "routes" not in payload:
        raise ValueError("named_race_cells.json: prerace_router requires routes")
    routes = tuple(
        _parse_named_race_prerace_route(item)
        for item in _as_sequence(payload["routes"], "prerace_router.routes")
    )
    if not routes:
        raise ValueError("named_race_cells.json: prerace_router.routes must be non-empty")
    return NamedRacePreraceRouter(routes=routes)


def _parse_named_race_cell(value: object) -> NamedRaceCell:
    cell = _as_mapping(value, "named_race_cell")
    source = "named_race_cells.json"
    prerace_payload = cell.get("prerace_router")
    return NamedRaceCell(
        variant=_required_str(payload=cell, field="variant", source=source),
        venue=_required_str(payload=cell, field="venue", source=source),
        race_name_token=_required_str(payload=cell, field="race_name_token", source=source),
        base_variant=_required_str(payload=cell, field="base_variant", source=source),
        model_version=_optional_str(payload=cell, field="model_version"),
        feature_count=_optional_int(payload=cell, field="feature_count"),
        architecture=_optional_str(payload=cell, field="architecture"),
        effective_after=_optional_str(payload=cell, field="effective_after"),
        rerank_feature_count=_optional_int(payload=cell, field="rerank_feature_count"),
        rerank_model_version=_optional_str(payload=cell, field="rerank_model_version"),
        routing_mode=_optional_str(payload=cell, field="routing_mode"),
        prerace_router=(
            None
            if prerace_payload is None
            else _parse_named_race_prerace_router(prerace_payload)
        ),
    )


def _cells_for_named_race_category(*, category: str, value: object) -> tuple[NamedRaceCell, ...]:
    return tuple(_parse_named_race_cell(item) for item in _as_sequence(value, category))


def load_named_race_cells(path: Path) -> dict[str, tuple[NamedRaceCell, ...]]:
    if not path.exists():
        return {}
    payload = _as_mapping(json.loads(path.read_text(encoding="utf-8")), "named_race_cells")
    return {
        category: _cells_for_named_race_category(category=category, value=value)
        for category, value in payload.items()
    }


def named_race_rerank_variant_name(variant: str) -> str:
    return f"{variant}{NAMED_RACE_RERANK_VARIANT_SUFFIX}"


def _named_race_lock1_routing_mode(cell: NamedRaceCell) -> str:
    if cell.routing_mode is not None:
        return cell.routing_mode
    if cell.rerank_model_version is None:
        return "direct"
    return LOCK1_RERANK_REST_ROUTING_MODE


def _inherit_named_race_variant_spec(*, cell: NamedRaceCell, base: VariantSpec) -> VariantSpec:
    rerank_variant = (
        None if cell.rerank_model_version is None else named_race_rerank_variant_name(cell.variant)
    )
    return VariantSpec(
        model_version=base.model_version if cell.model_version is None else cell.model_version,
        feature_count=base.feature_count if cell.feature_count is None else cell.feature_count,
        architecture=base.architecture if cell.architecture is None else cell.architecture,
        routing_mode=_named_race_lock1_routing_mode(cell),
        base_variant=cell.base_variant,
        rerank_variant=rerank_variant,
    )


def _named_race_rerank_variant_spec(
    *,
    cell: NamedRaceCell,
    base: VariantSpec,
    rerank_model_version: str,
) -> VariantSpec:
    feature_count = (
        base.feature_count if cell.rerank_feature_count is None else cell.rerank_feature_count
    )
    return VariantSpec(
        model_version=rerank_model_version,
        feature_count=feature_count,
        architecture=base.architecture if cell.architecture is None else cell.architecture,
        routing_mode="direct",
        base_variant=cell.base_variant,
    )


def _named_race_lock_variant_spec(
    *,
    cell: NamedRaceCell,
    base: VariantSpec,
    model_version: str,
) -> VariantSpec:
    rerank_variant = (
        None if cell.rerank_model_version is None else named_race_rerank_variant_name(cell.variant)
    )
    return VariantSpec(
        model_version=model_version,
        feature_count=base.feature_count if cell.feature_count is None else cell.feature_count,
        architecture=base.architecture if cell.architecture is None else cell.architecture,
        routing_mode=_named_race_lock1_routing_mode(cell),
        base_variant=cell.base_variant,
        rerank_variant=rerank_variant,
    )


def _register_named_race_prerace_lock_variants(
    *,
    cell: NamedRaceCell,
    base: VariantSpec,
    variants: dict[str, VariantSpec],
) -> None:
    if cell.prerace_router is None:
        return
    for route in cell.prerace_router.routes:
        if route.variant is None or route.model_version is None:
            continue
        if route.variant == cell.variant:
            continue
        existing = variants.get(route.variant)
        if existing is None:
            variants[route.variant] = _named_race_lock_variant_spec(
                cell=cell,
                base=base,
                model_version=route.model_version,
            )
            continue
        if existing.model_version != route.model_version:
            raise ValueError(
                f"named_race_cells.json: variant '{route.variant}' already exists"
            )


def apply_named_race_cells(
    *,
    routing: CategoryRouting,
    cells: tuple[NamedRaceCell, ...],
) -> CategoryRouting:
    variants = dict(routing.variants)
    index: dict[tuple[str, str], NamedRaceCell] = {}
    for cell in cells:
        key = (cell.venue, cell.race_name_token)
        if key in index:
            raise ValueError(
                "named_race_cells.json: duplicate cell for venue="
                f"{cell.venue} token={cell.race_name_token}"
            )
        if cell.variant in variants:
            raise ValueError(f"named_race_cells.json: variant '{cell.variant}' already exists")
        base = variants.get(cell.base_variant)
        if base is None:
            raise ValueError(
                f"named_race_cells.json: cell '{cell.variant}' references missing "
                f"base_variant '{cell.base_variant}'"
            )
        routing_mode = _named_race_lock1_routing_mode(cell)
        if routing_mode == LOCK1_RERANK_REST_ROUTING_MODE and cell.rerank_model_version is None:
            raise ValueError(
                f"named_race_cells.json: cell '{cell.variant}' routing_mode "
                f"'{LOCK1_RERANK_REST_ROUTING_MODE}' requires rerank_model_version"
            )
        if cell.rerank_model_version is not None:
            rerank_name = named_race_rerank_variant_name(cell.variant)
            if rerank_name in variants:
                raise ValueError(f"named_race_cells.json: variant '{rerank_name}' already exists")
            variants[rerank_name] = _named_race_rerank_variant_spec(
                cell=cell,
                base=base,
                rerank_model_version=cell.rerank_model_version,
            )
        _register_named_race_prerace_lock_variants(cell=cell, base=base, variants=variants)
        if cell.variant in variants:
            raise ValueError(f"named_race_cells.json: variant '{cell.variant}' already exists")
        variants[cell.variant] = _inherit_named_race_variant_spec(cell=cell, base=base)
        index[key] = cell
    return CategoryRouting(
        default_variant=routing.default_variant,
        variants=variants,
        rules=routing.rules,
        named_race_index=index,
    )


def _attach_named_race_cells(
    *,
    routing: dict[str, CategoryRouting],
    named_cells: Mapping[str, tuple[NamedRaceCell, ...]],
) -> dict[str, CategoryRouting]:
    return {
        category: apply_named_race_cells(
            routing=category_routing,
            cells=named_cells.get(category, ()),
        )
        for category, category_routing in routing.items()
    }


def _prerace_when_matches(
    *,
    entry: Mapping[str, object],
    when: NamedRacePreraceWhen,
    category: str,
) -> bool:
    if when.month is not None:
        month = resolve_dimension(entry, "month", category)
        if month is None or month not in when.month:
            return False
    if when.kyori is not None:
        kyori = resolve_dimension(entry, "kyori", category)
        if kyori is None or kyori not in when.kyori:
            return False
    return True


def _named_race_prerace_variant(
    *,
    cell: NamedRaceCell,
    entry: Mapping[str, object],
    category: str,
) -> str:
    router = cell.prerace_router
    if router is None:
        return cell.variant
    for route in router.routes:
        if not _prerace_when_matches(entry=entry, when=route.when, category=category):
            continue
        if route.variant is None:
            return cell.variant
        return route.variant
    return cell.variant


def _lookup_named_race_variant(
    *,
    entry: Mapping[str, object],
    routing: CategoryRouting,
    category: str,
) -> str | None:
    venue = resolve_dimension(entry, "venue", category)
    token = derive_race_name_token(entry)
    if venue is None or token is None:
        return None
    cell = routing.named_race_index.get((venue, token))
    if cell is None:
        return None
    if not rule_is_effective(entry, cell.effective_after):
        return None
    return _named_race_prerace_variant(cell=cell, entry=entry, category=category)


def load_cell_router(config_path: Path | None = None) -> CellRouter:
    path = config_path if config_path is not None else Path(__file__).parent / CONFIG_FILE_NAME
    if not path.exists():
        return CellRouter({})
    payload = _as_mapping(json.loads(path.read_text(encoding="utf-8")), "root")
    routing = {
        category: _parse_category_routing(_as_mapping(entry, category))
        for category, entry in payload.items()
    }
    named_cells = load_named_race_cells(path.with_name(NAMED_RACE_CELLS_FILE_NAME))
    return CellRouter(_attach_named_race_cells(routing=routing, named_cells=named_cells))


def build_base_model_r2_key(category: str, base_model_version: str, file_name: str) -> str:
    return f"{R2_KEY_PREFIX}/{category}/{base_model_version}/{file_name}"


def build_base_metadata_r2_key(category: str, base_model_version: str) -> str:
    return build_base_model_r2_key(category, base_model_version, METADATA_FILE_NAME)
