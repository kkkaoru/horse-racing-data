"""Canonical CELL schema for cross-source evaluation-metric ingestion.

A CELL is one (category, class, subgroup, racetrack, season, surface) slice of
the finish-position / running-style evaluation surface. This module
replicates — it does not import, since this is a standalone uv project with no
cross-app imports — the exact subgroup-boundary rules from
``apps/finish-position-predict-container/src/predict_lib/subgroup.py`` so the
two stay directly comparable. Any change to those boundaries must be mirrored
here by hand.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

CATEGORY_JRA: Final[str] = "jra"
CATEGORY_NAR: Final[str] = "nar"
CATEGORY_BANEI: Final[str] = "banei"
CATEGORIES: Final[tuple[str, str, str]] = (CATEGORY_JRA, CATEGORY_NAR, CATEGORY_BANEI)

DISTANCE_BAND_SPRINT: Final[str] = "sprint"
DISTANCE_BAND_MILE: Final[str] = "mile"
DISTANCE_BAND_INTERMEDIATE: Final[str] = "intermediate"
DISTANCE_BAND_LONG: Final[str] = "long"
DISTANCE_BAND_EXTENDED: Final[str] = "extended"

FIELD_SIZE_SMALL: Final[str] = "small"
FIELD_SIZE_MEDIUM: Final[str] = "medium"
FIELD_SIZE_LARGE: Final[str] = "large"

SEASON_SPRING: Final[str] = "spring"
SEASON_SUMMER: Final[str] = "summer"
SEASON_AUTUMN: Final[str] = "autumn"
SEASON_WINTER: Final[str] = "winter"

SURFACE_TURF: Final[str] = "turf"
SURFACE_DIRT: Final[str] = "dirt"
SURFACE_OBSTACLE: Final[str] = "obstacle"

# JRA track_code grouping, matching subgroup.py: 10-22 turf, 23-29 dirt, 51-59 obstacle.
TURF_TRACK_CODES: Final[frozenset[str]] = frozenset(
    {"10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22"}
)
DIRT_TRACK_CODES: Final[frozenset[str]] = frozenset({"23", "24", "25", "26", "27", "28", "29"})
OBSTACLE_TRACK_CODES: Final[frozenset[str]] = frozenset(
    {"51", "52", "53", "54", "55", "56", "57", "58", "59"}
)


def classify_distance_band(kyori: int | None) -> str | None:
    if kyori is None:
        return None
    if kyori <= 1400:
        return DISTANCE_BAND_SPRINT
    if kyori <= 1800:
        return DISTANCE_BAND_MILE
    if kyori <= 2200:
        return DISTANCE_BAND_INTERMEDIATE
    if kyori <= 2800:
        return DISTANCE_BAND_LONG
    return DISTANCE_BAND_EXTENDED


def classify_field_size_band(shusso_tosu: int | None) -> str | None:
    if shusso_tosu is None:
        return None
    if shusso_tosu <= 8:
        return FIELD_SIZE_SMALL
    if shusso_tosu <= 14:
        return FIELD_SIZE_MEDIUM
    return FIELD_SIZE_LARGE


def classify_season_band(kaisai_tsukihi: str | None) -> str | None:
    if kaisai_tsukihi is None or len(kaisai_tsukihi) < 2:
        return None
    head = kaisai_tsukihi[:2]
    if not head.isdigit():
        return None
    month = int(head)
    if month in (3, 4, 5):
        return SEASON_SPRING
    if month in (6, 7, 8):
        return SEASON_SUMMER
    if month in (9, 10, 11):
        return SEASON_AUTUMN
    return SEASON_WINTER


def classify_surface(track_code: str | None) -> str | None:
    if track_code is None:
        return None
    code = track_code.strip()
    if not code:
        return None
    if code in TURF_TRACK_CODES:
        return SURFACE_TURF
    if code in OBSTACLE_TRACK_CODES:
        return SURFACE_OBSTACLE
    if code in DIRT_TRACK_CODES:
        return SURFACE_DIRT
    return None


def classify_all(
    *,
    kyori: int | None = None,
    shusso_tosu: int | None = None,
    kaisai_tsukihi: str | None = None,
    track_code: str | None = None,
    class_code: str | None = None,
    keibajo_code: str | None = None,
) -> dict[str, str | None]:
    return {
        "distance_band": classify_distance_band(kyori),
        "field_size_band": classify_field_size_band(shusso_tosu),
        "season_band": classify_season_band(kaisai_tsukihi),
        "surface": classify_surface(track_code),
        "class_code": class_code,
        "venue": keibajo_code,
    }


# Canonical CELL key columns, in the order they should be displayed/grouped by.
CELL_KEY_COLUMNS: Final[tuple[str, ...]] = (
    "category",
    "class_code",
    "venue",
    "distance_band",
    "season_band",
    "surface",
    "field_size_band",
    "track_condition",
)

# Dimension-only columns from a SPECIFIC non-canonical source schema --
# `ingest_local_pg_history.RS_BUCKET_CELL_DIMENSION_COLUMNS` (the running-style
# BUCKET-HISTORY schema) -- that `logging_api.select_headline_metrics` must
# also never treat as a metric candidate, even though (unlike
# `CELL_KEY_COLUMNS` above) they are not part of the canonical cross-source
# CELL taxonomy every ingestion path normalizes toward via `COLUMN_ALIASES`.
# `keibajo_code`/`kyoso_joken_code` from that same source tuple already rename
# to `venue`/`class_code` (`COLUMN_ALIASES` below) and so are already covered
# by `CELL_KEY_COLUMNS`; these six never get renamed/classified and would
# otherwise reach `select_headline_metrics` bare:
#   - `condition_key`, `grade_code`, `race_name`: label/free-text identifiers
#     that are ALL-NULL for this ingestion path. A column that is None/NaN
#     for every row is STILL numeric-dtype in the specific case of a JSON
#     round-trip (`pd.read_json` coerces a column of all JSON `null` to
#     float64 NaN, not object/None -- verified: the equivalent all-null
#     PARQUET round-trip stays `object` dtype and is already safely skipped
#     by `pd.api.types.is_numeric_dtype`), which the same predicate reports
#     as numeric. Left unexcluded, `select_headline_metrics` would compute a
#     fabricated `overall_<column>` weighted mean over pure NaN noise.
#   - `kyori`, `kyoso_shubetsu_code`, `track_code`: raw, un-banded/
#     un-classified dimension values (race distance in meters, race-type
#     code, track code) carried by that same source tuple. `kyori` in
#     particular is frequently POPULATED (a real int, e.g. 1600) rather than
#     null, so it would silently produce an equally meaningless but harder-
#     to-notice `overall_kyori` "headline metric" -- just the race-count-
#     weighted average race distance, not any kind of accuracy signal.
# None of these are canonical cross-source CELL dimensions, so they live in
# their own constant rather than being folded into CELL_KEY_COLUMNS itself
# (which is documented above as "in the order they should be displayed/
# grouped by" -- a free-text field like `race_name` was never display/
# groupby-key shaped in the first place).
NON_METRIC_DIMENSION_COLUMNS: Final[tuple[str, ...]] = (
    "condition_key",
    "grade_code",
    "race_name",
    "kyori",
    "kyoso_shubetsu_code",
    "track_code",
)

# Rank-1..6 base metrics (feedback_eval_rank_1_to_6: evaluation must report
# through rank 6, not just top1-3) plus the two cumulative box metrics.
RANK_METRIC_COLUMNS: Final[tuple[str, ...]] = (
    "top1",
    "place2",
    "place3",
    "place4",
    "place5",
    "place6",
)
AGGREGATE_METRIC_COLUMNS: Final[tuple[str, ...]] = ("top3_box", "fukusho_2p")
METRIC_COLUMNS: Final[tuple[str, ...]] = (
    "race_count",
    *RANK_METRIC_COLUMNS,
    *AGGREGATE_METRIC_COLUMNS,
)
LB95_SUFFIX: Final[str] = "_lb95"
OPTIONAL_METRIC_COLUMNS: Final[tuple[str, ...]] = (
    *(f"{metric}{LB95_SUFFIX}" for metric in METRIC_COLUMNS if metric != "race_count"),
    "ndcg_at_3",
)

# Variant column names seen elsewhere in this repo, mapped to the canonical
# CELL_KEY_COLUMNS names above.
COLUMN_ALIASES: Final[dict[str, str]] = {
    "keibajo_code": "venue",
    "kyori_band": "distance_band",
    "current_baba_condition": "track_condition",
    "baba_condition": "track_condition",
    "kyoso_joken_code": "class_code",
    "class_label": "class_code",
    "season": "season_band",
}


def normalize_cell_columns(mapping: Mapping[str, object]) -> dict[str, object]:
    """Rename known variant cell-dimension column names to the canonical schema.

    Canonical names always win: if both an alias and its canonical form are
    present in the same mapping, the alias's value is discarded in favor of
    the value already stored under the canonical key.
    """
    result: dict[str, object] = dict(mapping)
    for alias, canonical in COLUMN_ALIASES.items():
        if alias in result:
            value = result.pop(alias)
            if canonical not in result:
                result[canonical] = value
    return result
