"""Subgroup classification for finish-position predictions.

Pure functions that map race metadata to subgroup dimension values.
Used by the Container serving path (predict_upcoming.py) and evaluation
scripts (aggregate_bucket_eval_duckdb.py, serve_accuracy_report.py).

The surface mapping follows the JRA ``track_code`` grouping used by the
finish-position training scripts (``train_finish_position_lgbm_stacking.py``):
codes ``10``-``22`` are turf, ``23``-``29`` are dirt, and ``51``-``59`` are
obstacle (障害) races. Keeping this module in sync with the training-side
prefixes ensures serve-path subgroup labels match the labels seen at fit time.
"""

from __future__ import annotations

from typing import Final

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

# JRA track_code grouping, matching the training-side prefixes in
# train_finish_position_lgbm_stacking.py: 10-22 turf, 23-29 dirt, 51-59 obstacle.
TURF_TRACK_CODES: Final[frozenset[str]] = frozenset(
    {"10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22"}
)
DIRT_TRACK_CODES: Final[frozenset[str]] = frozenset(
    {"23", "24", "25", "26", "27", "28", "29"}
)
OBSTACLE_TRACK_CODES: Final[frozenset[str]] = frozenset(
    {"51", "52", "53", "54", "55", "56", "57", "58", "59"}
)

SUBGROUP_DIMENSIONS: Final[tuple[str, ...]] = (
    "distance_band",
    "field_size_band",
    "season_band",
    "surface",
    "class_code",
    "venue",
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
