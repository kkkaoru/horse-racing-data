"""Shared feature-selection policy for finish-position and running-style models."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Final, Literal, Sequence

PredictionTarget = Literal["finish_position", "running_style"]

FINISH_POSITION_META_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source",
        "race_date",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
        "umaban",
        "category",
        "race_id",
        "race_year",
        "feature_schema_version",
        "target_race_id",
        "current_race_grade_letter",
    }
)

SHARED_LABEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "finish_position",
        "finish_norm",
        "target_corner_1_norm",
        "target_corner_2_norm",
        "target_corner_3_norm",
        "target_corner_4_norm",
        "target_running_style_class",
    }
)

RUNNING_STYLE_META_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source",
        "race_date",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
        "bamei",
        "umaban",
        "category",
        "race_id",
        "race_year",
        "feature_schema_version",
    }
)

RUNNING_STYLE_LEAK_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "rs_p_nige",
        "rs_p_senkou",
        "rs_p_sashi",
        "rs_p_oikomi",
    }
)

RUNNING_STYLE_CELL_DERIVED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "__rs_cell_category",
        "__rs_cell_surface",
        "__rs_cell_distance_band",
        "__rs_cell_season",
        "__rs_cell_class",
        "__rs_cell_venue",
        "__rs_cell_subgroup",
    }
)

TARGET_EXCLUDED_COLUMNS: Final[dict[PredictionTarget, frozenset[str]]] = {
    "finish_position": FINISH_POSITION_META_COLUMNS | SHARED_LABEL_COLUMNS,
    "running_style": (
        RUNNING_STYLE_META_COLUMNS
        | SHARED_LABEL_COLUMNS
        | RUNNING_STYLE_LEAK_COLUMNS
        | RUNNING_STYLE_CELL_DERIVED_COLUMNS
    ),
}


MARKET_DEVIATION_FEATURES: Final[frozenset[str]] = frozenset(
    {"exotic_umaren_p2_rank_residual", "exotic_sanrenpuku_p3_rank_residual"}
)
"""The only odds-derived finish-position inputs allowed: 馬連 / 3連複 deviation
from the market's own ordering (see ``add_exotic_market_deviation_features``)."""

BANNED_MARKET_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"tansho|ninki|popular|fav(_|ou?rite)|fukusho|wide|umatan|sanrentan|wakuren"
    r"|odds|market|^exotic_|form_market_edge"
)
"""Any other odds / popularity derived column. Win (単勝) odds and popularity are
banned outright so the ranking cannot follow the win market; raw exotic implied
probabilities are banned because they re-encode the same popularity order."""


CATEGORY_DISALLOWED_FEATURES: Final[dict[str, frozenset[str]]] = {"jra": MARKET_DEVIATION_FEATURES}
"""Per-category inputs served models must not use. JRA serves without the exotic
residuals: they are trained on final odds but served on live odds, and the serve-time
values hurt JRA accuracy (the container rejects them for JRA models)."""


def category_disallowed_features(category: str) -> frozenset[str]:
    """Columns a ``category`` model must never be trained on (empty when none)."""
    return CATEGORY_DISALLOWED_FEATURES.get(category, frozenset())


def is_banned_market_feature(column: str) -> bool:
    """True when ``column`` is an odds/popularity input finish models must not use."""
    return column not in MARKET_DEVIATION_FEATURES and bool(BANNED_MARKET_PATTERN.search(column))


@dataclass(frozen=True)
class FeatureSelectionSpec:
    prediction_target: PredictionTarget
    feature_names: tuple[str, ...]
    feature_set_hash: str


def normalize_feature_names(feature_names: Sequence[str]) -> list[str]:
    """Return a stable, duplicate-free feature list for hashing and manifests."""
    return sorted({name.strip() for name in feature_names if name.strip()})


def compute_feature_set_hash(feature_names: Sequence[str]) -> str:
    """Order-independent SHA-256 hash shared by local search and cell adoption."""
    canonical = json.dumps(normalize_feature_names(feature_names), separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def resolve_feature_columns_for_target(
    df_columns: Sequence[str], prediction_target: PredictionTarget
) -> list[str]:
    """Resolve model input columns using the same exclusion policy per target."""
    excluded = TARGET_EXCLUDED_COLUMNS[prediction_target]
    # Win odds / popularity are banned for every target: running-style
    # probabilities (rs_p_*) feed every finish-position model.
    return [
        column
        for column in df_columns
        if column not in excluded and not is_banned_market_feature(column)
    ]


def build_feature_selection_spec(
    prediction_target: PredictionTarget, feature_names: Sequence[str]
) -> FeatureSelectionSpec:
    normalized = tuple(normalize_feature_names(feature_names))
    return FeatureSelectionSpec(
        prediction_target=prediction_target,
        feature_names=normalized,
        feature_set_hash=compute_feature_set_hash(normalized),
    )
