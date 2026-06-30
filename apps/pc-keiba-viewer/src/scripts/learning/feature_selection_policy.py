"""Shared feature-selection policy for finish-position and running-style models."""

from __future__ import annotations

import hashlib
import json
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


@dataclass(frozen=True)
class FeatureSelectionSpec:
    prediction_target: PredictionTarget
    feature_names: tuple[str, ...]
    feature_set_hash: str


def normalize_feature_names(feature_names: Sequence[str]) -> list[str]:
    """Return a stable, duplicate-free feature list for hashing and manifests."""
    return sorted({name for name in feature_names if name})


def compute_feature_set_hash(feature_names: Sequence[str]) -> str:
    """Order-independent SHA-256 hash shared by local search and cell adoption."""
    canonical = json.dumps(normalize_feature_names(feature_names), separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def resolve_feature_columns_for_target(
    df_columns: Sequence[str], prediction_target: PredictionTarget
) -> list[str]:
    """Resolve model input columns using the same exclusion policy per target."""
    excluded = TARGET_EXCLUDED_COLUMNS[prediction_target]
    return [column for column in df_columns if column not in excluded]


def build_feature_selection_spec(
    prediction_target: PredictionTarget, feature_names: Sequence[str]
) -> FeatureSelectionSpec:
    normalized = tuple(normalize_feature_names(feature_names))
    return FeatureSelectionSpec(
        prediction_target=prediction_target,
        feature_names=normalized,
        feature_set_hash=compute_feature_set_hash(normalized),
    )
