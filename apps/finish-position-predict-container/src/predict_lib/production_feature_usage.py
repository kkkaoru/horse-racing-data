"""Fail-closed feature-layer usage checks for selected production artifacts."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Final

from .artifact_integrity import derive_selected_artifact_keys
from .model_meta import Category

RELATIONSHIP_FEATURE_NAMES: Final[frozenset[str]] = frozenset(
    {
        "bataiju_futan_ratio",
        "futan_per_barei",
        "bataiju_per_kyori_log",
        "bataiju_diff_from_race_mean",
        "bataiju_rank_in_race",
        "futan_minus_bataiju_zscore_in_race",
        "barei_diff_from_race_mean",
        "past_speed_kg_normalized_avg5",
        "past_speed_futan_normalized_avg5",
        "past_speed_age_adjusted_avg5",
        "past_speed_volatility_5",
        "past_finish_position_volatility_5",
    }
)

# Every ``*_v2`` pedigree column the feature layers emit (normalised breeding
# ids, Ban-ei moisture, floored distance band, nvd_nu similar-race fallback).
# Serving skips their computation (``--skip-v2-features``) unless a selected
# scorer contract lists one of them.
V2_PEDIGREE_FEATURE_NAMES: Final[frozenset[str]] = frozenset(
    {
        # finish_position_features_duckdb.py
        "sire_distance_win_rate_v2",
        "dam_sire_distance_win_rate_v2",
        "sire_avg_finish_at_distance_v2",
        "pedigree_distance_x_horse_distance_v2",
        # add-near-miss-features.py
        "sire_distance_place2_rate_v2",
        "sire_grade_place2_rate_v2",
        "damsire_distance_place2_rate_v2",
        # add-baba-pedigree-affinity-features.py
        "current_baba_condition_v2",
        "horse_baba_career_starts_v2",
        "horse_baba_win_rate_v2",
        "sire_baba_career_starts_v2",
        "sire_baba_win_rate_v2",
        "damsire_baba_career_starts_v2",
        "damsire_baba_win_rate_v2",
        "sire_horse_baba_combined_score_v2",
        # add-banei-futan-class-features.py
        "sire_futan_class_starts_v2",
        "sire_futan_class_win_rate_v2",
        "damsire_futan_class_starts_v2",
        "damsire_futan_class_win_rate_v2",
        # add-similar-race-features.py
        "sim_sire_win_rate_v2",
        "sim_sire_place_rate_v2",
        "sim_sire_offspring_count_v2",
        "sim_damsire_win_rate_v2",
        "sim_damsire_place_rate_v2",
        "sim_damsire_offspring_count_v2",
        # add-sire-venue-bias-features.py
        "sire_venue_surface_dist_win_rate_v2",
        "sire_venue_surface_dist_place_rate_v2",
        "sire_venue_surface_dist_runs_v2",
        "sire_venue_surface_win_rate_v2",
        "sire_venue_surface_place_rate_v2",
    }
)

_MODEL_FILE_NAMES: Final[frozenset[str]] = frozenset({"model.json", "model.txt"})
_METADATA_FILE_NAME: Final[str] = "metadata.json"
_TRANSFORMER_NORM_FILE_NAME: Final[str] = "norm.json"
_HYBRID_MANIFEST_FILE_NAME: Final[str] = "manifest.json"


def _feature_names_from_contract(path: Path, field: str) -> tuple[str, ...] | None:
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    if field == "stats.feature_names":
        stats = payload.get("stats")
        if not isinstance(stats, Mapping):
            return None
        raw_names = stats.get("feature_names")
    else:
        raw_names = payload.get(field)
    if not isinstance(raw_names, Sequence) or isinstance(raw_names, (str, bytes)):
        return None
    if not raw_names or not all(isinstance(name, str) and name for name in raw_names):
        return None
    return tuple(raw_names)


def relationship_layer_is_provably_unused(
    category: Category,
    *,
    artifact_root: Path = Path("/models"),
    selected_keys: Iterable[str] | None = None,
) -> bool:
    """Return true only when every selected scorer contract excludes all 12 fields.

    Missing, malformed, or unfamiliar selector artifacts fail closed. This is
    intentionally a serving-only optimization: offline/training chains still
    materialize the relationship columns exactly as before.
    """
    return _selected_contracts_exclude(
        category, RELATIONSHIP_FEATURE_NAMES, artifact_root, selected_keys
    )


def v2_pedigree_features_provably_unused(
    category: Category,
    *,
    artifact_root: Path = Path("/models"),
    selected_keys: Iterable[str] | None = None,
) -> bool:
    """Return true only when no selected scorer contract lists a v2 pedigree column.

    Same fail-closed contract as ``relationship_layer_is_provably_unused``;
    training/store builds never consult it and always emit the v2 columns.
    """
    return _selected_contracts_exclude(
        category, V2_PEDIGREE_FEATURE_NAMES, artifact_root, selected_keys
    )


def _selected_contracts_exclude(
    category: Category,
    feature_names: frozenset[str],
    artifact_root: Path,
    selected_keys: Iterable[str] | None,
) -> bool:
    try:
        keys = frozenset(
            derive_selected_artifact_keys() if selected_keys is None else selected_keys
        )
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    prefix = f"finish-position/{category}/"
    category_keys = frozenset(key for key in keys if key.startswith(prefix))
    if not category_keys:
        return False

    feature_contracts: list[tuple[str, str]] = []
    for key in category_keys:
        name = Path(key).name
        parent = str(Path(key).parent)
        if name in _MODEL_FILE_NAMES:
            if f"{parent}/{_METADATA_FILE_NAME}" not in category_keys:
                return False
            continue
        if name == _METADATA_FILE_NAME:
            feature_contracts.append((key, "feature_names"))
            continue
        if name == _TRANSFORMER_NORM_FILE_NAME:
            feature_contracts.append((key, "feature_order"))
            continue
        if name == _HYBRID_MANIFEST_FILE_NAME:
            feature_contracts.append((key, "stats.feature_names"))
            continue
        if name.endswith(".npz"):
            norm_key = f"{parent}/{_TRANSFORMER_NORM_FILE_NAME}"
            manifest_key = f"{parent}/{_HYBRID_MANIFEST_FILE_NAME}"
            if norm_key not in category_keys and manifest_key not in category_keys:
                return False
            continue
        # An enabled ensemble manifest or a future scorer format needs an
        # explicit feature-contract parser before this optimization is safe.
        return False

    for key, field in feature_contracts:
        names = _feature_names_from_contract(artifact_root / key, field)
        if names is None or feature_names.intersection(names):
            return False
    return True
