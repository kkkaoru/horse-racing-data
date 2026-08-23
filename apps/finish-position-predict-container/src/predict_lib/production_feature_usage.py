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

_MODEL_FILE_NAMES: Final[frozenset[str]] = frozenset({"model.json", "model.txt"})
_METADATA_FILE_NAME: Final[str] = "metadata.json"
_TRANSFORMER_NORM_FILE_NAME: Final[str] = "norm.json"


def _feature_names_from_contract(path: Path, field: str) -> tuple[str, ...] | None:
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
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
        if name.endswith(".npz"):
            if f"{parent}/{_TRANSFORMER_NORM_FILE_NAME}" not in category_keys:
                return False
            continue
        # An enabled ensemble manifest or a future scorer format needs an
        # explicit feature-contract parser before this optimization is safe.
        return False

    for key, field in feature_contracts:
        names = _feature_names_from_contract(artifact_root / key, field)
        if names is None or RELATIONSHIP_FEATURE_NAMES.intersection(names):
            return False
    return True
