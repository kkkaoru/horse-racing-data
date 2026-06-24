"""Pure-function library for NAR small-field per-race model routing.

Evaluation proved CatBoost beats XGBoost on NAR races with <=8 runners (~20%
of NAR races), while XGBoost stays the baseline for larger fields. This module
implements the SCORE-LEVEL post-processing merge: given predictions from two
independently-trained models covering the same fold, it routes each race to one
model's rows based on field size (rows per ``race_id``).

The router is model-agnostic (``small_field`` vs ``large_field``); the caller
passes CatBoost predictions as ``small_field_predictions`` and XGBoost
predictions as ``large_field_predictions``. All functions are deterministic and
do no file/network I/O.
"""

from __future__ import annotations

from typing import Final, cast

import pandas as pd

DEFAULT_SMALL_FIELD_THRESHOLD: Final[int] = 8

_RACE_ID_COL: Final[str] = "race_id"
_UMABAN_COL: Final[str] = "umaban"


def compute_field_sizes(predictions: pd.DataFrame) -> pd.Series:
    """Return a Series indexed by ``race_id`` giving the row count per race."""
    return cast(pd.Series, predictions.groupby(_RACE_ID_COL).size())


def select_small_field_race_ids(predictions: pd.DataFrame, threshold: int) -> set[str]:
    """Return race_ids whose field size is ``<= threshold``."""
    field_sizes = compute_field_sizes(predictions)
    small = field_sizes[field_sizes <= threshold]
    return {str(race_id) for race_id in small.index}


def route_small_field_predictions(
    small_field_predictions: pd.DataFrame,
    large_field_predictions: pd.DataFrame,
    *,
    threshold: int = DEFAULT_SMALL_FIELD_THRESHOLD,
) -> pd.DataFrame:
    """Per-race merge: small fields take ``small_field`` rows, large fields take ``large_field``.

    Field size is determined from ``large_field_predictions`` (the baseline /
    full prediction set, which must contain every race). For each race with
    field size ``<= threshold`` the rows come from ``small_field_predictions``;
    otherwise from ``large_field_predictions``. A small-field race missing from
    ``small_field_predictions`` falls back to its large-field rows (the race is
    never dropped). The output preserves the input column schema, is sorted by
    ``(race_id, umaban)`` for determinism, and has a reset index. An empty
    ``large_field_predictions`` yields an empty frame with the same columns.

    Caller convention: pass CatBoost as ``small_field_predictions`` and XGBoost
    as ``large_field_predictions``.
    """
    if large_field_predictions.empty:
        return large_field_predictions.iloc[0:0].reset_index(drop=True)
    small_race_ids = select_small_field_race_ids(large_field_predictions, threshold)
    available_small = set(small_field_predictions[_RACE_ID_COL].astype(str).unique())
    routed_to_small = small_race_ids & available_small
    small_rows = small_field_predictions[
        small_field_predictions[_RACE_ID_COL].astype(str).isin(routed_to_small)
    ]
    large_rows = large_field_predictions[
        ~large_field_predictions[_RACE_ID_COL].astype(str).isin(routed_to_small)
    ]
    combined = pd.concat([small_rows, large_rows], ignore_index=True)
    ordered = combined.sort_values(
        by=[_RACE_ID_COL, _UMABAN_COL],
        ascending=[True, True],
        kind="stable",
    ).reset_index(drop=True)
    return ordered[list(large_field_predictions.columns)]
