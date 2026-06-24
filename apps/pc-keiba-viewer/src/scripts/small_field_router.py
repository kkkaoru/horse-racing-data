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

from typing import Final

import polars as pl

DEFAULT_SMALL_FIELD_THRESHOLD: Final[int] = 8

_RACE_ID_COL: Final[str] = "race_id"
_UMABAN_COL: Final[str] = "umaban"
_FIELD_SIZE_COL: Final[str] = "field_size"


def compute_field_sizes(predictions: pl.DataFrame) -> pl.DataFrame:
    """Return a frame with one row per ``race_id`` and its row count.

    Columns: ``race_id`` and ``field_size``.
    """
    return predictions.group_by(_RACE_ID_COL).agg(pl.len().alias(_FIELD_SIZE_COL))


def select_small_field_race_ids(predictions: pl.DataFrame, threshold: int) -> set[str]:
    """Return race_ids whose field size is ``<= threshold``."""
    field_sizes = compute_field_sizes(predictions)
    small = field_sizes.filter(pl.col(_FIELD_SIZE_COL) <= threshold)
    return {str(race_id) for race_id in small[_RACE_ID_COL].to_list()}


def route_small_field_predictions(
    small_field_predictions: pl.DataFrame,
    large_field_predictions: pl.DataFrame,
    *,
    threshold: int = DEFAULT_SMALL_FIELD_THRESHOLD,
) -> pl.DataFrame:
    """Per-race merge: small fields take ``small_field`` rows, large fields take ``large_field``.

    Field size is determined from ``large_field_predictions`` (the baseline /
    full prediction set, which must contain every race). For each race with
    field size ``<= threshold`` the rows come from ``small_field_predictions``;
    otherwise from ``large_field_predictions``. A small-field race missing from
    ``small_field_predictions`` falls back to its large-field rows (the race is
    never dropped). The output preserves the input column schema and is sorted by
    ``(race_id, umaban)`` for determinism. An empty ``large_field_predictions``
    yields an empty frame with the same columns.

    Caller convention: pass CatBoost as ``small_field_predictions`` and XGBoost
    as ``large_field_predictions``.
    """
    if large_field_predictions.is_empty():
        return large_field_predictions.clear()
    small_race_ids = select_small_field_race_ids(large_field_predictions, threshold)
    available_small = set(
        small_field_predictions[_RACE_ID_COL].cast(pl.String).to_list()
    )
    routed_to_small = small_race_ids & available_small
    small_rows = small_field_predictions.filter(
        pl.col(_RACE_ID_COL).cast(pl.String).is_in(routed_to_small)
    )
    large_rows = large_field_predictions.filter(
        ~pl.col(_RACE_ID_COL).cast(pl.String).is_in(routed_to_small)
    )
    combined = pl.concat([small_rows, large_rows])
    ordered = combined.sort(
        by=[_RACE_ID_COL, _UMABAN_COL],
        descending=[False, False],
        maintain_order=True,
    )
    return ordered.select(large_field_predictions.columns)
