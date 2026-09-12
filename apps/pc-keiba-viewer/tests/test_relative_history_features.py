from __future__ import annotations

import polars as pl
import pytest
from learning.relative_history_features import add_relative_history_features


@pytest.fixture
def history() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "race_id": ["a", "a", "b", "b", "c", "c"],
            "horse_id": ["h1", "h2", "h1", "h2", "h1", "h2"],
            "race_date": [
                "20240101",
                "20240101",
                "20240101",
                "20240101",
                "20240102",
                "20240102",
            ],
            "finish": [1, 2, 2, 1, 1, 2],
            "distance_m": [200.0] * 6,
            "clock_seconds": [100.0, 200.0, 200.0, 100.0, 150.0, 200.0],
        }
    ).with_columns(
        pl.col("race_date").str.strptime(pl.Date, "%Y%m%d").alias("observed_date")
    )


def test_same_day_excluded_and_current_contrast_not_exposed(
    history: pl.DataFrame,
) -> None:
    result = add_relative_history_features(history).sort("race_id", "horse_id")
    assert result["past_relative_speed_mean"].to_list() == [
        None,
        None,
        None,
        None,
        0,
        0,
    ]
    assert result["past_relative_speed_28d"].to_list() == [None, None, None, None, 0, 0]
    assert "_context_z" not in result.columns
    assert "_context_speed" not in result.columns


def test_future_outcomes_do_not_change_target_history(history: pl.DataFrame) -> None:
    changed = history.with_columns(
        pl.when(pl.col("race_id") == "c")
        .then(5000.0)
        .otherwise(pl.col("clock_seconds"))
        .alias("clock_seconds")
    )
    result = (
        add_relative_history_features(changed)
        .filter(pl.col("race_id") == "c")
        .sort("horse_id")
    )
    assert result["past_relative_speed_mean"].to_list() == [0, 0]


def test_common_time_unit_rescaling_leaves_contrast_invariant(
    history: pl.DataFrame,
) -> None:
    scaled = history.with_columns((pl.col("clock_seconds") * 5).alias("clock_seconds"))
    result = (
        add_relative_history_features(scaled)
        .filter(pl.col("race_id") == "c")
        .sort("horse_id")
    )
    assert result["past_relative_speed_mean"].to_list() == pytest.approx(
        [0, 0], abs=1e-12
    )


def test_old_history_excluded_from_recent_windows(history: pl.DataFrame) -> None:
    old = history.with_columns(
        pl.when(pl.col("race_id") == "c")
        .then(pl.lit("20260101"))
        .otherwise(pl.col("race_date"))
        .alias("race_date")
    ).with_columns(
        pl.col("race_date").str.strptime(pl.Date, "%Y%m%d").alias("observed_date")
    )
    result = add_relative_history_features(old).filter(pl.col("race_id") == "c")
    assert result["past_relative_speed_mean"].to_list() == [0, 0]
    assert result["past_relative_speed_365d"].to_list() == [None, None]
    assert result["past_relative_speed_28d"].to_list() == [None, None]


@pytest.mark.parametrize("singleton", [True, False])
def test_unidentified_contrast_remains_missing(
    history: pl.DataFrame, singleton: bool
) -> None:
    frame = (
        history.filter(pl.col("horse_id") == "h1")
        if singleton
        else history.with_columns(pl.lit(100.0).alias("clock_seconds"))
    )
    result = add_relative_history_features(frame).filter(pl.col("race_id") == "c")
    assert result["past_relative_speed_mean"].drop_nulls().to_list() == []


def test_reserved_columns_rejected(history: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="reserved"):
        add_relative_history_features(
            history.with_columns(pl.lit(0).alias("past_relative_speed_mean"))
        )


def test_duplicate_observations_rejected(history: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="unique"):
        add_relative_history_features(pl.concat([history, history.head(1)]))
