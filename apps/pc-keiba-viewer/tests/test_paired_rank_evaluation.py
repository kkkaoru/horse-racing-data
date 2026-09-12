from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from learning.paired_rank_evaluation import (
    compare_rank_predictions,
    main,
    validate_predictions,
)


@pytest.fixture
def predictions() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "race_id": ["r"] * 5,
            "race_date": ["20240101"] * 5,
            "horse_id": ["a", "b", "c", "d", "e"],
            "horse_number": [1, 2, 3, 4, 5],
            "finish": [1, 2, 3, 4, 5],
            "predicted_rank": [1, 2, 3, 4, 5],
        }
    )


def test_short_confirmation_abstains(predictions: pl.DataFrame) -> None:
    result = compare_rank_predictions(predictions, predictions)
    assert result.races == 1
    assert result.dates == 1
    assert result.deltas == [0, 0, 0, 0, 0]
    assert result.observed_rank_support == [1, 1, 1, 1, 1]
    assert result.lower is None
    assert result.upper is None
    assert result.promotion_eligible is False


def test_cluster_bootstrap_preserves_exact_paired_gain(
    predictions: pl.DataFrame,
) -> None:
    days = pl.DataFrame({"day": pl.int_range(1, 21, eager=True)})
    full = predictions.join(days, how="cross").with_columns(
        pl.col("day").cast(pl.String).alias("race_id"),
        (pl.lit("202401") + pl.col("day").cast(pl.String).str.zfill(2)).alias(
            "race_date"
        ),
    )
    baseline = full.with_columns((6 - pl.col("predicted_rank")).alias("predicted_rank"))
    result = compare_rank_predictions(baseline, full, samples=1000)
    assert result.races == 20
    assert result.lower == [1, 1, 0, 1, 1]
    assert result.upper == [1, 1, 0, 1, 1]
    assert result.baseline_accuracy == [0, 0, 1, 0, 0]
    assert result.candidate_accuracy == [1, 1, 1, 1, 1]


def test_short_fields_report_missing_support(predictions: pl.DataFrame) -> None:
    short = predictions.head(2)
    result = compare_rank_predictions(short, short)
    assert result.observed_rank_support == [1, 1, 0, 0, 0]
    assert result.baseline_accuracy == [1, 1, 0, 0, 0]


@pytest.mark.parametrize("empty", [True, False])
def test_missing_observations_rejected(predictions: pl.DataFrame, empty: bool) -> None:
    bad = (
        predictions.head(0)
        if empty
        else predictions.with_columns(pl.lit(None).alias("finish"))
    )
    with pytest.raises(ValueError, match="complete identities"):
        validate_predictions(bad)


def test_duplicate_horse_rejected(predictions: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="Duplicate"):
        validate_predictions(pl.concat([predictions, predictions.head(1)]))


@pytest.mark.parametrize("rank", [0.0, 1.5, float("inf")])
def test_invalid_numeric_rank_rejected(predictions: pl.DataFrame, rank: float) -> None:
    with pytest.raises(ValueError, match="positive finite integers"):
        validate_predictions(
            predictions.with_columns(pl.lit(rank).alias("predicted_rank"))
        )


def test_non_permutation_rejected(predictions: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="permutation"):
        validate_predictions(
            predictions.with_columns(pl.lit(1).alias("predicted_rank"))
        )


def test_mismatched_dates_rejected(predictions: pl.DataFrame) -> None:
    other = predictions.with_columns(pl.lit("20240102").alias("race_date"))
    with pytest.raises(ValueError, match="identical entrants"):
        compare_rank_predictions(predictions, other)


def test_small_resampling_budget_rejected(predictions: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="1000"):
        compare_rank_predictions(predictions, predictions, samples=5)


def test_cli_persists_audit(predictions: pl.DataFrame, tmp_path: Path) -> None:
    predictions.write_parquet(tmp_path / "predictions.parquet")
    main(
        [
            "--baseline",
            str(tmp_path / "predictions.parquet"),
            "--candidate",
            str(tmp_path / "predictions.parquet"),
            "--output",
            str(tmp_path / "report.json"),
        ]
    )
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["uncertainty"] == "insufficient-date-clusters"
    assert report["promotion_eligible"] is False
