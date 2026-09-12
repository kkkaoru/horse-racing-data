from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from timesfm_finish_position.nar_banei_exact import (
    Readout,
    exact_hits,
    normalized_components,
    optimize_readout,
    predicted_ranks,
    readout_scores,
    recency_weights,
)


@pytest.fixture
def frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "race_id": ["r"] * 5,
            "horse_id": ["a", "b", "c", "d", "e"],
            "horse_number": [1, 2, 3, 4, 5],
            "finish": [1, 2, 3, 4, 5],
            "year": [2020] * 5,
            "race_date": ["20200901"] * 5,
            "baseline_score": [0.75, 1.0, 0.5, 0.0, 0.25],
            "history_count": [8] * 5,
            "timesfm_performance": [1.0, 0.75, 0.5, 0.25, 0.0],
            "timesfm_relative_speed": [1.0, 0.75, 0.5, 0.25, 0.0],
        }
    )


def test_exact_positions_are_not_winner_topk(frame: pl.DataFrame) -> None:
    assert exact_hits(
        frame, np.asarray(frame["baseline_score"].to_numpy(), dtype=np.float64)
    ).tolist() == [0, 0, 1, 0, 0]
    scores = readout_scores(frame, Readout("p", temporal_weight=1, speed_weight=0.5))
    assert exact_hits(frame, scores).tolist() == [1, 1, 1, 1, 1]
    assert predicted_ranks(frame, np.ones(5)).tolist() == [1, 2, 3, 4, 5]


def test_missing_history_and_ties(frame: pl.DataFrame) -> None:
    tied = frame.with_columns(pl.lit(1.0).alias("timesfm_performance"))
    assert (
        normalized_components(tied, origin="timesfm", minimum_history=1)[:, 0].tolist() == [0.5] * 5
    )
    unavailable = frame.with_columns(pl.Series("history_count", [1, 0, 0, 0, 0]))
    assert normalized_components(unavailable, origin="timesfm", minimum_history=1)[
        :, 0
    ].tolist() == [0.75, 1.0, 0.5, 0.0, 0.25]
    assert readout_scores(frame, Readout("p")).tolist() == [0.75, 1.0, 0.5, 0.0, 0.25]


def test_centered_magnitudes_and_day_speed(frame: pl.DataFrame) -> None:
    compressed = frame.with_columns(pl.Series("timesfm_performance", [0.6, 0.55, 0.5, 0.45, 0.4]))
    scores = readout_scores(compressed, Readout("p", temporal_weight=1, normalization="centered"))
    assert scores.tolist() == pytest.approx([0.6, 0.55, 0.5, 0.45, 0.4])
    day = frame.rename({"timesfm_performance": "timesfm_day_speed"}).drop("timesfm_relative_speed")
    assert readout_scores(day, Readout("p", temporal_weight=1)).tolist() == [
        1.0,
        0.75,
        0.5,
        0.25,
        0.0,
    ]
    _, trials = optimize_readout(
        {"p": [compressed]},
        origin="timesfm",
        evaluation_year=2021,
        n_trials=10,
        seed=42,
        normalizations=("rank", "centered"),
    )
    assert len(trials) == 10


def test_innovations_preserve_baseline_and_forecast_changes(frame: pl.DataFrame) -> None:
    anchored = frame.with_columns(
        pl.lit(0.5).alias("last_performance"),
        pl.lit(0.5).alias("last_relative_speed"),
    )
    matrix = normalized_components(
        anchored, origin="timesfm", minimum_history=1, normalization="innovation"
    )
    assert matrix.tolist() == [
        [1.25, 1.25],
        [1.25, 1.25],
        [0.5, 0.5],
        [-0.25, -0.25],
        [-0.25, -0.25],
    ]
    scores = readout_scores(
        anchored, Readout("p", origin="last", temporal_weight=1, normalization="innovation")
    )
    assert scores.tolist() == [0.75, 1.0, 0.5, 0.0, 0.25]


def test_innovation_missing_anchors_and_observations(frame: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="anchors"):
        normalized_components(
            frame, origin="timesfm", minimum_history=1, normalization="innovation"
        )
    missing = frame.drop("timesfm_relative_speed").with_columns(
        pl.Series("last_performance", [0.5, None, float("nan"), float("inf"), 0.5]),
        pl.Series("history_count", [8, 8, 8, 8, 0]),
    )
    scores = readout_scores(missing, Readout("p", temporal_weight=1, normalization="innovation"))
    assert scores.tolist() == [0.75, 1.0, 0.5, 0.0, 0.25]


def test_invalid_normalizations(frame: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="normalization"):
        Readout("p", normalization="bad")
    with pytest.raises(ValueError, match="normalization"):
        normalized_components(frame, origin="timesfm", minimum_history=1, normalization="bad")
    with pytest.raises(ValueError, match="component count"):
        normalized_components(
            frame.with_columns(pl.lit(1.0).alias("timesfm_day_speed")),
            origin="timesfm",
            minimum_history=1,
        )
    with pytest.raises(ValueError, match="normalization"):
        optimize_readout(
            {"p": [frame]},
            origin="timesfm",
            evaluation_year=2021,
            n_trials=1,
            seed=42,
            normalizations=(),
        )


def test_per_component_missingness_does_not_become_observed_history(frame: pl.DataFrame) -> None:
    missing = frame.with_columns(pl.lit(0).alias("history_count_performance"))
    scores = readout_scores(missing, Readout("p", temporal_weight=1))
    assert scores.tolist() == [0.75, 1.0, 0.5, 0.0, 0.25]


def test_body_readout_and_missing_body_history(frame: pl.DataFrame) -> None:
    body = frame.rename({"timesfm_relative_speed": "timesfm_body_weight"})
    config = Readout("performance-body-full", temporal_weight=1, speed_weight=1)
    assert readout_scores(body, config).tolist() == [1.0, 0.75, 0.5, 0.25, 0.0]
    missing = body.with_columns(pl.lit(0).alias("history_count_body_weight"))
    assert readout_scores(missing, config).tolist() == [0.75, 1.0, 0.5, 0.0, 0.25]


def test_recency_decay_and_precomputed_search(frame: pl.DataFrame) -> None:
    dated = frame.with_columns(
        pl.lit("20200226").alias("race_date"),
        pl.Series("latest_history_date", ["20200212", "20200129", "20200101", None, "20200226"]),
        pl.Series("history_count", [8, 8, 8, 0, 0]),
    )
    assert recency_weights(dated, 14).tolist() == [0.5, 0.25, 0.0625, 0.0, 0.0]
    config = Readout("p", temporal_weight=1, half_life_days=14)
    assert readout_scores(dated, config).tolist() == [0.875, 0.875, 0.46875, 0.0, 0.25]
    _, trials = optimize_readout(
        {"p": [dated]},
        origin="timesfm",
        evaluation_year=2021,
        n_trials=30,
        seed=42,
        half_lives=(0, 14),
    )
    assert len(trials) == 30


@pytest.mark.parametrize("latest", ["20200901", "20200902", None])
def test_invalid_known_recency_dates(frame: pl.DataFrame, latest: str | None) -> None:
    dated = frame.with_columns(pl.lit(latest, dtype=pl.String).alias("latest_history_date"))
    with pytest.raises(ValueError, match="strictly prior"):
        recency_weights(dated, 14)


def test_recency_requires_dates_and_positive_half_life(frame: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="Recency requires"):
        recency_weights(frame, 14)
    with pytest.raises(ValueError, match="Recency requires"):
        recency_weights(frame, 0)
    with pytest.raises(ValueError, match="nonnegative"):
        Readout("p", half_life_days=-1)


@pytest.mark.parametrize("halves", [(), (-1,)])
def test_invalid_search_half_lives(frame: pl.DataFrame, halves: tuple[int, ...]) -> None:
    with pytest.raises(ValueError, match="half-lives"):
        optimize_readout(
            {"p": [frame]},
            origin="timesfm",
            evaluation_year=2021,
            n_trials=1,
            seed=42,
            half_lives=halves,
        )


def test_single_component(frame: pl.DataFrame) -> None:
    one = frame.drop("timesfm_relative_speed")
    assert readout_scores(one, Readout("p", temporal_weight=1)).tolist() == [
        1.0,
        0.75,
        0.5,
        0.25,
        0.0,
    ]


@pytest.mark.parametrize(
    "origin,weight,minimum", [("bad", 0.0, 1), ("timesfm", 2.0, 1), ("timesfm", 0.0, 0)]
)
def test_bad_config(origin: str, weight: float, minimum: int) -> None:
    with pytest.raises(ValueError):
        Readout("p", origin, weight, minimum_history=minimum)


def test_bad_scores_and_labels(frame: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="Invalid scores"):
        predicted_ranks(frame, np.array([np.nan]))
    with pytest.raises(ValueError, match="positive finish"):
        exact_hits(frame.with_columns(pl.lit(0).alias("finish")), np.ones(5))
    with pytest.raises(ValueError, match="No forecast"):
        normalized_components(frame, origin="absent", minimum_history=1)
    assert predicted_ranks(frame.head(0), np.empty(0)).tolist() == []


def test_real_rustuna_exact_objective(frame: pl.DataFrame) -> None:
    config, trials = optimize_readout(
        {"p": [frame]}, origin="timesfm", evaluation_year=2021, n_trials=20, seed=42
    )
    assert len(trials) == 20
    assert config.temporal_weight > 0
    assert exact_hits(frame, readout_scores(frame, config)).tolist() == [1, 1, 1, 1, 1]


def test_infeasible_changes_keep_control(frame: pl.DataFrame) -> None:
    perfect = frame.with_columns(
        pl.col("timesfm_performance").alias("baseline_score"),
        pl.col("baseline_score").alias("timesfm_performance"),
        pl.col("baseline_score").alias("timesfm_relative_speed"),
    )
    config, trials = optimize_readout(
        {"p": [perfect]}, origin="timesfm", evaluation_year=2021, n_trials=20, seed=42
    )
    assert config.temporal_weight == 0
    assert any(trial["feasible"] is False for trial in trials)


def test_single_component_search(frame: pl.DataFrame) -> None:
    config, _ = optimize_readout(
        {"p": [frame.drop("timesfm_relative_speed")]},
        origin="timesfm",
        evaluation_year=2021,
        n_trials=2,
        seed=42,
    )
    assert config.speed_weight == 0


@pytest.mark.parametrize(
    "kind", ["empty", "empty-fold", "future", "future-date", "duplicate", "mismatch"]
)
def test_hpo_integrity(frame: pl.DataFrame, kind: str) -> None:
    development = {"p": [frame]}
    if kind == "empty":
        development = {}
    elif kind == "empty-fold":
        development = {"p": [frame.head(0)]}
    elif kind == "future":
        development = {"p": [frame.with_columns(pl.lit(2021).alias("year"))]}
    elif kind == "future-date":
        development = {"p": [frame.with_columns(pl.lit("20210101").alias("race_date"))]}
    elif kind == "duplicate":
        development = {"p": [frame, frame]}
    else:
        development["q"] = [frame.with_columns(pl.lit(0.1).alias("baseline_score"))]
    with pytest.raises(ValueError):
        optimize_readout(development, origin="timesfm", evaluation_year=2021, n_trials=1, seed=42)
