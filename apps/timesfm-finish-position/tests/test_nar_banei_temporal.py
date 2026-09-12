from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import polars as pl
import pytest

from timesfm_finish_position.domain import FloatArray
from timesfm_finish_position.nar_banei_temporal import (
    build_queries,
    forecast_targets,
    observation_index,
)


@dataclass
class LastForecaster:
    failure: str = ""

    @property
    def backend(self) -> str:
        return "test"

    def predict(self, contexts: Sequence[FloatArray], *, horizon: int) -> tuple[FloatArray, ...]:
        assert horizon == 1
        if self.failure == "omitted":
            return ()
        if self.failure == "nan":
            return tuple(np.full((context.shape[0], 1), np.nan) for context in contexts)
        return tuple(context[:, -1:] for context in contexts)


@pytest.fixture
def observations() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "horse_id": ["a"] * 5,
            "race_id": ["r0", "r1", "r2", "r3", "r4"],
            "race_date": ["20191201", "20191210", "20200102", "20200102", "20200103"],
            "performance": [None, None, 1.0, 0.0, 0.0],
            "relative_speed": [None, 1.0, None, 2.0, -1.0],
        }
    )


@pytest.fixture
def targets() -> pl.DataFrame:
    return pl.DataFrame(
        {"horse_id": ["a", "a", "new"], "race_date": ["20200102", "20200104", "20200104"]}
    )


def test_previous_day_and_forward_only_imputation(
    observations: pl.DataFrame, targets: pl.DataFrame
) -> None:
    queries = build_queries(
        observation_index(observations, targets), targets, columns=("performance", "relative_speed")
    )
    assert len(queries) == 2
    assert queries[0].context.tolist() == [[0.5], [1.0]]
    assert queries[0].latest_date == "20191210"
    assert queries[1].context.tolist() == [[0.5, 1.0, 0.0, 0.0], [1.0, 1.0, 2.0, -1.0]]
    assert queries[1].latest_date == "20200103"


def test_body_history_obeys_same_day_cutoff(
    observations: pl.DataFrame, targets: pl.DataFrame
) -> None:
    body = observations.with_columns(pl.Series("body_weight", [None, 0.01, 0.99, 1.0, 0.02]))
    queries = build_queries(observation_index(body, targets), targets, columns=("body_weight",))
    assert queries[0].context.tolist() == [[0.01]]
    assert queries[0].component_counts == (1,)
    result = forecast_targets(targets, queries, LastForecaster(), columns=("body_weight",))
    assert result["timesfm_body_weight"].head(2).to_list() == [0.01, 0.02]


def test_future_mutation_cannot_change_first_query(
    observations: pl.DataFrame, targets: pl.DataFrame
) -> None:
    changed = observations.with_columns(
        pl.when(pl.col("race_date") >= "20200102")
        .then(999.0)
        .otherwise(pl.col("performance"))
        .alias("performance")
    )
    queries = build_queries(
        observation_index(changed, targets), targets, columns=("performance", "relative_speed")
    )
    assert queries[0].context.tolist() == [[0.5], [1.0]]


def test_frozen_year_reuses_one_step_without_future_horizon(
    observations: pl.DataFrame, targets: pl.DataFrame
) -> None:
    queries = build_queries(
        observation_index(observations, targets),
        targets,
        columns=("performance", "relative_speed"),
        frozen_year=True,
    )
    assert len(queries) == 1
    assert queries[0].target_rows == (0, 1)
    assert queries[0].context.tolist() == [[0.5], [1.0]]


def test_day_speed_is_past_only(observations: pl.DataFrame, targets: pl.DataFrame) -> None:
    values = observations.with_columns(pl.col("relative_speed").alias("day_speed"))
    queries = build_queries(observation_index(values, targets), targets, columns=("day_speed",))
    assert queries[0].context.tolist() == [[1.0]]
    assert queries[0].latest_date == "20191210"


def test_cap_and_all_missing_profile(observations: pl.DataFrame, targets: pl.DataFrame) -> None:
    queries = build_queries(
        observation_index(observations, targets), targets, columns=("performance",), max_history=2
    )
    assert len(queries) == 1
    assert queries[0].context.tolist() == [[0.0, 0.0]]


@pytest.mark.parametrize(
    "columns,cap", [((), None), (("future_outcome",), None), (("performance",), 0)]
)
def test_invalid_configuration(
    targets: pl.DataFrame, columns: tuple[str, ...], cap: int | None
) -> None:
    with pytest.raises(ValueError, match=r"Unsupported|positive"):
        build_queries({}, targets, columns=columns, max_history=cap)


def test_forecast_and_matched_classical_controls(
    observations: pl.DataFrame, targets: pl.DataFrame
) -> None:
    queries = build_queries(
        observation_index(observations, targets), targets, columns=("performance", "relative_speed")
    )
    result = forecast_targets(
        targets, queries, LastForecaster(), columns=("performance", "relative_speed"), chunk_size=1
    )
    assert result["history_count"].to_list() == [1, 4, 0]
    assert result["history_count_performance"].to_list() == [0, 3, 0]
    assert result["history_count_relative_speed"].to_list() == [1, 3, 0]
    assert result["timesfm_performance"].head(2).to_list() == [0.5, 0.0]
    assert result["mean5_performance"].head(2).to_list() == [0.5, 0.375]
    assert result["last_relative_speed"].head(2).to_list() == [1.0, -1.0]
    assert result["timesfm_performance"].is_nan().to_list() == [False, False, True]


@pytest.mark.parametrize("failure", ["omitted", "nan"])
def test_bad_forecaster_rejected(
    observations: pl.DataFrame, targets: pl.DataFrame, failure: str
) -> None:
    queries = build_queries(
        observation_index(observations, targets), targets, columns=("performance",)
    )
    with pytest.raises(ValueError, match=r"omitted|Invalid"):
        forecast_targets(targets, queries, LastForecaster(failure), columns=("performance",))


def test_invalid_chunk(targets: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="positive"):
        forecast_targets(targets, (), LastForecaster(), columns=("performance",), chunk_size=0)
