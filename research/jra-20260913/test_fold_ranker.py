"""Test model boundaries without expensive or nondeterministic native fitting."""

from pathlib import Path
from unittest.mock import Mock

import fold_ranker
import numpy as np
import numpy.typing as npt
import pytest


def test_market_and_noncausal_features_are_excluded() -> None:
    assert fold_ranker.market_free_features(
        [
            "kyori",
            "odds_score",
            "popularity_score",
            "tansho_odds",
            "tansho_ninkijun",
            "last_race_finish_norm",
            "finish_position",
            "target_corner_1_norm",
            "venue_temperature",
            "weather_normalized",
            "weight_diff_from_avg",
            "career_win_rate",
        ]
    ) == ("kyori", "last_race_finish_norm", "career_win_rate")


def test_duplicate_feature_names_fail() -> None:
    with pytest.raises(ValueError, match="unique"):
        fold_ranker.market_free_features(["kyori", "kyori"])


def test_empty_market_free_schema_fails() -> None:
    with pytest.raises(ValueError, match="No market-free"):
        fold_ranker.market_free_features(["odds_score"])


def test_top5_relevance_preserves_dnf_and_dq_semantics() -> None:
    values = fold_ranker.relevance_for_top5(
        np.array([1, 2, 3, 4, 5, 6, np.nan, np.nan], dtype=np.float32),
        np.array(["0", "0", "0", "0", "0", "0", "4", "5"], dtype=np.str_),
    )
    assert values.tolist() == [5, 4, 3, 2, 1, 0, 0, 0]


@pytest.mark.parametrize("value", [0.0, float("nan"), float("inf"), 1.5])
def test_invalid_classified_finish_fails(value: float) -> None:
    with pytest.raises(ValueError):
        fold_ranker.relevance_for_top5(
            np.array([value], dtype=np.float32), np.array(["0"], dtype=np.str_)
        )


def test_nonstarter_is_not_a_negative_training_label() -> None:
    with pytest.raises(ValueError, match="Nonstarters"):
        fold_ranker.relevance_for_top5(
            np.array([np.nan], dtype=np.float32), np.array(["1"], dtype=np.str_)
        )


@pytest.mark.parametrize("values", [[], [[1.0]], [1.0, 2.0]])
def test_misaligned_labels_fail(values: list[float] | list[list[float]]) -> None:
    with pytest.raises(ValueError, match="align"):
        fold_ranker.relevance_for_top5(
            np.array(values, dtype=np.float32), np.array(["0"], dtype=np.str_)
        )


@pytest.mark.parametrize(("iterations", "depth", "threads"), [(0, 6, 4), (1, 0, 4), (1, 6, 0)])
def test_invalid_dimensions_fail(iterations: int, depth: int, threads: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        fold_ranker.RankerConfig(iterations=iterations, depth=depth, threads=threads)


def test_invalid_learning_rate_fails() -> None:
    with pytest.raises(ValueError, match="Learning rate"):
        fold_ranker.RankerConfig(learning_rate=0.0)


def test_fit_groups_rows_and_disables_external_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factory = Mock()
    factory.return_value.predict.return_value = np.array([0.6, 0.1])
    pool = Mock()
    monkeypatch.setattr(fold_ranker, "CatBoostRanker", factory)
    monkeypatch.setattr(fold_ranker, "Pool", pool)
    result = fold_ranker.fit_and_predict(
        x_train=np.array([[1], [2]], dtype=np.float32),
        finishes=np.array([1, 2], dtype=np.float32),
        abnormality=np.array(["0", "0"], dtype=np.str_),
        race_ids=np.array(["b", "a"], dtype=np.str_),
        x_evaluation=np.array([[3], [4]], dtype=np.float32),
        output=tmp_path / "fit",
        config=fold_ranker.RankerConfig(iterations=2),
    )
    assert result.tolist() == [0.6, 0.1]
    assert factory.call_args.kwargs["allow_writing_files"] is False
    assert factory.call_args.kwargs["thread_count"] == 4
    assert pool.call_args.args[0].tolist() == [[2], [1]]
    assert pool.call_args.kwargs["label"].tolist() == [4, 5]
    assert pool.call_args.kwargs["group_id"] == ["a", "b"]
    assert factory.return_value.save_model.call_count == 2


@pytest.mark.parametrize("values", [[[1, 2]], [1], [[float("inf")]]])
def test_invalid_matrix_fails(tmp_path: Path, values: list[list[float]] | list[float]) -> None:
    with pytest.raises(ValueError):
        fold_ranker.fit_and_predict(
            x_train=np.asarray(values, dtype=np.float32),
            finishes=np.array([1], dtype=np.float32),
            abnormality=np.array(["0"], dtype=np.str_),
            race_ids=np.array(["a"], dtype=np.str_),
            x_evaluation=np.array([[3]], dtype=np.float32),
            output=tmp_path / "bad",
            config=fold_ranker.RankerConfig(),
        )


def test_misaligned_training_rows_fail(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="align"):
        fold_ranker.fit_and_predict(
            x_train=np.array([[1], [2]], dtype=np.float32),
            finishes=np.array([1], dtype=np.float32),
            abnormality=np.array(["0"], dtype=np.str_),
            race_ids=np.array(["a"], dtype=np.str_),
            x_evaluation=np.array([[3]], dtype=np.float32),
            output=tmp_path / "bad",
            config=fold_ranker.RankerConfig(),
        )


@pytest.mark.parametrize("scores", [np.array([np.nan]), np.array([1.0, 2.0])])
def test_invalid_predictions_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scores: npt.NDArray[np.float64]
) -> None:
    factory = Mock()
    factory.return_value.predict.return_value = scores
    monkeypatch.setattr(fold_ranker, "CatBoostRanker", factory)
    monkeypatch.setattr(fold_ranker, "Pool", Mock())
    with pytest.raises(ValueError, match="forecasts"):
        fold_ranker.fit_and_predict(
            x_train=np.array([[1], [2]], dtype=np.float32),
            finishes=np.array([1, 2], dtype=np.float32),
            abnormality=np.array(["0", "0"], dtype=np.str_),
            race_ids=np.array(["a", "a"], dtype=np.str_),
            x_evaluation=np.array([[3]], dtype=np.float32),
            output=tmp_path / "bad",
            config=fold_ranker.RankerConfig(),
        )
