"""Probability assignment, roster semantics, and bounded classifier tests."""

from pathlib import Path
from unittest.mock import Mock

import numpy as np
import numpy.typing as npt
import position_classifier
import pytest
from fold_ranker import RankerConfig


def test_joint_assignment_can_trade_tiny_winner_margin_for_exact_second() -> None:
    probabilities = np.array([[0, 0.51, 0.49, 0, 0, 0], [0.4, 0.5, 0.1, 0, 0, 0]], dtype=np.float64)
    assert position_classifier.assign_joint_positions(probabilities, winner_weight=2).tolist() == [
        1,
        0,
    ]
    assert position_classifier.assign_positions(probabilities).tolist() == [0, 1]


def test_joint_assignment_respects_large_winner_priority() -> None:
    probabilities = np.array([[0, 0.51, 0.49, 0, 0, 0], [0.4, 0.5, 0.1, 0, 0, 0]], dtype=np.float64)
    assert position_classifier.assign_joint_positions(
        probabilities, winner_weight=100
    ).tolist() == [0, 1]


@pytest.mark.parametrize("weight", [0.5, float("inf"), float("nan")])
def test_joint_assignment_rejects_invalid_winner_priority(weight: float) -> None:
    with pytest.raises(ValueError, match="Winner utility"):
        position_classifier.assign_joint_positions(
            np.array([[0, 1, 0, 0, 0, 0]], dtype=np.float64), winner_weight=weight
        )


def test_joint_assignment_keeps_outside_top_five_runner_in_tail() -> None:
    probabilities = np.eye(6, dtype=np.float64)
    assert position_classifier.assign_joint_positions(probabilities, winner_weight=1).tolist() == [
        1,
        2,
        3,
        4,
        5,
        0,
    ]


def test_joint_assignment_decodes_races_independently() -> None:
    probabilities = np.array(
        [[0, 0.51, 0.49, 0, 0, 0], [0.4, 0.5, 0.1, 0, 0, 0], [0, 1, 0, 0, 0, 0]], dtype=np.float64
    )
    scores = position_classifier.rank_scores_for_races(
        probabilities, np.array(["a", "a", "b"], dtype=np.str_), winner_weight=2
    )
    assert scores.tolist() == [1, 2, 1]


def test_joint_assignment_rejects_missing_probability_mass() -> None:
    with pytest.raises(ValueError, match="sum to one"):
        position_classifier.assign_joint_positions(
            np.zeros((1, 6), dtype=np.float64), winner_weight=2
        )


def test_assignment_optimizes_exact_positions_not_mean_rank() -> None:
    probabilities = np.array(
        [
            [0, 0.6, 0.35, 0.05, 0, 0],
            [0, 0.35, 0.05, 0.60, 0, 0],
            [0, 0.05, 0.60, 0.35, 0, 0],
        ],
        dtype=np.float64,
    )
    assert position_classifier.assign_positions(probabilities).tolist() == [0, 2, 1]


def test_winner_probability_has_lexicographic_priority() -> None:
    probabilities = np.array(
        [
            [0, 0.51, 0.49, 0, 0, 0],
            [0, 0.5, 0.1, 0.4, 0, 0],
            [0, 0, 0.4, 0.6, 0, 0],
        ],
        dtype=np.float64,
    )
    assert position_classifier.assign_positions(probabilities).tolist() == [0, 2, 1]


def test_single_runner_and_stable_equal_probabilities() -> None:
    assert position_classifier.assign_positions(np.full((1, 6), 1 / 6)).tolist() == [0]
    assert position_classifier.assign_positions(np.full((7, 6), 1 / 6)).tolist() == [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
    ]


@pytest.mark.parametrize(
    "probabilities",
    [
        np.empty((0, 6)),
        np.ones((2, 5)),
        np.ones(6),
        np.full((1, 6), np.nan),
        np.full((1, 6), -0.1),
        np.full((1, 6), 1.1),
        np.full((1, 6), 0.1),
    ],
)
def test_invalid_probabilities_fail(probabilities: npt.NDArray[np.float64]) -> None:
    with pytest.raises(ValueError):
        position_classifier.assign_positions(probabilities)


def test_classifier_retains_dnf_and_balances_races(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factory = Mock()
    factory.return_value.classes_ = np.array([0, 1, 2])
    factory.return_value.predict_proba.return_value = np.array([[0.2, 0.5, 0.3]])
    pool = Mock()
    monkeypatch.setattr(position_classifier, "CatBoostClassifier", factory)
    monkeypatch.setattr(position_classifier, "Pool", pool)
    result = position_classifier.fit_position_probabilities(
        x_train=np.array([[1], [2], [3]], dtype=np.float32),
        finishes=np.array([1, 2, np.nan], dtype=np.float32),
        abnormality=np.array(["0", "0", "4"], dtype=np.str_),
        race_ids=np.array(["a", "a", "b"], dtype=np.str_),
        x_evaluation=np.array([[4]], dtype=np.float32),
        output=tmp_path / "fit",
        config=RankerConfig(iterations=2),
    )
    assert result.tolist() == [[0.2, 0.5, 0.3, 0, 0, 0]]
    assert pool.call_args.kwargs["label"].tolist() == [1, 2, 0]
    assert pool.call_args.kwargs["weight"].tolist() == [0.75, 0.75, 1.5]
    assert factory.call_args.kwargs["allow_writing_files"] is False
    assert factory.return_value.save_model.call_count == 2


@pytest.mark.parametrize("values", [[[1, 2]], [1], [[float("inf")]]])
def test_invalid_feature_matrix_fails(
    tmp_path: Path, values: list[list[float]] | list[float]
) -> None:
    with pytest.raises(ValueError):
        position_classifier.fit_position_probabilities(
            x_train=np.asarray(values, dtype=np.float32),
            finishes=np.array([1], dtype=np.float32),
            abnormality=np.array(["0"], dtype=np.str_),
            race_ids=np.array(["a"], dtype=np.str_),
            x_evaluation=np.array([[3]], dtype=np.float32),
            output=tmp_path / "bad",
            config=RankerConfig(),
        )


def test_misaligned_rows_fail(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="align"):
        position_classifier.fit_position_probabilities(
            x_train=np.array([[1], [2]], dtype=np.float32),
            finishes=np.array([1], dtype=np.float32),
            abnormality=np.array(["0"], dtype=np.str_),
            race_ids=np.array(["a"], dtype=np.str_),
            x_evaluation=np.array([[3]], dtype=np.float32),
            output=tmp_path / "bad",
            config=RankerConfig(),
        )


@pytest.mark.parametrize("class_ids", [[-1], [6], [0, 0], [0, 1]])
def test_invalid_native_layout_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, class_ids: list[int]
) -> None:
    factory = Mock()
    factory.return_value.classes_ = np.array(class_ids)
    factory.return_value.predict_proba.return_value = np.array([[1.0]])
    monkeypatch.setattr(position_classifier, "CatBoostClassifier", factory)
    monkeypatch.setattr(position_classifier, "Pool", Mock())
    with pytest.raises(ValueError, match="class layout"):
        position_classifier.fit_position_probabilities(
            x_train=np.array([[1]], dtype=np.float32),
            finishes=np.array([1], dtype=np.float32),
            abnormality=np.array(["0"], dtype=np.str_),
            race_ids=np.array(["a"], dtype=np.str_),
            x_evaluation=np.array([[3]], dtype=np.float32),
            output=tmp_path / "bad",
            config=RankerConfig(),
        )


def test_independent_races_do_not_share_assigned_slots() -> None:
    probabilities = np.array(
        [
            [0, 0.6, 0.4, 0, 0, 0],
            [0, 0.2, 0.8, 0, 0, 0],
            [0, 0.3, 0.7, 0, 0, 0],
        ],
        dtype=np.float64,
    )
    assert position_classifier.rank_scores_for_races(
        probabilities, np.array(["a", "b", "a"], dtype=np.str_)
    ).tolist() == [2, 1, 1]


def test_group_decoder_rejects_missing_race_ids() -> None:
    with pytest.raises(ValueError, match="align"):
        position_classifier.rank_scores_for_races(
            np.full((2, 6), 1 / 6), np.array(["a"], dtype=np.str_)
        )


def test_group_decoder_rejects_empty_inputs() -> None:
    with pytest.raises(ValueError, match="No races"):
        position_classifier.rank_scores_for_races(np.empty((0, 6)), np.array([], dtype=np.str_))


def test_nonfinite_native_forecasts_fail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    factory = Mock()
    factory.return_value.classes_ = np.array([0])
    factory.return_value.predict_proba.return_value = np.array([[np.nan]])
    monkeypatch.setattr(position_classifier, "CatBoostClassifier", factory)
    monkeypatch.setattr(position_classifier, "Pool", Mock())
    with pytest.raises(ValueError, match="Nonfinite"):
        position_classifier.fit_position_probabilities(
            x_train=np.array([[1]], dtype=np.float32),
            finishes=np.array([1], dtype=np.float32),
            abnormality=np.array(["0"], dtype=np.str_),
            race_ids=np.array(["a"], dtype=np.str_),
            x_evaluation=np.array([[3]], dtype=np.float32),
            output=tmp_path / "bad",
            config=RankerConfig(),
        )
