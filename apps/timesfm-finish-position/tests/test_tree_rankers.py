from __future__ import annotations

import numpy as np
import pytest

from timesfm_finish_position.tree_rankers import (
    RankerKind,
    fit_ranker,
    group_sizes,
    relevance_labels,
)


class FakeRanker:
    def __init__(self) -> None:
        self.group: list[int] | None = None
        self.group_id: np.ndarray | None = None

    def fit(
        self,
        _features: np.ndarray,
        _labels: np.ndarray,
        *,
        group: list[int] | None = None,
        group_id: np.ndarray | None = None,
    ) -> object:
        self.group = group
        self.group_id = group_id
        return self

    def predict(self, features: np.ndarray) -> np.ndarray:
        return np.zeros(len(features), dtype=np.float64)


def _training_rows() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    return (
        np.asarray([[1.0], [2.0], [3.0], [4.0]], dtype=np.float64),
        np.asarray([1, 2, 2, 1], dtype=np.int64),
        np.asarray(["r1", "r1", "r2", "r2"], dtype=np.str_),
    )


def test_group_sizes_and_relevance_labels() -> None:
    _, finish, races = _training_rows()
    assert group_sizes(races) == [2, 2]
    assert relevance_labels(finish).tolist() == [4.0, 3.0, 3.0, 4.0]
    with pytest.raises(ValueError, match="must not be empty"):
        group_sizes(np.asarray([], dtype=np.str_))
    with pytest.raises(ValueError, match="not contiguous"):
        group_sizes(np.asarray(["r1", "r2", "r1"], dtype=np.str_))
    with pytest.raises(ValueError, match="must be positive"):
        relevance_labels(np.asarray([0], dtype=np.int64))


@pytest.mark.parametrize("kind", tuple(RankerKind))
def test_fit_ranker_passes_correct_query_contract(kind: RankerKind) -> None:
    features, finish, races = _training_rows()
    created: list[FakeRanker] = []

    def factory(_kind: RankerKind, estimators: int, threads: int) -> FakeRanker:
        assert estimators == 12
        assert threads == 2
        model = FakeRanker()
        created.append(model)
        return model

    result = fit_ranker(
        kind,
        features,
        finish,
        races,
        estimators=12,
        threads=2,
        factory=factory,
    )
    assert result is created[0]
    if kind is RankerKind.CATBOOST_YETIRANK:
        assert created[0].group is None
        assert created[0].group_id is not None
        assert created[0].group_id.tolist() == [0, 0, 1, 1]
    else:
        assert created[0].group == [2, 2]
        assert created[0].group_id is None


def test_fit_ranker_rejects_misaligned_features() -> None:
    _, finish, races = _training_rows()
    with pytest.raises(ValueError, match="must align"):
        fit_ranker(
            RankerKind.XGBOOST_PAIRWISE,
            np.zeros((3, 1), dtype=np.float64),
            finish,
            races,
        )
