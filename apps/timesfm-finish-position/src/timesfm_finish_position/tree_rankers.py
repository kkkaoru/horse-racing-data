"""Portable wrappers for tree learning-to-rank baselines and candidates."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from enum import StrEnum
from typing import Protocol, cast

import numpy as np

from .domain import FloatArray
from .lab_domain import LabIntArray


class RankerKind(StrEnum):
    """Supported tree ranking implementations."""

    LIGHTGBM_LAMBDARANK = "lightgbm-lambdarank"
    LIGHTGBM_XENDCG = "lightgbm-rank-xendcg"
    XGBOOST_PAIRWISE = "xgboost-pairwise"
    CATBOOST_YETIRANK = "catboost-yetirank"


class RankerLike(Protocol):
    """Common inference boundary."""

    def predict(self, features: FloatArray) -> FloatArray:
        """Return one score per runner."""
        ...


class GroupRankerLike(RankerLike, Protocol):
    """LightGBM/XGBoost group-fit boundary."""

    def fit(self, features: FloatArray, labels: FloatArray, *, group: list[int]) -> object:
        """Fit query groups."""
        ...


class CatRankerLike(RankerLike, Protocol):
    """CatBoost group-id fit boundary."""

    def fit(self, features: FloatArray, labels: FloatArray, *, group_id: LabIntArray) -> object:
        """Fit ordered group IDs."""
        ...


class Constructor(Protocol):
    """Dynamic ranker constructor."""

    def __call__(self, **kwargs: object) -> RankerLike:
        """Create a configured ranker."""
        ...


def _constructor(module_name: str, attribute: str) -> Constructor:
    module = importlib.import_module(module_name)
    return cast("Constructor", vars(module)[attribute])


def group_sizes(race_ids: np.ndarray[tuple[int], np.dtype[np.str_]]) -> list[int]:
    """Return contiguous query sizes and reject interleaved races."""
    if len(race_ids) == 0:
        raise ValueError("ranking data must not be empty")
    changes = np.flatnonzero(np.r_[True, race_ids[1:] != race_ids[:-1], True])
    sizes = np.diff(changes).astype(np.int64).tolist()
    seen: set[str] = set()
    previous = ""
    for race_id in race_ids:
        current = str(race_id)
        if current != previous:
            if current in seen:
                raise ValueError(f"race rows are not contiguous: {current}")
            seen.add(current)
            previous = current
    return sizes


def relevance_labels(finish_positions: LabIntArray) -> FloatArray:
    """Use graded Top4 relevance for all ranking objectives."""
    if np.any(finish_positions < 1):
        raise ValueError("finish positions must be positive")
    return np.maximum(5 - finish_positions, 0).astype(np.float64)


def _make_ranker(kind: RankerKind, *, estimators: int, threads: int) -> RankerLike:
    common: dict[str, object] = {
        "n_estimators": estimators,
        "learning_rate": 0.05,
        "random_state": 20260902,
    }
    if kind in (RankerKind.LIGHTGBM_LAMBDARANK, RankerKind.LIGHTGBM_XENDCG):
        objective = "lambdarank" if kind is RankerKind.LIGHTGBM_LAMBDARANK else "rank_xendcg"
        return _constructor("lightgbm", "LGBMRanker")(
            **common,
            objective=objective,
            num_leaves=31,
            n_jobs=threads,
            verbosity=-1,
        )
    if kind is RankerKind.XGBOOST_PAIRWISE:
        return _constructor("xgboost", "XGBRanker")(
            **common,
            objective="rank:pairwise",
            eval_metric="ndcg@3",
            max_depth=6,
            tree_method="hist",
            n_jobs=threads,
        )
    return _constructor("catboost", "CatBoostRanker")(
        iterations=estimators,
        learning_rate=0.05,
        depth=6,
        loss_function="YetiRankPairwise",
        random_seed=20260902,
        thread_count=threads,
        verbose=False,
        allow_writing_files=False,
    )


def fit_ranker(
    kind: RankerKind,
    features: FloatArray,
    finish_positions: LabIntArray,
    race_ids: np.ndarray[tuple[int], np.dtype[np.str_]],
    *,
    estimators: int = 300,
    threads: int = 4,
    factory: Callable[[RankerKind, int, int], RankerLike] | None = None,
) -> RankerLike:
    """Fit one deterministic ranker on already chronological rows."""
    if features.ndim != 2 or features.shape[0] != len(finish_positions):
        raise ValueError("ranking features and labels must align")
    sizes = group_sizes(race_ids)
    labels = relevance_labels(finish_positions)
    ranker = (
        factory(kind, estimators, threads)
        if factory
        else _make_ranker(kind, estimators=estimators, threads=threads)
    )
    if kind is RankerKind.CATBOOST_YETIRANK:
        group_ids = np.repeat(np.arange(len(sizes), dtype=np.int64), sizes)
        cast("CatRankerLike", ranker).fit(features, labels, group_id=group_ids)
    else:
        cast("GroupRankerLike", ranker).fit(features, labels, group=sizes)
    return ranker
