"""A bounded market-free ranking intervention; exact-rank evaluation is separate."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import numpy.typing as npt
from catboost import CatBoostRanker, Pool

MAX_RELEVANCE_PLUS_ONE: int = 6
NONCAUSAL_OR_CURRENT_FIELDS: frozenset[str] = frozenset(
    {
        "finish_position",
        "finish_norm",
        "kakutei_chakujun",
        "weather_normalized",
        "track_condition_normalized",
        "rain_x_speed_decay",
        "wind_x_front_runner",
        "wind_x_field_size",
        "rain_x_track_condition",
        "cold_x_speed_effect",
        "weight_diff_from_avg",
        "weight_zscore",
        "zogen_sa",
        "bataiju_futan_ratio",
        "bataiju_per_kyori_log",
        "bataiju_diff_from_race_mean",
        "bataiju_rank_in_race",
        "futan_minus_bataiju_zscore_in_race",
    }
)
MARKET_TOKENS: tuple[str, ...] = ("market", "odds", "popularity", "ninki")


@dataclass(frozen=True)
class RankerConfig:
    iterations: int = 100
    depth: int = 6
    learning_rate: float = 0.05
    seed: int = 20260913
    threads: int = 4

    def __post_init__(self) -> None:
        if min(self.iterations, self.depth, self.threads) < 1:
            raise ValueError("Training dimensions must be positive")
        if not 0 < self.learning_rate <= 1:
            raise ValueError("Learning rate must be in (0, 1]")


def _allowed_feature(name: str) -> bool:
    text = name.lower()
    return (
        name not in NONCAUSAL_OR_CURRENT_FIELDS
        and not text.startswith(("target_", "venue_"))
        and not any(token in text for token in MARKET_TOKENS)
    )


def market_free_features(names: Sequence[str]) -> tuple[str, ...]:
    """Keep order; remove market, labels and unproven same-day feature families."""
    if len(set(names)) != len(names):
        raise ValueError("Feature names must be unique")
    selected = tuple(name for name in names if _allowed_feature(name))
    if not selected:
        raise ValueError("No market-free features remain")
    return selected


def relevance_for_top5(
    finishes: npt.NDArray[np.float32], abnormality: npt.NDArray[np.str_]
) -> npt.NDArray[np.float32]:
    """DNF/DQ have zero Top5 relevance, but their actual finish remains undefined."""
    if finishes.ndim != 1 or finishes.shape != abnormality.shape or len(finishes) == 0:
        raise ValueError("Finishes and source statuses must align")
    if np.any(~np.isin(abnormality, ("0", "4", "5", "6", "7"))):
        raise ValueError("Nonstarters or unknown source status reached the training roster")
    explicit_undefined = np.isin(abnormality, ("4", "5"))
    classified = ~explicit_undefined
    if np.any(~np.isfinite(finishes[classified])) or np.any(finishes[classified] < 1):
        raise ValueError("Unexplained missing or invalid classified finish")
    if np.any(finishes[classified] != np.floor(finishes[classified])):
        raise ValueError("Classified finishes must be integers")
    return np.where(explicit_undefined, 0, np.maximum(MAX_RELEVANCE_PLUS_ONE - finishes, 0)).astype(
        np.float32
    )


def fit_and_predict(
    *,
    x_train: npt.NDArray[np.float32],
    finishes: npt.NDArray[np.float32],
    abnormality: npt.NDArray[np.str_],
    race_ids: npt.NDArray[np.str_],
    x_evaluation: npt.NDArray[np.float32],
    output: Path,
    config: RankerConfig,
) -> npt.NDArray[np.float64]:
    """Train one fresh grouped model; save it without CatBoost scratch output."""
    if x_train.ndim != 2 or x_evaluation.ndim != 2 or x_train.shape[1] != x_evaluation.shape[1]:
        raise ValueError("Feature matrices must have the same column schema")
    if len(x_train) != len(finishes) or race_ids.shape != finishes.shape:
        raise ValueError("Training rows and race groups must align")
    if np.isinf(x_train).any() or np.isinf(x_evaluation).any():
        raise ValueError("Infinite features are invalid; structural missingness must be NaN")
    labels = relevance_for_top5(finishes, abnormality)
    order = np.argsort(race_ids, kind="stable")
    output.mkdir(parents=True, exist_ok=False)
    model = CatBoostRanker(
        loss_function="YetiRank",
        iterations=config.iterations,
        depth=config.depth,
        learning_rate=config.learning_rate,
        random_seed=config.seed,
        l2_leaf_reg=3.0,
        thread_count=config.threads,
        task_type="CPU",
        verbose=False,
        allow_writing_files=False,
    )
    model.fit(Pool(x_train[order], label=labels[order], group_id=race_ids[order].tolist()))
    model.save_model(str(output / "model.cbm"))
    model.save_model(str(output / "model.json"), format="json")
    scores = np.asarray(model.predict(x_evaluation), dtype=np.float64)
    if scores.shape != (len(x_evaluation),) or not np.isfinite(scores).all():
        raise ValueError("Invalid native model forecasts")
    return scores
