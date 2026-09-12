"""Explicit rank-gain contracts shared by offline evaluation and artifact training."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final, Literal

import numpy as np

RelevanceMode = Literal["top3", "reciprocal-rank"]
RELEVANCE_MODES: Final[tuple[RelevanceMode, ...]] = ("top3", "reciprocal-rank")
DEFAULT_RELEVANCE_MODE: Final[RelevanceMode] = "top3"


def parse_relevance_mode(value: object) -> RelevanceMode:
    if value == "top3":
        return "top3"
    if value == "reciprocal-rank":
        return "reciprocal-rank"
    raise ValueError("relevance mode must be top3 or reciprocal-rank")


def rank_gains(ranks: Sequence[float], *, mode: RelevanceMode) -> np.ndarray:
    """Preserve ties and reject invalid finishes before generating monotone gains.

    Reciprocal rank retains a decreasing gain below third place without making
    larger fields automatically produce higher winner gains. It is an offline
    hypothesis, not a calibrated probability or a claim of accuracy improvement.
    """
    checked_mode = parse_relevance_mode(mode)
    values = np.asarray(ranks, dtype=np.float64)
    if values.ndim != 1 or not bool(
        (np.isfinite(values) & (values > 0) & (values == np.floor(values))).all()
    ):
        raise ValueError("rank gains require positive finite integer finishes")
    if checked_mode == "reciprocal-rank":
        return np.reciprocal(values)
    return np.maximum(4.0 - values, 0.0)
