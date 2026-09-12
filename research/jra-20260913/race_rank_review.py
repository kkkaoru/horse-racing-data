"""Paired exact-position diagnostics with complete rosters and causal market snapshots."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

EXACT_RANKS: tuple[int, ...] = (1, 2, 3, 4, 5)


@dataclass(frozen=True)
class MarketPoint:
    horse_number: int
    fetched_at: datetime
    odds: float

    def __post_init__(self) -> None:
        if self.horse_number < 1 or not math.isfinite(self.odds) or self.odds <= 0:
            raise ValueError("Market points require a positive horse number and finite odds")
        if self.fetched_at.utcoffset() is None:
            raise ValueError("Market timestamps must be timezone aware")


@dataclass(frozen=True)
class RankReview:
    runners: int
    support: tuple[int, ...]
    model_hits: tuple[int, ...]
    market_hits: tuple[int, ...] | None
    model_minus_market: tuple[int, ...] | None
    same_market_positions: tuple[int, ...] | None
    identical_market_order: bool | None
    normalized_order_distance: float | None


def _market_key(point: MarketPoint) -> tuple[float, int]:
    return point.odds, point.horse_number


def market_at_cutoff(
    *, points: Sequence[MarketPoint], horses: tuple[int, ...], cutoff: datetime
) -> tuple[int, ...] | None:
    """Use each runner's latest available quote; never substitute a later quote."""
    if cutoff.utcoffset() is None:
        raise ValueError("Prediction cutoff must be timezone aware")
    if len(set(horses)) != len(horses) or not horses:
        raise ValueError("Prediction roster must be nonempty and unique")
    latest: dict[int, MarketPoint] = {}
    for point in points:
        if point.fetched_at > cutoff or point.horse_number not in horses:
            continue
        previous = latest.get(point.horse_number)
        if previous is not None and previous.fetched_at == point.fetched_at:
            if previous.odds != point.odds:
                raise ValueError("Conflicting market quotes at the same timestamp")
            continue
        if previous is None or point.fetched_at > previous.fetched_at:
            latest[point.horse_number] = point
    if set(latest) != set(horses):
        return None
    return tuple(point.horse_number for point in sorted(latest.values(), key=_market_key))


def _validate_orders(
    *, predicted: tuple[int, ...], actual: Mapping[int, int | None], market: tuple[int, ...] | None
) -> None:
    if not predicted or len(set(predicted)) != len(predicted) or set(predicted) != set(actual):
        raise ValueError("Prediction and result rosters must match exactly without duplicates")
    if any(horse < 1 for horse in predicted):
        raise ValueError("Horse numbers must be positive")
    if any(rank is not None and (rank < 1 or rank > len(predicted)) for rank in actual.values()):
        raise ValueError("Undefined finishes must be None, not fabricated numeric ranks")
    if market is not None and (len(market) != len(predicted) or set(market) != set(predicted)):
        raise ValueError("Market roster must match the complete prediction roster")


def _exact_hits(order: tuple[int, ...], actual: Mapping[int, int | None]) -> tuple[int, ...]:
    return tuple(
        int(rank <= len(order) and actual[order[rank - 1]] == rank) for rank in EXACT_RANKS
    )


def review_orders(
    *, predicted: tuple[int, ...], actual: Mapping[int, int | None], market: tuple[int, ...] | None
) -> RankReview:
    """Measure exact ranks, not winner inclusion; retain DNF/DQ as undefined finishes."""
    _validate_orders(predicted=predicted, actual=actual, market=market)
    hits = _exact_hits(predicted, actual)
    support = tuple(int(rank in actual.values()) for rank in EXACT_RANKS)
    if market is None:
        return RankReview(len(predicted), support, hits, None, None, None, None, None)
    market_hits = _exact_hits(market, actual)
    positions = {horse: index for index, horse in enumerate(market)}
    distance = sum(abs(index - positions[horse]) for index, horse in enumerate(predicted))
    maximum_distance = len(predicted) ** 2 // 2
    return RankReview(
        runners=len(predicted),
        support=support,
        model_hits=hits,
        market_hits=market_hits,
        model_minus_market=tuple(a - b for a, b in zip(hits, market_hits, strict=True)),
        same_market_positions=tuple(
            int(rank <= len(predicted) and predicted[rank - 1] == market[rank - 1])
            for rank in EXACT_RANKS
        ),
        identical_market_order=predicted == market,
        normalized_order_distance=distance / maximum_distance if maximum_distance else 0.0,
    )
