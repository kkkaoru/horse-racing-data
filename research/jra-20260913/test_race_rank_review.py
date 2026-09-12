"""Deterministic complete-roster and prediction-time market tests."""

from datetime import UTC, datetime

import pytest
from race_rank_review import MarketPoint, market_at_cutoff, review_orders


def test_exact_ranks_are_not_winner_topk() -> None:
    result = review_orders(
        predicted=(2, 1, 3, 5, 4),
        actual={1: 1, 2: 2, 3: 3, 4: 4, 5: 5},
        market=(1, 2, 3, 4, 5),
    )
    assert result.model_hits == (0, 0, 1, 0, 0)
    assert result.market_hits == (1, 1, 1, 1, 1)
    assert result.model_minus_market == (-1, -1, 0, -1, -1)
    assert result.support == (1, 1, 1, 1, 1)
    assert result.same_market_positions == (0, 0, 1, 0, 0)
    assert result.identical_market_order is False
    assert result.normalized_order_distance == pytest.approx(1 / 3)


def test_dnf_and_dead_heat_keep_roster_and_rank_support() -> None:
    result = review_orders(predicted=(1, 2, 3), actual={1: 1, 2: 1, 3: None}, market=None)
    assert result.runners == 3
    assert result.model_hits == (1, 0, 0, 0, 0)
    assert result.support == (1, 0, 0, 0, 0)
    assert result.market_hits is None


def test_single_runner_distance_is_defined() -> None:
    result = review_orders(predicted=(1,), actual={1: 1}, market=(1,))
    assert result.identical_market_order is True
    assert result.normalized_order_distance == 0.0
    assert result.same_market_positions == (1, 0, 0, 0, 0)


@pytest.mark.parametrize(
    ("predicted", "actual", "market"),
    [
        ((), {}, None),
        ((1, 1), {1: 1}, None),
        ((1, 2), {1: 1}, None),
        ((0,), {0: 1}, None),
        ((1,), {1: 0}, None),
        ((1,), {1: 2}, None),
        ((1, 2), {1: 1, 2: 2}, (1,)),
        ((1, 2), {1: 1, 2: 2}, (1, 3)),
    ],
)
def test_invalid_rosters_fail(
    predicted: tuple[int, ...], actual: dict[int, int | None], market: tuple[int, ...] | None
) -> None:
    with pytest.raises(ValueError):
        review_orders(predicted=predicted, actual=actual, market=market)


def test_prediction_time_ignores_future_prices_and_other_runners() -> None:
    points = [
        MarketPoint(1, datetime(2026, 9, 12, 0, tzinfo=UTC), 5.0),
        MarketPoint(1, datetime(2026, 9, 12, 2, tzinfo=UTC), 1.1),
        MarketPoint(2, datetime(2026, 9, 12, 0, tzinfo=UTC), 2.0),
        MarketPoint(3, datetime(2026, 9, 12, 0, tzinfo=UTC), 1.0),
        MarketPoint(1, datetime(2026, 9, 11, 22, tzinfo=UTC), 8.0),
        MarketPoint(1, datetime(2026, 9, 12, 0, tzinfo=UTC), 5.0),
    ]
    assert market_at_cutoff(
        points=points, horses=(1, 2), cutoff=datetime(2026, 9, 12, 1, tzinfo=UTC)
    ) == (2, 1)


def test_latest_quote_and_odds_tie_are_deterministic() -> None:
    assert market_at_cutoff(
        points=[
            MarketPoint(1, datetime(2026, 9, 11, 22, tzinfo=UTC), 8.0),
            MarketPoint(1, datetime(2026, 9, 12, 0, tzinfo=UTC), 2.0),
            MarketPoint(2, datetime(2026, 9, 12, 0, tzinfo=UTC), 2.0),
        ],
        horses=(2, 1),
        cutoff=datetime(2026, 9, 12, 1, tzinfo=UTC),
    ) == (1, 2)


def test_missing_horse_quote_is_not_imputed() -> None:
    assert (
        market_at_cutoff(
            points=[MarketPoint(1, datetime(2026, 9, 12, tzinfo=UTC), 2.0)],
            horses=(1, 2),
            cutoff=datetime(2026, 9, 12, 1, tzinfo=UTC),
        )
        is None
    )


def test_conflicting_same_time_quotes_fail() -> None:
    with pytest.raises(ValueError, match="Conflicting"):
        market_at_cutoff(
            points=[
                MarketPoint(1, datetime(2026, 9, 12, tzinfo=UTC), 2.0),
                MarketPoint(1, datetime(2026, 9, 12, tzinfo=UTC), 3.0),
            ],
            horses=(1,),
            cutoff=datetime(2026, 9, 12, 1, tzinfo=UTC),
        )


@pytest.mark.parametrize("horses", [(), (1, 1)])
def test_market_roster_must_be_unique(horses: tuple[int, ...]) -> None:
    with pytest.raises(ValueError, match="roster"):
        market_at_cutoff(points=[], horses=horses, cutoff=datetime(2026, 9, 12, tzinfo=UTC))


def test_cutoff_must_be_aware() -> None:
    with pytest.raises(ValueError, match="timezone"):
        market_at_cutoff(points=[], horses=(1,), cutoff=datetime(2026, 9, 12))


@pytest.mark.parametrize(("horse", "odds"), [(0, 1.0), (1, 0.0), (1, float("nan"))])
def test_invalid_market_point(horse: int, odds: float) -> None:
    with pytest.raises(ValueError, match="positive"):
        MarketPoint(horse, datetime(2026, 9, 12, tzinfo=UTC), odds)


def test_quote_must_be_aware() -> None:
    with pytest.raises(ValueError, match="timezone"):
        MarketPoint(1, datetime(2026, 9, 12), 2.0)
