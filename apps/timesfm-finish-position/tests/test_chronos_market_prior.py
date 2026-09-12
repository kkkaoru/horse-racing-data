import numpy as np
import pytest

from timesfm_finish_position.chronos_market_prior import market_scores, parse_market_prior


def test_expected_rank_preserves_prices_and_ties() -> None:
    scores = market_scores(
        odds=np.array([2.0, 4.0, 4.0]),
        horse_ids=np.array(["a", "c", "b"]),
        prior="pl_expected_performance",
    )
    np.testing.assert_allclose(scores, [2 / 3, 5 / 12, 5 / 12])
    assert scores.sum() == pytest.approx(1.5)


def test_original_rank_prior_preserves_deterministic_tie_break() -> None:
    scores = market_scores(
        odds=np.array([2.0, 4.0, 4.0]), horse_ids=np.array(["a", "c", "b"]), prior="rank_percentile"
    )
    assert scores.tolist() == [1.0, 0.0, 0.5]


def test_singleton_and_extreme_finite_prices() -> None:
    assert market_scores(
        odds=np.array([1.0]), horse_ids=np.array(["a"]), prior="pl_expected_performance"
    ).tolist() == [1.0]
    np.testing.assert_allclose(
        market_scores(
            odds=np.array([1e308, 1e308]),
            horse_ids=np.array(["a", "b"]),
            prior="pl_expected_performance",
        ),
        [0.5, 0.5],
    )


@pytest.mark.parametrize("prices", [[], [1.0, 2.0], [[1.0]]])
def test_invalid_alignment(prices: list[float] | list[list[float]]) -> None:
    with pytest.raises(ValueError, match="aligned"):
        market_scores(
            odds=np.asarray(prices, dtype=np.float64),
            horse_ids=np.array(["a"]),
            prior="rank_percentile",
        )


@pytest.mark.parametrize("price", [0.0, -1.0, float("nan"), float("inf")])
def test_invalid_prices(price: float) -> None:
    with pytest.raises(ValueError, match="odds"):
        market_scores(
            odds=np.array([price]), horse_ids=np.array(["a"]), prior="pl_expected_performance"
        )


@pytest.mark.parametrize("value", [None, "unknown", 1])
def test_invalid_prior(value: object) -> None:
    with pytest.raises(ValueError, match="prior"):
        parse_market_prior(value)
