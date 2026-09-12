"""Market priors for separately declared development-only readout comparisons."""

from typing import Literal

import numpy as np
import numpy.typing as npt

MarketPrior = Literal["rank_percentile", "pl_expected_performance"]
PRIOR_NAMES: tuple[MarketPrior, ...] = ("rank_percentile", "pl_expected_performance")


def parse_market_prior(value: object) -> MarketPrior:
    for prior in PRIOR_NAMES:
        if isinstance(value, str) and value == prior:
            return prior
    raise ValueError("Unknown market prior")


def market_scores(
    *, odds: npt.NDArray[np.float64], horse_ids: npt.NDArray[np.str_], prior: MarketPrior
) -> npt.NDArray[np.float64]:
    """PL prior is expected normalized rank, not a fitted win-probability model.

    It preserves market ordering and equal-price ties, while retaining odds
    confidence that equally spaced rank percentiles discard.
    """
    prior = parse_market_prior(prior)
    if odds.ndim != 1 or horse_ids.shape != odds.shape or not len(odds):
        raise ValueError("Market runners must be nonempty and aligned")
    if not np.all(np.isfinite(odds) & (odds >= 1)):
        raise ValueError("Market odds must be finite and at least one")
    if prior == "rank_percentile":
        scores = np.empty(len(odds), dtype=np.float64)
        scores[np.lexsort((horse_ids, odds))] = np.linspace(1, 0, len(odds))
        return scores
    if len(odds) == 1:
        return np.ones(1, dtype=np.float64)
    strength = 1.0 / odds
    strength /= strength.max()
    pairwise = strength[:, None] / (strength[:, None] + strength[None, :])
    return (pairwise.sum(axis=1) - 0.5) / (len(odds) - 1)
