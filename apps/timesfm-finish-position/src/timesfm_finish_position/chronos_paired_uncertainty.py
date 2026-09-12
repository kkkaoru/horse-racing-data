"""Descriptive paired-race uncertainty; not selection-adjusted inference."""

import math
from dataclasses import dataclass
from fractions import Fraction

import numpy as np
import numpy.typing as npt

RANKS: int = 5
DEFAULT_BOOTSTRAP_DRAWS: int = 10000
DEFAULT_BOOTSTRAP_SEED: int = 20260912
CONFIDENCE_QUANTILES: tuple[float, float] = (0.025, 0.975)


@dataclass(frozen=True)
class PairedUncertainty:
    races: int
    blocks: int
    delta: list[int]
    gains: list[int]
    losses: list[int]
    race_bootstrap_count_interval_95: list[list[float]]
    block_bootstrap_rate_pp_interval_95: list[list[float]]
    mcnemar_exact_two_sided_p: list[float]
    draws: int
    seed: int


def _mcnemar(gains: int, losses: int) -> float:
    discordant = gains + losses
    tail = sum(math.comb(discordant, k) for k in range(min(gains, losses) + 1))
    return min(1.0, float(Fraction(2 * tail, 2**discordant)))


def paired_effect_uncertainty(
    effects: npt.NDArray[np.generic],
    blocks: npt.NDArray[np.str_],
    *,
    draws: int = DEFAULT_BOOTSTRAP_DRAWS,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> PairedUncertainty:
    """Bootstrap full five-rank effect vectors, preserving within-race dependence.

    Multinomial sampling of distinct vectors equals resampling individual races.
    Block resampling uses pooled count differences / pooled race counts, not an
    unweighted average of unequal-sized monthly rates. Empirical zero intervals
    do not rule out unobserved future effects. Adaptive selection and repeated
    horses are not corrected by the race bootstrap.
    """
    if effects.ndim != 2 or effects.shape[1] != RANKS or len(effects) < 1:
        raise ValueError("Expected nonempty five-rank race effect matrix")
    if blocks.shape != (len(effects),) or draws < 1:
        raise ValueError("Invalid block labels or bootstrap draw count")
    if not np.issubdtype(effects.dtype, np.integer) or not np.isin(effects, [-1, 0, 1]).all():
        raise ValueError("Race effects must be integer differences in -1, 0, 1")
    validated = effects.astype(np.int64, copy=False)
    rng = np.random.default_rng(seed)
    vectors, counts = np.unique(validated, axis=0, return_counts=True)
    race_draws = rng.multinomial(len(effects), counts / len(effects), size=draws) @ vectors
    names, inverse = np.unique(blocks, return_inverse=True)
    block_counts = np.bincount(inverse)
    block_effects = np.zeros((len(names), RANKS), dtype=np.int64)
    np.add.at(block_effects, inverse, validated)
    weights = rng.multinomial(len(names), np.full(len(names), 1 / len(names)), size=draws)
    block_rates = 100 * (weights @ block_effects) / (weights @ block_counts)[:, None]
    gains = np.count_nonzero(effects == 1, axis=0).tolist()
    losses = np.count_nonzero(effects == -1, axis=0).tolist()
    return PairedUncertainty(
        len(effects),
        len(names),
        validated.sum(axis=0).tolist(),
        gains,
        losses,
        np.quantile(race_draws, CONFIDENCE_QUANTILES, axis=0).T.tolist(),
        np.quantile(block_rates, CONFIDENCE_QUANTILES, axis=0).T.tolist(),
        [_mcnemar(gain, loss) for gain, loss in zip(gains, losses, strict=True)],
        draws,
        seed,
    )
