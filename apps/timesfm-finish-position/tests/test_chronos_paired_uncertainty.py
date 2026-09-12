"""Resampling preserves the paired five-rank vector and explicit supports."""

import numpy as np
import numpy.typing as npt
import pytest

from timesfm_finish_position.chronos_paired_uncertainty import paired_effect_uncertainty


def test_homogeneous_vectors_and_unequal_block_sizes() -> None:
    effects = np.array([[1, -1, 0, 0, 0], [1, -1, 0, 0, 0], [1, -1, 0, 0, 0], [1, -1, 0, 0, 0]])
    result = paired_effect_uncertainty(effects, np.array(["202401", "202401", "202401", "202402"]))
    assert result.races == 4
    assert result.draws == 10000
    assert result.seed == 20260912
    assert result.blocks == 2
    assert result.delta == [4, -4, 0, 0, 0]
    assert result.gains == [4, 0, 0, 0, 0]
    assert result.losses == [0, 4, 0, 0, 0]
    assert result.race_bootstrap_count_interval_95 == [[4, 4], [-4, -4], [0, 0], [0, 0], [0, 0]]
    assert result.block_bootstrap_rate_pp_interval_95 == [
        [100, 100],
        [-100, -100],
        [0, 0],
        [0, 0],
        [0, 0],
    ]
    assert result.mcnemar_exact_two_sided_p == [0.125, 0.125, 1.0, 1.0, 1.0]


def test_block_rates_use_pooled_race_counts() -> None:
    effects = np.array(
        [
            [1, 0, 0, 0, 0],
            [1, 0, 0, 0, 0],
            [1, 0, 0, 0, 0],
            [1, 0, 0, 0, 0],
            [1, 0, 0, 0, 0],
            [1, 0, 0, 0, 0],
            [1, 0, 0, 0, 0],
            [1, 0, 0, 0, 0],
            [1, 0, 0, 0, 0],
            [-1, 0, 0, 0, 0],
            [-1, 0, 0, 0, 0],
            [-1, 0, 0, 0, 0],
        ]
    )
    result = paired_effect_uncertainty(
        effects, np.array(["a", "a", "a", "a", "a", "a", "a", "a", "a", "b", "c", "d"])
    )
    assert result.delta == [6, 0, 0, 0, 0]
    assert result.block_bootstrap_rate_pp_interval_95[0] == pytest.approx([-100, 92.85714285714286])


def test_balanced_discordance() -> None:
    result = paired_effect_uncertainty(
        np.array([[1, 0, 0, 0, 0], [-1, 0, 0, 0, 0]]),
        np.array(["a", "b"]),
    )
    assert result.delta == [0, 0, 0, 0, 0]
    assert result.race_bootstrap_count_interval_95 == [[-2, 2], [0, 0], [0, 0], [0, 0], [0, 0]]
    assert result.block_bootstrap_rate_pp_interval_95 == [
        [-100, 100],
        [0, 0],
        [0, 0],
        [0, 0],
        [0, 0],
    ]
    assert result.mcnemar_exact_two_sided_p == [1.0, 1.0, 1.0, 1.0, 1.0]


def test_zero_discordance_is_empirically_degenerate() -> None:
    result = paired_effect_uncertainty(np.zeros((1, 5), dtype=np.int64), np.array(["a"]), draws=1)
    assert result.race_bootstrap_count_interval_95 == [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]
    assert result.mcnemar_exact_two_sided_p == [1.0, 1.0, 1.0, 1.0, 1.0]


@pytest.mark.parametrize(
    "effects", [np.zeros((0, 5), dtype=np.int64), np.zeros(5), np.zeros((1, 4))]
)
def test_invalid_matrix_shape(effects: npt.NDArray[np.generic]) -> None:
    with pytest.raises(ValueError, match="effect matrix"):
        paired_effect_uncertainty(effects, np.array(["a"]))


def test_mismatched_blocks_and_invalid_draws() -> None:
    with pytest.raises(ValueError, match="block labels"):
        paired_effect_uncertainty(np.zeros((1, 5), dtype=np.int64), np.array([], dtype=np.str_))
    with pytest.raises(ValueError, match="draw count"):
        paired_effect_uncertainty(np.zeros((1, 5), dtype=np.int64), np.array(["a"]), draws=0)


@pytest.mark.parametrize("effects", [np.full((1, 5), 2), np.zeros((1, 5))])
def test_nondiscrete_effects(effects: npt.NDArray[np.generic]) -> None:
    with pytest.raises(ValueError, match="integer differences"):
        paired_effect_uncertainty(effects, np.array(["a"]))
