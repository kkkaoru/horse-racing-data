from __future__ import annotations

import numpy as np
import pytest
import torch

from timesfm_finish_position.pytorch_mpx import (
    RaceSetTransformer,
    graded_relevance,
    is_banned_odds_feature,
    is_current_race_outcome_feature,
    listnet_loss,
    pad_races,
    resolve_mpx_device,
    select_numeric_features,
    winner_contained_hits,
)


def test_odds_and_popularity_columns_are_banned() -> None:
    assert is_banned_odds_feature("tansho_odds") is True
    assert is_banned_odds_feature("inverse_odds_implied_prob") is True
    assert is_banned_odds_feature("popularity_rank_in_race") is True
    assert is_banned_odds_feature("career_win_rate") is False


def test_select_numeric_features_drops_identity_and_odds() -> None:
    assert is_current_race_outcome_feature("finish_norm") is True
    assert is_current_race_outcome_feature("last_race_finish_norm") is False
    selected = select_numeric_features(
        [
            "race_id",
            "finish_position",
            "finish_norm",
            "target_corner_1_norm",
            "tansho_odds",
            "career_win_rate",
            "speed_index_avg_5",
            "last_race_finish_norm",
        ]
    )
    assert selected == ["career_win_rate", "speed_index_avg_5", "last_race_finish_norm"]


def test_select_numeric_features_requires_remaining_columns() -> None:
    with pytest.raises(ValueError, match="no odds-free"):
        select_numeric_features(["race_id", "tansho_odds"])


def test_resolve_mpx_device_honors_cpu_and_rejects_unknown() -> None:
    assert resolve_mpx_device("cpu") == torch.device("cpu")
    with pytest.raises(ValueError, match="unsupported torch"):
        resolve_mpx_device("tpu")
    if torch.backends.mps.is_available():
        assert resolve_mpx_device("mps") == torch.device("mps")


def test_graded_relevance_normalizes_finish_order() -> None:
    relevance = graded_relevance(np.asarray([1.0, 2.0, 3.0]))
    assert relevance.tolist() == pytest.approx(
        [0.469278727455, 0.296081727926, 0.234639544619], rel=1e-6
    )


def test_pad_races_masks_empty_slots_and_listnet_is_finite() -> None:
    features, targets, mask = pad_races(
        [np.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)],
        [np.asarray([0.6, 0.4], dtype=np.float64)],
        width=2,
        maximum_runners=3,
    )
    assert features.shape == (1, 3, 2)
    assert mask.tolist() == [[True, True, False]]
    single, single_target, single_mask = pad_races(
        [np.asarray([1.0, 0.0], dtype=np.float32)],
        [np.asarray([1.0], dtype=np.float64)],
        width=2,
        maximum_runners=2,
    )
    assert single_mask.tolist() == [[True, False]]
    assert single[0, 0].tolist() == [1.0, 0.0]
    assert single_target[0, 0].item() == 1.0
    model = RaceSetTransformer(2, embedding_dim=8, heads=2, layers=1, dropout=0.0)
    loss = listnet_loss(model(features, mask), targets, mask)
    assert torch.isfinite(loss)
    assert loss.item() > 0.0


def test_winner_contained_hits_counts_each_topk_cutoff() -> None:
    hits = winner_contained_hits(
        [np.asarray([0.9, 0.2, 0.1]), np.asarray([0.1, 0.8, 0.3])],
        [np.asarray([1.0, 2.0, 3.0]), np.asarray([2.0, 3.0, 1.0])],
    )
    assert hits == {"top1": 1, "top2": 2, "top3": 2, "top4": 2, "top5": 2}
    tied = winner_contained_hits(
        [np.asarray([0.0, 0.0, 0.0])],
        [np.asarray([3.0, 2.0, 1.0])],
    )
    assert tied == {"top1": 0, "top2": 0, "top3": 1, "top4": 1, "top5": 1}


def test_race_set_transformer_rejects_invalid_sizes() -> None:
    with pytest.raises(ValueError, match="divisible"):
        RaceSetTransformer(4, embedding_dim=6, heads=4)
    with pytest.raises(ValueError, match="feature_count"):
        RaceSetTransformer(0)
    with pytest.raises(ValueError, match="dropout"):
        RaceSetTransformer(4, embedding_dim=8, heads=2, dropout=1.0)
    with pytest.raises(ValueError, match="positive"):
        RaceSetTransformer(4, embedding_dim=8, heads=2, layers=-1)
    mlp = RaceSetTransformer(2, embedding_dim=8, heads=2, layers=0, dropout=0.0)
    features = torch.zeros((1, 3, 2))
    mask = torch.tensor([[True, True, False]])
    scores = mlp(features, mask)
    assert scores.shape == (1, 3)
    assert scores[0, 2].item() == -1e9


def test_pad_and_hit_helpers_reject_misaligned_races() -> None:
    with pytest.raises(ValueError, match="same races"):
        pad_races([np.zeros((2, 1))], [], width=1)
    with pytest.raises(ValueError, match="same races"):
        winner_contained_hits([np.asarray([1.0])], [])
    with pytest.raises(ValueError, match="aligned"):
        winner_contained_hits([np.asarray([1.0, 0.0])], [np.asarray([1.0])])
    with pytest.raises(ValueError, match=r"shape \(runners, width\)"):
        pad_races([np.zeros((2,), dtype=np.float32)], [np.zeros(2)], width=1)
    empty = graded_relevance(np.asarray([np.nan, np.nan]))
    assert empty.tolist() == [0.0, 0.0]
    with pytest.raises(ValueError, match="runner count"):
        pad_races(
            [np.zeros((2, 1), dtype=np.float32)],
            [np.zeros(1, dtype=np.float64)],
            width=1,
        )
    with pytest.raises(ValueError, match="aligned"):
        winner_contained_hits([np.asarray([])], [np.asarray([])])


def test_resolve_mpx_device_auto_returns_a_known_backend() -> None:
    device = resolve_mpx_device("auto")
    assert device.type in {"mps", "cuda", "cpu"}


def test_resolve_mpx_device_requires_requested_accelerator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(torch.backends.mps, "is_available", lambda: False)
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    with pytest.raises(RuntimeError, match="MPS"):
        resolve_mpx_device("mps")
    with pytest.raises(RuntimeError, match="CUDA"):
        resolve_mpx_device("cuda")
    assert resolve_mpx_device("auto") == torch.device("cpu")
