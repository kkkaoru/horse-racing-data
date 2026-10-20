from __future__ import annotations

from predict_lib.market_feature_policy import is_banned_market_feature


def test_is_banned_market_feature_bans_win_odds_and_popularity():
    assert is_banned_market_feature("tansho_odds") is True
    assert is_banned_market_feature("odds_score") is True
    assert is_banned_market_feature("popularity_score") is True
    assert is_banned_market_feature("exotic_umaren_implied_prob") is True


def test_is_banned_market_feature_allows_deviation_features_and_non_market_columns():
    assert is_banned_market_feature("exotic_umaren_p2_rank_residual") is False
    assert is_banned_market_feature("exotic_sanrenpuku_p3_rank_residual") is False
    assert is_banned_market_feature("speed_index_avg_5") is False
