"""Tests for shared feature-selection policy."""

from __future__ import annotations

import learning.feature_selection_policy as subject


def test_resolve_finish_position_features_excludes_meta_and_labels() -> None:
    columns = [
        "race_id",
        "finish_position",
        "target_corner_2_norm",
        "target_running_style_class",
        "speed_index",
        "rs_p_nige",
    ]
    assert subject.resolve_feature_columns_for_target(
        columns, "finish_position"
    ) == ["speed_index", "rs_p_nige"]


def test_resolve_finish_position_features_bans_market_columns_but_keeps_deviation() -> None:
    columns = [
        "tansho_odds",
        "tansho_ninkijun_raw",
        "popularity_score",
        "odds_score",
        "inverse_odds_implied_prob",
        "horse_odds_avg",
        "horse_popularity_norm_avg",
        "field_dominant_favorite_indicator",
        "form_market_edge",
        "sim_odds_rank_correlation",
        "market_entropy",
        "exotic_sanrenpuku_p3",
        "exotic_umaren_p2",
        "exotic_wide_p3",
        "exotic_umaren_sanrenpuku_place_shape",
        "exotic_umaren_p2_rank_residual",
        "exotic_sanrenpuku_p3_rank_residual",
        "speed_index",
    ]
    assert subject.resolve_feature_columns_for_target(columns, "finish_position") == [
        "exotic_umaren_p2_rank_residual",
        "exotic_sanrenpuku_p3_rank_residual",
        "speed_index",
    ]


def test_is_banned_market_feature_flags_exotic_order_pools() -> None:
    assert subject.is_banned_market_feature("exotic_umatan_p1") is True
    assert subject.is_banned_market_feature("sim_fav_win_rate") is True
    assert subject.is_banned_market_feature("exotic_umaren_p2_rank_residual") is False
    assert subject.is_banned_market_feature("kohan3f_avg_5") is False


def test_resolve_running_style_features_bans_market_columns() -> None:
    assert subject.resolve_feature_columns_for_target(
        ["tansho_odds", "popularity_score", "odds_score", "speed_index"], "running_style"
    ) == ["speed_index"]


def test_resolve_running_style_features_excludes_leakage_and_cell_columns() -> None:
    columns = [
        "race_id",
        "bamei",
        "target_corner_2_norm",
        "target_running_style_class",
        "rs_p_nige",
        "__rs_cell_surface",
        "speed_index",
    ]
    assert subject.resolve_feature_columns_for_target(
        columns, "running_style"
    ) == ["speed_index"]


def test_compute_feature_set_hash_is_order_independent_and_duplicate_free() -> None:
    left = subject.compute_feature_set_hash(["b", "a", "a"])
    right = subject.compute_feature_set_hash(["a", "b"])
    assert left == right
    assert len(left) == 64


def test_normalize_feature_names_strips_empty_and_duplicate_names() -> None:
    result = subject.normalize_feature_names([" feat_b ", "", "feat_a", "feat_a "])
    assert result == ["feat_a", "feat_b"]


def test_compute_feature_set_hash_ignores_whitespace_and_empty_names() -> None:
    left = subject.compute_feature_set_hash([" feat_b ", "", "feat_a", "feat_a "])
    right = subject.compute_feature_set_hash(["feat_a", "feat_b"])
    assert left == right


def test_build_feature_selection_spec_normalizes_names() -> None:
    spec = subject.build_feature_selection_spec("running_style", [" z ", "a", "z"])
    assert spec.prediction_target == "running_style"
    assert spec.feature_names == ("a", "z")
    assert spec.feature_set_hash == subject.compute_feature_set_hash(["a", "z"])
