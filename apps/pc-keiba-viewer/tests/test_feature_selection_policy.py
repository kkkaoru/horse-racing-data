"""Tests for shared feature-selection policy."""

from __future__ import annotations

import learning.feature_selection_policy as subject


def test_resolve_finish_position_features_excludes_meta_and_labels() -> None:
    columns = [
        "race_id",
        "finish_position",
        "target_running_style_class",
        "speed_index",
        "rs_p_nige",
    ]
    assert subject.resolve_feature_columns_for_target(
        columns, "finish_position"
    ) == ["speed_index", "rs_p_nige"]


def test_resolve_running_style_features_excludes_leakage_and_cell_columns() -> None:
    columns = [
        "race_id",
        "bamei",
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


def test_build_feature_selection_spec_normalizes_names() -> None:
    spec = subject.build_feature_selection_spec("running_style", ["z", "a", "z"])
    assert spec.prediction_target == "running_style"
    assert spec.feature_names == ("a", "z")
    assert spec.feature_set_hash == subject.compute_feature_set_hash(["a", "z"])
