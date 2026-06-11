from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import per_class_ensemble_lib as subject


# ---------------------------------------------------------------------------
# Class-code helpers
# ---------------------------------------------------------------------------


def test_is_other_class_true_for_other_label():
    assert subject.is_other_class("other") is True


def test_is_other_class_false_for_named_class():
    assert subject.is_other_class("005") is False


def test_is_other_class_false_for_empty_string():
    assert subject.is_other_class("") is False


def test_class_filter_mask_named_code_exact_match():
    codes = pd.Series(["005", "010", "005", None])
    mask = subject.class_filter_mask(codes, "005")
    assert mask.tolist() == [True, False, True, False]


def test_class_filter_mask_other_includes_nan_and_unknown_codes():
    codes = pd.Series(["005", "010", None, "999", "703"])
    mask = subject.class_filter_mask(codes, "other")
    assert mask.tolist() == [False, False, True, True, False]


def test_class_filter_mask_other_with_all_known_codes_returns_all_false():
    codes = pd.Series(["005", "010", "016", "703", "701"])
    mask = subject.class_filter_mask(codes, "other")
    assert mask.tolist() == [False, False, False, False, False]


# ---------------------------------------------------------------------------
# normalize_within_race
# ---------------------------------------------------------------------------


def test_normalize_within_race_three_horse_race_returns_evenly_spaced_scores():
    df = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_score": [10.0, 5.0, 1.0],
        "actual_finish_position": [1, 2, 3],
    })
    out = subject.normalize_within_race(df)
    out_sorted = out.sort_values("predicted_score", ascending=False).reset_index(
        drop=True,
    )
    assert out_sorted["normalized_score"].tolist() == [1.0, 0.5, 0.0]


def test_normalize_within_race_single_horse_race_returns_half():
    df = pd.DataFrame({
        "race_id": ["r1"],
        "ketto_toroku_bango": ["a"],
        "predicted_score": [10.0],
        "actual_finish_position": [1],
    })
    out = subject.normalize_within_race(df)
    assert out["normalized_score"].tolist() == [0.5]


def test_normalize_within_race_ties_break_by_ketto_toroku_bango_ascending():
    df = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["zz", "aa"],
        "predicted_score": [5.0, 5.0],
        "actual_finish_position": [2, 1],
    })
    out = subject.normalize_within_race(df)
    sorted_out = out.sort_values("normalized_score", ascending=False).reset_index(
        drop=True,
    )
    assert sorted_out["ketto_toroku_bango"].tolist() == ["aa", "zz"]


def test_normalize_within_race_empty_dataframe_returns_empty_with_column():
    df = pd.DataFrame({
        "race_id": [],
        "ketto_toroku_bango": [],
        "predicted_score": [],
        "actual_finish_position": [],
    })
    out = subject.normalize_within_race(df)
    assert "normalized_score" in out.columns
    assert out.empty


def test_normalize_within_race_two_races_with_different_sizes():
    df = pd.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2", "r2"],
        "ketto_toroku_bango": ["a", "b", "c", "d", "e"],
        "predicted_score": [3.0, 1.0, 5.0, 2.0, 1.0],
        "actual_finish_position": [1, 2, 1, 2, 3],
    })
    out = subject.normalize_within_race(df)
    r1 = out[out["race_id"] == "r1"]
    r2 = out[out["race_id"] == "r2"]
    assert sorted(r1["normalized_score"].tolist()) == [0.0, 1.0]
    assert sorted(r2["normalized_score"].tolist()) == [0.0, 0.5, 1.0]


# ---------------------------------------------------------------------------
# blend_normalized
# ---------------------------------------------------------------------------


def _make_normalized(race_id: str, horses: list[str], scores: list[float],
                     actuals: list[int]) -> pd.DataFrame:
    return pd.DataFrame({
        "race_id": [race_id] * len(horses),
        "ketto_toroku_bango": horses,
        "normalized_score": scores,
        "actual_finish_position": actuals,
    })


def test_blend_normalized_two_equal_members_averages_scores():
    left = _make_normalized("r1", ["a", "b"], [1.0, 0.0], [1, 2])
    right = _make_normalized("r1", ["a", "b"], [0.0, 1.0], [1, 2])
    blended = subject.blend_normalized([left, right], [0.5, 0.5])
    sorted_blended = blended.sort_values("ketto_toroku_bango").reset_index(drop=True)
    assert sorted_blended["blended_score"].tolist() == [0.5, 0.5]


def test_blend_normalized_three_members_weights_sum_to_target():
    left = _make_normalized("r1", ["a"], [1.0], [1])
    mid = _make_normalized("r1", ["a"], [0.5], [1])
    right = _make_normalized("r1", ["a"], [0.0], [1])
    blended = subject.blend_normalized([left, mid, right], [0.5, 0.3, 0.2])
    assert blended["blended_score"].tolist() == [0.5 * 1.0 + 0.3 * 0.5 + 0.2 * 0.0]


def test_blend_normalized_inner_join_drops_unjoined_pairs():
    left = _make_normalized("r1", ["a", "b"], [1.0, 0.5], [1, 2])
    right = _make_normalized("r1", ["a"], [0.0], [1])
    blended = subject.blend_normalized([left, right], [0.5, 0.5])
    assert blended.shape[0] == 1
    assert blended["ketto_toroku_bango"].tolist() == ["a"]


def test_blend_normalized_empty_members_list_raises():
    with pytest.raises(ValueError):
        subject.blend_normalized([], [])


def test_blend_normalized_length_mismatch_raises():
    left = _make_normalized("r1", ["a"], [1.0], [1])
    with pytest.raises(ValueError):
        subject.blend_normalized([left], [0.5, 0.5])


# ---------------------------------------------------------------------------
# compute_top1
# ---------------------------------------------------------------------------


def test_compute_top1_full_hit_returns_one():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2"],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "blended_score": [1.0, 0.0, 1.0, 0.0],
        "actual_finish_position": [1, 2, 1, 2],
    })
    assert subject.compute_top1(blended) == 1.0


def test_compute_top1_full_miss_returns_zero():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "blended_score": [1.0, 0.0],
        "actual_finish_position": [5, 1],
    })
    assert subject.compute_top1(blended) == 0.0


def test_compute_top1_mixed_50_percent_returns_half():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2"],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "blended_score": [1.0, 0.0, 1.0, 0.0],
        "actual_finish_position": [1, 2, 4, 1],
    })
    assert subject.compute_top1(blended) == 0.5


def test_compute_top1_empty_returns_zero():
    blended = pd.DataFrame({
        "race_id": [], "ketto_toroku_bango": [],
        "blended_score": [], "actual_finish_position": [],
    })
    assert subject.compute_top1(blended) == 0.0


def test_compute_top1_uses_ketto_toroku_tiebreak_when_scores_tie():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["b", "a"],
        "blended_score": [1.0, 1.0],
        "actual_finish_position": [3, 1],
    })
    # Tiebreak: ketto_toroku ascending → 'a' wins → actual==1 → top1 hit
    assert subject.compute_top1(blended) == 1.0


# ---------------------------------------------------------------------------
# compute_topk_metric
# ---------------------------------------------------------------------------


def test_compute_topk_metric_place_k2_full_hit():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "blended_score": [3.0, 2.0, 1.0],
        "actual_finish_position": [1, 2, 3],
    })
    assert subject.compute_topk_metric(blended, 2, "place") == 1.0


def test_compute_topk_metric_place_k2_miss_returns_zero():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "blended_score": [3.0, 2.0, 1.0],
        "actual_finish_position": [1, 5, 3],
    })
    assert subject.compute_topk_metric(blended, 2, "place") == 0.0


def test_compute_topk_metric_box_top3_exact_set_match():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "blended_score": [4.0, 3.0, 2.0, 1.0],
        "actual_finish_position": [2, 1, 3, 4],
    })
    assert subject.compute_topk_metric(blended, 3, "box_top3") == 1.0


def test_compute_topk_metric_box_top3_wrong_top3_set_returns_zero():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "blended_score": [4.0, 3.0, 2.0, 1.0],
        "actual_finish_position": [1, 2, 4, 3],
    })
    assert subject.compute_topk_metric(blended, 3, "box_top3") == 0.0


def test_compute_topk_metric_box_top3_excludes_races_shorter_than_three():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "blended_score": [2.0, 1.0],
        "actual_finish_position": [1, 2],
    })
    assert subject.compute_topk_metric(blended, 3, "box_top3") == 0.0


def test_compute_topk_metric_unknown_metric_raises():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "blended_score": [1.0, 0.0],
        "actual_finish_position": [1, 2],
    })
    with pytest.raises(ValueError):
        subject.compute_topk_metric(blended, 2, "bogus")


def test_compute_topk_metric_empty_blended_returns_zero():
    blended = pd.DataFrame({
        "race_id": [], "ketto_toroku_bango": [],
        "blended_score": [], "actual_finish_position": [],
    })
    assert subject.compute_topk_metric(blended, 2, "place") == 0.0


def test_compute_topk_metric_place_k_zero_raises():
    blended = pd.DataFrame({
        "race_id": ["r1"], "ketto_toroku_bango": ["a"],
        "blended_score": [1.0], "actual_finish_position": [1],
    })
    with pytest.raises(ValueError):
        subject.compute_topk_metric(blended, 0, "place")


def test_compute_topk_metric_place_two_races_one_hit():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2"],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "blended_score": [3.0, 2.0, 3.0, 2.0],
        "actual_finish_position": [1, 2, 1, 5],
    })
    # r1: 2nd predicted is 'b' with actual=2 → hit. r2: 2nd predicted is 'd' actual=5 → miss
    assert subject.compute_topk_metric(blended, 2, "place") == 0.5


def test_compute_topk_metric_box_top3_no_eligible_races_returns_zero():
    blended = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "blended_score": [1.0, 0.0],
        "actual_finish_position": [1, 2],
    })
    assert subject.compute_topk_metric(blended, 3, "box_top3") == 0.0


def test_compute_topk_metric_place_no_eligible_races_returns_zero():
    # All races shorter than k=5 → no eligible races
    blended = pd.DataFrame({
        "race_id": ["r1", "r1", "r2", "r2"],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "blended_score": [1.0, 0.0, 1.0, 0.0],
        "actual_finish_position": [1, 2, 1, 2],
    })
    assert subject.compute_topk_metric(blended, 5, "place") == 0.0


# ---------------------------------------------------------------------------
# simplex_softmax
# ---------------------------------------------------------------------------


def test_simplex_softmax_empty_returns_empty():
    assert subject.simplex_softmax([], 0, 0.2) == []


def test_simplex_softmax_single_member_returns_one():
    assert subject.simplex_softmax([2.5], 0, 0.2) == [1.0]


def test_simplex_softmax_all_zero_logits_returns_uniform_weights():
    weights = subject.simplex_softmax([0.0, 0.0, 0.0], 0, 0.2)
    assert weights == pytest.approx([1 / 3, 1 / 3, 1 / 3])


def test_simplex_softmax_weights_sum_to_one():
    weights = subject.simplex_softmax([1.0, 2.0, -1.0], 0, 0.2)
    assert sum(weights) == pytest.approx(1.0)


def test_simplex_softmax_very_negative_anchor_is_forced_to_min_value():
    weights = subject.simplex_softmax([-10.0, 5.0, 5.0], 0, 0.2)
    assert weights[0] == pytest.approx(0.2)
    assert sum(weights) == pytest.approx(1.0)


def test_simplex_softmax_anchor_above_min_passes_through_unchanged():
    weights = subject.simplex_softmax([5.0, 0.0, 0.0], 0, 0.2)
    assert weights[0] > 0.2
    assert sum(weights) == pytest.approx(1.0)


def test_simplex_softmax_invalid_anchor_idx_negative_raises():
    with pytest.raises(ValueError):
        subject.simplex_softmax([0.0, 0.0], -1, 0.2)


def test_simplex_softmax_invalid_anchor_idx_out_of_range_raises():
    with pytest.raises(ValueError):
        subject.simplex_softmax([0.0, 0.0], 5, 0.2)


def test_simplex_softmax_invalid_min_anchor_value_above_one_raises():
    with pytest.raises(ValueError):
        subject.simplex_softmax([0.0, 0.0], 0, 1.5)


def test_simplex_softmax_invalid_min_anchor_value_negative_raises():
    with pytest.raises(ValueError):
        subject.simplex_softmax([0.0, 0.0], 0, -0.1)


def test_simplex_softmax_min_value_one_forces_anchor_full():
    weights = subject.simplex_softmax([-10.0, 5.0], 0, 1.0)
    assert weights[0] == 1.0
    assert weights[1] == 0.0


# ---------------------------------------------------------------------------
# wilson_lower_bound
# ---------------------------------------------------------------------------


def test_wilson_lower_bound_zero_n_returns_zero():
    assert subject.wilson_lower_bound(0.5, 0) == 0.0


def test_wilson_lower_bound_p_zero_returns_zero():
    # p=0, n=100 → lower bound clamped to 0
    assert subject.wilson_lower_bound(0.0, 100) == pytest.approx(0.0, abs=1e-10)


def test_wilson_lower_bound_p_one_n_one_hundred_high_lower():
    # p=1.0, n=100 → lower bound matches the Wilson formula
    expected = (1.0 + 1.96**2 / (2 * 100) - 1.96 * math.sqrt(
        (1.0 * 0.0 + 1.96**2 / (4 * 100)) / 100,
    )) / (1.0 + 1.96**2 / 100)
    assert subject.wilson_lower_bound(1.0, 100) == pytest.approx(expected, abs=1e-9)


def test_wilson_lower_bound_p_half_n_one_hundred_matches_formula():
    # Known closed-form: p=0.5, n=100, z=1.96 →
    # lower ≈ (0.5 + 1.96²/200 − 1.96 * sqrt((0.25 + 1.96²/400)/100)) / (1 + 1.96²/100)
    z_sq = 1.96 ** 2
    denom = 1.0 + z_sq / 100.0
    center = 0.5 + z_sq / (2.0 * 100.0)
    margin = 1.96 * math.sqrt(
        (0.5 * 0.5 + z_sq / (4.0 * 100.0)) / 100.0,
    )
    expected = (center - margin) / denom
    assert subject.wilson_lower_bound(0.5, 100) == pytest.approx(expected, abs=1e-9)


def test_wilson_lower_bound_clamps_p_above_one_to_one():
    # When p > 1.0 we clamp before computation; result equals the p=1.0 case
    expected = subject.wilson_lower_bound(1.0, 100)
    assert subject.wilson_lower_bound(1.5, 100) == pytest.approx(expected, abs=1e-12)


def test_wilson_lower_bound_clamps_negative_p_to_zero():
    expected = subject.wilson_lower_bound(0.0, 100)
    assert subject.wilson_lower_bound(-0.5, 100) == pytest.approx(expected, abs=1e-12)


def test_wilson_lower_bound_custom_z_score():
    # z=0 should collapse to p itself (no margin, no denom shift)
    assert subject.wilson_lower_bound(0.6, 50, z=0.0) == pytest.approx(0.6, abs=1e-9)


# ---------------------------------------------------------------------------
# load_class_predictions (parquet I/O integration)
# ---------------------------------------------------------------------------


def _write_year_parquet(
    root: Path,
    year: int,
    race_id: str,
    horses: list[str],
    scores: list[float],
    actuals: list[int],
) -> None:
    year_dir = root / f"race_year={year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({
        "race_id": [race_id] * len(horses),
        "ketto_toroku_bango": horses,
        "predicted_score": scores,
        "actual_finish_position": actuals,
        "umaban": list(range(1, len(horses) + 1)),
    })
    df.to_parquet(year_dir / "predictions.parquet", index=False)


def test_load_class_predictions_filters_to_class_and_concatenates_years(tmp_path: Path):
    _write_year_parquet(
        tmp_path, 2018, "jra:2018:0101:01:01", ["a", "b"], [1.0, 0.5], [1, 2],
    )
    _write_year_parquet(
        tmp_path, 2019, "jra:2019:0101:01:01", ["c", "d"], [2.0, 1.0], [1, 2],
    )
    pg_map = pd.DataFrame({
        "race_id": ["jra:2018:0101:01:01", "jra:2019:0101:01:01"],
        "kyoso_joken_code": ["005", "010"],
    })
    out = subject.load_class_predictions(tmp_path, "005", [2018, 2019], pg_map)
    assert out["race_id"].tolist() == ["jra:2018:0101:01:01"] * 2
    assert sorted(out["ketto_toroku_bango"].tolist()) == ["a", "b"]


def test_load_class_predictions_skips_missing_year_dirs(tmp_path: Path):
    _write_year_parquet(
        tmp_path, 2020, "jra:2020:0101:01:01", ["e"], [1.0], [1],
    )
    pg_map = pd.DataFrame({
        "race_id": ["jra:2020:0101:01:01"],
        "kyoso_joken_code": ["005"],
    })
    out = subject.load_class_predictions(
        tmp_path, "005", [2018, 2019, 2020], pg_map,
    )
    assert out.shape[0] == 1


def test_load_class_predictions_returns_empty_when_no_years_present(tmp_path: Path):
    pg_map = pd.DataFrame({
        "race_id": ["jra:2020:0101:01:01"],
        "kyoso_joken_code": ["005"],
    })
    out = subject.load_class_predictions(tmp_path, "005", [2018], pg_map)
    assert out.empty
    assert sorted(out.columns.tolist()) == sorted([
        "race_id", "ketto_toroku_bango", "predicted_score", "actual_finish_position",
    ])


def test_load_class_predictions_concats_multi_part_year(tmp_path: Path):
    year_dir = tmp_path / "race_year=2018"
    year_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "race_id": ["jra:2018:0101:01:01"],
        "ketto_toroku_bango": ["a"],
        "predicted_score": [1.0],
        "actual_finish_position": [1],
        "umaban": [1],
    }).to_parquet(year_dir / "part1.parquet", index=False)
    pd.DataFrame({
        "race_id": ["jra:2018:0101:01:01"],
        "ketto_toroku_bango": ["b"],
        "predicted_score": [0.5],
        "actual_finish_position": [2],
        "umaban": [2],
    }).to_parquet(year_dir / "part2.parquet", index=False)
    pg_map = pd.DataFrame({
        "race_id": ["jra:2018:0101:01:01"],
        "kyoso_joken_code": ["005"],
    })
    out = subject.load_class_predictions(tmp_path, "005", [2018], pg_map)
    assert out.shape[0] == 2


def test_load_class_predictions_other_class_includes_null_kyoso(tmp_path: Path):
    _write_year_parquet(
        tmp_path, 2018, "jra:2018:0101:01:01", ["a"], [1.0], [1],
    )
    pg_map = pd.DataFrame({
        "race_id": ["jra:2018:0101:01:01"],
        "kyoso_joken_code": [None],
    })
    out = subject.load_class_predictions(tmp_path, "other", [2018], pg_map)
    assert out.shape[0] == 1


def test_load_class_predictions_empty_year_parquet_is_skipped(tmp_path: Path):
    # Empty parquet (no rows) under year dir.
    year_dir = tmp_path / "race_year=2018"
    year_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "race_id": pd.Series([], dtype=str),
        "ketto_toroku_bango": pd.Series([], dtype=str),
        "predicted_score": pd.Series([], dtype=np.float64),
        "actual_finish_position": pd.Series([], dtype=np.int64),
        "umaban": pd.Series([], dtype=np.int64),
    }).to_parquet(year_dir / "empty.parquet", index=False)
    pg_map = pd.DataFrame({
        "race_id": pd.Series([], dtype=str),
        "kyoso_joken_code": pd.Series([], dtype=str),
    })
    out = subject.load_class_predictions(tmp_path, "005", [2018], pg_map)
    assert out.empty


def test_load_class_predictions_year_dir_with_no_parquet_files_is_skipped(
    tmp_path: Path,
):
    year_dir = tmp_path / "race_year=2018"
    year_dir.mkdir(parents=True, exist_ok=True)
    # No parquet files written under the year directory.
    pg_map = pd.DataFrame({
        "race_id": pd.Series([], dtype=str),
        "kyoso_joken_code": pd.Series([], dtype=str),
    })
    out = subject.load_class_predictions(tmp_path, "005", [2018], pg_map)
    assert out.empty


# ---------------------------------------------------------------------------
# compute_fukusho_2p
# ---------------------------------------------------------------------------


def _make_blended(
    race_ids: list[str],
    horses: list[str],
    scores: list[float],
    actuals: list[int],
) -> pd.DataFrame:
    return pd.DataFrame({
        "race_id": race_ids,
        "ketto_toroku_bango": horses,
        "blended_score": scores,
        "actual_finish_position": actuals,
    })


def test_compute_fukusho_2p_two_of_three_hit_returns_one():
    # Predicted top-3: a(score=3, actual=1), b(score=2, actual=4), c(score=1, actual=3)
    # pred top-3 actual finishes: {1, 4, 3} — 1 and 3 are <=3, so fukusho_cnt=2 → hit
    df = _make_blended(
        ["r1", "r1", "r1", "r1"],
        ["a", "b", "c", "d"],
        [3.0, 2.0, 1.0, 0.5],
        [1, 4, 3, 2],
    )
    assert subject.compute_fukusho_2p(df) == 1.0


def test_compute_fukusho_2p_only_one_of_three_hit_returns_zero():
    # Predicted top-3: a(score=3, actual=1), b(score=2, actual=5), c(score=1, actual=6)
    # pred top-3 actual finishes: {1, 5, 6} — only 1 is <=3, fukusho_cnt=1 → miss
    df = _make_blended(
        ["r1", "r1", "r1", "r1"],
        ["a", "b", "c", "d"],
        [3.0, 2.0, 1.0, 0.5],
        [1, 5, 6, 2],
    )
    assert subject.compute_fukusho_2p(df) == 0.0


def test_compute_fukusho_2p_all_three_hit_returns_one():
    # Predicted top-3: a(actual=2), b(actual=1), c(actual=3) — all <=3, fukusho_cnt=3 → hit
    df = _make_blended(
        ["r1", "r1", "r1"],
        ["a", "b", "c"],
        [3.0, 2.0, 1.0],
        [2, 1, 3],
    )
    assert subject.compute_fukusho_2p(df) == 1.0


def test_compute_fukusho_2p_two_races_mixed_returns_half():
    # r1: 2-of-3 hit (score=1.0), r2: 1-of-3 hit (score=0.0) → mean=0.5
    df = _make_blended(
        ["r1", "r1", "r1", "r1", "r2", "r2", "r2", "r2"],
        ["a", "b", "c", "d", "e", "f", "g", "h"],
        [3.0, 2.0, 1.0, 0.5, 3.0, 2.0, 1.0, 0.5],
        [1, 4, 3, 2, 1, 5, 6, 2],
    )
    assert subject.compute_fukusho_2p(df) == pytest.approx(0.5)


def test_compute_fukusho_2p_short_field_two_horses_returns_zero():
    # Only 2 horses in race: at most 2 predicted-top-3 candidates, but need >=2 AND <=3
    # a(actual=1), b(actual=2) — both <=3 but there are only 2 predicted rows, fukusho_cnt=2 → hit
    # Wait: head(3) on 2 rows gives 2 rows; {1,2} both <=3, cnt=2 → hit=1
    # But per I1 semantics short races CAN hit if both score; let's test a 1-horse race returns 0
    df = _make_blended(
        ["r1"],
        ["a"],
        [1.0],
        [1],
    )
    assert subject.compute_fukusho_2p(df) == 0.0


def test_compute_fukusho_2p_two_horse_race_both_top3_returns_one():
    # 2 horses, both finish top-3; head(3)=2 rows, fukusho_cnt=2 → hit
    df = _make_blended(
        ["r1", "r1"],
        ["a", "b"],
        [2.0, 1.0],
        [1, 2],
    )
    assert subject.compute_fukusho_2p(df) == 1.0


def test_compute_fukusho_2p_empty_dataframe_returns_zero():
    df = pd.DataFrame({
        "race_id": pd.Series([], dtype=str),
        "ketto_toroku_bango": pd.Series([], dtype=str),
        "blended_score": pd.Series([], dtype=float),
        "actual_finish_position": pd.Series([], dtype=int),
    })
    assert subject.compute_fukusho_2p(df) == 0.0


def test_compute_fukusho_2p_tiebreak_by_ketto_toroku_bango():
    # Scores tie at 2.0 for b and c; b < c alphabetically so b is picked 2nd, c 3rd
    # a(score=3, actual=5), b(score=2, actual=2), c(score=2, actual=3)
    # Predicted top-3 by score: a(5), b(2), c(3) — {5,2,3}: 2 and 3 are <=3, cnt=2 → hit
    df = _make_blended(
        ["r1", "r1", "r1", "r1"],
        ["a", "b", "c", "d"],
        [3.0, 2.0, 2.0, 1.0],
        [5, 2, 3, 1],
    )
    assert subject.compute_fukusho_2p(df) == 1.0


# ---------------------------------------------------------------------------
# compute_rentai_hit
# ---------------------------------------------------------------------------


def test_compute_rentai_hit_predicted_top2_equals_actual_top2_returns_one():
    # Predicted top-2: a(score=2, actual=2), b(score=1, actual=1) → pred_top2={2,1}
    # actual_top2 = {1,2} → subset match AND len>=2 → hit
    df = _make_blended(
        ["r1", "r1", "r1"],
        ["a", "b", "c"],
        [2.0, 1.0, 0.5],
        [2, 1, 3],
    )
    assert subject.compute_rentai_hit(df) == 1.0


def test_compute_rentai_hit_one_miss_in_predicted_top2_returns_zero():
    # Predicted top-2: a(score=2, actual=1), b(score=1, actual=4) → pred_top2={1,4}
    # actual_top2 = {1,2} → 2 not in pred_top2 → miss
    df = _make_blended(
        ["r1", "r1", "r1"],
        ["a", "b", "c"],
        [2.0, 1.0, 0.5],
        [1, 4, 2],
    )
    assert subject.compute_rentai_hit(df) == 0.0


def test_compute_rentai_hit_two_races_mixed_returns_half():
    # r1: hit (pred top-2 actual={1,2}), r2: miss (pred top-2 actual={1,4})
    df = _make_blended(
        ["r1", "r1", "r1", "r2", "r2", "r2"],
        ["a", "b", "c", "d", "e", "f"],
        [2.0, 1.0, 0.5, 2.0, 1.0, 0.5],
        [2, 1, 3, 1, 4, 2],
    )
    assert subject.compute_rentai_hit(df) == pytest.approx(0.5)


def test_compute_rentai_hit_single_horse_race_returns_zero():
    # Only 1 horse: len(pred_top2)=1 < 2 → not a hit
    df = _make_blended(
        ["r1"],
        ["a"],
        [1.0],
        [1],
    )
    assert subject.compute_rentai_hit(df) == 0.0


def test_compute_rentai_hit_empty_dataframe_returns_zero():
    df = pd.DataFrame({
        "race_id": pd.Series([], dtype=str),
        "ketto_toroku_bango": pd.Series([], dtype=str),
        "blended_score": pd.Series([], dtype=float),
        "actual_finish_position": pd.Series([], dtype=int),
    })
    assert subject.compute_rentai_hit(df) == 0.0


def test_compute_rentai_hit_both_predicted_top2_outside_actual_top2_returns_zero():
    # Predicted top-2: a(actual=3), b(actual=4) → pred_top2={3,4}
    # actual_top2={1,2} → {1,2} not subset of {3,4} → miss
    df = _make_blended(
        ["r1", "r1", "r1", "r1"],
        ["a", "b", "c", "d"],
        [2.0, 1.0, 0.5, 0.1],
        [3, 4, 1, 2],
    )
    assert subject.compute_rentai_hit(df) == 0.0


def test_compute_rentai_hit_tiebreak_by_ketto_toroku_bango():
    # Scores tie at 1.0 for a and b; a < b alphabetically → a is top-1, b is top-2
    # a(score=1, actual=1), b(score=1, actual=2) → pred_top2={1,2}
    # actual_top2 = {1,2} → hit
    df = _make_blended(
        ["r1", "r1", "r1"],
        ["a", "b", "c"],
        [1.0, 1.0, 0.5],
        [1, 2, 3],
    )
    assert subject.compute_rentai_hit(df) == 1.0
