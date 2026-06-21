from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import subgroup_diagnostics as subject


def _make_ground_truth(races: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(races)


def _make_predictions(races: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(races)


def test_get_source_label_jra_non_banei():
    assert subject.get_source_label("jra", "10") == "jra"


def test_get_source_label_nar_banei_keibajo():
    assert subject.get_source_label("nar", "83") == "banei"


def test_get_source_label_jra_banei_keibajo_overrides_source():
    assert subject.get_source_label("jra", "83") == "banei"


def test_get_source_label_nar_non_banei():
    assert subject.get_source_label("nar", "40") == "nar"


def test_get_surface_label_jra_turf_code_10():
    assert subject.get_surface_label("10", "jra") == "turf"


def test_get_surface_label_jra_turf_code_22():
    assert subject.get_surface_label("22", "jra") == "turf"


def test_get_surface_label_jra_dirt_code_23():
    assert subject.get_surface_label("23", "jra") == "dirt"


def test_get_surface_label_jra_dirt_code_29():
    assert subject.get_surface_label("29", "jra") == "dirt"


def test_get_surface_label_jra_unknown_track_code_returns_other():
    assert subject.get_surface_label("99", "jra") == "other"


def test_get_surface_label_nar_always_dirt():
    assert subject.get_surface_label("10", "nar") == "dirt"


def test_get_surface_label_banei_always_dirt():
    assert subject.get_surface_label("10", "banei") == "dirt"


def test_get_distance_band_sprint_below_max():
    assert subject.get_distance_band(1000) == "sprint"


def test_get_distance_band_sprint_at_boundary():
    assert subject.get_distance_band(1199) == "sprint"


def test_get_distance_band_mile_just_above_sprint():
    assert subject.get_distance_band(1200) == "mile"


def test_get_distance_band_mile_at_boundary():
    assert subject.get_distance_band(1599) == "mile"


def test_get_distance_band_intermediate_just_above_mile():
    assert subject.get_distance_band(1600) == "intermediate"


def test_get_distance_band_intermediate_at_boundary():
    assert subject.get_distance_band(1999) == "intermediate"


def test_get_distance_band_long_above_intermediate():
    assert subject.get_distance_band(2000) == "long"


def test_get_distance_band_long_at_boundary():
    assert subject.get_distance_band(2399) == "long"


def test_get_distance_band_extended_above_long():
    assert subject.get_distance_band(2400) == "extended"


def test_get_distance_band_extended_far():
    assert subject.get_distance_band(3200) == "extended"


def test_make_subgroup_key_concatenates_with_underscores():
    assert subject.make_subgroup_key("jra", "turf", "mile") == "jra_turf_mile"


def test_make_subgroup_key_nar_dirt_sprint():
    assert subject.make_subgroup_key("nar", "dirt", "sprint") == "nar_dirt_sprint"


def test_assign_subgroup_keys_jra_turf_mile():
    df = pd.DataFrame([{
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 1400,
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.tolist() == ["jra_turf_mile"]


def test_assign_subgroup_keys_nar_dirt_long():
    df = pd.DataFrame([{
        "source": "nar",
        "keibajo_code": "40",
        "track_code": "10",
        "kyori": 2200,
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.tolist() == ["nar_dirt_long"]


def test_assign_subgroup_keys_jra_turf_extended():
    df = pd.DataFrame([{
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 3000,
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.tolist() == ["jra_turf_extended"]


def test_assign_subgroup_keys_banei_dirt_sprint():
    df = pd.DataFrame([{
        "source": "nar",
        "keibajo_code": "83",
        "track_code": "10",
        "kyori": 200,
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.tolist() == ["banei_dirt_sprint"]


def test_assign_subgroup_keys_multiple_rows():
    df = pd.DataFrame([
        {"source": "jra", "keibajo_code": "10", "track_code": "23", "kyori": 1100},
        {"source": "nar", "keibajo_code": "40", "track_code": "10", "kyori": 1800},
    ])
    result = subject.assign_subgroup_keys(df)
    assert result.tolist() == ["jra_dirt_sprint", "nar_dirt_intermediate"]


def test_dcg_at_3_perfect_prediction():
    dcg = subject._dcg_at_3([1, 2, 3])
    expected = 3.0 / np.log2(2) + 2.0 / np.log2(3) + 1.0 / np.log2(4)
    assert abs(dcg - expected) < 1e-9


def test_dcg_at_3_mixed_order():
    dcg = subject._dcg_at_3([2, 1, 3])
    expected = 2.0 / np.log2(2) + 3.0 / np.log2(3) + 1.0 / np.log2(4)
    assert abs(dcg - expected) < 1e-9


def test_dcg_at_3_empty_list_returns_zero():
    assert subject._dcg_at_3([]) == 0.0


def test_dcg_at_3_irrelevant_positions_yield_zero_contribution():
    dcg = subject._dcg_at_3([4, 5, 6])
    assert dcg == 0.0


def test_dcg_at_3_only_uses_first_three_positions():
    dcg_short = subject._dcg_at_3([1, 2, 3])
    dcg_long = subject._dcg_at_3([1, 2, 3, 4, 5, 6])
    assert abs(dcg_short - dcg_long) < 1e-9


def test_compute_race_ndcg_perfect_returns_one():
    group = pd.DataFrame({
        "race_id": ["r1"] * 5,
        "ketto_toroku_bango": ["a", "b", "c", "d", "e"],
        "predicted_rank": [1, 2, 3, 4, 5],
        "finish_position": [1, 2, 3, 4, 5],
    })
    result = subject.compute_race_ndcg(group)
    assert abs(result - 1.0) < 1e-9


def test_compute_race_ndcg_worst_order_is_less_than_one():
    group = pd.DataFrame({
        "race_id": ["r1"] * 5,
        "ketto_toroku_bango": ["a", "b", "c", "d", "e"],
        "predicted_rank": [1, 2, 3, 4, 5],
        "finish_position": [5, 4, 3, 2, 1],
    })
    result = subject.compute_race_ndcg(group)
    assert result < 1.0


def test_compute_race_ndcg_two_horse_perfect_gives_one():
    group = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "predicted_rank": [1, 2],
        "finish_position": [1, 2],
    })
    result = subject.compute_race_ndcg(group)
    assert abs(result - 1.0) < 1e-9


def test_compute_race_ndcg_all_irrelevant_gives_zero():
    group = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "predicted_rank": [1, 2],
        "finish_position": [4, 5],
    })
    result = subject.compute_race_ndcg(group)
    assert result == 0.0


def test_compute_race_ndcg_with_nan_finish_position():
    group = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1, 2, 3],
        "finish_position": [1.0, float("nan"), 3.0],
    })
    result = subject.compute_race_ndcg(group)
    assert 0.0 <= result <= 1.0


def test_compute_race_top1_true_when_predicted_rank1_finishes_first():
    group = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1, 2, 3],
        "finish_position": [1, 2, 3],
    })
    assert subject.compute_race_top1(group) is True


def test_compute_race_top1_false_when_predicted_rank1_finishes_second():
    group = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1, 2, 3],
        "finish_position": [2, 1, 3],
    })
    assert subject.compute_race_top1(group) is False


def test_compute_race_top1_false_when_predicted_rank1_has_nan_finish_position():
    group = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1, 2, 3],
        "finish_position": [float("nan"), 1.0, 2.0],
    })
    assert subject.compute_race_top1(group) is False


def test_compute_race_top3_box_true_when_exact_match():
    group = pd.DataFrame({
        "race_id": ["r1"] * 4,
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    assert subject.compute_race_top3_box(group) is True


def test_compute_race_top3_box_true_when_same_horses_different_order():
    group = pd.DataFrame({
        "race_id": ["r1"] * 4,
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [2, 1, 3, 4],
    })
    assert subject.compute_race_top3_box(group) is True


def test_compute_race_top3_box_false_when_different_horses():
    group = pd.DataFrame({
        "race_id": ["r1"] * 4,
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 4, 3],
    })
    assert subject.compute_race_top3_box(group) is False


def test_evaluate_subgroup_empty_df_returns_zero_race_count():
    empty = pd.DataFrame(columns=[
        "race_id", "ketto_toroku_bango", "predicted_rank", "finish_position",
    ])
    result = subject.evaluate_subgroup(empty)
    assert result["race_count"] == 0
    assert result["ndcg_at_3"] == 0.0
    assert result["top1_accuracy"] == 0.0
    assert result["top3_box_accuracy"] == 0.0


def test_evaluate_subgroup_single_race_perfect():
    joined = pd.DataFrame({
        "race_id": ["r1"] * 5,
        "ketto_toroku_bango": ["a", "b", "c", "d", "e"],
        "predicted_rank": [1, 2, 3, 4, 5],
        "finish_position": [1, 2, 3, 4, 5],
    })
    result = subject.evaluate_subgroup(joined)
    assert result["race_count"] == 1
    assert abs(result["ndcg_at_3"] - 1.0) < 1e-9
    assert result["top1_accuracy"] == 1.0
    assert result["top3_box_accuracy"] == 1.0


def test_evaluate_subgroup_less_than_3_runners_top3_not_counted():
    joined = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "predicted_rank": [1, 2],
        "finish_position": [1, 2],
    })
    result = subject.evaluate_subgroup(joined)
    assert result["race_count"] == 1
    assert result["top1_accuracy"] == 1.0
    assert result["top3_box_accuracy"] == 0.0


def test_evaluate_subgroup_top3_box_not_counted_for_two_horse_races_but_race_still_counted():
    joined = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "predicted_rank": [1, 2],
        "finish_position": [2, 1],
    })
    result = subject.evaluate_subgroup(joined)
    assert result["race_count"] == 1
    assert result["top1_accuracy"] == 0.0
    assert result["top3_box_accuracy"] == 0.0


def test_evaluate_subgroup_two_races_mixed_results():
    joined = pd.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1", "r2", "r2", "r2", "r2"],
        "ketto_toroku_bango": ["a", "b", "c", "d", "e", "f", "g", "h"],
        "predicted_rank": [1, 2, 3, 4, 1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4, 4, 3, 2, 1],
    })
    result = subject.evaluate_subgroup(joined)
    assert result["race_count"] == 2
    assert result["top1_accuracy"] == 0.5
    assert result["top3_box_accuracy"] == 0.5


def test_evaluate_subgroup_top3_not_counted_when_fewer_than_3_valid_finish_positions():
    joined = pd.DataFrame({
        "race_id": ["r1"] * 4,
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1.0, 2.0, float("nan"), float("nan")],
    })
    result = subject.evaluate_subgroup(joined)
    assert result["race_count"] == 1
    assert result["top3_box_accuracy"] == 0.0


def test_compute_subgroup_diagnostics_returns_sorted_list():
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1",
            "ketto_toroku_bango": "a",
            "finish_position": 1,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "10",
            "kyori": 1400,
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "b",
            "finish_position": 2,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "10",
            "kyori": 1400,
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "c",
            "finish_position": 3,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "10",
            "kyori": 1400,
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "jra_turf_mile"
    assert results[0]["race_count"] == 1


def test_compute_subgroup_diagnostics_empty_ground_truth_returns_empty():
    ground_truth = pd.DataFrame(columns=[
        "race_id", "ketto_toroku_bango", "finish_position",
        "source", "keibajo_code", "track_code", "kyori",
    ])
    predictions = _make_predictions([{
        "race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1,
    }])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert results == []


def test_compute_subgroup_diagnostics_empty_predictions_returns_zero_ndcg():
    # left join: ground_truth rows appear with NaN predicted_rank → NDCG=0.0 (all races penalized).
    ground_truth = _make_ground_truth([{
        "race_id": "r1",
        "ketto_toroku_bango": "a",
        "finish_position": 1,
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 1400,
    }])
    predictions = pd.DataFrame(
        columns=["race_id", "ketto_toroku_bango", "predicted_rank"]
    )
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["ndcg_at_3"] == 0.0
    assert results[0]["race_count"] == 1


def test_compute_subgroup_diagnostics_no_matching_rows_returns_zero_ndcg():
    # left join: ground_truth horses without matching predictions get NaN predicted_rank → NDCG=0.0.
    ground_truth = _make_ground_truth([{
        "race_id": "r1",
        "ketto_toroku_bango": "a",
        "finish_position": 1,
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 1400,
    }])
    predictions = _make_predictions([{
        "race_id": "r999",
        "ketto_toroku_bango": "z",
        "predicted_rank": 1,
    }])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["ndcg_at_3"] == 0.0
    assert results[0]["race_count"] == 1


def test_compute_subgroup_diagnostics_multiple_subgroups_sorted():
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1",
            "ketto_toroku_bango": "a",
            "finish_position": 1,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "10",
            "kyori": 1400,
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "b",
            "finish_position": 2,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "10",
            "kyori": 1400,
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "c",
            "finish_position": 3,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "10",
            "kyori": 1400,
        },
        {
            "race_id": "r2",
            "ketto_toroku_bango": "d",
            "finish_position": 1,
            "source": "nar",
            "keibajo_code": "40",
            "track_code": "10",
            "kyori": 1100,
        },
        {
            "race_id": "r2",
            "ketto_toroku_bango": "e",
            "finish_position": 2,
            "source": "nar",
            "keibajo_code": "40",
            "track_code": "10",
            "kyori": 1100,
        },
        {
            "race_id": "r2",
            "ketto_toroku_bango": "f",
            "finish_position": 3,
            "source": "nar",
            "keibajo_code": "40",
            "track_code": "10",
            "kyori": 1100,
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
        {"race_id": "r2", "ketto_toroku_bango": "d", "predicted_rank": 1},
        {"race_id": "r2", "ketto_toroku_bango": "e", "predicted_rank": 2},
        {"race_id": "r2", "ketto_toroku_bango": "f", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 2
    assert results[0]["subgroup"] < results[1]["subgroup"]
    assert {results[0]["subgroup"], results[1]["subgroup"]} == {"jra_turf_mile", "nar_dirt_sprint"}


def test_compute_subgroup_diagnostics_banei_subgroup():
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1",
            "ketto_toroku_bango": "a",
            "finish_position": 1,
            "source": "nar",
            "keibajo_code": "83",
            "track_code": "10",
            "kyori": 200,
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "b",
            "finish_position": 2,
            "source": "nar",
            "keibajo_code": "83",
            "track_code": "10",
            "kyori": 200,
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "c",
            "finish_position": 3,
            "source": "nar",
            "keibajo_code": "83",
            "track_code": "10",
            "kyori": 200,
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "banei_dirt_sprint"


def test_compute_subgroup_diagnostics_jra_dirt_subgroup():
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1",
            "ketto_toroku_bango": "a",
            "finish_position": 1,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "25",
            "kyori": 1400,
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "b",
            "finish_position": 2,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "25",
            "kyori": 1400,
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "c",
            "finish_position": 3,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "25",
            "kyori": 1400,
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "jra_dirt_mile"


def test_compute_subgroup_diagnostics_jra_other_track_code():
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1",
            "ketto_toroku_bango": "a",
            "finish_position": 1,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "99",
            "kyori": 2500,
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "b",
            "finish_position": 2,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "99",
            "kyori": 2500,
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "c",
            "finish_position": 3,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "99",
            "kyori": 2500,
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "jra_other_extended"


def test_evaluate_subgroup_top3_not_counted_when_fewer_than_3_valid_predicted_ranks():
    joined = pd.DataFrame({
        "race_id": ["r1"] * 4,
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1.0, 2.0, float("nan"), float("nan")],
        "finish_position": [1.0, 2.0, 3.0, 4.0],
    })
    result = subject.evaluate_subgroup(joined)
    assert result["race_count"] == 1
    assert result["top3_box_accuracy"] == 0.0


def test_compute_race_top1_returns_false_when_all_predicted_rank_nan():
    group = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "predicted_rank": [float("nan"), float("nan")],
        "finish_position": [1.0, 2.0],
    })
    assert subject.compute_race_top1(group) is False


def test_compute_race_ndcg_excludes_nan_predicted_rank_from_dcg_but_not_ideal():
    # Horse "a" has no predicted rank but finishes 1st (rel=3.0).
    # DCG: only b(rank=1,finish=2nd,rel=2.0) and c(rank=2,finish=3rd,rel=1.0) contribute.
    # Ideal: all 3 horses (including a) define the best possible ordering.
    # So NDCG is penalized for missing the winner, not inflated to 1.0.
    group = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [float("nan"), 1.0, 2.0],
        "finish_position": [1.0, 2.0, 3.0],
    })
    result = subject.compute_race_ndcg(group)
    # exact: DCG = 2/log2(2) + 1/log2(3); ideal = 3/log2(2) + 2/log2(3) + 1/log2(4)
    dcg = 2.0 / np.log2(2) + 1.0 / np.log2(3)
    ideal_dcg = 3.0 / np.log2(2) + 2.0 / np.log2(3) + 1.0 / np.log2(4)
    assert abs(result - dcg / ideal_dcg) < 1e-9


def test_compute_race_ndcg_returns_one_when_predicted_perfectly_among_ranked_horses():
    # No NaN predicted_rank: perfect ranking → NDCG = 1.0.
    group = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1.0, 2.0, 3.0],
        "finish_position": [1.0, 2.0, 3.0],
    })
    result = subject.compute_race_ndcg(group)
    assert abs(result - 1.0) < 1e-9


def test_compute_race_ndcg_excludes_nan_finish_position_from_dcg_slot():
    # Horse "a" has predicted_rank=1 but finish_position=NaN (e.g. scratched).
    # It must NOT occupy a DCG slot — only "b" (rank=2, finish=1) contributes.
    # Ideal: only "b" has a known finish_position.
    # Result: perfect prediction among scoreable horses → NDCG = 1.0.
    group = pd.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "predicted_rank": [1.0, 2.0],
        "finish_position": [float("nan"), 1.0],
    })
    result = subject.compute_race_ndcg(group)
    assert abs(result - 1.0) < 1e-9


def test_compute_subgroup_diagnostics_penalizes_unpredicted_winner_via_left_join():
    # ground_truth has 3 horses. predictions only covers b and c — the winner (a) is absent.
    # After left join, "a" appears with NaN predicted_rank and finish_position=1.
    # NDCG must be < 1.0 because the winner was never ranked.
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1", "ketto_toroku_bango": "a", "finish_position": 1,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1600,
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "b", "finish_position": 2,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1600,
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "c", "finish_position": 3,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1600,
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 2},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["ndcg_at_3"] < 1.0
