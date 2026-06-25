from __future__ import annotations

import numpy as np
import polars as pl

import learning.subgroup_diagnostics as subject


def _make_ground_truth(races: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(races)


def _make_predictions(races: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(races)


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


def test_get_season_spring():
    assert subject.get_season(3) == "spring"
    assert subject.get_season(4) == "spring"
    assert subject.get_season(5) == "spring"


def test_get_season_summer():
    assert subject.get_season(6) == "summer"
    assert subject.get_season(7) == "summer"
    assert subject.get_season(8) == "summer"


def test_get_season_autumn():
    assert subject.get_season(9) == "autumn"
    assert subject.get_season(10) == "autumn"
    assert subject.get_season(11) == "autumn"


def test_get_season_winter():
    assert subject.get_season(12) == "winter"
    assert subject.get_season(1) == "winter"
    assert subject.get_season(2) == "winter"


def test_get_class_label_returns_grade_code():
    assert subject.get_class_label("G1") == "G1"
    assert subject.get_class_label("A") == "A"
    assert subject.get_class_label("NEW") == "NEW"
    assert subject.get_class_label("OP") == "OP"


def test_get_class_label_empty_returns_unknown():
    assert subject.get_class_label("") == "unknown"


def test_make_subgroup_key_five_parts():
    assert subject.make_subgroup_key("jra", "turf", "mile", "G2", "summer") == "jra_turf_mile_G2_summer"


def test_make_subgroup_key_defaults_unknown():
    assert subject.make_subgroup_key("jra", "turf", "mile") == "jra_turf_mile_unknown_unknown"


def test_make_subgroup_key_nar_dirt_sprint_with_class_season():
    assert subject.make_subgroup_key("nar", "dirt", "sprint", "A", "winter") == "nar_dirt_sprint_A_winter"


def test_assign_subgroup_keys_jra_turf_mile_with_class_and_season():
    df = pl.DataFrame([{
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 1400,
        "grade_code": "G2",
        "kaisai_nengappi": "20260615",
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.to_list() == ["jra_turf_mile_G2_summer"]


def test_assign_subgroup_keys_nar_dirt_long_with_class_and_season():
    df = pl.DataFrame([{
        "source": "nar",
        "keibajo_code": "40",
        "track_code": "10",
        "kyori": 2200,
        "grade_code": "A",
        "kaisai_nengappi": "20261201",
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.to_list() == ["nar_dirt_long_A_winter"]


def test_assign_subgroup_keys_jra_turf_extended_spring():
    df = pl.DataFrame([{
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 3000,
        "grade_code": "G1",
        "kaisai_nengappi": "20260405",
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.to_list() == ["jra_turf_extended_G1_spring"]


def test_assign_subgroup_keys_banei_dirt_sprint_autumn():
    df = pl.DataFrame([{
        "source": "nar",
        "keibajo_code": "83",
        "track_code": "10",
        "kyori": 200,
        "grade_code": "B",
        "kaisai_nengappi": "20261010",
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.to_list() == ["banei_dirt_sprint_B_autumn"]


def test_assign_subgroup_keys_multiple_rows_with_class_and_season():
    df = pl.DataFrame([
        {"source": "jra", "keibajo_code": "10", "track_code": "23", "kyori": 1100,
         "grade_code": "OP", "kaisai_nengappi": "20260115"},
        {"source": "nar", "keibajo_code": "40", "track_code": "10", "kyori": 1800,
         "grade_code": "C", "kaisai_nengappi": "20260720"},
    ])
    result = subject.assign_subgroup_keys(df)
    assert result.to_list() == ["jra_dirt_sprint_OP_winter", "nar_dirt_intermediate_C_summer"]


def test_assign_subgroup_keys_missing_grade_code_column_uses_unknown():
    df = pl.DataFrame([{
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 1400,
        "kaisai_nengappi": "20260615",
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.to_list() == ["jra_turf_mile_unknown_summer"]


def test_assign_subgroup_keys_missing_kaisai_nengappi_column_uses_unknown():
    df = pl.DataFrame([{
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 1400,
        "grade_code": "G1",
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.to_list() == ["jra_turf_mile_G1_unknown"]


def test_assign_subgroup_keys_missing_both_optional_columns_uses_unknown():
    df = pl.DataFrame([{
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 1400,
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.to_list() == ["jra_turf_mile_unknown_unknown"]


def test_assign_subgroup_keys_null_grade_code_uses_unknown():
    df = pl.DataFrame([{
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 1400,
        "grade_code": None,
        "kaisai_nengappi": "20260615",
    }])
    result = subject.assign_subgroup_keys(df)
    assert result.to_list() == ["jra_turf_mile_unknown_summer"]


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


def test_dcg_at_3_two_positions_uses_two_discount_slots():
    dcg = subject._dcg_at_3([1, 2])
    expected = 3.0 / np.log2(2) + 2.0 / np.log2(3)
    assert abs(dcg - expected) < 1e-9


def test_ideal_dcg_at_3_matches_reference_formula():
    result = subject._ideal_dcg_at_3([3.0, 2.0, 1.0])
    expected = 3.0 / np.log2(2) + 2.0 / np.log2(3) + 1.0 / np.log2(4)
    assert abs(result - expected) < 1e-9


def test_ideal_dcg_at_3_empty_returns_zero():
    assert subject._ideal_dcg_at_3([]) == 0.0


def test_ideal_dcg_at_3_single_relevance_uses_first_discount():
    result = subject._ideal_dcg_at_3([3.0])
    expected = 3.0 / np.log2(2)
    assert abs(result - expected) < 1e-9


def test_compute_race_ndcg_perfect_returns_one():
    group = pl.DataFrame({
        "race_id": ["r1"] * 5,
        "ketto_toroku_bango": ["a", "b", "c", "d", "e"],
        "predicted_rank": [1, 2, 3, 4, 5],
        "finish_position": [1, 2, 3, 4, 5],
    })
    result = subject.compute_race_ndcg(group)
    assert abs(result - 1.0) < 1e-9


def test_compute_race_ndcg_worst_order_is_less_than_one():
    group = pl.DataFrame({
        "race_id": ["r1"] * 5,
        "ketto_toroku_bango": ["a", "b", "c", "d", "e"],
        "predicted_rank": [1, 2, 3, 4, 5],
        "finish_position": [5, 4, 3, 2, 1],
    })
    result = subject.compute_race_ndcg(group)
    assert result < 1.0


def test_compute_race_ndcg_two_horse_perfect_gives_one():
    group = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "predicted_rank": [1, 2],
        "finish_position": [1, 2],
    })
    result = subject.compute_race_ndcg(group)
    assert abs(result - 1.0) < 1e-9


def test_compute_race_ndcg_all_irrelevant_gives_zero():
    group = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "predicted_rank": [1, 2],
        "finish_position": [4, 5],
    })
    result = subject.compute_race_ndcg(group)
    assert result == 0.0


def test_compute_race_ndcg_with_null_finish_position():
    group = pl.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1, 2, 3],
        "finish_position": [1.0, None, 3.0],
    })
    result = subject.compute_race_ndcg(group)
    assert 0.0 <= result <= 1.0


def test_compute_race_top1_true_when_predicted_rank1_finishes_first():
    group = pl.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1, 2, 3],
        "finish_position": [1, 2, 3],
    })
    assert subject.compute_race_top1(group) is True


def test_compute_race_top1_false_when_predicted_rank1_finishes_second():
    group = pl.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1, 2, 3],
        "finish_position": [2, 1, 3],
    })
    assert subject.compute_race_top1(group) is False


def test_compute_race_top1_false_when_predicted_rank1_has_null_finish_position():
    group = pl.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1, 2, 3],
        "finish_position": [None, 1.0, 2.0],
    })
    assert subject.compute_race_top1(group) is False


def test_compute_race_top3_box_true_when_exact_match():
    group = pl.DataFrame({
        "race_id": ["r1"] * 4,
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 3, 4],
    })
    assert subject.compute_race_top3_box(group) is True


def test_compute_race_top3_box_true_when_same_horses_different_order():
    group = pl.DataFrame({
        "race_id": ["r1"] * 4,
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [2, 1, 3, 4],
    })
    assert subject.compute_race_top3_box(group) is True


def test_compute_race_top3_box_false_when_different_horses():
    group = pl.DataFrame({
        "race_id": ["r1"] * 4,
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1, 2, 4, 3],
    })
    assert subject.compute_race_top3_box(group) is False


def test_compute_race_top3_box_excludes_null_predicted_rank():
    # Horse "a" (winner) has null predicted_rank — should NOT appear in predicted_top3.
    # predicted_top3 = {"b", "c"}, actual_top3 = {"a", "b", "c"} → False
    group = pl.DataFrame({
        "race_id": ["r1"] * 3,
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [None, 1.0, 2.0],
        "finish_position": [1, 2, 3],
    })
    assert subject.compute_race_top3_box(group) is False


def test_evaluate_subgroup_empty_df_returns_zero_race_count():
    empty = pl.DataFrame(
        schema={
            "race_id": pl.Utf8,
            "ketto_toroku_bango": pl.Utf8,
            "predicted_rank": pl.Int64,
            "finish_position": pl.Int64,
        }
    )
    result = subject.evaluate_subgroup(empty)
    assert result["race_count"] == 0
    assert result["ndcg_at_3"] == 0.0
    assert result["top1_accuracy"] == 0.0
    assert result["top3_box_accuracy"] == 0.0


def test_evaluate_subgroup_single_race_perfect():
    joined = pl.DataFrame({
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
    joined = pl.DataFrame({
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
    joined = pl.DataFrame({
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
    joined = pl.DataFrame({
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
    joined = pl.DataFrame({
        "race_id": ["r1"] * 4,
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1, 2, 3, 4],
        "finish_position": [1.0, 2.0, None, None],
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
            "grade_code": "G2",
            "kaisai_nengappi": "20260615",
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "b",
            "finish_position": 2,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "10",
            "kyori": 1400,
            "grade_code": "G2",
            "kaisai_nengappi": "20260615",
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "c",
            "finish_position": 3,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "10",
            "kyori": 1400,
            "grade_code": "G2",
            "kaisai_nengappi": "20260615",
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "jra_turf_mile_G2_summer"
    assert results[0]["race_count"] == 1


def test_compute_subgroup_diagnostics_empty_ground_truth_returns_empty():
    ground_truth = pl.DataFrame(
        schema={
            "race_id": pl.Utf8,
            "ketto_toroku_bango": pl.Utf8,
            "finish_position": pl.Int64,
            "source": pl.Utf8,
            "keibajo_code": pl.Utf8,
            "track_code": pl.Utf8,
            "kyori": pl.Int64,
        }
    )
    predictions = _make_predictions([{
        "race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1,
    }])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert results == []


def test_compute_subgroup_diagnostics_empty_predictions_returns_zero_ndcg():
    # left join: ground_truth rows appear with null predicted_rank → NDCG=0.0 (all races penalized).
    ground_truth = _make_ground_truth([{
        "race_id": "r1",
        "ketto_toroku_bango": "a",
        "finish_position": 1,
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 1400,
        "grade_code": "A",
        "kaisai_nengappi": "20260315",
    }])
    predictions = pl.DataFrame(
        schema={
            "race_id": pl.Utf8,
            "ketto_toroku_bango": pl.Utf8,
            "predicted_rank": pl.Int64,
        }
    )
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["ndcg_at_3"] == 0.0
    assert results[0]["race_count"] == 1


def test_compute_subgroup_diagnostics_no_matching_rows_returns_zero_ndcg():
    # left join: ground_truth horses without matching predictions get null predicted_rank → NDCG=0.0.
    ground_truth = _make_ground_truth([{
        "race_id": "r1",
        "ketto_toroku_bango": "a",
        "finish_position": 1,
        "source": "jra",
        "keibajo_code": "10",
        "track_code": "10",
        "kyori": 1400,
        "grade_code": "A",
        "kaisai_nengappi": "20260315",
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
            "grade_code": "G1",
            "kaisai_nengappi": "20260405",
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "b",
            "finish_position": 2,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "10",
            "kyori": 1400,
            "grade_code": "G1",
            "kaisai_nengappi": "20260405",
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "c",
            "finish_position": 3,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "10",
            "kyori": 1400,
            "grade_code": "G1",
            "kaisai_nengappi": "20260405",
        },
        {
            "race_id": "r2",
            "ketto_toroku_bango": "d",
            "finish_position": 1,
            "source": "nar",
            "keibajo_code": "40",
            "track_code": "10",
            "kyori": 1100,
            "grade_code": "C",
            "kaisai_nengappi": "20261215",
        },
        {
            "race_id": "r2",
            "ketto_toroku_bango": "e",
            "finish_position": 2,
            "source": "nar",
            "keibajo_code": "40",
            "track_code": "10",
            "kyori": 1100,
            "grade_code": "C",
            "kaisai_nengappi": "20261215",
        },
        {
            "race_id": "r2",
            "ketto_toroku_bango": "f",
            "finish_position": 3,
            "source": "nar",
            "keibajo_code": "40",
            "track_code": "10",
            "kyori": 1100,
            "grade_code": "C",
            "kaisai_nengappi": "20261215",
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
    assert {results[0]["subgroup"], results[1]["subgroup"]} == {
        "jra_turf_mile_G1_spring", "nar_dirt_sprint_C_winter",
    }


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
            "grade_code": "B",
            "kaisai_nengappi": "20260810",
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "b",
            "finish_position": 2,
            "source": "nar",
            "keibajo_code": "83",
            "track_code": "10",
            "kyori": 200,
            "grade_code": "B",
            "kaisai_nengappi": "20260810",
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "c",
            "finish_position": 3,
            "source": "nar",
            "keibajo_code": "83",
            "track_code": "10",
            "kyori": 200,
            "grade_code": "B",
            "kaisai_nengappi": "20260810",
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "banei_dirt_sprint_B_summer"


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
            "grade_code": "OP",
            "kaisai_nengappi": "20260920",
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "b",
            "finish_position": 2,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "25",
            "kyori": 1400,
            "grade_code": "OP",
            "kaisai_nengappi": "20260920",
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "c",
            "finish_position": 3,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "25",
            "kyori": 1400,
            "grade_code": "OP",
            "kaisai_nengappi": "20260920",
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "jra_dirt_mile_OP_autumn"


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
            "grade_code": "G3",
            "kaisai_nengappi": "20260215",
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "b",
            "finish_position": 2,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "99",
            "kyori": 2500,
            "grade_code": "G3",
            "kaisai_nengappi": "20260215",
        },
        {
            "race_id": "r1",
            "ketto_toroku_bango": "c",
            "finish_position": 3,
            "source": "jra",
            "keibajo_code": "10",
            "track_code": "99",
            "kyori": 2500,
            "grade_code": "G3",
            "kaisai_nengappi": "20260215",
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "jra_other_extended_G3_winter"


def test_evaluate_subgroup_top3_not_counted_when_fewer_than_3_valid_predicted_ranks():
    joined = pl.DataFrame({
        "race_id": ["r1"] * 4,
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1.0, 2.0, None, None],
        "finish_position": [1.0, 2.0, 3.0, 4.0],
    })
    result = subject.evaluate_subgroup(joined)
    assert result["race_count"] == 1
    assert result["top3_box_accuracy"] == 0.0


def test_compute_race_top1_returns_false_when_all_predicted_rank_null():
    group = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "predicted_rank": [None, None],
        "finish_position": [1.0, 2.0],
    })
    assert subject.compute_race_top1(group) is False


def test_compute_race_ndcg_excludes_null_predicted_rank_from_dcg_but_not_ideal():
    # Horse "a" has no predicted rank but finishes 1st (rel=3.0).
    # DCG: only b(rank=1,finish=2nd,rel=2.0) and c(rank=2,finish=3rd,rel=1.0) contribute.
    # Ideal: all 3 horses (including a) define the best possible ordering.
    # So NDCG is penalized for missing the winner, not inflated to 1.0.
    group = pl.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [None, 1.0, 2.0],
        "finish_position": [1.0, 2.0, 3.0],
    })
    result = subject.compute_race_ndcg(group)
    # exact: DCG = 2/log2(2) + 1/log2(3); ideal = 3/log2(2) + 2/log2(3) + 1/log2(4)
    dcg = 2.0 / np.log2(2) + 1.0 / np.log2(3)
    ideal_dcg = 3.0 / np.log2(2) + 2.0 / np.log2(3) + 1.0 / np.log2(4)
    assert abs(result - dcg / ideal_dcg) < 1e-9


def test_compute_race_ndcg_returns_one_when_predicted_perfectly_among_ranked_horses():
    # No null predicted_rank: perfect ranking → NDCG = 1.0.
    group = pl.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1.0, 2.0, 3.0],
        "finish_position": [1.0, 2.0, 3.0],
    })
    result = subject.compute_race_ndcg(group)
    assert abs(result - 1.0) < 1e-9


def test_compute_race_ndcg_excludes_null_finish_position_from_dcg_slot():
    # Horse "a" has predicted_rank=1 but finish_position=null (e.g. scratched).
    # It must NOT occupy a DCG slot — only "b" (rank=2, finish=1) contributes.
    # Ideal: only "b" has a known finish_position.
    # Result: perfect prediction among scoreable horses → NDCG = 1.0.
    group = pl.DataFrame({
        "race_id": ["r1", "r1"],
        "ketto_toroku_bango": ["a", "b"],
        "predicted_rank": [1.0, 2.0],
        "finish_position": [None, 1.0],
    })
    result = subject.compute_race_ndcg(group)
    assert abs(result - 1.0) < 1e-9


def test_compute_subgroup_diagnostics_penalizes_unpredicted_winner_via_left_join():
    # ground_truth has 3 horses. predictions only covers b and c — the winner (a) is absent.
    # After left join, "a" appears with null predicted_rank and finish_position=1.
    # NDCG must be < 1.0 because the winner was never ranked.
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1", "ketto_toroku_bango": "a", "finish_position": 1,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1600,
            "grade_code": "A", "kaisai_nengappi": "20260715",
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "b", "finish_position": 2,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1600,
            "grade_code": "A", "kaisai_nengappi": "20260715",
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "c", "finish_position": 3,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1600,
            "grade_code": "A", "kaisai_nengappi": "20260715",
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 2},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["ndcg_at_3"] < 1.0


def test_compute_subgroup_diagnostics_does_not_mutate_input_frames():
    # The internal `_subgroup` column is added to the join result, never to the
    # caller's frames; the new column must not leak back into the inputs.
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1", "ketto_toroku_bango": "a", "finish_position": 1,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "grade_code": "A", "kaisai_nengappi": "20260615",
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "b", "finish_position": 2,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "grade_code": "A", "kaisai_nengappi": "20260615",
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
    ])
    subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert ground_truth.columns == [
        "race_id", "ketto_toroku_bango", "finish_position",
        "source", "keibajo_code", "track_code", "kyori",
        "grade_code", "kaisai_nengappi",
    ]
    assert predictions.columns == ["race_id", "ketto_toroku_bango", "predicted_rank"]


def test_compute_subgroup_diagnostics_perfect_prediction_scores_one():
    # End-to-end sanity: a perfectly ranked single-subgroup race
    # yields NDCG=1.0 and top1/top3 accuracy 1.0.
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1", "ketto_toroku_bango": "a", "finish_position": 1,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "grade_code": "G1", "kaisai_nengappi": "20260405",
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "b", "finish_position": 2,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "grade_code": "G1", "kaisai_nengappi": "20260405",
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "c", "finish_position": 3,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "grade_code": "G1", "kaisai_nengappi": "20260405",
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert results[0]["ndcg_at_3"] == 1.0
    assert results[0]["top1_accuracy"] == 1.0
    assert results[0]["top3_box_accuracy"] == 1.0


def _reference_metrics(joined: pl.DataFrame) -> tuple[float, float, float]:
    # Scalar per-race reference, mirroring the pre-vectorization loop body.
    ndcg_scores: list[float] = []
    top1_hits = 0
    top3_hits = 0
    race_count = 0
    for (_race_id,), group in joined.group_by("race_id", maintain_order=True):
        race_count += 1
        ndcg_scores.append(subject.compute_race_ndcg(group))
        if subject.compute_race_top1(group):
            top1_hits += 1
        if (
            group["finish_position"].is_not_null().sum() >= 3
            and group["predicted_rank"].is_not_null().sum() >= 3
            and subject.compute_race_top3_box(group)
        ):
            top3_hits += 1
    safe = max(race_count, 1)
    ndcg = float(np.mean(ndcg_scores)) if ndcg_scores else 0.0
    return ndcg, top1_hits / safe, top3_hits / safe


def test_evaluate_subgroup_matches_scalar_reference_on_messy_multi_race():
    joined = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1", "r2", "r2", "r2", "r3", "r3", "r3", "r3"],
        "ketto_toroku_bango": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"],
        "predicted_rank": [1.0, 2.0, 3.0, None, 1.0, 2.0, None, 3.0, 1.0, 2.0, 4.0],
        "finish_position": [2.0, 1.0, 3.0, 4.0, None, 1.0, 2.0, 1.0, 3.0, 2.0, 4.0],
    })
    ref_ndcg, ref_top1, ref_top3 = _reference_metrics(joined)
    result = subject.evaluate_subgroup(joined)
    assert result["race_count"] == 3
    assert abs(result["ndcg_at_3"] - ref_ndcg) < 1e-9
    assert result["top1_accuracy"] == ref_top1
    assert result["top3_box_accuracy"] == ref_top3


def test_evaluate_subgroup_top1_tie_uses_first_in_stable_order():
    # Two horses share predicted_rank=1; the first row (winner) must be the top1 pick.
    joined = pl.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c"],
        "predicted_rank": [1.0, 1.0, 2.0],
        "finish_position": [1.0, 2.0, 3.0],
    })
    ref_ndcg, ref_top1, ref_top3 = _reference_metrics(joined)
    result = subject.evaluate_subgroup(joined)
    assert result["top1_accuracy"] == 1.0
    assert result["top1_accuracy"] == ref_top1
    assert abs(result["ndcg_at_3"] - ref_ndcg) < 1e-9
    assert result["top3_box_accuracy"] == ref_top3


def test_evaluate_subgroup_top3_box_tie_uses_stable_order():
    # Predicted ranks tie at slot 3/4; finish positions also tie — the vectorized
    # ordinal rank must select the same first-3 horse sets as the stable sort.
    joined = pl.DataFrame({
        "race_id": ["r1", "r1", "r1", "r1"],
        "ketto_toroku_bango": ["a", "b", "c", "d"],
        "predicted_rank": [1.0, 2.0, 3.0, 3.0],
        "finish_position": [1.0, 2.0, 3.0, 3.0],
    })
    ref_ndcg, ref_top1, ref_top3 = _reference_metrics(joined)
    result = subject.evaluate_subgroup(joined)
    assert result["top3_box_accuracy"] == ref_top3
    assert result["top1_accuracy"] == ref_top1
    assert abs(result["ndcg_at_3"] - ref_ndcg) < 1e-9


def test_assign_subgroup_keys_matches_scalar_helpers_row_by_row():
    df = pl.DataFrame([
        {"source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1000,
         "grade_code": "G1", "kaisai_nengappi": "20260315"},
        {"source": "jra", "keibajo_code": "10", "track_code": "25", "kyori": 1600,
         "grade_code": "OP", "kaisai_nengappi": "20260720"},
        {"source": "jra", "keibajo_code": "10", "track_code": "99", "kyori": 2400,
         "grade_code": "A", "kaisai_nengappi": "20261105"},
        {"source": "nar", "keibajo_code": "40", "track_code": "10", "kyori": 1300,
         "grade_code": "C", "kaisai_nengappi": "20260120"},
        {"source": "nar", "keibajo_code": "83", "track_code": "22", "kyori": 200,
         "grade_code": "B", "kaisai_nengappi": "20260810"},
        {"source": "jra", "keibajo_code": "83", "track_code": "10", "kyori": 2100,
         "grade_code": "NEW", "kaisai_nengappi": "20261220"},
    ])
    expected = [
        subject.make_subgroup_key(
            subject.get_source_label(row["source"], row["keibajo_code"]),
            subject.get_surface_label(
                row["track_code"],
                subject.get_source_label(row["source"], row["keibajo_code"]),
            ),
            subject.get_distance_band(int(row["kyori"])),
            subject.get_class_label(row["grade_code"]),
            subject.get_season(int(str(row["kaisai_nengappi"])[4:6])),
        )
        for row in df.iter_rows(named=True)
    ]
    assert subject.assign_subgroup_keys(df).to_list() == expected


def test_season_expr_all_four_seasons():
    df = pl.DataFrame({
        "source": ["jra"] * 4,
        "keibajo_code": ["10"] * 4,
        "track_code": ["10"] * 4,
        "kyori": [1400] * 4,
        "grade_code": ["A"] * 4,
        "kaisai_nengappi": ["20260315", "20260720", "20261015", "20261215"],
    })
    keys = subject.assign_subgroup_keys(df).to_list()
    assert keys[0].endswith("_spring")
    assert keys[1].endswith("_summer")
    assert keys[2].endswith("_autumn")
    assert keys[3].endswith("_winter")


def test_season_expr_january_february_are_winter():
    df = pl.DataFrame({
        "source": ["jra", "jra"],
        "keibajo_code": ["10", "10"],
        "track_code": ["10", "10"],
        "kyori": [1400, 1400],
        "grade_code": ["A", "A"],
        "kaisai_nengappi": ["20260115", "20260220"],
    })
    keys = subject.assign_subgroup_keys(df).to_list()
    assert keys[0].endswith("_winter")
    assert keys[1].endswith("_winter")


def test_class_expr_various_grade_codes():
    df = pl.DataFrame({
        "source": ["jra"] * 5,
        "keibajo_code": ["10"] * 5,
        "track_code": ["10"] * 5,
        "kyori": [1400] * 5,
        "grade_code": ["G1", "G2", "G3", "OP", "NEW"],
        "kaisai_nengappi": ["20260615"] * 5,
    })
    keys = subject.assign_subgroup_keys(df).to_list()
    assert keys[0] == "jra_turf_mile_G1_summer"
    assert keys[1] == "jra_turf_mile_G2_summer"
    assert keys[2] == "jra_turf_mile_G3_summer"
    assert keys[3] == "jra_turf_mile_OP_summer"
    assert keys[4] == "jra_turf_mile_NEW_summer"


def test_compute_subgroup_diagnostics_missing_optional_columns_uses_unknown():
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1", "ketto_toroku_bango": "a", "finish_position": 1,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "b", "finish_position": 2,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "c", "finish_position": 3,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "jra_turf_mile_unknown_unknown"
    assert results[0]["race_count"] == 1


def test_compute_subgroup_diagnostics_missing_only_grade_code():
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1", "ketto_toroku_bango": "a", "finish_position": 1,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "kaisai_nengappi": "20260915",
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "b", "finish_position": 2,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "kaisai_nengappi": "20260915",
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "c", "finish_position": 3,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "kaisai_nengappi": "20260915",
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "jra_turf_mile_unknown_autumn"


def test_compute_subgroup_diagnostics_missing_only_kaisai_nengappi():
    ground_truth = _make_ground_truth([
        {
            "race_id": "r1", "ketto_toroku_bango": "a", "finish_position": 1,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "grade_code": "G2",
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "b", "finish_position": 2,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "grade_code": "G2",
        },
        {
            "race_id": "r1", "ketto_toroku_bango": "c", "finish_position": 3,
            "source": "jra", "keibajo_code": "10", "track_code": "10", "kyori": 1400,
            "grade_code": "G2",
        },
    ])
    predictions = _make_predictions([
        {"race_id": "r1", "ketto_toroku_bango": "a", "predicted_rank": 1},
        {"race_id": "r1", "ketto_toroku_bango": "b", "predicted_rank": 2},
        {"race_id": "r1", "ketto_toroku_bango": "c", "predicted_rank": 3},
    ])
    results = subject.compute_subgroup_diagnostics(predictions, ground_truth)
    assert len(results) == 1
    assert results[0]["subgroup"] == "jra_turf_mile_G2_unknown"


def test_season_expr_december_is_winter():
    df = pl.DataFrame({
        "source": ["jra"],
        "keibajo_code": ["10"],
        "track_code": ["10"],
        "kyori": [1400],
        "grade_code": ["A"],
        "kaisai_nengappi": ["20261201"],
    })
    keys = subject.assign_subgroup_keys(df).to_list()
    assert keys[0] == "jra_turf_mile_A_winter"
