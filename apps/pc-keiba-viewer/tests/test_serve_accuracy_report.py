"""Tests for serve_accuracy_report module.

Covers all pure-compute helpers and CLI arg parsing.
I/O functions (query_*) are tested with mock connections.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from pytest import CaptureFixture

import serve_accuracy_report as subject


# ── infer_era ─────────────────────────────────────────────────────────────────


def test_infer_era_none_returns_unknown() -> None:
    assert subject.infer_era(None) == "UNKNOWN"


def test_infer_era_before_cutoff_returns_degraded() -> None:
    # 2026-06-10 23:59 UTC = before cutoff
    gen_at = datetime(2026, 6, 10, 23, 59, 0, tzinfo=timezone.utc)
    assert subject.infer_era(gen_at) == "DEGRADED"


def test_infer_era_one_minute_before_cutoff_returns_degraded() -> None:
    # 2026-06-11 00:29 UTC = 09:29 JST = before the 09:30 cron fix
    gen_at = datetime(2026, 6, 11, 0, 29, 0, tzinfo=timezone.utc)
    assert subject.infer_era(gen_at) == "DEGRADED"


def test_infer_era_at_cutoff_returns_post_fix() -> None:
    # 2026-06-11 00:30 UTC = 09:30 JST = cron fix went live
    gen_at = datetime(2026, 6, 11, 0, 30, 0, tzinfo=timezone.utc)
    assert subject.infer_era(gen_at) == "POST_FIX"


def test_infer_era_after_cutoff_returns_post_fix() -> None:
    gen_at = datetime(2026, 6, 14, 9, 30, 0, tzinfo=timezone.utc)
    assert subject.infer_era(gen_at) == "POST_FIX"


def test_infer_era_old_date_returns_degraded() -> None:
    gen_at = datetime(2026, 5, 1, 3, 0, 0, tzinfo=timezone.utc)
    assert subject.infer_era(gen_at) == "DEGRADED"


# ── compute_corner1_norm ──────────────────────────────────────────────────────


def test_corner1_norm_zero_returns_none() -> None:
    # corner_1 = '00' means no corner 1 data (straight track)
    assert subject.compute_corner1_norm("00", 16) is None


def test_corner1_norm_empty_returns_none() -> None:
    assert subject.compute_corner1_norm("", 16) is None


def test_corner1_norm_non_digit_returns_none() -> None:
    assert subject.compute_corner1_norm("xx", 16) is None


def test_corner1_norm_small_field_returns_none() -> None:
    # shusso_tosu <= 1 → undefined
    assert subject.compute_corner1_norm("01", 1) is None


def test_corner1_norm_leader_returns_zero() -> None:
    # Horse at position 1 = leader = 0.0
    result = subject.compute_corner1_norm("01", 16)
    assert result == pytest.approx(0.0)


def test_corner1_norm_last_returns_one() -> None:
    # Last position in field of 16 → (16-1)/(16-1) = 1.0
    result = subject.compute_corner1_norm("16", 16)
    assert result == pytest.approx(1.0)


def test_corner1_norm_middle_field_16() -> None:
    # Position 8 in field of 16 → (8-1)/(16-1) = 7/15 ≈ 0.467
    result = subject.compute_corner1_norm("08", 16)
    assert result == pytest.approx(7 / 15)


def test_corner1_norm_position_2_field_10() -> None:
    result = subject.compute_corner1_norm("02", 10)
    assert result == pytest.approx(1 / 9)


# ── classify_running_style ────────────────────────────────────────────────────


def test_classify_rs_none_returns_none() -> None:
    assert subject.classify_running_style(None) is None


def test_classify_rs_zero_is_nige() -> None:
    assert subject.classify_running_style(0.0) == subject.RS_CLASS_NIGE


def test_classify_rs_senkou_boundary() -> None:
    assert subject.classify_running_style(0.3) == subject.RS_CLASS_SENKOU


def test_classify_rs_just_above_zero_is_senkou() -> None:
    assert subject.classify_running_style(0.01) == subject.RS_CLASS_SENKOU


def test_classify_rs_sashi_boundary() -> None:
    assert subject.classify_running_style(0.7) == subject.RS_CLASS_SASHI


def test_classify_rs_just_above_senkou_threshold_is_sashi() -> None:
    assert subject.classify_running_style(0.31) == subject.RS_CLASS_SASHI


def test_classify_rs_above_sashi_threshold_is_oikomi() -> None:
    assert subject.classify_running_style(0.71) == subject.RS_CLASS_OIKOMI


def test_classify_rs_one_is_oikomi() -> None:
    assert subject.classify_running_style(1.0) == subject.RS_CLASS_OIKOMI


# ── classify_distance_band ────────────────────────────────────────────────────


def test_classify_distance_band_sprint_boundary() -> None:
    assert subject.classify_distance_band(1400) == "sprint"


def test_classify_distance_band_sprint_low() -> None:
    assert subject.classify_distance_band(1000) == "sprint"


def test_classify_distance_band_mile_boundary() -> None:
    assert subject.classify_distance_band(1800) == "mile"


def test_classify_distance_band_mile_just_above_sprint() -> None:
    assert subject.classify_distance_band(1401) == "mile"


def test_classify_distance_band_intermediate_boundary() -> None:
    assert subject.classify_distance_band(2200) == "intermediate"


def test_classify_distance_band_intermediate_just_above_mile() -> None:
    assert subject.classify_distance_band(1801) == "intermediate"


def test_classify_distance_band_long_boundary() -> None:
    assert subject.classify_distance_band(2800) == "long"


def test_classify_distance_band_long_just_above_intermediate() -> None:
    assert subject.classify_distance_band(2201) == "long"


def test_classify_distance_band_extended() -> None:
    assert subject.classify_distance_band(3200) == "extended"


# ── classify_field_size_band ──────────────────────────────────────────────────


def test_classify_field_size_band_small_boundary() -> None:
    assert subject.classify_field_size_band(8) == "small"


def test_classify_field_size_band_small_low() -> None:
    assert subject.classify_field_size_band(5) == "small"


def test_classify_field_size_band_medium_boundary() -> None:
    assert subject.classify_field_size_band(14) == "medium"


def test_classify_field_size_band_medium_just_above_small() -> None:
    assert subject.classify_field_size_band(9) == "medium"


def test_classify_field_size_band_large() -> None:
    assert subject.classify_field_size_band(18) == "large"


# ── classify_season_band ──────────────────────────────────────────────────────


def test_classify_season_band_spring() -> None:
    assert subject.classify_season_band("0415") == "spring"


def test_classify_season_band_summer() -> None:
    assert subject.classify_season_band("0701") == "summer"


def test_classify_season_band_autumn() -> None:
    assert subject.classify_season_band("1031") == "autumn"


def test_classify_season_band_winter_january() -> None:
    assert subject.classify_season_band("0102") == "winter"


def test_classify_season_band_winter_december() -> None:
    assert subject.classify_season_band("1225") == "winter"


# ── aggregate_fp_metrics ──────────────────────────────────────────────────────


def test_aggregate_fp_metrics_empty() -> None:
    result = subject.aggregate_fp_metrics([])
    assert result == (0, 0, 0, 0, 0)


def test_aggregate_fp_metrics_top1_hit() -> None:
    # Race with predicted rank 1 winning; pred rank 3 misses top3 so box is 0
    race_rows = [[(1, 1), (2, 3), (3, 5)]]
    top1, place2, place3, _fk2, top3_box = subject.aggregate_fp_metrics(race_rows)
    assert top1 == 1
    assert place2 == 1
    assert place3 == 1
    assert top3_box == 0


def test_aggregate_fp_metrics_place2_hit_but_not_top1() -> None:
    # pred rank 3 finishes 5th so box fails even though pred1 and pred2 hit
    race_rows = [[(1, 2), (2, 1), (3, 5)]]
    top1, place2, place3, _fukusho_2p, top3_box = subject.aggregate_fp_metrics(race_rows)
    assert top1 == 0
    assert place2 == 1
    assert place3 == 1
    assert top3_box == 0


def test_aggregate_fp_metrics_place3_hit() -> None:
    # pred rank 2 finishes 5th so box fails even though pred1 and pred3 hit top3
    race_rows = [[(1, 3), (2, 5), (3, 1)]]
    top1, place2, place3, _fk, top3_box = subject.aggregate_fp_metrics(race_rows)
    assert top1 == 0
    assert place2 == 0
    assert place3 == 1
    assert top3_box == 0


def test_aggregate_fp_metrics_top3_box_hit_when_all_three_in_top3() -> None:
    # All three predicted top horses land in the actual top 3
    race_rows = [[(1, 2), (2, 3), (3, 1)]]
    top1, place2, place3, _fk, top3_box = subject.aggregate_fp_metrics(race_rows)
    assert top1 == 0
    assert place2 == 1
    assert place3 == 1
    assert top3_box == 1


def test_aggregate_fp_metrics_top3_box_zero_when_one_prediction_misses() -> None:
    # pred1 and pred2 hit, pred3 misses — place3=1 but top3_box=0
    race_rows = [[(1, 1), (2, 2), (3, 4)]]
    _top1, _place2, place3, _fk, top3_box = subject.aggregate_fp_metrics(race_rows)
    assert place3 == 1
    assert top3_box == 0


def test_aggregate_fp_metrics_all_miss() -> None:
    race_rows = [[(1, 8), (2, 7), (3, 6)]]
    top1, place2, place3, fukusho_2p, top3_box = subject.aggregate_fp_metrics(race_rows)
    assert top1 == 0
    assert place2 == 0
    assert place3 == 0
    assert fukusho_2p == 0
    assert top3_box == 0


def test_aggregate_fp_metrics_fukusho_2p_hit_via_second_prediction() -> None:
    # Predicted rank 2 finishes 2nd → fukusho_2p hit
    race_rows = [[(1, 5), (2, 2), (3, 6)]]
    top1, place2, _place3, fukusho_2p, _top3_box = subject.aggregate_fp_metrics(race_rows)
    assert top1 == 0
    assert place2 == 0
    assert fukusho_2p == 1


def test_aggregate_fp_metrics_multiple_races() -> None:
    race_rows = [
        [(1, 1)],  # top1 hit
        [(1, 4)],  # miss
        [(1, 2)],  # place2 hit
    ]
    top1, place2, place3, _fk3, _tb3 = subject.aggregate_fp_metrics(race_rows)
    assert top1 == 1
    assert place2 == 2  # race 1 and race 3
    assert place3 == 2


def test_aggregate_fp_metrics_race_no_rank1() -> None:
    # If no horse has predicted_rank=1, no top1/place2/place3/top3_box hits
    race_rows = [[(2, 1), (3, 2)]]
    top1, place2, _place3_2, _fk4, top3_box = subject.aggregate_fp_metrics(race_rows)
    assert top1 == 0
    assert place2 == 0
    assert top3_box == 0


# ── SubgroupAccuracy properties ───────────────────────────────────────────────


def test_subgroup_accuracy_pcts_zero_races() -> None:
    sg = subject.SubgroupAccuracy(
        dimension="distance_band", band="sprint", races=0,
        top1_hits=0, place2_hits=0, place3_hits=0,
        fukusho_2p_hits=0, top3_box_hits=0,
    )
    assert sg.top1_pct == 0.0
    assert sg.place2_pct == 0.0
    assert sg.place3_pct == 0.0
    assert sg.fukusho_2p_pct == 0.0
    assert sg.top3_box_pct == 0.0


def test_subgroup_accuracy_pcts_nonzero() -> None:
    sg = subject.SubgroupAccuracy(
        dimension="venue", band="05", races=4,
        top1_hits=1, place2_hits=2, place3_hits=3,
        fukusho_2p_hits=2, top3_box_hits=3,
    )
    assert sg.top1_pct == pytest.approx(25.0)
    assert sg.place2_pct == pytest.approx(50.0)
    assert sg.place3_pct == pytest.approx(75.0)
    assert sg.fukusho_2p_pct == pytest.approx(50.0)
    assert sg.top3_box_pct == pytest.approx(75.0)


# ── compute_subgroup_accuracies ───────────────────────────────────────────────


def test_compute_subgroup_accuracies_empty() -> None:
    assert subject.compute_subgroup_accuracies([]) == []


def test_compute_subgroup_accuracies_single_race_four_dims() -> None:
    # one race: sprint / small / spring / venue 05, pred1 wins
    partitions = [("sprint", "small", "spring", "05", [(1, 1), (2, 3)])]
    result = subject.compute_subgroup_accuracies(partitions)
    # 4 dimensions, each with exactly 1 band → 4 entries
    assert len(result) == 4
    assert result[0].dimension == "distance_band"
    assert result[0].band == "sprint"
    assert result[0].races == 1
    assert result[0].top1_hits == 1
    assert result[1].dimension == "field_size_band"
    assert result[1].band == "small"
    assert result[2].dimension == "season_band"
    assert result[2].band == "spring"
    assert result[3].dimension == "venue"
    assert result[3].band == "05"
    assert result[3].top1_hits == 1


def test_compute_subgroup_accuracies_distance_split() -> None:
    # two races differing only in distance band; field/season/venue identical
    partitions = [
        ("sprint", "medium", "summer", "05", [(1, 1)]),      # sprint top1 hit
        ("mile", "medium", "summer", "05", [(1, 4)]),        # mile miss
    ]
    result = subject.compute_subgroup_accuracies(partitions)
    distance = [sg for sg in result if sg.dimension == "distance_band"]
    assert len(distance) == 2
    # sorted by band: mile then sprint
    assert distance[0].band == "mile"
    assert distance[0].races == 1
    assert distance[0].top1_hits == 0
    assert distance[1].band == "sprint"
    assert distance[1].races == 1
    assert distance[1].top1_hits == 1
    # field_size_band collapses to one band ("medium") spanning both races
    field = [sg for sg in result if sg.dimension == "field_size_band"]
    assert len(field) == 1
    assert field[0].band == "medium"
    assert field[0].races == 2
    assert field[0].top1_hits == 1


def test_compute_subgroup_accuracies_venue_split_hits() -> None:
    partitions = [
        ("mile", "large", "autumn", "06", [(1, 2), (2, 1)]),   # place2 + fukusho_2p
        ("mile", "large", "autumn", "08", [(1, 3), (2, 9)]),   # place3 only
    ]
    result = subject.compute_subgroup_accuracies(partitions)
    venues = [sg for sg in result if sg.dimension == "venue"]
    assert len(venues) == 2
    assert venues[0].band == "06"
    assert venues[0].place2_hits == 1
    assert venues[0].fukusho_2p_hits == 1
    assert venues[0].top1_hits == 0
    assert venues[1].band == "08"
    assert venues[1].place3_hits == 1
    assert venues[1].top3_box_hits == 0
    assert venues[1].fukusho_2p_hits == 0


# ── compute_rs_per_class ──────────────────────────────────────────────────────


def test_compute_rs_per_class_all_nige() -> None:
    pred_labels = [0, 0, 0]
    actual_labels = [0, 0, 0]
    result = subject.compute_rs_per_class(pred_labels, actual_labels)
    nige = result[0]
    assert nige.tp == 3
    assert nige.pred_count == 3
    assert nige.actual_count == 3
    assert nige.precision == pytest.approx(1.0)
    assert nige.recall == pytest.approx(1.0)


def test_compute_rs_per_class_perfect_all_classes() -> None:
    pred_labels = [0, 1, 2, 3]
    actual_labels = [0, 1, 2, 3]
    result = subject.compute_rs_per_class(pred_labels, actual_labels)
    for cls in result:
        assert cls.tp == 1
        assert cls.precision == pytest.approx(1.0)
        assert cls.recall == pytest.approx(1.0)


def test_compute_rs_per_class_all_wrong() -> None:
    pred_labels = [0, 0, 0]
    actual_labels = [1, 2, 3]
    result = subject.compute_rs_per_class(pred_labels, actual_labels)
    nige = result[0]
    assert nige.tp == 0
    assert nige.pred_count == 3
    assert nige.precision == pytest.approx(0.0)
    # actual_count of nige = 0 → recall is None
    assert nige.recall is None


def test_compute_rs_per_class_empty() -> None:
    result = subject.compute_rs_per_class([], [])
    assert len(result) == 4
    for cls in result:
        assert cls.tp == 0
        assert cls.pred_count == 0
        assert cls.actual_count == 0
        assert cls.precision is None
        assert cls.recall is None
        assert cls.f1 is None


def test_compute_rs_per_class_partial_correct() -> None:
    pred_labels = [0, 0, 1, 2]
    actual_labels = [0, 1, 1, 3]
    result = subject.compute_rs_per_class(pred_labels, actual_labels)
    nige = result[0]
    assert nige.tp == 1
    assert nige.pred_count == 2
    assert nige.actual_count == 1
    senkou = result[1]
    assert senkou.tp == 1
    assert senkou.pred_count == 1
    assert senkou.actual_count == 2


# ── RunningStyleClassMetrics ──────────────────────────────────────────────────


def test_rs_class_metrics_f1_none_when_both_zero() -> None:
    m = subject.RunningStyleClassMetrics(label="nige", cls_idx=0,
                                         pred_count=0, actual_count=0, tp=0)
    assert m.f1 is None


def test_rs_class_metrics_precision_zero_when_no_preds() -> None:
    m = subject.RunningStyleClassMetrics(label="nige", cls_idx=0,
                                         pred_count=0, actual_count=5, tp=0)
    assert m.precision is None
    assert m.recall == pytest.approx(0.0)


def test_rs_class_metrics_f1_harmonic_mean() -> None:
    m = subject.RunningStyleClassMetrics(label="nige", cls_idx=0,
                                         pred_count=4, actual_count=4, tp=2)
    assert m.precision == pytest.approx(0.5)
    assert m.recall == pytest.approx(0.5)
    assert m.f1 == pytest.approx(0.5)


# ── compute_macro_f1 ──────────────────────────────────────────────────────────


def test_compute_macro_f1_empty() -> None:
    assert subject.compute_macro_f1([]) is None


def test_compute_macro_f1_all_none() -> None:
    per_class = [
        subject.RunningStyleClassMetrics(label="nige", cls_idx=0,
                                          pred_count=0, actual_count=0, tp=0),
    ]
    assert subject.compute_macro_f1(per_class) is None


def test_compute_macro_f1_perfect() -> None:
    per_class = [
        subject.RunningStyleClassMetrics(label=subject.RS_CLASS_LABELS[i], cls_idx=i,
                                          pred_count=4, actual_count=4, tp=4)
        for i in range(4)
    ]
    assert subject.compute_macro_f1(per_class) == pytest.approx(1.0)


def test_compute_macro_f1_mixed() -> None:
    # nige: prec=1.0 rec=1.0 f1=1.0; senkou: prec=0.5 rec=0.5 f1=0.5
    per_class = [
        subject.RunningStyleClassMetrics(label="nige", cls_idx=0,
                                          pred_count=2, actual_count=2, tp=2),
        subject.RunningStyleClassMetrics(label="senkou", cls_idx=1,
                                          pred_count=4, actual_count=4, tp=2),
        subject.RunningStyleClassMetrics(label="sashi", cls_idx=2,
                                          pred_count=0, actual_count=0, tp=0),
        subject.RunningStyleClassMetrics(label="oikomi", cls_idx=3,
                                          pred_count=0, actual_count=0, tp=0),
    ]
    # Only two have valid F1: (1.0 + 0.5) / 2 = 0.75
    assert subject.compute_macro_f1(per_class) == pytest.approx(0.75)


# ── FinishPositionMetrics properties ─────────────────────────────────────────


def test_fp_metrics_pcts_zero_races() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=0, horses=0,
        top1_hits=0, place2_hits=0, place3_hits=0,
        fukusho_2p_hits=0, top3_box_hits=0,
        prediction_generated_at_jst="",
    )
    assert m.top1_pct == 0.0
    assert m.place2_pct == 0.0
    assert m.place3_pct == 0.0
    assert m.fukusho_2p_pct == 0.0
    assert m.top3_box_pct == 0.0


def test_fp_metrics_pcts_correct() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=10, horses=100,
        top1_hits=4, place2_hits=6, place3_hits=7,
        fukusho_2p_hits=8, top3_box_hits=7,
        prediction_generated_at_jst="2026-06-14 09:30:00 JST",
    )
    assert m.top1_pct == pytest.approx(40.0)
    assert m.place2_pct == pytest.approx(60.0)
    assert m.place3_pct == pytest.approx(70.0)
    assert m.fukusho_2p_pct == pytest.approx(80.0)
    assert m.top3_box_pct == pytest.approx(70.0)


# ── RunningStyleMetrics properties ────────────────────────────────────────────


def test_rs_metrics_label_share_empty() -> None:
    m = subject.RunningStyleMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        total_horses=0, overall_accuracy=0.0,
    )
    assert m.pred_label_share == {}
    assert m.actual_label_share == {}


def test_rs_metrics_label_share_computed() -> None:
    per_class = [
        subject.RunningStyleClassMetrics(label="nige", cls_idx=0,
                                          pred_count=2, actual_count=1, tp=1),
        subject.RunningStyleClassMetrics(label="senkou", cls_idx=1,
                                          pred_count=6, actual_count=5, tp=4),
        subject.RunningStyleClassMetrics(label="sashi", cls_idx=2,
                                          pred_count=2, actual_count=2, tp=1),
        subject.RunningStyleClassMetrics(label="oikomi", cls_idx=3,
                                          pred_count=0, actual_count=2, tp=0),
    ]
    m = subject.RunningStyleMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        total_horses=10, overall_accuracy=0.6, per_class=per_class,
    )
    ps = m.pred_label_share
    assert ps["nige"] == pytest.approx(0.2)
    assert ps["senkou"] == pytest.approx(0.6)
    assert ps["oikomi"] == pytest.approx(0.0)


# ── format_fp_report ──────────────────────────────────────────────────────────


def test_format_fp_report_contains_era() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260606", category="jra", era="DEGRADED",
        races=24, horses=200,
        top1_hits=0, place2_hits=2, place3_hits=5,
        fukusho_2p_hits=5, top3_box_hits=5,
        prediction_generated_at_jst="2026-06-06 05:27:00 JST",
    )
    report = subject.format_fp_report(m)
    assert "DEGRADED" in report
    assert "20260606" in report
    assert "JRA" in report
    assert "0.00%" in report


def test_format_fp_report_postfix_era() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=24, horses=200,
        top1_hits=12, place2_hits=14, place3_hits=16,
        fukusho_2p_hits=18, top3_box_hits=16,
        prediction_generated_at_jst="2026-06-14 09:30:00 JST",
    )
    report = subject.format_fp_report(m)
    assert "POST_FIX" in report
    assert "50.00%" in report


def test_format_fp_report_shows_baselines_for_jra() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=10, horses=100,
        top1_hits=5, place2_hits=6, place3_hits=7,
        fukusho_2p_hits=8, top3_box_hits=7,
        prediction_generated_at_jst="",
    )
    report = subject.format_fp_report(m)
    assert "31.78%" in report
    assert "44.71%" in report


def test_format_fp_report_nar_no_baselines() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="nar", era="POST_FIX",
        races=10, horses=100,
        top1_hits=5, place2_hits=6, place3_hits=7,
        fukusho_2p_hits=8, top3_box_hits=7,
        prediction_generated_at_jst="",
    )
    report = subject.format_fp_report(m)
    assert "31.78%" not in report


def test_format_fp_report_shows_model_version_counts() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=10, horses=100,
        top1_hits=5, place2_hits=6, place3_hits=7,
        fukusho_2p_hits=8, top3_box_hits=7,
        prediction_generated_at_jst="",
        model_version_counts={"iter14": 80, "iter26": 20},
    )
    report = subject.format_fp_report(m)
    assert "iter14" in report
    assert "80" in report


# ── format_subgroup_report ────────────────────────────────────────────────────


def test_format_subgroup_report_groups_by_dimension() -> None:
    subgroups = [
        subject.SubgroupAccuracy(
            dimension="distance_band", band="sprint", races=4,
            top1_hits=2, place2_hits=3, place3_hits=3,
            fukusho_2p_hits=2, top3_box_hits=3,
        ),
        subject.SubgroupAccuracy(
            dimension="venue", band="05", races=4,
            top1_hits=1, place2_hits=2, place3_hits=3,
            fukusho_2p_hits=2, top3_box_hits=3,
        ),
    ]
    report = subject.format_subgroup_report(subgroups)
    assert "Subgroup breakdown:" in report
    assert "[distance_band]" in report
    assert "sprint" in report
    assert "[venue]" in report
    assert "50.00%" in report


def test_format_fp_report_includes_subgroups_when_present() -> None:
    subgroups = [
        subject.SubgroupAccuracy(
            dimension="distance_band", band="mile", races=2,
            top1_hits=1, place2_hits=1, place3_hits=2,
            fukusho_2p_hits=1, top3_box_hits=2,
        ),
    ]
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=2, horses=20,
        top1_hits=1, place2_hits=1, place3_hits=2,
        fukusho_2p_hits=1, top3_box_hits=2,
        prediction_generated_at_jst="2026-06-14 09:30:00 JST",
        subgroups=subgroups,
    )
    report = subject.format_fp_report(m)
    assert "Subgroup breakdown:" in report
    assert "[distance_band]" in report
    assert "mile" in report


def test_format_fp_report_omits_subgroups_when_empty() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=2, horses=20,
        top1_hits=1, place2_hits=1, place3_hits=2,
        fukusho_2p_hits=1, top3_box_hits=2,
        prediction_generated_at_jst="2026-06-14 09:30:00 JST",
    )
    report = subject.format_fp_report(m)
    assert "Subgroup breakdown:" not in report


# ── format_rs_report ──────────────────────────────────────────────────────────


def test_format_rs_report_contains_accuracy() -> None:
    per_class = [
        subject.RunningStyleClassMetrics(label="nige", cls_idx=0,
                                          pred_count=4, actual_count=4, tp=3),
        subject.RunningStyleClassMetrics(label="senkou", cls_idx=1,
                                          pred_count=4, actual_count=4, tp=2),
        subject.RunningStyleClassMetrics(label="sashi", cls_idx=2,
                                          pred_count=4, actual_count=4, tp=2),
        subject.RunningStyleClassMetrics(label="oikomi", cls_idx=3,
                                          pred_count=4, actual_count=4, tp=1),
    ]
    m = subject.RunningStyleMetrics(
        date_str="20260607", category="jra", era="POST_FIX",
        total_horses=16, overall_accuracy=0.5, per_class=per_class,
        macro_f1=0.6, model_version="jra-running-style-lgbm-prod-v3",
    )
    report = subject.format_rs_report(m)
    assert "50.00%" in report
    assert "jra-running-style-lgbm-prod-v3" in report
    assert "nige" in report


# ── metrics_to_dict ───────────────────────────────────────────────────────────


def test_metrics_to_dict_both_none() -> None:
    result = subject.metrics_to_dict(None, None)
    assert result == {}


def test_metrics_to_dict_rs_only() -> None:
    per_class = [
        subject.RunningStyleClassMetrics(label="nige", cls_idx=0,
                                          pred_count=4, actual_count=4, tp=3),
        subject.RunningStyleClassMetrics(label="senkou", cls_idx=1,
                                          pred_count=4, actual_count=4, tp=2),
        subject.RunningStyleClassMetrics(label="sashi", cls_idx=2,
                                          pred_count=4, actual_count=4, tp=2),
        subject.RunningStyleClassMetrics(label="oikomi", cls_idx=3,
                                          pred_count=4, actual_count=4, tp=1),
    ]
    rs = subject.RunningStyleMetrics(
        date_str="20260607", category="jra", era="POST_FIX",
        total_horses=16, overall_accuracy=0.5, per_class=per_class,
        macro_f1=0.6, model_version="jra-running-style-lgbm-prod-v3",
    )
    result = subject.metrics_to_dict(None, rs)
    assert "finish_position" not in result
    assert "running_style" in result
    serialized = json.dumps(result)
    assert '"era": "POST_FIX"' in serialized
    assert '"macro_f1_pct": 60.0' in serialized
    assert '"per_class"' in serialized
    # Verify 4 per_class entries (4 label entries in serialized JSON)
    assert serialized.count('"label"') == 4


def test_metrics_to_dict_rs_no_macro_f1() -> None:
    rs = subject.RunningStyleMetrics(
        date_str="20260607", category="jra", era="POST_FIX",
        total_horses=0, overall_accuracy=0.0, macro_f1=None,
    )
    result = subject.metrics_to_dict(None, rs)
    assert "running_style" in result
    assert '"macro_f1_pct": null' in json.dumps(result)


def test_metrics_to_dict_fp_only() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=24, horses=200,
        top1_hits=10, place2_hits=12, place3_hits=14,
        fukusho_2p_hits=16, top3_box_hits=14,
        prediction_generated_at_jst="2026-06-14 09:30:00 JST",
    )
    result = subject.metrics_to_dict(m, None)
    assert "finish_position" in result
    assert "running_style" not in result
    serialized = json.dumps(result)
    assert '"era": "POST_FIX"' in serialized
    assert f'"top1_pct": {m.top1_pct}' in serialized


def test_metrics_to_dict_fp_subgroups_serialized() -> None:
    subgroups = [
        subject.SubgroupAccuracy(
            dimension="distance_band", band="sprint", races=4,
            top1_hits=1, place2_hits=2, place3_hits=3,
            fukusho_2p_hits=2, top3_box_hits=3,
        ),
    ]
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=4, horses=40,
        top1_hits=1, place2_hits=2, place3_hits=3,
        fukusho_2p_hits=2, top3_box_hits=3,
        prediction_generated_at_jst="",
        subgroups=subgroups,
    )
    result = subject.metrics_to_dict(m, None)
    fp = result.get("finish_position")
    assert fp is not None
    sgs = fp["subgroups"]
    assert sgs == [
        {
            "dimension": "distance_band",
            "band": "sprint",
            "races": 4,
            "top1_pct": 25.0,
            "place2_pct": 50.0,
            "place3_pct": 75.0,
            "fukusho_2p_pct": 50.0,
            "top3_box_pct": 75.0,
        },
    ]
    serialized = json.dumps(result)
    assert '"dimension": "distance_band"' in serialized


def test_metrics_to_dict_fp_subgroups_empty_default() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=4, horses=40,
        top1_hits=1, place2_hits=2, place3_hits=3,
        fukusho_2p_hits=2, top3_box_hits=3,
        prediction_generated_at_jst="",
    )
    result = subject.metrics_to_dict(m, None)
    fp = result.get("finish_position")
    assert fp is not None
    assert fp["subgroups"] == []


def test_metrics_to_dict_json_serializable() -> None:
    m = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=10, horses=100,
        top1_hits=4, place2_hits=5, place3_hits=6,
        fukusho_2p_hits=7, top3_box_hits=6,
        prediction_generated_at_jst="",
        model_version_counts={"iter14": 50, "iter26": 50},
    )
    result = subject.metrics_to_dict(m, None)
    # Should not raise
    json.dumps(result)


# ── validate_date_arg ─────────────────────────────────────────────────────────


def test_validate_date_valid() -> None:
    subject.validate_date_arg("20260614")  # Should not raise


def test_validate_date_invalid_format() -> None:
    with pytest.raises(ValueError, match="YYYYMMDD"):
        subject.validate_date_arg("2026-06-14")


def test_validate_date_invalid_month() -> None:
    with pytest.raises(ValueError, match="Invalid date"):
        subject.validate_date_arg("20261399")


def test_validate_date_non_digit() -> None:
    with pytest.raises(ValueError, match="YYYYMMDD"):
        subject.validate_date_arg("2026abcd")


# ── parse_args ────────────────────────────────────────────────────────────────


def test_parse_args_minimum_required() -> None:
    args = subject.parse_args(["--date", "20260614", "--category", "jra"])
    assert args.date == "20260614"
    assert args.category == "jra"
    assert args.json_output is False
    assert args.no_rs is False


def test_parse_args_json_flag() -> None:
    args = subject.parse_args(["--date", "20260614", "--category", "jra", "--json"])
    assert args.json_output is True


def test_parse_args_no_rs_flag() -> None:
    args = subject.parse_args(["--date", "20260614", "--category", "nar", "--no-rs"])
    assert args.no_rs is True


def test_parse_args_invalid_category() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(["--date", "20260614", "--category", "ban-ei"])


def test_parse_args_missing_required() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(["--date", "20260614"])


def test_parse_args_custom_pg_url() -> None:
    args = subject.parse_args([
        "--date", "20260614", "--category", "jra",
        "--pg-url", "postgresql://foo:bar@localhost/test",
    ])
    assert args.pg_url == "postgresql://foo:bar@localhost/test"


# ── query_finish_position_metrics (mocked) ────────────────────────────────────


def _make_mock_conn(fetchall_return: Sequence[Sequence[object]]) -> MagicMock:
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = fetchall_return
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    return mock_conn


def test_query_fp_metrics_no_rows_returns_none() -> None:
    mock_conn = _make_mock_conn([])
    result = subject.query_finish_position_metrics(mock_conn, "20260614", "jra")
    assert result is None


def test_query_fp_metrics_basic_result() -> None:
    gen_at = datetime(2026, 6, 14, 0, 30, 0, tzinfo=timezone.utc)
    # (keibajo, race_bango, pred_rank, actual_rank, model_version, gen_at, kyori, tosu)
    rows = [
        ("05", "01", 1, 1, "iter14", gen_at, 1200, 16),
        ("05", "01", 2, 3, "iter14", gen_at, 1200, 16),
        ("05", "02", 1, 4, "iter14", gen_at, 2000, 16),
        ("05", "02", 2, 1, "iter14", gen_at, 2000, 16),
    ]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_finish_position_metrics(mock_conn, "20260614", "jra")
    assert result is not None
    assert result.races == 2
    assert result.top1_hits == 1   # race 01: pred1=actual1
    assert result.place2_hits == 1
    assert result.place3_hits == 1
    assert result.era == "POST_FIX"


def test_query_fp_metrics_degraded_era() -> None:
    gen_at = datetime(2026, 6, 5, 20, 27, 0, tzinfo=timezone.utc)
    rows = [
        ("05", "01", 1, 5, "iter14", gen_at, 1600, 12),
    ]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_finish_position_metrics(mock_conn, "20260606", "jra")
    assert result is not None
    assert result.era == "DEGRADED"
    assert result.top1_hits == 0
    assert result.races == 1


def test_query_fp_metrics_nar_category() -> None:
    gen_at = datetime(2026, 6, 14, 0, 30, 0, tzinfo=timezone.utc)
    rows = [("30", "01", 1, 1, "iter12", gen_at, 1400, 10)]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_finish_position_metrics(mock_conn, "20260614", "nar")
    assert result is not None
    assert result.category == "nar"
    assert result.top1_hits == 1


def test_query_fp_metrics_sql_uses_per_horse_distinct_on() -> None:
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    subject.query_finish_position_metrics(mock_conn, "20260614", "jra")
    sql_call = mock_cur.execute.call_args[0][0]
    assert "DISTINCT ON (keibajo_code, race_bango, ketto_toroku_bango)" in sql_call
    assert "ORDER BY keibajo_code, race_bango, ketto_toroku_bango, prediction_generated_at DESC" in sql_call


def test_query_fp_metrics_populates_subgroups() -> None:
    gen_at = datetime(2026, 6, 14, 0, 30, 0, tzinfo=timezone.utc)
    # race 01: sprint (1200), small (8), venue 05, pred1 wins
    # race 02: long (2400), large (16), venue 05, pred1 finishes 4th (miss)
    rows = [
        ("05", "01", 1, 1, "iter14", gen_at, 1200, 8),
        ("05", "02", 1, 4, "iter14", gen_at, 2400, 16),
    ]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_finish_position_metrics(mock_conn, "20260614", "jra")
    assert result is not None
    distance = [sg for sg in result.subgroups if sg.dimension == "distance_band"]
    assert len(distance) == 2
    assert distance[0].band == "long"
    assert distance[0].top1_hits == 0
    assert distance[1].band == "sprint"
    assert distance[1].top1_hits == 1
    season = [sg for sg in result.subgroups if sg.dimension == "season_band"]
    assert len(season) == 1
    assert season[0].band == "summer"
    assert season[0].races == 2
    venue = [sg for sg in result.subgroups if sg.dimension == "venue"]
    assert len(venue) == 1
    assert venue[0].band == "05"
    assert venue[0].races == 2
    assert venue[0].top1_hits == 1


def test_query_fp_metrics_jst_display_converts_utc_to_jst() -> None:
    # UTC 00:30 = JST 09:30; the displayed string must show 09:30 JST
    gen_at = datetime(2026, 6, 14, 0, 30, 0, tzinfo=timezone.utc)
    rows = [("05", "01", 1, 1, "iter14", gen_at, 1600, 16)]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_finish_position_metrics(mock_conn, "20260614", "jra")
    assert result is not None
    assert result.prediction_generated_at_jst == "2026-06-14 09:30:00 JST"


# ── query_running_style_metrics (mocked) ──────────────────────────────────────


def test_query_rs_metrics_no_rows_returns_none() -> None:
    mock_conn = _make_mock_conn([])
    result = subject.query_running_style_metrics(mock_conn, "20260614", "jra")
    assert result is None


def test_query_rs_metrics_straight_track_excluded() -> None:
    """Horses on straight tracks (corner_1='00') should be excluded."""
    gen_at = datetime(2026, 6, 14, 0, 30, 0, tzinfo=timezone.utc)
    # All have corner_1='00' = straight track → all filtered out
    rows = [
        ("05", "01", "horse1", "nige", 0, 0.9, 0.05, 0.03, 0.02,
         "jra-running-style-lgbm-prod-v3", gen_at, "00", 16),
        ("05", "01", "horse2", "senkou", 1, 0.1, 0.7, 0.15, 0.05,
         "jra-running-style-lgbm-prod-v3", gen_at, "00", 16),
    ]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_running_style_metrics(mock_conn, "20260614", "jra")
    assert result is None


def test_query_rs_metrics_with_corner_data() -> None:
    """Verify per-class metrics computed from corner data."""
    gen_at = datetime(2026, 6, 14, 0, 30, 0, tzinfo=timezone.utc)
    # horse1: predicted nige (class 0), corner_1='01', shusso_tosu=16 → norm=0 → actual nige ✓
    # horse2: predicted senkou (class 1), corner_1='08', shusso_tosu=16 → norm≈0.467 → actual sashi ✗
    rows = [
        ("05", "02", "horse1", "nige", 0, 0.9, 0.05, 0.03, 0.02,
         "jra-running-style-lgbm-prod-v3", gen_at, "01", 16),
        ("05", "02", "horse2", "senkou", 1, 0.1, 0.7, 0.15, 0.05,
         "jra-running-style-lgbm-prod-v3", gen_at, "08", 16),
    ]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_running_style_metrics(mock_conn, "20260614", "jra")
    assert result is not None
    assert result.total_horses == 2
    # 1/2 correct (horse1 correct, horse2 wrong)
    assert result.overall_accuracy == pytest.approx(0.5)
    assert result.era == "POST_FIX"


def test_query_rs_metrics_nar_category() -> None:
    gen_at = datetime(2026, 6, 14, 0, 30, 0, tzinfo=timezone.utc)
    rows = [
        ("30", "01", "horseA", "senkou", 1, 0.05, 0.8, 0.1, 0.05,
         "nar-running-style-lgbm-prod-v3", gen_at, "02", 10),
    ]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_running_style_metrics(mock_conn, "20260614", "nar")
    assert result is not None
    assert result.category == "nar"


def test_query_rs_metrics_jst_display_converts_utc_to_jst() -> None:
    gen_at = datetime(2026, 6, 14, 0, 30, 0, tzinfo=timezone.utc)
    rows = [
        ("05", "01", "horse1", "nige", 0, 0.9, 0.05, 0.03, 0.02,
         "jra-running-style-lgbm-prod-v3", gen_at, "01", 16),
    ]
    mock_conn = _make_mock_conn(rows)
    result = subject.query_running_style_metrics(mock_conn, "20260614", "jra")
    assert result is not None
    assert result.prediction_generated_at_jst == "2026-06-14 09:30:00 JST"


# ── run() integration (mocked) ────────────────────────────────────────────────


def test_run_returns_1_when_no_data(capsys: CaptureFixture[str]) -> None:
    with (
        patch("serve_accuracy_report.query_finish_position_metrics", return_value=None),
        patch("serve_accuracy_report.query_running_style_metrics", return_value=None),
        patch("serve_accuracy_report.psycopg") as mock_psycopg,
    ):
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value = mock_conn
        code = subject.run("20260614", "jra", "postgresql://test")
    assert code == 1
    captured = capsys.readouterr()
    assert "No served predictions" in captured.out


def test_run_returns_0_with_data(capsys: CaptureFixture[str]) -> None:
    fp = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=24, horses=200,
        top1_hits=10, place2_hits=13, place3_hits=15,
        fukusho_2p_hits=17, top3_box_hits=15,
        prediction_generated_at_jst="2026-06-14 09:30:00 JST",
    )
    with (
        patch("serve_accuracy_report.query_finish_position_metrics", return_value=fp),
        patch("serve_accuracy_report.query_running_style_metrics", return_value=None),
        patch("serve_accuracy_report.psycopg") as mock_psycopg,
    ):
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value = mock_conn
        code = subject.run("20260614", "jra", "postgresql://test", no_rs=True)
    assert code == 0
    captured = capsys.readouterr()
    assert "POST_FIX" in captured.out


def test_run_json_output(capsys: CaptureFixture[str]) -> None:
    fp = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=24, horses=200,
        top1_hits=10, place2_hits=13, place3_hits=15,
        fukusho_2p_hits=17, top3_box_hits=15,
        prediction_generated_at_jst="",
    )
    with (
        patch("serve_accuracy_report.query_finish_position_metrics", return_value=fp),
        patch("serve_accuracy_report.query_running_style_metrics", return_value=None),
        patch("serve_accuracy_report.psycopg") as mock_psycopg,
    ):
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value = mock_conn
        code = subject.run("20260614", "jra", "postgresql://test",
                           json_output=True, no_rs=True)
    assert code == 0
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["finish_position"]["era"] == "POST_FIX"


def test_run_with_rs_text_output(capsys: CaptureFixture[str]) -> None:
    fp = subject.FinishPositionMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        races=24, horses=200,
        top1_hits=10, place2_hits=13, place3_hits=15,
        fukusho_2p_hits=17, top3_box_hits=15,
        prediction_generated_at_jst="2026-06-14 09:30:00 JST",
    )
    per_class = [
        subject.RunningStyleClassMetrics(label="nige", cls_idx=0,
                                          pred_count=4, actual_count=4, tp=3),
        subject.RunningStyleClassMetrics(label="senkou", cls_idx=1,
                                          pred_count=4, actual_count=4, tp=2),
        subject.RunningStyleClassMetrics(label="sashi", cls_idx=2,
                                          pred_count=4, actual_count=4, tp=2),
        subject.RunningStyleClassMetrics(label="oikomi", cls_idx=3,
                                          pred_count=4, actual_count=4, tp=1),
    ]
    rs = subject.RunningStyleMetrics(
        date_str="20260614", category="jra", era="POST_FIX",
        total_horses=16, overall_accuracy=0.5, per_class=per_class,
        macro_f1=0.6, model_version="jra-running-style-lgbm-prod-v3",
    )
    with (
        patch("serve_accuracy_report.query_finish_position_metrics", return_value=fp),
        patch("serve_accuracy_report.query_running_style_metrics", return_value=rs),
        patch("serve_accuracy_report.psycopg") as mock_psycopg,
    ):
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value = mock_conn
        code = subject.run("20260614", "jra", "postgresql://test")
    assert code == 0
    captured = capsys.readouterr()
    assert "Running-Style" in captured.out
    assert "Finish-Position" in captured.out


def test_run_invalid_date_raises() -> None:
    with pytest.raises(ValueError):
        subject.run("2026-06-14", "jra", "postgresql://test")


def test_run_no_data_json_output(capsys: CaptureFixture[str]) -> None:
    with (
        patch("serve_accuracy_report.query_finish_position_metrics", return_value=None),
        patch("serve_accuracy_report.query_running_style_metrics", return_value=None),
        patch("serve_accuracy_report.psycopg") as mock_psycopg,
    ):
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value = mock_conn
        code = subject.run("20260614", "jra", "postgresql://test", json_output=True)
    assert code == 1
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["error"] == "no_data"
