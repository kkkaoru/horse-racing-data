from __future__ import annotations

import decimal
import io
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import evaluate_running_style_pipeline as subject


def _empty_confusion() -> dict[tuple[str, str], int]:
    return {(a, p): 0 for a in subject.CLASS_LABELS for p in subject.CLASS_LABELS}


def _empty_log_loss() -> tuple[dict[str, float], dict[str, int]]:
    return ({label: 0.0 for label in subject.CLASS_LABELS}, {label: 0 for label in subject.CLASS_LABELS})


def _make_dims(
    *,
    category: str = "jra",
    window_from: str = "20200101",
    keibajo_code: str = "05",
    kyori: int = 1600,
    kyoso_shubetsu_code: str = "11",
) -> subject.BucketDimensions:
    return subject.BucketDimensions(
        category=category,
        window_from=window_from,
        keibajo_code=keibajo_code,
        kyori=kyori,
        kyoso_shubetsu_code=kyoso_shubetsu_code,
        kyoso_joken_code=None,
        track_code=None,
        grade_code=None,
        race_name=None,
    )


def _make_record(
    *,
    category: str = "jra",
    window_from: str = "20200101",
    keibajo_code: str = "05",
    kyori: int = 1600,
    confusion: dict[tuple[str, str], int] | None = None,
    race_count: int = 1,
    prediction_count: int = 0,
    log_loss_sum: dict[str, float] | None = None,
    log_loss_count: dict[str, int] | None = None,
    top2_hit_count: int = 0,
) -> subject.BucketRecord:
    actual_confusion = confusion if confusion is not None else _empty_confusion()
    if log_loss_sum is None or log_loss_count is None:
        default_sum, default_count = _empty_log_loss()
        actual_sum = log_loss_sum if log_loss_sum is not None else default_sum
        actual_count = log_loss_count if log_loss_count is not None else default_count
    else:
        actual_sum = log_loss_sum
        actual_count = log_loss_count
    dims = _make_dims(
        category=category,
        window_from=window_from,
        keibajo_code=keibajo_code,
        kyori=kyori,
    )
    metrics = subject.BucketMetrics(
        race_count=race_count,
        prediction_count=prediction_count,
        confusion=actual_confusion,
        log_loss_sum=actual_sum,
        log_loss_count=actual_count,
        top2_hit_count=top2_hit_count,
    )
    return subject.BucketRecord(dims=dims, metrics=metrics)


def _balanced_confusion(diagonal: int) -> dict[tuple[str, str], int]:
    confusion = _empty_confusion()
    for label in subject.CLASS_LABELS:
        confusion[(label, label)] = diagonal
    return confusion


def test_parse_args_defaults_period_to_all():
    namespace = subject.parse_args(
        [
            "--pg-url",
            "postgres://test",
            "--running-style-feature-version",
            "v1",
            "--model-version-jra",
            "jra-x",
            "--model-version-nar",
            "nar-y",
            "--output",
            "/tmp/r.md",
        ]
    )
    assert namespace.period == "all"
    assert namespace.pg_url == "postgres://test"
    assert namespace.running_style_feature_version == "v1"
    assert namespace.model_version_jra == "jra-x"
    assert namespace.model_version_nar == "nar-y"
    assert namespace.output == Path("/tmp/r.md")


def test_parse_args_accepts_oos_only_period():
    namespace = subject.parse_args(
        [
            "--pg-url",
            "postgres://test",
            "--running-style-feature-version",
            "v1",
            "--model-version-jra",
            "jra-x",
            "--model-version-nar",
            "nar-y",
            "--output",
            "/tmp/r.md",
            "--period",
            "oos-only",
        ]
    )
    assert namespace.period == "oos-only"


def test_parse_args_rejects_unknown_period():
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--pg-url",
                "postgres://test",
                "--running-style-feature-version",
                "v1",
                "--model-version-jra",
                "jra-x",
                "--model-version-nar",
                "nar-y",
                "--output",
                "/tmp/r.md",
                "--period",
                "bogus",
            ]
        )


def test_parse_args_requires_output():
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--pg-url",
                "postgres://test",
                "--running-style-feature-version",
                "v1",
                "--model-version-jra",
                "jra-x",
                "--model-version-nar",
                "nar-y",
            ]
        )


def test_to_int_handles_decimal():
    assert subject.to_int(decimal.Decimal("3.7")) == 3


def test_to_int_accepts_bool():
    assert subject.to_int(True) == 1


def test_to_int_handles_empty_string():
    assert subject.to_int("") == 0


def test_to_int_handles_none():
    assert subject.to_int(None) == 0


def test_to_int_handles_numeric_string():
    assert subject.to_int("42") == 42


def test_to_float_handles_decimal():
    assert subject.to_float(decimal.Decimal("1.5")) == 1.5


def test_to_float_handles_bool():
    assert subject.to_float(False) == 0.0


def test_to_float_handles_int():
    assert subject.to_float(7) == 7.0


def test_to_float_handles_empty_string():
    assert subject.to_float("") == 0.0


def test_to_float_handles_none():
    assert subject.to_float(None) == 0.0


def test_to_float_handles_numeric_string():
    assert subject.to_float("2.5") == 2.5


def test_to_optional_str_handles_none():
    assert subject.to_optional_str(None) is None


def test_to_optional_str_preserves_string():
    assert subject.to_optional_str("abc") == "abc"


def test_to_optional_str_coerces_int():
    assert subject.to_optional_str(123) == "123"


def test_window_from_to_year_extracts_first_four_chars():
    assert subject.window_from_to_year("20200101") == 2020


def test_is_train_year_returns_true_for_2020():
    assert subject.is_train_year(2020) is True


def test_is_train_year_returns_false_for_2015():
    assert subject.is_train_year(2015) is False


def test_is_train_year_returns_false_for_2026():
    assert subject.is_train_year(2026) is False


def test_filter_by_period_all_returns_all_records():
    record_train = _make_record(window_from="20200101", prediction_count=10)
    record_oos = _make_record(window_from="20060101", prediction_count=10)
    filtered = subject.filter_by_period([record_train, record_oos], "all")
    assert len(filtered) == 2


def test_filter_by_period_oos_only_excludes_train_years():
    record_train = _make_record(window_from="20200101", prediction_count=10)
    record_oos = _make_record(window_from="20060101", prediction_count=10)
    filtered = subject.filter_by_period([record_train, record_oos], "oos-only")
    assert len(filtered) == 1
    assert filtered[0].dims.window_from == "20060101"


def test_safe_divide_returns_zero_on_zero_denom():
    assert subject.safe_divide(10.0, 0.0) == 0.0


def test_safe_divide_returns_quotient():
    assert subject.safe_divide(6.0, 3.0) == 2.0


def test_compute_per_class_metrics_returns_precision_and_recall():
    confusion = _empty_confusion()
    confusion[("nige", "nige")] = 8
    confusion[("nige", "senkou")] = 2
    confusion[("senkou", "nige")] = 1
    confusion[("senkou", "senkou")] = 5
    result = subject.compute_per_class_metrics(confusion)
    nige_precision = 8 / (8 + 1)
    assert result["nige"].precision == pytest.approx(nige_precision)
    assert result["nige"].recall == 0.8
    assert result["nige"].actual_count == 10
    assert result["nige"].predicted_count == 9


def test_compute_per_class_metrics_handles_zero_predictions():
    confusion = _empty_confusion()
    confusion[("nige", "senkou")] = 5
    result = subject.compute_per_class_metrics(confusion)
    assert result["nige"].precision == 0.0
    assert result["nige"].recall == 0.0
    assert result["nige"].f1 == 0.0


def test_aggregate_metrics_returns_zero_metrics_when_empty():
    aggregate = subject.aggregate_metrics([])
    assert aggregate.prediction_count == 0
    assert aggregate.accuracy == 0.0
    assert aggregate.top2_accuracy == 0.0
    assert aggregate.log_loss == 0.0


def test_aggregate_metrics_sums_per_record_counts():
    confusion_a = _empty_confusion()
    confusion_a[("nige", "nige")] = 5
    confusion_b = _empty_confusion()
    confusion_b[("nige", "nige")] = 3
    record_a = _make_record(
        confusion=confusion_a,
        prediction_count=10,
        race_count=1,
        top2_hit_count=7,
        log_loss_sum={"nige": 2.0, "senkou": 0.0, "sashi": 0.0, "oikomi": 0.0},
        log_loss_count={"nige": 5, "senkou": 0, "sashi": 0, "oikomi": 0},
    )
    record_b = _make_record(
        confusion=confusion_b,
        prediction_count=5,
        race_count=1,
        top2_hit_count=4,
        log_loss_sum={"nige": 1.0, "senkou": 0.0, "sashi": 0.0, "oikomi": 0.0},
        log_loss_count={"nige": 3, "senkou": 0, "sashi": 0, "oikomi": 0},
    )
    aggregate = subject.aggregate_metrics([record_a, record_b])
    assert aggregate.prediction_count == 15
    assert aggregate.race_count == 2
    assert aggregate.top2_accuracy == pytest.approx(11.0 / 15.0)
    assert aggregate.log_loss == pytest.approx(3.0 / 8.0)
    assert aggregate.accuracy == 1.0


def test_group_records_by_year_buckets_records_by_window():
    a = _make_record(window_from="20200101")
    b = _make_record(window_from="20200101")
    c = _make_record(window_from="20210101")
    grouped = subject.group_records_by_year([a, b, c])
    assert sorted(grouped.keys()) == [2020, 2021]
    assert len(grouped[2020]) == 2
    assert len(grouped[2021]) == 1


def test_compute_per_year_metrics_returns_sorted_year_metrics():
    record_a = _make_record(window_from="20210101", confusion=_balanced_confusion(2), prediction_count=8)
    record_b = _make_record(window_from="20200101", confusion=_balanced_confusion(1), prediction_count=4)
    per_year = subject.compute_per_year_metrics([record_a, record_b])
    assert per_year[0].year == 2020
    assert per_year[1].year == 2021


def test_detect_year_changes_flags_swings_above_threshold():
    metrics_a = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.4,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    metrics_b = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.7,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    changes = subject.detect_year_changes(
        [
            subject.YearMetrics(year=2019, metrics=metrics_a),
            subject.YearMetrics(year=2020, metrics=metrics_b),
        ]
    )
    assert len(changes) == 1
    assert changes[0].year_from == 2019
    assert changes[0].year_to == 2020
    assert changes[0].delta_pp == pytest.approx(30.0)


def test_detect_year_changes_returns_empty_when_no_swing():
    metrics_a = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.45,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    metrics_b = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.5,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    changes = subject.detect_year_changes(
        [
            subject.YearMetrics(year=2019, metrics=metrics_a),
            subject.YearMetrics(year=2020, metrics=metrics_b),
        ]
    )
    assert changes == []


def test_detect_drift_warnings_flags_precision_far_above_recall():
    per_class_a: dict[str, subject.PerClassMetrics] = {
        "nige": subject.PerClassMetrics(0.95, 0.1, 0.0, 0, 0),
        "senkou": subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0),
        "sashi": subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0),
        "oikomi": subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0),
    }
    metrics_a = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.4,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class=per_class_a,
    )
    warnings = subject.detect_drift_warnings([subject.YearMetrics(year=2020, metrics=metrics_a)])
    assert len(warnings) == 1


def test_detect_drift_warnings_returns_empty_when_no_drift():
    per_class_a: dict[str, subject.PerClassMetrics] = {
        label: subject.PerClassMetrics(0.5, 0.5, 0.5, 0, 0) for label in subject.CLASS_LABELS
    }
    metrics_a = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.5,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class=per_class_a,
    )
    warnings = subject.detect_drift_warnings([subject.YearMetrics(year=2020, metrics=metrics_a)])
    assert warnings == []


def test_detect_train_leakage_returns_true_for_huge_gap():
    train = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.77,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    oos = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.44,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    assert subject.detect_train_leakage(train, oos) is True


def test_detect_train_leakage_returns_false_when_oos_empty():
    train = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.77,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    oos = subject.AggregateMetrics(
        prediction_count=0,
        race_count=0,
        accuracy=0.0,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    assert subject.detect_train_leakage(train, oos) is False


def test_detect_train_leakage_returns_false_when_train_empty():
    train = subject.AggregateMetrics(
        prediction_count=0,
        race_count=0,
        accuracy=0.0,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    oos = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.5,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    assert subject.detect_train_leakage(train, oos) is False


def test_detect_train_leakage_returns_false_for_small_gap():
    train = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.55,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    oos = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.5,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    assert subject.detect_train_leakage(train, oos) is False


def test_detect_nige_leakage_returns_true_when_nige_precision_high_and_recall_low():
    per_class: dict[str, subject.PerClassMetrics] = {
        "nige": subject.PerClassMetrics(0.95, 0.5, 0.0, 0, 0),
        "senkou": subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0),
        "sashi": subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0),
        "oikomi": subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0),
    }
    train = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.5,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class=per_class,
    )
    assert subject.detect_nige_leakage(train) is True


def test_detect_nige_leakage_returns_false_when_precision_below_threshold():
    per_class: dict[str, subject.PerClassMetrics] = {
        "nige": subject.PerClassMetrics(0.7, 0.2, 0.0, 0, 0),
        "senkou": subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0),
        "sashi": subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0),
        "oikomi": subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0),
    }
    train = subject.AggregateMetrics(
        prediction_count=10,
        race_count=1,
        accuracy=0.5,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class=per_class,
    )
    assert subject.detect_nige_leakage(train) is False


def test_detect_nige_leakage_returns_false_when_train_empty():
    train = subject.AggregateMetrics(
        prediction_count=0,
        race_count=0,
        accuracy=0.0,
        top2_accuracy=0.0,
        log_loss=0.0,
        per_class={label: subject.PerClassMetrics(0.0, 0.0, 0.0, 0, 0) for label in subject.CLASS_LABELS},
    )
    assert subject.detect_nige_leakage(train) is False


def test_collect_weak_buckets_returns_low_precision_classes():
    confusion = _empty_confusion()
    confusion[("nige", "nige")] = 5
    confusion[("senkou", "nige")] = 95
    record = _make_record(confusion=confusion, prediction_count=200, race_count=10)
    buckets = subject.collect_weak_buckets([record])
    assert len(buckets) >= 1
    nige_buckets = [bucket for bucket in buckets if bucket.class_label == "nige"]
    assert nige_buckets
    assert nige_buckets[0].precision < 0.5


def test_collect_weak_buckets_skips_records_below_min_samples():
    confusion = _empty_confusion()
    confusion[("nige", "nige")] = 1
    confusion[("senkou", "nige")] = 99
    record = _make_record(confusion=confusion, prediction_count=50, race_count=5)
    buckets = subject.collect_weak_buckets([record])
    assert buckets == []


def test_collect_weak_buckets_returns_sorted_by_precision_asc():
    confusion_low = _empty_confusion()
    confusion_low[("nige", "nige")] = 5
    confusion_low[("senkou", "nige")] = 95
    record_low = _make_record(
        keibajo_code="01",
        confusion=confusion_low,
        prediction_count=200,
        race_count=10,
    )
    confusion_mid = _empty_confusion()
    confusion_mid[("nige", "nige")] = 40
    confusion_mid[("senkou", "nige")] = 60
    record_mid = _make_record(
        keibajo_code="02",
        confusion=confusion_mid,
        prediction_count=200,
        race_count=10,
    )
    buckets = subject.collect_weak_buckets([record_mid, record_low])
    nige_only = [bucket for bucket in buckets if bucket.class_label == "nige"]
    assert nige_only[0].keibajo_code == "01"


def test_build_category_report_includes_train_and_oos_metrics():
    train_record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(10),
        prediction_count=40,
        race_count=5,
        top2_hit_count=35,
    )
    oos_record = _make_record(
        window_from="20060101",
        confusion=_balanced_confusion(2),
        prediction_count=20,
        race_count=2,
        top2_hit_count=10,
    )
    report = subject.build_category_report("jra", "jra-v2", [train_record, oos_record])
    assert report.category == "jra"
    assert report.model_version == "jra-v2"
    assert report.train_metrics.prediction_count == 40
    assert report.oos_metrics.prediction_count == 20
    assert report.all_metrics.prediction_count == 60


def test_filter_records_for_category_keeps_matching_records():
    jra_record = _make_record(category="jra")
    nar_record = _make_record(category="nar")
    filtered = subject.filter_records_for_category([jra_record, nar_record], "jra")
    assert len(filtered) == 1
    assert filtered[0].dims.category == "jra"


def test_build_confusion_from_row_extracts_16_counts():
    row = [
        "jra",
        "20200101",
        "05",
        1600,
        "11",
        None,
        None,
        None,
        None,
        1,
        16,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        0.0,
        0,
        0.0,
        0,
        0.0,
        0,
        0.0,
        0,
        14,
    ]
    confusion = subject.build_confusion_from_row(row)
    assert confusion[("nige", "nige")] == 1
    assert confusion[("nige", "senkou")] == 2
    assert confusion[("oikomi", "oikomi")] == 16


def test_build_log_loss_from_row_extracts_sums_and_counts():
    row = [
        "jra",
        "20200101",
        "05",
        1600,
        "11",
        None,
        None,
        None,
        None,
        1,
        16,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1.5,
        2,
        2.5,
        3,
        3.5,
        4,
        4.5,
        5,
        14,
    ]
    sums, counts = subject.build_log_loss_from_row(row)
    assert sums["nige"] == 1.5
    assert counts["nige"] == 2
    assert sums["oikomi"] == 4.5
    assert counts["oikomi"] == 5


def test_parse_db_row_returns_bucket_record_with_dims_and_metrics():
    row = [
        "jra",
        "20200101",
        "05",
        1600,
        "11",
        "703",
        "10",
        "G",
        "Race",
        1,
        16,
        1,
        0,
        0,
        0,
        0,
        2,
        0,
        0,
        0,
        0,
        3,
        0,
        0,
        0,
        0,
        4,
        decimal.Decimal("1.5"),
        2,
        0.0,
        0,
        0.0,
        0,
        0.0,
        0,
        14,
    ]
    record = subject.parse_db_row(row)
    assert record.dims.category == "jra"
    assert record.dims.kyori == 1600
    assert record.dims.kyoso_joken_code == "703"
    assert record.metrics.prediction_count == 16
    assert record.metrics.top2_hit_count == 14
    assert record.metrics.log_loss_sum["nige"] == 1.5


def test_format_percent_renders_two_decimals():
    assert subject.format_percent(0.5) == "50.00%"


def test_format_float_renders_four_decimals():
    assert subject.format_float(1.23456) == "1.2346"


def test_format_int_renders_thousand_separator():
    assert subject.format_int(1_234_567) == "1,234,567"


def test_render_section_overall_lists_all_three_period_tables():
    train_record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(5),
        prediction_count=20,
    )
    oos_record = _make_record(
        window_from="20060101",
        confusion=_balanced_confusion(1),
        prediction_count=4,
    )
    report = subject.build_category_report("jra", "jra-v2", [train_record, oos_record])
    lines = subject.render_section_overall(report)
    rendered = "\n".join(lines)
    assert "Section 1: JRA overall metrics" in rendered
    assert "#### All" in rendered
    assert "#### Train (2016-2025)" in rendered
    assert "#### OOS (pre-2016 + 2026+)" in rendered


def test_render_section_train_oos_gap_lists_year_table_and_gap():
    train_record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(10),
        prediction_count=40,
    )
    oos_record = _make_record(
        window_from="20060101",
        confusion=_balanced_confusion(1),
        prediction_count=4,
    )
    report = subject.build_category_report("jra", "jra-v2", [train_record, oos_record])
    lines = subject.render_section_train_oos_gap(report)
    rendered = "\n".join(lines)
    assert "Section 2:" in rendered
    assert "Train - OOS gap" in rendered
    assert "2020" in rendered


def test_render_section_train_oos_gap_lists_drift_years_when_swing_exceeds_threshold():
    confusion_low = _empty_confusion()
    confusion_low[("nige", "senkou")] = 100
    record_low = _make_record(
        window_from="20180101",
        confusion=confusion_low,
        prediction_count=100,
    )
    record_high = _make_record(
        window_from="20190101",
        confusion=_balanced_confusion(25),
        prediction_count=100,
    )
    report = subject.build_category_report("jra", "jra-v2", [record_low, record_high])
    lines = subject.render_section_train_oos_gap(report)
    rendered = "\n".join(lines)
    assert "Year-over-year accuracy shifts > 20pp detected" in rendered


def test_render_section_per_class_drift_lists_warnings_when_present():
    confusion = _empty_confusion()
    confusion[("nige", "nige")] = 95
    confusion[("senkou", "nige")] = 5
    confusion[("nige", "senkou")] = 100
    record = _make_record(
        window_from="20200101",
        confusion=confusion,
        prediction_count=200,
    )
    report = subject.build_category_report("jra", "jra-v2", [record])
    lines = subject.render_section_per_class_drift(report)
    rendered = "\n".join(lines)
    assert "Section 3:" in rendered
    assert "Warnings" in rendered or "inflate" in rendered


def test_render_section_weak_buckets_returns_table_when_buckets_present():
    confusion = _empty_confusion()
    confusion[("nige", "nige")] = 5
    confusion[("nige", "senkou")] = 195
    record = _make_record(
        window_from="20200101",
        confusion=confusion,
        prediction_count=200,
    )
    report = subject.build_category_report("jra", "jra-v2", [record])
    lines = subject.render_section_weak_buckets(report)
    rendered = "\n".join(lines)
    assert "Section 4:" in rendered
    assert "Precision" in rendered


def test_render_section_weak_buckets_states_no_buckets_when_empty():
    record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(50),
        prediction_count=200,
    )
    report = subject.build_category_report("jra", "jra-v2", [record])
    lines = subject.render_section_weak_buckets(report)
    rendered = "\n".join(lines)
    assert "No weak bucket exceeded" in rendered


def test_render_section_recommendations_lists_p4b_when_train_leakage_detected():
    train_record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(40),
        prediction_count=160,
    )
    confusion_oos = _empty_confusion()
    confusion_oos[("nige", "senkou")] = 100
    oos_record = _make_record(
        window_from="20060101",
        confusion=confusion_oos,
        prediction_count=100,
    )
    report = subject.build_category_report("jra", "jra-v2", [train_record, oos_record])
    lines = subject.render_section_recommendations([report])
    rendered = "\n".join(lines)
    assert "P4b" in rendered


def test_render_section_recommendations_lists_p4c_when_nige_leakage_detected():
    confusion = _empty_confusion()
    confusion[("nige", "nige")] = 90
    confusion[("nige", "senkou")] = 110
    record = _make_record(
        window_from="20200101",
        confusion=confusion,
        prediction_count=200,
    )
    report = subject.build_category_report("jra", "jra-v2", [record])
    lines = subject.render_section_recommendations([report])
    rendered = "\n".join(lines)
    assert "P4c" in rendered


def test_render_section_recommendations_returns_no_anti_pattern_line_when_clean():
    record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(50),
        prediction_count=200,
    )
    report = subject.build_category_report("jra", "jra-v2", [record])
    lines = subject.render_section_recommendations([report])
    rendered = "\n".join(lines)
    assert "No critical anti-pattern" in rendered


def test_detect_critical_returns_true_when_any_category_leaks():
    train_record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(40),
        prediction_count=160,
    )
    confusion_oos = _empty_confusion()
    confusion_oos[("nige", "senkou")] = 100
    oos_record = _make_record(
        window_from="20060101",
        confusion=confusion_oos,
        prediction_count=100,
    )
    leaky_report = subject.build_category_report("jra", "jra-v2", [train_record, oos_record])
    clean_report = subject.build_category_report(
        "nar",
        "nar-v1",
        [
            _make_record(
                window_from="20200101",
                confusion=_balanced_confusion(50),
                prediction_count=200,
            )
        ],
    )
    assert subject.detect_critical([leaky_report, clean_report]) is True


def test_detect_critical_returns_false_when_no_category_leaks():
    record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(50),
        prediction_count=200,
    )
    report = subject.build_category_report("jra", "jra-v2", [record])
    assert subject.detect_critical([report]) is False


def test_build_report_context_filters_by_period_and_marks_critical():
    train_record = _make_record(
        category="jra",
        window_from="20200101",
        confusion=_balanced_confusion(40),
        prediction_count=160,
    )
    confusion_oos = _empty_confusion()
    confusion_oos[("nige", "senkou")] = 100
    oos_record = _make_record(
        category="jra",
        window_from="20060101",
        confusion=confusion_oos,
        prediction_count=100,
    )
    nar_record = _make_record(
        category="nar",
        window_from="20200101",
        confusion=_balanced_confusion(50),
        prediction_count=200,
    )
    ctx = subject.build_report_context(
        feature_version="v1",
        period="all",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        records=[train_record, oos_record, nar_record],
    )
    assert ctx.feature_version == "v1"
    assert ctx.period == "all"
    assert len(ctx.categories) == 2
    assert ctx.critical is True


def test_build_report_context_excludes_train_records_when_period_is_oos_only():
    train_record = _make_record(
        category="jra",
        window_from="20200101",
        confusion=_balanced_confusion(40),
        prediction_count=160,
    )
    oos_record = _make_record(
        category="jra",
        window_from="20060101",
        confusion=_balanced_confusion(2),
        prediction_count=8,
    )
    ctx = subject.build_report_context(
        feature_version="v1",
        period="oos-only",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        records=[train_record, oos_record],
    )
    assert len(ctx.categories) == 1
    assert ctx.categories[0].all_metrics.prediction_count == 8


def test_build_report_context_drops_nar_category_when_no_nar_records():
    jra_record = _make_record(
        category="jra",
        window_from="20200101",
        confusion=_balanced_confusion(50),
        prediction_count=200,
    )
    ctx = subject.build_report_context(
        feature_version="v1",
        period="all",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        records=[jra_record],
    )
    assert len(ctx.categories) == 1
    assert ctx.categories[0].category == "jra"


def test_build_report_context_drops_jra_category_when_no_jra_records():
    nar_record = _make_record(
        category="nar",
        window_from="20200101",
        confusion=_balanced_confusion(50),
        prediction_count=200,
    )
    ctx = subject.build_report_context(
        feature_version="v1",
        period="all",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        records=[nar_record],
    )
    assert len(ctx.categories) == 1
    assert ctx.categories[0].category == "nar"


def test_render_report_includes_all_sections_and_header():
    record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(50),
        prediction_count=200,
    )
    ctx = subject.build_report_context(
        feature_version="v1",
        period="all",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        records=[record],
    )
    rendered = subject.render_report(ctx)
    assert "Running-style model evaluation report" in rendered
    assert "Section 1:" in rendered
    assert "Section 2:" in rendered
    assert "Section 3:" in rendered
    assert "Section 4:" in rendered
    assert "Section 5:" in rendered


def test_render_report_includes_critical_footer_when_critical():
    train_record = _make_record(
        category="jra",
        window_from="20200101",
        confusion=_balanced_confusion(40),
        prediction_count=160,
    )
    confusion_oos = _empty_confusion()
    confusion_oos[("nige", "senkou")] = 100
    oos_record = _make_record(
        category="jra",
        window_from="20060101",
        confusion=confusion_oos,
        prediction_count=100,
    )
    ctx = subject.build_report_context(
        feature_version="v1",
        period="all",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        records=[train_record, oos_record],
    )
    rendered = subject.render_report(ctx)
    assert "Critical anti-pattern detected" in rendered


def test_print_summary_writes_per_category_line():
    record = _make_record(
        category="jra",
        window_from="20200101",
        confusion=_balanced_confusion(50),
        prediction_count=200,
    )
    ctx = subject.build_report_context(
        feature_version="v1",
        period="all",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        records=[record],
    )
    stream = io.StringIO()
    subject.print_summary(ctx, stream)
    assert "[jra]" in stream.getvalue()


def test_print_summary_writes_critical_warning_line_when_critical():
    train_record = _make_record(
        category="jra",
        window_from="20200101",
        confusion=_balanced_confusion(40),
        prediction_count=160,
    )
    confusion_oos = _empty_confusion()
    confusion_oos[("nige", "senkou")] = 100
    oos_record = _make_record(
        category="jra",
        window_from="20060101",
        confusion=confusion_oos,
        prediction_count=100,
    )
    ctx = subject.build_report_context(
        feature_version="v1",
        period="all",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        records=[train_record, oos_record],
    )
    stream = io.StringIO()
    subject.print_summary(ctx, stream)
    assert "CRITICAL" in stream.getvalue()


def _build_mock_query(rows: list[tuple[object, ...]]) -> MagicMock:
    return MagicMock(return_value=rows)


def _make_db_row(
    *,
    category: str,
    window_from: str,
    diagonal: int = 0,
    off_diagonal_nige_to_senkou: int = 0,
    prediction_count: int = 0,
    race_count: int = 1,
    top2_hit_count: int = 0,
) -> tuple[object, ...]:
    return (
        category,
        window_from,
        "05",
        1600,
        "11",
        None,
        None,
        None,
        None,
        race_count,
        prediction_count,
        diagonal,
        off_diagonal_nige_to_senkou,
        0,
        0,
        0,
        diagonal,
        0,
        0,
        0,
        0,
        diagonal,
        0,
        0,
        0,
        0,
        diagonal,
        decimal.Decimal("0.0"),
        0,
        decimal.Decimal("0.0"),
        0,
        decimal.Decimal("0.0"),
        0,
        decimal.Decimal("0.0"),
        0,
        top2_hit_count,
    )


def test_fetch_bucket_rows_parses_returned_rows_via_query():
    rows: list[tuple[object, ...]] = [
        _make_db_row(category="jra", window_from="20200101", diagonal=5, prediction_count=20),
        _make_db_row(category="nar", window_from="20200101", diagonal=4, prediction_count=16),
    ]
    query_mock = _build_mock_query(rows)
    records = subject.fetch_bucket_rows(
        pg_url="postgres://test",
        feature_version="v1",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        query=query_mock,
    )
    query_mock.assert_called_once()
    call_args = query_mock.call_args.args
    assert call_args[0] == "postgres://test"
    assert "running_style_model_bucket_evaluations" in call_args[1]
    assert call_args[2] == ("v1", "jra", "jra-v2", "nar", "nar-v1.5")
    assert len(records) == 2
    assert records[0].dims.category == "jra"
    assert records[1].dims.category == "nar"


def test_run_pipeline_writes_output_and_returns_zero_when_clean(tmp_path: Path):
    rows: list[tuple[object, ...]] = [
        _make_db_row(category="jra", window_from="20200101", diagonal=50, prediction_count=200, top2_hit_count=200),
    ]
    query_mock = _build_mock_query(rows)
    output = tmp_path / "report.md"
    stdout = io.StringIO()
    exit_code = subject.run_pipeline(
        pg_url="postgres://test",
        feature_version="v1",
        period="all",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        output_path=output,
        query=query_mock,
        stdout=stdout,
    )
    assert exit_code == 0
    assert output.exists()
    rendered = output.read_text(encoding="utf-8")
    assert "Section 1:" in rendered
    assert "[jra]" in stdout.getvalue()


def test_run_pipeline_returns_one_when_critical_anti_pattern_detected(tmp_path: Path):
    rows: list[tuple[object, ...]] = [
        _make_db_row(
            category="jra",
            window_from="20200101",
            diagonal=40,
            prediction_count=160,
            top2_hit_count=160,
        ),
        _make_db_row(
            category="jra",
            window_from="20060101",
            diagonal=0,
            off_diagonal_nige_to_senkou=100,
            prediction_count=100,
            top2_hit_count=0,
        ),
    ]
    query_mock = _build_mock_query(rows)
    output = tmp_path / "report.md"
    stdout = io.StringIO()
    exit_code = subject.run_pipeline(
        pg_url="postgres://test",
        feature_version="v1",
        period="all",
        model_version_jra="jra-v2",
        model_version_nar="nar-v1.5",
        output_path=output,
        query=query_mock,
        stdout=stdout,
    )
    assert exit_code == 1
    rendered = output.read_text(encoding="utf-8")
    assert "Critical anti-pattern detected" in rendered


def test_main_invokes_run_pipeline_and_exits_with_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    output = tmp_path / "report.md"
    argv = [
        "evaluate_running_style_pipeline.py",
        "--pg-url",
        "postgres://test",
        "--running-style-feature-version",
        "v1",
        "--model-version-jra",
        "jra-x",
        "--model-version-nar",
        "nar-y",
        "--output",
        str(output),
    ]
    monkeypatch.setattr("sys.argv", argv)
    run_pipeline_mock = MagicMock(return_value=0)
    monkeypatch.setattr(subject, "run_pipeline", run_pipeline_mock)
    with pytest.raises(SystemExit) as exc:
        subject.main()
    assert exc.value.code == 0
    run_pipeline_mock.assert_called_once()
    kwargs = run_pipeline_mock.call_args.kwargs
    assert kwargs["pg_url"] == "postgres://test"
    assert kwargs["feature_version"] == "v1"
    assert kwargs["period"] == "all"
    assert kwargs["model_version_jra"] == "jra-x"
    assert kwargs["model_version_nar"] == "nar-y"
    assert kwargs["output_path"] == output


def test_main_exits_with_one_when_run_pipeline_reports_critical(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    output = tmp_path / "report.md"
    argv = [
        "evaluate_running_style_pipeline.py",
        "--pg-url",
        "postgres://test",
        "--running-style-feature-version",
        "v1",
        "--model-version-jra",
        "jra-x",
        "--model-version-nar",
        "nar-y",
        "--output",
        str(output),
    ]
    monkeypatch.setattr("sys.argv", argv)
    run_pipeline_mock = MagicMock(return_value=1)
    monkeypatch.setattr(subject, "run_pipeline", run_pipeline_mock)
    with pytest.raises(SystemExit) as exc:
        subject.main()
    assert exc.value.code == 1


def test_category_recommendations_returns_empty_for_clean_report():
    record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(50),
        prediction_count=200,
    )
    report = subject.build_category_report("jra", "jra-v2", [record])
    assert subject.category_recommendations(report) == []


def test_summary_line_includes_category_model_and_gap():
    train_record = _make_record(
        window_from="20200101",
        confusion=_balanced_confusion(50),
        prediction_count=200,
    )
    oos_record = _make_record(
        window_from="20060101",
        confusion=_balanced_confusion(10),
        prediction_count=40,
    )
    report = subject.build_category_report("jra", "jra-v2", [train_record, oos_record])
    line = subject.summary_line(report)
    assert "[jra]" in line
    assert "jra-v2" in line
    assert "gap=" in line


def test_default_psycopg_query_runs_select_against_psycopg(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}
    fake_rows = [("jra", "20200101")]
    cursor = MagicMock()
    cursor.fetchall.return_value = fake_rows

    def fake_execute(sql: bytes, params: tuple[object, ...]) -> None:
        captured["sql"] = sql
        captured["params"] = params

    cursor.execute = fake_execute
    cursor_ctx = MagicMock()
    cursor_ctx.__enter__.return_value = cursor
    cursor_ctx.__exit__.return_value = False
    connection = MagicMock()
    connection.cursor.return_value = cursor_ctx
    connection_ctx = MagicMock()
    connection_ctx.__enter__.return_value = connection
    connection_ctx.__exit__.return_value = False

    def fake_connect(url: str) -> object:
        captured["url"] = url
        return connection_ctx

    fake_module = type("M", (), {"connect": staticmethod(fake_connect)})

    def fake_import_module(name: str) -> object:
        captured["module"] = name
        return fake_module

    monkeypatch.setattr("importlib.import_module", fake_import_module)
    result = subject.default_psycopg_query("postgres://x", "select 1", ("v1",))
    assert result == fake_rows
    assert captured["url"] == "postgres://x"
    assert captured["module"] == "psycopg"
    assert captured["sql"] == b"select 1"
    assert captured["params"] == ("v1",)
