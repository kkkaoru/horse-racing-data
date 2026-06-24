from __future__ import annotations

import polars as pl
from polars.testing import assert_frame_equal

import small_field_router as subject


def test_default_small_field_threshold_is_8():
    assert subject.DEFAULT_SMALL_FIELD_THRESHOLD == 8


def test_compute_field_sizes_counts_rows_per_race():
    predictions = pl.DataFrame({
        "race_id": ["nar:a", "nar:a", "nar:a", "nar:b", "nar:b"],
        "ketto_toroku_bango": ["1", "2", "3", "4", "5"],
        "umaban": [1, 2, 3, 1, 2],
        "predicted_score": [0.9, 0.5, 0.1, 0.8, 0.2],
        "predicted_rank": [1, 2, 3, 1, 2],
    })
    sizes = subject.compute_field_sizes(predictions)
    sizes_map = dict(zip(sizes["race_id"].to_list(), sizes["field_size"].to_list(), strict=True))
    assert sizes_map["nar:a"] == 3
    assert sizes_map["nar:b"] == 2


def test_select_small_field_race_ids_includes_exactly_eight_runners():
    predictions = pl.DataFrame({
        "race_id": ["nar:eight"] * 8,
        "ketto_toroku_bango": [str(i) for i in range(8)],
        "umaban": list(range(1, 9)),
        "predicted_score": [0.5] * 8,
        "predicted_rank": list(range(1, 9)),
    })
    selected = subject.select_small_field_race_ids(predictions, 8)
    assert selected == {"nar:eight"}


def test_select_small_field_race_ids_excludes_nine_runners():
    predictions = pl.DataFrame({
        "race_id": ["nar:nine"] * 9,
        "ketto_toroku_bango": [str(i) for i in range(9)],
        "umaban": list(range(1, 10)),
        "predicted_score": [0.5] * 9,
        "predicted_rank": list(range(1, 10)),
    })
    selected = subject.select_small_field_race_ids(predictions, 8)
    assert selected == set()


def test_select_small_field_race_ids_boundary_mixed():
    predictions = pl.DataFrame({
        "race_id": ["nar:small", "nar:small", "nar:big", "nar:big", "nar:big"],
        "ketto_toroku_bango": ["1", "2", "3", "4", "5"],
        "umaban": [1, 2, 1, 2, 3],
        "predicted_score": [0.9, 0.1, 0.8, 0.5, 0.2],
        "predicted_rank": [1, 2, 1, 2, 3],
    })
    selected = subject.select_small_field_race_ids(predictions, 2)
    assert selected == {"nar:small"}


def test_route_small_race_takes_small_field_model_rows():
    small_field = pl.DataFrame({
        "race_id": ["nar:s", "nar:s"],
        "ketto_toroku_bango": ["10", "20"],
        "umaban": [1, 2],
        "predicted_score": [0.99, 0.01],
        "predicted_rank": [1, 2],
    })
    large_field = pl.DataFrame({
        "race_id": ["nar:s", "nar:s"],
        "ketto_toroku_bango": ["10", "20"],
        "umaban": [1, 2],
        "predicted_score": [0.11, 0.22],
        "predicted_rank": [2, 1],
    })
    routed = subject.route_small_field_predictions(small_field, large_field, threshold=8)
    expected = pl.DataFrame({
        "race_id": ["nar:s", "nar:s"],
        "ketto_toroku_bango": ["10", "20"],
        "umaban": [1, 2],
        "predicted_score": [0.99, 0.01],
        "predicted_rank": [1, 2],
    })
    assert_frame_equal(routed, expected)


def test_route_large_race_takes_large_field_model_rows():
    small_field = pl.DataFrame({
        "race_id": ["nar:l"] * 9,
        "ketto_toroku_bango": [str(i) for i in range(9)],
        "umaban": list(range(1, 10)),
        "predicted_score": [0.5] * 9,
        "predicted_rank": list(range(1, 10)),
    })
    large_field = pl.DataFrame({
        "race_id": ["nar:l"] * 9,
        "ketto_toroku_bango": [str(i) for i in range(9)],
        "umaban": list(range(1, 10)),
        "predicted_score": [0.3] * 9,
        "predicted_rank": list(range(9, 0, -1)),
    })
    routed = subject.route_small_field_predictions(small_field, large_field, threshold=8)
    assert_frame_equal(routed, large_field)


def test_route_exactly_eight_routed_to_small_field():
    small_field = pl.DataFrame({
        "race_id": ["nar:8"] * 8,
        "ketto_toroku_bango": [str(i) for i in range(8)],
        "umaban": list(range(1, 9)),
        "predicted_score": [0.7] * 8,
        "predicted_rank": list(range(1, 9)),
    })
    large_field = pl.DataFrame({
        "race_id": ["nar:8"] * 8,
        "ketto_toroku_bango": [str(i) for i in range(8)],
        "umaban": list(range(1, 9)),
        "predicted_score": [0.2] * 8,
        "predicted_rank": list(range(8, 0, -1)),
    })
    routed = subject.route_small_field_predictions(small_field, large_field)
    assert routed["predicted_score"].to_list() == [0.7] * 8


def test_route_nine_routed_to_large_field():
    small_field = pl.DataFrame({
        "race_id": ["nar:9"] * 9,
        "ketto_toroku_bango": [str(i) for i in range(9)],
        "umaban": list(range(1, 10)),
        "predicted_score": [0.7] * 9,
        "predicted_rank": list(range(1, 10)),
    })
    large_field = pl.DataFrame({
        "race_id": ["nar:9"] * 9,
        "ketto_toroku_bango": [str(i) for i in range(9)],
        "umaban": list(range(1, 10)),
        "predicted_score": [0.2] * 9,
        "predicted_rank": list(range(9, 0, -1)),
    })
    routed = subject.route_small_field_predictions(small_field, large_field)
    assert routed["predicted_score"].to_list() == [0.2] * 9


def test_route_empty_large_field_returns_empty_frame():
    small_field = pl.DataFrame({
        "race_id": ["nar:x", "nar:x"],
        "ketto_toroku_bango": ["1", "2"],
        "umaban": [1, 2],
        "predicted_score": [0.9, 0.1],
        "predicted_rank": [1, 2],
    })
    large_field = pl.DataFrame(
        schema={
            "race_id": pl.String,
            "ketto_toroku_bango": pl.String,
            "umaban": pl.Int64,
            "predicted_score": pl.Float64,
            "predicted_rank": pl.Int64,
        },
    )
    routed = subject.route_small_field_predictions(small_field, large_field)
    assert routed.is_empty()
    assert routed.columns == [
        "race_id",
        "ketto_toroku_bango",
        "umaban",
        "predicted_score",
        "predicted_rank",
    ]


def test_route_small_race_absent_from_small_field_falls_back_to_large():
    small_field = pl.DataFrame({
        "race_id": ["nar:present", "nar:present"],
        "ketto_toroku_bango": ["1", "2"],
        "umaban": [1, 2],
        "predicted_score": [0.99, 0.01],
        "predicted_rank": [1, 2],
    })
    large_field = pl.DataFrame({
        "race_id": ["nar:present", "nar:present", "nar:missing", "nar:missing"],
        "ketto_toroku_bango": ["1", "2", "3", "4"],
        "umaban": [1, 2, 1, 2],
        "predicted_score": [0.11, 0.22, 0.33, 0.44],
        "predicted_rank": [2, 1, 1, 2],
    })
    routed = subject.route_small_field_predictions(small_field, large_field, threshold=8)
    expected = pl.DataFrame({
        "race_id": ["nar:missing", "nar:missing", "nar:present", "nar:present"],
        "ketto_toroku_bango": ["3", "4", "1", "2"],
        "umaban": [1, 2, 1, 2],
        "predicted_score": [0.33, 0.44, 0.99, 0.01],
        "predicted_rank": [1, 2, 1, 2],
    })
    assert_frame_equal(routed, expected)


def test_route_output_sorted_by_race_id_then_umaban():
    small_field = pl.DataFrame({
        "race_id": ["nar:a", "nar:a"],
        "ketto_toroku_bango": ["1", "2"],
        "umaban": [2, 1],
        "predicted_score": [0.4, 0.6],
        "predicted_rank": [2, 1],
    })
    large_field = pl.DataFrame({
        "race_id": ["nar:b", "nar:a", "nar:a", "nar:b"],
        "ketto_toroku_bango": ["7", "1", "2", "8"],
        "umaban": [2, 2, 1, 1],
        "predicted_score": [0.2, 0.5, 0.5, 0.3],
        "predicted_rank": [9, 9, 9, 9],
    })
    routed = subject.route_small_field_predictions(small_field, large_field, threshold=8)
    assert routed["race_id"].to_list() == ["nar:a", "nar:a", "nar:b", "nar:b"]
    assert routed["umaban"].to_list() == [1, 2, 1, 2]
