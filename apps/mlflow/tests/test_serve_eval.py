"""Tests for mlflow_tracking.serve_eval.

Entirely hermetic: every `conn`/cursor used below is a `unittest.mock.MagicMock`
built by `_make_mock_conn`, never a real psycopg2 connection -- these tests
never touch Neon or the local PostgreSQL replica.

Private helpers (`_compute_race_hits`, `_safe_int`, `_parse_actual_rank`) are
never called directly from here (matching this package's existing convention
of testing private helpers only through the public functions that call them,
e.g. test_backfill_serve_timeline.py's `_default_subprocess_runner` test) --
they are exercised through `build_fp_race_eval_rows`/`fetch_race_results`/
`fetch_race_metadata` instead.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from mlflow_tracking import serve_eval

FpResults = dict[tuple[str, str, str], dict[str, object]]
RsRaceMeta = dict[tuple[str, str], dict[str, object]]


def _make_mock_conn(fetchall_return: Sequence[Sequence[object]]) -> MagicMock:
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = fetchall_return
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    return mock_conn


# ── compute_corner1_norm ─────────────────────────────────────────────────────


def test_compute_corner1_norm_empty_string_returns_none() -> None:
    assert serve_eval.compute_corner1_norm("", 10) is None


def test_compute_corner1_norm_literal_00_returns_none() -> None:
    assert serve_eval.compute_corner1_norm("00", 10) is None


def test_compute_corner1_norm_non_digit_returns_none() -> None:
    assert serve_eval.compute_corner1_norm("ab", 10) is None


def test_compute_corner1_norm_digit_zero_returns_none() -> None:
    """A digit string worth 0 that isn't literally "00" (e.g. "000") still None."""
    assert serve_eval.compute_corner1_norm("000", 10) is None


def test_compute_corner1_norm_single_horse_field_returns_none() -> None:
    assert serve_eval.compute_corner1_norm("01", 1) is None


def test_compute_corner1_norm_computes_normal_case() -> None:
    result = serve_eval.compute_corner1_norm("05", 10)
    assert result == pytest.approx(4 / 9)


# ── classify_running_style ───────────────────────────────────────────────────


def test_classify_running_style_none_input_returns_none() -> None:
    assert serve_eval.classify_running_style(None) is None


def test_classify_running_style_zero_is_nige() -> None:
    assert serve_eval.classify_running_style(0.0) == serve_eval.RS_CLASS_NIGE


def test_classify_running_style_below_senkou_threshold_is_senkou() -> None:
    assert serve_eval.classify_running_style(0.15) == serve_eval.RS_CLASS_SENKOU


def test_classify_running_style_at_senkou_threshold_is_senkou() -> None:
    assert serve_eval.classify_running_style(0.3) == serve_eval.RS_CLASS_SENKOU


def test_classify_running_style_between_thresholds_is_sashi() -> None:
    assert serve_eval.classify_running_style(0.5) == serve_eval.RS_CLASS_SASHI


def test_classify_running_style_at_sashi_threshold_is_sashi() -> None:
    assert serve_eval.classify_running_style(0.7) == serve_eval.RS_CLASS_SASHI


def test_classify_running_style_above_sashi_threshold_is_oikomi() -> None:
    assert serve_eval.classify_running_style(0.85) == serve_eval.RS_CLASS_OIKOMI


# ── is_genuine ────────────────────────────────────────────────────────────────


def test_is_genuine_none_generated_at_returns_false() -> None:
    assert serve_eval.is_genuine("20260614", None) is False


def test_is_genuine_exactly_tolerance_days_away_returns_true() -> None:
    generated_at = datetime(2026, 6, 17, 3, 0, 0, tzinfo=UTC)
    assert serve_eval.is_genuine("20260614", generated_at) is True


def test_is_genuine_one_day_beyond_tolerance_returns_false() -> None:
    generated_at = datetime(2026, 6, 18, 3, 0, 0, tzinfo=UTC)
    assert serve_eval.is_genuine("20260614", generated_at) is False


def test_is_genuine_same_day_returns_true() -> None:
    generated_at = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)
    assert serve_eval.is_genuine("20260614", generated_at) is True


def test_is_genuine_custom_tolerance_days_override() -> None:
    generated_at = datetime(2026, 6, 15, 3, 0, 0, tzinfo=UTC)
    assert serve_eval.is_genuine("20260614", generated_at, tolerance_days=1) is True
    assert serve_eval.is_genuine("20260614", generated_at, tolerance_days=0) is False


# ── classify_serving_kind / partition_live_backfill ─────────────────────────


def test_classify_serving_kind_same_day_is_live() -> None:
    generated_at = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)
    assert serve_eval.classify_serving_kind("20260614", generated_at) == "live"


def test_classify_serving_kind_pre_race_negative_lag_is_live() -> None:
    generated_at = datetime(2026, 6, 12, 3, 0, 0, tzinfo=UTC)
    assert serve_eval.classify_serving_kind("20260614", generated_at) == "live"


def test_classify_serving_kind_exactly_at_lag_boundary_is_live() -> None:
    generated_at = datetime(2026, 6, 15, 3, 0, 0, tzinfo=UTC)
    assert serve_eval.classify_serving_kind("20260614", generated_at) == "live"


def test_classify_serving_kind_one_day_beyond_boundary_is_backfill() -> None:
    generated_at = datetime(2026, 6, 16, 3, 0, 0, tzinfo=UTC)
    assert serve_eval.classify_serving_kind("20260614", generated_at) == "backfill"


def test_classify_serving_kind_custom_live_lag_days_override() -> None:
    generated_at = datetime(2026, 6, 16, 3, 0, 0, tzinfo=UTC)
    assert serve_eval.classify_serving_kind("20260614", generated_at, live_lag_days=2) == "live"
    assert serve_eval.classify_serving_kind("20260614", generated_at, live_lag_days=1) == "backfill"


def _genuine_row(*, kaisai_tsukihi: str, generated_at: datetime, model_version: str = "v1") -> dict[
    str, object
]:
    return {
        "kaisai_nen": "2026",
        "kaisai_tsukihi": kaisai_tsukihi,
        "prediction_generated_at": generated_at,
        "model_version": model_version,
    }


def test_partition_live_backfill_splits_rows() -> None:
    live_row = _genuine_row(
        kaisai_tsukihi="0614", generated_at=datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)
    )
    backfill_row = _genuine_row(
        kaisai_tsukihi="0614", generated_at=datetime(2026, 6, 17, 3, 0, 0, tzinfo=UTC)
    )
    live_rows, backfill_rows = serve_eval.partition_live_backfill([live_row, backfill_row])
    assert live_rows == [live_row]
    assert backfill_rows == [backfill_row]


def test_partition_live_backfill_all_live() -> None:
    row = _genuine_row(
        kaisai_tsukihi="0614", generated_at=datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)
    )
    live_rows, backfill_rows = serve_eval.partition_live_backfill([row])
    assert live_rows == [row]
    assert backfill_rows == []


def test_partition_live_backfill_all_backfill() -> None:
    row = _genuine_row(
        kaisai_tsukihi="0614", generated_at=datetime(2026, 6, 17, 3, 0, 0, tzinfo=UTC)
    )
    live_rows, backfill_rows = serve_eval.partition_live_backfill([row])
    assert live_rows == []
    assert backfill_rows == [row]


def test_partition_live_backfill_empty_input() -> None:
    assert serve_eval.partition_live_backfill([]) == ([], [])


# ── fetch_banei_race_count ───────────────────────────────────────────────────


def test_fetch_banei_race_count_shape_and_query() -> None:
    mock_conn = _make_mock_conn([("01",), ("02",)])
    result = serve_eval.fetch_banei_race_count(mock_conn, "20260614")

    assert result == 2
    mock_cur = mock_conn.cursor.return_value
    called_sql, called_params = mock_cur.execute.call_args[0]
    assert "nvd_ra" in called_sql
    assert "keibajo_code" in called_sql
    assert called_params == ("2026", "0614", serve_eval.BANEI_KEIBAJO_CODE)


def test_fetch_banei_race_count_zero_rows() -> None:
    mock_conn = _make_mock_conn([])
    result = serve_eval.fetch_banei_race_count(mock_conn, "20260614")
    assert result == 0


# ── resolve_result_tables / resolve_source ──────────────────────────────────


def test_resolve_result_tables_jra() -> None:
    assert serve_eval.resolve_result_tables("jra") == ("jvd_se", "jvd_ra")


def test_resolve_result_tables_nar() -> None:
    assert serve_eval.resolve_result_tables("nar") == ("nvd_se", "nvd_ra")


def test_resolve_result_tables_banei() -> None:
    assert serve_eval.resolve_result_tables("banei") == ("nvd_se", "nvd_ra")


def test_resolve_result_tables_invalid_category_raises() -> None:
    with pytest.raises(ValueError, match="unknown category"):
        serve_eval.resolve_result_tables("bogus")


def test_resolve_source_jra() -> None:
    assert serve_eval.resolve_source("jra") == "jra"


def test_resolve_source_nar() -> None:
    assert serve_eval.resolve_source("nar") == "nar"


def test_resolve_source_banei() -> None:
    assert serve_eval.resolve_source("banei") == "nar"


def test_resolve_source_invalid_category_raises() -> None:
    with pytest.raises(ValueError, match="unknown category"):
        serve_eval.resolve_source("bogus")


# ── partition_by_banei ───────────────────────────────────────────────────────


def test_partition_by_banei_splits_mixed_rows() -> None:
    rows = [{"keibajo_code": "05"}, {"keibajo_code": "83"}, {"keibajo_code": "30"}]
    non_banei, banei = serve_eval.partition_by_banei(rows)
    assert non_banei == [{"keibajo_code": "05"}, {"keibajo_code": "30"}]
    assert banei == [{"keibajo_code": "83"}]


def test_partition_by_banei_all_banei() -> None:
    rows = [{"keibajo_code": "83"}, {"keibajo_code": "83"}]
    non_banei, banei = serve_eval.partition_by_banei(rows)
    assert non_banei == []
    assert banei == rows


def test_partition_by_banei_all_non_banei() -> None:
    rows = [{"keibajo_code": "05"}, {"keibajo_code": "30"}]
    non_banei, banei = serve_eval.partition_by_banei(rows)
    assert non_banei == rows
    assert banei == []


def test_partition_by_banei_empty_input() -> None:
    non_banei, banei = serve_eval.partition_by_banei([])
    assert non_banei == []
    assert banei == []


# ── filter_genuine_rows ──────────────────────────────────────────────────────


def test_filter_genuine_rows_keeps_only_genuine() -> None:
    genuine_gen_at = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)
    stale_gen_at = datetime(2020, 1, 1, 3, 0, 0, tzinfo=UTC)
    rows = [
        {
            "kaisai_nen": "2026",
            "kaisai_tsukihi": "0614",
            "prediction_generated_at": genuine_gen_at,
            "model_version": "iter14",
        },
        {
            "kaisai_nen": "2026",
            "kaisai_tsukihi": "0614",
            "prediction_generated_at": stale_gen_at,
            "model_version": "iter14",
        },
    ]
    result = serve_eval.filter_genuine_rows(rows)
    assert len(result) == 1
    assert result[0]["prediction_generated_at"] == genuine_gen_at


def test_filter_genuine_rows_empty_input() -> None:
    assert serve_eval.filter_genuine_rows([]) == []


# ── group_by_model_version ───────────────────────────────────────────────────


def test_group_by_model_version_multiple_versions() -> None:
    rows = [
        {"model_version": "iter14", "id": 1},
        {"model_version": "iter20", "id": 2},
        {"model_version": "iter14", "id": 3},
    ]
    result = serve_eval.group_by_model_version(rows)
    assert result == {
        "iter14": [{"model_version": "iter14", "id": 1}, {"model_version": "iter14", "id": 3}],
        "iter20": [{"model_version": "iter20", "id": 2}],
    }


def test_group_by_model_version_single_version() -> None:
    rows = [{"model_version": "iter14", "id": 1}]
    result = serve_eval.group_by_model_version(rows)
    assert result == {"iter14": [{"model_version": "iter14", "id": 1}]}


def test_group_by_model_version_empty_input() -> None:
    assert serve_eval.group_by_model_version([]) == {}


# ── fetch_fp_prediction_rows ─────────────────────────────────────────────────


def test_fetch_fp_prediction_rows_shape_and_query() -> None:
    gen_at = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)
    row = (
        "05",
        "01",
        "1234567890",
        "iter14",
        1,
        gen_at,
        "sprint",
        "medium",
        "summer",
        "A",
        "turf",
        "2026",
        "0614",
    )
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_fp_prediction_rows(mock_conn, "jra", "20260601", "20260630")

    assert result == [
        {
            "keibajo_code": "05",
            "race_bango": "01",
            "ketto_toroku_bango": "1234567890",
            "model_version": "iter14",
            "predicted_rank": 1,
            "prediction_generated_at": gen_at,
            "distance_band": "sprint",
            "field_size_band": "medium",
            "season_band": "summer",
            "class_code": "A",
            "surface": "turf",
            "kaisai_nen": "2026",
            "kaisai_tsukihi": "0614",
        }
    ]
    mock_cur = mock_conn.cursor.return_value
    called_sql, called_params = mock_cur.execute.call_args[0]
    assert "race_finish_position_model_predictions" in called_sql
    assert "WHERE" in called_sql
    assert called_params == ("jra", "20260601", "20260630")


# ── fetch_rs_prediction_rows ─────────────────────────────────────────────────


def test_fetch_rs_prediction_rows_shape_and_query() -> None:
    gen_at = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)
    row = ("05", "01", "1234567890", "iter40", 2, "sashi", gen_at, "2026", "0614")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_rs_prediction_rows(mock_conn, "nar", "20260601", "20260630")

    assert result == [
        {
            "keibajo_code": "05",
            "race_bango": "01",
            "ketto_toroku_bango": "1234567890",
            "model_version": "iter40",
            "predicted_class": 2,
            "predicted_label": "sashi",
            "prediction_generated_at": gen_at,
            "kaisai_nen": "2026",
            "kaisai_tsukihi": "0614",
        }
    ]
    mock_cur = mock_conn.cursor.return_value
    called_sql, called_params = mock_cur.execute.call_args[0]
    assert "race_running_style_model_predictions" in called_sql
    assert "WHERE" in called_sql
    assert called_params == ("nar", "20260601", "20260630")


# ── fetch_race_results (also exercises the private _parse_actual_rank) ──────


def test_fetch_race_results_shape_and_query() -> None:
    row = ("05", "01", "1234567890", "1", "05")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_results(mock_conn, "jra", "20260614")

    assert result == {
        ("05", "01", "1234567890"): {"actual_rank": 1, "corner_1": "05"},
    }
    mock_cur = mock_conn.cursor.return_value
    called_sql, called_params = mock_cur.execute.call_args[0]
    assert "jvd_se" in called_sql
    assert "WHERE" in called_sql
    assert called_params == ("2026", "0614")


def test_fetch_race_results_uses_nar_table_for_banei() -> None:
    mock_conn = _make_mock_conn([])
    serve_eval.fetch_race_results(mock_conn, "banei", "20260614")
    mock_cur = mock_conn.cursor.return_value
    called_sql = mock_cur.execute.call_args[0][0]
    assert "nvd_se" in called_sql


def test_fetch_race_results_non_numeric_kakutei_chakujun_is_none() -> None:
    row = ("05", "01", "1234567890", "中止", "00")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_results(mock_conn, "jra", "20260614")
    assert result[("05", "01", "1234567890")]["actual_rank"] is None


def test_fetch_race_results_zero_kakutei_chakujun_is_none() -> None:
    row = ("05", "01", "1234567890", "0", "00")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_results(mock_conn, "jra", "20260614")
    assert result[("05", "01", "1234567890")]["actual_rank"] is None


def test_fetch_race_results_negative_looking_string_is_none() -> None:
    row = ("05", "01", "1234567890", "-1", "00")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_results(mock_conn, "jra", "20260614")
    assert result[("05", "01", "1234567890")]["actual_rank"] is None


def test_fetch_race_results_valid_positive_digit_string() -> None:
    row = ("05", "01", "1234567890", "12", "08")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_results(mock_conn, "jra", "20260614")
    assert result[("05", "01", "1234567890")]["actual_rank"] == 12


# ── fetch_race_metadata (also exercises the private _safe_int) ─────────────


def test_fetch_race_metadata_shape_and_query() -> None:
    row = ("05", "01", 1200, 16, "10", "A")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_metadata(mock_conn, "jra", "20260614")

    assert result == {
        ("05", "01"): {
            "kyori": 1200,
            "shusso_tosu": 16,
            "track_code": "10",
            "kyoso_joken_code": "A",
        },
    }
    mock_cur = mock_conn.cursor.return_value
    called_sql, called_params = mock_cur.execute.call_args[0]
    assert "jvd_ra" in called_sql
    assert "WHERE" in called_sql
    assert called_params == ("2026", "0614")


def test_fetch_race_metadata_uses_nar_table_for_nar() -> None:
    mock_conn = _make_mock_conn([])
    serve_eval.fetch_race_metadata(mock_conn, "nar", "20260614")
    mock_cur = mock_conn.cursor.return_value
    called_sql = mock_cur.execute.call_args[0][0]
    assert "nvd_ra" in called_sql


def test_fetch_race_metadata_none_track_code_and_class_code() -> None:
    row = ("05", "01", 1200, 16, None, None)
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_metadata(mock_conn, "jra", "20260614")
    assert result[("05", "01")]["track_code"] is None
    assert result[("05", "01")]["kyoso_joken_code"] is None


def test_fetch_race_metadata_kyori_as_valid_digit_string() -> None:
    row = ("05", "01", "1200", 16, "10", "A")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_metadata(mock_conn, "jra", "20260614")
    assert result[("05", "01")]["kyori"] == 1200


def test_fetch_race_metadata_shusso_tosu_empty_string_is_none() -> None:
    row = ("05", "01", 1200, "", "10", "A")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_metadata(mock_conn, "jra", "20260614")
    assert result[("05", "01")]["shusso_tosu"] is None


def test_fetch_race_metadata_kyori_non_numeric_garbage_is_none() -> None:
    row = ("05", "01", "abc", 16, "10", "A")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_metadata(mock_conn, "jra", "20260614")
    assert result[("05", "01")]["kyori"] is None


def test_fetch_race_metadata_shusso_tosu_none_value_is_none() -> None:
    row = ("05", "01", 1200, None, "10", "A")
    mock_conn = _make_mock_conn([row])
    result = serve_eval.fetch_race_metadata(mock_conn, "jra", "20260614")
    assert result[("05", "01")]["shusso_tosu"] is None


# ── build_fp_race_eval_rows (also exercises the private _compute_race_hits) ─


def _fp_pred_row(
    keibajo_code: str,
    race_bango: str,
    ketto_toroku_bango: str,
    predicted_rank: int,
    *,
    kaisai_nen: str = "2026",
    kaisai_tsukihi: str = "0614",
) -> dict[str, object]:
    return {
        "keibajo_code": keibajo_code,
        "race_bango": race_bango,
        "ketto_toroku_bango": ketto_toroku_bango,
        "model_version": "iter14",
        "predicted_rank": predicted_rank,
        "prediction_generated_at": datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC),
        "distance_band": "sprint",
        "field_size_band": "medium",
        "season_band": "summer",
        "class_code": "A",
        "surface": "turf",
        "kaisai_nen": kaisai_nen,
        "kaisai_tsukihi": kaisai_tsukihi,
    }


def test_build_fp_race_eval_rows_all_horses_match_all_hit_flags_true() -> None:
    """pairs end up [(1,1),(2,2),(3,3)] -- every _compute_race_hits flag is 1."""
    pred_rows = [
        _fp_pred_row("05", "01", "H1", 1),
        _fp_pred_row("05", "01", "H2", 2),
        _fp_pred_row("05", "01", "H3", 3),
    ]
    results: FpResults = {
        ("05", "01", "H1"): {"actual_rank": 1, "corner_1": "01"},
        ("05", "01", "H2"): {"actual_rank": 2, "corner_1": "02"},
        ("05", "01", "H3"): {"actual_rank": 3, "corner_1": "03"},
    }
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.venue == "05"
    assert row.race_bango == "01"
    assert row.race_date == "20260614"
    assert row.matched_horses == 3
    assert row.top1_hit == 1
    assert row.place2_hit == 1
    assert row.place3_hit == 1
    assert row.fukusho_2p_hit == 1
    assert row.top3_box_hit == 1
    assert row.predicted_top1_ketto == "H1"
    assert row.actual_top1_ketto == "H1"
    assert row.distance_band == "sprint"
    assert row.field_size_band == "medium"
    assert row.season_band == "summer"
    assert row.class_code == "A"
    assert row.surface == "turf"


def test_build_fp_race_eval_rows_top1_finishes_outside_top3_all_flags_zero() -> None:
    """pairs = [(1,4)] -- pred_rank==1 exists but its actual_rank is outside
    top3, so every _compute_race_hits flag is 0."""
    pred_rows = [_fp_pred_row("05", "01", "H1", 1)]
    results: FpResults = {("05", "01", "H1"): {"actual_rank": 4, "corner_1": "01"}}
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.top1_hit == 0
    assert row.place2_hit == 0
    assert row.place3_hit == 0
    assert row.fukusho_2p_hit == 0
    assert row.top3_box_hit == 0


def test_build_fp_race_eval_rows_rank1_horse_unmatched_others_still_compute_flags() -> None:
    """H1 (predicted_rank==1) has no matching result, so `pairs` becomes
    [(2,1),(3,3)] -- no pred_rank==1 entry at all. top1/place2/place3 stay 0,
    but fukusho_2p_hit/top3_box_hit are still derived from H2/H3's pairs."""
    pred_rows = [
        _fp_pred_row("05", "01", "H1", 1),
        _fp_pred_row("05", "01", "H2", 2),
        _fp_pred_row("05", "01", "H3", 3),
    ]
    results: FpResults = {
        ("05", "01", "H2"): {"actual_rank": 1, "corner_1": "02"},
        ("05", "01", "H3"): {"actual_rank": 3, "corner_1": "03"},
    }
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.top1_hit == 0
    assert row.place2_hit == 0
    assert row.place3_hit == 0
    assert row.fukusho_2p_hit == 1
    assert row.top3_box_hit == 0
    assert row.predicted_top1_ketto == "H1"
    assert row.actual_top1_ketto == "H2"


def test_build_fp_race_eval_rows_partial_match_still_emits_row() -> None:
    """H2 has no matching result -- the race is still emitted using only H1/H3's pairs."""
    pred_rows = [
        _fp_pred_row("05", "01", "H1", 1),
        _fp_pred_row("05", "01", "H2", 2),
        _fp_pred_row("05", "01", "H3", 3),
    ]
    results: FpResults = {
        ("05", "01", "H1"): {"actual_rank": 1, "corner_1": "01"},
        ("05", "01", "H3"): {"actual_rank": 3, "corner_1": "03"},
    }
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.matched_horses == 2
    assert row.top1_hit == 1
    assert row.predicted_top1_ketto == "H1"
    assert row.actual_top1_ketto == "H1"


def test_build_fp_race_eval_rows_zero_matches_skips_race() -> None:
    pred_rows = [_fp_pred_row("05", "01", "H1", 1)]
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, {})
    assert rows == []


def test_build_fp_race_eval_rows_predicted_top1_resolves_even_when_unmatched() -> None:
    """predicted_top1_ketto comes from pred_rows directly -- it still resolves
    even though the predicted_rank==1 horse (H1) has no matching result, and
    no matched horse here is actually rank 1 either."""
    pred_rows = [
        _fp_pred_row("05", "01", "H1", 1),
        _fp_pred_row("05", "01", "H2", 2),
    ]
    results: FpResults = {
        ("05", "01", "H2"): {"actual_rank": 4, "corner_1": "02"},
    }
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.matched_horses == 1
    assert row.predicted_top1_ketto == "H1"
    assert row.actual_top1_ketto is None


def test_build_fp_race_eval_rows_dnf_horse_has_none_actual_rank_skipped() -> None:
    """H2 has a results entry (it finished the race data-wise) but its
    actual_rank is None (DNF/non-numeric kakutei_chakujun) -- it is excluded
    from `pairs` the same way an entirely-missing result would be."""
    pred_rows = [
        _fp_pred_row("05", "01", "H1", 1),
        _fp_pred_row("05", "01", "H2", 2),
    ]
    results: FpResults = {
        ("05", "01", "H1"): {"actual_rank": 1, "corner_1": "01"},
        ("05", "01", "H2"): {"actual_rank": None, "corner_1": "02"},
    }
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)
    assert len(rows) == 1
    assert rows[0].matched_horses == 1


def test_build_fp_race_eval_rows_no_predicted_rank1_row_leaves_top1_ketto_none() -> None:
    """No row in this race's group has predicted_rank==1 at all (e.g. the
    model never ranked anyone first) -- predicted_top1_ketto stays None."""
    pred_rows = [
        _fp_pred_row("05", "01", "H2", 2),
        _fp_pred_row("05", "01", "H3", 3),
    ]
    results: FpResults = {
        ("05", "01", "H2"): {"actual_rank": 2, "corner_1": "02"},
        ("05", "01", "H3"): {"actual_rank": 3, "corner_1": "03"},
    }
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)
    assert len(rows) == 1
    assert rows[0].predicted_top1_ketto is None


def test_build_fp_race_eval_rows_no_result_entry_for_horse_skips_it() -> None:
    """A horse absent entirely from `results` (no key at all) is skipped
    the same way a None actual_rank is."""
    pred_rows = [
        _fp_pred_row("05", "01", "H1", 1),
        _fp_pred_row("05", "01", "H2", 2),
    ]
    results: FpResults = {("05", "01", "H1"): {"actual_rank": 1, "corner_1": "01"}}
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)
    assert len(rows) == 1
    assert rows[0].matched_horses == 1


# ── build_fp_race_eval_rows: place4/5/6 small-field exclusion ───────────────
#
# `total_starters` (the race's TRUE starter count, derived from every
# DISTINCT ketto_toroku_bango key in `results` sharing this race's
# (keibajo_code, race_bango) -- NOT just the horses a prediction matched)
# gates place4_hit/place5_hit/place6_hit to None whenever it falls below
# that rank. Only ONE horse (H1, predicted_rank=1) is ever included in
# `pred_rows` below -- `results` alone carries the other starters, proving
# the count comes from `results`, not from how many predictions matched.


def _n_starter_results(
    keibajo_code: str, race_bango: str, n: int, *, winner_ketto: str = "H1"
) -> FpResults:
    """Build a `results` dict for one race with exactly `n` distinct
    starters (H1..Hn), each with a distinct actual_rank 1..n: `winner_ketto`
    (default "H1") gets actual_rank==1, and every other horse gets the next
    unused rank in 2..n, in H-index order."""
    results: FpResults = {}
    next_rank = 2
    for i in range(1, n + 1):
        ketto = f"H{i}"
        if ketto == winner_ketto:
            actual_rank = 1
        else:
            actual_rank = next_rank
            next_rank += 1
        results[(keibajo_code, race_bango, ketto)] = {
            "actual_rank": actual_rank,
            "corner_1": "01",
        }
    return results


def test_build_fp_race_eval_rows_place456_all_none_when_total_starters_is_3() -> None:
    """A 3-starter field: place4_hit/place5_hit/place6_hit are ALL None (no
    3-fewer-starter race can produce a legitimate rank-4/5/6 finisher at
    all) -- place2_hit/place3_hit are UNAFFECTED (no guard for them at all,
    see `_compute_race_hits`'s own docstring)."""
    pred_rows = [_fp_pred_row("05", "01", "H1", 1)]
    results = _n_starter_results("05", "01", 3)
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.top1_hit == 1
    assert row.place2_hit == 1
    assert row.place3_hit == 1
    assert row.place4_hit is None
    assert row.place5_hit is None
    assert row.place6_hit is None


def test_build_fp_race_eval_rows_place4_hit_place56_none_when_total_starters_is_4() -> None:
    """A 4-starter field: place4_hit is a real 0/1 (4 >= 4), but
    place5_hit/place6_hit stay None (4 < 5, 4 < 6)."""
    pred_rows = [_fp_pred_row("05", "01", "H1", 1)]
    results = _n_starter_results("05", "01", 4)
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.place4_hit == 1
    assert row.place5_hit is None
    assert row.place6_hit is None


def test_build_fp_race_eval_rows_place45_hit_place6_none_when_total_starters_is_5() -> None:
    """A 5-starter field: place4_hit/place5_hit are real 0/1 (5 >= 4, 5 >= 5),
    place6_hit stays None (5 < 6) -- this is the concrete "6th place in a
    5-horse field cannot exist" scenario this whole design exists to guard
    against: a naive ungated formula would have made place6_hit trivially 1
    here (pred1_actual=1 <= 6), silently inflating place6_pct."""
    pred_rows = [_fp_pred_row("05", "01", "H1", 1)]
    results = _n_starter_results("05", "01", 5)
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.place4_hit == 1
    assert row.place5_hit == 1
    assert row.place6_hit is None


def test_build_fp_race_eval_rows_place456_all_hit_when_total_starters_is_6() -> None:
    """A 6-starter field: place4_hit/place5_hit/place6_hit are ALL real 0/1
    (6 >= 4, 6 >= 5, 6 >= 6) -- the boundary case where rank 6 first becomes
    legitimately computable."""
    pred_rows = [_fp_pred_row("05", "01", "H1", 1)]
    results = _n_starter_results("05", "01", 6)
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.place4_hit == 1
    assert row.place5_hit == 1
    assert row.place6_hit == 1


def test_build_fp_race_eval_rows_place456_all_hit_when_total_starters_is_7_plus() -> None:
    """A normal large field (7+ starters, the realistic jra/nar common case):
    place4_hit/place5_hit/place6_hit are all real 0/1, with no exclusion at
    all -- the "normal large-field case" this design must not disturb."""
    pred_rows = [_fp_pred_row("05", "01", "H1", 1)]
    results = _n_starter_results("05", "01", 12)
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.matched_horses == 1
    assert row.place4_hit == 1
    assert row.place5_hit == 1
    assert row.place6_hit == 1


def test_build_fp_race_eval_rows_place456_miss_vs_hit_distinguished_from_none() -> None:
    """A 6-starter field where the predicted winner (H1) actually finished
    5th: place4_hit=0 (a real MISS, 5 > 4, distinct from None), place5_hit=1
    (a real HIT, 5 <= 5), place6_hit=1 (5 <= 6) -- proves None/0/1 are three
    genuinely distinct outcomes, not just None-vs-nonzero."""
    pred_rows = [_fp_pred_row("05", "01", "H1", 1)]
    results: FpResults = {
        ("05", "01", "H1"): {"actual_rank": 5, "corner_1": "01"},
        ("05", "01", "H2"): {"actual_rank": 1, "corner_1": "01"},
        ("05", "01", "H3"): {"actual_rank": 2, "corner_1": "01"},
        ("05", "01", "H4"): {"actual_rank": 3, "corner_1": "01"},
        ("05", "01", "H5"): {"actual_rank": 4, "corner_1": "01"},
        ("05", "01", "H6"): {"actual_rank": 6, "corner_1": "01"},
    }
    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)

    assert len(rows) == 1
    row = rows[0]
    assert row.top1_hit == 0
    assert row.place4_hit == 0
    assert row.place5_hit == 1
    assert row.place6_hit == 1


def test_build_fp_race_eval_rows_starter_count_isolated_per_race() -> None:
    """`results` spans TWO races on the same date -- the small race's
    starter count (3) must never leak into the large race's (7), and vice
    versa, even though both entries live in the same `results` dict passed
    to one `build_fp_race_eval_rows` call. Both races reuse the same
    ketto_toroku_bango values ("H1".."H7") -- the (keibajo_code, race_bango,
    ketto_toroku_bango) key already disambiguates them, exactly like real
    per-race horse identity."""
    pred_rows = [
        _fp_pred_row("05", "01", "H1", 1),
        _fp_pred_row("10", "02", "H1", 1),
    ]
    small_race = _n_starter_results("05", "01", 3)
    large_race = _n_starter_results("10", "02", 7)
    results: FpResults = {**small_race, **large_race}

    rows = serve_eval.build_fp_race_eval_rows("jra", "iter14", pred_rows, results)
    by_race = {(r.venue, r.race_bango): r for r in rows}

    assert by_race[("05", "01")].place6_hit is None
    assert by_race[("10", "02")].place6_hit == 1


# ── build_rs_horse_eval_rows ─────────────────────────────────────────────────


def _rs_pred_row(
    keibajo_code: str,
    race_bango: str,
    ketto_toroku_bango: str,
    predicted_class: int,
) -> dict[str, object]:
    return {
        "keibajo_code": keibajo_code,
        "race_bango": race_bango,
        "ketto_toroku_bango": ketto_toroku_bango,
        "model_version": "iter40",
        "predicted_class": predicted_class,
        "predicted_label": serve_eval.RS_CLASS_LABELS[predicted_class],
        "prediction_generated_at": datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC),
        "kaisai_nen": "2026",
        "kaisai_tsukihi": "0614",
    }


def test_build_rs_horse_eval_rows_normal_horse_hit() -> None:
    pred_rows = [_rs_pred_row("05", "01", "H1", serve_eval.RS_CLASS_SASHI)]
    results: FpResults = {("05", "01", "H1"): {"actual_rank": 5, "corner_1": "05"}}
    race_meta: RsRaceMeta = {
        ("05", "01"): {
            "kyori": 1200,
            "shusso_tosu": 10,
            "track_code": "10",
            "kyoso_joken_code": "A",
        }
    }
    rows = serve_eval.build_rs_horse_eval_rows("jra", "iter40", pred_rows, results, race_meta)

    assert len(rows) == 1
    row = rows[0]
    # corner_1="05" over shusso_tosu=10 -> (5-1)/(10-1) = 0.444 -> sashi
    assert row.actual_class == serve_eval.RS_CLASS_SASHI
    assert row.actual_label == "sashi"
    assert row.predicted_class == serve_eval.RS_CLASS_SASHI
    assert row.predicted_label == "sashi"
    assert row.hit == 1
    assert row.venue == "05"
    assert row.race_date == "20260614"
    assert row.distance_band == "sprint"
    assert row.field_size_band == "medium"
    assert row.season_band == "summer"
    assert row.surface == "turf"
    assert row.class_code == "A"


def test_build_rs_horse_eval_rows_predicted_class_miss() -> None:
    pred_rows = [_rs_pred_row("05", "01", "H1", serve_eval.RS_CLASS_NIGE)]
    results: FpResults = {("05", "01", "H1"): {"actual_rank": 5, "corner_1": "05"}}
    race_meta: RsRaceMeta = {
        ("05", "01"): {
            "kyori": 1200,
            "shusso_tosu": 10,
            "track_code": "10",
            "kyoso_joken_code": "A",
        }
    }
    rows = serve_eval.build_rs_horse_eval_rows("jra", "iter40", pred_rows, results, race_meta)
    assert len(rows) == 1
    assert rows[0].hit == 0


def test_build_rs_horse_eval_rows_no_matching_result_is_skipped() -> None:
    pred_rows = [_rs_pred_row("05", "01", "H1", serve_eval.RS_CLASS_NIGE)]
    rows = serve_eval.build_rs_horse_eval_rows("jra", "iter40", pred_rows, {}, {})
    assert rows == []


def test_build_rs_horse_eval_rows_straight_course_is_skipped() -> None:
    """corner_1 is "00" (or blank) -- classify_running_style returns None, so
    the horse is skipped entirely."""
    pred_rows = [_rs_pred_row("05", "01", "H1", serve_eval.RS_CLASS_NIGE)]
    results: FpResults = {("05", "01", "H1"): {"actual_rank": 1, "corner_1": "00"}}
    race_meta: RsRaceMeta = {
        ("05", "01"): {
            "kyori": 1200,
            "shusso_tosu": 10,
            "track_code": "10",
            "kyoso_joken_code": "A",
        }
    }
    rows = serve_eval.build_rs_horse_eval_rows("jra", "iter40", pred_rows, results, race_meta)
    assert rows == []


def test_build_rs_horse_eval_rows_no_race_meta_entry_is_skipped() -> None:
    pred_rows = [_rs_pred_row("05", "01", "H1", serve_eval.RS_CLASS_NIGE)]
    results: FpResults = {("05", "01", "H1"): {"actual_rank": 1, "corner_1": "05"}}
    rows = serve_eval.build_rs_horse_eval_rows("jra", "iter40", pred_rows, results, {})
    assert rows == []


def test_build_rs_horse_eval_rows_shusso_tosu_none_is_skipped() -> None:
    pred_rows = [_rs_pred_row("05", "01", "H1", serve_eval.RS_CLASS_NIGE)]
    results: FpResults = {("05", "01", "H1"): {"actual_rank": 1, "corner_1": "05"}}
    race_meta: RsRaceMeta = {
        ("05", "01"): {
            "kyori": 1200,
            "shusso_tosu": None,
            "track_code": "10",
            "kyoso_joken_code": "A",
        }
    }
    rows = serve_eval.build_rs_horse_eval_rows("jra", "iter40", pred_rows, results, race_meta)
    assert rows == []


# ── compute_rank_pct ─────────────────────────────────────────────────────────


def test_compute_rank_pct_all_hits() -> None:
    assert serve_eval.compute_rank_pct([1, 1, 1]) == 100.0


def test_compute_rank_pct_mix_of_hits_and_misses() -> None:
    assert serve_eval.compute_rank_pct([1, 0, 1, 0]) == 50.0


def test_compute_rank_pct_skips_none_from_denominator() -> None:
    """Two eligible races (one hit, one miss) plus one None (excluded from
    the denominator, not just the numerator) -- 1/2 == 50%, not 1/3."""
    assert serve_eval.compute_rank_pct([1, 0, None]) == 50.0


def test_compute_rank_pct_all_none_returns_none() -> None:
    assert serve_eval.compute_rank_pct([None, None]) is None


def test_compute_rank_pct_empty_list_returns_none() -> None:
    assert serve_eval.compute_rank_pct([]) is None


# ── aggregate_fp_day_metrics ─────────────────────────────────────────────────


def _fp_eval_row(
    *,
    top1_hit: int,
    place2_hit: int,
    place3_hit: int,
    fukusho_2p_hit: int,
    top3_box_hit: int,
    place4_hit: int | None = None,
    place5_hit: int | None = None,
    place6_hit: int | None = None,
) -> serve_eval.FpRaceEvalRow:
    return serve_eval.FpRaceEvalRow(
        category="jra",
        race_date="20260614",
        venue="05",
        race_bango="01",
        model_version="iter14",
        distance_band="sprint",
        field_size_band="medium",
        season_band="summer",
        class_code="A",
        surface="turf",
        predicted_top1_ketto="H1",
        actual_top1_ketto="H1",
        matched_horses=3,
        top1_hit=top1_hit,
        place2_hit=place2_hit,
        place3_hit=place3_hit,
        place4_hit=place4_hit,
        place5_hit=place5_hit,
        place6_hit=place6_hit,
        fukusho_2p_hit=fukusho_2p_hit,
        top3_box_hit=top3_box_hit,
    )


def test_aggregate_fp_day_metrics_computes_percentages() -> None:
    rows = [
        _fp_eval_row(
            top1_hit=1,
            place2_hit=1,
            place3_hit=1,
            fukusho_2p_hit=1,
            top3_box_hit=1,
            place4_hit=1,
            place5_hit=1,
            place6_hit=1,
        ),
        _fp_eval_row(
            top1_hit=1,
            place2_hit=1,
            place3_hit=1,
            fukusho_2p_hit=0,
            top3_box_hit=0,
            place4_hit=1,
            place5_hit=0,
            place6_hit=0,
        ),
        _fp_eval_row(
            top1_hit=0,
            place2_hit=1,
            place3_hit=1,
            fukusho_2p_hit=1,
            top3_box_hit=0,
            place4_hit=0,
            place5_hit=0,
            place6_hit=0,
        ),
        _fp_eval_row(top1_hit=0, place2_hit=0, place3_hit=0, fukusho_2p_hit=0, top3_box_hit=0),
    ]
    result = serve_eval.aggregate_fp_day_metrics(rows)
    assert result == {
        "races": 4.0,
        "top1_pct": 50.0,
        "place2_pct": 75.0,
        "place3_pct": 75.0,
        # place4/5/6_pct are each computed over only the 3 rows above with a
        # non-None place{N}_hit (the 4th row's default None simulates a
        # too-small-a-field race, excluded from every rank-4/5/6 population).
        "place4_pct": pytest.approx(200.0 / 3.0),
        "place5_pct": pytest.approx(100.0 / 3.0),
        "place6_pct": pytest.approx(100.0 / 3.0),
        "fukusho_2p_pct": 50.0,
        "top3_box_pct": 25.0,
    }


def test_aggregate_fp_day_metrics_place456_all_none_yields_none_pct() -> None:
    """Every row's place4/5/6_hit is None (e.g. every race this day had a
    too-small field for those ranks) -- the corresponding *_pct must be None,
    never a fabricated 0.0, while the always-present metrics are unaffected."""
    rows = [
        _fp_eval_row(top1_hit=1, place2_hit=1, place3_hit=1, fukusho_2p_hit=1, top3_box_hit=1),
        _fp_eval_row(top1_hit=0, place2_hit=0, place3_hit=0, fukusho_2p_hit=0, top3_box_hit=0),
    ]
    result = serve_eval.aggregate_fp_day_metrics(rows)
    assert result["place4_pct"] is None
    assert result["place5_pct"] is None
    assert result["place6_pct"] is None
    assert result["top1_pct"] == 50.0


def test_aggregate_fp_day_metrics_empty_list_returns_zero_shape() -> None:
    result = serve_eval.aggregate_fp_day_metrics([])
    assert result == {
        "races": 0.0,
        "top1_pct": 0.0,
        "place2_pct": 0.0,
        "place3_pct": 0.0,
        "place4_pct": None,
        "place5_pct": None,
        "place6_pct": None,
        "fukusho_2p_pct": 0.0,
        "top3_box_pct": 0.0,
    }


# ── aggregate_rs_day_metrics ─────────────────────────────────────────────────


def _rs_eval_row(*, predicted_class: int, actual_class: int) -> serve_eval.RsHorseEvalRow:
    return serve_eval.RsHorseEvalRow(
        category="jra",
        race_date="20260614",
        venue="05",
        race_bango="01",
        ketto_toroku_bango="H1",
        model_version="iter40",
        distance_band="sprint",
        field_size_band="medium",
        season_band="summer",
        class_code="A",
        surface="turf",
        predicted_class=predicted_class,
        predicted_label=serve_eval.RS_CLASS_LABELS[predicted_class],
        actual_class=actual_class,
        actual_label=serve_eval.RS_CLASS_LABELS[actual_class],
        hit=1 if predicted_class == actual_class else 0,
    )


def test_aggregate_rs_day_metrics_computes_per_class_and_overall() -> None:
    rows = [
        _rs_eval_row(
            predicted_class=serve_eval.RS_CLASS_NIGE, actual_class=serve_eval.RS_CLASS_NIGE
        ),
        _rs_eval_row(
            predicted_class=serve_eval.RS_CLASS_SENKOU, actual_class=serve_eval.RS_CLASS_SASHI
        ),
        _rs_eval_row(
            predicted_class=serve_eval.RS_CLASS_SASHI, actual_class=serve_eval.RS_CLASS_SENKOU
        ),
        _rs_eval_row(
            predicted_class=serve_eval.RS_CLASS_OIKOMI, actual_class=serve_eval.RS_CLASS_OIKOMI
        ),
    ]
    result = serve_eval.aggregate_rs_day_metrics(rows)

    assert result["total_horses"] == 4
    assert result["overall_accuracy_pct"] == 50.0
    assert result["macro_f1_pct"] == pytest.approx(100.0)
    assert result["per_class"] == [
        {"label": "nige", "f1_pct": pytest.approx(100.0)},
        {"label": "senkou", "f1_pct": None},
        {"label": "sashi", "f1_pct": None},
        {"label": "oikomi", "f1_pct": pytest.approx(100.0)},
    ]


def test_aggregate_rs_day_metrics_precision_none_and_recall_none_combinations() -> None:
    """One row: predicted=senkou, actual=nige. NIGE has precision=None
    (never predicted) but recall=0.0 (not None); SENKOU has precision=0.0
    (not None) but recall=None (never actually senkou) -- both combinations
    of "one side None" are covered by this single row."""
    rows = [
        _rs_eval_row(
            predicted_class=serve_eval.RS_CLASS_SENKOU, actual_class=serve_eval.RS_CLASS_NIGE
        )
    ]
    result = serve_eval.aggregate_rs_day_metrics(rows)

    assert result["per_class"] == [
        {"label": "nige", "f1_pct": None},
        {"label": "senkou", "f1_pct": None},
        {"label": "sashi", "f1_pct": None},
        {"label": "oikomi", "f1_pct": None},
    ]
    assert result["macro_f1_pct"] is None
    assert result["overall_accuracy_pct"] == 0.0


def test_aggregate_rs_day_metrics_empty_list_returns_zero_filled_shape() -> None:
    result = serve_eval.aggregate_rs_day_metrics([])
    assert result["total_horses"] == 0
    assert result["overall_accuracy_pct"] == 0.0
    assert result["macro_f1_pct"] is None
    assert result["per_class"] == [
        {"label": "nige", "f1_pct": None},
        {"label": "senkou", "f1_pct": None},
        {"label": "sashi", "f1_pct": None},
        {"label": "oikomi", "f1_pct": None},
    ]


# ── fp_eval_rows_to_dataframe / rs_eval_rows_to_dataframe ───────────────────

_FP_EVAL_ROW_FIELD_NAMES = [f.name for f in dataclasses.fields(serve_eval.FpRaceEvalRow)]
_RS_EVAL_ROW_FIELD_NAMES = [f.name for f in dataclasses.fields(serve_eval.RsHorseEvalRow)]


def test_fp_eval_rows_to_dataframe_with_rows() -> None:
    rows = [
        _fp_eval_row(top1_hit=1, place2_hit=1, place3_hit=1, fukusho_2p_hit=1, top3_box_hit=1)
    ]
    df = serve_eval.fp_eval_rows_to_dataframe(rows)
    assert len(df) == 1
    assert list(df.columns) == _FP_EVAL_ROW_FIELD_NAMES
    assert df.iloc[0]["venue"] == "05"


def test_fp_eval_rows_to_dataframe_empty() -> None:
    df = serve_eval.fp_eval_rows_to_dataframe([])
    assert len(df) == 0
    assert list(df.columns) == _FP_EVAL_ROW_FIELD_NAMES


def test_rs_eval_rows_to_dataframe_with_rows() -> None:
    rows = [
        _rs_eval_row(
            predicted_class=serve_eval.RS_CLASS_NIGE, actual_class=serve_eval.RS_CLASS_NIGE
        )
    ]
    df = serve_eval.rs_eval_rows_to_dataframe(rows)
    assert len(df) == 1
    assert list(df.columns) == _RS_EVAL_ROW_FIELD_NAMES
    assert df.iloc[0]["hit"] == 1


def test_rs_eval_rows_to_dataframe_empty() -> None:
    df = serve_eval.rs_eval_rows_to_dataframe([])
    assert len(df) == 0
    assert list(df.columns) == _RS_EVAL_ROW_FIELD_NAMES
