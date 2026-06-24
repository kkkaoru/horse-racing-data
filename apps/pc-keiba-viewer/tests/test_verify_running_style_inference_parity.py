"""Tests for verify_running_style_inference_parity (Agent W5, parity framework)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import numpy as np
import polars as pl
import pytest

import verify_running_style_inference_parity as subject


def test_parse_args_full_set() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", "/tmp/out.parquet",
        "--model-flatbin", "/tmp/model.flatbin",
        "--rs-p-from-flatbin", "/tmp/v1_5.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "jra-running-style-lgbm-prod-v2",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
        "--phase", "phase2",
        "--sample-limit", "500",
        "--tolerance", "1e-10",
        "--report-dir", "tmp/parity",
    ])
    assert args.features_parquet == "/tmp/feat.parquet"
    assert args.output_parquet == "/tmp/out.parquet"
    assert args.model_flatbin == "/tmp/model.flatbin"
    assert args.rs_p_from_flatbin == "/tmp/v1_5.flatbin"
    assert args.predicted_at == "2026-05-31T00:00:00Z"
    assert args.pg_dsn == "postgresql://u:p@h/db"
    assert args.model_version == "jra-running-style-lgbm-prod-v2"
    assert args.feature_version == "v1"
    assert args.category == "jra"
    assert args.year == "2026"
    assert args.phase == "phase2"
    assert args.sample_limit == 500
    assert args.tolerance == 1e-10
    assert args.report_dir == "tmp/parity"


def test_parse_args_defaults_to_phase2_and_sample_1000() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", "/tmp/out.parquet",
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "jra-running-style-lgbm-prod-v1.5",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
    ])
    assert args.phase == "phase2"
    assert args.sample_limit == 1000
    assert args.tolerance == 1e-12
    assert args.report_dir == "tmp/parity"
    assert args.rs_p_from_flatbin is None


def test_parse_args_accepts_phase1() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", "/tmp/out.parquet",
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
        "--phase", "phase1",
    ])
    assert args.phase == "phase1"


def test_parse_args_accepts_phase3() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", "/tmp/out.parquet",
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
        "--phase", "phase3",
    ])
    assert args.phase == "phase3"


def test_parse_args_rejects_unknown_phase() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--output-parquet", "/tmp/out.parquet",
            "--model-flatbin", "/tmp/model.flatbin",
            "--predicted-at", "2026-05-31T00:00:00Z",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
            "--feature-version", "v1",
            "--category", "jra",
            "--year", "2026",
            "--phase", "phase42",
        ])


def test_parse_args_rejects_unknown_category() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--output-parquet", "/tmp/out.parquet",
            "--model-flatbin", "/tmp/model.flatbin",
            "--predicted-at", "2026-05-31T00:00:00Z",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
            "--feature-version", "v1",
            "--category", "banei",
            "--year", "2026",
        ])


def test_parse_args_requires_features_parquet() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--output-parquet", "/tmp/out.parquet",
            "--model-flatbin", "/tmp/model.flatbin",
            "--predicted-at", "2026-05-31T00:00:00Z",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
            "--feature-version", "v1",
            "--category", "jra",
            "--year", "2026",
        ])


def test_parse_args_requires_output_parquet() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--model-flatbin", "/tmp/model.flatbin",
            "--predicted-at", "2026-05-31T00:00:00Z",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
            "--feature-version", "v1",
            "--category", "jra",
            "--year", "2026",
        ])


def test_parse_args_requires_model_flatbin() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--output-parquet", "/tmp/out.parquet",
            "--predicted-at", "2026-05-31T00:00:00Z",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
            "--feature-version", "v1",
            "--category", "jra",
            "--year", "2026",
        ])


def test_parse_args_requires_predicted_at() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--output-parquet", "/tmp/out.parquet",
            "--model-flatbin", "/tmp/model.flatbin",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
            "--feature-version", "v1",
            "--category", "jra",
            "--year", "2026",
        ])


def test_parse_args_pg_dsn_is_optional() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", "/tmp/out.parquet",
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--model-version", "v",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
    ])
    assert args.pg_dsn is None


def test_parse_args_requires_model_version() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--output-parquet", "/tmp/out.parquet",
            "--model-flatbin", "/tmp/model.flatbin",
            "--predicted-at", "2026-05-31T00:00:00Z",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--feature-version", "v1",
            "--category", "jra",
            "--year", "2026",
        ])


def test_parse_args_requires_feature_version() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--output-parquet", "/tmp/out.parquet",
            "--model-flatbin", "/tmp/model.flatbin",
            "--predicted-at", "2026-05-31T00:00:00Z",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
            "--category", "jra",
            "--year", "2026",
        ])


def test_parse_args_requires_category() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--output-parquet", "/tmp/out.parquet",
            "--model-flatbin", "/tmp/model.flatbin",
            "--predicted-at", "2026-05-31T00:00:00Z",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
            "--feature-version", "v1",
            "--year", "2026",
        ])


def test_parse_args_requires_year() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--output-parquet", "/tmp/out.parquet",
            "--model-flatbin", "/tmp/model.flatbin",
            "--predicted-at", "2026-05-31T00:00:00Z",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
            "--feature-version", "v1",
            "--category", "jra",
        ])


def test_build_sample_keys_sql_is_md5_deterministic_order() -> None:
    sql = subject.build_sample_keys_sql()
    assert "md5(source || kaisai_nen || kaisai_tsukihi || keibajo_code || race_bango)" in sql
    assert "ORDER BY" in sql
    assert "LIMIT %s" in sql
    assert "model_version = %s" in sql
    assert "race_running_style_model_predictions" in sql
    assert "GROUP BY" in sql
    assert "kaisai_nen = %s" in sql


def test_build_prod_predictions_sql_selects_probabilities_and_predicted_class() -> None:
    sql = subject.build_prod_predictions_sql()
    assert "p_nige" in sql
    assert "p_senkou" in sql
    assert "p_sashi" in sql
    assert "p_oikomi" in sql
    assert "predicted_class" in sql
    assert "ketto_toroku_bango" in sql
    assert "model_version = %s" in sql
    assert "kaisai_nen = %s" in sql


def test_md5_sample_sql_is_deterministic_with_same_seed() -> None:
    first = subject.build_sample_keys_sql()
    second = subject.build_sample_keys_sql()
    assert first == second


def test_coerce_race_key_row_returns_5_tuple_of_strings() -> None:
    result = subject.coerce_race_key_row(("jra", "2026", "0103", "05", "01"))
    assert result == ("jra", "2026", "0103", "05", "01")


def test_coerce_race_key_row_stringifies_non_string_values() -> None:
    result = subject.coerce_race_key_row(("jra", 2026, "0103", 5, 1))
    assert result == ("jra", "2026", "0103", "5", "1")


def test_fetch_sample_race_keys_executes_md5_query_with_params() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01"),
        ("jra", "2026", "0203", "30", "10"),
    ]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector = MagicMock(return_value=connection)
    result = subject.fetch_sample_race_keys(
        {"pg_dsn": "dsn", "model_version": "v1.5", "year": "2026", "limit": 1000},
        pg_connector=connector,
    )
    assert result == [
        ("jra", "2026", "0103", "05", "01"),
        ("jra", "2026", "0203", "30", "10"),
    ]
    cursor.execute.assert_called_once_with(
        subject.build_sample_keys_sql(), ("v1.5", "2026", 1000),
    )
    connection.close.assert_called_once()


def test_fetch_sample_race_keys_closes_connection_on_error() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("boom")
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector = MagicMock(return_value=connection)
    with pytest.raises(RuntimeError):
        subject.fetch_sample_race_keys(
            {"pg_dsn": "dsn", "model_version": "v1.5", "year": "2026", "limit": 1000},
            pg_connector=connector,
        )
    connection.close.assert_called_once()


def test_fetch_prod_predictions_returns_dataframe_with_expected_columns() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "2020100001", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector = MagicMock(return_value=connection)
    frame = subject.fetch_prod_predictions(
        {"pg_dsn": "dsn", "model_version": "v1.5", "year": "2026", "limit": 1000},
        pg_connector=connector,
    )
    assert list(frame.columns) == [
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
        "p_nige",
        "p_senkou",
        "p_sashi",
        "p_oikomi",
        "predicted_class",
    ]
    assert len(frame) == 1
    connection.close.assert_called_once()


def test_fetch_prod_predictions_closes_connection_on_error() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("pg down")
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector = MagicMock(return_value=connection)
    with pytest.raises(RuntimeError):
        subject.fetch_prod_predictions(
            {"pg_dsn": "dsn", "model_version": "v1.5", "year": "2026", "limit": 1000},
            pg_connector=connector,
        )
    connection.close.assert_called_once()


def test_assert_probability_sums_to_one_returns_true_when_within_tolerance() -> None:
    frame = pl.DataFrame({
        "p_nige": [0.4],
        "p_senkou": [0.3],
        "p_sashi": [0.2],
        "p_oikomi": [0.1],
    })
    assert subject.assert_probability_sums_to_one(frame, tolerance=1e-12) is True


def test_assert_probability_sums_to_one_returns_false_when_outside_tolerance() -> None:
    frame = pl.DataFrame({
        "p_nige": [0.5],
        "p_senkou": [0.3],
        "p_sashi": [0.2],
        "p_oikomi": [0.5],
    })
    assert subject.assert_probability_sums_to_one(frame, tolerance=1e-12) is False


def test_smoke_mocked_booster_writes_probabilities_with_sums_to_one() -> None:
    booster = MagicMock()
    booster.predict.return_value = np.array([[0.5, 0.25, 0.15, 0.10]])
    probabilities = booster.predict(np.zeros((1, 5)))
    frame = pl.DataFrame({
        "p_nige": probabilities[:, 0],
        "p_senkou": probabilities[:, 1],
        "p_sashi": probabilities[:, 2],
        "p_oikomi": probabilities[:, 3],
    })
    assert subject.assert_probability_sums_to_one(frame, tolerance=1e-12) is True


def test_tolerance_arithmetic_max_diff_per_class() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "p_nige": [0.700000000001],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.099999999999],
        "predicted_class": [0],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["2020100001"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    diffs = subject.compute_max_diff_per_class(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
    )
    assert diffs["p_nige"] > 0.0
    assert diffs["p_senkou"] == 0.0
    assert diffs["p_sashi"] == 0.0
    assert diffs["p_oikomi"] > 0.0


def test_compute_max_diff_per_class_returns_zero_dict_when_no_overlap() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["X"],
        "p_nige": [0.5],
        "p_senkou": [0.2],
        "p_sashi": [0.2],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["Y"],
        "p_nige": [0.5],
        "p_senkou": [0.2],
        "p_sashi": [0.2],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    diffs = subject.compute_max_diff_per_class(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
    )
    assert diffs == {"p_nige": 0.0, "p_senkou": 0.0, "p_sashi": 0.0, "p_oikomi": 0.0}


def test_argmax_agreement_calculation_full_match() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra", "jra"],
        "kaisai_nen": ["2026", "2026"],
        "kaisai_tsukihi": ["0103", "0103"],
        "keibajo_code": ["05", "05"],
        "race_bango": ["01", "02"],
        "ketto_toroku_bango": ["A", "B"],
        "p_nige": [0.7, 0.1],
        "p_senkou": [0.1, 0.7],
        "p_sashi": [0.1, 0.1],
        "p_oikomi": [0.1, 0.1],
        "predicted_class": [0, 1],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra", "jra"],
        "kaisai_nen": ["2026", "2026"],
        "kaisai_tsukihi": ["0103", "0103"],
        "keibajo_code": ["05", "05"],
        "race_bango": ["01", "02"],
        "ketto_toroku_bango": ["A", "B"],
        "p_nige": [0.7, 0.1],
        "p_senkou": [0.1, 0.7],
        "p_sashi": [0.1, 0.1],
        "p_oikomi": [0.1, 0.1],
        "predicted_class": [0, 1],
    })
    agreement = subject.compute_argmax_agreement(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
    )
    assert agreement == 1.0


def test_argmax_agreement_calculation_partial_match() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra", "jra"],
        "kaisai_nen": ["2026", "2026"],
        "kaisai_tsukihi": ["0103", "0103"],
        "keibajo_code": ["05", "05"],
        "race_bango": ["01", "02"],
        "ketto_toroku_bango": ["A", "B"],
        "p_nige": [0.7, 0.1],
        "p_senkou": [0.1, 0.7],
        "p_sashi": [0.1, 0.1],
        "p_oikomi": [0.1, 0.1],
        "predicted_class": [0, 1],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra", "jra"],
        "kaisai_nen": ["2026", "2026"],
        "kaisai_tsukihi": ["0103", "0103"],
        "keibajo_code": ["05", "05"],
        "race_bango": ["01", "02"],
        "ketto_toroku_bango": ["A", "B"],
        "p_nige": [0.7, 0.1],
        "p_senkou": [0.1, 0.7],
        "p_sashi": [0.1, 0.1],
        "p_oikomi": [0.1, 0.1],
        "predicted_class": [0, 2],
    })
    agreement = subject.compute_argmax_agreement(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
    )
    assert agreement == 0.5


def test_compute_argmax_agreement_returns_one_when_no_overlap() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["B"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    agreement = subject.compute_argmax_agreement(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
    )
    assert agreement == 1.0


def test_evaluate_parity_passes_when_zero_diff_and_full_agreement() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    result = subject.evaluate_parity(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
    )
    assert result["passed"] is True
    assert result["rows_compared"] == 1
    assert result["argmax_agreement"] == 1.0


def test_evaluate_parity_fails_when_argmax_disagrees() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [1],
    })
    result = subject.evaluate_parity(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
    )
    assert result["passed"] is False


def test_evaluate_parity_fails_when_diff_exceeds_tolerance() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.700001],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.099999],
        "predicted_class": [0],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    result = subject.evaluate_parity(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
    )
    assert result["passed"] is False


def test_evaluate_parity_fails_when_no_overlap() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["B"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    result = subject.evaluate_parity(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
    )
    assert result["passed"] is False
    assert result["rows_compared"] == 0


def test_phase_2_skip_when_env_not_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PARITY_PG_DSN", raising=False)
    assert subject.is_phase_two_enabled() is False
    assert subject.resolve_phase_two_dsn() == ""


def test_phase_2_runs_when_env_set_mocked_pg_psycopg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARITY_PG_DSN", "postgresql://u:p@h/db")
    assert subject.is_phase_two_enabled() is True
    assert subject.resolve_phase_two_dsn() == "postgresql://u:p@h/db"
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "2020100001", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector = MagicMock(return_value=connection)
    frame = subject.fetch_prod_predictions(
        {
            "pg_dsn": subject.resolve_phase_two_dsn(),
            "model_version": "v1.5",
            "year": "2026",
            "limit": 1000,
        },
        pg_connector=connector,
    )
    assert len(frame) == 1
    connection.close.assert_called_once()


def test_resolve_pg_dsn_prefers_explicit_over_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARITY_PG_DSN", "postgresql://env:env@h/db")
    assert subject.resolve_pg_dsn("postgresql://explicit:explicit@h/db") == (
        "postgresql://explicit:explicit@h/db"
    )


def test_resolve_pg_dsn_falls_back_to_env_when_explicit_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARITY_PG_DSN", "postgresql://env:env@h/db")
    assert subject.resolve_pg_dsn(None) == "postgresql://env:env@h/db"


def test_resolve_pg_dsn_falls_back_to_env_when_explicit_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARITY_PG_DSN", "postgresql://env:env@h/db")
    assert subject.resolve_pg_dsn("") == "postgresql://env:env@h/db"


def test_resolve_pg_dsn_raises_when_neither_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PARITY_PG_DSN", raising=False)
    with pytest.raises(ValueError):
        subject.resolve_pg_dsn(None)


def test_collect_mismatches_returns_only_rows_above_threshold() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra", "jra"],
        "kaisai_nen": ["2026", "2026"],
        "kaisai_tsukihi": ["0103", "0103"],
        "keibajo_code": ["05", "05"],
        "race_bango": ["01", "02"],
        "ketto_toroku_bango": ["A", "B"],
        "p_nige": [0.7, 0.500001],
        "p_senkou": [0.1, 0.2],
        "p_sashi": [0.1, 0.2],
        "p_oikomi": [0.1, 0.099999],
        "predicted_class": [0, 0],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra", "jra"],
        "kaisai_nen": ["2026", "2026"],
        "kaisai_tsukihi": ["0103", "0103"],
        "keibajo_code": ["05", "05"],
        "race_bango": ["01", "02"],
        "ketto_toroku_bango": ["A", "B"],
        "p_nige": [0.7, 0.5],
        "p_senkou": [0.1, 0.2],
        "p_sashi": [0.1, 0.2],
        "p_oikomi": [0.1, 0.1],
        "predicted_class": [0, 0],
    })
    mismatches = subject.collect_mismatches(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
        max_diff_threshold=1e-9,
    )
    assert len(mismatches) == 1
    assert mismatches[0, "ketto_toroku_bango"] == "B"


def test_collect_mismatches_returns_empty_when_no_overlap() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    prod_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["B"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [0],
    })
    mismatches = subject.collect_mismatches(
        {"local_frame": local_frame, "prod_frame": prod_frame, "tolerance": 1e-12},
        max_diff_threshold=1e-9,
    )
    assert len(mismatches) == 0
    assert list(mismatches.columns) == [
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
        "max_diff",
    ]


def test_build_phase_three_report_passes_when_within_thresholds() -> None:
    report = subject.build_phase_three_report(
        parity={
            "rows_compared": 1000,
            "max_diff_per_class": {"p_nige": 0.0, "p_senkou": 0.0, "p_sashi": 0.0, "p_oikomi": 0.0},
            "argmax_agreement": 1.0,
            "passed": True,
        },
        model_version="jra-running-style-lgbm-prod-v1.5",
        mismatches_count=0,
        generated_at_utc="20260531T000000Z",
        max_diff_threshold=1e-9,
        agreement_threshold=0.999,
    )
    assert report["passed"] is True
    assert report["mismatches_count"] == 0
    assert report["rows_compared"] == 1000


def test_build_phase_three_report_fails_when_agreement_below_threshold() -> None:
    report = subject.build_phase_three_report(
        parity={
            "rows_compared": 1000,
            "max_diff_per_class": {"p_nige": 0.0, "p_senkou": 0.0, "p_sashi": 0.0, "p_oikomi": 0.0},
            "argmax_agreement": 0.99,
            "passed": False,
        },
        model_version="jra-running-style-lgbm-prod-v1.5",
        mismatches_count=10,
        generated_at_utc="20260531T000000Z",
        max_diff_threshold=1e-9,
        agreement_threshold=0.999,
    )
    assert report["passed"] is False


def test_build_phase_three_report_fails_when_max_diff_above_threshold() -> None:
    report = subject.build_phase_three_report(
        parity={
            "rows_compared": 1000,
            "max_diff_per_class": {"p_nige": 1e-5, "p_senkou": 0.0, "p_sashi": 0.0, "p_oikomi": 0.0},
            "argmax_agreement": 1.0,
            "passed": False,
        },
        model_version="jra-running-style-lgbm-prod-v1.5",
        mismatches_count=1,
        generated_at_utc="20260531T000000Z",
        max_diff_threshold=1e-9,
        agreement_threshold=0.999,
    )
    assert report["passed"] is False


def test_resolve_report_paths_uses_model_version_subdirectory(tmp_path: Path) -> None:
    json_path, parquet_path = subject.resolve_report_paths(
        report_dir=tmp_path.as_posix(),
        model_version="jra-running-style-lgbm-prod-v1.5",
        generated_at_utc="20260531T000000Z",
    )
    assert json_path == tmp_path / "jra-running-style-lgbm-prod-v1.5" / "20260531T000000Z.json"
    assert parquet_path == tmp_path / "jra-running-style-lgbm-prod-v1.5" / "mismatches.parquet"


def test_phase_3_writes_json_report_with_required_fields(tmp_path: Path) -> None:
    report = subject.PhaseThreeReport(
        generated_at_utc="20260531T000000Z",
        model_version="jra-running-style-lgbm-prod-v1.5",
        rows_compared=875,
        max_diff_per_class={"p_nige": 0.0, "p_senkou": 0.0, "p_sashi": 0.0, "p_oikomi": 0.0},
        argmax_agreement=1.0,
        max_diff_threshold=1e-9,
        agreement_threshold=0.999,
        mismatches_count=0,
        passed=True,
    )
    json_path, parquet_path = subject.write_phase_three_artifacts(
        report=report,
        mismatches=pl.DataFrame(
            schema=["source", "kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango", "ketto_toroku_bango", "max_diff"],
        ),
        report_dir=tmp_path.as_posix(),
    )
    assert parquet_path is None
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["model_version"] == "jra-running-style-lgbm-prod-v1.5"
    assert payload["generated_at_utc"] == "20260531T000000Z"
    assert payload["rows_compared"] == 875
    assert payload["argmax_agreement"] == 1.0
    assert payload["mismatches_count"] == 0
    assert payload["passed"] is True


def test_phase_3_dumps_mismatches_parquet_when_diff_above_threshold(tmp_path: Path) -> None:
    report = subject.PhaseThreeReport(
        generated_at_utc="20260531T000000Z",
        model_version="jra-running-style-lgbm-prod-v1.5",
        rows_compared=2,
        max_diff_per_class={"p_nige": 1e-5, "p_senkou": 0.0, "p_sashi": 0.0, "p_oikomi": 0.0},
        argmax_agreement=1.0,
        max_diff_threshold=1e-9,
        agreement_threshold=0.999,
        mismatches_count=1,
        passed=False,
    )
    mismatches = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["02"],
        "ketto_toroku_bango": ["B"],
        "max_diff": [1e-5],
    })
    json_path, parquet_path = subject.write_phase_three_artifacts(
        report=report, mismatches=mismatches, report_dir=tmp_path.as_posix(),
    )
    assert parquet_path is not None
    assert parquet_path.is_file()
    loaded = pl.read_parquet(parquet_path)
    assert len(loaded) == 1
    assert json_path.is_file()


def test_phase_3_no_mismatches_dump_when_diff_zero(tmp_path: Path) -> None:
    report = subject.PhaseThreeReport(
        generated_at_utc="20260531T000000Z",
        model_version="jra-running-style-lgbm-prod-v1.5",
        rows_compared=1,
        max_diff_per_class={"p_nige": 0.0, "p_senkou": 0.0, "p_sashi": 0.0, "p_oikomi": 0.0},
        argmax_agreement=1.0,
        max_diff_threshold=1e-9,
        agreement_threshold=0.999,
        mismatches_count=0,
        passed=True,
    )
    json_path, parquet_path = subject.write_phase_three_artifacts(
        report=report,
        mismatches=pl.DataFrame(
            schema=["source", "kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango", "ketto_toroku_bango", "max_diff"],
        ),
        report_dir=tmp_path.as_posix(),
    )
    assert parquet_path is None
    assert (tmp_path / "jra-running-style-lgbm-prod-v1.5" / "mismatches.parquet").is_file() is False
    assert json_path.is_file()


def test_default_pg_connector_invokes_psycopg_connect() -> None:
    fake_psycopg = MagicMock()
    fake_connection = MagicMock()
    fake_psycopg.connect.return_value = fake_connection
    with patch.dict(sys.modules, {"psycopg": fake_psycopg}):
        result = subject.default_pg_connector("postgresql://u:p@h/db")
    assert result is fake_connection
    fake_psycopg.connect.assert_called_once_with("postgresql://u:p@h/db")


def test_build_w3_command_without_rs_p_from_flatbin() -> None:
    cmd = subject.build_w3_command(
        features_parquet="/tmp/feat.parquet",
        output_parquet="/tmp/out.parquet",
        model_flatbin="/tmp/model.flatbin",
        category="jra",
        model_version="jra-running-style-lgbm-prod-v1.5",
        feature_version="v1",
        predicted_at="2026-05-31T00:00:00Z",
        rs_p_from_flatbin=None,
    )
    assert cmd[0] == "bun"
    assert cmd[1] == "run"
    assert cmd[2] == subject.W3_INFERENCE_SCRIPT_PATH
    assert "--rs-p-from-flatbin" not in cmd
    assert "--model-flatbin" in cmd
    assert "--features-parquet" in cmd
    assert "--output-parquet" in cmd
    assert "--category" in cmd
    assert "--model-version" in cmd
    assert "--feature-version" in cmd
    assert "--predicted-at" in cmd


def test_build_w3_command_includes_rs_p_from_flatbin_when_provided() -> None:
    cmd = subject.build_w3_command(
        features_parquet="/tmp/feat.parquet",
        output_parquet="/tmp/out.parquet",
        model_flatbin="/tmp/model_v2.flatbin",
        category="jra",
        model_version="jra-running-style-lgbm-prod-v2",
        feature_version="v1",
        predicted_at="2026-05-31T00:00:00Z",
        rs_p_from_flatbin="/tmp/v1_5.flatbin",
    )
    assert "--rs-p-from-flatbin" in cmd
    rs_p_index = cmd.index("--rs-p-from-flatbin")
    assert cmd[rs_p_index + 1] == "/tmp/v1_5.flatbin"


def test_default_local_inference_runner_invokes_runner_with_w3_args() -> None:
    runner = MagicMock()
    runner.return_value = MagicMock(returncode=0, stdout="", stderr="")
    subject.default_local_inference_runner(
        features_parquet="/tmp/feat.parquet",
        output_parquet="/tmp/out.parquet",
        model_flatbin="/tmp/model.flatbin",
        category="jra",
        model_version="jra-running-style-lgbm-prod-v1.5",
        feature_version="v1",
        predicted_at="2026-05-31T00:00:00Z",
        rs_p_from_flatbin=None,
        runner=runner,
    )
    runner.assert_called_once()
    invoked_args = runner.call_args.args[0]
    assert invoked_args[0] == "bun"
    assert invoked_args[2] == subject.W3_INFERENCE_SCRIPT_PATH
    assert "--model-flatbin" in invoked_args
    assert "--features-parquet" in invoked_args
    assert "--output-parquet" in invoked_args
    assert "--category" in invoked_args
    assert "--model-version" in invoked_args
    assert "--feature-version" in invoked_args
    assert "--predicted-at" in invoked_args
    assert runner.call_args.kwargs["check"] is True
    assert runner.call_args.kwargs["capture_output"] is True
    assert runner.call_args.kwargs["text"] is True


def test_default_local_inference_runner_passes_rs_p_from_flatbin_when_v2() -> None:
    runner = MagicMock()
    runner.return_value = MagicMock(returncode=0, stdout="", stderr="")
    subject.default_local_inference_runner(
        features_parquet="/tmp/feat.parquet",
        output_parquet="/tmp/out.parquet",
        model_flatbin="/tmp/model_v2.flatbin",
        category="jra",
        model_version="jra-running-style-lgbm-prod-v2",
        feature_version="v1",
        predicted_at="2026-05-31T00:00:00Z",
        rs_p_from_flatbin="/tmp/v1_5.flatbin",
        runner=runner,
    )
    invoked_args = runner.call_args.args[0]
    assert "--rs-p-from-flatbin" in invoked_args


def test_utc_now_iso_format_is_z_suffixed() -> None:
    value = subject.utc_now_iso()
    assert value.endswith("Z")
    assert "T" in value
    assert len(value) == 16


def test_filter_features_by_race_keys_inner_joins_on_race_key() -> None:
    features = pl.DataFrame({
        "source": ["jra", "jra", "jra"],
        "kaisai_nen": ["2026", "2026", "2026"],
        "kaisai_tsukihi": ["0103", "0103", "0203"],
        "keibajo_code": ["05", "05", "05"],
        "race_bango": ["01", "02", "01"],
        "ketto_toroku_bango": ["A", "B", "C"],
        "career_win_rate": [0.1, 0.2, 0.3],
    })
    sample = [
        ("jra", "2026", "0103", "05", "01"),
        ("jra", "2026", "0203", "05", "01"),
    ]
    filtered = subject.filter_features_by_race_keys(features, sample)
    assert len(filtered) == 2
    assert set(filtered["ketto_toroku_bango"]) == {"A", "C"}


def test_filter_features_by_race_keys_returns_empty_when_no_sample() -> None:
    features = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
    })
    filtered = subject.filter_features_by_race_keys(features, [])
    assert len(filtered) == 0


def test_default_parquet_writer_writes_parquet(tmp_path: Path) -> None:
    frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
    })
    target = (tmp_path / "out.parquet").as_posix()
    subject.default_parquet_writer(frame, target)
    loaded = pl.read_parquet(target)
    assert len(loaded) == 1


def test_derive_predicted_class_adds_argmax_when_missing() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
    })
    augmented = subject.derive_predicted_class(local_frame)
    assert "predicted_class" in augmented.columns
    assert augmented[0, "predicted_class"] == 0


def test_derive_predicted_class_preserves_existing_column() -> None:
    local_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.1],
        "p_senkou": [0.7],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "predicted_class": [99],
    })
    augmented = subject.derive_predicted_class(local_frame)
    assert augmented[0, "predicted_class"] == 99


def test_coerce_phase_two_args_uses_explicit_pg_dsn() -> None:
    parsed = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", "/tmp/out.parquet",
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
    ])
    coerced = subject.coerce_phase_two_args(parsed)
    assert coerced["pg_dsn"] == "postgresql://u:p@h/db"
    assert coerced["features_parquet"] == "/tmp/feat.parquet"
    assert coerced["output_parquet"] == "/tmp/out.parquet"
    assert coerced["model_flatbin"] == "/tmp/model.flatbin"
    assert coerced["rs_p_from_flatbin"] is None
    assert coerced["category"] == "jra"
    assert coerced["year"] == "2026"


def test_coerce_phase_two_args_passes_rs_p_from_flatbin() -> None:
    parsed = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", "/tmp/out.parquet",
        "--model-flatbin", "/tmp/model_v2.flatbin",
        "--rs-p-from-flatbin", "/tmp/v1_5.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "jra-running-style-lgbm-prod-v2",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
    ])
    coerced = subject.coerce_phase_two_args(parsed)
    assert coerced["rs_p_from_flatbin"] == "/tmp/v1_5.flatbin"


def test_run_phase_one_returns_smoke_message() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", "/tmp/out.parquet",
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
        "--phase", "phase1",
    ])
    outcome = subject.run(
        args,
        deps={
            "pg_connector": MagicMock(),
            "local_inference_runner": MagicMock(),
            "parquet_reader": MagicMock(),
            "parquet_writer": MagicMock(),
            "subprocess_runner": MagicMock(),
            "clock_iso": lambda: "20260531T000000Z",
        },
    )
    assert outcome["phase"] == "phase1"
    assert "smoke" in cast(str, outcome["message"])


def test_run_phase_two_e2e_passes_when_local_matches_prod(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", (tmp_path / "out.parquet").as_posix(),
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
        "--phase", "phase2",
    ])
    features = pl.DataFrame({
        "source": ["jra", "jra"],
        "kaisai_nen": ["2026", "2025"],
        "kaisai_tsukihi": ["0103", "0103"],
        "keibajo_code": ["05", "05"],
        "race_bango": ["01", "01"],
        "ketto_toroku_bango": ["A", "Z"],
        "career_win_rate": [0.1, 0.5],
    })
    output_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
        "model_version": ["v1.5"],
        "running_style_feature_version": ["v1"],
    })
    sample_cursor = MagicMock()
    sample_cursor.fetchall.return_value = [("jra", "2026", "0103", "05", "01")]
    prod_cursor = MagicMock()
    prod_cursor.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "A", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    connections = [MagicMock(), MagicMock()]
    connections[0].cursor.return_value = sample_cursor
    connections[1].cursor.return_value = prod_cursor
    connector = MagicMock(side_effect=connections)
    parquet_reader = MagicMock(side_effect=[features, output_frame])
    parquet_writer = MagicMock()
    local_runner = MagicMock()
    outcome = subject.run(
        args,
        deps={
            "pg_connector": connector,
            "local_inference_runner": local_runner,
            "parquet_reader": parquet_reader,
            "parquet_writer": parquet_writer,
            "subprocess_runner": MagicMock(),
            "clock_iso": lambda: "20260531T000000Z",
        },
    )
    parity_result = cast(subject.ParityResult, outcome["result"])
    assert outcome["phase"] == "phase2"
    assert parity_result["passed"] is True
    assert parity_result["rows_compared"] == 1
    local_runner.assert_called_once()
    assert local_runner.call_args.kwargs["model_flatbin"] == "/tmp/model.flatbin"
    assert local_runner.call_args.kwargs["category"] == "jra"
    parquet_writer.assert_called_once()


def test_run_phase_two_fails_when_diff_exceeds_tolerance(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", (tmp_path / "out.parquet").as_posix(),
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
        "--phase", "phase2",
    ])
    features = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "career_win_rate": [0.1],
    })
    output_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.700001],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.099999],
    })
    sample_cursor = MagicMock()
    sample_cursor.fetchall.return_value = [("jra", "2026", "0103", "05", "01")]
    prod_cursor = MagicMock()
    prod_cursor.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "A", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    connections = [MagicMock(), MagicMock()]
    connections[0].cursor.return_value = sample_cursor
    connections[1].cursor.return_value = prod_cursor
    connector = MagicMock(side_effect=connections)
    outcome = subject.run(
        args,
        deps={
            "pg_connector": connector,
            "local_inference_runner": MagicMock(),
            "parquet_reader": MagicMock(side_effect=[features, output_frame]),
            "parquet_writer": MagicMock(),
            "subprocess_runner": MagicMock(),
            "clock_iso": lambda: "20260531T000000Z",
        },
    )
    parity_result = cast(subject.ParityResult, outcome["result"])
    assert outcome["phase"] == "phase2"
    assert parity_result["passed"] is False


def test_run_phase_three_writes_json_report_and_no_mismatches(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", (tmp_path / "out.parquet").as_posix(),
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
        "--phase", "phase3",
        "--report-dir", tmp_path.as_posix(),
    ])
    features = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "career_win_rate": [0.1],
    })
    output_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.7],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.1],
    })
    sample_cursor = MagicMock()
    sample_cursor.fetchall.return_value = [("jra", "2026", "0103", "05", "01")]
    prod_cursor_phase2 = MagicMock()
    prod_cursor_phase2.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "A", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    prod_cursor_phase3 = MagicMock()
    prod_cursor_phase3.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "A", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    connections = [MagicMock(), MagicMock(), MagicMock()]
    connections[0].cursor.return_value = sample_cursor
    connections[1].cursor.return_value = prod_cursor_phase2
    connections[2].cursor.return_value = prod_cursor_phase3
    connector = MagicMock(side_effect=connections)
    outcome = subject.run(
        args,
        deps={
            "pg_connector": connector,
            "local_inference_runner": MagicMock(),
            "parquet_reader": MagicMock(side_effect=[features, output_frame, output_frame]),
            "parquet_writer": MagicMock(),
            "subprocess_runner": MagicMock(),
            "clock_iso": lambda: "20260531T000000Z",
        },
    )
    assert outcome["phase"] == "phase3"
    assert outcome["mismatches_path"] is None
    report_path = Path(cast(str, outcome["report_path"]))
    assert report_path.is_file()


def test_run_phase_three_dumps_mismatches_when_diff_exceeds_threshold(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", (tmp_path / "out.parquet").as_posix(),
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
        "--phase", "phase3",
        "--report-dir", tmp_path.as_posix(),
    ])
    features = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "career_win_rate": [0.1],
    })
    output_frame = pl.DataFrame({
        "source": ["jra"],
        "kaisai_nen": ["2026"],
        "kaisai_tsukihi": ["0103"],
        "keibajo_code": ["05"],
        "race_bango": ["01"],
        "ketto_toroku_bango": ["A"],
        "p_nige": [0.700001],
        "p_senkou": [0.1],
        "p_sashi": [0.1],
        "p_oikomi": [0.099999],
    })
    sample_cursor = MagicMock()
    sample_cursor.fetchall.return_value = [("jra", "2026", "0103", "05", "01")]
    prod_cursor_phase2 = MagicMock()
    prod_cursor_phase2.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "A", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    prod_cursor_phase3 = MagicMock()
    prod_cursor_phase3.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "A", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    connections = [MagicMock(), MagicMock(), MagicMock()]
    connections[0].cursor.return_value = sample_cursor
    connections[1].cursor.return_value = prod_cursor_phase2
    connections[2].cursor.return_value = prod_cursor_phase3
    connector = MagicMock(side_effect=connections)
    outcome = subject.run(
        args,
        deps={
            "pg_connector": connector,
            "local_inference_runner": MagicMock(),
            "parquet_reader": MagicMock(side_effect=[features, output_frame, output_frame]),
            "parquet_writer": MagicMock(),
            "subprocess_runner": MagicMock(),
            "clock_iso": lambda: "20260531T000000Z",
        },
    )
    assert outcome["phase"] == "phase3"
    assert outcome["mismatches_path"] is not None
    mismatches_path = Path(cast(str, outcome["mismatches_path"]))
    assert mismatches_path.is_file()


def test_main_invokes_default_runner_and_prints_json_outcome(
    capsys: pytest.CaptureFixture[str],
) -> None:
    argv = [
        "--features-parquet", "/tmp/feat.parquet",
        "--output-parquet", "/tmp/out.parquet",
        "--model-flatbin", "/tmp/model.flatbin",
        "--predicted-at", "2026-05-31T00:00:00Z",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--feature-version", "v1",
        "--category", "jra",
        "--year", "2026",
        "--phase", "phase1",
    ]
    subject.main(argv)
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["phase"] == "phase1"


def test_phase_two_env_gate_skips_when_dsn_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARITY_PG_DSN", "")
    assert subject.is_phase_two_enabled() is False


def test_phase_two_env_gate_reads_dsn_from_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARITY_PG_DSN", "postgresql://x:y@h/db")
    assert subject.resolve_phase_two_dsn() == "postgresql://x:y@h/db"


def test_module_constants_are_stable() -> None:
    assert subject.PROBABILITY_COLUMNS == ("p_nige", "p_senkou", "p_sashi", "p_oikomi")
    assert subject.PREDICTED_CLASS_COLUMN == "predicted_class"
    assert subject.RACE_KEY_COLUMNS == (
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
    )
    assert subject.DEFAULT_SAMPLE_LIMIT == 1000
    assert subject.DEFAULT_TOLERANCE_PHASE2 == 1e-12
    assert subject.DEFAULT_TOLERANCE_PHASE3_MAX_DIFF == 1e-9
    assert subject.DEFAULT_TOLERANCE_PHASE3_AGREEMENT == 0.999
    assert subject.ENV_PARITY_PG_DSN == "PARITY_PG_DSN"
    assert subject.SUPPORTED_CATEGORIES == ("jra", "nar")
    assert subject.W3_INFERENCE_SCRIPT_PATH == (
        "apps/sync-realtime-data/src/scripts/run-running-style-inference-local.ts"
    )


def test_environment_variable_default_is_empty_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PARITY_PG_DSN", raising=False)
    assert os.environ.get("PARITY_PG_DSN", "") == ""
