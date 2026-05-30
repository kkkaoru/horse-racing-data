"""Tests for verify_running_style_inference_parity (Agent W5, parity framework)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

import verify_running_style_inference_parity as subject


def test_parse_args_full_set() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--predictions-parquet", "/tmp/preds.parquet",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "jra-running-style-lgbm-prod-v1.5",
        "--phase", "phase2",
        "--sample-limit", "500",
        "--tolerance", "1e-10",
        "--report-dir", "tmp/parity",
    ])
    assert args.features_parquet == "/tmp/feat.parquet"
    assert args.predictions_parquet == "/tmp/preds.parquet"
    assert args.pg_dsn == "postgresql://u:p@h/db"
    assert args.model_version == "jra-running-style-lgbm-prod-v1.5"
    assert args.phase == "phase2"
    assert args.sample_limit == 500
    assert args.tolerance == 1e-10
    assert args.report_dir == "tmp/parity"


def test_parse_args_defaults_to_phase2_and_sample_1000() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--predictions-parquet", "/tmp/preds.parquet",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "jra-running-style-lgbm-prod-v1.5",
    ])
    assert args.phase == "phase2"
    assert args.sample_limit == 1000
    assert args.tolerance == 1e-12
    assert args.report_dir == "tmp/parity"


def test_parse_args_accepts_phase1() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--predictions-parquet", "/tmp/preds.parquet",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "jra-running-style-lgbm-prod-v1.5",
        "--phase", "phase1",
    ])
    assert args.phase == "phase1"


def test_parse_args_accepts_phase3() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--predictions-parquet", "/tmp/preds.parquet",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "jra-running-style-lgbm-prod-v1.5",
        "--phase", "phase3",
    ])
    assert args.phase == "phase3"


def test_parse_args_rejects_unknown_phase() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--predictions-parquet", "/tmp/preds.parquet",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
            "--phase", "phase42",
        ])


def test_parse_args_requires_features_parquet() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--predictions-parquet", "/tmp/preds.parquet",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
        ])


def test_parse_args_requires_predictions_parquet() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--pg-dsn", "postgresql://u:p@h/db",
            "--model-version", "v",
        ])


def test_parse_args_requires_pg_dsn() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--predictions-parquet", "/tmp/preds.parquet",
            "--model-version", "v",
        ])


def test_parse_args_requires_model_version() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat.parquet",
            "--predictions-parquet", "/tmp/preds.parquet",
            "--pg-dsn", "postgresql://u:p@h/db",
        ])


def test_build_sample_keys_sql_is_md5_deterministic_order() -> None:
    sql = subject.build_sample_keys_sql()
    assert "md5(source || kaisai_nen || kaisai_tsukihi || keibajo_code || race_bango)" in sql
    assert "ORDER BY" in sql
    assert "LIMIT %s" in sql
    assert "model_version = %s" in sql
    assert "race_running_style_model_predictions" in sql
    assert "GROUP BY" in sql


def test_build_prod_predictions_sql_selects_probabilities_and_predicted_class() -> None:
    sql = subject.build_prod_predictions_sql()
    assert "p_nige" in sql
    assert "p_senkou" in sql
    assert "p_sashi" in sql
    assert "p_oikomi" in sql
    assert "predicted_class" in sql
    assert "ketto_toroku_bango" in sql
    assert "model_version = %s" in sql


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
        ("nar", "2026", "0203", "30", "10"),
    ]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector = MagicMock(return_value=connection)
    result = subject.fetch_sample_race_keys(
        {"pg_dsn": "dsn", "model_version": "v1.5", "limit": 1000},
        pg_connector=connector,
    )
    assert result == [
        ("jra", "2026", "0103", "05", "01"),
        ("nar", "2026", "0203", "30", "10"),
    ]
    cursor.execute.assert_called_once_with(
        subject.build_sample_keys_sql(), ("v1.5", 1000),
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
            {"pg_dsn": "dsn", "model_version": "v1.5", "limit": 1000},
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
        {"pg_dsn": "dsn", "model_version": "v1.5", "limit": 1000},
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
            {"pg_dsn": "dsn", "model_version": "v1.5", "limit": 1000},
            pg_connector=connector,
        )
    connection.close.assert_called_once()


def test_assert_probability_sums_to_one_returns_true_when_within_tolerance() -> None:
    frame = pd.DataFrame({
        "p_nige": [0.4],
        "p_senkou": [0.3],
        "p_sashi": [0.2],
        "p_oikomi": [0.1],
    })
    assert subject.assert_probability_sums_to_one(frame, tolerance=1e-12) is True


def test_assert_probability_sums_to_one_returns_false_when_outside_tolerance() -> None:
    frame = pd.DataFrame({
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
    frame = pd.DataFrame({
        "p_nige": probabilities[:, 0],
        "p_senkou": probabilities[:, 1],
        "p_sashi": probabilities[:, 2],
        "p_oikomi": probabilities[:, 3],
    })
    assert subject.assert_probability_sums_to_one(frame, tolerance=1e-12) is True


def test_tolerance_arithmetic_max_diff_per_class() -> None:
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
        {"pg_dsn": subject.resolve_phase_two_dsn(), "model_version": "v1.5", "limit": 1000},
        pg_connector=connector,
    )
    assert len(frame) == 1
    connection.close.assert_called_once()


def test_collect_mismatches_returns_only_rows_above_threshold() -> None:
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
    assert mismatches.iloc[0]["ketto_toroku_bango"] == "B"


def test_collect_mismatches_returns_empty_when_no_overlap() -> None:
    local_frame = pd.DataFrame({
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
    prod_frame = pd.DataFrame({
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
        mismatches=pd.DataFrame(
            columns=["source", "kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango", "ketto_toroku_bango", "max_diff"],
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
    mismatches = pd.DataFrame({
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
    loaded = pd.read_parquet(parquet_path)
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
        mismatches=pd.DataFrame(
            columns=["source", "kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango", "ketto_toroku_bango", "max_diff"],
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


def test_default_local_inference_runner_invokes_subprocess_run() -> None:
    with patch("subprocess.run") as run_mock:
        subject.default_local_inference_runner(
            features_parquet="/tmp/feat.parquet",
            output_parquet="/tmp/preds.parquet",
            model_version="jra-running-style-lgbm-prod-v1.5",
        )
    run_mock.assert_called_once()
    invoked_args = run_mock.call_args.args[0]
    assert invoked_args[0] == "bun"
    assert "--features-parquet" in invoked_args
    assert "--output-parquet" in invoked_args
    assert "--model-version" in invoked_args


def test_utc_now_iso_format_is_z_suffixed() -> None:
    value = subject.utc_now_iso()
    assert value.endswith("Z")
    assert "T" in value
    assert len(value) == 16


def test_run_phase_one_returns_smoke_message() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--predictions-parquet", "/tmp/preds.parquet",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--phase", "phase1",
    ])
    outcome = subject.run(
        args,
        pg_connector=MagicMock(),
        local_inference_runner=MagicMock(),
        pandas_reader=MagicMock(),
        clock_iso=lambda: "20260531T000000Z",
    )
    assert outcome["phase"] == "phase1"
    assert "smoke" in cast(str, outcome["message"])


def test_run_phase_two_invokes_local_inference_then_pg_fetch_then_evaluate() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--predictions-parquet", "/tmp/preds.parquet",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--phase", "phase2",
    ])
    local_frame = pd.DataFrame({
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
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "A", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector = MagicMock(return_value=connection)
    local_runner = MagicMock()
    pandas_reader = MagicMock(return_value=local_frame)
    outcome = subject.run(
        args,
        pg_connector=connector,
        local_inference_runner=local_runner,
        pandas_reader=pandas_reader,
        clock_iso=lambda: "20260531T000000Z",
    )
    local_runner.assert_called_once_with(
        features_parquet="/tmp/feat.parquet",
        output_parquet="/tmp/preds.parquet",
        model_version="v1.5",
    )
    pandas_reader.assert_called_once_with("/tmp/preds.parquet")
    parity_result = cast(subject.ParityResult, outcome["result"])
    assert outcome["phase"] == "phase2"
    assert parity_result["passed"] is True


def test_run_phase_three_writes_json_report_and_no_mismatches(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--predictions-parquet", "/tmp/preds.parquet",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--phase", "phase3",
        "--report-dir", tmp_path.as_posix(),
    ])
    local_frame = pd.DataFrame({
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
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "A", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector = MagicMock(return_value=connection)
    outcome = subject.run(
        args,
        pg_connector=connector,
        local_inference_runner=MagicMock(),
        pandas_reader=MagicMock(return_value=local_frame),
        clock_iso=lambda: "20260531T000000Z",
    )
    assert outcome["phase"] == "phase3"
    assert outcome["mismatches_path"] is None
    report_path = Path(cast(str, outcome["report_path"]))
    assert report_path.is_file()


def test_run_phase_three_dumps_mismatches_when_diff_exceeds_threshold(tmp_path: Path) -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat.parquet",
        "--predictions-parquet", "/tmp/preds.parquet",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
        "--phase", "phase3",
        "--report-dir", tmp_path.as_posix(),
    ])
    local_frame = pd.DataFrame({
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
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("jra", "2026", "0103", "05", "01", "A", 0.7, 0.1, 0.1, 0.1, 0),
    ]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector = MagicMock(return_value=connection)
    outcome = subject.run(
        args,
        pg_connector=connector,
        local_inference_runner=MagicMock(),
        pandas_reader=MagicMock(return_value=local_frame),
        clock_iso=lambda: "20260531T000000Z",
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
        "--predictions-parquet", "/tmp/preds.parquet",
        "--pg-dsn", "postgresql://u:p@h/db",
        "--model-version", "v1.5",
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


def test_environment_variable_default_is_empty_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PARITY_PG_DSN", raising=False)
    assert os.environ.get("PARITY_PG_DSN", "") == ""
