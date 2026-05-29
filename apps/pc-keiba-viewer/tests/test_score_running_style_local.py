"""Tests for score_running_style_local (Phase B)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

import score_running_style_local as subject


def test_parse_args_minimum_required() -> None:
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat",
        "--model-version", "running-style-jra-v7",
        "--output-parquet", "/tmp/out",
        "--running-style-feature-version", "v1",
        "--pg-url", "postgres://x",
        "--category", "jra",
    ])
    assert args.model_version == "running-style-jra-v7"


def test_parse_args_invalid_category_rejected() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args([
            "--features-parquet", "/tmp/feat",
            "--model-version", "m",
            "--output-parquet", "/tmp/out",
            "--running-style-feature-version", "v1",
            "--pg-url", "postgres://x",
            "--category", "bogus",
        ])


def test_build_active_model_query_uses_active_table() -> None:
    sql = subject.build_active_model_query("jra")
    assert "running_style_active_models" in sql


def test_build_active_model_query_embeds_category_literal() -> None:
    sql = subject.build_active_model_query("ban-ei")
    assert "where category = 'ban-ei'" in sql


def test_resolve_active_model_returns_version_and_path() -> None:
    psql_runner = MagicMock(
        return_value='{"model_version": "running-style-jra-v7", "artifact_path": "/p/model.txt"}',
    )
    version, path = subject.resolve_active_model(
        psql_runner=psql_runner, pg_url="postgres://x", category="jra",
    )
    assert version == "running-style-jra-v7"
    assert path == "/p/model.txt"


def test_resolve_active_model_raises_when_empty() -> None:
    psql_runner = MagicMock(return_value="")
    with pytest.raises(RuntimeError):
        subject.resolve_active_model(
            psql_runner=psql_runner, pg_url="postgres://x", category="jra",
        )


def test_resolve_active_model_rejects_non_string_payload() -> None:
    psql_runner = MagicMock(return_value='{"model_version": 123, "artifact_path": null}')
    with pytest.raises(RuntimeError):
        subject.resolve_active_model(
            psql_runner=psql_runner, pg_url="postgres://x", category="jra",
        )


def test_run_psql_raises_when_subprocess_fails() -> None:
    fake_result = MagicMock()
    fake_result.returncode = 1
    fake_result.stderr = "boom"
    fake_result.stdout = ""
    with patch("subprocess.run", return_value=fake_result):
        with pytest.raises(RuntimeError):
            subject.run_psql("postgres://x", "select 1")


def test_run_psql_returns_trimmed_stdout_on_success() -> None:
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stderr = ""
    fake_result.stdout = "abc\n"
    with patch("subprocess.run", return_value=fake_result):
        assert subject.run_psql("postgres://x", "select 1") == "abc"


def test_build_label_series_maps_argmax_to_class_labels() -> None:
    probs = np.array([[0.7, 0.1, 0.1, 0.1], [0.1, 0.1, 0.7, 0.1]])
    labels = subject.build_label_series(probs)
    assert labels == ["nige", "sashi"]


def test_attach_probability_columns_writes_four_columns() -> None:
    frame = pd.DataFrame({"x": [0, 0]})
    probs = np.array([[0.4, 0.3, 0.2, 0.1], [0.1, 0.2, 0.3, 0.4]])
    result = subject.attach_probability_columns(frame, probs)
    assert list(result["p_nige"]) == [0.4, 0.1]
    assert list(result["p_oikomi"]) == [0.1, 0.4]


def test_attach_label_and_versions_sets_metadata_columns() -> None:
    frame = pd.DataFrame({"x": [0]})
    probs = np.array([[0.6, 0.2, 0.1, 0.1]])
    result = subject.attach_label_and_versions(
        frame, probs, feature_version="v1", model_version="m1",
    )
    assert list(result["predicted_label"]) == ["nige"]
    assert list(result["running_style_feature_version"]) == ["v1"]
    assert list(result["model_version"]) == ["m1"]


def test_score_frame_uses_predict_softmax_and_attaches_labels() -> None:
    booster = MagicMock()
    frame = pd.DataFrame({
        "race_id": ["r1"],
        "umaban": [1],
        "speed_index_avg_5": [50.0],
        "target_running_style_class": [0],
    })
    with patch.object(
        subject, "predict_softmax",
        return_value=np.array([[0.7, 0.1, 0.1, 0.1]]),
    ) as predict_mock:
        result = subject.score_frame(
            booster=booster, frame=frame, feature_version="v1", model_version="m1",
        )
    assert predict_mock.called
    assert list(result["predicted_label"]) == ["nige"]


def test_write_predictions_parquet_partitions_by_category_and_year(tmp_path: Path) -> None:
    output_dir = str(tmp_path / "out")
    frame = MagicMock(spec=pd.DataFrame)
    subject.write_predictions_parquet(frame, output_dir)
    frame.to_parquet.assert_called_once_with(
        output_dir, partition_cols=["category", "race_year"], index=False,
    )


def test_run_aborts_when_model_version_does_not_match() -> None:
    psql_runner = MagicMock(
        return_value='{"model_version": "active-other", "artifact_path": "/p"}',
    )
    booster_loader = MagicMock()
    pandas_reader = MagicMock()
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat",
        "--model-version", "requested-mismatch",
        "--output-parquet", "/tmp/out",
        "--running-style-feature-version", "v1",
        "--pg-url", "postgres://x",
        "--category", "jra",
    ])
    with pytest.raises(RuntimeError):
        subject.run(
            args,
            psql_runner=psql_runner,
            booster_loader=booster_loader,
            pandas_reader=pandas_reader,
        )


def test_run_happy_path_invokes_booster_and_writes_parquet() -> None:
    psql_runner = MagicMock(
        return_value='{"model_version": "active-m", "artifact_path": "/p/model.txt"}',
    )
    booster_loader = MagicMock(return_value=MagicMock())
    frame = pd.DataFrame({
        "race_id": ["r"],
        "umaban": [1],
        "speed_index_avg_5": [50.0],
    })
    pandas_reader = MagicMock(return_value=frame)
    args = subject.parse_args([
        "--features-parquet", "/tmp/feat",
        "--model-version", "active-m",
        "--output-parquet", "/tmp/out",
        "--running-style-feature-version", "v1",
        "--pg-url", "postgres://x",
        "--category", "jra",
    ])
    with patch.object(
        subject, "predict_softmax",
        return_value=np.array([[0.6, 0.2, 0.1, 0.1]]),
    ):
        with patch.object(subject, "write_predictions_parquet") as write_mock:
            subject.run(
                args,
                psql_runner=psql_runner,
                booster_loader=booster_loader,
                pandas_reader=pandas_reader,
            )
    booster_loader.assert_called_once_with(model_file="/p/model.txt")
    assert write_mock.called


def test_main_uses_psql_runner_and_lightgbm() -> None:
    fake_booster = MagicMock()
    argv = [
        "--features-parquet", "/tmp/feat",
        "--model-version", "active-m",
        "--output-parquet", "/tmp/out",
        "--running-style-feature-version", "v1",
        "--pg-url", "postgres://x",
        "--category", "jra",
    ]
    frame = pd.DataFrame({"x": [1]})
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stderr = ""
    fake_result.stdout = '{"model_version": "active-m", "artifact_path": "/p/model.txt"}'
    with patch("lightgbm.Booster", return_value=fake_booster) as booster_mock:
        with patch("subprocess.run", return_value=fake_result):
            with patch.object(subject, "score_frame", return_value=frame):
                with patch.object(subject, "write_predictions_parquet"):
                    with patch.object(pd, "read_parquet", return_value=frame):
                        subject.main(argv)
    booster_mock.assert_called_once_with(model_file="/p/model.txt")
