from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

import score_finish_position_local as subject


def test_parse_args_full_set():
    args = subject.parse_args(
        [
            "--features-parquet",
            "tmp/features",
            "--model-version",
            "jra-finish-position-lambdarank-v7-baseline",
            "--output-parquet",
            "tmp/predictions",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--pg-url",
            "postgresql://u:p@h/db",
            "--category",
            "jra",
        ]
    )
    assert args.features_parquet == Path("tmp/features")
    assert args.model_version == "jra-finish-position-lambdarank-v7-baseline"
    assert args.output_parquet == Path("tmp/predictions")
    assert args.finish_position_version == "v1"
    assert args.running_style_feature_version == "v1"
    assert args.pg_url == "postgresql://u:p@h/db"
    assert args.category == "jra"


def test_parse_args_rejects_unknown_category():
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--features-parquet",
                "x",
                "--model-version",
                "x",
                "--output-parquet",
                "x",
                "--finish-position-version",
                "v1",
                "--running-style-feature-version",
                "v1",
                "--pg-url",
                "x",
                "--category",
                "bogus",
            ]
        )


def test_normalize_arguments_converts_paths():
    raw = subject.parse_args(
        [
            "--features-parquet",
            "tmp/features",
            "--model-version",
            "m1",
            "--output-parquet",
            "tmp/predictions",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--pg-url",
            "x",
            "--category",
            "nar",
        ]
    )
    normalized = subject.normalize_arguments(raw)
    assert normalized["features_parquet"] == Path("tmp/features")
    assert normalized["output_parquet"] == Path("tmp/predictions")
    assert normalized["model_version"] == "m1"
    assert normalized["category"] == "nar"


def test_build_active_model_query_uses_finish_position_active_models_table():
    sql = subject.build_active_model_query("jra")
    assert (
        sql
        == "select json_build_object('model_version', model_version, 'artifact_path', artifact_path) from finish_position_active_models where category = 'jra' limit 1"
    )


def test_resolve_active_model_returns_active_pair():
    runner = MagicMock(
        return_value=json.dumps({
            "model_version": "model-v7",
            "artifact_path": "/path/to/model.txt",
        })
    )
    model_version, artifact_path = subject.resolve_active_model(
        psql_runner=runner, pg_url="postgres://u", category="jra",
    )
    assert model_version == "model-v7"
    assert artifact_path == "/path/to/model.txt"
    runner.assert_called_once()


def test_resolve_active_model_raises_when_psql_returns_empty():
    runner = MagicMock(return_value="")
    with pytest.raises(RuntimeError) as info:
        subject.resolve_active_model(psql_runner=runner, pg_url="x", category="jra")
    assert "No active finish-position model" in str(info.value)


def test_resolve_active_model_rejects_non_string_artifact_path():
    runner = MagicMock(
        return_value=json.dumps({"model_version": "m", "artifact_path": 7})
    )
    with pytest.raises(RuntimeError) as info:
        subject.resolve_active_model(psql_runner=runner, pg_url="x", category="jra")
    assert "string model_version and artifact_path" in str(info.value)


def test_resolve_model_version_matches_active():
    assert subject.resolve_model_version("v7", "v7") == "v7"


def test_resolve_model_version_mismatch_raises():
    with pytest.raises(RuntimeError) as info:
        subject.resolve_model_version("v7", "v6")
    assert "does not match active finish-position model" in str(info.value)


def test_attach_versions_adds_three_columns():
    frame = pd.DataFrame({"race_id": ["r1"]})
    stamped = subject.attach_versions(
        frame,
        finish_position_version="v1",
        running_style_feature_version="v1",
        model_version="m7",
    )
    assert stamped["finish_position_version"].tolist() == ["v1"]
    assert stamped["running_style_feature_version"].tolist() == ["v1"]
    assert stamped["model_version"].tolist() == ["m7"]


def test_score_features_frame_uses_score_dataset_callable():
    booster = MagicMock()
    scored = pd.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "ketto_toroku_bango": ["a", "b"],
            "umaban": [1, 2],
            "predicted_score": [0.7, 0.3],
            "predicted_rank": [1, 2],
        }
    )
    score_dataset = MagicMock(return_value=scored)
    features = pd.DataFrame({"race_id": ["r1", "r1"]})
    result = subject.score_features_frame(
        booster=booster,
        features=features,
        finish_position_version="v1",
        running_style_feature_version="v1",
        model_version="m7",
        score_dataset=score_dataset,
    )
    score_dataset.assert_called_once()
    assert result["finish_position_version"].tolist() == ["v1", "v1"]
    assert result["running_style_feature_version"].tolist() == ["v1", "v1"]
    assert result["model_version"].tolist() == ["m7", "m7"]
    assert result["predicted_rank"].tolist() == [1, 2]


def test_write_predictions_parquet_partitions_by_category_and_year(tmp_path: Path):
    frame = MagicMock(spec=pd.DataFrame)
    output_dir = tmp_path / "predictions"
    subject.write_predictions_parquet(frame, output_dir)
    assert output_dir.exists()
    frame.to_parquet.assert_called_once_with(
        output_dir.as_posix(),
        partition_cols=["category", "race_year"],
        index=False,
        existing_data_behavior="delete_matching",
    )


def test_write_predictions_parquet_passes_delete_matching_behavior(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    captured_kwargs: list[dict] = []

    def mock_to_parquet(self, path, **kwargs):
        captured_kwargs.append(kwargs)
        # Don't actually write — just capture kwargs

    monkeypatch.setattr(pd.DataFrame, "to_parquet", mock_to_parquet)
    frame = pd.DataFrame({"category": ["jra"], "race_year": [2024], "predicted_score": [0.5]})
    subject.write_predictions_parquet(frame, tmp_path / "out")
    assert len(captured_kwargs) == 1
    assert captured_kwargs[0].get("existing_data_behavior") == "delete_matching"


def test_run_orchestrates_resolve_score_and_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    features_parquet = tmp_path / "features"
    features_parquet.mkdir()
    output_parquet = tmp_path / "predictions"
    psql_runner = MagicMock(
        return_value=json.dumps({
            "model_version": "model-v7",
            "artifact_path": "/path/to/model.txt",
        })
    )
    fake_booster = MagicMock()
    booster_loader = MagicMock(return_value=fake_booster)
    features_frame = pd.DataFrame(
        {"race_id": ["r1", "r1"], "ketto_toroku_bango": ["a", "b"]},
    )
    pandas_reader = MagicMock(return_value=features_frame)
    scored_frame = pd.DataFrame(
        {
            "race_id": ["r1", "r1"],
            "ketto_toroku_bango": ["a", "b"],
            "umaban": [1, 2],
            "predicted_score": [0.7, 0.3],
            "predicted_rank": [1, 2],
        }
    )
    score_dataset = MagicMock(return_value=scored_frame)
    write_mock = MagicMock()
    monkeypatch.setattr(subject, "write_predictions_parquet", write_mock)
    args: subject.PhaseBArguments = {
        "features_parquet": features_parquet,
        "model_version": "model-v7",
        "output_parquet": output_parquet,
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
        "pg_url": "postgres://u:p@h/db",
        "category": "jra",
    }
    result = subject.run(
        args,
        psql_runner=psql_runner,
        booster_loader=booster_loader,
        pandas_reader=pandas_reader,
        score_dataset=score_dataset,
    )
    write_mock.assert_called_once()
    assert result == {
        "output_parquet": output_parquet.as_posix(),
        "rows_written": 2,
        "model_version": "model-v7",
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
        "category": "jra",
    }
    psql_runner.assert_called_once()
    booster_loader.assert_called_once_with(Path("/path/to/model.txt"))
    pandas_reader.assert_called_once_with(features_parquet.as_posix())
    score_dataset.assert_called_once()


def test_run_aborts_on_model_version_mismatch(tmp_path: Path):
    psql_runner = MagicMock(
        return_value=json.dumps({
            "model_version": "model-v7",
            "artifact_path": "/path/to/model.txt",
        })
    )
    args: subject.PhaseBArguments = {
        "features_parquet": tmp_path / "features",
        "model_version": "model-v6-wrong",
        "output_parquet": tmp_path / "predictions",
        "finish_position_version": "v1",
        "running_style_feature_version": "v1",
        "pg_url": "postgres://u",
        "category": "jra",
    }
    with pytest.raises(RuntimeError) as info:
        subject.run(
            args,
            psql_runner=psql_runner,
            booster_loader=MagicMock(),
            pandas_reader=MagicMock(),
            score_dataset=MagicMock(),
        )
    assert "does not match active finish-position model" in str(info.value)


def test_run_psql_invokes_psql_subprocess(monkeypatch: pytest.MonkeyPatch):
    captured = MagicMock()
    captured.returncode = 0
    captured.stdout = "result-stdout"
    captured.stderr = ""
    fake_subprocess_run = MagicMock(return_value=captured)
    monkeypatch.setattr(subprocess, "run", fake_subprocess_run)
    output = subject.run_psql("postgres://u", "select 1")
    assert output == "result-stdout"
    fake_subprocess_run.assert_called_once()


def test_run_psql_raises_on_non_zero_exit(monkeypatch: pytest.MonkeyPatch):
    captured = MagicMock()
    captured.returncode = 1
    captured.stdout = ""
    captured.stderr = "boom"
    fake_subprocess_run = MagicMock(return_value=captured)
    monkeypatch.setattr(subprocess, "run", fake_subprocess_run)
    with pytest.raises(RuntimeError) as info:
        subject.run_psql("postgres://u", "select 1")
    assert "psql failed: boom" in str(info.value)


def test_main_calls_run_and_prints_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    fake_run = MagicMock(
        return_value={
            "output_parquet": "tmp/out",
            "rows_written": 1,
            "model_version": "m7",
            "finish_position_version": "v1",
            "running_style_feature_version": "v1",
            "category": "jra",
        }
    )
    monkeypatch.setattr(subject, "run", fake_run)
    subject.main(
        [
            "--features-parquet",
            "tmp/features",
            "--model-version",
            "m7",
            "--output-parquet",
            "tmp/out",
            "--finish-position-version",
            "v1",
            "--running-style-feature-version",
            "v1",
            "--pg-url",
            "postgres://u:p@h/db",
            "--category",
            "jra",
        ]
    )
    fake_run.assert_called_once()
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip())
    assert payload["model_version"] == "m7"
    assert payload["finish_position_version"] == "v1"
    assert payload["running_style_feature_version"] == "v1"
    assert payload["category"] == "jra"
