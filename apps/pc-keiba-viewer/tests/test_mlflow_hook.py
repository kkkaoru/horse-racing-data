from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

import mlflow_hook as subject


def test_mlflow_enabled_defaults_true_when_env_unset(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("HORSE_RACING_MLFLOW_ENABLED", raising=False)
    assert subject.mlflow_enabled() is True


def test_mlflow_enabled_true_when_env_is_one(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENABLED", "1")
    assert subject.mlflow_enabled() is True


def test_mlflow_enabled_false_when_env_is_zero(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENABLED", "0")
    assert subject.mlflow_enabled() is False


def test_mlflow_enabled_true_for_unrecognized_value(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENABLED", "yes")
    assert subject.mlflow_enabled() is True


def test_normalize_category_maps_ban_ei_to_banei():
    assert subject.normalize_category("ban-ei") == "banei"


def test_normalize_category_passes_through_jra():
    assert subject.normalize_category("jra") == "jra"


def test_normalize_category_passes_through_nar():
    assert subject.normalize_category("nar") == "nar"


def test_repo_root_contains_apps_pc_keiba_viewer():
    root = subject.repo_root()
    assert (root / "apps" / "pc-keiba-viewer").is_dir()


def test_mlflow_project_dir_points_at_apps_mlflow():
    assert subject.mlflow_project_dir() == subject.repo_root() / "apps" / "mlflow"


def test_build_manifest_minimal_fields_only():
    manifest = subject.build_manifest(
        task="finish-position",
        category="jra",
        model_version="jra-cb-v9-sim-2013",
        eval_regime="wf",
    )
    assert manifest["schema"] == "hr-mlflow-training-run/v1"
    assert manifest["task"] == "finish-position"
    assert manifest["category"] == "jra"
    assert manifest["model_version"] == "jra-cb-v9-sim-2013"
    assert manifest["eval_regime"] == "wf"
    assert manifest["aggregate_metrics"] == {}
    assert manifest["register"] is False
    assert manifest["champion"] is False
    assert "artifact_dir" not in manifest
    assert "experiment" not in manifest
    assert "cell_report" not in manifest
    assert "params" not in manifest
    assert "tags" not in manifest


def test_build_manifest_normalizes_banei_category():
    manifest = subject.build_manifest(
        task="finish-position",
        category="ban-ei",
        model_version="banei-cb-v9-sim-2011",
        eval_regime="wf",
    )
    assert manifest["category"] == "banei"


def test_build_manifest_all_optional_fields_present(tmp_path: Path):
    manifest = subject.build_manifest(
        task="running-style",
        category="nar",
        model_version="nar-xgb-iter40",
        eval_regime="serve",
        artifact_dir=tmp_path / "models" / "nar",
        experiment="nar-running-style",
        cell_report=tmp_path / "cell.json",
        aggregate_metrics={"top1_accuracy": 0.42},
        params={"iteration_id": 40},
        tags={"date": "20260707"},
        register=True,
        champion=True,
    )
    assert manifest.get("artifact_dir") == str(tmp_path / "models" / "nar")
    assert manifest.get("experiment") == "nar-running-style"
    assert manifest.get("cell_report") == str(tmp_path / "cell.json")
    assert manifest["aggregate_metrics"] == {"top1_accuracy": 0.42}
    assert manifest.get("params") == {"iteration_id": 40}
    assert manifest.get("tags") == {"date": "20260707"}
    assert manifest["register"] is True
    assert manifest["champion"] is True


def test_build_manifest_accepts_string_artifact_dir_and_cell_report():
    manifest = subject.build_manifest(
        task="finish-position",
        category="jra",
        model_version="jra-cb-v9-sim-2013",
        eval_regime="wf",
        artifact_dir="tmp/models/jra",
        cell_report="tmp/cell.json",
    )
    assert manifest.get("artifact_dir") == "tmp/models/jra"
    assert manifest.get("cell_report") == "tmp/cell.json"


def test_emit_training_run_returns_false_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENABLED", "0")
    run_mock = MagicMock()
    monkeypatch.setattr(subprocess, "run", run_mock)
    result = subject.emit_training_run({"schema": subject.MANIFEST_SCHEMA})
    assert result is False
    run_mock.assert_not_called()


def test_emit_training_run_writes_manifest_json_and_returns_true_on_success(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENABLED", "1")
    captured: dict[str, object] = {}

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        captured["cmd"] = cmd
        manifest_path = Path(cmd[-1])
        captured["manifest_text"] = manifest_path.read_text(encoding="utf-8")
        return subprocess.CompletedProcess(cmd, returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(subprocess, "run", fake_run)
    manifest = subject.build_manifest(
        task="finish-position",
        category="jra",
        model_version="jra-cb-v9-sim-2013",
        eval_regime="wf",
        aggregate_metrics={"top1_accuracy": 0.5},
    )
    result = subject.emit_training_run(manifest)
    assert result is True
    cmd = cast(list[str], captured["cmd"])
    assert cmd[0] == "uv"
    assert cmd[1] == "run"
    assert cmd[2] == "--project"
    assert cmd[3] == str(subject.mlflow_project_dir())
    assert cmd[4:8] == ["python", "-m", "mlflow_tracking.cli", "log-training-run"]
    written = json.loads(cast(str, captured["manifest_text"]))
    assert written["schema"] == "hr-mlflow-training-run/v1"
    assert written["task"] == "finish-position"
    assert written["category"] == "jra"
    assert written["model_version"] == "jra-cb-v9-sim-2013"
    assert written["eval_regime"] == "wf"
    assert written["aggregate_metrics"] == {"top1_accuracy": 0.5}


def test_emit_training_run_returns_false_on_nonzero_exit(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENABLED", "1")

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(cmd, returncode=1, stdout=b"", stderr=b"boom")

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = subject.emit_training_run({"schema": subject.MANIFEST_SCHEMA})
    assert result is False


def test_emit_training_run_returns_false_when_uv_missing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENABLED", "1")

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise FileNotFoundError("uv not found")

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = subject.emit_training_run({"schema": subject.MANIFEST_SCHEMA})
    assert result is False


def test_emit_training_run_returns_false_on_timeout(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENABLED", "1")

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=180)

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = subject.emit_training_run({"schema": subject.MANIFEST_SCHEMA})
    assert result is False


def test_safe_emit_training_run_returns_true_on_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENABLED", "1")
    monkeypatch.setattr(subject, "emit_training_run", MagicMock(return_value=True))
    result = subject.safe_emit_training_run(
        task="finish-position",
        category="jra",
        model_version="jra-cb-v9-sim-2013",
        eval_regime="wf",
    )
    assert result is True


def test_safe_emit_training_run_catches_build_manifest_exception(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        subject, "build_manifest", MagicMock(side_effect=RuntimeError("boom")),
    )
    result = subject.safe_emit_training_run(
        task="finish-position",
        category="jra",
        model_version="jra-cb-v9-sim-2013",
        eval_regime="wf",
    )
    assert result is False


def test_safe_emit_training_run_catches_emit_training_run_exception(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        subject, "emit_training_run", MagicMock(side_effect=RuntimeError("boom")),
    )
    result = subject.safe_emit_training_run(
        task="finish-position",
        category="jra",
        model_version="jra-cb-v9-sim-2013",
        eval_regime="wf",
    )
    assert result is False


def test_flatten_numeric_metrics_keeps_int_and_float():
    out = subject.flatten_numeric_metrics({"races": 10, "top1_pct": 42.5})
    assert out == {"races": 10.0, "top1_pct": 42.5}


def test_flatten_numeric_metrics_skips_bool():
    out = subject.flatten_numeric_metrics({"is_ok": True, "races": 10})
    assert out == {"races": 10.0}


def test_flatten_numeric_metrics_skips_none_and_nested_structures():
    out = subject.flatten_numeric_metrics({
        "macro_f1_pct": None,
        "model_version_counts": {"a": 1},
        "subgroups": [1, 2, 3],
        "label": "jra",
        "top1_pct": 1.0,
    })
    assert out == {"top1_pct": 1.0}


def test_flatten_numeric_metrics_returns_empty_for_empty_input():
    assert subject.flatten_numeric_metrics({}) == {}
