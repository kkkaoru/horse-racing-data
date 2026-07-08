"""Tests for mlflow_ui.launchd."""

from __future__ import annotations

import plistlib
from pathlib import Path

import pytest

from mlflow_ui import config, launchd


def _make_cfg(tmp_path: Path) -> config.Config:
    data_dir = tmp_path / "data"
    return config.Config(
        data_dir=data_dir,
        backend_store_uri=f"sqlite:///{data_dir / 'mlflow.db'}",
        artifacts_destination=f"file://{data_dir / 'mlartifacts'}",
        host="127.0.0.1",
        port=5252,
        artifacts_mode="local",
    )


def test_generate_plist_structure(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)

    text = launchd.generate_plist(cfg, uv_path="/opt/homebrew/bin/uv")
    parsed = plistlib.loads(text.encode())

    assert parsed["Label"] == "com.horse-racing.mlflow-ui"
    assert parsed["KeepAlive"] is True
    assert parsed["RunAtLoad"] is True
    assert parsed["ProgramArguments"] == [
        "/opt/homebrew/bin/uv",
        "run",
        "--project",
        str(config.REPO_ROOT / "apps" / "mlflow-ui"),
        "python",
        "-m",
        "mlflow_ui.cli",
        "foreground",
    ]
    assert parsed["StandardOutPath"] == str(cfg.data_dir / "mlflow-ui-launchd.out.log")
    assert parsed["StandardErrorPath"] == str(cfg.data_dir / "mlflow-ui-launchd.err.log")


def test_generate_plist_default_uv_path(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)

    text = launchd.generate_plist(cfg)
    parsed = plistlib.loads(text.encode())

    assert parsed["ProgramArguments"][0] == "uv"


def test_generate_plist_carries_set_env_vars_and_omits_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_cfg(tmp_path)
    monkeypatch.setenv("HORSE_RACING_MLFLOW_UI_PORT", "6100")
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct123")

    text = launchd.generate_plist(cfg)
    parsed = plistlib.loads(text.encode())

    env_vars = parsed["EnvironmentVariables"]
    assert env_vars["HORSE_RACING_MLFLOW_UI_PORT"] == "6100"
    assert env_vars["R2_ACCOUNT_ID"] == "acct123"
    assert "HORSE_RACING_MLFLOW_R2_BUCKET" not in env_vars
    assert "R2_SECRET_ACCESS_KEY" not in env_vars


def test_generate_plist_empty_environment_when_nothing_set(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)

    text = launchd.generate_plist(cfg)
    parsed = plistlib.loads(text.encode())

    assert parsed["EnvironmentVariables"] == {}


def test_generate_plist_carries_backend_store_uri_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_cfg(tmp_path)
    monkeypatch.setenv("HORSE_RACING_MLFLOW_BACKEND_URI", "postgresql://user:pw@host/db")

    text = launchd.generate_plist(cfg)
    parsed = plistlib.loads(text.encode())

    assert (
        parsed["EnvironmentVariables"]["HORSE_RACING_MLFLOW_BACKEND_URI"]
        == "postgresql://user:pw@host/db"
    )


def test_generate_plist_program_arguments_never_contain_backend_uri_or_artifacts_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression guard for the ps-argv DSN leak: the backend URI (which may
    embed a plaintext database password) must only ever reach the
    supervised process via EnvironmentVariables, never baked into
    ProgramArguments -- a running process's argv is world-readable via `ps`,
    but its plist-supplied environment is not."""
    cfg = _make_cfg(tmp_path)
    monkeypatch.setenv("HORSE_RACING_MLFLOW_BACKEND_URI", "postgresql://user:pw@host/db")

    text = launchd.generate_plist(cfg)
    parsed = plistlib.loads(text.encode())

    program_arguments = parsed["ProgramArguments"]
    assert "postgresql://user:pw@host/db" not in program_arguments
    assert not any("--backend-store-uri" in arg for arg in program_arguments)
    assert not any("--artifacts-destination" in arg for arg in program_arguments)
