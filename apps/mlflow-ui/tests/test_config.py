"""Tests for mlflow_ui.config."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mlflow_ui import config


def test_repo_root_points_at_actual_repo_root() -> None:
    # apps/mlflow-ui lives two levels under the repo root.
    assert (config.REPO_ROOT / "apps" / "mlflow-ui").is_dir()
    assert (config.REPO_ROOT / "apps" / "mlflow-ui" / "pyproject.toml").is_file()


def test_default_config_values() -> None:
    cfg = config.load_config()

    assert cfg.data_dir == config.DEFAULT_DATA_DIR
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 5252
    assert cfg.artifacts_mode == "local"
    assert cfg.r2_bucket is None
    assert cfg.r2_prefix == "mlflow"


def test_default_sqlite_uri_has_exactly_four_slashes() -> None:
    cfg = config.load_config()

    assert cfg.backend_store_uri.startswith("sqlite:")
    remainder = cfg.backend_store_uri[len("sqlite:") :]
    slash_run = len(remainder) - len(remainder.lstrip("/"))
    assert slash_run == 4
    assert cfg.backend_store_uri == f"sqlite:///{cfg.data_dir / 'mlflow.db'}"


def test_default_local_artifacts_uri_has_exactly_three_slashes() -> None:
    cfg = config.load_config()

    assert cfg.artifacts_destination.startswith("file:")
    remainder = cfg.artifacts_destination[len("file:") :]
    slash_run = len(remainder) - len(remainder.lstrip("/"))
    assert slash_run == 3
    assert cfg.artifacts_destination == f"file://{cfg.data_dir / 'mlartifacts'}"


def test_data_dir_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    override = tmp_path / "custom-data"
    monkeypatch.setenv("HORSE_RACING_MLFLOW_DATA_DIR", str(override))

    cfg = config.load_config()

    assert cfg.data_dir == override
    assert cfg.backend_store_uri == f"sqlite:///{override / 'mlflow.db'}"
    assert cfg.artifacts_destination == f"file://{override / 'mlartifacts'}"


def test_backend_uri_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_BACKEND_URI", "sqlite:////custom/path/mlflow.db")

    cfg = config.load_config()

    assert cfg.backend_store_uri == "sqlite:////custom/path/mlflow.db"


def test_host_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_UI_HOST", "0.0.0.0")

    cfg = config.load_config()

    assert cfg.host == "0.0.0.0"


def test_port_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_UI_PORT", "9999")

    cfg = config.load_config()

    assert cfg.port == 9999


def test_invalid_port_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_UI_PORT", "not-a-port")

    with pytest.raises(ValueError, match="not-a-port"):
        config.load_config()


def test_invalid_artifacts_mode_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "gcs")

    with pytest.raises(ValueError, match="gcs"):
        config.load_config()


def test_explicit_local_mode_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "local")

    cfg = config.load_config()

    assert cfg.artifacts_mode == "local"


def test_r2_prefix_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_R2_BUCKET", "my-bucket")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_R2_PREFIX", "custom-prefix")
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")

    cfg = config.load_config()

    assert cfg.r2_prefix == "custom-prefix"
    assert cfg.artifacts_destination == "s3://my-bucket/custom-prefix"


def _set_full_r2_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_R2_BUCKET", "my-bucket")
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")


def test_r2_mode_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_full_r2_env(monkeypatch)

    cfg = config.load_config()

    assert cfg.artifacts_mode == "r2"
    assert cfg.artifacts_destination == "s3://my-bucket/mlflow"
    assert cfg.r2_account_id == "acct123"
    assert cfg.r2_access_key_id == "key123"
    assert cfg.r2_secret_access_key == "secret123"
    assert cfg.r2_bucket == "my-bucket"


def test_r2_mode_cloudflare_account_id_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")
    monkeypatch.setenv("HORSE_RACING_MLFLOW_R2_BUCKET", "my-bucket")
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "fallback-acct")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")

    cfg = config.load_config()

    assert cfg.r2_account_id == "fallback-acct"


def test_r2_mode_prefers_r2_account_id_over_cloudflare_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_full_r2_env(monkeypatch)
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "should-not-be-used")

    cfg = config.load_config()

    assert cfg.r2_account_id == "acct123"


def test_r2_mode_missing_everything_raises_naming_all_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")

    with pytest.raises(ValueError) as excinfo:
        config.load_config()

    message = str(excinfo.value)
    assert "R2_ACCOUNT_ID" in message
    assert "R2_ACCESS_KEY_ID" in message
    assert "R2_SECRET_ACCESS_KEY" in message
    assert "HORSE_RACING_MLFLOW_R2_BUCKET" in message


def test_r2_mode_missing_bucket_only_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ARTIFACTS_MODE", "r2")
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")

    with pytest.raises(ValueError) as excinfo:
        config.load_config()

    message = str(excinfo.value)
    assert "HORSE_RACING_MLFLOW_R2_BUCKET" in message
    assert "R2_ACCESS_KEY_ID" not in message


def test_server_env_local_mode_is_empty() -> None:
    cfg = config.load_config()

    assert config.server_env(cfg) == {}


def test_server_env_r2_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_full_r2_env(monkeypatch)
    cfg = config.load_config()

    env = config.server_env(cfg)

    assert env["MLFLOW_S3_ENDPOINT_URL"] == "https://acct123.r2.cloudflarestorage.com"
    assert env["AWS_ACCESS_KEY_ID"] == "key123"
    assert env["AWS_SECRET_ACCESS_KEY"] == "secret123"
    assert env["AWS_DEFAULT_REGION"] == "auto"


def test_server_env_does_not_clobber_existing_aws_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_full_r2_env(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "operator-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "operator-secret")
    cfg = config.load_config()

    env = config.server_env(cfg)

    assert "AWS_ACCESS_KEY_ID" not in env
    assert "AWS_SECRET_ACCESS_KEY" not in env
    assert env["MLFLOW_S3_ENDPOINT_URL"] == "https://acct123.r2.cloudflarestorage.com"


def test_ensure_wal_sets_wal_mode_and_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "nested" / "mlflow.db"

    config.ensure_wal(db_path)
    config.ensure_wal(db_path)

    conn = sqlite3.connect(db_path)
    try:
        (mode,) = conn.execute("PRAGMA journal_mode;").fetchone()
    finally:
        conn.close()
    assert mode.lower() == "wal"
