"""Shared pytest fixtures for mlflow_tracking tests."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest
from mlflow import MlflowClient

from mlflow_tracking import config

WriteJsonFixture = Callable[[Path, object], None]


@pytest.fixture(autouse=True)
def isolate_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point HORSE_RACING_MLFLOW_DATA_DIR at tmp_path for every test.

    logging_api.get_or_create_experiment() falls back to
    config.resolve_artifact_location() (no explicit artifact_location) in
    most call sites exercised by this suite, and that function's "local" mode
    resolves against config.get_data_dir()'s real default
    (<repo>/apps/mlflow/data) whenever this env var is unset. Without this
    autouse override, every such test would write real artifact files into
    the actual repo directory instead of an isolated tmp_path.
    """
    monkeypatch.setenv(config.ENV_DATA_DIR, str(tmp_path / "data"))


@pytest.fixture(autouse=True)
def isolate_env_file_loaders(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point HORSE_RACING_MLFLOW_ENV_FILE / _ROOT_ENV_FILE at guaranteed-
    nonexistent paths for every test.

    config.load_dotenv_local() / load_repo_root_env_fallback() otherwise
    default to the real apps/mlflow/.env.local and repo-root .env on disk --
    real, gitignored, secret-bearing files. Any test that calls cli.main()
    (or either loader directly) without its own override would read those
    real files, making outcomes depend on machine state instead of being
    hermetic. Individual loader tests override this by passing an explicit
    env_file argument, which always wins over this env var.
    """
    monkeypatch.setenv(config.ENV_ENV_FILE, str(tmp_path / "nonexistent-env-local"))
    monkeypatch.setenv(config.ENV_ROOT_ENV_FILE, str(tmp_path / "nonexistent-root-env"))


@pytest.fixture
def client(tmp_path: Path) -> MlflowClient:
    """A fresh MlflowClient backed by an isolated sqlite tracking store."""
    db_path = tmp_path / "mlflow.db"
    return MlflowClient(tracking_uri=f"sqlite:///{db_path.as_posix()}")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


@pytest.fixture
def write_json() -> WriteJsonFixture:
    """Write `payload` as JSON to `path`, creating parent directories as needed."""
    return _write_json
