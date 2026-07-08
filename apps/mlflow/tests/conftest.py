"""Shared pytest fixtures for mlflow_tracking tests."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest
from mlflow import MlflowClient

from mlflow_tracking import config

WriteJsonFixture = Callable[[Path, object], None]


def clear_ambient_backend_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unconditionally clear HORSE_RACING_MLFLOW_BACKEND_URI and the generic
    MLFLOW_TRACKING_URI.

    Extracted to a standalone function (rather than inlined only in
    isolate_data_dir below) so a regression test can invoke this exact logic
    directly against a simulated ambient value -- see
    test_config.py::test_clear_ambient_backend_uri_overrides_preset_env_var.

    Incident (2026-07-08): config.get_tracking_uri() checks HORSE_RACING_
    MLFLOW_BACKEND_URI FIRST, before falling back to the isolated sqlite
    default built from HORSE_RACING_MLFLOW_DATA_DIR. Neither this fixture nor
    test_cli.py's clear_cli_env used to clear it, only ENV_TRACKING_URI/
    ENV_ARTIFACTS_MODE/ENV_R2_BUCKET/ENV_DATA_DIR. When this var happened to
    be present in the ambient process environment (e.g. exported for
    interactive use of the real Neon-backed CLI) while running `pytest`,
    tests that build an MlflowClient via config.get_tracking_uri()/
    build_client() -- including ones that call cli.main() -- silently
    connected to and wrote into the real production tracking store instead
    of an isolated tmp_path one. Two registered-model champion aliases
    (jra-finish-position, jra-running-style) were overwritten with fake test
    data before this was caught and manually reverted. This must be cleared
    regardless of what else sets or fails to set it, for every test in this
    package.

    config.ENV_TRACKING_URI (the plain MLFLOW_TRACKING_URI) is a second,
    independent leak vector: it is the standard mlflow env var that
    mlflow.tracking.MlflowClient()/mlflow.set_tracking_uri() consult directly
    (outside this package's own config.get_tracking_uri() resolution order)
    whenever any code constructs a client without an explicit tracking_uri
    argument. Previously only test_cli.py's own clear_cli_env fixture cleared
    it, so any other test file in this package was exposed the same way the
    backend-URI var was. Cleared here too so the guarantee is package-wide,
    not file-local.
    """
    monkeypatch.delenv(config.ENV_BACKEND_URI, raising=False)
    monkeypatch.delenv(config.ENV_TRACKING_URI, raising=False)


@pytest.fixture(autouse=True)
def isolate_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point HORSE_RACING_MLFLOW_DATA_DIR at tmp_path for every test, and
    unconditionally clear HORSE_RACING_MLFLOW_BACKEND_URI and
    MLFLOW_TRACKING_URI.

    logging_api.get_or_create_experiment() falls back to
    config.resolve_artifact_location() (no explicit artifact_location) in
    most call sites exercised by this suite, and that function's "local" mode
    resolves against config.get_data_dir()'s real default
    (<repo>/apps/mlflow/data) whenever this env var is unset. Without this
    autouse override, every such test would write real artifact files into
    the actual repo directory instead of an isolated tmp_path.

    See clear_ambient_backend_uri()'s docstring for why the backend-URI/
    tracking-URI clearing lives here too: it must apply to every test file in
    this package, not just the ones (like test_cli.py) that happen to have
    their own local env-clearing fixture.
    """
    monkeypatch.setenv(config.ENV_DATA_DIR, str(tmp_path / "data"))
    clear_ambient_backend_uri(monkeypatch)


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
