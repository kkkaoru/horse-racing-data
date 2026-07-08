"""Shared pytest fixtures for mlflow-ui tests.

Every test runs with a clean slate for the package's env vars so tests never
depend on (or leak into) whatever happens to be set in the invoking shell.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

_ENV_VARS_TO_ISOLATE: tuple[str, ...] = (
    "HORSE_RACING_MLFLOW_DATA_DIR",
    "HORSE_RACING_MLFLOW_BACKEND_URI",
    "HORSE_RACING_MLFLOW_ARTIFACTS_MODE",
    "HORSE_RACING_MLFLOW_UI_HOST",
    "HORSE_RACING_MLFLOW_UI_PORT",
    "HORSE_RACING_MLFLOW_R2_BUCKET",
    "HORSE_RACING_MLFLOW_R2_PREFIX",
    "R2_ACCOUNT_ID",
    "CLOUDFLARE_ACCOUNT_ID",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "MLFLOW_S3_ENDPOINT_URL",
    "MLFLOW_SERVER_ENABLE_JOB_EXECUTION",
)


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in _ENV_VARS_TO_ISOLATE:
        monkeypatch.delenv(name, raising=False)
    yield


@pytest.fixture(autouse=True)
def isolate_env_file_loaders(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point HORSE_RACING_MLFLOW_ENV_FILE / _ROOT_ENV_FILE at guaranteed-
    nonexistent paths for every test.

    config.load_dotenv_local() / load_repo_root_env_fallback() otherwise
    default to the real apps/mlflow/.env.local and repo-root .env on disk --
    real, gitignored, secret-bearing files. Any test that calls cli.main()
    (or either loader directly) without its own override would read those
    real files, making outcomes depend on machine state (and even hit the
    real production server via a fully-resolved backend config) instead of
    being hermetic. Individual loader tests override this by passing an
    explicit env_file argument, which always wins over this env var.
    """
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENV_FILE", str(tmp_path / "nonexistent-env-local"))
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ROOT_ENV_FILE", str(tmp_path / "nonexistent-root-env"))
