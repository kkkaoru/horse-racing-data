"""Shared pytest fixtures for mlflow-ui tests.

Every test runs with a clean slate for the package's env vars so tests never
depend on (or leak into) whatever happens to be set in the invoking shell.
"""

from __future__ import annotations

from collections.abc import Iterator

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
)


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in _ENV_VARS_TO_ISOLATE:
        monkeypatch.delenv(name, raising=False)
    yield
