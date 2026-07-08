"""Shared pytest fixtures for the pc-keiba-viewer Python test suite."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def disable_mlflow_hook_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep MLflow logging opt-in-only during tests.

    ``mlflow_hook.mlflow_enabled()`` defaults to True in production so local
    training/eval runs log to MLflow without extra flags. Without this
    fixture, any test that exercises a hooked script's main()/run() entry
    point would attempt a real ``uv run --project apps/mlflow ...``
    subprocess call. Tests that specifically exercise the MLflow integration
    re-enable it via ``monkeypatch.setenv`` within their own test body, which
    overrides this fixture's setenv call for that test only.
    """
    monkeypatch.setenv("HORSE_RACING_MLFLOW_ENABLED", "0")
