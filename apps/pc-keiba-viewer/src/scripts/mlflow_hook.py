#!/usr/bin/env python3
"""Best-effort adapter that forwards local training/eval results to MLflow.

``apps/mlflow`` (module ``mlflow_tracking``) owns a sqlite-backed MLflow
tracking store and exposes
``uv run --project apps/mlflow python -m mlflow_tracking.cli log-training-run
<manifest.json>`` as its ingestion path. This module is the pc-keiba-viewer
side of that integration: it builds the fixed manifest schema the CLI
consumes and shells out to it via subprocess, so pc-keiba-viewer itself never
gains an MLflow (or any other) dependency.

Every public entry point is best-effort and non-fatal: disabled-by-env,
missing ``uv``, a non-zero CLI exit, a timeout, or any other exception are all
swallowed, logged to stderr, and reported back as ``False`` / a no-op.
Callers invoke these functions from the tail of a training run without
risking the run's own exit code or stdout.

Enable/disable via ``HORSE_RACING_MLFLOW_ENABLED`` (default: enabled). The
package test suite disables it globally via an autouse fixture in
``tests/conftest.py`` so unit tests never shell out to the real CLI; a test
that needs to exercise the real code path re-enables it via
``monkeypatch.setenv`` and mocks ``subprocess.run`` itself.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Final, NotRequired, TypedDict

if TYPE_CHECKING:
    from collections.abc import Mapping

MLFLOW_ENABLED_ENV_VAR: Final[str] = "HORSE_RACING_MLFLOW_ENABLED"
MLFLOW_DISABLED_VALUE: Final[str] = "0"
MANIFEST_SCHEMA: Final[str] = "hr-mlflow-training-run/v1"
CLI_TIMEOUT_SECONDS: Final[int] = 180
CATEGORY_ALIASES: Final[dict[str, str]] = {"ban-ei": "banei"}
REPO_ROOT_PARENT_DEPTH: Final[int] = 4


class TrainingRunManifest(TypedDict):
    """The hr-mlflow-training-run/v1 payload consumed by mlflow_tracking.cli."""

    schema: str
    task: str
    category: str
    model_version: str
    eval_regime: str
    aggregate_metrics: dict[str, float]
    register: bool
    champion: bool
    artifact_dir: NotRequired[str]
    experiment: NotRequired[str]
    cell_report: NotRequired[str]
    params: NotRequired[dict[str, object]]
    tags: NotRequired[dict[str, object]]


def mlflow_enabled() -> bool:
    """MLflow logging is on by default; set HORSE_RACING_MLFLOW_ENABLED=0 to opt out."""
    return os.environ.get(MLFLOW_ENABLED_ENV_VAR, "1") != MLFLOW_DISABLED_VALUE


def normalize_category(category: str) -> str:
    """Map the repo's internal 'ban-ei' spelling to mlflow_tracking's 'banei' enum value."""
    return CATEGORY_ALIASES.get(category, category)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[REPO_ROOT_PARENT_DEPTH]


def mlflow_project_dir() -> Path:
    return repo_root() / "apps" / "mlflow"


def build_manifest(
    *,
    task: str,
    category: str,
    model_version: str,
    eval_regime: str,
    artifact_dir: Path | str | None = None,
    experiment: str | None = None,
    cell_report: Path | str | None = None,
    aggregate_metrics: Mapping[str, float] | None = None,
    params: Mapping[str, object] | None = None,
    tags: Mapping[str, object] | None = None,
    register: bool = False,
    champion: bool = False,
) -> TrainingRunManifest:
    """Build the fixed hr-mlflow-training-run/v1 manifest dict.

    Path-valued fields are stringified here so the manifest is always plain
    JSON regardless of what the caller passes in.
    """
    manifest: TrainingRunManifest = {
        "schema": MANIFEST_SCHEMA,
        "task": task,
        "category": normalize_category(category),
        "model_version": model_version,
        "eval_regime": eval_regime,
        "aggregate_metrics": dict(aggregate_metrics) if aggregate_metrics is not None else {},
        "register": register,
        "champion": champion,
    }
    if artifact_dir is not None:
        manifest["artifact_dir"] = str(artifact_dir)
    if experiment is not None:
        manifest["experiment"] = experiment
    if cell_report is not None:
        manifest["cell_report"] = str(cell_report)
    if params is not None:
        manifest["params"] = dict(params)
    if tags is not None:
        manifest["tags"] = dict(tags)
    return manifest


def emit_training_run(manifest: Mapping[str, object]) -> bool:
    """Forward a pre-built manifest to the mlflow_tracking CLI.

    Returns True only when the subprocess exits 0. Disabled-by-env, a missing
    ``uv`` executable, a non-zero exit, and a timeout all return False; this
    function never raises.
    """
    if not mlflow_enabled():
        return False
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix="hr-mlflow-manifest-",
        delete=False,
        encoding="utf-8",
    ) as handle:
        json.dump(dict(manifest), handle, ensure_ascii=False, default=str)
        manifest_path = Path(handle.name)
    try:
        result = subprocess.run(
            [
                "uv",
                "run",
                "--project",
                str(mlflow_project_dir()),
                "python",
                "-m",
                "mlflow_tracking.cli",
                "log-training-run",
                str(manifest_path),
            ],
            capture_output=True,
            timeout=CLI_TIMEOUT_SECONDS,
            check=False,
        )
    except FileNotFoundError:
        print("[mlflow_hook] uv executable not found; skipping MLflow logging", file=sys.stderr)
        return False
    except subprocess.TimeoutExpired:
        print(
            "[mlflow_hook] mlflow_tracking CLI timed out; skipping MLflow logging",
            file=sys.stderr,
        )
        return False
    if result.returncode != 0:
        stderr_tail = result.stderr.decode("utf-8", errors="replace")[-2000:]
        print(
            f"[mlflow_hook] mlflow_tracking CLI exited {result.returncode}: {stderr_tail}",
            file=sys.stderr,
        )
        return False
    return True


def safe_emit_training_run(
    *,
    task: str,
    category: str,
    model_version: str,
    eval_regime: str,
    artifact_dir: Path | str | None = None,
    experiment: str | None = None,
    cell_report: Path | str | None = None,
    aggregate_metrics: Mapping[str, float] | None = None,
    params: Mapping[str, object] | None = None,
    tags: Mapping[str, object] | None = None,
    register: bool = False,
    champion: bool = False,
) -> bool:
    """Build-and-emit in one call; any exception in either step is caught here.

    This is the entry point training/eval scripts call from their completion
    point: one call, no local try/except needed, and a failure here is
    guaranteed to never affect the caller's exit code or output.
    """
    try:
        manifest = build_manifest(
            task=task,
            category=category,
            model_version=model_version,
            eval_regime=eval_regime,
            artifact_dir=artifact_dir,
            experiment=experiment,
            cell_report=cell_report,
            aggregate_metrics=aggregate_metrics,
            params=params,
            tags=tags,
            register=register,
            champion=champion,
        )
        return emit_training_run(manifest)
    except Exception as exc:
        print(f"[mlflow_hook] failed to emit training run: {exc}", file=sys.stderr)
        return False


def flatten_numeric_metrics(source: Mapping[str, object]) -> dict[str, float]:
    """Keep only top-level non-bool int/float values, coerced to float.

    Native result dicts (serve_accuracy_report's metrics_to_dict, DuckDB
    rollups with None placeholders) mix scalars with nested lists/dicts and
    None values that don't fit the manifest's flat aggregate_metrics map.
    """
    flattened: dict[str, float] = {}
    for key, value in source.items():
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            flattened[key] = float(value)
    return flattened
