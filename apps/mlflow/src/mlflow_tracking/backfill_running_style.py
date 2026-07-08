"""Backfill MLflow registry versions for running-style (脚質) model artifacts.

Trainer: apps/pc-keiba-viewer/src/scripts/running_style_lightgbm.py (4-class
softmax LightGBM). Each trained version writes a ``model.txt`` (LightGBM text
dump) + ``metadata.json`` pair via that module's ``write_model_metadata()``.
Ban-ei is explicitly OUT of scope for running-style (Ban-ei is characterized
by futan_juryo, not running style): only ``jra-running-style`` and
``nar-running-style`` are registered here.

``artifact_root`` (default ``apps/pc-keiba-viewer/tmp/models/``) is a SHARED,
untracked staging directory that also holds unrelated finish-position trial
artifacts (e.g. ``jra-cb-v9-sim/``) — it may not exist at all, and even when
it does, most of its subdirectories are not running-style models.
``discover_metadata_files`` therefore filters by RS-specific schema markers
(``class_labels`` + ``feature_columns``) rather than trusting every
``metadata.json`` it finds.

Serving is via a Cloudflare R2 MUTABLE pointer (bucket
``pc-keiba-finish-position-models``, key
``running-style/models/{jra|nar}/latest.flatbin`` — no version in the key),
not a versioned R2 path, so every registered version is tagged
``r2_serving_key`` / ``r2_key_mutable=true`` to record that the pointer is
not immutable. When a local ``model.txt`` mirror exists it is preferred as
the version source (``file://``); otherwise the version is registered
directly against the R2 URI.

Calibrator: isotonic one-vs-rest, canonical tracked files at
``docs/finish-position-accuracy/calibrators/{jra,nar}-rs-v3-calibrators.json``
(schema: ``{category, fit_year, classes, calibrators: {class: {x, y}}}``).

★ Regime guardrail: RS evaluation numbers exist in two very different
regimes — true out-of-sample (JRA ~48.3%, NAR ~52.3%) vs. a leaky
self-consistency regime that reports a misleading ~94-95%
(docs/finish-position-accuracy/history/rs-model-audit.md). This module only
registers model *versions*, not eval runs, so it does not carry that
guardrail itself — see ``ingest_eval.py``'s ``eval_regime`` parameter for the
enforcement point.

A ``running_style_model_bucket_evaluations`` PG table also holds per-cell
additive confusion counts; this module (and the rest of this package) never
connects to a database directly (Neon cost rule) — export its rows to
parquet and feed them through ``ingest_eval.ingest_cell_report`` instead.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Final, TypedDict

from mlflow import MlflowClient
from mlflow.entities import Metric, Param, RunTag
from mlflow.entities.model_registry import ModelVersion

from mlflow_tracking import config, registry
from mlflow_tracking.logging_api import (
    get_or_create_experiment,
    log_batch_chunked,
    log_json_artifact,
    route_json_fields,
)

EXPERIMENT_REGISTRY_BACKFILL: Final[str] = config.EXPERIMENT_RS_REGISTRY_BACKFILL
TASK: Final[registry.Task] = "running-style"

# Running-style covers only jra/nar; Ban-ei is out of scope (see module docstring).
RS_CATEGORIES: Final[tuple[registry.Category, registry.Category]] = ("jra", "nar")

DEFAULT_ARTIFACT_ROOT: Final[Path] = (
    config.REPO_ROOT / "apps" / "pc-keiba-viewer" / "tmp" / "models"
)
DEFAULT_CALIBRATOR_DIR: Final[Path] = (
    config.REPO_ROOT / "docs" / "finish-position-accuracy" / "calibrators"
)

MODEL_FILE_NAME: Final[str] = "model.txt"
PRODUCTION_VERSION_SUFFIX: Final[str] = "-running-style-lgbm-prod-v3"

R2_SERVING_BUCKET: Final[str] = "pc-keiba-finish-position-models"

RS_IDENTITY_TAG_KEYS: Final[frozenset[str]] = frozenset(
    {"model_version", "feature_schema_version", "class_weight_scheme"}
)
RS_FLATTEN_PARAM_KEYS: Final[frozenset[str]] = frozenset({"hyperparameters"})
WF_RESULTS_KEY: Final[str] = "walk_forward_results"
WF_METRIC_KEYS: Final[tuple[str, ...]] = (
    "precision_nige",
    "recall_nige",
    "log_loss_nige",
    "multi_log_loss",
    "accuracy",
    "train_rows",
    "valid_rows",
)
PRODUCTION_METRIC_KEY: Final[str] = "production_precision_nige"

CALIBRATOR_TAG: Final[str] = "isotonic-ovr"


class RunningStyleBackfillSummary(TypedDict):
    versions_registered: int
    errors: list[str]
    champion_sync: dict[str, str]


def production_version_label(category: str) -> str:
    """Return the known-production model_version label, e.g. 'jra-running-style-lgbm-prod-v3'."""
    return f"{category}{PRODUCTION_VERSION_SUFFIX}"


def build_r2_serving_key(category: str) -> str:
    """Build the mutable R2 serving pointer key for a category."""
    return f"running-style/models/{category}/latest.flatbin"


def build_r2_serving_uri(category: str) -> str:
    """Build the s3:// URI for a category's R2 serving pointer."""
    return f"s3://{R2_SERVING_BUCKET}/{build_r2_serving_key(category)}"


def calibrator_path_for(category: str, calibrator_dir: Path = DEFAULT_CALIBRATOR_DIR) -> Path:
    """Return the canonical tracked calibrator JSON path for a category."""
    return calibrator_dir / f"{category}-rs-v3-calibrators.json"


def _infer_category(label: str) -> registry.Category | None:
    """Infer jra/nar from a version label or directory name prefix.

    RS metadata.json carries no explicit "category" field, unlike
    finish-position's, so the category is inferred from the
    ``{category}-running-style-lgbm-...`` naming convention.
    """
    for category in RS_CATEGORIES:
        if label.startswith(f"{category}-"):
            return category
    return None


def _looks_like_running_style_metadata(metadata: object) -> bool:
    """Return True when `metadata` has the RS-specific schema markers.

    Distinguishes a genuine running-style metadata.json from an unrelated
    finish-position metadata.json that happens to live in the same shared
    staging directory.
    """
    return (
        isinstance(metadata, dict) and "class_labels" in metadata and "feature_columns" in metadata
    )


def discover_metadata_files(artifact_root: Path) -> list[Path]:
    """Return metadata.json paths for running-style version dirs directly under `artifact_root`.

    `artifact_root` may be a shared staging directory also holding unrelated
    finish-position artifacts, so each candidate is content-checked via
    `_looks_like_running_style_metadata` rather than trusted by path alone.
    """
    if not artifact_root.is_dir():
        return []
    result: list[Path] = []
    for path in sorted(artifact_root.glob("*/metadata.json")):
        try:
            metadata = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if _looks_like_running_style_metadata(metadata):
            result.append(path)
    return result


def _log_walk_forward_metrics(
    client: MlflowClient, run_id: str, walk_forward_results: dict[str, object]
) -> None:
    """Log each holdout year's walk-forward metrics as a metric series (step=year)."""
    metric_items: list[Metric] = []
    for year_key, year_metrics in walk_forward_results.items():
        if not isinstance(year_metrics, dict):
            continue
        try:
            step = int(year_key)
        except ValueError:
            step = 0
        for metric_key in WF_METRIC_KEYS:
            value = year_metrics.get(metric_key)
            if isinstance(value, int | float):
                metric_items.append(Metric(f"wf_{metric_key}", float(value), 0, step))
    if metric_items:
        log_batch_chunked(client, run_id, metrics=metric_items)


def _attach_calibrator(
    client: MlflowClient,
    run_id: str,
    category: str,
    calibrator_dir: Path,
) -> int | None:
    """Log the category's canonical calibrator JSON as a run artifact.

    Returns the calibrator's `fit_year` when found and well-formed, else None.
    """
    calibrator_path = calibrator_path_for(category, calibrator_dir)
    if not calibrator_path.is_file():
        return None
    calibrator_payload = json.loads(calibrator_path.read_text(encoding="utf-8"))
    log_json_artifact(client, run_id, calibrator_path.name, calibrator_payload)
    if isinstance(calibrator_payload, dict):
        fit_year = calibrator_payload.get("fit_year")
        if isinstance(fit_year, int):
            return fit_year
    return None


def _base_version_tags(category: str, calibrator_fit_year: int | None) -> dict[str, str]:
    tags = registry.version_tags(category=category, task=TASK)
    tags["r2_serving_key"] = build_r2_serving_key(category)
    tags["r2_key_mutable"] = "true"
    if calibrator_fit_year is not None:
        tags["calibrator"] = CALIBRATOR_TAG
        tags["calibrator_fit_year"] = str(calibrator_fit_year)
    return tags


def backfill_one_running_style_metadata(
    client: MlflowClient,
    metadata_path: Path,
    calibrator_dir: Path = DEFAULT_CALIBRATOR_DIR,
) -> ModelVersion:
    """Register one running-style model version from its metadata.json (+ model.txt if present)."""
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    model_version_label = str(metadata.get("model_version", metadata_path.parent.name))
    category = _infer_category(model_version_label) or _infer_category(metadata_path.parent.name)
    if category is None:
        raise ValueError(
            f"cannot infer jra/nar category from model_version: {model_version_label!r}"
        )

    routable_metadata = {k: v for k, v in metadata.items() if k != WF_RESULTS_KEY}
    params, tags, artifact_payloads = route_json_fields(
        routable_metadata,
        identity_tag_keys=RS_IDENTITY_TAG_KEYS,
        flatten_param_keys=RS_FLATTEN_PARAM_KEYS,
    )
    train_start = metadata.get("train_start_date")
    train_end = metadata.get("train_end_date")
    train_window = (
        f"{train_start}-{train_end}"
        if isinstance(train_start, str) and isinstance(train_end, str)
        else None
    )

    experiment_id = get_or_create_experiment(client, EXPERIMENT_REGISTRY_BACKFILL)
    run = client.create_run(experiment_id, run_name=model_version_label)
    run_id = run.info.run_id
    log_batch_chunked(
        client,
        run_id,
        params=[Param(k, v) for k, v in params.items()],
        tags=[RunTag(k, v) for k, v in tags.items()],
    )
    for artifact_key, payload in artifact_payloads.items():
        log_json_artifact(client, run_id, f"{artifact_key}.json", payload)

    walk_forward_results = metadata.get(WF_RESULTS_KEY)
    if isinstance(walk_forward_results, dict):
        _log_walk_forward_metrics(client, run_id, walk_forward_results)

    production_precision = metadata.get(PRODUCTION_METRIC_KEY)
    if isinstance(production_precision, int | float):
        log_batch_chunked(
            client,
            run_id,
            metrics=[Metric(PRODUCTION_METRIC_KEY, float(production_precision), 0, 0)],
        )

    calibrator_fit_year = _attach_calibrator(client, run_id, category, calibrator_dir)
    client.set_terminated(run_id, status="FINISHED")

    registered_name = registry.registered_model_name(category, TASK)
    model_file = metadata_path.parent / MODEL_FILE_NAME
    source_uri = (
        f"file://{metadata_path.parent.resolve()}"
        if model_file.is_file()
        else build_r2_serving_uri(category)
    )
    new_version_tags = _base_version_tags(category, calibrator_fit_year)
    # `tags` (from route_json_fields) already carries the RS_IDENTITY_TAG_KEYS
    # values (model_version/feature_schema_version/class_weight_scheme) as
    # run tags; mirror them onto the version too, the same way finish-position
    # duplicates architecture/category from run tags onto the version.
    new_version_tags.update(tags)
    new_version_tags["model_version"] = model_version_label
    if train_window is not None:
        new_version_tags["train_window"] = train_window
    return registry.register_version(
        client, registered_name, source_uri, run_id=run_id, tags=new_version_tags
    )


def register_production_pointer_if_missing(
    client: MlflowClient,
    category: registry.Category,
    calibrator_dir: Path = DEFAULT_CALIBRATOR_DIR,
) -> ModelVersion | None:
    """Ensure a registry entry exists for `category`'s known production version.

    A no-op when a version already carries the production model_version
    label (e.g. because `backfill_one_running_style_metadata` already
    registered it from a local mirror). Otherwise registers a version
    directly against the R2 serving URI — no local model.txt/metadata.json
    mirror is required for this fallback path, only the mutable R2 pointer.
    """
    registered_name = registry.registered_model_name(category, TASK)
    production_label = production_version_label(category)
    if registry.find_version_by_label(client, registered_name, production_label) is not None:
        return None

    experiment_id = get_or_create_experiment(client, EXPERIMENT_REGISTRY_BACKFILL)
    run = client.create_run(experiment_id, run_name=production_label)
    run_id = run.info.run_id
    calibrator_fit_year = _attach_calibrator(client, run_id, category, calibrator_dir)
    client.set_terminated(run_id, status="FINISHED")

    new_version_tags = _base_version_tags(category, calibrator_fit_year)
    new_version_tags["model_version"] = production_label
    source_uri = build_r2_serving_uri(category)
    return registry.register_version(
        client, registered_name, source_uri, run_id=run_id, tags=new_version_tags
    )


def _sync_running_style_champions(client: MlflowClient) -> dict[str, str]:
    """Set the champion alias to each category's production version label."""
    result: dict[str, str] = {}
    for category in RS_CATEGORIES:
        registered_name = registry.registered_model_name(category, TASK)
        production_label = production_version_label(category)
        mlflow_version = registry.find_version_by_label(client, registered_name, production_label)
        if mlflow_version is None:
            result[category] = (
                f"skipped: no registered version tagged model_version={production_label}"
            )
            continue
        registry.set_champion(client, registered_name, mlflow_version)
        result[category] = "ok"
    return result


def backfill_running_style(
    client: MlflowClient,
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
    calibrator_dir: Path = DEFAULT_CALIBRATOR_DIR,
) -> RunningStyleBackfillSummary:
    """Backfill every running-style model version found under `artifact_root`,
    ensure a production-pointer entry exists for jra/nar even without a local
    mirror, and sync the champion alias to each category's production version.

    Never raises for an individual artifact — a malformed metadata.json is
    recorded in `errors` and skipped so one bad artifact cannot block the rest.
    """
    errors: list[str] = []
    count = 0
    for metadata_path in discover_metadata_files(artifact_root):
        try:
            backfill_one_running_style_metadata(client, metadata_path, calibrator_dir)
            count += 1
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            errors.append(f"{metadata_path}: {exc}")

    for category in RS_CATEGORIES:
        try:
            if register_production_pointer_if_missing(client, category, calibrator_dir) is not None:
                count += 1
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            errors.append(f"production pointer ({category}): {exc}")

    champion_sync = _sync_running_style_champions(client)
    return RunningStyleBackfillSummary(
        versions_registered=count, errors=errors, champion_sync=champion_sync
    )
