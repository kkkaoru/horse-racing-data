"""Ingest a single training-run manifest into MLflow.

Consumed by env-gated hooks in the pc-keiba-viewer trainers, which call:
``uv run --project <repo>/apps/mlflow python -m mlflow_tracking.cli
log-training-run <manifest-path>``. The manifest schema
(``hr-mlflow-training-run/v1``) is a fixed cross-repo contract — do not change
its field names/shape without also updating the trainer-side hook.

Manifest shape (``?`` = optional, defaults noted)::

    {
      "schema": "hr-mlflow-training-run/v1",
      "task": "finish-position" | "running-style",
      "category": "jra" | "nar" | "banei",
      "model_version": "<string>",
      "artifact_dir": "<abs path containing metadata.json>",   ?
      "eval_regime": "wf" | "oos" | "serve" | "self-consistency" | "unspecified",
      "experiment": "<explicit experiment name override>",     ?
      "cell_report": "<abs path to a parquet|json cell table>", ?
      "aggregate_metrics": {"<metric>": <number>},              ?
      "params": {...},                                          ?
      "tags": {...},                                             ?
      "register": false,   ? (default false)
      "champion": false    ? (default false)
    }

`register`/`champion` are best-effort side effects on top of the logged run;
everything else is pure logging. The caller (hook) treats a non-zero CLI exit
as a non-fatal warning, so this module never leaves a run half-open on error
paths that occur after run creation — it always terminates what it started.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Final, cast

from mlflow import MlflowClient
from mlflow.entities import Metric, Param, RunTag

from mlflow_tracking import config, ingest_eval, logging_api, registry
from mlflow_tracking.logging_api import (
    get_or_create_experiment,
    log_batch_chunked,
    log_json_artifact,
    route_json_fields,
)

MANIFEST_SCHEMA: Final[str] = "hr-mlflow-training-run/v1"

VALID_TASKS: Final[frozenset[str]] = frozenset({"finish-position", "running-style"})

# Generic (family-agnostic) metadata.json routing: the union of
# finish-position's and running-style's identity/flatten keys, since
# artifact_dir's metadata.json could be either shape and this ingestion path
# has no other signal to tell which.
GENERIC_IDENTITY_TAG_KEYS: Final[frozenset[str]] = frozenset(
    {
        "model_version",
        "architecture",
        "category",
        "note",
        "feature_schema_version",
        "class_weight_scheme",
    }
)
GENERIC_FLATTEN_PARAM_KEYS: Final[frozenset[str]] = frozenset(
    {"hyperparams", "hyperparameters", "scale_and_bias"}
)


def _resolve_experiment(task: str, eval_regime: str) -> str:
    """Pick the experiment by task+eval_regime (running-style has one eval
    experiment regardless of regime; finish-position splits wf vs serve)."""
    if task == "running-style":
        return config.EXPERIMENT_RS_EVAL
    if eval_regime == "serve":
        return config.EXPERIMENT_FP_SERVE_ACCURACY
    return config.EXPERIMENT_FP_WF_EVAL


def _require_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"manifest '{key}' must be a non-empty string")
    return value


def _optional_str(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"manifest '{key}' must be a non-empty string when present")
    return value


def _optional_dict(payload: dict[str, object], key: str) -> dict[str, object]:
    value = payload.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"manifest '{key}' must be an object when present")
    return cast(dict[str, object], value)


def _optional_bool(payload: dict[str, object], key: str) -> bool:
    value = payload.get(key, False)
    return bool(value)


def _ingest_artifact_dir(client: MlflowClient, run_id: str, artifact_dir: Path) -> None:
    """Ingest artifact_dir's metadata.json the same way the backfill_*
    modules do for their own metadata.json (family-agnostic key routing)."""
    metadata_path = artifact_dir / "metadata.json"
    if not metadata_path.is_file():
        return
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(metadata, dict):
        return
    params, tags, artifact_payloads = route_json_fields(
        metadata,
        identity_tag_keys=GENERIC_IDENTITY_TAG_KEYS,
        flatten_param_keys=GENERIC_FLATTEN_PARAM_KEYS,
    )
    log_batch_chunked(
        client,
        run_id,
        params=[Param(k, v) for k, v in params.items()],
        tags=[RunTag(k, v) for k, v in tags.items()],
    )
    for artifact_key, payload in artifact_payloads.items():
        log_json_artifact(client, run_id, f"{artifact_key}.json", payload)


def _ingest_cell_report(client: MlflowClient, run_id: str, cell_report_path: Path) -> None:
    """Ingest cell_report through the same normalize + log_table + parquet
    path as ingest_eval.ingest_cell_report, but into an already-open run
    rather than creating a new one."""
    cell_df = ingest_eval.normalize_cell_dataframe(ingest_eval.read_cell_table(cell_report_path))
    headline = logging_api.select_headline_metrics(cell_df)
    if headline:
        headline_items = [Metric(k, v, 0, 0) for k, v in headline.items()]
        log_batch_chunked(client, run_id, metrics=headline_items)
    logging_api.log_cell_table(client, run_id, cell_df)


def _register_and_promote(
    client: MlflowClient,
    run_id: str,
    task: registry.Task,
    category: registry.Category,
    model_version_label: str,
    artifact_dir: str | None,
    should_set_champion: bool,
) -> None:
    registered_name = registry.registered_model_name(category, task)
    source_uri = (
        f"file://{Path(artifact_dir).resolve()}" if artifact_dir is not None else f"runs:/{run_id}"
    )
    version_tags = registry.version_tags(category=category, task=task)
    version_tags["model_version"] = model_version_label
    version = registry.register_version(
        client, registered_name, source_uri, run_id=run_id, tags=version_tags
    )
    if should_set_champion:
        registry.set_champion(client, registered_name, version.version)


def log_training_run(client: MlflowClient, manifest_path: Path) -> str:
    """Ingest one training-run manifest, returning the created run_id.

    Raises ValueError/OSError/json.JSONDecodeError on a malformed manifest —
    callers (the CLI) are expected to catch these and exit non-zero with a
    clear message, per the fixed manifest contract's error-handling policy.
    """
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"manifest must be a JSON object: {manifest_path}")

    schema = payload.get("schema")
    if schema != MANIFEST_SCHEMA:
        raise ValueError(f"unsupported manifest schema: {schema!r} (expected {MANIFEST_SCHEMA!r})")

    task_raw = _require_str(payload, "task")
    if task_raw not in VALID_TASKS:
        raise ValueError(f"manifest 'task' must be one of {sorted(VALID_TASKS)}, got: {task_raw!r}")
    task: registry.Task = "finish-position" if task_raw == "finish-position" else "running-style"

    category = registry.normalize_category(_require_str(payload, "category"))
    if task == "running-style" and category == "banei":
        raise ValueError("running-style does not support category=banei (Ban-ei is out of scope)")

    model_version_label = _require_str(payload, "model_version")
    eval_regime = _require_str(payload, "eval_regime")
    ingest_eval.validate_eval_regime(eval_regime)

    experiment_override = _optional_str(payload, "experiment")
    resolved_experiment = (
        experiment_override
        if experiment_override is not None
        else _resolve_experiment(task, eval_regime)
    )

    manifest_params = _optional_dict(payload, "params")
    manifest_tags = _optional_dict(payload, "tags")
    aggregate_metrics = _optional_dict(payload, "aggregate_metrics")

    experiment_id = get_or_create_experiment(client, resolved_experiment)
    run = client.create_run(experiment_id, run_name=model_version_label)
    run_id = run.info.run_id

    tags: dict[str, str] = {
        "eval_regime": eval_regime,
        "task": task,
        "category": category,
        "model_version": model_version_label,
    }
    tags.update({str(k): str(v) for k, v in manifest_tags.items()})
    params: dict[str, str] = {str(k): str(v) for k, v in manifest_params.items()}
    metric_items = [
        Metric(str(k), float(v), 0, 0)
        for k, v in aggregate_metrics.items()
        if isinstance(v, int | float)
    ]
    log_batch_chunked(
        client,
        run_id,
        metrics=metric_items,
        params=[Param(k, v) for k, v in params.items()],
        tags=[RunTag(k, v) for k, v in tags.items()],
    )

    artifact_dir = _optional_str(payload, "artifact_dir")
    if artifact_dir is not None:
        _ingest_artifact_dir(client, run_id, Path(artifact_dir))

    cell_report = _optional_str(payload, "cell_report")
    if cell_report is not None:
        _ingest_cell_report(client, run_id, Path(cell_report))

    client.set_terminated(run_id, status="FINISHED")

    if _optional_bool(payload, "register"):
        _register_and_promote(
            client,
            run_id,
            task,
            category,
            model_version_label,
            artifact_dir,
            _optional_bool(payload, "champion"),
        )

    return run_id
