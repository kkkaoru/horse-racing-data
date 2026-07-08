"""Backfill MLflow registry versions from the finish-position container's
on-disk model artifacts (metadata.json / manifest.json / model_meta.json /
cell_routing.json).

Every public function takes the models root / predict_lib root path as a
parameter (default = the real repo path) so tests exercise fixture trees
instead of the live models/ directory.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Final, TypedDict, cast

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

logger: Final = logging.getLogger(__name__)

EXPERIMENT_REGISTRY_BACKFILL: Final[str] = config.EXPERIMENT_FP_REGISTRY_BACKFILL
TASK: Final[registry.Task] = "finish-position"

DEFAULT_MODELS_ROOT: Final[Path] = (
    config.REPO_ROOT / "apps" / "finish-position-predict-container" / "models" / "finish-position"
)
DEFAULT_PREDICT_LIB_ROOT: Final[Path] = (
    config.REPO_ROOT / "apps" / "finish-position-predict-container" / "src" / "predict_lib"
)

METADATA_IDENTITY_TAG_KEYS: Final[frozenset[str]] = frozenset(
    {"model_version", "architecture", "category", "note"}
)
METADATA_FLATTEN_PARAM_KEYS: Final[frozenset[str]] = frozenset({"hyperparams", "scale_and_bias"})

MANIFEST_IDENTITY_TAG_KEYS: Final[frozenset[str]] = frozenset(
    {
        "model_version",
        "category",
        "ensemble_type",
        "baseline_version",
        "source_experiment",
        "note",
        "search_method",
    }
)
MANIFEST_FLATTEN_PARAM_KEYS: Final[frozenset[str]] = frozenset(
    {"validation_window", "holdout_window"}
)
MANIFEST_METRIC_KEYS: Final[tuple[str, ...]] = (
    "holdout_top1",
    "baseline_holdout_top1",
    "delta_pp",
)


class BackfillSummary(TypedDict):
    base_versions_registered: int
    per_class_versions_registered: int
    errors: list[str]
    champion_sync: dict[str, str]
    cell_routing_run_id: str | None


def discover_base_metadata_files(models_root: Path) -> list[Path]:
    """Return metadata.json paths for category-level (non-per-class) model dirs."""
    if not models_root.is_dir():
        return []
    return sorted(models_root.glob("*/*/metadata.json"))


def discover_per_class_manifests(models_root: Path) -> list[Path]:
    """Return manifest.json paths for per-class ensemble model dirs."""
    if not models_root.is_dir():
        return []
    return sorted(models_root.glob("*/per-class/*/*/manifest.json"))


def _resolve_model_version_label(raw_value: object, directory_name: str) -> tuple[str, str | None]:
    """Resolve the canonical `model_version` label for a registered version.

    Every production pointer (model_meta.json / cell_routing.json) references
    the artifact's on-disk DIRECTORY name, not whatever string metadata.json
    or manifest.json happens to carry, so the directory name always wins as
    the canonical label. When the recorded value differs (e.g. a metadata.json
    typo like "-CLEAN" vs. the real "-clean" directory), that would otherwise
    make registry.find_version_by_label's lookup (keyed off the directory
    name via model_meta.json/cell_routing.json) silently never match this
    version. The raw value is preserved via the second return value so the
    caller can tag it separately instead of discarding it.

    Returns (canonical_label, raw_value_if_it_differed_else_None).
    """
    if raw_value is None:
        return directory_name, None
    raw_label = str(raw_value)
    if raw_label == directory_name:
        return directory_name, None
    logger.warning(
        "model_version=%r does not match artifact directory name=%r; registering under "
        "the directory name (raw value kept as metadata_model_version_raw tag)",
        raw_label,
        directory_name,
    )
    return directory_name, raw_label


def backfill_one_metadata_file(
    client: MlflowClient,
    metadata_path: Path,
    models_root: Path = DEFAULT_MODELS_ROOT,
) -> ModelVersion:
    """Register one base model version from its metadata.json."""
    raw_category = metadata_path.relative_to(models_root).parts[0]
    category = registry.normalize_category(raw_category)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    model_version_label, metadata_model_version_raw = _resolve_model_version_label(
        metadata.get("model_version"), metadata_path.parent.name
    )
    params, tags, artifact_payloads = route_json_fields(
        metadata,
        identity_tag_keys=METADATA_IDENTITY_TAG_KEYS,
        flatten_param_keys=METADATA_FLATTEN_PARAM_KEYS,
    )
    tags.setdefault("category", category)

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
    client.set_terminated(run_id, status="FINISHED")

    registered_name = registry.registered_model_name(category, TASK)
    train_date_range = metadata.get("train_date_range")
    train_window = "-".join(train_date_range) if isinstance(train_date_range, list) else None
    new_version_tags = registry.version_tags(
        architecture=str(metadata.get("architecture")) if metadata.get("architecture") else None,
        feature_count=metadata.get("feature_count")
        if isinstance(metadata.get("feature_count"), int)
        else None,
        train_window=train_window,
        category=category,
        task=TASK,
    )
    new_version_tags["model_version"] = model_version_label
    if metadata_model_version_raw is not None:
        new_version_tags["metadata_model_version_raw"] = metadata_model_version_raw
    source_uri = f"file://{metadata_path.parent.resolve()}"
    return registry.register_version(
        client, registered_name, source_uri, run_id=run_id, tags=new_version_tags
    )


def backfill_one_manifest(
    client: MlflowClient,
    manifest_path: Path,
    models_root: Path = DEFAULT_MODELS_ROOT,
) -> ModelVersion:
    """Register one per-class ensemble version from its manifest.json."""
    relative_parts = manifest_path.relative_to(models_root).parts
    # {category}/per-class/{class_code}/{version}/manifest.json
    raw_category, class_code = relative_parts[0], relative_parts[2]
    category = registry.normalize_category(raw_category)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    model_version_label, metadata_model_version_raw = _resolve_model_version_label(
        manifest.get("model_version"), manifest_path.parent.name
    )
    params, tags, artifact_payloads = route_json_fields(
        manifest,
        identity_tag_keys=MANIFEST_IDENTITY_TAG_KEYS,
        flatten_param_keys=MANIFEST_FLATTEN_PARAM_KEYS,
    )
    tags["category"] = category
    tags["class_code"] = class_code

    experiment_id = get_or_create_experiment(client, EXPERIMENT_REGISTRY_BACKFILL)
    run = client.create_run(experiment_id, run_name=model_version_label)
    run_id = run.info.run_id
    metric_items = [
        Metric(key, float(manifest[key]), 0, 0)
        for key in MANIFEST_METRIC_KEYS
        if isinstance(manifest.get(key), int | float)
    ]
    log_batch_chunked(
        client,
        run_id,
        metrics=metric_items,
        params=[Param(k, v) for k, v in params.items()],
        tags=[RunTag(k, v) for k, v in tags.items()],
    )
    for artifact_key, payload in artifact_payloads.items():
        log_json_artifact(client, run_id, f"{artifact_key}.json", payload)
    log_json_artifact(client, run_id, "manifest.json", manifest)
    client.set_terminated(run_id, status="FINISHED")

    registered_name = registry.registered_model_name(category, TASK)
    new_version_tags = registry.version_tags(
        category=category, task=TASK, routing_scope=f"class:{class_code}"
    )
    new_version_tags["model_version"] = model_version_label
    if metadata_model_version_raw is not None:
        new_version_tags["metadata_model_version_raw"] = metadata_model_version_raw
    new_version_tags["class_code"] = class_code
    if isinstance(manifest.get("ensemble_type"), str):
        new_version_tags["ensemble_type"] = manifest["ensemble_type"]
    source_uri = f"file://{manifest_path.parent.resolve()}"
    return registry.register_version(
        client, registered_name, source_uri, run_id=run_id, tags=new_version_tags
    )


def sync_champions(
    client: MlflowClient,
    predict_lib_root: Path = DEFAULT_PREDICT_LIB_ROOT,
) -> dict[str, str]:
    """Read model_meta.json and set the champion alias on each category's
    matching registered version (matched by the `model_version` tag).

    Returns a dict of category -> "ok" | "skipped: <reason>" for reporting.
    Never raises for an individual category: a missing/unregistered
    model_version is reported, not fatal, so it never blocks the others.
    """
    meta_path = predict_lib_root / "model_meta.json"
    result: dict[str, str] = {}
    if not meta_path.is_file():
        return result
    payload = json.loads(meta_path.read_text(encoding="utf-8"))
    model_versions = payload.get("model_versions")
    if not isinstance(model_versions, dict):
        return result
    for raw_category, model_version_label in model_versions.items():
        try:
            category = registry.normalize_category(str(raw_category))
        except ValueError as exc:
            result[str(raw_category)] = f"skipped: {exc}"
            continue
        registered_name = registry.registered_model_name(category, TASK)
        mlflow_version = registry.find_version_by_label(
            client, registered_name, str(model_version_label)
        )
        if mlflow_version is None:
            result[category] = (
                f"skipped: no registered version tagged model_version={model_version_label}"
            )
            continue
        registry.set_champion(client, registered_name, mlflow_version)
        result[category] = "ok"
    return result


def ingest_cell_routing(
    client: MlflowClient,
    predict_lib_root: Path = DEFAULT_PREDICT_LIB_ROOT,
) -> str | None:
    """Upload cell_routing.json as an artifact on a routing run and tag the
    model versions its rules reference with routing_scope.

    Returns the created run_id, or None if cell_routing.json is absent.
    Gracefully skips (never raises) a routed model_version with no matching
    registered version.
    """
    routing_path = predict_lib_root / "cell_routing.json"
    if not routing_path.is_file():
        return None
    payload = json.loads(routing_path.read_text(encoding="utf-8"))

    experiment_id = get_or_create_experiment(client, EXPERIMENT_REGISTRY_BACKFILL)
    run = client.create_run(experiment_id, run_name="cell-routing-sync")
    run_id = run.info.run_id
    log_json_artifact(client, run_id, "cell_routing.json", payload)

    for raw_category, category_payload in payload.items():
        _sync_cell_routing_category(client, raw_category, category_payload)

    client.set_terminated(run_id, status="FINISHED")
    return run_id


def _sync_cell_routing_category(
    client: MlflowClient, raw_category: str, category_payload: object
) -> None:
    try:
        category = registry.normalize_category(str(raw_category))
    except ValueError:
        return
    if not isinstance(category_payload, dict):
        return
    registered_name = registry.registered_model_name(category, TASK)
    rules = category_payload.get("rules")
    variants = category_payload.get("variants")
    if not isinstance(rules, list) or not isinstance(variants, dict):
        return
    variant_names = {rule.get("variant") for rule in rules if isinstance(rule, dict)}
    typed_variants = cast(Mapping[str, object], variants)
    for variant_name in variant_names:
        _tag_routed_variant(client, registered_name, category, variant_name, typed_variants)


def _tag_routed_variant(
    client: MlflowClient,
    registered_name: str,
    category: str,
    variant_name: object,
    variants: Mapping[str, object],
) -> None:
    if not isinstance(variant_name, str):
        return
    variant = variants.get(variant_name)
    if not isinstance(variant, dict):
        return
    model_version_label = variant.get("model_version")
    if not isinstance(model_version_label, str):
        return
    mlflow_version = registry.find_version_by_label(client, registered_name, model_version_label)
    if mlflow_version is None:
        logger.warning(
            "cell_routing.json references model_version=%s (%s) with no registered "
            "version; skipping",
            model_version_label,
            category,
        )
        return
    client.set_model_version_tag(registered_name, mlflow_version, "routing_scope", variant_name)


def backfill_finish_position(
    client: MlflowClient,
    models_root: Path = DEFAULT_MODELS_ROOT,
    predict_lib_root: Path = DEFAULT_PREDICT_LIB_ROOT,
) -> BackfillSummary:
    """Run the full finish-position registry backfill: base versions,
    per-class ensembles, champion alias sync, and cell_routing.json ingestion.

    Never raises for an individual artifact — a malformed or half-written
    metadata.json/manifest.json is recorded in `errors` and skipped so one bad
    artifact cannot block the rest of the backfill.
    """
    errors: list[str] = []
    base_count = 0
    for metadata_path in discover_base_metadata_files(models_root):
        try:
            backfill_one_metadata_file(client, metadata_path, models_root)
            base_count += 1
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            errors.append(f"{metadata_path}: {exc}")

    per_class_count = 0
    for manifest_path in discover_per_class_manifests(models_root):
        try:
            backfill_one_manifest(client, manifest_path, models_root)
            per_class_count += 1
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            errors.append(f"{manifest_path}: {exc}")

    champion_sync = sync_champions(client, predict_lib_root)
    cell_routing_run_id = ingest_cell_routing(client, predict_lib_root)

    return BackfillSummary(
        base_versions_registered=base_count,
        per_class_versions_registered=per_class_count,
        errors=errors,
        champion_sync=champion_sync,
        cell_routing_run_id=cell_routing_run_id,
    )
