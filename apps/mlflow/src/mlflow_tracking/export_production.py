"""Export MLflow registry state into schema-compatible artifacts consumed by
the finish-position production serving path.

These are drop-in-schema CANDIDATES for manual review, not automatic
deploys: `export_active_models` produces the model_meta.json-adjacent shape
(model_versions/subclass_overrides) and `export_cell_routing` produces a
cell_routing.json-compatible fragment for one category. The actual bake
(model_meta.json) / Neon flip (finish_position_active_models) remains an
explicit human/orchestrated deploy step — this module never writes into
apps/finish-position-predict-container (WIP-owned by another session).

cell_router.py's load_cell_router() treats EVERY top-level JSON key as a
category-routing entry with no tolerance for unknown keys (verified
read-only), so provenance is written to a sidecar `<output>.provenance.json`
file instead of being embedded in the export itself.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Protocol

import boto3
from mlflow import MlflowClient
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException

from mlflow_tracking import config, registry

logger: Final = logging.getLogger(__name__)

ACTIVE_MODELS_CATEGORIES: Final[tuple[registry.Category, ...]] = ("jra", "nar", "banei")
CELL_ROUTING_CLASS_DIMENSION: Final[str] = "kyoso_joken_code"
CHAMPION_VARIANT_NAME: Final[str] = "champion"


def default_export_dir() -> Path:
    """Return the default export directory, resolved at call time from
    config.get_data_dir() (not a module-level constant) so it honors
    HORSE_RACING_MLFLOW_DATA_DIR the same way the rest of this package's
    local-mode paths do.
    """
    return config.get_data_dir() / "exports"


def default_active_models_path(export_dir: Path | None = None) -> Path:
    resolved = export_dir if export_dir is not None else default_export_dir()
    return resolved / "active_models.json"


def default_cell_routing_path(category: str, export_dir: Path | None = None) -> Path:
    resolved = export_dir if export_dir is not None else default_export_dir()
    return resolved / f"cell_routing_{category}.json"


def to_cell_routing_category_key(category: registry.Category) -> str:
    """Map the registry's canonical category form to cell_routing.json's
    on-disk key spelling ("banei" -> "ban-ei"; jra/nar pass through)."""
    return "ban-ei" if category == "banei" else category


def write_json_file(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_provenance_sidecar(
    output_path: Path,
    *,
    registry_versions_used: list[str],
    run_ids: list[str],
    missing_categories: list[str] | None = None,
) -> Path:
    """Write a <output>.provenance.json sidecar (never embedded in the export
    itself — cell_router.py's parser treats every top-level key as a category
    routing entry, so it cannot tolerate an extra provenance key).

    `missing_categories` records categories that were skipped for lacking a
    champion alias (only non-empty for a `--allow-missing` partial
    active-models export); always present in the sidecar (as `[]` when not
    applicable) so both export kinds share one schema.
    """
    provenance_path = output_path.with_name(f"{output_path.name}.provenance.json")
    write_json_file(
        provenance_path,
        {
            "exported_at": datetime.now(UTC).isoformat(),
            "registry_versions_used": registry_versions_used,
            "run_ids": sorted({run_id for run_id in run_ids if run_id}),
            "missing_categories": sorted(missing_categories) if missing_categories else [],
        },
    )
    return provenance_path


def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"--upload-r2 must be an s3:// URI, got: {s3_uri!r}")
    bucket, _, key = s3_uri[len("s3://") :].partition("/")
    if not bucket or not key:
        raise ValueError(f"--upload-r2 must be s3://bucket/key, got: {s3_uri!r}")
    return bucket, key


class _PutObjectClient(Protocol):
    # Mirrors config._HeadBucketClient's pattern: boto3's real put_object only
    # accepts capitalized Bucket=/Key=/Body= keywords, so **kwargs (rather
    # than named parameters) avoids re-declaring those non-lowercase names.
    def put_object(self, **kwargs: object) -> object: ...


def upload_to_r2(local_path: Path, s3_uri: str, *, client: _PutObjectClient | None = None) -> None:
    """Upload a local export file to R2 via an s3://bucket/key URI.

    Maps the repo-standard R2 env vars onto the S3-compatible ones first
    (config.apply_r2_env()), same as the artifact-store path. Uses a plain
    put_object (the file is small JSON) rather than the high-level
    upload_file() transfer manager, which is not reliably stub-friendly.
    """
    bucket, key = _parse_s3_uri(s3_uri)
    config.apply_r2_env()
    s3_client = client if client is not None else boto3.client("s3")
    s3_client.put_object(Bucket=bucket, Key=key, Body=local_path.read_bytes())


def _champion_version(client: MlflowClient, registered_name: str) -> ModelVersion | None:
    """Return the champion ModelVersion for `registered_name`, or None if the
    registered model or its champion alias does not exist."""
    try:
        registered = client.get_registered_model(registered_name)
    except MlflowException:
        return None
    champion_version = registered.aliases.get(registry.CHAMPION_ALIAS)
    if champion_version is None:
        return None
    try:
        return client.get_model_version(registered_name, str(champion_version))
    except MlflowException:
        return None


def _search_versions(client: MlflowClient, registered_name: str) -> list[ModelVersion]:
    try:
        return list(client.search_model_versions(f"name='{registered_name}'"))
    except MlflowException:
        return []


def build_active_models_export(
    client: MlflowClient,
) -> tuple[dict[str, object], list[str], list[str], list[str]]:
    """Build the {"model_versions": ..., "subclass_overrides": ...} export.

    `model_versions` comes from each category's champion alias.
    `subclass_overrides` comes from versions tagged routing_scope="class:<code>"
    (the convention backfill_finish_position.py's per-class manifest
    registration already uses). A category with no registered model at all,
    or one registered but with no champion alias set, is reported the same
    way in the returned `missing_categories` list -- both mean "no active
    model for this category", which the caller (export_active_models) must
    not drop from the export silently. Returns (payload,
    registry_versions_used, run_ids, missing_categories).
    """
    model_versions: dict[str, str] = {}
    subclass_overrides: dict[str, dict[str, str]] = {}
    versions_used: list[str] = []
    run_ids: list[str] = []
    missing_categories: list[str] = []

    for category in ACTIVE_MODELS_CATEGORIES:
        registered_name = registry.registered_model_name(category, "finish-position")
        champion_mv = _champion_version(client, registered_name)
        if champion_mv is None:
            missing_categories.append(category)
            continue
        model_versions[category] = champion_mv.tags.get("model_version", "")
        versions_used.append(f"{registered_name}:{champion_mv.version}")
        if champion_mv.run_id:
            run_ids.append(champion_mv.run_id)

        for mv in _search_versions(client, registered_name):
            routing_scope = mv.tags.get("routing_scope", "")
            if not routing_scope.startswith("class:"):
                continue
            class_code = routing_scope.split(":", 1)[1]
            subclass_overrides.setdefault(category, {})[class_code] = mv.tags.get(
                "model_version", ""
            )
            versions_used.append(f"{registered_name}:{mv.version}")
            if mv.run_id:
                run_ids.append(mv.run_id)

    payload: dict[str, object] = {
        "model_versions": model_versions,
        "subclass_overrides": subclass_overrides,
    }
    return payload, versions_used, run_ids, missing_categories


def export_active_models(
    client: MlflowClient,
    output_path: Path | None = None,
    *,
    upload_r2: str | None = None,
    allow_missing: bool = False,
) -> Path:
    """Write the active-models export (+ provenance sidecar), optionally
    uploading it to R2. Returns the output path.

    Mirrors export_cell_routing's existing hard-fail behavior: a category
    with no champion alias is never dropped from the export silently. Raises
    ValueError (unless `allow_missing=True`) naming every such category, so
    a champion-sync gap surfaces as a loud CLI failure instead of a quietly
    incomplete model_versions dict. When `allow_missing=True`, writes the
    partial export anyway and logs a warning, and the missing categories are
    also recorded in the provenance sidecar for later audit.
    """
    resolved_output = output_path if output_path is not None else default_active_models_path()
    payload, versions_used, run_ids, missing_categories = build_active_models_export(client)
    if missing_categories and not allow_missing:
        raise ValueError(
            "missing champion alias for categories: "
            f"{sorted(missing_categories)}; pass --allow-missing to export a partial file"
        )
    if missing_categories:
        logger.warning(
            "export-active-models: writing a partial export; missing champion alias for "
            "categories: %s",
            sorted(missing_categories),
        )
    write_json_file(resolved_output, payload)
    write_provenance_sidecar(
        resolved_output,
        registry_versions_used=versions_used,
        run_ids=run_ids,
        missing_categories=missing_categories,
    )
    if upload_r2 is not None:
        upload_to_r2(resolved_output, upload_r2)
    return resolved_output


def _variant_spec(mv: ModelVersion) -> dict[str, object]:
    try:
        feature_count = int(mv.tags.get("feature_count", "0"))
    except ValueError:
        feature_count = 0
    return {
        "model_version": mv.tags.get("model_version", ""),
        "feature_count": feature_count,
        "architecture": mv.tags.get("architecture", "unknown"),
    }


def build_cell_routing_export(
    client: MlflowClient, category: str
) -> tuple[dict[str, object], list[str], list[str]]:
    """Build a cell_routing.json-compatible fragment for one category from
    the registry's champion alias (-> default_variant) and any
    routing_scope="class:<code>" tagged versions (-> variants + rules).

    Versions tagged with a non-"class:" routing_scope (e.g. carried over from
    a prior cell_routing.json ingestion) are included as variants but WITHOUT
    a reconstructed rule: the original cell-dimension conditions that
    selected them live in the source cell_routing.json's rules, not in a flat
    registry tag, so they are not recoverable here — the operator must
    complete those rules by hand. Returns (payload, registry_versions_used, run_ids).
    """
    resolved_category = registry.normalize_category(category)
    registered_name = registry.registered_model_name(resolved_category, "finish-position")
    champion_mv = _champion_version(client, registered_name)
    if champion_mv is None:
        raise ValueError(f"{registered_name} has no champion alias set")

    versions_used = [f"{registered_name}:{champion_mv.version}"]
    run_ids = [champion_mv.run_id] if champion_mv.run_id else []

    variants: dict[str, object] = {CHAMPION_VARIANT_NAME: _variant_spec(champion_mv)}
    rules: list[dict[str, object]] = []

    for mv in _search_versions(client, registered_name):
        if str(mv.version) == str(champion_mv.version):
            continue
        routing_scope = mv.tags.get("routing_scope", "")
        if not routing_scope:
            continue
        if routing_scope.startswith("class:"):
            class_code = routing_scope.split(":", 1)[1]
            variant_name = f"class-{class_code}"
            variants[variant_name] = _variant_spec(mv)
            rules.append(
                {
                    "conditions": [
                        {"dimension": CELL_ROUTING_CLASS_DIMENSION, "values": [class_code]}
                    ],
                    "variant": variant_name,
                }
            )
        else:
            variants.setdefault(routing_scope, _variant_spec(mv))
        versions_used.append(f"{registered_name}:{mv.version}")
        if mv.run_id:
            run_ids.append(mv.run_id)

    category_key = to_cell_routing_category_key(resolved_category)
    payload: dict[str, object] = {
        category_key: {
            "default_variant": CHAMPION_VARIANT_NAME,
            "variants": variants,
            "rules": rules,
        }
    }
    return payload, versions_used, run_ids


def export_cell_routing(
    client: MlflowClient,
    category: str,
    output_path: Path | None = None,
    *,
    upload_r2: str | None = None,
) -> Path:
    """Write the cell-routing export (+ provenance sidecar), optionally
    uploading it to R2. Returns the output path."""
    resolved_output = (
        output_path if output_path is not None else default_cell_routing_path(category)
    )
    payload, versions_used, run_ids = build_cell_routing_export(client, category)
    write_json_file(resolved_output, payload)
    write_provenance_sidecar(resolved_output, registry_versions_used=versions_used, run_ids=run_ids)
    if upload_r2 is not None:
        upload_to_r2(resolved_output, upload_r2)
    return resolved_output
