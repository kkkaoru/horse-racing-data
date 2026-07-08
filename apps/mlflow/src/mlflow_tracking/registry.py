"""Model-registry helpers: naming, version registration, and champion/challenger aliases.

Registers model versions by URI reference via
``MlflowClient.create_model_version`` — never by binary upload, since the
booster artifacts already live on disk (baked into the finish-position
container image) or in R2.
"""

from __future__ import annotations

import os
import warnings
from typing import Final, Literal

from mlflow import MlflowClient
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException

Category = Literal["jra", "nar", "banei"]
Task = Literal["finish-position", "running-style"]

CATEGORIES: Final[tuple[Category, ...]] = ("jra", "nar", "banei")
TASKS: Final[tuple[Task, ...]] = ("finish-position", "running-style")

# On-disk category directory name -> canonical registry category. "ban-ei" is
# the directory name used under
# apps/finish-position-predict-container/models/finish-position/; the
# registry always normalizes it to "banei".
DIRECTORY_CATEGORY_ALIASES: Final[dict[str, Category]] = {"ban-ei": "banei"}

FILE_URI_ALLOW_ENV: Final[str] = "MLFLOW_ALLOW_FILE_URI_AS_MODEL_VERSION_SOURCE"

CHAMPION_ALIAS: Final[str] = "champion"
CHALLENGER_ALIAS: Final[str] = "challenger"


def normalize_category(raw: str) -> Category:
    """Normalize a directory-style category (e.g. "ban-ei") to the registry form."""
    aliased = DIRECTORY_CATEGORY_ALIASES.get(raw, raw)
    for category in CATEGORIES:
        if category == aliased:
            return category
    raise ValueError(f"unsupported category: {raw!r}")


def registered_model_name(category: Category, task: Task) -> str:
    """Return the registered-model name, e.g. 'jra-finish-position'."""
    return f"{category}-{task}"


def _allow_file_uri_sources() -> None:
    """Set MLFLOW_ALLOW_FILE_URI_AS_MODEL_VERSION_SOURCE=true if unset, with a warning.

    file:// sources are the norm for local (non-R2) model artifacts in this
    package; some mlflow backends gate them behind this flag, so it is set
    defensively before any file:// registration regardless of the installed
    mlflow version's exact enforcement behavior.
    """
    if FILE_URI_ALLOW_ENV not in os.environ:
        warnings.warn(
            f"Setting {FILE_URI_ALLOW_ENV}=true to allow file:// model-version sources.",
            stacklevel=2,
        )
        os.environ[FILE_URI_ALLOW_ENV] = "true"


def _ensure_registered_model(client: MlflowClient, name: str) -> None:
    try:
        client.get_registered_model(name)
    except MlflowException:
        client.create_registered_model(name)


def register_version(
    client: MlflowClient,
    name: str,
    source_uri: str,
    *,
    run_id: str | None = None,
    tags: dict[str, str] | None = None,
) -> ModelVersion:
    """Register a model version by URI reference (no binary upload).

    Creates the registered model first if it does not already exist.
    """
    _ensure_registered_model(client, name)
    if source_uri.startswith("file://"):
        _allow_file_uri_sources()
    return client.create_model_version(name, source=source_uri, run_id=run_id, tags=tags)


def find_version_by_label(client: MlflowClient, name: str, model_version_label: str) -> str | None:
    """Find the mlflow version number whose `model_version` tag matches `label`.

    Returns None both when no version matches and when the lookup itself
    fails (e.g. malformed filter, backend error) — callers treat "no match"
    and "lookup failed" the same way: skip, don't crash the caller.

    Tries an exact match first; if none exists, falls back to a
    case-insensitive (casefold) match with a warning. This is defense in
    depth against upstream label casing typos (e.g. a metadata.json
    "model_version" that differs only in case from the directory name /
    model_meta.json pointer that references it) that would otherwise make an
    otherwise-registered version invisible to champion-alias sync.
    """
    try:
        versions = client.search_model_versions(f"name='{name}'")
    except MlflowException:
        return None
    casefold_target = model_version_label.casefold()
    casefold_match: str | None = None
    for mv in versions:
        tag_value = mv.tags.get("model_version")
        if tag_value == model_version_label:
            return mv.version
        already_found = casefold_match is not None
        if not already_found and tag_value is not None and tag_value.casefold() == casefold_target:
            casefold_match = mv.version
    if casefold_match is not None:
        warnings.warn(
            f"find_version_by_label: no exact match for model_version={model_version_label!r} "
            f"on {name!r}; using case-insensitive match instead (version {casefold_match})",
            stacklevel=2,
        )
    return casefold_match


def set_champion(client: MlflowClient, name: str, version: str) -> None:
    """Point the 'champion' alias at `version` (MLflow 3.x alias idiom, not legacy stages)."""
    client.set_registered_model_alias(name, CHAMPION_ALIAS, version)


def set_challenger(client: MlflowClient, name: str, version: str) -> None:
    """Point the 'challenger' alias at `version`."""
    client.set_registered_model_alias(name, CHALLENGER_ALIAS, version)


def version_tags(
    *,
    architecture: str | None = None,
    feature_count: int | None = None,
    train_window: str | None = None,
    routing_scope: str | None = None,
    category: str | None = None,
    task: str | None = None,
) -> dict[str, str]:
    """Build the standard model-version tag set, omitting unset fields."""
    fields: dict[str, str | None] = {
        "architecture": architecture,
        "feature_count": None if feature_count is None else str(feature_count),
        "train_window": train_window,
        "routing_scope": routing_scope,
        "category": category,
        "task": task,
    }
    return {k: v for k, v in fields.items() if v is not None}
