"""Deterministic production-artifact manifest validation and local verification.

The tracked manifest is a digest contract, not an artifact store. Production
selectors remain authoritative; this module derives their reachable keys and
fails closed when a selected key is absent from the manifest or its observed
bytes differ. Unselected historical objects are warnings only.

Scope: this module only ever reads LOCAL build-context bytes -- a staged
``models/`` directory before a container build, or the container image's own
``/models`` tree at ``docker build`` time. It performs no network
re-verification of the live R2 running-style objects (see
``ArtifactObservation`` and DEPLOY.md's Scope note); those are attestation-
recorded only. If a future caller adds a network-based observer, an auth or
network failure MUST report ``state="unavailable"`` (-> ``INDETERMINATE``),
never ``"missing"`` or a fabricated ``"present"`` -- a transient network issue
must never silently block or silently pass an ordinary offline build.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Final, Literal, cast

from .cell_router import load_cell_router
from .deploy_flags import DEPLOY_FLAGS_PATH, load_deploy_flags
from .model_meta import (
    CATEGORIES,
    METADATA_FILE_NAME,
    MODEL_FILE_NAME,
    MODEL_META_JSON_PATH,
    NAR_TRANSFORMER_MODEL_VERSION,
    load_model_meta,
)
from .running_style_routing import (
    RUNNING_STYLE_ROUTING_PATH,
    RunningStyleCategoryRouting,
    load_running_style_cell_routing,
)

IntegrityStatus = Literal["MATCH", "INTEGRITY_FAILURE", "INDETERMINATE"]
ObservationState = Literal["present", "missing", "unavailable"]
ArtifactSystem = Literal["finish-position", "running-style"]
ArtifactCategory = Literal["jra", "nar", "ban-ei"]
ActivationMode = Literal["required", "conditional"]

SCHEMA_VERSION: Final[str] = "production-artifacts/v1"
SHA256_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
MANIFEST_FILE_NAME: Final[str] = "production-artifacts.json"
MANIFEST_PATH: Final[Path] = Path(__file__).parents[2] / MANIFEST_FILE_NAME
CELL_ROUTING_PATH: Final[Path] = Path(__file__).parent / "cell_routing.json"
FINISH_POSITION_BUCKET: Final[str] = "pc-keiba-finish-position-models"
FINISH_POSITION_PREFIX: Final[str] = "finish-position"
RUNNING_STYLE_PREFIX: Final[str] = "running-style/models"
RUNNING_STYLE_LATEST_FILE_NAME: Final[str] = "latest.flatbin"
RUNNING_STYLE_CALIBRATOR_FILE_NAME: Final[str] = "calibrators.json"
NAR_TRANSFORMER_FILE_NAMES: Final[tuple[str, ...]] = (
    "norm.json",
    "weights_s1.npz",
    "weights_s2.npz",
    "weights_s3.npz",
)
# Categories the running-style cell-routing closure ever considers, mirroring
# apps/sync-realtime-data's RunningStyleCellCategory ("ban-ei" | "jra" | "nar")
# -- NOT just the two categories that happen to have a live artifact today.
RUNNING_STYLE_ROUTABLE_CATEGORIES: Final[tuple[str, ...]] = ("jra", "nar", "ban-ei")
# Categories with a source-level "latest" fallback key when no cell routing is
# configured for them. Ban-ei has no such fallback: RunningStyleSource is only
# "jra" | "nar" (a Ban-ei job's job.source is "nar"), so an unrouted Ban-ei
# race resolves through the "nar" iteration below, not its own key.
RUNNING_STYLE_FALLBACK_CATEGORIES: Final[frozenset[str]] = frozenset({"jra", "nar"})
# tryLoadCalibrators(bucket, job.source) in running-style-queue.ts only ever
# loads a jra or nar calibrator -- a Ban-ei race's job.source is "nar", so its
# cell-routed model (if any) is still calibrated with the NAR calibrator.
RUNNING_STYLE_CALIBRATION_SOURCE_BY_CATEGORY: Final[dict[str, str]] = {
    "jra": "jra",
    "nar": "nar",
    "ban-ei": "nar",
}
ALLOWED_SYSTEMS: Final[frozenset[str]] = frozenset({"finish-position", "running-style"})
ALLOWED_CATEGORIES: Final[frozenset[str]] = frozenset({"jra", "nar", "ban-ei"})
ALLOWED_ACTIVATIONS: Final[frozenset[str]] = frozenset({"required", "conditional"})
ALLOWED_ROLES: Final[frozenset[str]] = frozenset(
    {
        "calibrator",
        "metadata",
        "model",
        "normalization",
        "weights-s1",
        "weights-s2",
        "weights-s3",
    }
)
FINISH_POSITION_MODEL_ROLES: Final[frozenset[str]] = frozenset({"metadata", "model"})
NAR_TRANSFORMER_ROLES: Final[frozenset[str]] = frozenset(
    {"normalization", "weights-s1", "weights-s2", "weights-s3"}
)
RUNNING_STYLE_ROLES: Final[frozenset[str]] = frozenset({"calibrator", "model"})
RUNNING_STYLE_CALIBRATABLE_CATEGORIES: Final[frozenset[str]] = frozenset({"jra", "nar"})
MANIFEST_KEYS: Final[frozenset[str]] = frozenset({"schema_version", "artifacts", "not_applicable"})
ARTIFACT_KEYS: Final[frozenset[str]] = frozenset(
    {
        "activation",
        "activation_environment",
        "bundle_id",
        "category",
        "container_path",
        "id",
        "model_version",
        "release_id",
        "role",
        "serving_bucket",
        "serving_key",
        "sha256",
        "size_bytes",
        "source_ref",
        "system",
    }
)
NOT_APPLICABLE_KEYS: Final[frozenset[str]] = frozenset({"category", "reason", "system"})
BAN_EI_RUNNING_STYLE_NA_KEY: Final[tuple[str, str]] = ("running-style", "ban-ei")
LOADER_RELEVANT_SUFFIXES: Final[frozenset[str]] = frozenset({".flatbin", ".json", ".npz"})
CHUNK_SIZE_BYTES: Final[int] = 1024 * 1024


class ManifestValidationError(ValueError):
    """Raised when the tracked manifest violates its deterministic v1 contract."""


@dataclass(frozen=True)
class ArtifactSpec:
    id: str
    system: ArtifactSystem
    category: ArtifactCategory
    model_version: str
    release_id: str
    bundle_id: str
    role: str
    sha256: str
    size_bytes: int
    source_ref: str
    serving_bucket: str
    serving_key: str
    container_path: str | None
    activation: ActivationMode
    activation_environment: str | None


@dataclass(frozen=True)
class NotApplicableSpec:
    system: ArtifactSystem
    category: ArtifactCategory
    reason: str


@dataclass(frozen=True)
class ProductionArtifactManifest:
    schema_version: str
    artifacts: tuple[ArtifactSpec, ...]
    not_applicable: tuple[NotApplicableSpec, ...]


@dataclass(frozen=True)
class ArtifactObservation:
    """One artifact's observed local bytes.

    ``state="unavailable"`` means the observing machinery itself could not
    establish present-vs-absent (e.g. a local ``PermissionError``, or -- for
    any FUTURE network-based observer -- an auth/timeout/connectivity
    failure). It MUST map to ``INDETERMINATE``, never ``MATCH`` or
    ``INTEGRITY_FAILURE``: a transient network issue must never silently
    block, nor silently pass, an ordinary offline build. ``state="missing"``
    is reserved for a DEFINITIVE absence (a local filesystem
    ``FileNotFoundError``) established without ambiguity -- never used for a
    check that merely failed to run. This module today only ever observes
    local build-context bytes (see the module docstring's Scope note), so
    ``"unavailable"`` currently only arises from local read errors; the state
    model already supports a future network-based observer without any
    further contract change.
    """

    serving_key: str
    state: ObservationState
    sha256: str | None = None
    size_bytes: int | None = None
    detail: str | None = None


@dataclass(frozen=True)
class IntegrityFinding:
    status: Literal["INTEGRITY_FAILURE", "INDETERMINATE"]
    code: str
    serving_key: str
    message: str


@dataclass(frozen=True)
class IntegrityReport:
    status: IntegrityStatus
    manifest_root_sha256: str
    selected_count: int
    observed_count: int
    findings: tuple[IntegrityFinding, ...]
    warnings: tuple[str, ...]
    not_applicable: tuple[NotApplicableSpec, ...] = ()

    @property
    def exit_code(self) -> int:
        if self.status == "MATCH":
            return 0
        if self.status == "INTEGRITY_FAILURE":
            return 1
        return 2

    def to_json(self) -> str:
        payload: dict[str, object] = {
            "findings": [asdict(finding) for finding in self.findings],
            "manifest_root_sha256": self.manifest_root_sha256,
            "not_applicable": [asdict(item) for item in self.not_applicable],
            "observed_count": self.observed_count,
            "selected_count": self.selected_count,
            "status": self.status,
            "warnings": list(self.warnings),
        }
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _as_mapping(value: object, context: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ManifestValidationError(f"{context} must be an object")
    return {str(key): item for key, item in value.items()}


def _as_sequence(value: object, context: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise ManifestValidationError(f"{context} must be an array")
    return value


def _require_exact_keys(
    payload: Mapping[str, object], expected: frozenset[str], context: str
) -> None:
    actual = frozenset(payload)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        raise ManifestValidationError(
            f"{context} keys differ: missing={missing}, unexpected={unexpected}"
        )


def _require_string(payload: Mapping[str, object], key: str, context: str) -> str:
    value = payload[key]
    if not isinstance(value, str) or not value:
        raise ManifestValidationError(f"{context}.{key} must be a non-empty string")
    return value


def _optional_string(payload: Mapping[str, object], key: str, context: str) -> str | None:
    value = payload[key]
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ManifestValidationError(f"{context}.{key} must be null or a non-empty string")
    return value


def _require_choice(
    payload: Mapping[str, object],
    key: str,
    allowed: frozenset[str],
    context: str,
) -> str:
    value = _require_string(payload, key, context)
    if value not in allowed:
        raise ManifestValidationError(f"{context}.{key} has unsupported value: {value}")
    return value


def _require_relative_path(value: str, context: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or str(path) != value:
        raise ManifestValidationError(f"{context} must be a normalized relative path")
    return value


def _validate_serving_locator(
    *,
    category: str,
    container_path: str | None,
    context: str,
    model_version: str,
    serving_bucket: str,
    serving_key: str,
    system: str,
) -> None:
    if serving_bucket != FINISH_POSITION_BUCKET:
        raise ManifestValidationError(f"{context}.serving_bucket must be {FINISH_POSITION_BUCKET}")
    if system == "finish-position":
        expected_prefix = f"{FINISH_POSITION_PREFIX}/{category}/{model_version}/"
        if not serving_key.startswith(expected_prefix):
            raise ManifestValidationError(
                f"{context}.serving_key must match finish-position category/model_version"
            )
        if container_path != serving_key:
            raise ManifestValidationError(
                f"{context}.container_path must mirror the finish-position serving key"
            )
        return
    expected_prefix = f"{RUNNING_STYLE_PREFIX}/{category}/"
    if not serving_key.startswith(expected_prefix):
        raise ManifestValidationError(f"{context}.serving_key must match running-style category")


def _parse_artifact(value: object, index: int) -> ArtifactSpec:
    context = f"artifacts[{index}]"
    payload = _as_mapping(value, context)
    _require_exact_keys(payload, ARTIFACT_KEYS, context)
    system = _require_choice(payload, "system", ALLOWED_SYSTEMS, context)
    category = _require_choice(payload, "category", ALLOWED_CATEGORIES, context)
    role = _require_choice(payload, "role", ALLOWED_ROLES, context)
    digest = _require_string(payload, "sha256", context)
    if SHA256_PATTERN.fullmatch(digest) is None:
        raise ManifestValidationError(f"{context}.sha256 must be a lowercase SHA-256 digest")
    size_bytes = payload["size_bytes"]
    if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes <= 0:
        raise ManifestValidationError(f"{context}.size_bytes must be a positive integer")
    source_ref = _require_string(payload, "source_ref", context)
    source_parts = PurePosixPath(source_ref.lower().replace("://", "/")).parts
    if "://" not in source_ref or "tmp" in source_parts:
        raise ManifestValidationError(f"{context}.source_ref must be non-tmp provenance")
    serving_key = _require_relative_path(
        _require_string(payload, "serving_key", context), f"{context}.serving_key"
    )
    container_path_raw = _optional_string(payload, "container_path", context)
    container_path = (
        None
        if container_path_raw is None
        else _require_relative_path(container_path_raw, f"{context}.container_path")
    )
    if system == "finish-position" and container_path is None:
        raise ManifestValidationError(f"{context}.container_path is required for finish-position")
    if system == "running-style" and container_path is not None:
        raise ManifestValidationError(f"{context}.container_path must be null for running-style")
    activation = _require_choice(payload, "activation", ALLOWED_ACTIVATIONS, context)
    activation_environment = _optional_string(payload, "activation_environment", context)
    if activation == "conditional" and activation_environment is None:
        raise ManifestValidationError(
            f"{context}.activation_environment is required for conditional artifacts"
        )
    if activation == "required" and activation_environment is not None:
        raise ManifestValidationError(
            f"{context}.activation_environment must be null for required artifacts"
        )
    model_version = _require_string(payload, "model_version", context)
    serving_bucket = _require_string(payload, "serving_bucket", context)
    _validate_serving_locator(
        category=category,
        container_path=container_path,
        context=context,
        model_version=model_version,
        serving_bucket=serving_bucket,
        serving_key=serving_key,
        system=system,
    )
    return ArtifactSpec(
        activation=cast(ActivationMode, activation),
        activation_environment=activation_environment,
        bundle_id=_require_string(payload, "bundle_id", context),
        category=cast(ArtifactCategory, category),
        container_path=container_path,
        id=_require_string(payload, "id", context),
        model_version=model_version,
        release_id=_require_string(payload, "release_id", context),
        role=role,
        serving_bucket=serving_bucket,
        serving_key=serving_key,
        sha256=digest,
        size_bytes=size_bytes,
        source_ref=source_ref,
        system=cast(ArtifactSystem, system),
    )


def _parse_not_applicable(value: object, index: int) -> NotApplicableSpec:
    context = f"not_applicable[{index}]"
    payload = _as_mapping(value, context)
    _require_exact_keys(payload, NOT_APPLICABLE_KEYS, context)
    system = _require_choice(payload, "system", ALLOWED_SYSTEMS, context)
    category = _require_choice(payload, "category", ALLOWED_CATEGORIES, context)
    return NotApplicableSpec(
        category=cast(ArtifactCategory, category),
        reason=_require_string(payload, "reason", context),
        system=cast(ArtifactSystem, system),
    )


def _validate_unique_and_sorted(manifest: ProductionArtifactManifest) -> None:
    artifact_ids = [artifact.id for artifact in manifest.artifacts]
    if artifact_ids != sorted(artifact_ids):
        raise ManifestValidationError("artifacts must be sorted by id")
    if len(artifact_ids) != len(set(artifact_ids)):
        raise ManifestValidationError("artifact ids must be unique")
    serving_keys = [artifact.serving_key for artifact in manifest.artifacts]
    if len(serving_keys) != len(set(serving_keys)):
        raise ManifestValidationError("artifact serving keys must be unique")
    na_keys = [(item.system, item.category) for item in manifest.not_applicable]
    if na_keys != sorted(na_keys):
        raise ManifestValidationError("not_applicable entries must be sorted by system/category")
    if len(na_keys) != len(set(na_keys)):
        raise ManifestValidationError("not_applicable system/category pairs must be unique")


def _validate_bundles(manifest: ProductionArtifactManifest) -> None:
    bundles: dict[str, list[ArtifactSpec]] = defaultdict(list)
    for artifact in manifest.artifacts:
        bundles[artifact.bundle_id].append(artifact)
    for bundle_id, artifacts in bundles.items():
        identities = {
            (
                artifact.system,
                artifact.category,
                artifact.model_version,
                artifact.release_id,
                artifact.activation,
                artifact.activation_environment,
            )
            for artifact in artifacts
        }
        if len(identities) != 1:
            raise ManifestValidationError(
                f"bundle {bundle_id} mixes system/category/model/release/activation"
            )
        roles = frozenset(artifact.role for artifact in artifacts)
        if len(roles) != len(artifacts):
            raise ManifestValidationError(f"bundle {bundle_id} has duplicate-role artifacts")
        if artifacts[0].system == "running-style":
            if len(roles) != 1 or not roles.issubset(RUNNING_STYLE_ROLES):
                raise ManifestValidationError(
                    f"running-style bundle {bundle_id} must be single-role (model XOR "
                    "calibrator) -- shared calibrator coverage is validated per "
                    "category/source, not per bundle"
                )
            continue
        expected_finish_roles = (
            NAR_TRANSFORMER_ROLES
            if artifacts[0].model_version == NAR_TRANSFORMER_MODEL_VERSION
            else FINISH_POSITION_MODEL_ROLES
        )
        if roles != expected_finish_roles:
            raise ManifestValidationError(
                f"finish-position bundle {bundle_id} has an incomplete artifact role set"
            )


def _validate_running_style_calibration_coverage(manifest: ProductionArtifactManifest) -> None:
    """A running-style model bundle never carries its own calibrator (see
    ``_validate_bundles``): production loads exactly ONE calibrator per
    source (jra/nar) and shares it across however many routed models exist
    for that source (``tryLoadCalibrators(bucket, job.source)`` in
    ``running-style-queue.ts``). This validates that coverage at the
    category/source level instead: every category with a manifested model
    artifact must have a same-source calibrator artifact somewhere in the
    manifest, without requiring (or permitting) a duplicate calibrator entry
    per routed model.
    """
    running_style = [
        artifact for artifact in manifest.artifacts if artifact.system == "running-style"
    ]
    model_categories = {
        artifact.category for artifact in running_style if artifact.role == "model"
    }
    calibrator_categories = {
        artifact.category for artifact in running_style if artifact.role == "calibrator"
    }
    non_calibratable = sorted(calibrator_categories - RUNNING_STYLE_CALIBRATABLE_CATEGORIES)
    if non_calibratable:
        raise ManifestValidationError(
            "running-style calibrator artifacts must be category jra or nar, got: "
            f"{non_calibratable}"
        )
    for category in sorted(model_categories):
        source = RUNNING_STYLE_CALIBRATION_SOURCE_BY_CATEGORY[category]
        if source not in calibrator_categories:
            raise ManifestValidationError(
                f"running-style category={category} has a model artifact but no "
                f"{source} calibrator artifact to pair with it"
            )


def _validate_not_applicable_conflicts(manifest: ProductionArtifactManifest) -> None:
    artifact_keys = {(artifact.system, artifact.category) for artifact in manifest.artifacts}
    conflicts = sorted(
        (item.system, item.category)
        for item in manifest.not_applicable
        if (item.system, item.category) in artifact_keys
    )
    if conflicts:
        raise ManifestValidationError(
            f"not_applicable conflicts with manifested artifacts: {conflicts}"
        )


def parse_manifest(payload: object) -> ProductionArtifactManifest:
    root = _as_mapping(payload, "manifest")
    _require_exact_keys(root, MANIFEST_KEYS, "manifest")
    schema_version = _require_string(root, "schema_version", "manifest")
    if schema_version != SCHEMA_VERSION:
        raise ManifestValidationError(f"unsupported manifest schema: {schema_version}")
    artifacts = tuple(
        _parse_artifact(value, index)
        for index, value in enumerate(_as_sequence(root["artifacts"], "manifest.artifacts"))
    )
    if not artifacts:
        raise ManifestValidationError("manifest.artifacts must not be empty")
    not_applicable = tuple(
        _parse_not_applicable(value, index)
        for index, value in enumerate(
            _as_sequence(root["not_applicable"], "manifest.not_applicable")
        )
    )
    manifest = ProductionArtifactManifest(
        artifacts=artifacts,
        not_applicable=not_applicable,
        schema_version=schema_version,
    )
    _validate_unique_and_sorted(manifest)
    _validate_bundles(manifest)
    _validate_running_style_calibration_coverage(manifest)
    _validate_not_applicable_conflicts(manifest)
    return manifest


def load_manifest(path: Path = MANIFEST_PATH) -> ProductionArtifactManifest:
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ManifestValidationError(f"invalid manifest JSON: {error.msg}") from error
    return parse_manifest(payload)


def manifest_root_sha256(manifest: ProductionArtifactManifest) -> str:
    payload: dict[str, object] = {
        "artifacts": [asdict(artifact) for artifact in manifest.artifacts],
        "not_applicable": [asdict(item) for item in manifest.not_applicable],
        "schema_version": manifest.schema_version,
    }
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _finish_position_key(category: str, model_version: str, file_name: str) -> str:
    return f"{FINISH_POSITION_PREFIX}/{category}/{model_version}/{file_name}"


def _running_style_selected_keys(
    routing: Mapping[str, RunningStyleCategoryRouting],
) -> frozenset[str]:
    """The full running-style selector closure: for every routable category
    (jra / nar / ban-ei -- not just the two with a live artifact today), fold
    in either the tracked cell-routing config's reachable ``modelKey``
    values, or -- only for jra/nar, which have a source-level fallback -- the
    hardcoded ``latest.flatbin`` key ``resolveRunningStyleCellRoute`` falls
    back to when no config exists for that category. Each category that
    contributes any model key also contributes its (possibly shared) source
    calibrator key, deduplicated via the ``set`` union below regardless of
    how many categories map to the same calibration source.
    """
    selected: set[str] = set()
    calibration_sources: set[str] = set()
    for category in RUNNING_STYLE_ROUTABLE_CATEGORIES:
        category_routing = routing.get(category)
        if category_routing is None:
            if category not in RUNNING_STYLE_FALLBACK_CATEGORIES:
                continue
            selected.add(f"{RUNNING_STYLE_PREFIX}/{category}/{RUNNING_STYLE_LATEST_FILE_NAME}")
            calibration_sources.add(category)
            continue
        selected.update(category_routing.reachable_model_keys())
        calibration_sources.add(RUNNING_STYLE_CALIBRATION_SOURCE_BY_CATEGORY[category])
    selected.update(
        f"{RUNNING_STYLE_PREFIX}/{source}/{RUNNING_STYLE_CALIBRATOR_FILE_NAME}"
        for source in calibration_sources
    )
    return frozenset(selected)


def derive_selected_artifact_keys(
    *,
    model_meta_path: Path = MODEL_META_JSON_PATH,
    cell_routing_path: Path = CELL_ROUTING_PATH,
    nar_transformer_enabled: bool | None = None,
    deploy_flags_path: Path = DEPLOY_FLAGS_PATH,
    running_style_routing_path: Path = RUNNING_STYLE_ROUTING_PATH,
) -> frozenset[str]:
    """Derive every artifact key production selectors can currently reach.

    ``nar_transformer_enabled=None`` (the default) resolves the NAR
    transformer bundle's activation from the tracked ``deploy_flags.json``
    declaration rather than the local process environment, so a stray shell
    export of ``NAR_TRANSFORMER_BLEND_ENABLED`` (which has no relationship to
    the actual Cloudflare Worker secret) can no longer change what this
    preflight verifies. Pass an explicit ``True``/``False`` to override the
    tracked declaration (e.g. to test a rollback before flipping the tracked
    file, or from a caller that already knows the value some other trusted
    way).
    """
    versions, _counts = load_model_meta(model_meta_path)
    selected: set[str] = set()
    for category in CATEGORIES:
        version = versions[category]
        selected.add(_finish_position_key(category, version, MODEL_FILE_NAME))
        selected.add(_finish_position_key(category, version, METADATA_FILE_NAME))
    router = load_cell_router(cell_routing_path)
    for category in CATEGORIES:
        if not router.has_routing(category):
            continue
        for variant in router.routing_for(category).variants.values():
            selected.add(_finish_position_key(category, variant.model_version, MODEL_FILE_NAME))
            selected.add(_finish_position_key(category, variant.model_version, METADATA_FILE_NAME))
    transformer_enabled = (
        load_deploy_flags(deploy_flags_path).nar_transformer_blend_enabled
        if nar_transformer_enabled is None
        else nar_transformer_enabled
    )
    if transformer_enabled:
        selected.update(
            _finish_position_key("nar", NAR_TRANSFORMER_MODEL_VERSION, file_name)
            for file_name in NAR_TRANSFORMER_FILE_NAMES
        )
    routing = load_running_style_cell_routing(running_style_routing_path)
    selected.update(_running_style_selected_keys(routing))
    return frozenset(selected)


def _status_from_findings(findings: Iterable[IntegrityFinding]) -> IntegrityStatus:
    statuses = {finding.status for finding in findings}
    if "INTEGRITY_FAILURE" in statuses:
        return "INTEGRITY_FAILURE"
    if "INDETERMINATE" in statuses:
        return "INDETERMINATE"
    return "MATCH"


def _build_report(
    manifest: ProductionArtifactManifest,
    selected_keys: frozenset[str],
    observed_count: int,
    findings: Iterable[IntegrityFinding],
    warnings: Iterable[str],
) -> IntegrityReport:
    ordered_findings = tuple(
        sorted(findings, key=lambda item: (item.serving_key, item.code, item.message))
    )
    return IntegrityReport(
        findings=ordered_findings,
        manifest_root_sha256=manifest_root_sha256(manifest),
        not_applicable=manifest.not_applicable,
        observed_count=observed_count,
        selected_count=len(selected_keys),
        status=_status_from_findings(ordered_findings),
        warnings=tuple(sorted(set(warnings))),
    )


def verify_selector_closure(
    manifest: ProductionArtifactManifest, selected_keys: frozenset[str]
) -> IntegrityReport:
    manifested = {artifact.serving_key for artifact in manifest.artifacts}
    findings = [
        IntegrityFinding(
            code="selected-unmanifested",
            message="Selected artifact has no manifest entry",
            serving_key=serving_key,
            status="INTEGRITY_FAILURE",
        )
        for serving_key in sorted(selected_keys - manifested)
    ]
    warnings = [
        f"unselected manifest artifact: {serving_key}"
        for serving_key in sorted(manifested - selected_keys)
    ]
    na_keys = {(item.system, item.category) for item in manifest.not_applicable}
    if BAN_EI_RUNNING_STYLE_NA_KEY not in na_keys:
        findings.append(
            IntegrityFinding(
                code="missing-not-applicable",
                message="Ban-ei running-style must be explicitly marked N/A",
                serving_key="running-style:ban-ei",
                status="INTEGRITY_FAILURE",
            )
        )
    return _build_report(manifest, selected_keys, 0, findings, warnings)


def verify_observations(
    manifest: ProductionArtifactManifest,
    selected_keys: frozenset[str],
    observations: Iterable[ArtifactObservation],
) -> IntegrityReport:
    observation_list = tuple(observations)
    observed_by_key: dict[str, ArtifactObservation] = {}
    findings: list[IntegrityFinding] = []
    warnings: list[str] = []
    for observation in observation_list:
        if observation.serving_key in observed_by_key:
            findings.append(
                IntegrityFinding(
                    code="duplicate-observation",
                    message="Artifact was observed more than once",
                    serving_key=observation.serving_key,
                    status="INTEGRITY_FAILURE",
                )
            )
            continue
        observed_by_key[observation.serving_key] = observation
    manifest_by_key = {artifact.serving_key: artifact for artifact in manifest.artifacts}
    for serving_key in sorted(selected_keys):
        artifact = manifest_by_key.get(serving_key)
        if artifact is None:
            findings.append(
                IntegrityFinding(
                    code="selected-unmanifested",
                    message="Selected artifact has no manifest entry",
                    serving_key=serving_key,
                    status="INTEGRITY_FAILURE",
                )
            )
            continue
        observation = observed_by_key.get(serving_key)
        if observation is None or observation.state == "missing":
            findings.append(
                IntegrityFinding(
                    code="selected-missing",
                    message="Selected artifact is definitively missing",
                    serving_key=serving_key,
                    status="INTEGRITY_FAILURE",
                )
            )
            continue
        if observation.state == "unavailable":
            findings.append(
                IntegrityFinding(
                    code="observation-unavailable",
                    message=observation.detail or "Artifact observation was unavailable",
                    serving_key=serving_key,
                    status="INDETERMINATE",
                )
            )
            continue
        if observation.sha256 != artifact.sha256:
            findings.append(
                IntegrityFinding(
                    code="sha256-mismatch",
                    message="Selected artifact SHA-256 differs from manifest",
                    serving_key=serving_key,
                    status="INTEGRITY_FAILURE",
                )
            )
        if observation.size_bytes != artifact.size_bytes:
            findings.append(
                IntegrityFinding(
                    code="size-mismatch",
                    message="Selected artifact size differs from manifest",
                    serving_key=serving_key,
                    status="INTEGRITY_FAILURE",
                )
            )
    for serving_key in sorted(set(observed_by_key) - selected_keys):
        warnings.append(f"unselected observed artifact: {serving_key}")
    return _build_report(
        manifest,
        selected_keys,
        len(observation_list),
        findings,
        warnings,
    )


def combine_reports(*reports: IntegrityReport) -> IntegrityReport:
    if not reports:
        raise ValueError("at least one report is required")
    roots = {report.manifest_root_sha256 for report in reports}
    if len(roots) != 1:
        raise ValueError("reports use different manifests")
    findings = tuple(finding for report in reports for finding in report.findings)
    warnings = tuple(warning for report in reports for warning in report.warnings)
    ordered_findings = tuple(
        sorted(findings, key=lambda item: (item.serving_key, item.code, item.message))
    )
    return IntegrityReport(
        findings=ordered_findings,
        manifest_root_sha256=reports[0].manifest_root_sha256,
        not_applicable=reports[0].not_applicable,
        observed_count=max(report.observed_count for report in reports),
        selected_count=max(report.selected_count for report in reports),
        status=_status_from_findings(ordered_findings),
        warnings=tuple(sorted(set(warnings))),
    )


def _observe_file(root: Path, serving_key: str) -> ArtifactObservation:
    path = root / serving_key
    digest = hashlib.sha256()
    size_bytes = 0
    try:
        with path.open("rb") as file:
            while chunk := file.read(CHUNK_SIZE_BYTES):
                digest.update(chunk)
                size_bytes += len(chunk)
    except FileNotFoundError:
        return ArtifactObservation(serving_key=serving_key, state="missing")
    except OSError as error:
        return ArtifactObservation(
            detail=f"{type(error).__name__}: {error}",
            serving_key=serving_key,
            state="unavailable",
        )
    return ArtifactObservation(
        serving_key=serving_key,
        sha256=digest.hexdigest(),
        size_bytes=size_bytes,
        state="present",
    )


def _is_loader_relevant(path: Path) -> bool:
    return path.suffix in LOADER_RELEVANT_SUFFIXES and path.name != MANIFEST_FILE_NAME


def observe_artifact_tree(
    root: Path, selected_keys: frozenset[str]
) -> tuple[ArtifactObservation, ...]:
    observations = [_observe_file(root, serving_key) for serving_key in sorted(selected_keys)]
    if not root.exists():
        return tuple(observations)
    selected = set(selected_keys)
    for path in root.rglob("*"):
        if not path.is_file() or not _is_loader_relevant(path):
            continue
        serving_key = path.relative_to(root).as_posix()
        if serving_key in selected:
            continue
        observations.append(ArtifactObservation(serving_key=serving_key, state="present"))
    return tuple(observations)


def _filter_selected_keys(
    manifest: ProductionArtifactManifest,
    selected_keys: frozenset[str],
    system: ArtifactSystem | None,
) -> frozenset[str]:
    if system is None:
        return selected_keys
    keys_for_system = {
        artifact.serving_key for artifact in manifest.artifacts if artifact.system == system
    }
    return frozenset(selected_keys.intersection(keys_for_system))


def _parse_enabled(value: str) -> bool:
    return value == "enabled"


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify production artifact integrity")
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument(
        "--system",
        choices=("finish-position", "running-style"),
    )
    parser.add_argument(
        "--nar-transformer",
        choices=("enabled", "disabled"),
        default=None,
        help=(
            "Override the tracked deploy_flags.json nar_transformer_blend_enabled "
            "value. Omit to use the tracked declaration (the deploy-gate default)."
        ),
    )
    return parser


def _error_report(status: Literal["INTEGRITY_FAILURE", "INDETERMINATE"], message: str) -> str:
    payload: dict[str, object] = {
        "findings": [
            {
                "code": "verification-error",
                "message": message,
                "serving_key": "manifest",
                "status": status,
            }
        ],
        "manifest_root_sha256": "",
        "not_applicable": [],
        "observed_count": 0,
        "selected_count": 0,
        "status": status,
        "warnings": [],
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    args = _argument_parser().parse_args(argv)
    try:
        manifest = load_manifest(args.manifest)
        nar_transformer_override = (
            None if args.nar_transformer is None else _parse_enabled(args.nar_transformer)
        )
        selected = derive_selected_artifact_keys(nar_transformer_enabled=nar_transformer_override)
        static_report = verify_selector_closure(manifest, selected)
        report = static_report
        if args.artifact_root is not None:
            system = args.system
            selected_for_observation = _filter_selected_keys(manifest, selected, system)
            observations = observe_artifact_tree(args.artifact_root, selected_for_observation)
            report = combine_reports(
                static_report,
                verify_observations(manifest, selected_for_observation, observations),
            )
    except (ManifestValidationError, ValueError, json.JSONDecodeError) as error:
        print(_error_report("INTEGRITY_FAILURE", str(error)))
        return 1
    except OSError as error:
        print(_error_report("INDETERMINATE", f"{type(error).__name__}: {error}"))
        return 2
    print(report.to_json())
    return report.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
