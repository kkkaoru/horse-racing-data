"""Tests for the deterministic production artifact integrity contract."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from predict_lib import artifact_integrity
from predict_lib.artifact_integrity import (
    MANIFEST_PATH,
    ArtifactObservation,
    ArtifactSpec,
    IntegrityFinding,
    IntegrityReport,
    ManifestValidationError,
    ProductionArtifactManifest,
    combine_reports,
    derive_selected_artifact_keys,
    load_manifest,
    main,
    manifest_root_sha256,
    observe_artifact_tree,
    parse_manifest,
    stage_artifact_tree,
    verify_observations,
    verify_selector_closure,
)
from predict_lib.deploy_flags import DeployFlagsValidationError
from predict_lib.running_style_routing import RunningStyleRoutingValidationError
from predict_lib.stage1_routing import Stage1RoutingValidationError, load_stage1_routing

EXPECTED_MANIFEST_ROOT = "bf286a079a9afa0f75bc2d4e10cfdf0a11accdd60a6e1696055c4b3710073879"
JRA_RS_MODEL_KEY = "running-style/models/jra/latest.flatbin"
JRA_RS_CALIBRATOR_KEY = "running-style/models/jra/calibrators.json"
REPO_ROOT = Path(__file__).resolve().parents[3]


def _payload() -> dict[str, object]:
    value: object = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return {str(key): item for key, item in value.items()}


def _artifact_payload(payload: dict[str, object], index: int = 0) -> dict[str, object]:
    artifacts = payload["artifacts"]
    assert isinstance(artifacts, list)
    artifact = artifacts[index]
    assert isinstance(artifact, dict)
    return cast(dict[str, object], artifact)


def _jra_model(manifest: ProductionArtifactManifest) -> ArtifactSpec:
    return next(
        artifact for artifact in manifest.artifacts if artifact.serving_key == JRA_RS_MODEL_KEY
    )


def _write_running_style_routing(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "running_style_cell_routing.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_deploy_flags(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "deploy_flags.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_manifest_is_deterministic_and_selector_complete() -> None:
    manifest = load_manifest()
    selected = derive_selected_artifact_keys(nar_transformer_enabled=True)
    report = verify_selector_closure(manifest, selected)

    assert manifest.schema_version == "production-artifacts/v1"
    assert len(manifest.artifacts) == 24
    assert len(selected) == 24
    assert manifest_root_sha256(manifest) == EXPECTED_MANIFEST_ROOT
    assert report.status == "MATCH"
    assert report.exit_code == 0
    assert report.findings == ()
    assert report.warnings == ()


def test_manifest_records_exact_jra_running_style_pair() -> None:
    manifest = load_manifest()
    artifacts = {
        artifact.serving_key: artifact
        for artifact in manifest.artifacts
        if artifact.category == "jra" and artifact.system == "running-style"
    }

    assert artifacts[JRA_RS_MODEL_KEY].sha256 == (
        "278690ed18e7aa1f6847ee4349b1ec98281a5633cc10bd000de55c608ffbfc76"
    )
    assert artifacts[JRA_RS_MODEL_KEY].size_bytes == 50943889
    assert artifacts[JRA_RS_CALIBRATOR_KEY].sha256 == (
        "0f23a5a40dea956b1de06699432e130beb86dc3448bd3188485e60d1a7067ee7"
    )
    assert artifacts[JRA_RS_CALIBRATOR_KEY].size_bytes == 23212
    assert artifacts[JRA_RS_MODEL_KEY].release_id == (
        "jra-running-style-lgbm-prod-v3-fixed-export-rollback-refit-20260711"
    )
    # Model and calibrator are DIFFERENT bundles (a shared calibrator can serve
    # multiple routed models -- see test_shared_calibrator_* below), not a
    # forced 1:1 bundle pairing.
    assert artifacts[JRA_RS_MODEL_KEY].bundle_id != artifacts[JRA_RS_CALIBRATOR_KEY].bundle_id


def test_manifest_marks_ban_ei_running_style_not_applicable() -> None:
    manifest = load_manifest()

    assert manifest.not_applicable == (
        artifact_integrity.NotApplicableSpec(
            category="ban-ei",
            reason="Production running-style selectors and R2 keys support JRA and NAR only.",
            system="running-style",
        ),
    )


def test_disabled_transformer_is_unselected_warning_only() -> None:
    manifest = load_manifest()
    selected = derive_selected_artifact_keys(nar_transformer_enabled=False)
    report = verify_selector_closure(manifest, selected)

    assert len(selected) == 20
    assert report.status == "MATCH"
    assert len(report.warnings) == 4
    assert report.warnings[0] == (
        "unselected manifest artifact: "
        "finish-position/nar/iter40-nar-settransformer-blend-v1/norm.json"
    )


def test_selected_unmanifested_is_integrity_failure() -> None:
    manifest = load_manifest()
    selected = derive_selected_artifact_keys(nar_transformer_enabled=True)
    reduced = replace(
        manifest,
        artifacts=tuple(
            artifact for artifact in manifest.artifacts if artifact.serving_key != JRA_RS_MODEL_KEY
        ),
    )
    report = verify_selector_closure(reduced, selected)

    assert report.status == "INTEGRITY_FAILURE"
    assert report.exit_code == 1
    assert report.findings[0].code == "selected-unmanifested"
    assert report.findings[0].serving_key == JRA_RS_MODEL_KEY


def test_missing_ban_ei_na_is_integrity_failure() -> None:
    manifest = replace(load_manifest(), not_applicable=())
    selected = derive_selected_artifact_keys(nar_transformer_enabled=True)
    report = verify_selector_closure(manifest, selected)

    assert report.status == "INTEGRITY_FAILURE"
    assert report.findings[0].code == "missing-not-applicable"
    assert report.findings[0].serving_key == "running-style:ban-ei"


def test_matching_observation_is_match() -> None:
    manifest = load_manifest()
    artifact = _jra_model(manifest)
    report = verify_observations(
        manifest,
        frozenset({artifact.serving_key}),
        (
            ArtifactObservation(
                serving_key=artifact.serving_key,
                sha256=artifact.sha256,
                size_bytes=artifact.size_bytes,
                state="present",
            ),
        ),
    )

    assert report.status == "MATCH"
    assert report.observed_count == 1
    assert report.findings == ()
    assert report.warnings == ()


def test_sha_mismatch_is_integrity_failure() -> None:
    manifest = load_manifest()
    artifact = _jra_model(manifest)
    report = verify_observations(
        manifest,
        frozenset({artifact.serving_key}),
        (
            ArtifactObservation(
                serving_key=artifact.serving_key,
                sha256="0" * 64,
                size_bytes=artifact.size_bytes,
                state="present",
            ),
        ),
    )

    assert report.status == "INTEGRITY_FAILURE"
    assert report.findings[0].code == "sha256-mismatch"


def test_size_mismatch_is_integrity_failure() -> None:
    manifest = load_manifest()
    artifact = _jra_model(manifest)
    report = verify_observations(
        manifest,
        frozenset({artifact.serving_key}),
        (
            ArtifactObservation(
                serving_key=artifact.serving_key,
                sha256=artifact.sha256,
                size_bytes=1,
                state="present",
            ),
        ),
    )

    assert report.status == "INTEGRITY_FAILURE"
    assert report.findings[0].code == "size-mismatch"


def test_missing_observation_is_integrity_failure() -> None:
    manifest = load_manifest()
    report = verify_observations(
        manifest,
        frozenset({JRA_RS_MODEL_KEY}),
        (ArtifactObservation(serving_key=JRA_RS_MODEL_KEY, state="missing"),),
    )

    assert report.status == "INTEGRITY_FAILURE"
    assert report.findings[0].code == "selected-missing"


def test_omitted_observation_is_integrity_failure() -> None:
    manifest = load_manifest()
    report = verify_observations(manifest, frozenset({JRA_RS_MODEL_KEY}), ())

    assert report.status == "INTEGRITY_FAILURE"
    assert report.findings[0].code == "selected-missing"


def test_unavailable_observation_is_indeterminate() -> None:
    manifest = load_manifest()
    report = verify_observations(
        manifest,
        frozenset({JRA_RS_MODEL_KEY}),
        (
            ArtifactObservation(
                detail="remote request timed out",
                serving_key=JRA_RS_MODEL_KEY,
                state="unavailable",
            ),
        ),
    )

    assert report.status == "INDETERMINATE"
    assert report.exit_code == 2
    assert report.findings[0].code == "observation-unavailable"
    assert report.findings[0].message == "remote request timed out"


def test_unavailable_observation_uses_default_detail() -> None:
    manifest = load_manifest()
    report = verify_observations(
        manifest,
        frozenset({JRA_RS_MODEL_KEY}),
        (ArtifactObservation(serving_key=JRA_RS_MODEL_KEY, state="unavailable"),),
    )

    assert report.status == "INDETERMINATE"
    assert report.findings[0].message == "Artifact observation was unavailable"


def test_unselected_observation_is_warning_only() -> None:
    manifest = load_manifest()
    report = verify_observations(
        manifest,
        frozenset(),
        (
            ArtifactObservation(
                serving_key="running-style/models/jra/historical.flatbin",
                state="missing",
            ),
        ),
    )

    assert report.status == "MATCH"
    assert report.findings == ()
    assert report.warnings == (
        "unselected observed artifact: running-style/models/jra/historical.flatbin",
    )


def test_duplicate_observation_is_integrity_failure() -> None:
    manifest = load_manifest()
    artifact = _jra_model(manifest)
    observation = ArtifactObservation(
        serving_key=artifact.serving_key,
        sha256=artifact.sha256,
        size_bytes=artifact.size_bytes,
        state="present",
    )
    report = verify_observations(
        manifest,
        frozenset({artifact.serving_key}),
        (observation, observation),
    )

    assert report.status == "INTEGRITY_FAILURE"
    assert report.findings[0].code == "duplicate-observation"


def test_definitive_failure_takes_precedence_over_indeterminate() -> None:
    manifest = load_manifest()
    report = verify_observations(
        manifest,
        frozenset({JRA_RS_MODEL_KEY, JRA_RS_CALIBRATOR_KEY}),
        (
            ArtifactObservation(serving_key=JRA_RS_MODEL_KEY, state="missing"),
            ArtifactObservation(serving_key=JRA_RS_CALIBRATOR_KEY, state="unavailable"),
        ),
    )

    assert report.status == "INTEGRITY_FAILURE"
    assert [finding.status for finding in report.findings] == [
        "INDETERMINATE",
        "INTEGRITY_FAILURE",
    ]


def test_observation_of_unmanifested_selected_key_fails() -> None:
    manifest = load_manifest()
    key = "running-style/models/jra/selected-new.flatbin"
    report = verify_observations(
        manifest,
        frozenset({key}),
        (ArtifactObservation(serving_key=key, state="present"),),
    )

    assert report.status == "INTEGRITY_FAILURE"
    assert report.findings[0].code == "selected-unmanifested"


def test_observe_artifact_tree_hashes_selected_and_lists_orphan(tmp_path: Path) -> None:
    selected_key = "finish-position/jra/example/model.json"
    selected_path = tmp_path / selected_key
    selected_path.parent.mkdir(parents=True)
    selected_path.write_bytes(b"selected")
    orphan_path = tmp_path / "finish-position/jra/historical/model.json"
    orphan_path.parent.mkdir(parents=True)
    orphan_path.write_bytes(b"historical")

    observations = observe_artifact_tree(tmp_path, frozenset({selected_key}))

    assert observations == (
        ArtifactObservation(
            serving_key=selected_key,
            sha256="d7cbbb688b2e506c022e95cef8c4f629a29b9b36a6e50324e70dff466dbb95af",
            size_bytes=8,
            state="present",
        ),
        ArtifactObservation(
            serving_key="finish-position/jra/historical/model.json",
            state="present",
        ),
    )


def test_observe_artifact_tree_reports_missing_root() -> None:
    observations = observe_artifact_tree(
        Path("/path/that/does/not/exist"),
        frozenset({"finish-position/jra/example/model.json"}),
    )

    assert observations == (
        ArtifactObservation(
            serving_key="finish-position/jra/example/model.json",
            state="missing",
        ),
    )


def test_observe_artifact_tree_reports_unreadable_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail_open(path: Path, *args: object, **kwargs: object) -> object:
        _ = path, args, kwargs
        raise PermissionError("read denied")

    monkeypatch.setattr(Path, "open", fail_open)
    observations = observe_artifact_tree(
        tmp_path,
        frozenset({"finish-position/jra/example/model.json"}),
    )

    assert observations == (
        ArtifactObservation(
            detail="PermissionError: read denied",
            serving_key="finish-position/jra/example/model.json",
            state="unavailable",
        ),
    )


def test_stage_artifact_tree_copies_only_selected_files(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    selected_model = source_root / "finish-position/jra/selected/model.json"
    selected_metadata = source_root / "finish-position/jra/selected/metadata.json"
    unselected_model = source_root / "finish-position/jra/old/model.json"
    selected_model.parent.mkdir(parents=True)
    unselected_model.parent.mkdir(parents=True)
    selected_model.write_text("selected-model", encoding="utf-8")
    selected_metadata.write_text("selected-metadata", encoding="utf-8")
    unselected_model.write_text("old-model", encoding="utf-8")
    target_root = tmp_path / "staged"

    staged = stage_artifact_tree(
        source_root,
        target_root,
        frozenset(
            {
                "finish-position/jra/selected/model.json",
                "finish-position/jra/selected/metadata.json",
            }
        ),
    )

    assert staged == (
        "finish-position/jra/selected/metadata.json",
        "finish-position/jra/selected/model.json",
    )
    assert (target_root / "finish-position/jra/selected/model.json").read_text(
        encoding="utf-8"
    ) == "selected-model"
    assert (target_root / "finish-position/jra/selected/metadata.json").read_text(
        encoding="utf-8"
    ) == "selected-metadata"
    assert not (target_root / "finish-position/jra/old/model.json").exists()


def test_stage_artifact_tree_rejects_existing_target(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "staged"
    source_root.mkdir()
    target_root.mkdir()

    with pytest.raises(FileExistsError):
        stage_artifact_tree(source_root, target_root, frozenset())


def test_combine_reports_merges_and_deduplicates_warnings() -> None:
    report_match = IntegrityReport(
        findings=(),
        manifest_root_sha256=EXPECTED_MANIFEST_ROOT,
        observed_count=0,
        selected_count=20,
        status="MATCH",
        warnings=("orphan",),
    )
    report_indeterminate = IntegrityReport(
        findings=(
            IntegrityFinding(
                code="unavailable",
                message="timeout",
                serving_key=JRA_RS_MODEL_KEY,
                status="INDETERMINATE",
            ),
        ),
        manifest_root_sha256=EXPECTED_MANIFEST_ROOT,
        observed_count=20,
        selected_count=20,
        status="INDETERMINATE",
        warnings=("orphan",),
    )

    combined = combine_reports(report_match, report_indeterminate)

    assert combined.status == "INDETERMINATE"
    assert combined.observed_count == 20
    assert combined.warnings == ("orphan",)


def test_combine_reports_requires_input() -> None:
    with pytest.raises(ValueError, match="at least one report"):
        combine_reports()


def test_combine_reports_rejects_different_manifests() -> None:
    report_one = IntegrityReport(
        findings=(),
        manifest_root_sha256="1" * 64,
        observed_count=0,
        selected_count=0,
        status="MATCH",
        warnings=(),
    )
    report_two = replace(report_one, manifest_root_sha256="2" * 64)

    with pytest.raises(ValueError, match="different manifests"):
        combine_reports(report_one, report_two)


def test_parse_rejects_non_object() -> None:
    with pytest.raises(ManifestValidationError, match="manifest must be an object"):
        parse_manifest([])


def test_parse_rejects_wrong_root_keys() -> None:
    payload = _payload()
    payload["unexpected"] = True

    with pytest.raises(ManifestValidationError, match="manifest keys differ"):
        parse_manifest(payload)


def test_parse_rejects_unsupported_schema() -> None:
    payload = _payload()
    payload["schema_version"] = "production-artifacts/v2"

    with pytest.raises(ManifestValidationError, match="unsupported manifest schema"):
        parse_manifest(payload)


def test_parse_rejects_non_array_artifacts() -> None:
    payload = _payload()
    payload["artifacts"] = {}

    with pytest.raises(ManifestValidationError, match=r"manifest\.artifacts must be an array"):
        parse_manifest(payload)


def test_parse_rejects_empty_artifacts() -> None:
    payload = _payload()
    payload["artifacts"] = []

    with pytest.raises(ManifestValidationError, match="must not be empty"):
        parse_manifest(payload)


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("sha256", "ABC", "lowercase SHA-256"),
        ("size_bytes", True, "positive integer"),
        ("source_ref", "/tmp/model.json", "non-tmp provenance"),
        ("serving_key", "../model.json", "normalized relative path"),
        ("role", "unknown", "unsupported value"),
        ("activation", "standby", "unsupported value"),
    ],
)
def test_parse_rejects_invalid_artifact_fields(field: str, value: object, match: str) -> None:
    payload = _payload()
    _artifact_payload(payload)[field] = value

    with pytest.raises(ManifestValidationError, match=match):
        parse_manifest(payload)


def test_parse_rejects_empty_required_string() -> None:
    payload = _payload()
    _artifact_payload(payload)["id"] = ""

    with pytest.raises(ManifestValidationError, match="id must be a non-empty string"):
        parse_manifest(payload)


def test_parse_rejects_invalid_optional_string() -> None:
    payload = _payload()
    _artifact_payload(payload)["activation_environment"] = 1

    with pytest.raises(ManifestValidationError, match="must be null or a non-empty string"):
        parse_manifest(payload)


def test_parse_rejects_wrong_serving_bucket() -> None:
    payload = _payload()
    _artifact_payload(payload)["serving_bucket"] = "different-bucket"

    with pytest.raises(ManifestValidationError, match="serving_bucket must be"):
        parse_manifest(payload)


def test_parse_rejects_finish_position_key_for_different_model() -> None:
    payload = _payload()
    artifact = _artifact_payload(payload)
    key = "finish-position/ban-ei/different/metadata.json"
    artifact["serving_key"] = key
    artifact["container_path"] = key

    with pytest.raises(ManifestValidationError, match="must match finish-position"):
        parse_manifest(payload)


def test_parse_rejects_finish_position_container_key_mismatch() -> None:
    payload = _payload()
    _artifact_payload(payload)["container_path"] = (
        "finish-position/ban-ei/banei-cb-v8-window2011-wf-15y/other.json"
    )

    with pytest.raises(ManifestValidationError, match="must mirror"):
        parse_manifest(payload)


def test_parse_rejects_running_style_key_for_different_category() -> None:
    payload = _payload()
    artifacts = payload["artifacts"]
    assert isinstance(artifacts, list)
    index = next(
        index
        for index, value in enumerate(artifacts)
        if isinstance(value, dict) and value.get("serving_key") == JRA_RS_MODEL_KEY
    )
    _artifact_payload(payload, index)["serving_key"] = (
        "running-style/models/nar/jra-selected.flatbin"
    )

    with pytest.raises(ManifestValidationError, match="must match running-style category"):
        parse_manifest(payload)


def test_parse_rejects_missing_finish_position_container_path() -> None:
    payload = _payload()
    _artifact_payload(payload)["container_path"] = None

    with pytest.raises(ManifestValidationError, match="container_path is required"):
        parse_manifest(payload)


def test_parse_rejects_running_style_container_path() -> None:
    payload = _payload()
    artifacts = payload["artifacts"]
    assert isinstance(artifacts, list)
    index = next(
        index
        for index, value in enumerate(artifacts)
        if isinstance(value, dict) and value.get("system") == "running-style"
    )
    _artifact_payload(payload, index)["container_path"] = "running-style/model.flatbin"

    with pytest.raises(ManifestValidationError, match="container_path must be null"):
        parse_manifest(payload)


def test_parse_rejects_conditional_without_environment() -> None:
    payload = _payload()
    _artifact_payload(payload)["activation"] = "conditional"

    with pytest.raises(ManifestValidationError, match="activation_environment is required"):
        parse_manifest(payload)


def test_parse_rejects_required_with_environment() -> None:
    payload = _payload()
    _artifact_payload(payload)["activation_environment"] = "FLAG"

    with pytest.raises(ManifestValidationError, match="activation_environment must be null"):
        parse_manifest(payload)


def test_parse_rejects_unsorted_artifacts() -> None:
    payload = _payload()
    artifacts = payload["artifacts"]
    assert isinstance(artifacts, list)
    artifacts[0], artifacts[1] = artifacts[1], artifacts[0]

    with pytest.raises(ManifestValidationError, match="sorted by id"):
        parse_manifest(payload)


def test_parse_rejects_duplicate_artifact_id() -> None:
    payload = _payload()
    first = _artifact_payload(payload, 0)
    second = _artifact_payload(payload, 1)
    second["id"] = first["id"]

    with pytest.raises(ManifestValidationError, match="artifact ids must be unique"):
        parse_manifest(payload)


def test_parse_rejects_duplicate_serving_key() -> None:
    payload = _payload()
    first = _artifact_payload(payload, 0)
    second = _artifact_payload(payload, 1)
    second["serving_key"] = first["serving_key"]
    second["container_path"] = first["container_path"]

    with pytest.raises(ManifestValidationError, match="serving keys must be unique"):
        parse_manifest(payload)


def test_parse_rejects_mixed_bundle_identity() -> None:
    payload = _payload()
    second = _artifact_payload(payload, 1)
    second["release_id"] = "different-release"

    with pytest.raises(ManifestValidationError, match="mixes system/category/model/release"):
        parse_manifest(payload)


def test_parse_rejects_incomplete_finish_position_bundle() -> None:
    payload = _payload()
    artifacts = payload["artifacts"]
    assert isinstance(artifacts, list)
    payload["artifacts"] = artifacts[1:]

    with pytest.raises(ManifestValidationError, match="incomplete artifact role set"):
        parse_manifest(payload)


def test_parse_rejects_duplicate_role_artifacts_in_bundle() -> None:
    payload = _payload()
    artifacts = payload["artifacts"]
    assert isinstance(artifacts, list)
    first = _artifact_payload(payload, 0)
    duplicate = dict(first)
    duplicate["id"] = f"{first['id']}:duplicate"
    duplicate["serving_key"] = f"{first['serving_key']}.duplicate"
    duplicate["container_path"] = f"{first['container_path']}.duplicate"
    artifacts.append(duplicate)
    artifacts.sort(key=lambda item: cast(str, cast(dict[str, object], item)["id"]))

    with pytest.raises(ManifestValidationError, match="duplicate-role artifacts"):
        parse_manifest(payload)


def test_parse_rejects_unsorted_not_applicable_entries() -> None:
    payload = _payload()
    not_applicable = payload["not_applicable"]
    assert isinstance(not_applicable, list)
    not_applicable.append(
        {
            "category": "jra",
            "reason": "test only",
            "system": "finish-position",
        }
    )

    with pytest.raises(ManifestValidationError, match="entries must be sorted"):
        parse_manifest(payload)


def test_parse_rejects_duplicate_not_applicable_entries() -> None:
    payload = _payload()
    not_applicable = payload["not_applicable"]
    assert isinstance(not_applicable, list)
    not_applicable.append(dict(cast(dict[str, object], not_applicable[0])))

    with pytest.raises(ManifestValidationError, match="pairs must be unique"):
        parse_manifest(payload)


def test_parse_rejects_not_applicable_conflicting_with_manifested_artifact() -> None:
    payload = _payload()
    payload["not_applicable"] = [
        {"category": "jra", "reason": "test conflict", "system": "finish-position"},
        {
            "category": "ban-ei",
            "reason": "Production running-style selectors and R2 keys support JRA and NAR only.",
            "system": "running-style",
        },
    ]

    with pytest.raises(ManifestValidationError, match="conflicts with manifested artifacts"):
        parse_manifest(payload)


def test_parse_rejects_running_style_model_without_calibrator() -> None:
    payload = _payload()
    artifacts = payload["artifacts"]
    assert isinstance(artifacts, list)
    filtered = [
        value
        for value in artifacts
        if not (isinstance(value, dict) and value.get("serving_key") == JRA_RS_CALIBRATOR_KEY)
    ]
    payload["artifacts"] = filtered

    with pytest.raises(ManifestValidationError, match="no jra calibrator artifact"):
        parse_manifest(payload)


def test_parse_rejects_ban_ei_running_style_calibrator() -> None:
    """A running-style calibrator artifact must be jra or nar -- there is no
    Ban-ei calibrator concept at all (tryLoadCalibrators only ever loads a jra
    or nar calibrator, even for a Ban-ei race whose job.source is "nar")."""
    payload = _payload()
    artifacts = payload["artifacts"]
    assert isinstance(artifacts, list)
    calibrator_index = next(
        index
        for index, value in enumerate(artifacts)
        if isinstance(value, dict) and value.get("serving_key") == JRA_RS_CALIBRATOR_KEY
    )
    calibrator = _artifact_payload(payload, calibrator_index)
    calibrator["category"] = "ban-ei"
    calibrator["id"] = "running-style:ban-ei:calibrator-invalid"
    calibrator["serving_key"] = "running-style/models/ban-ei/calibrators.json"
    calibrator["bundle_id"] = "running-style:ban-ei:calibrator-invalid"

    with pytest.raises(ManifestValidationError, match="must be category jra or nar"):
        parse_manifest(payload)


def test_parse_rejects_running_style_bundle_pairing_model_and_calibrator_together() -> None:
    """The OLD schema forced exactly one model + one calibrator into the SAME
    bundle_id. That pairing is now actively rejected: a running-style bundle
    must be single-role, because a shared calibrator can serve more than one
    routed model and calibrator coverage is validated at the category/source
    level (see test_parse_accepts_shared_calibrator_across_multiple_routed_models
    below), not by forcing a 1:1 bundle pair."""
    payload: dict[str, object] = {
        "not_applicable": [],
        "schema_version": "production-artifacts/v1",
        "artifacts": [
            {
                "activation": "required",
                "activation_environment": None,
                "bundle_id": "running-style:jra:paired-bundle",
                "category": "jra",
                "container_path": None,
                "id": "running-style:jra:paired-bundle:calibrator",
                "model_version": "jra-running-style-lgbm-prod-v3",
                "release_id": "jra-running-style-lgbm-prod-v3-release",
                "role": "calibrator",
                "serving_bucket": "pc-keiba-finish-position-models",
                "serving_key": "running-style/models/jra/calibrators.json",
                "sha256": "0" * 64,
                "size_bytes": 1,
                "source_ref": "attestation://test/jra/calibrators.json",
                "system": "running-style",
            },
            {
                "activation": "required",
                "activation_environment": None,
                "bundle_id": "running-style:jra:paired-bundle",
                "category": "jra",
                "container_path": None,
                "id": "running-style:jra:paired-bundle:model",
                "model_version": "jra-running-style-lgbm-prod-v3",
                "release_id": "jra-running-style-lgbm-prod-v3-release",
                "role": "model",
                "serving_bucket": "pc-keiba-finish-position-models",
                "serving_key": "running-style/models/jra/latest.flatbin",
                "sha256": "1" * 64,
                "size_bytes": 2,
                "source_ref": "attestation://test/jra/latest.flatbin",
                "system": "running-style",
            },
        ],
    }

    with pytest.raises(ManifestValidationError, match="must be single-role"):
        parse_manifest(payload)


def test_parse_accepts_shared_calibrator_across_multiple_routed_models() -> None:
    """A single JRA calibrator can pair with TWO routed models (the source-
    level default plus a cell-routed variant) without a spurious duplicate
    serving-key rejection -- proving the schema now represents "N routed
    models share exactly one calibrator" instead of forcing a calibrator
    duplicate per model bundle."""
    payload: dict[str, object] = {
        "not_applicable": [],
        "schema_version": "production-artifacts/v1",
        "artifacts": [
            {
                "activation": "required",
                "activation_environment": None,
                "bundle_id": "running-style-calibrator:jra",
                "category": "jra",
                "container_path": None,
                "id": "running-style:jra:calibrator",
                "model_version": "jra-running-style-lgbm-prod-v3",
                "release_id": "jra-running-style-lgbm-prod-v3-release",
                "role": "calibrator",
                "serving_bucket": "pc-keiba-finish-position-models",
                "serving_key": "running-style/models/jra/calibrators.json",
                "sha256": "0" * 64,
                "size_bytes": 1,
                "source_ref": "attestation://test/jra/calibrators.json",
                "system": "running-style",
            },
            {
                "activation": "required",
                "activation_environment": None,
                "bundle_id": "running-style-model:jra:latest",
                "category": "jra",
                "container_path": None,
                "id": "running-style:jra:latest:model",
                "model_version": "jra-running-style-lgbm-prod-v3",
                "release_id": "jra-running-style-lgbm-prod-v3-release",
                "role": "model",
                "serving_bucket": "pc-keiba-finish-position-models",
                "serving_key": "running-style/models/jra/latest.flatbin",
                "sha256": "1" * 64,
                "size_bytes": 2,
                "source_ref": "attestation://test/jra/latest.flatbin",
                "system": "running-style",
            },
            {
                "activation": "required",
                "activation_environment": None,
                "bundle_id": "running-style-model:jra:tokyo-turf",
                "category": "jra",
                "container_path": None,
                "id": "running-style:jra:tokyo-turf:model",
                "model_version": "jra-running-style-lgbm-cell-tokyo-turf-v1",
                "release_id": "jra-running-style-lgbm-cell-tokyo-turf-v1-release",
                "role": "model",
                "serving_bucket": "pc-keiba-finish-position-models",
                "serving_key": "running-style/models/jra/cells/tokyo-turf.flatbin",
                "sha256": "2" * 64,
                "size_bytes": 3,
                "source_ref": "attestation://test/jra/cells/tokyo-turf.flatbin",
                "system": "running-style",
            },
        ],
    }

    manifest = parse_manifest(payload)

    assert len(manifest.artifacts) == 3


def test_load_manifest_rejects_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text("{", encoding="utf-8")

    with pytest.raises(ManifestValidationError, match="invalid manifest JSON"):
        load_manifest(path)


def test_report_json_is_deterministic_and_redacted() -> None:
    manifest = load_manifest()
    report = verify_selector_closure(
        manifest,
        derive_selected_artifact_keys(nar_transformer_enabled=True),
    )

    assert report.to_json() == (
        '{"findings":[],"manifest_root_sha256":"'
        + EXPECTED_MANIFEST_ROOT
        + '","not_applicable":[{"category":"ban-ei","reason":"Production running-style '
        'selectors and R2 keys support JRA and NAR only.","system":"running-style"}],'
        '"observed_count":0,"selected_count":24,"status":"MATCH","warnings":[]}'
    )


def test_main_static_match(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main([])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["status"] == "MATCH"
    assert output["selected_count"] == 24


def test_main_missing_local_artifacts_fail(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    exit_code = main(
        [
            "--artifact-root",
            str(tmp_path),
            "--system",
            "finish-position",
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert output["status"] == "INTEGRITY_FAILURE"


def test_main_without_system_verifies_all_selected_artifacts(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    exit_code = main(["--artifact-root", str(tmp_path)])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert output["selected_count"] == 24


def test_main_stage_requires_artifact_root_and_system(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    exit_code = main(["--stage-root", str(tmp_path / "staged")])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert output["status"] == "INTEGRITY_FAILURE"
    assert output["findings"][0]["message"] == (
        "--stage-root requires both --artifact-root and --system"
    )


def test_main_stages_only_after_successful_verification(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    manifest = load_manifest()
    artifact = next(
        item
        for item in manifest.artifacts
        if item.serving_key == "finish-position/jra/jra-cb-v10-prior-corner274-2013/model.json"
    )
    artifact_root = tmp_path / "models"
    stage_root = tmp_path / "staged"
    staged_calls: list[tuple[Path, Path, frozenset[str]]] = []

    monkeypatch.setattr(
        artifact_integrity,
        "derive_selected_artifact_keys",
        lambda **_kwargs: frozenset({artifact.serving_key}),
    )
    monkeypatch.setattr(
        artifact_integrity,
        "observe_artifact_tree",
        lambda _root, _keys: (
            ArtifactObservation(
                serving_key=artifact.serving_key,
                sha256=artifact.sha256,
                size_bytes=artifact.size_bytes,
                state="present",
            ),
        ),
    )
    monkeypatch.setattr(
        artifact_integrity,
        "stage_artifact_tree",
        lambda source, target, keys: staged_calls.append((source, target, keys)) or (),
    )

    exit_code = main(
        [
            "--artifact-root",
            str(artifact_root),
            "--system",
            "finish-position",
            "--stage-root",
            str(stage_root),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["status"] == "MATCH"
    assert staged_calls == [
        (
            artifact_root,
            stage_root,
            frozenset({"finish-position/jra/jra-cb-v10-prior-corner274-2013/model.json"}),
        )
    ]


def test_main_invalid_manifest_fails(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text("{}", encoding="utf-8")

    exit_code = main(["--manifest", str(path)])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert output["status"] == "INTEGRITY_FAILURE"
    assert output["manifest_root_sha256"] == ""


def test_main_os_error_is_indeterminate(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_load(_path: Path) -> ProductionArtifactManifest:
        raise OSError("storage unavailable")

    monkeypatch.setattr(artifact_integrity, "load_manifest", fail_load)
    exit_code = main([])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert output["status"] == "INDETERMINATE"
    assert output["findings"][0]["message"] == "OSError: storage unavailable"


def test_main_nar_transformer_flag_overrides_tracked_declaration(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(["--nar-transformer", "disabled"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["selected_count"] == 20


# ---------------------------------------------------------------------------
# Finding #2 regression: running-style cell-routing selector closure
# ---------------------------------------------------------------------------


def test_running_style_default_closure_matches_hardcoded_fallback_count() -> None:
    """The tracked running_style_cell_routing.json ships as ``{}`` (no live
    cell routing today), so the closure must still resolve to exactly the
    same 4 running-style keys the old hardcoded jra/nar loop produced --
    proving the rework is a non-regression for the current production
    topology."""
    selected = derive_selected_artifact_keys()
    running_style_selected = {key for key in selected if key.startswith("running-style/")}

    assert running_style_selected == {
        "running-style/models/jra/latest.flatbin",
        "running-style/models/jra/calibrators.json",
        "running-style/models/nar/latest.flatbin",
        "running-style/models/nar/calibrators.json",
    }


def test_running_style_cell_routing_matching_default_still_matches(tmp_path: Path) -> None:
    manifest = load_manifest()
    routing_path = _write_running_style_routing(
        tmp_path,
        {
            "jra": {
                "defaultVariantId": "latest",
                "rules": [],
                "variants": {"latest": {"modelKey": JRA_RS_MODEL_KEY}},
            }
        },
    )

    selected = derive_selected_artifact_keys(running_style_routing_path=routing_path)
    report = verify_selector_closure(manifest, selected)

    assert report.status == "MATCH"
    assert JRA_RS_MODEL_KEY in selected


def test_running_style_cell_routing_unmanifested_variant_fails_closed(tmp_path: Path) -> None:
    """A JRA cell-routing rule that points at a real, not-yet-manifested R2
    key must fail closed instead of silently reporting MATCH -- the exact
    bypass the independent review flagged: the OLD closure only ever looked
    at the hardcoded jra/nar "latest.flatbin" pair, so this new key would
    never even have entered `selected` and would have gone unverified."""
    manifest = load_manifest()
    new_variant_key = "running-style/models/jra/cells/tokyo-turf.flatbin"
    routing_path = _write_running_style_routing(
        tmp_path,
        {
            "jra": {
                "defaultVariantId": "latest",
                "rules": [
                    {
                        "conditions": [{"dimension": "venue", "values": ["05"]}],
                        "variantId": "tokyo-turf",
                    }
                ],
                "variants": {
                    "latest": {"modelKey": JRA_RS_MODEL_KEY},
                    "tokyo-turf": {"modelKey": new_variant_key},
                },
            }
        },
    )

    selected = derive_selected_artifact_keys(running_style_routing_path=routing_path)
    report = verify_selector_closure(manifest, selected)

    assert new_variant_key in selected
    assert report.status == "INTEGRITY_FAILURE"
    assert report.findings[0].code == "selected-unmanifested"
    assert report.findings[0].serving_key == new_variant_key


def test_running_style_cell_routing_ban_ei_unmanifested_variant_fails_closed(
    tmp_path: Path,
) -> None:
    """Ban-ei is reachable via the SAME cell-routing schema
    (RunningStyleCellCategory = "ban-ei" | "jra" | "nar") even though only
    jra/nar have a live artifact today. The OLD closure hardcoded
    ``for source in ("jra", "nar")`` and never considered Ban-ei at all, so a
    live-but-unmanifested Ban-ei cell model would have reported MATCH
    forever."""
    manifest = load_manifest()
    ban_ei_key = "running-style/models/ban-ei/latest.flatbin"
    routing_path = _write_running_style_routing(
        tmp_path,
        {
            "ban-ei": {
                "defaultVariantId": "latest",
                "rules": [],
                "variants": {"latest": {"modelKey": ban_ei_key}},
            }
        },
    )

    selected = derive_selected_artifact_keys(running_style_routing_path=routing_path)
    report = verify_selector_closure(manifest, selected)

    assert ban_ei_key in selected
    assert report.status == "INTEGRITY_FAILURE"
    assert report.findings[0].code == "selected-unmanifested"
    assert report.findings[0].serving_key == ban_ei_key


def test_running_style_cell_routing_ban_ei_uses_nar_calibrator(tmp_path: Path) -> None:
    """A Ban-ei job's ``job.source`` is always "nar" (RunningStyleSource is
    only "jra" | "nar"), so tryLoadCalibrators loads the NAR calibrator even
    for a Ban-ei cell-routed model. The closure must select the NAR
    calibrator key (not a nonexistent Ban-ei-specific one) whenever a Ban-ei
    model is selected."""
    routing_path = _write_running_style_routing(
        tmp_path,
        {
            "ban-ei": {
                "defaultVariantId": "latest",
                "rules": [],
                "variants": {"latest": {"modelKey": "running-style/models/ban-ei/latest.flatbin"}},
            }
        },
    )

    selected = derive_selected_artifact_keys(running_style_routing_path=routing_path)

    assert "running-style/models/nar/calibrators.json" in selected
    assert "running-style/models/ban-ei/calibrators.json" not in selected


def test_running_style_cell_routing_malformed_default_variant_fails_closed(
    tmp_path: Path,
) -> None:
    routing_path = _write_running_style_routing(
        tmp_path,
        {"jra": {"defaultVariantId": "missing-variant", "rules": [], "variants": {}}},
    )

    with pytest.raises(RunningStyleRoutingValidationError, match="undefined variant ids"):
        derive_selected_artifact_keys(running_style_routing_path=routing_path)


def test_running_style_cell_routing_invalid_category_fails_closed(tmp_path: Path) -> None:
    routing_path = _write_running_style_routing(tmp_path, {"usa": {"defaultVariantId": "latest"}})

    with pytest.raises(RunningStyleRoutingValidationError, match="unsupported categories"):
        derive_selected_artifact_keys(running_style_routing_path=routing_path)


# ---------------------------------------------------------------------------
# Finding #3 regression: NAR transformer selection derives from the tracked
# deploy-authority declaration, not ambient process environment.
# ---------------------------------------------------------------------------


def test_nar_transformer_default_ignores_ambient_environment_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stray ``NAR_TRANSFORMER_BLEND_ENABLED=0`` in the calling shell must
    NOT change the verifier's default: it has no relationship to the actual
    Cloudflare Worker secret. The tracked deploy_flags.json (which says
    ``true``) must still win."""
    monkeypatch.setenv("NAR_TRANSFORMER_BLEND_ENABLED", "0")

    selected = derive_selected_artifact_keys()

    assert any(
        key.startswith("finish-position/nar/iter40-nar-settransformer-blend-v1/")
        for key in selected
    )


def test_nar_transformer_deploy_flags_path_can_override_tracked_value(tmp_path: Path) -> None:
    flags_path = _write_deploy_flags(tmp_path, {"nar_transformer_blend_enabled": False})

    selected = derive_selected_artifact_keys(deploy_flags_path=flags_path)

    assert not any(
        key.startswith("finish-position/nar/iter40-nar-settransformer-blend-v1/")
        for key in selected
    )


def test_nar_transformer_explicit_override_wins_over_tracked_file(tmp_path: Path) -> None:
    flags_path = _write_deploy_flags(tmp_path, {"nar_transformer_blend_enabled": False})

    selected = derive_selected_artifact_keys(
        nar_transformer_enabled=True, deploy_flags_path=flags_path
    )

    assert any(
        key.startswith("finish-position/nar/iter40-nar-settransformer-blend-v1/")
        for key in selected
    )


def test_nar_transformer_deploy_flags_malformed_fails_closed(tmp_path: Path) -> None:
    flags_path = _write_deploy_flags(tmp_path, {})

    with pytest.raises(DeployFlagsValidationError, match="keys differ"):
        derive_selected_artifact_keys(deploy_flags_path=flags_path)


# ---------------------------------------------------------------------------
# Stage-1 market-free gated fallback: its artifact is always in the selector
# closure whenever stage1_routing.json enables it for a category (mirrors the
# cell-routing variant precedent: reachability does not depend on whether any
# particular race would actually trip the gate today).
# ---------------------------------------------------------------------------

_STAGE1_JRA_KEY_PREFIX = "finish-position/jra/jra-cb-stage1-marketfree235-2013/"


def _write_stage1_routing(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "stage1_routing.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_tracked_stage1_routing_is_selected_by_default() -> None:
    """The live tracked stage1_routing.json enables JRA -- its artifact must
    always be in the selector closure, matching the live manifest bundle."""
    selected = derive_selected_artifact_keys()

    assert any(key.startswith(_STAGE1_JRA_KEY_PREFIX) for key in selected)


def test_stage1_routing_path_can_disable_selection(tmp_path: Path) -> None:
    routing_path = _write_stage1_routing(
        tmp_path,
        {
            "jra": {
                "enabled": False,
                "model_version": "jra-cb-stage1-marketfree235-2013",
                "feature_count": 235,
                "architecture": "catboost",
                "stddev_threshold": 0.4,
                "enable_stddev_safety_net": True,
            }
        },
    )

    selected = derive_selected_artifact_keys(stage1_routing_path=routing_path)

    assert not any(key.startswith(_STAGE1_JRA_KEY_PREFIX) for key in selected)


def test_stage1_routing_path_empty_object_selects_nothing(tmp_path: Path) -> None:
    routing_path = _write_stage1_routing(tmp_path, {})

    selected = derive_selected_artifact_keys(stage1_routing_path=routing_path)

    assert not any(key.startswith(_STAGE1_JRA_KEY_PREFIX) for key in selected)


def test_stage1_routing_path_malformed_fails_closed(tmp_path: Path) -> None:
    routing_path = _write_stage1_routing(tmp_path, {"usa": {}})

    with pytest.raises(Stage1RoutingValidationError, match="unsupported categories"):
        derive_selected_artifact_keys(stage1_routing_path=routing_path)


def test_disabled_stage1_routing_is_unselected_warning_only(tmp_path: Path) -> None:
    """Disabling Stage-1 via an override (e.g. to preview a rollback before
    flipping the tracked file) makes its two manifested bundle artifacts
    unselected -- a warning, not an INTEGRITY_FAILURE, since the manifest
    still legitimately carries their bytes (registered, ready to flip back
    on) -- same pattern as ``test_disabled_transformer_is_unselected_warning_only``."""
    manifest = load_manifest()
    routing_path = _write_stage1_routing(
        tmp_path,
        {
            "jra": {
                "enabled": False,
                "model_version": "jra-cb-stage1-marketfree235-2013",
                "feature_count": 235,
                "architecture": "catboost",
                "stddev_threshold": 0.4,
                "enable_stddev_safety_net": True,
            }
        },
    )
    selected = derive_selected_artifact_keys(stage1_routing_path=routing_path)
    report = verify_selector_closure(manifest, selected)

    assert report.status == "MATCH"
    assert any(
        warning == f"unselected manifest artifact: {_STAGE1_JRA_KEY_PREFIX}metadata.json"
        for warning in report.warnings
    )
    assert any(
        warning == f"unselected manifest artifact: {_STAGE1_JRA_KEY_PREFIX}model.json"
        for warning in report.warnings
    )


def test_stage1_manifested_bundle_matches_tracked_routing_model_version() -> None:
    """The manifest's Stage-1 bundle_id must reference the SAME model_version
    the tracked stage1_routing.json actually routes to -- a drift here would
    mean the artifact-integrity preflight silently verifies bytes for a
    version that could never actually be selected at serve time."""
    manifest = load_manifest()
    stage1_config = load_stage1_routing()["jra"]
    stage1_artifacts = [
        artifact
        for artifact in manifest.artifacts
        if artifact.system == "finish-position"
        and artifact.category == "jra"
        and artifact.model_version == stage1_config.model_version
    ]

    assert {artifact.role for artifact in stage1_artifacts} == {"metadata", "model"}


# ---------------------------------------------------------------------------
# Finding #1 regression: deploy cannot proceed on nonzero local verifier
# result -- both the cron package's `deploy` script and the container
# Dockerfile wire the verifier in ahead of the actual deploy/build action.
# ---------------------------------------------------------------------------


def test_dockerfile_stages_only_selected_models_and_verifies_runtime_tree() -> None:
    dockerfile = (REPO_ROOT / "apps/finish-position-predict-container/Dockerfile").read_text(
        encoding="utf-8"
    )
    lines = dockerfile.splitlines()
    source_copy_index = next(
        index
        for index, line in enumerate(lines)
        if line == "COPY apps/finish-position-predict-container/models /artifact-source"
    )
    staged_copy_index = next(
        index
        for index, line in enumerate(lines)
        if line == "COPY --from=model-artifacts /selected-models /models"
    )
    verify_run_index = next(
        index
        for index, line in enumerate(lines)
        if line.startswith("RUN")
        and "predict_lib.artifact_integrity" in line
        and "--artifact-root /models" in line
    )

    assert source_copy_index < staged_copy_index < verify_run_index
    assert "--stage-root /selected-models" in dockerfile
    assert "COPY apps/finish-position-predict-container/models /models" not in dockerfile
    assert "--artifact-root /models" in lines[verify_run_index]
    assert "--system finish-position" in lines[verify_run_index]


def test_deploy_script_blocks_wrangler_when_verification_fails(tmp_path: Path) -> None:
    """Adversarial regression: `apps/finish-position-cron/package.json`'s
    `deploy` script must run the artifact-integrity preflight before
    `wrangler deploy` and never reach wrangler when the preflight fails.
    Proven by executing the REAL deploy script string with a stub `wrangler`
    on PATH that only ever writes a marker file if invoked. The ambient
    `apps/finish-position-predict-container/models/` directory is gitignored
    and may or may not be populated with real artifacts depending on the
    checkout, so the execution proof redirects `--artifact-root` to a
    guaranteed-empty tmp dir: this makes the preflight fail closed
    deterministically regardless of local models/ state, while the
    assertions above still pin the real deploy script's
    `artifact:verify`-before-`wrangler deploy` ordering."""
    cron_package_json = json.loads(
        (REPO_ROOT / "apps/finish-position-cron/package.json").read_text(encoding="utf-8")
    )
    deploy_script = cron_package_json["scripts"]["deploy"]
    assert "artifact:verify" in deploy_script
    assert "wrangler deploy" in deploy_script
    assert "--artifact-root models" in deploy_script

    empty_artifact_root = tmp_path / "empty-models"
    empty_artifact_root.mkdir()
    hermetic_script = deploy_script.replace(
        "--artifact-root models", f"--artifact-root {empty_artifact_root}"
    )

    stub_bin = tmp_path / "bin"
    stub_bin.mkdir()
    marker = tmp_path / "wrangler-invoked"
    wrangler_stub = stub_bin / "wrangler"
    wrangler_stub.write_text(f"#!/bin/sh\ntouch {marker}\n", encoding="utf-8")
    wrangler_stub.chmod(0o755)

    result = subprocess.run(
        ["sh", "-c", hermetic_script],
        check=False,
        cwd=str(REPO_ROOT / "apps/finish-position-cron"),
        env={**os.environ, "PATH": f"{stub_bin}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode != 0
    assert not marker.exists()
    assert '"status":"INTEGRITY_FAILURE"' in result.stdout
