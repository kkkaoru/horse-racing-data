"""Tests for mlflow_tracking.backfill_finish_position."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from mlflow_tracking import backfill_finish_position as bfp
from mlflow_tracking import registry

WriteJsonFixture = Callable[[Path, object], None]

BASE_METADATA: dict[str, object] = {
    "model_version": "test-cb-v1",
    "architecture": "catboost",
    "category": "jra",
    "feature_count": 5,
    "feature_names": ["a", "b", "c", "d", "e"],
    "n_train_rows": 100,
    "hyperparams": {"iterations": 10, "learning_rate": 0.1},
    "scale_and_bias": {"scale": 1.0, "bias": 0.0},
    "train_date_range": ["20200101", "20201231"],
    "note": "test model",
}

MANIFEST: dict[str, object] = {
    "model_version": "test-ensemble-C-v1",
    "category": "nar",
    "kyoso_joken_code": "C",
    "ensemble_type": "rank_blend",
    "members": [
        {"model_version": "member-a", "weight": 0.5, "is_baseline": True},
        {"model_version": "member-b", "weight": 0.5, "is_baseline": False},
    ],
    "validation_window": {"start_year": 2018, "end_year": 2022, "race_count": 100},
    "holdout_window": {"start_year": 2023, "end_year": 2026, "race_count": 200},
    "holdout_top1": 0.55,
    "baseline_holdout_top1": 0.53,
    "delta_pp": 0.02,
    "search_method": "optuna_tpe",
    "n_trials": 100,
    "seed": 42,
    "baseline_version": "member-a",
    "source_experiment": "tmp/experiment.json",
    "note": "test manifest",
}


def test_discover_base_metadata_files_finds_expected_paths(
    tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(tmp_path / "jra" / "test-cb-v1" / "metadata.json", BASE_METADATA)
    write_json(
        tmp_path / "nar" / "per-class" / "C" / "test-ensemble-C-v1" / "manifest.json", MANIFEST
    )
    found = bfp.discover_base_metadata_files(tmp_path)
    assert found == [tmp_path / "jra" / "test-cb-v1" / "metadata.json"]


def test_discover_base_metadata_files_returns_empty_for_missing_root(tmp_path: Path) -> None:
    assert bfp.discover_base_metadata_files(tmp_path / "missing") == []


def test_discover_per_class_manifests_finds_expected_paths(
    tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(
        tmp_path / "nar" / "per-class" / "C" / "test-ensemble-C-v1" / "manifest.json", MANIFEST
    )
    found = bfp.discover_per_class_manifests(tmp_path)
    assert found == [tmp_path / "nar" / "per-class" / "C" / "test-ensemble-C-v1" / "manifest.json"]


def test_discover_per_class_manifests_returns_empty_for_missing_root(tmp_path: Path) -> None:
    assert bfp.discover_per_class_manifests(tmp_path / "missing") == []


def test_backfill_one_metadata_file_registers_version_with_expected_tags(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata_path = tmp_path / "jra" / "test-cb-v1" / "metadata.json"
    write_json(metadata_path, BASE_METADATA)
    version = bfp.backfill_one_metadata_file(client, metadata_path, tmp_path)
    assert version.name == "jra-finish-position"
    assert version.tags["model_version"] == "test-cb-v1"
    assert version.tags["architecture"] == "catboost"
    assert version.tags["category"] == "jra"
    assert version.tags["feature_count"] == "5"
    assert version.tags["train_window"] == "20200101-20201231"
    assert version.run_id is not None
    run = client.get_run(version.run_id)
    assert run.info.status == "FINISHED"
    assert run.data.params["feature_count"] == "5"
    assert run.data.params["hyperparams.iterations"] == "10"
    assert run.data.tags["note"] == "test model"


def test_backfill_one_metadata_file_routes_long_feature_names_to_artifact(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    long_metadata = dict(BASE_METADATA)
    long_metadata["feature_names"] = [f"feature_{i:04d}" for i in range(600)]
    metadata_path = tmp_path / "jra" / "test-cb-v2" / "metadata.json"
    write_json(metadata_path, long_metadata)
    version = bfp.backfill_one_metadata_file(client, metadata_path, tmp_path)
    assert version.run_id is not None
    artifact_paths = {a.path for a in client.list_artifacts(version.run_id)}
    assert "feature_names.json" in artifact_paths
    run = client.get_run(version.run_id)
    assert "feature_names" not in run.data.params


def test_backfill_one_metadata_file_uses_directory_name_when_model_version_missing(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata = {"architecture": "catboost", "category": "jra", "feature_count": 3}
    metadata_path = tmp_path / "jra" / "unlabeled-version" / "metadata.json"
    write_json(metadata_path, metadata)
    version = bfp.backfill_one_metadata_file(client, metadata_path, tmp_path)
    assert version.tags["model_version"] == "unlabeled-version"


def test_backfill_one_metadata_file_uses_directory_name_when_metadata_model_version_mismatches(
    client: MlflowClient,
    tmp_path: Path,
    write_json: WriteJsonFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    typo_metadata = dict(BASE_METADATA)
    typo_metadata["model_version"] = "TEST-CB-V1"  # real-world case: "-CLEAN" vs. dir's "-clean"
    metadata_path = tmp_path / "jra" / "test-cb-v1" / "metadata.json"
    write_json(metadata_path, typo_metadata)
    with caplog.at_level("WARNING"):
        version = bfp.backfill_one_metadata_file(client, metadata_path, tmp_path)
    assert version.tags["model_version"] == "test-cb-v1"
    assert version.tags["metadata_model_version_raw"] == "TEST-CB-V1"
    assert "does not match artifact directory name" in caplog.text


def test_backfill_one_manifest_registers_version_with_expected_tags_and_metrics(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest_path = tmp_path / "nar" / "per-class" / "C" / "test-ensemble-C-v1" / "manifest.json"
    write_json(manifest_path, MANIFEST)
    version = bfp.backfill_one_manifest(client, manifest_path, tmp_path)
    assert version.name == "nar-finish-position"
    assert version.tags["model_version"] == "test-ensemble-C-v1"
    assert version.tags["class_code"] == "C"
    assert version.tags["category"] == "nar"
    assert version.tags["ensemble_type"] == "rank_blend"
    assert version.tags["routing_scope"] == "class:C"
    assert version.run_id is not None
    run = client.get_run(version.run_id)
    assert run.data.metrics["holdout_top1"] == 0.55
    assert run.data.metrics["baseline_holdout_top1"] == 0.53
    assert run.data.metrics["delta_pp"] == 0.02
    assert run.data.params["validation_window.start_year"] == "2018"
    assert run.data.params["n_trials"] == "100"
    assert run.data.tags["search_method"] == "optuna_tpe"
    assert run.data.tags["baseline_version"] == "member-a"
    artifact_paths = {a.path for a in client.list_artifacts(version.run_id)}
    assert "manifest.json" in artifact_paths


def test_backfill_one_manifest_routes_long_field_to_artifact(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MANIFEST)
    manifest["members"] = [
        {"model_version": f"member-{i:04d}", "weight": 0.01, "is_baseline": False}
        for i in range(200)
    ]
    manifest_path = tmp_path / "nar" / "per-class" / "C" / "test-long-manifest-v1" / "manifest.json"
    write_json(manifest_path, manifest)
    version = bfp.backfill_one_manifest(client, manifest_path, tmp_path)
    assert version.run_id is not None
    artifact_paths = {a.path for a in client.list_artifacts(version.run_id)}
    assert "members.json" in artifact_paths
    run = client.get_run(version.run_id)
    assert "members" not in run.data.params


def test_backfill_one_manifest_uses_directory_name_when_manifest_model_version_mismatches(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    typo_manifest = dict(MANIFEST)
    typo_manifest["model_version"] = "TEST-ENSEMBLE-C-V1"
    manifest_path = tmp_path / "nar" / "per-class" / "C" / "test-ensemble-C-v1" / "manifest.json"
    write_json(manifest_path, typo_manifest)
    version = bfp.backfill_one_manifest(client, manifest_path, tmp_path)
    assert version.tags["model_version"] == "test-ensemble-C-v1"
    assert version.tags["metadata_model_version_raw"] == "TEST-ENSEMBLE-C-V1"


def test_backfill_one_metadata_file_is_idempotent_on_unchanged_artifact(
    client: MlflowClient,
    tmp_path: Path,
    write_json: WriteJsonFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Re-running against the identical metadata_path (no change to
    metadata.json) must not mint a second version -- the same bug shape as
    the verified jra-running-style v1/v2 duplicate, but for the base
    category-tree registration path.
    """
    metadata_path = tmp_path / "jra" / "test-cb-v1" / "metadata.json"
    write_json(metadata_path, BASE_METADATA)

    first = bfp.backfill_one_metadata_file(client, metadata_path, tmp_path)
    with caplog.at_level("INFO"):
        second = bfp.backfill_one_metadata_file(client, metadata_path, tmp_path)

    assert second.version == first.version
    assert "skipping duplicate registration" in caplog.text
    versions = client.search_model_versions("name='jra-finish-position'")
    matching = [v for v in versions if v.tags.get("model_version") == "test-cb-v1"]
    assert len(matching) == 1


def test_backfill_one_metadata_file_registers_new_version_when_source_changes(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """Same model_version label reused under a genuinely different source
    (different parent directory -> different resolved file:// URI) must
    still register as a new version -- proves the duplicate guard is not
    overly broad (label-only matching would have wrongly no-op'd this).
    """
    first_path = tmp_path / "run-1" / "jra" / "test-cb-v1" / "metadata.json"
    write_json(first_path, BASE_METADATA)
    first = bfp.backfill_one_metadata_file(client, first_path, tmp_path / "run-1")

    second_path = tmp_path / "run-2" / "jra" / "test-cb-v1" / "metadata.json"
    write_json(second_path, BASE_METADATA)
    second = bfp.backfill_one_metadata_file(client, second_path, tmp_path / "run-2")

    assert second.version != first.version
    assert second.source != first.source
    versions = client.search_model_versions("name='jra-finish-position'")
    assert len(versions) == 2


def test_backfill_one_manifest_is_idempotent_on_unchanged_artifact(
    client: MlflowClient,
    tmp_path: Path,
    write_json: WriteJsonFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Re-running against the identical manifest_path (no change to
    manifest.json) must not mint a second version -- same bug shape as the
    base metadata path, but for the per-class ensemble registration path.
    """
    manifest_path = tmp_path / "nar" / "per-class" / "C" / "test-ensemble-C-v1" / "manifest.json"
    write_json(manifest_path, MANIFEST)

    first = bfp.backfill_one_manifest(client, manifest_path, tmp_path)
    with caplog.at_level("INFO"):
        second = bfp.backfill_one_manifest(client, manifest_path, tmp_path)

    assert second.version == first.version
    assert "skipping duplicate registration" in caplog.text
    versions = client.search_model_versions("name='nar-finish-position'")
    matching = [v for v in versions if v.tags.get("model_version") == "test-ensemble-C-v1"]
    assert len(matching) == 1


def test_backfill_one_manifest_registers_new_version_when_source_changes(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """Same model_version label reused under a genuinely different source
    must still register as a new version -- proves the duplicate guard is
    not overly broad.
    """
    first_manifest_path = (
        tmp_path / "run-1" / "nar" / "per-class" / "C" / "test-ensemble-C-v1" / "manifest.json"
    )
    write_json(first_manifest_path, MANIFEST)
    first = bfp.backfill_one_manifest(client, first_manifest_path, tmp_path / "run-1")

    second_manifest_path = (
        tmp_path / "run-2" / "nar" / "per-class" / "C" / "test-ensemble-C-v1" / "manifest.json"
    )
    write_json(second_manifest_path, MANIFEST)
    second = bfp.backfill_one_manifest(client, second_manifest_path, tmp_path / "run-2")

    assert second.version != first.version
    assert second.source != first.source
    versions = client.search_model_versions("name='nar-finish-position'")
    assert len(versions) == 2


def test_sync_champions_resolves_metadata_model_version_directory_mismatch(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """Reproduces the real jra-cb-v9-sim-2013-clean incident: metadata.json's
    model_version carried a stray "-CLEAN" case typo while the directory
    name, and hence model_meta.json's pointer, used lowercase "-clean". The
    canonicalization in backfill_one_metadata_file must make champion sync
    succeed without needing registry.find_version_by_label's casefold
    fallback at all (the version's tag now matches model_meta.json exactly).
    """
    typo_metadata = dict(BASE_METADATA)
    typo_metadata["model_version"] = "jra-cb-v9-sim-2013-CLEAN"
    metadata_path = tmp_path / "models" / "jra" / "jra-cb-v9-sim-2013-clean" / "metadata.json"
    write_json(metadata_path, typo_metadata)
    bfp.backfill_one_metadata_file(client, metadata_path, tmp_path / "models")

    write_json(
        tmp_path / "predict_lib" / "model_meta.json",
        {"model_versions": {"jra": "jra-cb-v9-sim-2013-clean"}},
    )
    result = bfp.sync_champions(client, tmp_path / "predict_lib")
    assert result == {"jra": "ok"}
    registered = client.get_registered_model("jra-finish-position")
    assert registry.CHAMPION_ALIAS in registered.aliases


def test_sync_champions_skips_category_when_search_model_versions_raises(
    client: MlflowClient,
    tmp_path: Path,
    write_json: WriteJsonFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(filter_string: str) -> None:
        raise MlflowException("boom")

    write_json(tmp_path / "model_meta.json", {"model_versions": {"jra": "v1"}})
    monkeypatch.setattr(client, "search_model_versions", _raise)
    result = bfp.sync_champions(client, tmp_path)
    assert result["jra"].startswith("skipped:")


def test_backfill_one_manifest_uses_directory_name_when_model_version_missing(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = {"category": "nar", "kyoso_joken_code": "OP"}
    manifest_path = tmp_path / "nar" / "per-class" / "OP" / "unlabeled-ensemble" / "manifest.json"
    write_json(manifest_path, manifest)
    version = bfp.backfill_one_manifest(client, manifest_path, tmp_path)
    assert version.tags["model_version"] == "unlabeled-ensemble"


def test_sync_champions_returns_empty_when_model_meta_missing(
    client: MlflowClient, tmp_path: Path
) -> None:
    assert bfp.sync_champions(client, tmp_path) == {}


def test_sync_champions_returns_empty_when_model_versions_not_a_dict(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(tmp_path / "model_meta.json", {"model_versions": "not-a-dict"})
    assert bfp.sync_champions(client, tmp_path) == {}


def test_sync_champions_sets_champion_for_matching_registered_version(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata_path = tmp_path / "models" / "jra" / "test-cb-v1" / "metadata.json"
    write_json(metadata_path, BASE_METADATA)
    bfp.backfill_one_metadata_file(client, metadata_path, tmp_path / "models")

    write_json(
        tmp_path / "predict_lib" / "model_meta.json",
        {"model_versions": {"jra": "test-cb-v1"}},
    )
    result = bfp.sync_champions(client, tmp_path / "predict_lib")
    assert result == {"jra": "ok"}
    registered = client.get_registered_model("jra-finish-position")
    assert registry.CHAMPION_ALIAS in registered.aliases


def test_sync_champions_skips_unsupported_category(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(
        tmp_path / "model_meta.json",
        {"model_versions": {"keirin": "some-version"}},
    )
    result = bfp.sync_champions(client, tmp_path)
    assert result["keirin"].startswith("skipped:")


def test_sync_champions_skips_when_no_registered_version_matches(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(
        tmp_path / "model_meta.json",
        {"model_versions": {"jra": "never-registered"}},
    )
    result = bfp.sync_champions(client, tmp_path)
    assert result["jra"].startswith("skipped:")


def test_ingest_cell_routing_returns_none_when_file_missing(
    client: MlflowClient, tmp_path: Path
) -> None:
    assert bfp.ingest_cell_routing(client, tmp_path) is None


def test_ingest_cell_routing_tags_registered_variant_and_skips_unregistered(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata_path = tmp_path / "models" / "jra" / "sim-version" / "metadata.json"
    sim_metadata = dict(BASE_METADATA)
    sim_metadata["model_version"] = "sim-version"
    write_json(metadata_path, sim_metadata)
    bfp.backfill_one_metadata_file(client, metadata_path, tmp_path / "models")

    cell_routing = {
        "jra": {
            "default_variant": "sim",
            "variants": {
                "sim": {
                    "model_version": "sim-version",
                    "feature_count": 5,
                    "architecture": "catboost",
                },
                "unregistered": {
                    "model_version": "never-registered-version",
                    "feature_count": 5,
                    "architecture": "catboost",
                },
            },
            "rules": [
                {
                    "conditions": [{"dimension": "kyoso_joken_code", "values": ["703"]}],
                    "variant": "sim",
                },
                {
                    "conditions": [{"dimension": "kyoso_joken_code", "values": ["704"]}],
                    "variant": "unregistered",
                },
            ],
        },
        "not-a-category": {"rules": [], "variants": {}},
        "banei": "not-a-dict-payload",
    }
    write_json(tmp_path / "predict_lib" / "cell_routing.json", cell_routing)
    run_id = bfp.ingest_cell_routing(client, tmp_path / "predict_lib")
    assert run_id is not None
    artifact_paths = {a.path for a in client.list_artifacts(run_id)}
    assert "cell_routing.json" in artifact_paths

    registered = client.get_registered_model("jra-finish-position")
    versions = client.search_model_versions("name='jra-finish-position'")
    sim_version = next(v for v in versions if v.tags.get("model_version") == "sim-version")
    assert sim_version.tags.get("routing_scope") == "sim"
    assert registered.name == "jra-finish-position"


def test_ingest_cell_routing_skips_category_with_missing_rules_or_variants(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(
        tmp_path / "cell_routing.json",
        {"jra": {"variants": {"sim": {"model_version": "x"}}}},
    )
    run_id = bfp.ingest_cell_routing(client, tmp_path)
    assert run_id is not None


def test_ingest_cell_routing_skips_rule_with_missing_variant_key(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(
        tmp_path / "cell_routing.json",
        {
            "jra": {
                "variants": {"sim": {"model_version": "x"}},
                "rules": [{"conditions": []}],
            }
        },
    )
    run_id = bfp.ingest_cell_routing(client, tmp_path)
    assert run_id is not None


def test_ingest_cell_routing_skips_variant_referenced_but_absent_from_variants(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(
        tmp_path / "cell_routing.json",
        {
            "jra": {
                "variants": {"sim": {"model_version": "x"}},
                "rules": [{"variant": "missing-variant"}],
            }
        },
    )
    run_id = bfp.ingest_cell_routing(client, tmp_path)
    assert run_id is not None


def test_ingest_cell_routing_skips_variant_with_non_string_model_version(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(
        tmp_path / "cell_routing.json",
        {
            "jra": {
                "variants": {"sim": {"model_version": None}},
                "rules": [{"variant": "sim"}],
            }
        },
    )
    run_id = bfp.ingest_cell_routing(client, tmp_path)
    assert run_id is not None


def test_backfill_finish_position_full_happy_path(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    models_root = tmp_path / "models" / "finish-position"
    write_json(models_root / "jra" / "test-cb-v1" / "metadata.json", BASE_METADATA)
    write_json(
        models_root / "nar" / "per-class" / "C" / "test-ensemble-C-v1" / "manifest.json", MANIFEST
    )
    predict_lib_root = tmp_path / "predict_lib"
    write_json(predict_lib_root / "model_meta.json", {"model_versions": {"jra": "test-cb-v1"}})
    write_json(
        predict_lib_root / "cell_routing.json",
        {
            "jra": {
                "variants": {
                    "sim": {
                        "model_version": "test-cb-v1",
                        "feature_count": 5,
                        "architecture": "catboost",
                    }
                },
                "rules": [{"conditions": [], "variant": "sim"}],
            }
        },
    )

    summary = bfp.backfill_finish_position(client, models_root, predict_lib_root)
    assert summary["base_versions_registered"] == 1
    assert summary["per_class_versions_registered"] == 1
    assert summary["errors"] == []
    assert summary["champion_sync"] == {"jra": "ok"}
    assert summary["cell_routing_run_id"] is not None


def test_backfill_finish_position_is_idempotent_across_repeated_calls(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """Re-running the full outer backfill over an unchanged models_root must
    not grow the registered-version counts on the second call -- the outer
    function's own regression check for the duplicate-registration bug, on
    top of the per-file checks above (mirrors
    test_backfill_running_style_is_idempotent_across_repeated_calls).
    """
    models_root = tmp_path / "models" / "finish-position"
    write_json(models_root / "jra" / "test-cb-v1" / "metadata.json", BASE_METADATA)
    write_json(
        models_root / "nar" / "per-class" / "C" / "test-ensemble-C-v1" / "manifest.json", MANIFEST
    )
    predict_lib_root = tmp_path / "predict_lib"

    first_summary = bfp.backfill_finish_position(client, models_root, predict_lib_root)
    assert first_summary["base_versions_registered"] == 1
    assert first_summary["per_class_versions_registered"] == 1

    second_summary = bfp.backfill_finish_position(client, models_root, predict_lib_root)
    assert second_summary["base_versions_registered"] == 0
    assert second_summary["per_class_versions_registered"] == 0
    assert second_summary["errors"] == []

    assert len(client.search_model_versions("name='jra-finish-position'")) == 1
    assert len(client.search_model_versions("name='nar-finish-position'")) == 1


def test_discover_flat_metadata_files_finds_one_level_metadata(
    tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(tmp_path / "jra-cb-flat-v1" / "metadata.json", BASE_METADATA)
    found = bfp.discover_flat_metadata_files(tmp_path)
    assert found == [tmp_path / "jra-cb-flat-v1" / "metadata.json"]


def test_discover_flat_metadata_files_returns_empty_for_missing_root(tmp_path: Path) -> None:
    assert bfp.discover_flat_metadata_files(tmp_path / "missing") == []


def test_backfill_one_metadata_file_accepts_category_override_for_flat_layout(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata_path = tmp_path / "test-cb-v1" / "metadata.json"
    write_json(metadata_path, BASE_METADATA)
    version = bfp.backfill_one_metadata_file(client, metadata_path, tmp_path, category="jra")
    assert version.name == "jra-finish-position"
    assert version.tags["category"] == "jra"
    assert version.tags["model_version"] == "test-cb-v1"


def test_backfill_finish_position_flat_registers_new_version(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(tmp_path / "test-cb-v1" / "metadata.json", BASE_METADATA)
    summary = bfp.backfill_finish_position_flat(client, tmp_path)
    assert summary["versions_registered"] == 1
    assert summary["skipped_existing"] == []
    assert summary["errors"] == []
    versions = client.search_model_versions("name='jra-finish-position'")
    assert len(versions) == 1
    assert versions[0].tags["model_version"] == "test-cb-v1"


def test_backfill_finish_position_flat_skips_already_registered_label(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    nested_path = tmp_path / "nested" / "jra" / "test-cb-v1" / "metadata.json"
    write_json(nested_path, BASE_METADATA)
    bfp.backfill_one_metadata_file(client, nested_path, tmp_path / "nested")

    flat_root = tmp_path / "flat"
    write_json(flat_root / "test-cb-v1" / "metadata.json", BASE_METADATA)
    summary = bfp.backfill_finish_position_flat(client, flat_root)
    assert summary["versions_registered"] == 0
    assert summary["skipped_existing"] == ["test-cb-v1"]
    assert summary["errors"] == []
    versions = client.search_model_versions("name='jra-finish-position'")
    assert len(versions) == 1


def test_backfill_finish_position_flat_records_error_for_missing_category_field(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata_without_category = {k: v for k, v in BASE_METADATA.items() if k != "category"}
    write_json(tmp_path / "no-category-v1" / "metadata.json", metadata_without_category)
    summary = bfp.backfill_finish_position_flat(client, tmp_path)
    assert summary["versions_registered"] == 0
    assert summary["skipped_existing"] == []
    assert len(summary["errors"]) == 1
    assert "missing a string 'category' field" in summary["errors"][0]


def test_backfill_finish_position_flat_records_error_for_malformed_json(
    client: MlflowClient, tmp_path: Path
) -> None:
    bad_path = tmp_path / "bad-version" / "metadata.json"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_text("{not valid json", encoding="utf-8")
    summary = bfp.backfill_finish_position_flat(client, tmp_path)
    assert summary["versions_registered"] == 0
    assert len(summary["errors"]) == 1


def test_backfill_finish_position_records_errors_without_stopping(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    models_root = tmp_path / "models"
    good_metadata_path = models_root / "jra" / "good-version" / "metadata.json"
    write_json(good_metadata_path, BASE_METADATA)
    bad_metadata_path = models_root / "jra" / "bad-version" / "metadata.json"
    bad_metadata_path.parent.mkdir(parents=True, exist_ok=True)
    bad_metadata_path.write_text("{not valid json", encoding="utf-8")

    bad_manifest_path = models_root / "nar" / "per-class" / "C" / "bad-ensemble" / "manifest.json"
    bad_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    bad_manifest_path.write_text("{not valid json", encoding="utf-8")

    summary = bfp.backfill_finish_position(client, models_root, tmp_path / "no-predict-lib")
    assert summary["base_versions_registered"] == 1
    assert summary["per_class_versions_registered"] == 0
    assert len(summary["errors"]) == 2
    assert summary["champion_sync"] == {}
    assert summary["cell_routing_run_id"] is None
