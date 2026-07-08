"""Tests for mlflow_tracking.backfill_running_style."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from mlflow import MlflowClient

from mlflow_tracking import backfill_running_style as brs

WriteJsonFixture = Callable[[Path, object], None]

RS_METADATA: dict[str, object] = {
    "model_version": "jra-running-style-lgbm-prod-v3",
    "num_classes": 4,
    "class_labels": ["nige", "senkou", "sashi", "oikomi"],
    "feature_columns": ["speed_index", "corner_1_norm", "career_win_rate"],
    "categorical_features": ["keibajo_code"],
    "train_rows": 500000,
    "train_start_date": "2005-01-01",
    "train_end_date": "2025-12-31",
    "feature_schema_version": "v2",
    "hyperparameters": {"num_leaves": 63, "learning_rate": 0.05},
    "class_weight_scheme": "balanced",
    "walk_forward_results": {
        "2024": {
            "holdout_year": 2024,
            "train_start_date": "2005-01-01",
            "train_end_date": "2023-12-31",
            "train_rows": 400000,
            "valid_rows": 20000,
            "precision_nige": 0.55,
            "recall_nige": 0.5,
            "log_loss_nige": 0.3,
            "multi_log_loss": 0.9,
            "accuracy": 0.483,
        },
        "2025": {
            "holdout_year": 2025,
            "train_start_date": "2005-01-01",
            "train_end_date": "2024-12-31",
            "train_rows": 420000,
            "valid_rows": 21000,
            "precision_nige": 0.56,
            "recall_nige": 0.51,
            "log_loss_nige": 0.29,
            "multi_log_loss": 0.88,
            "accuracy": 0.49,
        },
    },
    "production_precision_nige": 0.552,
}

FOREIGN_FP_METADATA: dict[str, object] = {
    "model_version": "jra-cb-v9-sim",
    "architecture": "catboost",
    "category": "jra",
    "feature_count": 250,
    "feature_names": ["a", "b"],
}

CALIBRATOR_PAYLOAD: dict[str, object] = {
    "category": "jra",
    "fit_year": 2025,
    "classes": ["nige", "senkou", "sashi", "oikomi"],
    "calibrators": {
        "nige": {"x": [0.0, 1.0], "y": [0.0, 1.0]},
    },
}


def test_production_version_label() -> None:
    assert brs.production_version_label("jra") == "jra-running-style-lgbm-prod-v3"


def test_build_r2_serving_key() -> None:
    assert brs.build_r2_serving_key("nar") == "running-style/models/nar/latest.flatbin"


def test_build_r2_serving_uri() -> None:
    assert (
        brs.build_r2_serving_uri("jra")
        == "s3://pc-keiba-finish-position-models/running-style/models/jra/latest.flatbin"
    )


def test_calibrator_path_for(tmp_path: Path) -> None:
    result = brs.calibrator_path_for("nar", tmp_path)
    assert result == tmp_path / "nar-rs-v3-calibrators.json"


def test_discover_metadata_files_returns_empty_for_missing_root(tmp_path: Path) -> None:
    assert brs.discover_metadata_files(tmp_path / "missing") == []


def test_discover_metadata_files_finds_rs_metadata(
    tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(tmp_path / "jra-running-style-lgbm-prod-v3" / "metadata.json", RS_METADATA)
    found = brs.discover_metadata_files(tmp_path)
    assert found == [tmp_path / "jra-running-style-lgbm-prod-v3" / "metadata.json"]


def test_discover_metadata_files_filters_out_foreign_finish_position_metadata(
    tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(tmp_path / "jra-running-style-lgbm-prod-v3" / "metadata.json", RS_METADATA)
    write_json(tmp_path / "jra-cb-v9-sim" / "metadata.json", FOREIGN_FP_METADATA)
    found = brs.discover_metadata_files(tmp_path)
    assert found == [tmp_path / "jra-running-style-lgbm-prod-v3" / "metadata.json"]


def test_discover_metadata_files_skips_malformed_json(tmp_path: Path) -> None:
    bad_path = tmp_path / "bad-version" / "metadata.json"
    bad_path.parent.mkdir(parents=True)
    bad_path.write_text("{not valid json", encoding="utf-8")
    assert brs.discover_metadata_files(tmp_path) == []


def test_backfill_one_running_style_metadata_with_local_model_file(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    version_dir = tmp_path / "jra-running-style-lgbm-prod-v3"
    write_json(version_dir / "metadata.json", RS_METADATA)
    (version_dir / "model.txt").write_text("dummy booster text", encoding="utf-8")
    calibrator_dir = tmp_path / "calibrators"
    write_json(calibrator_dir / "jra-rs-v3-calibrators.json", CALIBRATOR_PAYLOAD)

    version = brs.backfill_one_running_style_metadata(
        client, version_dir / "metadata.json", calibrator_dir
    )
    assert version.name == "jra-running-style"
    assert version.source is not None
    assert version.source.startswith("file://")
    assert version.tags["model_version"] == "jra-running-style-lgbm-prod-v3"
    assert version.tags["category"] == "jra"
    assert version.tags["feature_schema_version"] == "v2"
    assert version.tags["class_weight_scheme"] == "balanced"
    assert version.tags["r2_serving_key"] == "running-style/models/jra/latest.flatbin"
    assert version.tags["r2_key_mutable"] == "true"
    assert version.tags["calibrator"] == "isotonic-ovr"
    assert version.tags["calibrator_fit_year"] == "2025"
    assert version.tags["train_window"] == "2005-01-01-2025-12-31"
    assert version.run_id is not None

    run = client.get_run(version.run_id)
    assert run.data.params["hyperparameters.num_leaves"] == "63"
    assert run.data.metrics["production_precision_nige"] == 0.552
    wf_history = client.get_metric_history(version.run_id, "wf_accuracy")
    steps = {m.step: m.value for m in wf_history}
    assert steps == {2024: 0.483, 2025: 0.49}
    artifact_paths = {a.path for a in client.list_artifacts(version.run_id)}
    assert "jra-rs-v3-calibrators.json" in artifact_paths


def test_backfill_one_running_style_metadata_without_local_model_file_uses_r2_source(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata_path = tmp_path / "jra-running-style-lgbm-prod-v3" / "metadata.json"
    write_json(metadata_path, RS_METADATA)
    version = brs.backfill_one_running_style_metadata(
        client, metadata_path, tmp_path / "no-calibrators"
    )
    assert (
        version.source
        == "s3://pc-keiba-finish-position-models/running-style/models/jra/latest.flatbin"
    )
    assert "calibrator" not in version.tags


def test_backfill_one_running_style_metadata_raises_when_category_unresolvable(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata = dict(RS_METADATA)
    metadata["model_version"] = "unknown-category-version"
    metadata_path = tmp_path / "unknown-category-version" / "metadata.json"
    write_json(metadata_path, metadata)
    with pytest.raises(ValueError, match="cannot infer jra/nar category"):
        brs.backfill_one_running_style_metadata(client, metadata_path, tmp_path)


def test_register_production_pointer_if_missing_creates_version(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    write_json(tmp_path / "jra-rs-v3-calibrators.json", CALIBRATOR_PAYLOAD)
    version = brs.register_production_pointer_if_missing(client, "jra", tmp_path)
    assert version is not None
    assert version.tags["model_version"] == "jra-running-style-lgbm-prod-v3"
    assert (
        version.source
        == "s3://pc-keiba-finish-position-models/running-style/models/jra/latest.flatbin"
    )
    assert version.tags["calibrator"] == "isotonic-ovr"


def test_register_production_pointer_if_missing_is_noop_when_already_registered(
    client: MlflowClient, tmp_path: Path
) -> None:
    first = brs.register_production_pointer_if_missing(client, "nar", tmp_path)
    assert first is not None
    second = brs.register_production_pointer_if_missing(client, "nar", tmp_path)
    assert second is None


def test_backfill_running_style_full_happy_path_with_local_production_mirror(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    artifact_root = tmp_path / "models"
    write_json(artifact_root / "jra-running-style-lgbm-prod-v3" / "metadata.json", RS_METADATA)
    nar_metadata = dict(RS_METADATA)
    nar_metadata["model_version"] = "nar-running-style-lgbm-prod-v3"
    write_json(artifact_root / "nar-running-style-lgbm-prod-v3" / "metadata.json", nar_metadata)

    summary = brs.backfill_running_style(client, artifact_root, tmp_path / "no-calibrators")
    assert summary["versions_registered"] == 2
    assert summary["errors"] == []
    assert summary["champion_sync"] == {"jra": "ok", "nar": "ok"}


def test_backfill_running_style_adds_production_pointer_when_local_mirror_is_a_different_version(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    artifact_root = tmp_path / "models"
    trial_metadata = dict(RS_METADATA)
    trial_metadata["model_version"] = "jra-running-style-lgbm-v4-trial"
    write_json(artifact_root / "jra-running-style-lgbm-v4-trial" / "metadata.json", trial_metadata)

    summary = brs.backfill_running_style(client, artifact_root, tmp_path / "no-calibrators")
    # 1 trial version from the scan + 2 production-pointer fallbacks (jra, nar)
    assert summary["versions_registered"] == 3
    assert summary["champion_sync"] == {"jra": "ok", "nar": "ok"}
    versions = client.search_model_versions("name='jra-running-style'")
    labels = {v.tags.get("model_version") for v in versions}
    assert labels == {"jra-running-style-lgbm-v4-trial", "jra-running-style-lgbm-prod-v3"}


def test_backfill_running_style_ignores_malformed_metadata_at_discovery(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    artifact_root = tmp_path / "models"
    write_json(artifact_root / "jra-running-style-lgbm-prod-v3" / "metadata.json", RS_METADATA)
    bad_path = artifact_root / "nar-running-style-lgbm-bad" / "metadata.json"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_text('{"class_labels": ["a"], "feature_columns": [}', encoding="utf-8")

    summary = brs.backfill_running_style(client, artifact_root, tmp_path / "no-calibrators")
    # Malformed JSON is filtered out at discovery time, not surfaced as an error.
    assert summary["errors"] == []
    assert summary["versions_registered"] >= 1


def test_backfill_running_style_records_error_when_category_unresolvable(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    artifact_root = tmp_path / "models"
    metadata = dict(RS_METADATA)
    metadata["model_version"] = "unknown-category-version"
    write_json(artifact_root / "unknown-category-version" / "metadata.json", metadata)

    summary = brs.backfill_running_style(client, artifact_root, tmp_path / "no-calibrators")
    assert any("unknown-category-version" in error for error in summary["errors"])


def test_backfill_running_style_champion_sync_skips_when_pointer_registration_fails(
    client: MlflowClient, tmp_path: Path
) -> None:
    calibrator_dir = tmp_path / "calibrators"
    calibrator_dir.mkdir()
    # Malformed calibrator JSON makes register_production_pointer_if_missing
    # raise while attaching it to jra, before the jra pointer version is
    # registered -- nar has no calibrator file at all and should still succeed.
    (calibrator_dir / "jra-rs-v3-calibrators.json").write_text("{not valid json", encoding="utf-8")

    summary = brs.backfill_running_style(client, tmp_path / "empty-models", calibrator_dir)
    assert any("production pointer (jra)" in error for error in summary["errors"])
    assert summary["champion_sync"]["jra"].startswith("skipped:")
    assert summary["champion_sync"]["nar"] == "ok"


def test_backfill_one_running_style_metadata_routes_long_field_to_artifact(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata = dict(RS_METADATA)
    metadata["feature_columns"] = [f"feature_{i:04d}" for i in range(600)]
    metadata_path = tmp_path / "jra-running-style-lgbm-prod-v3" / "metadata.json"
    write_json(metadata_path, metadata)
    version = brs.backfill_one_running_style_metadata(client, metadata_path, tmp_path / "no-cal")
    assert version.run_id is not None
    artifact_paths = {a.path for a in client.list_artifacts(version.run_id)}
    assert "feature_columns.json" in artifact_paths


def test_backfill_one_running_style_metadata_calibrator_without_fit_year_still_attaches(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata_path = tmp_path / "jra-running-style-lgbm-prod-v3" / "metadata.json"
    write_json(metadata_path, RS_METADATA)
    calibrator_dir = tmp_path / "calibrators"
    write_json(calibrator_dir / "jra-rs-v3-calibrators.json", {"classes": []})
    version = brs.backfill_one_running_style_metadata(client, metadata_path, calibrator_dir)
    assert "calibrator" not in version.tags
    assert version.run_id is not None
    artifact_paths = {a.path for a in client.list_artifacts(version.run_id)}
    assert "jra-rs-v3-calibrators.json" in artifact_paths


def test_backfill_one_running_style_metadata_calibrator_non_dict_payload_still_attaches(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata_path = tmp_path / "jra-running-style-lgbm-prod-v3" / "metadata.json"
    write_json(metadata_path, RS_METADATA)
    calibrator_dir = tmp_path / "calibrators"
    write_json(calibrator_dir / "jra-rs-v3-calibrators.json", ["not", "a", "dict"])
    version = brs.backfill_one_running_style_metadata(client, metadata_path, calibrator_dir)
    assert "calibrator" not in version.tags
    assert version.run_id is not None
    artifact_paths = {a.path for a in client.list_artifacts(version.run_id)}
    assert "jra-rs-v3-calibrators.json" in artifact_paths


def test_backfill_one_running_style_metadata_handles_malformed_walk_forward_entries(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata = dict(RS_METADATA)
    metadata["walk_forward_results"] = {
        "not-a-year": {"accuracy": 0.5},
        "2024": "not-a-dict",
        "2025": {"accuracy": "not-a-number"},
    }
    metadata_path = tmp_path / "jra-running-style-lgbm-prod-v3" / "metadata.json"
    write_json(metadata_path, metadata)
    version = brs.backfill_one_running_style_metadata(client, metadata_path, tmp_path / "no-cal")
    assert version.run_id is not None
    history = client.get_metric_history(version.run_id, "wf_accuracy")
    assert len(history) == 1
    assert history[0].step == 0
    assert history[0].value == 0.5


def test_backfill_one_running_style_metadata_with_no_numeric_walk_forward_metrics(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    metadata = dict(RS_METADATA)
    metadata["walk_forward_results"] = {"2024": {"train_start_date": "2020-01-01"}}
    metadata_path = tmp_path / "jra-running-style-lgbm-prod-v3" / "metadata.json"
    write_json(metadata_path, metadata)
    version = brs.backfill_one_running_style_metadata(client, metadata_path, tmp_path / "no-cal")
    assert version.run_id is not None
    assert client.get_metric_history(version.run_id, "wf_accuracy") == []
