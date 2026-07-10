"""Tests for mlflow_tracking.training_run."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from mlflow import MlflowClient

from mlflow_tracking import config, logging_api, registry, timeline, training_run

WriteJsonFixture = Callable[[Path, object], None]

MINIMAL_MANIFEST: dict[str, object] = {
    "schema": "hr-mlflow-training-run/v1",
    "task": "finish-position",
    "category": "jra",
    "model_version": "jra-cb-test-v1",
    "eval_regime": "wf",
}


def test_log_training_run_minimal_manifest(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, MINIMAL_MANIFEST)
    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    assert run.info.status == "FINISHED"
    assert run.data.tags["eval_regime"] == "wf"
    assert run.data.tags["task"] == "finish-position"
    assert run.data.tags["category"] == "jra"
    assert run.data.tags["model_version"] == "jra-cb-test-v1"
    experiment = client.get_experiment(run.info.experiment_id)
    assert experiment.name == config.EXPERIMENT_FP_WF_EVAL


def test_log_training_run_full_manifest_with_artifact_dir_and_register_champion(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    artifact_dir = tmp_path / "artifacts"
    write_json(
        artifact_dir / "metadata.json",
        {
            "model_version": "jra-cb-test-v2",
            "architecture": "catboost",
            "feature_count": 5,
            "hyperparams": {"iterations": 10},
        },
    )
    manifest = dict(MINIMAL_MANIFEST)
    manifest["model_version"] = "jra-cb-test-v2"
    manifest["artifact_dir"] = str(artifact_dir)
    manifest["aggregate_metrics"] = {"top1": 0.5}
    manifest["params"] = {"custom_param": "x"}
    manifest["tags"] = {"custom_tag": "y"}
    manifest["register"] = True
    manifest["champion"] = True
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)

    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    assert run.data.tags["custom_tag"] == "y"
    assert run.data.params["custom_param"] == "x"
    assert run.data.metrics["top1"] == 0.5
    assert run.data.params["feature_count"] == "5"
    assert run.data.params["hyperparams.iterations"] == "10"
    assert run.data.tags["architecture"] == "catboost"

    versions = client.search_model_versions("name='jra-finish-position'")
    matching = [v for v in versions if v.tags.get("model_version") == "jra-cb-test-v2"]
    assert len(matching) == 1
    assert matching[0].source is not None
    assert matching[0].source.startswith("file://")

    registered = client.get_registered_model("jra-finish-position")
    assert registered.aliases[registry.CHAMPION_ALIAS] == int(matching[0].version)


def test_log_training_run_register_without_artifact_dir_uses_runs_uri(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["model_version"] = "jra-cb-test-v3"
    manifest["register"] = True
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)

    run_id = training_run.log_training_run(client, manifest_path)
    versions = client.search_model_versions("name='jra-finish-position'")
    matching = [v for v in versions if v.tags.get("model_version") == "jra-cb-test-v3"]
    assert len(matching) == 1
    assert matching[0].source == f"runs:/{run_id}"


def test_log_training_run_with_cell_report(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    df = pd.DataFrame({"venue": ["05", "06"], "top1": [0.4, 0.6], "race_count": [10, 20]})
    parquet_path = tmp_path / "cell_report.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), parquet_path)

    manifest = dict(MINIMAL_MANIFEST)
    manifest["cell_report"] = str(parquet_path)
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)

    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    assert run.data.metrics["overall_top1"] == (0.4 * 10 + 0.6 * 20) / 30
    artifact_paths = {a.path for a in client.list_artifacts(run_id)}
    assert logging_api.CELL_METRICS_JSON_ARTIFACT in artifact_paths


def test_log_training_run_running_style_experiment_resolution(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = {
        "schema": "hr-mlflow-training-run/v1",
        "task": "running-style",
        "category": "nar",
        "model_version": "nar-running-style-lgbm-test-v1",
        "eval_regime": "serve",
    }
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    experiment = client.get_experiment(run.info.experiment_id)
    assert experiment.name == config.EXPERIMENT_RS_EVAL


def test_log_training_run_finish_position_serve_experiment_resolution(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["eval_regime"] = "serve"
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    experiment = client.get_experiment(run.info.experiment_id)
    assert experiment.name == config.EXPERIMENT_FP_SERVE_ACCURACY


def test_log_training_run_explicit_experiment_override(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["experiment"] = "custom-experiment"
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    experiment = client.get_experiment(run.info.experiment_id)
    assert experiment.name == "custom-experiment"


def test_log_training_run_routes_to_smoke_tests_experiment_with_run_type_tag(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """The smoke-run convention (see README.md): a manifest routes to
    `config.EXPERIMENT_SMOKE_TESTS` via the SAME generic "experiment"
    override exercised by `test_log_training_run_explicit_experiment_override`
    above, and carries `run_type=smoke` via the manifest's own generic "tags"
    field -- both mechanisms already exist and need no new plumbing, this
    proves the convention actually works end-to-end rather than only being
    documented."""
    manifest = dict(MINIMAL_MANIFEST)
    manifest["experiment"] = config.EXPERIMENT_SMOKE_TESTS
    manifest["tags"] = {"run_type": "smoke"}
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    experiment = client.get_experiment(run.info.experiment_id)
    assert experiment.name == config.EXPERIMENT_SMOKE_TESTS
    assert run.data.tags["run_type"] == "smoke"


def test_log_training_run_rejects_non_dict_manifest(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, ["not", "a", "dict"])
    with pytest.raises(ValueError, match="must be a JSON object"):
        training_run.log_training_run(client, manifest_path)


def test_log_training_run_rejects_wrong_schema(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["schema"] = "some-other-schema/v1"
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="unsupported manifest schema"):
        training_run.log_training_run(client, manifest_path)


def test_log_training_run_rejects_invalid_task(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["task"] = "unknown-task"
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="manifest 'task' must be one of"):
        training_run.log_training_run(client, manifest_path)


def test_log_training_run_rejects_invalid_category(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["category"] = "keirin"
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="unsupported category"):
        training_run.log_training_run(client, manifest_path)


def test_log_training_run_rejects_running_style_banei_combo(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = {
        "schema": "hr-mlflow-training-run/v1",
        "task": "running-style",
        "category": "banei",
        "model_version": "banei-running-style-test",
        "eval_regime": "wf",
    }
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="does not support category=banei"):
        training_run.log_training_run(client, manifest_path)


def test_log_training_run_rejects_missing_model_version(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = {k: v for k, v in MINIMAL_MANIFEST.items() if k != "model_version"}
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="manifest 'model_version' must be"):
        training_run.log_training_run(client, manifest_path)


def test_log_training_run_rejects_blank_eval_regime(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["eval_regime"] = "   "
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="manifest 'eval_regime' must be"):
        training_run.log_training_run(client, manifest_path)


def test_log_training_run_rejects_non_string_experiment(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["experiment"] = 123
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="manifest 'experiment' must be"):
        training_run.log_training_run(client, manifest_path)


def test_log_training_run_rejects_non_dict_params(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["params"] = "not-a-dict"
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="manifest 'params' must be an object"):
        training_run.log_training_run(client, manifest_path)


def test_log_training_run_ignores_non_numeric_aggregate_metric_values(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["aggregate_metrics"] = {"top1": 0.5, "note": "not-a-number"}
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    assert run.data.metrics == {"top1": 0.5}


def test_log_training_run_artifact_dir_without_metadata_json_is_a_noop(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    artifact_dir = tmp_path / "empty-artifacts"
    artifact_dir.mkdir()
    manifest = dict(MINIMAL_MANIFEST)
    manifest["artifact_dir"] = str(artifact_dir)
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    assert run.info.status == "FINISHED"


def test_log_training_run_artifact_dir_with_non_dict_metadata_json_is_a_noop(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    artifact_dir = tmp_path / "artifacts"
    write_json(artifact_dir / "metadata.json", ["not", "a", "dict"])
    manifest = dict(MINIMAL_MANIFEST)
    manifest["artifact_dir"] = str(artifact_dir)
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    assert run.info.status == "FINISHED"
    assert run.data.params == {}


def test_log_training_run_artifact_dir_routes_long_field_to_artifact(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    artifact_dir = tmp_path / "artifacts"
    write_json(
        artifact_dir / "metadata.json",
        {
            "model_version": "jra-cb-test-v4",
            "feature_names": [f"feature_{i:04d}" for i in range(600)],
        },
    )
    manifest = dict(MINIMAL_MANIFEST)
    manifest["model_version"] = "jra-cb-test-v4"
    manifest["artifact_dir"] = str(artifact_dir)
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    run_id = training_run.log_training_run(client, manifest_path)
    artifact_paths = {a.path for a in client.list_artifacts(run_id)}
    assert "feature_names.json" in artifact_paths


def test_log_training_run_serve_regime_with_date_tag_upserts_finish_position_timeline(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["eval_regime"] = "serve"
    manifest["tags"] = {"date": "20260601"}
    manifest["aggregate_metrics"] = {"top1_pct": 44.5, "place2_pct": 24.0}
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)

    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    timeline_run_id = run.data.tags["timeline_run_id:finish-position"]
    timeline_run = client.get_run(timeline_run_id)
    assert timeline_run.info.run_name == "timeline-finish-position-jra-v2"
    assert timeline_run.data.metrics["fp_top1_pct"] == 44.5
    assert timeline_run.data.metrics["fp_place2_pct"] == 24.0
    fp_dates = timeline.timeline_dates_present(client, "finish-position", "jra", "fp_top1_pct")
    assert fp_dates == {timeline.step_for_date("20260601")}


def test_log_training_run_serve_regime_with_date_tag_upserts_running_style_timeline(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = {
        "schema": "hr-mlflow-training-run/v1",
        "task": "running-style",
        "category": "nar",
        "model_version": "nar-running-style-lgbm-test-v1",
        "eval_regime": "serve",
        "tags": {"date": "20260602"},
        "aggregate_metrics": {"overall_accuracy_pct": 55.0, "macro_f1_pct": 50.0},
    }
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)

    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    timeline_run_id = run.data.tags["timeline_run_id:running-style"]
    timeline_run = client.get_run(timeline_run_id)
    assert timeline_run.info.run_name == "timeline-running-style-nar-v2"
    assert timeline_run.data.metrics["rs_overall_accuracy_pct"] == 55.0
    assert timeline_run.data.metrics["rs_macro_f1_pct"] == 50.0


def test_log_training_run_non_serve_regime_does_not_upsert_timeline_even_with_date_tag(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["eval_regime"] = "wf"
    manifest["tags"] = {"date": "20260601"}
    manifest["aggregate_metrics"] = {"top1_pct": 44.5}
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)

    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    assert "timeline_run_id:finish-position" not in run.data.tags
    fp_dates = timeline.timeline_dates_present(client, "finish-position", "jra", "fp_top1_pct")
    assert fp_dates == set()


def test_log_training_run_serve_regime_without_date_tag_does_not_upsert_timeline(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["eval_regime"] = "serve"
    manifest["aggregate_metrics"] = {"top1_pct": 44.5}
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)

    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    assert "timeline_run_id:finish-position" not in run.data.tags


def test_log_training_run_serve_regime_with_empty_timeline_extraction_does_not_upsert(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    manifest = dict(MINIMAL_MANIFEST)
    manifest["eval_regime"] = "serve"
    manifest["tags"] = {"date": "20260601"}
    manifest["aggregate_metrics"] = {"unrelated_metric": 1.0}
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)

    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    assert "timeline_run_id:finish-position" not in run.data.tags


def test_log_training_run_cell_report_with_no_headline_metrics_still_logs_table(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    df = pd.DataFrame({"notes": ["a", "b"]})
    parquet_path = tmp_path / "cell_report.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), parquet_path)
    manifest = dict(MINIMAL_MANIFEST)
    manifest["cell_report"] = str(parquet_path)
    manifest_path = tmp_path / "manifest.json"
    write_json(manifest_path, manifest)
    run_id = training_run.log_training_run(client, manifest_path)
    run = client.get_run(run_id)
    assert run.data.metrics == {}
    artifact_paths = {a.path for a in client.list_artifacts(run_id)}
    assert logging_api.CELL_METRICS_JSON_ARTIFACT in artifact_paths
