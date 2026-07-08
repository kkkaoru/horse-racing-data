"""Tests for mlflow_tracking.logging_api."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
from mlflow import MlflowClient
from mlflow.entities import Metric, Param, RunTag

from mlflow_tracking import logging_api


def test_get_or_create_experiment_creates_when_absent(client: MlflowClient) -> None:
    experiment_id = logging_api.get_or_create_experiment(client, "new-experiment")
    experiment = client.get_experiment(experiment_id)
    assert experiment.name == "new-experiment"


def test_get_or_create_experiment_reuses_existing(client: MlflowClient) -> None:
    first_id = logging_api.get_or_create_experiment(client, "reused-experiment")
    second_id = logging_api.get_or_create_experiment(client, "reused-experiment")
    assert first_id == second_id


def test_get_or_create_experiment_uses_explicit_artifact_location(
    client: MlflowClient, tmp_path: Path
) -> None:
    location = f"file://{tmp_path}/custom-artifacts"
    experiment_id = logging_api.get_or_create_experiment(
        client, "custom-location-experiment", artifact_location=location
    )
    experiment = client.get_experiment(experiment_id)
    assert experiment.artifact_location == location


def test_log_batch_chunked_splits_metrics_across_chunk_boundary(client: MlflowClient) -> None:
    experiment_id = logging_api.get_or_create_experiment(client, "chunk-metrics")
    run = client.create_run(experiment_id)
    metrics = [Metric(f"m{i}", float(i), 0, 0) for i in range(5)]
    logging_api.log_batch_chunked(client, run.info.run_id, metrics=metrics, chunk_size=2)
    logged = client.get_run(run.info.run_id).data.metrics
    for i in range(5):
        assert logged[f"m{i}"] == float(i)


def test_log_batch_chunked_splits_params_and_tags(client: MlflowClient) -> None:
    experiment_id = logging_api.get_or_create_experiment(client, "chunk-params-tags")
    run = client.create_run(experiment_id)
    params = [Param(f"p{i}", str(i)) for i in range(3)]
    tags = [RunTag(f"t{i}", str(i)) for i in range(3)]
    logging_api.log_batch_chunked(client, run.info.run_id, params=params, tags=tags, chunk_size=1)
    data = client.get_run(run.info.run_id).data
    for i in range(3):
        assert data.params[f"p{i}"] == str(i)
        assert data.tags[f"t{i}"] == str(i)


def test_log_batch_chunked_no_op_for_empty_sequences(client: MlflowClient) -> None:
    experiment_id = logging_api.get_or_create_experiment(client, "chunk-empty")
    run = client.create_run(experiment_id)
    logging_api.log_batch_chunked(client, run.info.run_id)
    data = client.get_run(run.info.run_id).data
    assert data.metrics == {}
    assert data.params == {}


def test_log_json_artifact_writes_readable_json(client: MlflowClient, tmp_path: Path) -> None:
    experiment_id = logging_api.get_or_create_experiment(client, "json-artifact")
    run = client.create_run(experiment_id)
    logging_api.log_json_artifact(client, run.info.run_id, "payload.json", {"a": 1})
    downloaded_path = client.download_artifacts(run.info.run_id, "payload.json", str(tmp_path))
    content = json.loads(Path(downloaded_path).read_text(encoding="utf-8"))
    assert content == {"a": 1}


def test_log_cell_table_logs_both_json_and_parquet(client: MlflowClient) -> None:
    experiment_id = logging_api.get_or_create_experiment(client, "cell-table")
    run = client.create_run(experiment_id)
    df = pd.DataFrame({"category": ["jra", "nar"], "top1": [0.4, 0.5]})
    logging_api.log_cell_table(client, run.info.run_id, df)
    artifact_paths = {a.path for a in client.list_artifacts(run.info.run_id)}
    assert logging_api.CELL_METRICS_JSON_ARTIFACT in artifact_paths
    assert logging_api.CELL_METRICS_PARQUET_ARTIFACT in artifact_paths


def test_select_headline_metrics_computes_weighted_overall_mean() -> None:
    df = pd.DataFrame({"top1": [0.4, 0.6], "race_count": [10, 30]})
    result = logging_api.select_headline_metrics(df)
    assert result["overall_top1"] == (0.4 * 10 + 0.6 * 30) / 40


def test_select_headline_metrics_uses_unweighted_mean_when_no_weight_column() -> None:
    df = pd.DataFrame({"top1": [0.4, 0.6]})
    result = logging_api.select_headline_metrics(df)
    assert result["overall_top1"] == 0.5


def test_select_headline_metrics_picks_up_any_numeric_column() -> None:
    """Metric-set agnostic: an arbitrary numeric column (e.g. a running-style
    metric name unrelated to finish-position) is summarized without needing
    to be on any hardcoded metric name list."""
    df = pd.DataFrame({"macro_f1": [0.4, 0.6]})
    result = logging_api.select_headline_metrics(df)
    assert result == {"overall_macro_f1": 0.5}


def test_select_headline_metrics_skips_non_numeric_columns() -> None:
    df = pd.DataFrame({"notes": ["a", "b"]})
    result = logging_api.select_headline_metrics(df)
    assert result == {}


def test_select_headline_metrics_excludes_cell_key_columns_even_if_numeric() -> None:
    """A column sharing a name with a CELL dimensional key is never treated
    as a metric, even if it happens to hold numeric values."""
    df = pd.DataFrame({"class_code": [1, 2], "top1": [0.4, 0.6]})
    result = logging_api.select_headline_metrics(df)
    assert "overall_class_code" not in result
    assert result["overall_top1"] == 0.5


def test_select_headline_metrics_returns_no_overall_when_total_weight_zero() -> None:
    df = pd.DataFrame({"top1": [0.4, 0.6], "race_count": [0, 0]})
    result = logging_api.select_headline_metrics(df)
    assert "overall_top1" not in result


def test_select_headline_metrics_skips_category_breakdown_for_single_category() -> None:
    df = pd.DataFrame({"category": ["jra", "jra"], "top1": [0.4, 0.6], "race_count": [10, 10]})
    result = logging_api.select_headline_metrics(df)
    assert "jra_top1" not in result
    assert "overall_top1" in result


def test_select_headline_metrics_breaks_down_by_category_when_multiple_present() -> None:
    df = pd.DataFrame(
        {
            "category": ["jra", "nar"],
            "top1": [0.4, 0.6],
            "race_count": [10, 20],
        }
    )
    result = logging_api.select_headline_metrics(df)
    assert result["jra_top1"] == 0.4
    assert result["nar_top1"] == 0.6


def test_select_headline_metrics_skips_category_with_zero_weight() -> None:
    df = pd.DataFrame(
        {
            "category": ["jra", "nar"],
            "top1": [0.4, 0.6],
            "race_count": [0, 20],
        }
    )
    result = logging_api.select_headline_metrics(df)
    assert "jra_top1" not in result
    assert result["nar_top1"] == 0.6


def test_log_eval_run_metric_timestamp_prefers_end_time(client: MlflowClient) -> None:
    run_id = logging_api.log_eval_run(
        client, "metric-ts-end", metrics={"m": 1.0}, start_time=100, end_time=200
    )
    history = client.get_metric_history(run_id, "m")
    assert history[0].timestamp == 200


def test_log_eval_run_metric_timestamp_falls_back_to_start_time(client: MlflowClient) -> None:
    run_id = logging_api.log_eval_run(client, "metric-ts-start", metrics={"m": 1.0}, start_time=100)
    history = client.get_metric_history(run_id, "m")
    assert history[0].timestamp == 100


def test_log_eval_run_metric_timestamp_falls_back_to_now_when_both_unset(
    client: MlflowClient,
) -> None:
    before = int(time.time() * 1000)
    run_id = logging_api.log_eval_run(client, "metric-ts-now", metrics={"m": 1.0})
    after = int(time.time() * 1000)
    history = client.get_metric_history(run_id, "m")
    assert before <= history[0].timestamp <= after


def test_flatten_dict_flattens_nested_mapping() -> None:
    result = logging_api.flatten_dict({"a": {"b": 1, "c": 2}, "d": 3})
    assert result == {"a.b": "1", "a.c": "2", "d": "3"}


def test_flatten_dict_returns_empty_for_empty_mapping() -> None:
    assert logging_api.flatten_dict({}) == {}


def test_flatten_dict_applies_prefix() -> None:
    result = logging_api.flatten_dict({"x": 1}, prefix="root")
    assert result == {"root.x": "1"}


def test_flatten_dict_respects_custom_separator() -> None:
    result = logging_api.flatten_dict({"a": {"b": 1}}, sep="/")
    assert result == {"a/b": "1"}


def test_route_json_fields_routes_identity_keys_to_tags() -> None:
    params, tags, artifacts = logging_api.route_json_fields(
        {"model_version": "v1"}, identity_tag_keys=frozenset({"model_version"})
    )
    assert tags == {"model_version": "v1"}
    assert params == {}
    assert artifacts == {}


def test_route_json_fields_flattens_configured_dict_keys_into_params() -> None:
    params, tags, artifacts = logging_api.route_json_fields(
        {"hyperparams": {"lr": 0.1}},
        identity_tag_keys=frozenset(),
        flatten_param_keys=frozenset({"hyperparams"}),
    )
    assert params == {"hyperparams.lr": "0.1"}
    assert tags == {}
    assert artifacts == {}


def test_route_json_fields_scalar_becomes_param() -> None:
    params, _tags, _artifacts = logging_api.route_json_fields(
        {"feature_count": 5}, identity_tag_keys=frozenset()
    )
    assert params == {"feature_count": "5"}


def test_route_json_fields_short_list_becomes_param() -> None:
    params, _tags, _artifacts = logging_api.route_json_fields(
        {"train_years": ["2020", "2021"]}, identity_tag_keys=frozenset()
    )
    assert params == {"train_years": '["2020", "2021"]'}


def test_route_json_fields_long_value_routes_to_artifact() -> None:
    long_list = [f"feature_{i:04d}" for i in range(600)]
    _params, _tags, artifacts = logging_api.route_json_fields(
        {"feature_names": long_list}, identity_tag_keys=frozenset()
    )
    assert artifacts == {"feature_names": long_list}


def test_route_json_fields_flatten_key_with_non_dict_value_falls_through_to_param() -> None:
    params, _tags, _artifacts = logging_api.route_json_fields(
        {"hyperparams": "not-a-dict"},
        identity_tag_keys=frozenset(),
        flatten_param_keys=frozenset({"hyperparams"}),
    )
    assert params == {"hyperparams": "not-a-dict"}


def test_log_eval_run_logs_params_tags_metrics_and_terminates_finished(
    client: MlflowClient,
) -> None:
    run_id = logging_api.log_eval_run(
        client,
        "eval-run-experiment",
        params={"lr": 0.1},
        tags={"category": "jra"},
        metrics={"top1": 0.5},
        start_time=1_000,
        end_time=2_000,
    )
    run = client.get_run(run_id)
    assert run.info.status == "FINISHED"
    assert run.info.start_time == 1_000
    assert run.info.end_time == 2_000
    assert run.data.params["lr"] == "0.1"
    assert run.data.tags["category"] == "jra"
    assert run.data.metrics["top1"] == 0.5
    history = client.get_metric_history(run_id, "top1")
    assert history[0].timestamp == 2_000


def test_log_eval_run_with_cell_df_logs_headline_metrics_and_table(client: MlflowClient) -> None:
    df = pd.DataFrame({"category": ["jra", "nar"], "top1": [0.4, 0.6], "race_count": [10, 20]})
    run_id = logging_api.log_eval_run(client, "eval-run-with-cells", cell_df=df)
    run = client.get_run(run_id)
    assert run.data.metrics["overall_top1"] == (0.4 * 10 + 0.6 * 20) / 30
    artifact_paths = {a.path for a in client.list_artifacts(run_id)}
    assert logging_api.CELL_METRICS_JSON_ARTIFACT in artifact_paths


def test_log_eval_run_with_cell_df_that_has_no_headline_metrics_still_logs_table(
    client: MlflowClient,
) -> None:
    df = pd.DataFrame({"notes": ["a", "b"]})
    run_id = logging_api.log_eval_run(client, "eval-run-no-headline", cell_df=df)
    run = client.get_run(run_id)
    assert run.data.metrics == {}
    artifact_paths = {a.path for a in client.list_artifacts(run_id)}
    assert logging_api.CELL_METRICS_JSON_ARTIFACT in artifact_paths


def test_log_eval_run_defaults_to_none_for_optional_arguments(client: MlflowClient) -> None:
    run_id = logging_api.log_eval_run(client, "eval-run-minimal")
    run = client.get_run(run_id)
    assert run.info.status == "FINISHED"
    assert run.data.params == {}
