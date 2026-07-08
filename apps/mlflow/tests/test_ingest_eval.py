"""Tests for mlflow_tracking.ingest_eval."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from mlflow import MlflowClient

from mlflow_tracking import ingest_eval, logging_api

WriteJsonFixture = Callable[[Path, object], None]

TRIALS_COLUMNS = (
    "trial_id",
    "category",
    "class_code",
    "subgroup_dimension",
    "subgroup_value",
    "season_band",
    "feature_set_hash",
    "feature_names",
    "feature_count",
    "rank1_accuracy",
    "rank2_accuracy",
    "rank3_accuracy",
    "rank4_accuracy",
    "rank5_accuracy",
    "rank6_accuracy",
    "top1_accuracy",
    "place2_accuracy",
    "place3_accuracy",
    "ndcg_at_3",
    "race_count",
    "rank1_lb95",
    "rank2_lb95",
    "rank3_lb95",
    "rank4_lb95",
    "rank5_lb95",
    "rank6_lb95",
    "verdict",
    "verdict_reason",
    "model_version",
    "train_window_start",
    "train_window_end",
    "blind_year",
)


_DOUBLE_COLUMNS = frozenset(
    {
        "rank1_accuracy",
        "rank2_accuracy",
        "rank3_accuracy",
        "rank4_accuracy",
        "rank5_accuracy",
        "rank6_accuracy",
        "top1_accuracy",
        "place2_accuracy",
        "place3_accuracy",
        "ndcg_at_3",
        "rank1_lb95",
        "rank2_lb95",
        "rank3_lb95",
        "rank4_lb95",
        "rank5_lb95",
        "rank6_lb95",
    }
)
_INTEGER_COLUMNS = frozenset({"feature_count", "race_count", "blind_year"})


def _column_type(name: str) -> str:
    if name in _DOUBLE_COLUMNS:
        return "DOUBLE"
    if name in _INTEGER_COLUMNS:
        return "INTEGER"
    return "VARCHAR"


def _build_trial_registry_duckdb(path: Path, rows: list[dict[str, object]]) -> None:
    con = duckdb.connect(str(path))
    try:
        columns_sql = ", ".join(f"{name} {_column_type(name)}" for name in TRIALS_COLUMNS)
        con.execute(f"CREATE TABLE trials ({columns_sql})")
        for row in rows:
            values = [row.get(name) for name in TRIALS_COLUMNS]
            placeholders = ", ".join("?" for _ in TRIALS_COLUMNS)
            con.execute(f"INSERT INTO trials VALUES ({placeholders})", values)
    finally:
        con.close()


def test_ingest_trial_registry_groups_rows_by_model_version(
    client: MlflowClient, tmp_path: Path
) -> None:
    duckdb_path = tmp_path / "trial_registry_jra.duckdb"
    _build_trial_registry_duckdb(
        duckdb_path,
        [
            {
                "trial_id": "t1",
                "category": "jra",
                "class_code": "A",
                "subgroup_dimension": "venue",
                "subgroup_value": "05",
                "season_band": "summer",
                "top1_accuracy": 0.4,
                "verdict": "ADOPT",
                "model_version": "v1",
            },
            {
                "trial_id": "t2",
                "category": "jra",
                "class_code": "A",
                "subgroup_dimension": "venue",
                "subgroup_value": "06",
                "season_band": "winter",
                "top1_accuracy": 0.5,
                "verdict": "ADOPT",
                "model_version": "v1",
            },
        ],
    )
    run_ids = ingest_eval.ingest_trial_registry(client, duckdb_path)
    assert len(run_ids) == 1
    run = client.get_run(run_ids[0])
    history = client.get_metric_history(run_ids[0], "top1_accuracy")
    assert {m.value for m in history} == {0.4, 0.5}
    assert run.data.tags["verdict"] == "ADOPT"
    assert run.data.tags["subgroup_value"] == "05;06"


def test_ingest_trial_registry_falls_back_to_trial_id_when_model_version_missing(
    client: MlflowClient, tmp_path: Path
) -> None:
    duckdb_path = tmp_path / "trial_registry_nar.duckdb"
    _build_trial_registry_duckdb(
        duckdb_path,
        [
            {
                "trial_id": "exploratory-1",
                "category": "nar",
                "top1_accuracy": 0.3,
            }
        ],
    )
    run_ids = ingest_eval.ingest_trial_registry(client, duckdb_path)
    assert len(run_ids) == 1
    run = client.get_run(run_ids[0])
    assert run.info.run_name == "trial:exploratory-1"


def test_ingest_serve_accuracy_logs_finish_position_and_running_style(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    payload = {
        "finish_position": {
            "date_str": "20260601",
            "category": "jra",
            "era": "POST_FIX",
            "top1_pct": 44.5,
            "place2_pct": 24.0,
            "place3_pct": 15.0,
            "fukusho_2p_pct": 74.0,
            "top3_box_pct": 30.0,
            "model_version_counts": {"jra-cb-v9-sim-2013-clean": 100},
            "subgroups": [
                {
                    "dimension": "venue",
                    "band": "05",
                    "top1_pct": 40.0,
                    "place2_pct": 20.0,
                    "place3_pct": 10.0,
                    "fukusho_2p_pct": 60.0,
                    "top3_box_pct": 25.0,
                },
                {
                    "dimension": "venue",
                    "band": "06",
                    "top1_pct": "not-a-number",
                },
                "not-a-dict-subgroup",
            ],
        },
        "running_style": {
            "date_str": "20260601",
            "category": "jra",
            "era": "POST_FIX",
            "overall_accuracy_pct": 55.0,
            "macro_f1_pct": 50.0,
            "model_version": "rs-v3",
        },
    }
    json_path = tmp_path / "serve_accuracy.json"
    write_json(json_path, payload)
    run_id = ingest_eval.ingest_serve_accuracy(client, json_path, eval_regime="serve")
    run = client.get_run(run_id)
    assert run.data.tags["date"] == "20260601"
    assert run.data.tags["category"] == "jra"
    assert run.data.tags["era"] == "POST_FIX"
    assert run.data.tags["eval_regime"] == "serve"
    assert run.data.tags["rs_model_version"] == "rs-v3"
    assert run.data.metrics["fp_top1_pct"] == 44.5
    assert run.data.metrics["fp_venue_05_top1_pct"] == 40.0
    assert run.data.metrics["rs_overall_accuracy_pct"] == 55.0
    artifact_paths = {a.path for a in client.list_artifacts(run_id)}
    assert "serve_accuracy.json" in artifact_paths
    experiment = client.get_experiment(run.info.experiment_id)
    assert experiment.name == "finish-position/serve-accuracy"


def test_ingest_serve_accuracy_handles_finish_position_only(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    payload = {
        "finish_position": {
            "date_str": "20260601",
            "category": "nar",
            "era": "DEGRADED",
            "top1_pct": 30.0,
            "model_version_counts": {},
        }
    }
    json_path = tmp_path / "serve_accuracy_fp_only.json"
    write_json(json_path, payload)
    run_id = ingest_eval.ingest_serve_accuracy(client, json_path, eval_regime="serve")
    run = client.get_run(run_id)
    assert "model_version_counts" not in run.data.tags
    assert "rs_model_version" not in run.data.tags


def test_ingest_serve_accuracy_handles_running_style_only(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    payload = {
        "running_style": {
            "date_str": "20260602",
            "category": "jra",
            "era": "POST_FIX",
            "overall_accuracy_pct": 60.0,
        }
    }
    json_path = tmp_path / "serve_accuracy_rs_only.json"
    write_json(json_path, payload)
    run_id = ingest_eval.ingest_serve_accuracy(client, json_path, eval_regime="oos")
    run = client.get_run(run_id)
    assert run.data.tags["date"] == "20260602"
    experiment = client.get_experiment(run.info.experiment_id)
    assert experiment.name == "running-style/eval"


def test_ingest_serve_accuracy_explicit_experiment_overrides_auto_detection(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    payload = {"running_style": {"date_str": "20260603", "category": "nar", "era": "POST_FIX"}}
    json_path = tmp_path / "serve_accuracy_override.json"
    write_json(json_path, payload)
    run_id = ingest_eval.ingest_serve_accuracy(
        client, json_path, eval_regime="unspecified", experiment_name="custom-experiment"
    )
    run = client.get_run(run_id)
    experiment = client.get_experiment(run.info.experiment_id)
    assert experiment.name == "custom-experiment"


def test_ingest_serve_accuracy_rejects_blank_eval_regime(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    json_path = tmp_path / "serve_accuracy_blank_regime.json"
    write_json(json_path, {"finish_position": {"date_str": "20260601", "category": "jra"}})
    with pytest.raises(ValueError, match="eval_regime must not be blank"):
        ingest_eval.ingest_serve_accuracy(client, json_path, eval_regime="   ")


def test_ingest_serve_accuracy_raises_when_neither_section_present(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    json_path = tmp_path / "serve_accuracy_empty.json"
    write_json(json_path, {})
    with pytest.raises(ValueError, match="neither finish_position nor running_style"):
        ingest_eval.ingest_serve_accuracy(client, json_path, eval_regime="serve")


def test_ingest_cell_report_reads_json_records_and_normalizes_columns(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    records = [
        {"keibajo_code": "05", "top1": 0.4, "race_count": 10},
        {"keibajo_code": "06", "top1": 0.6, "race_count": 20},
    ]
    json_path = tmp_path / "cell_report.json"
    write_json(json_path, records)
    run_id = ingest_eval.ingest_cell_report(client, json_path, eval_regime="oos")
    run = client.get_run(run_id)
    assert run.info.run_name == "cell_report"
    assert run.data.tags["source_file"] == "cell_report.json"
    assert run.data.tags["eval_regime"] == "oos"
    assert run.data.metrics["overall_top1"] == (0.4 * 10 + 0.6 * 20) / 30


def test_ingest_cell_report_reads_parquet(client: MlflowClient, tmp_path: Path) -> None:
    df = pd.DataFrame({"venue": ["05", "06"], "top1": [0.4, 0.6], "race_count": [10, 20]})
    parquet_path = tmp_path / "cell_report.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), parquet_path)
    run_id = ingest_eval.ingest_cell_report(
        client, parquet_path, eval_regime="oos", run_name="custom-run"
    )
    run = client.get_run(run_id)
    assert run.info.run_name == "custom-run"


def test_ingest_cell_report_rejects_unsupported_extension(
    client: MlflowClient, tmp_path: Path
) -> None:
    bad_path = tmp_path / "cell_report.csv"
    bad_path.write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unsupported cell-report file extension"):
        ingest_eval.ingest_cell_report(client, bad_path, eval_regime="oos")


def test_ingest_cell_report_passes_through_aggregate_metrics(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    json_path = tmp_path / "cell_report_agg.json"
    write_json(json_path, [{"venue": "05", "top1": 0.4}])
    run_id = ingest_eval.ingest_cell_report(
        client, json_path, eval_regime="oos", aggregate_metrics={"custom": 1.5}
    )
    run = client.get_run(run_id)
    assert run.data.metrics["custom"] == 1.5


def test_ingest_cell_report_rejects_blank_eval_regime(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    json_path = tmp_path / "cell_report_blank_regime.json"
    write_json(json_path, [{"venue": "05", "top1": 0.4}])
    with pytest.raises(ValueError, match="eval_regime must not be blank"):
        ingest_eval.ingest_cell_report(client, json_path, eval_regime="")


def test_ingest_cell_report_alias_collision_drops_alias_column(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    records = [{"venue": "05", "keibajo_code": "99", "top1": 0.4, "race_count": 10}]
    json_path = tmp_path / "cell_report_collision.json"
    write_json(json_path, records)
    run_id = ingest_eval.ingest_cell_report(client, json_path, eval_regime="oos")
    artifact_dir = client.download_artifacts(
        run_id, logging_api.CELL_METRICS_PARQUET_ARTIFACT, str(tmp_path)
    )
    result_df = pd.read_parquet(artifact_dir)
    assert list(result_df["venue"]) == ["05"]
    assert "keibajo_code" not in result_df.columns
