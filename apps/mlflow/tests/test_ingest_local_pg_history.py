"""Tests for mlflow_tracking.ingest_local_pg_history."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from mlflow import MlflowClient

from mlflow_tracking import ingest_local_pg_history as pg_history

MODEL_EVAL_ROW: dict[str, object] = {
    "model_version": "jra-cb-v7-lineage",
    "category": "jra",
    "evaluation_window_from": "20240101",
    "evaluation_window_to": "20251231",
    "race_count": 1000,
    "prediction_count": 12000,
    "top1_accuracy": Decimal("0.32"),
    "top3_box_accuracy": Decimal("0.58"),
    "top3_exact_accuracy": 0.12,
    "place1_accuracy": 0.32,
    "place2_accuracy": 0.21,
    "place3_accuracy": 0.18,
    "top3_winner_capture": 0.9,
    "top5_winner_capture": 0.95,
    "pair_score": 0.4,
    "ndcg_at_3": 0.5,
    "top3_place_relation": 0.6,
    "evaluated_at": datetime(2026, 5, 20, 22, 15, 2, tzinfo=UTC),
    "subgroup_key": None,
    "subgroup_value": None,
}

RS_BUCKET_ROW_BASE: dict[str, object] = {
    "model_version": "jra-running-style-lgbm-prod-v3",
    "running_style_feature_version": "v1",
    "category": "jra",
    "keibajo_code": "05",
    "kyori": 1600,
    "kyoso_shubetsu_code": "13",
    "kyoso_joken_code": "999",
    "condition_key": "オープン",
    "track_code": "10",
    "grade_code": "0",
    "race_name": "",
    "race_count": 100,
    "prediction_count": 1200,
    "cm_actual_nige_pred_nige_count": 40,
    "cm_actual_nige_pred_senkou_count": 10,
    "cm_actual_nige_pred_sashi_count": 0,
    "cm_actual_nige_pred_oikomi_count": 0,
    "cm_actual_senkou_pred_nige_count": 5,
    "cm_actual_senkou_pred_senkou_count": 35,
    "cm_actual_senkou_pred_sashi_count": 5,
    "cm_actual_senkou_pred_oikomi_count": 0,
    "cm_actual_sashi_pred_nige_count": 0,
    "cm_actual_sashi_pred_senkou_count": 5,
    "cm_actual_sashi_pred_sashi_count": 45,
    "cm_actual_sashi_pred_oikomi_count": 5,
    "cm_actual_oikomi_pred_nige_count": 0,
    "cm_actual_oikomi_pred_senkou_count": 0,
    "cm_actual_oikomi_pred_sashi_count": 10,
    "cm_actual_oikomi_pred_oikomi_count": 40,
    "log_loss_nige_sum": Decimal("12.0"),
    "log_loss_nige_count": 50,
    "log_loss_senkou_sum": 0,
    "log_loss_senkou_count": 0,
    "log_loss_sashi_sum": 27.0,
    "log_loss_sashi_count": 55,
    "log_loss_oikomi_sum": 20.0,
    "log_loss_oikomi_count": 50,
    "top2_hit_count": 900,
    "evaluated_at": datetime(2026, 6, 1, 15, 2, 36, tzinfo=UTC),
    "corner1_pair_score_sum": 30.0,
    "corner1_pair_score_count": 100,
    "corner3_pair_score_sum": 0,
    "corner3_pair_score_count": 0,
    "corner4_pair_score_sum": 40.0,
    "corner4_pair_score_count": 100,
    "finish_pair_score_sum": 50.0,
    "finish_pair_score_count": 100,
}


def test_read_table_reads_parquet(tmp_path: Path) -> None:
    path = tmp_path / "rows.parquet"
    pd.DataFrame([{"a": 1}]).to_parquet(path)
    df = pg_history.read_table(path)
    assert df.to_dict("records") == [{"a": 1}]


def test_read_table_reads_json(tmp_path: Path) -> None:
    path = tmp_path / "rows.json"
    path.write_text('[{"a": 1}]', encoding="utf-8")
    df = pg_history.read_table(path)
    assert df.to_dict("records") == [{"a": 1}]


def test_read_table_raises_for_unsupported_extension(tmp_path: Path) -> None:
    path = tmp_path / "rows.csv"
    path.write_text("a\n1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unsupported"):
        pg_history.read_table(path)


def test_ingest_model_prediction_evaluations_logs_one_run_per_row(
    client: MlflowClient, tmp_path: Path
) -> None:
    path = tmp_path / "model_prediction_evaluations.parquet"
    pd.DataFrame([MODEL_EVAL_ROW]).to_parquet(path)
    run_ids = pg_history.ingest_model_prediction_evaluations(client, path)
    assert len(run_ids) == 1
    run = client.get_run(run_ids[0])
    assert run.data.tags["model_version"] == "jra-cb-v7-lineage"
    assert run.data.tags["category"] == "jra"
    assert run.data.tags["eval_regime"] == "wf"
    assert run.data.tags["source"] == "local-pg-bucket-eval"
    assert "subgroup_key" not in run.data.tags
    # top1_accuracy arrives as a Decimal (psycopg2 numeric column) -- must
    # still land as a plain float metric.
    assert run.data.metrics["top1_accuracy"] == 0.32
    assert run.data.metrics["race_count"] == 1000.0
    assert run.info.start_time == int(
        datetime(2026, 5, 20, 22, 15, 2, tzinfo=UTC).timestamp() * 1000
    )


def test_ingest_model_prediction_evaluations_tags_subgroup_when_present(
    client: MlflowClient, tmp_path: Path
) -> None:
    row = dict(MODEL_EVAL_ROW)
    row["subgroup_key"] = "venue"
    row["subgroup_value"] = "05"
    path = tmp_path / "model_prediction_evaluations.parquet"
    pd.DataFrame([row]).to_parquet(path)
    run_ids = pg_history.ingest_model_prediction_evaluations(client, path)
    run = client.get_run(run_ids[0])
    assert run.data.tags["subgroup_key"] == "venue"
    assert run.data.tags["subgroup_value"] == "05"


def test_ingest_model_prediction_evaluations_omits_start_time_when_evaluated_at_missing(
    client: MlflowClient, tmp_path: Path
) -> None:
    row = {k: v for k, v in MODEL_EVAL_ROW.items() if k != "evaluated_at"}
    path = tmp_path / "model_prediction_evaluations.parquet"
    pd.DataFrame([row]).to_parquet(path)
    run_ids = pg_history.ingest_model_prediction_evaluations(client, path)
    run = client.get_run(run_ids[0])
    # No explicit start_time falls through to mlflow's own "now" default --
    # the point of this test is that ingestion succeeds without raising when
    # the source row carries no timestamp, not the exact value mlflow picks.
    assert run.info.start_time > 0


def test_ingest_model_prediction_evaluations_excludes_bool_and_nan_metrics(
    client: MlflowClient, tmp_path: Path
) -> None:
    row = dict(MODEL_EVAL_ROW)
    row["top3_exact_accuracy"] = True  # bool must never be coerced into a 0.0/1.0 metric
    row["place1_accuracy"] = float("nan")
    path = tmp_path / "model_prediction_evaluations.parquet"
    pd.DataFrame([row]).to_parquet(path)
    run_ids = pg_history.ingest_model_prediction_evaluations(client, path)
    run = client.get_run(run_ids[0])
    assert "top3_exact_accuracy" not in run.data.metrics
    assert "place1_accuracy" not in run.data.metrics


def test_ingest_model_prediction_evaluations_rejects_blank_eval_regime(
    client: MlflowClient, tmp_path: Path
) -> None:
    path = tmp_path / "model_prediction_evaluations.parquet"
    pd.DataFrame([MODEL_EVAL_ROW]).to_parquet(path)
    with pytest.raises(ValueError, match="eval_regime"):
        pg_history.ingest_model_prediction_evaluations(client, path, eval_regime=" ")


def test_bucket_row_to_cell_record_includes_dimensions_and_rates() -> None:
    record = pg_history.bucket_row_to_cell_record(RS_BUCKET_ROW_BASE)
    assert record["keibajo_code"] == "05"
    assert record["kyori"] == 1600
    assert record["race_count"] == 100.0
    # accuracy = diagonal(160) / total(200) * 100
    assert record["accuracy_pct"] == pytest.approx(80.0)
    # log_loss_senkou has count=0 and must be skipped, not divide-by-zero.
    assert record["log_loss_nige_avg"] == pytest.approx(12.0 / 50)
    assert record["log_loss_sashi_avg"] == pytest.approx(27.0 / 55)
    assert record["log_loss_oikomi_avg"] == pytest.approx(20.0 / 50)
    assert "log_loss_senkou_avg" not in record
    # corner3 has count=0 and must be skipped.
    assert record["corner1_pair_score_avg"] == pytest.approx(0.3)
    assert record["corner4_pair_score_avg"] == pytest.approx(0.4)
    assert record["finish_pair_score_avg"] == pytest.approx(0.5)
    assert "corner3_pair_score_avg" not in record
    assert record["top2_hit_pct"] == pytest.approx(900 / 1200 * 100.0)


def test_bucket_row_to_cell_record_omits_race_count_when_missing() -> None:
    row = {k: v for k, v in RS_BUCKET_ROW_BASE.items() if k != "race_count"}
    record = pg_history.bucket_row_to_cell_record(row)
    assert "race_count" not in record


def test_bucket_row_to_cell_record_omits_accuracy_pct_when_no_confusion_data() -> None:
    row = {k: v for k, v in RS_BUCKET_ROW_BASE.items() if not k.startswith("cm_actual_")}
    record = pg_history.bucket_row_to_cell_record(row)
    assert "accuracy_pct" not in record


def test_bucket_row_to_cell_record_omits_top2_hit_pct_when_prediction_count_missing() -> None:
    row = dict(RS_BUCKET_ROW_BASE)
    row["prediction_count"] = 0
    record = pg_history.bucket_row_to_cell_record(row)
    assert "top2_hit_pct" not in record


def test_ingest_running_style_bucket_evaluations_groups_by_model_category_feature_version(
    client: MlflowClient, tmp_path: Path
) -> None:
    other_row = dict(RS_BUCKET_ROW_BASE)
    other_row["model_version"] = "nar-running-style-lgbm-prod-v3"
    other_row["category"] = "nar"
    other_row["keibajo_code"] = "44"
    path = tmp_path / "running_style_model_bucket_evaluations.parquet"
    pd.DataFrame([RS_BUCKET_ROW_BASE, other_row]).to_parquet(path)

    run_ids = pg_history.ingest_running_style_bucket_evaluations(client, path)
    assert len(run_ids) == 2
    runs = [client.get_run(run_id) for run_id in run_ids]
    model_versions = {run.data.tags["model_version"] for run in runs}
    assert model_versions == {"jra-running-style-lgbm-prod-v3", "nar-running-style-lgbm-prod-v3"}
    jra_run = next(run for run in runs if run.data.tags["model_version"].startswith("jra"))
    assert jra_run.data.tags["running_style_feature_version"] == "v1"
    assert jra_run.data.tags["eval_regime"] == "wf"
    assert jra_run.data.tags["source"] == "local-pg-bucket-eval"
    assert jra_run.info.start_time == int(
        datetime(2026, 6, 1, 15, 2, 36, tzinfo=UTC).timestamp() * 1000
    )
    artifact_paths = {a.path for a in client.list_artifacts(jra_run.info.run_id)}
    assert "cell_metrics.parquet" in artifact_paths


def test_ingest_running_style_bucket_evaluations_rejects_blank_eval_regime(
    client: MlflowClient, tmp_path: Path
) -> None:
    path = tmp_path / "running_style_model_bucket_evaluations.parquet"
    pd.DataFrame([RS_BUCKET_ROW_BASE]).to_parquet(path)
    with pytest.raises(ValueError, match="eval_regime"):
        pg_history.ingest_running_style_bucket_evaluations(client, path, eval_regime="")
