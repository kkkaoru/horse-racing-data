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

from mlflow_tracking import ingest_eval, logging_api, timeline

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


def test_ingest_trial_registry_skips_run_for_group_with_no_populated_metrics_or_tags(
    client: MlflowClient, tmp_path: Path
) -> None:
    """A trial-group whose records populate none of TRIAL_METRIC_COLUMNS (nor
    TRIAL_TAG_COLUMNS) must not create a run at all -- verified real example:
    an empty wf-eval run named "v1" with nothing logged on it. Only the OTHER,
    genuinely populated group in this same duckdb file should produce a run."""
    duckdb_path = tmp_path / "trial_registry_empty_group.duckdb"
    _build_trial_registry_duckdb(
        duckdb_path,
        [
            {"trial_id": "empty-1"},
            {
                "trial_id": "populated-1",
                "category": "jra",
                "top1_accuracy": 0.4,
                "verdict": "ADOPT",
            },
        ],
    )
    run_ids = ingest_eval.ingest_trial_registry(client, duckdb_path)
    assert None not in run_ids
    assert len(run_ids) == 1
    run = client.get_run(run_ids[0])
    assert run.info.run_name == "trial:populated-1"

    experiment = client.get_experiment_by_name(ingest_eval.TRIAL_REGISTRY_EXPERIMENT)
    assert experiment is not None
    all_runs = client.search_runs([experiment.experiment_id])
    assert len(all_runs) == 1


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

    fp_timeline_run_id = run.data.tags["timeline_run_id:finish-position"]
    rs_timeline_run_id = run.data.tags["timeline_run_id:running-style"]
    assert fp_timeline_run_id != rs_timeline_run_id
    fp_timeline_run = client.get_run(fp_timeline_run_id)
    assert fp_timeline_run.info.run_name == "timeline-finish-position-jra-v2"
    assert fp_timeline_run.data.metrics["fp_top1_pct"] == 44.5
    rs_timeline_run = client.get_run(rs_timeline_run_id)
    assert rs_timeline_run.info.run_name == "timeline-running-style-jra-v2"
    assert rs_timeline_run.data.metrics["rs_overall_accuracy_pct"] == 55.0


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
    assert "timeline_run_id:running-style" not in run.data.tags
    fp_timeline_run_id = run.data.tags["timeline_run_id:finish-position"]
    fp_dates = timeline.timeline_dates_present(client, "finish-position", "nar", "fp_top1_pct")
    assert fp_dates == {timeline.step_for_date("20260601")}
    fp_timeline_run = client.get_run(fp_timeline_run_id)
    assert fp_timeline_run.data.metrics["fp_top1_pct"] == 30.0


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
    assert "timeline_run_id:finish-position" not in run.data.tags
    rs_timeline_run_id = run.data.tags["timeline_run_id:running-style"]
    rs_dates = timeline.timeline_dates_present(
        client, "running-style", "jra", "rs_overall_accuracy_pct"
    )
    assert rs_dates == {timeline.step_for_date("20260602")}
    rs_timeline_run = client.get_run(rs_timeline_run_id)
    assert rs_timeline_run.data.metrics["rs_overall_accuracy_pct"] == 60.0


def test_ingest_serve_accuracy_skips_timeline_upsert_when_fp_extraction_is_empty(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    payload = {
        "finish_position": {"date_str": "20260604", "category": "jra", "era": "POST_FIX"},
    }
    json_path = tmp_path / "serve_accuracy_fp_no_metrics.json"
    write_json(json_path, payload)
    run_id = ingest_eval.ingest_serve_accuracy(client, json_path, eval_regime="serve")
    run = client.get_run(run_id)
    assert "timeline_run_id:finish-position" not in run.data.tags


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


def test_ingest_serve_accuracy_is_idempotent_for_identical_payload(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """A repeated ingest of the IDENTICAL payload (the verified real
    duplicate: two identical win5-overlay runs created 28 seconds apart by a
    repeated call) must reuse the same run, not create a second one."""
    payload = {
        "finish_position": {
            "date_str": "20260601",
            "category": "jra",
            "era": "POST_FIX",
            "top1_pct": 44.5,
            "model_version_counts": {"jra-cb-v9-sim-2013-clean": 100},
        }
    }
    json_path = tmp_path / "serve_accuracy_dup.json"
    write_json(json_path, payload)

    first_run_id = ingest_eval.ingest_serve_accuracy(client, json_path, eval_regime="serve")
    second_run_id = ingest_eval.ingest_serve_accuracy(client, json_path, eval_regime="serve")
    assert first_run_id == second_run_id

    run = client.get_run(first_run_id)
    serve_key = run.data.tags[ingest_eval.SERVE_KEY_TAG]
    matches = client.search_runs(
        [run.info.experiment_id],
        filter_string=f"tags.{ingest_eval.SERVE_KEY_TAG} = '{serve_key}'",
    )
    assert len(matches) == 1


def test_ingest_serve_accuracy_reuses_run_and_updates_in_place_for_same_identity(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """Two payloads sharing the same (model_version identity, date, category)
    but differing in a metric value still reuse the same run
    (update-in-place), and the run ends up reflecting the LATEST call's
    data."""
    first_payload = {
        "finish_position": {
            "date_str": "20260601",
            "category": "jra",
            "era": "POST_FIX",
            "top1_pct": 44.5,
            "model_version_counts": {"jra-cb-v9-sim-2013-clean": 100},
        }
    }
    first_path = tmp_path / "serve_accuracy_v1.json"
    write_json(first_path, first_payload)
    first_run_id = ingest_eval.ingest_serve_accuracy(client, first_path, eval_regime="serve")

    second_payload = {
        "finish_position": {
            **first_payload["finish_position"],
            "top1_pct": 50.0,
        }
    }
    second_path = tmp_path / "serve_accuracy_v2.json"
    write_json(second_path, second_payload)
    second_run_id = ingest_eval.ingest_serve_accuracy(client, second_path, eval_regime="serve")

    assert first_run_id == second_run_id
    run = client.get_run(second_run_id)
    assert run.data.metrics["fp_top1_pct"] == 50.0
    assert run.data.tags["date"] == "20260601"
    assert run.data.tags["category"] == "jra"


def test_ingest_serve_accuracy_serve_key_reflects_model_version_identity_precedence(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """Indirect coverage of `_serve_accuracy_model_version_identity`'s
    precedence/fallback rules through the public `ingest_serve_accuracy`
    entry point: the resulting run's `serve_key` tag is assembled as
    f"{model_version_identity}:{date_str}:{category}" (see
    `ingest_serve_accuracy`'s docstring / `SERVE_KEY_TAG`), so asserting on
    its prefix exercises the same 4 precedence/fallback cases the old
    direct-private-call test covered, without reaching into a private
    helper from outside its module."""
    rs_version_payload = {
        "running_style": {
            "date_str": "20260601",
            "category": "jra",
            "era": "POST_FIX",
            "model_version": "rs-v3",
        }
    }
    rs_version_path = tmp_path / "serve_key_rs_version.json"
    write_json(rs_version_path, rs_version_payload)
    rs_version_run = client.get_run(
        ingest_eval.ingest_serve_accuracy(client, rs_version_path, eval_regime="serve")
    )
    assert rs_version_run.data.tags[ingest_eval.SERVE_KEY_TAG].startswith("rs-v3:")

    fp_counts_payload = {
        "finish_position": {
            "date_str": "20260602",
            "category": "jra",
            "era": "POST_FIX",
            "model_version_counts": {"b-model": 1, "a-model": 2},
        }
    }
    fp_counts_path = tmp_path / "serve_key_fp_counts.json"
    write_json(fp_counts_path, fp_counts_payload)
    fp_counts_run = client.get_run(
        ingest_eval.ingest_serve_accuracy(client, fp_counts_path, eval_regime="serve")
    )
    assert fp_counts_run.data.tags[ingest_eval.SERVE_KEY_TAG].startswith("a-model+b-model:")

    fp_empty_counts_payload = {
        "finish_position": {
            "date_str": "20260603",
            "category": "jra",
            "era": "POST_FIX",
            "model_version_counts": {},
        }
    }
    fp_empty_counts_path = tmp_path / "serve_key_fp_empty_counts.json"
    write_json(fp_empty_counts_path, fp_empty_counts_payload)
    fp_empty_counts_run = client.get_run(
        ingest_eval.ingest_serve_accuracy(client, fp_empty_counts_path, eval_regime="serve")
    )
    assert fp_empty_counts_run.data.tags[ingest_eval.SERVE_KEY_TAG].startswith("unspecified:")

    fp_unpopulated_payload = {"finish_position": {}}
    fp_unpopulated_path = tmp_path / "serve_key_fp_unpopulated.json"
    write_json(fp_unpopulated_path, fp_unpopulated_payload)
    fp_unpopulated_run = client.get_run(
        ingest_eval.ingest_serve_accuracy(client, fp_unpopulated_path, eval_regime="serve")
    )
    assert fp_unpopulated_run.data.tags[ingest_eval.SERVE_KEY_TAG].startswith("unspecified:")


def test_ingest_serve_accuracy_does_not_collide_across_distinct_identities(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """Two categories, two dates, and two model_versions must each produce a
    DISTINCT run -- the `serve_key` must not accidentally collide across any
    of the three identity components."""
    base_rs = {
        "date_str": "20260601",
        "category": "jra",
        "era": "POST_FIX",
        "model_version": "rs-v3",
        "overall_accuracy_pct": 55.0,
    }
    payloads = {
        "same": base_rs,
        "diff_category": {**base_rs, "category": "nar"},
        "diff_date": {**base_rs, "date_str": "20260602"},
        "diff_model_version": {**base_rs, "model_version": "rs-v4"},
    }

    run_ids = []
    for name, rs_section in payloads.items():
        json_path = tmp_path / f"serve_accuracy_{name}.json"
        write_json(json_path, {"running_style": rs_section})
        run_ids.append(ingest_eval.ingest_serve_accuracy(client, json_path, eval_regime="serve"))

    assert len(set(run_ids)) == len(run_ids)


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


# ── --run-key idempotency (optional; default behavior unchanged) ──────────


def test_ingest_cell_report_without_run_key_always_creates_new_run(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """Regression guard for the default (run_key omitted) path: it must
    behave EXACTLY as before this parameter existed -- two calls against the
    identical report (same run_name and all) still create two distinct
    runs, never reusing one."""
    json_path = tmp_path / "cell_report.json"
    write_json(json_path, [{"venue": "05", "top1": 0.4, "race_count": 10}])
    first_run_id = ingest_eval.ingest_cell_report(client, json_path, eval_regime="oos")
    second_run_id = ingest_eval.ingest_cell_report(client, json_path, eval_regime="oos")
    assert first_run_id != second_run_id


def test_ingest_cell_report_with_run_key_reuses_existing_run(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    json_path = tmp_path / "cell_report.json"
    write_json(json_path, [{"venue": "05", "top1": 0.4, "race_count": 10}])
    first_run_id = ingest_eval.ingest_cell_report(
        client, json_path, eval_regime="oos", run_key="daily-jra-oos"
    )
    second_run_id = ingest_eval.ingest_cell_report(
        client, json_path, eval_regime="oos", run_key="daily-jra-oos"
    )
    assert first_run_id == second_run_id

    run = client.get_run(first_run_id)
    assert run.data.tags[ingest_eval.RUN_KEY_TAG] == "daily-jra-oos"
    matches = client.search_runs(
        [run.info.experiment_id],
        filter_string=f"tags.{ingest_eval.RUN_KEY_TAG} = 'daily-jra-oos'",
    )
    assert len(matches) == 1
    assert run.info.status == "FINISHED"


def test_ingest_cell_report_with_run_key_updates_metrics_in_place(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """Two calls sharing the same run_key but differing in aggregate_metrics
    reuse the same run, ending up reflecting the LATEST call's data --
    mirroring `ingest_serve_accuracy`'s own reused-run update-in-place
    semantics."""
    json_path = tmp_path / "cell_report.json"
    write_json(json_path, [{"venue": "05", "top1": 0.4, "race_count": 10}])
    first_run_id = ingest_eval.ingest_cell_report(
        client,
        json_path,
        eval_regime="oos",
        run_key="daily-jra-oos",
        aggregate_metrics={"custom": 1.0},
    )
    second_run_id = ingest_eval.ingest_cell_report(
        client,
        json_path,
        eval_regime="oos",
        run_key="daily-jra-oos",
        aggregate_metrics={"custom": 2.0},
    )
    assert first_run_id == second_run_id
    run = client.get_run(second_run_id)
    assert run.data.metrics["custom"] == 2.0


def test_ingest_cell_report_with_run_key_logs_cell_table(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    """The run_key-reuse path still logs the full cell table artifact, same
    as the default (log_eval_run-backed) path."""
    json_path = tmp_path / "cell_report.json"
    write_json(json_path, [{"venue": "05", "top1": 0.4, "race_count": 10}])
    run_id = ingest_eval.ingest_cell_report(
        client, json_path, eval_regime="oos", run_key="daily-jra-oos"
    )
    artifact_paths = {a.path for a in client.list_artifacts(run_id)}
    assert logging_api.CELL_METRICS_JSON_ARTIFACT in artifact_paths
    assert logging_api.CELL_METRICS_PARQUET_ARTIFACT in artifact_paths


def test_ingest_cell_report_different_run_keys_get_different_runs(
    client: MlflowClient, tmp_path: Path, write_json: WriteJsonFixture
) -> None:
    json_path = tmp_path / "cell_report.json"
    write_json(json_path, [{"venue": "05", "top1": 0.4, "race_count": 10}])
    first_run_id = ingest_eval.ingest_cell_report(
        client, json_path, eval_regime="oos", run_key="key-a"
    )
    second_run_id = ingest_eval.ingest_cell_report(
        client, json_path, eval_regime="oos", run_key="key-b"
    )
    assert first_run_id != second_run_id
