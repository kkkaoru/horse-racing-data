from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import insert_running_style_bucket_evaluation_row as subject


def test_parse_args_requires_running_style_feature_version():
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--pg-url",
                "postgres://test",
                "--metrics-json",
                "/tmp/m.json",
                "--model-version",
                "v3",
                "--category",
                "jra",
                "--window-from",
                "20240101",
                "--window-to",
                "20251231",
            ]
        )


def test_parse_args_returns_all_specified_arguments():
    namespace = subject.parse_args(
        [
            "--pg-url",
            "postgres://test",
            "--metrics-json",
            "/tmp/m.json",
            "--model-version",
            "jra-running-style-ens-lgbm-trans-v1.3",
            "--running-style-feature-version",
            "v1",
            "--category",
            "jra",
            "--window-from",
            "20240101",
            "--window-to",
            "20251231",
        ]
    )
    assert namespace.pg_url == "postgres://test"
    assert namespace.metrics_json == Path("/tmp/m.json")
    assert namespace.model_version == "jra-running-style-ens-lgbm-trans-v1.3"
    assert namespace.running_style_feature_version == "v1"
    assert namespace.category == "jra"
    assert namespace.window_from == "20240101"
    assert namespace.window_to == "20251231"


def test_parse_args_requires_metrics_json():
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--pg-url",
                "postgres://test",
                "--model-version",
                "v3",
                "--running-style-feature-version",
                "v1",
                "--category",
                "jra",
                "--window-from",
                "20240101",
                "--window-to",
                "20251231",
            ]
        )


def test_parse_metrics_payload_accepts_a_list():
    rows = subject.parse_metrics_payload(json.dumps([{"source": "jra"}, "skip-this"]))
    assert rows == [{"source": "jra"}]


def test_parse_metrics_payload_accepts_an_object_with_rows():
    payload = {"rows": [{"source": "nar"}, 42]}
    rows = subject.parse_metrics_payload(json.dumps(payload))
    assert rows == [{"source": "nar"}]


def test_parse_metrics_payload_rejects_object_without_rows():
    with pytest.raises(ValueError):
        subject.parse_metrics_payload(json.dumps({"items": []}))


def test_parse_metrics_payload_rejects_scalar():
    with pytest.raises(ValueError):
        subject.parse_metrics_payload(json.dumps("oops"))


def test_normalize_row_coerces_string_kyori_to_int():
    normalized = subject.normalize_row(
        {
            "source": "jra",
            "keibajo_code": "05",
            "kyori": "2400",
            "kyoso_shubetsu_code": "12",
            "race_count": "10",
            "prediction_count": "160",
            "cm_actual_nige_pred_nige_count": "3",
            "log_loss_nige_sum": "1.5",
            "top2_hit_count": "8",
        }
    )
    assert normalized["kyori"] == 2400
    assert normalized["race_count"] == 10
    assert normalized["prediction_count"] == 160
    assert normalized["cm_actual_nige_pred_nige_count"] == 3
    assert normalized["log_loss_nige_sum"] == 1.5
    assert normalized["top2_hit_count"] == 8


def test_normalize_row_defaults_nullable_dims_to_none():
    normalized = subject.normalize_row({"source": "jra"})
    assert normalized["kyoso_joken_code"] is None
    assert normalized["condition_key"] is None
    assert normalized["track_code"] is None
    assert normalized["grade_code"] is None
    assert normalized["race_name"] is None


def test_normalize_row_keeps_optional_strings():
    normalized = subject.normalize_row(
        {
            "source": "nar",
            "kyoso_joken_code": "703",
            "condition_key": "A2",
            "track_code": "10",
            "grade_code": "A",
            "race_name": "有馬記念",
        }
    )
    assert normalized["kyoso_joken_code"] == "703"
    assert normalized["condition_key"] == "A2"
    assert normalized["track_code"] == "10"
    assert normalized["grade_code"] == "A"
    assert normalized["race_name"] == "有馬記念"


def test_normalize_row_coerces_non_string_to_str_for_optional_dim():
    normalized = subject.normalize_row({"source": "jra", "grade_code": 123})
    assert normalized["grade_code"] == "123"


def test_normalize_row_defaults_cm_counts_to_zero():
    normalized = subject.normalize_row({"source": "jra"})
    assert normalized["cm_actual_nige_pred_nige_count"] == 0
    assert normalized["cm_actual_oikomi_pred_oikomi_count"] == 0
    assert normalized["log_loss_nige_sum"] == 0.0
    assert normalized["log_loss_oikomi_count"] == 0
    assert normalized["top2_hit_count"] == 0


def test_to_int_accepts_bool():
    assert subject.to_int(True) == 1


def test_to_int_accepts_float():
    assert subject.to_int(3.7) == 3


def test_to_int_handles_empty_string():
    assert subject.to_int("") == 0


def test_to_int_handles_none():
    assert subject.to_int(None) == 0


def test_to_float_accepts_int():
    assert subject.to_float(5) == 5.0


def test_to_float_accepts_bool():
    assert subject.to_float(False) == 0.0


def test_to_float_handles_none():
    assert subject.to_float(None) == 0.0


def test_to_float_handles_empty_string():
    assert subject.to_float("") == 0.0


def test_to_optional_str_returns_none_for_none():
    assert subject.to_optional_str(None) is None


def test_to_optional_str_preserves_string():
    assert subject.to_optional_str("abc") == "abc"


def test_to_optional_str_coerces_non_string():
    assert subject.to_optional_str(42) == "42"


def test_build_upsert_sql_contains_running_style_table_and_on_conflict():
    sql = subject.build_upsert_sql()
    assert "INSERT INTO running_style_model_bucket_evaluations" in sql
    assert "ON CONFLICT" in sql
    assert "DO UPDATE SET" in sql


def test_build_upsert_sql_uses_additive_excluded_plus_existing_for_race_count():
    sql = subject.build_upsert_sql()
    assert (
        "race_count = excluded.race_count + running_style_model_bucket_evaluations.race_count"
        in sql
    )


def test_build_upsert_sql_includes_all_16_cm_columns_additive():
    sql = subject.build_upsert_sql()
    assert (
        "cm_actual_nige_pred_nige_count = excluded.cm_actual_nige_pred_nige_count + "
        "running_style_model_bucket_evaluations.cm_actual_nige_pred_nige_count"
    ) in sql
    assert (
        "cm_actual_oikomi_pred_oikomi_count = excluded.cm_actual_oikomi_pred_oikomi_count + "
        "running_style_model_bucket_evaluations.cm_actual_oikomi_pred_oikomi_count"
    ) in sql
    assert (
        "cm_actual_senkou_pred_sashi_count = excluded.cm_actual_senkou_pred_sashi_count + "
        "running_style_model_bucket_evaluations.cm_actual_senkou_pred_sashi_count"
    ) in sql


def test_build_upsert_sql_includes_8_log_loss_columns_additive():
    sql = subject.build_upsert_sql()
    assert (
        "log_loss_nige_sum = excluded.log_loss_nige_sum + "
        "running_style_model_bucket_evaluations.log_loss_nige_sum"
    ) in sql
    assert (
        "log_loss_oikomi_count = excluded.log_loss_oikomi_count + "
        "running_style_model_bucket_evaluations.log_loss_oikomi_count"
    ) in sql


def test_build_upsert_sql_includes_top2_hit_count_additive():
    sql = subject.build_upsert_sql()
    assert (
        "top2_hit_count = excluded.top2_hit_count + "
        "running_style_model_bucket_evaluations.top2_hit_count"
    ) in sql


def test_build_upsert_sql_includes_evaluated_at_now():
    sql = subject.build_upsert_sql()
    assert "evaluated_at = now()" in sql


def test_build_upsert_sql_lists_14_conflict_columns_with_coalesce():
    sql = subject.build_upsert_sql()
    assert "coalesce(race_name,'')" in sql
    assert "coalesce(kyoso_joken_code,'')" in sql
    assert "coalesce(condition_key,'')" in sql
    assert "coalesce(track_code,'')" in sql
    assert "coalesce(grade_code,'')" in sql


def test_build_row_template_has_41_placeholders_plus_now():
    template = subject.build_row_template()
    assert template.count("%s") == 41
    assert template.endswith(", now())")


def test_build_metric_columns_lists_16_cm_8_log_loss_top2_in_order():
    expected = [
        "race_count",
        "prediction_count",
        "cm_actual_nige_pred_nige_count",
        "cm_actual_nige_pred_senkou_count",
        "cm_actual_nige_pred_sashi_count",
        "cm_actual_nige_pred_oikomi_count",
        "cm_actual_senkou_pred_nige_count",
        "cm_actual_senkou_pred_senkou_count",
        "cm_actual_senkou_pred_sashi_count",
        "cm_actual_senkou_pred_oikomi_count",
        "cm_actual_sashi_pred_nige_count",
        "cm_actual_sashi_pred_senkou_count",
        "cm_actual_sashi_pred_sashi_count",
        "cm_actual_sashi_pred_oikomi_count",
        "cm_actual_oikomi_pred_nige_count",
        "cm_actual_oikomi_pred_senkou_count",
        "cm_actual_oikomi_pred_sashi_count",
        "cm_actual_oikomi_pred_oikomi_count",
        "log_loss_nige_sum",
        "log_loss_senkou_sum",
        "log_loss_sashi_sum",
        "log_loss_oikomi_sum",
        "log_loss_nige_count",
        "log_loss_senkou_count",
        "log_loss_sashi_count",
        "log_loss_oikomi_count",
        "top2_hit_count",
    ]
    assert subject.build_metric_columns() == expected


def test_build_insert_columns_includes_14_dimensions_first():
    columns = subject.build_insert_columns()
    assert columns[0] == "model_version"
    assert columns[1] == "running_style_feature_version"
    assert columns[2] == "category"
    assert columns[13] == "race_name"
    assert len(columns) == 14 + 27


def test_build_row_tuple_maps_into_41_column_order():
    row = subject.normalize_row(
        {
            "source": "jra",
            "keibajo_code": "05",
            "kyori": 2400,
            "kyoso_shubetsu_code": "12",
            "kyoso_joken_code": "703",
            "condition_key": None,
            "track_code": "10",
            "grade_code": None,
            "race_name": None,
            "race_count": 1,
            "prediction_count": 16,
            "cm_actual_nige_pred_nige_count": 2,
            "cm_actual_senkou_pred_senkou_count": 5,
            "cm_actual_sashi_pred_sashi_count": 3,
            "cm_actual_oikomi_pred_oikomi_count": 1,
            "log_loss_nige_sum": 1.5,
            "log_loss_nige_count": 2,
            "log_loss_senkou_sum": 2.5,
            "log_loss_senkou_count": 5,
            "log_loss_sashi_sum": 3.5,
            "log_loss_sashi_count": 4,
            "log_loss_oikomi_sum": 4.5,
            "log_loss_oikomi_count": 1,
            "top2_hit_count": 12,
        }
    )
    tup = subject.build_row_tuple(
        row,
        "jra-vX",
        "v1",
        "jra",
        "20240101",
        "20251231",
    )
    assert tup == (
        "jra-vX",
        "v1",
        "jra",
        "20240101",
        "20251231",
        "jra",
        "05",
        2400,
        "12",
        "703",
        None,
        "10",
        None,
        None,
        1,
        16,
        2,
        0,
        0,
        0,
        0,
        5,
        0,
        0,
        0,
        0,
        3,
        0,
        0,
        0,
        0,
        1,
        1.5,
        2.5,
        3.5,
        4.5,
        2,
        5,
        4,
        1,
        12,
    )


def test_execute_upsert_calls_execute_values_with_template_and_commits():
    cursor_ctx = MagicMock()
    cursor = MagicMock()
    cursor_ctx.__enter__.return_value = cursor
    cursor_ctx.__exit__.return_value = False

    connection_ctx = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor_ctx
    connection_ctx.__enter__.return_value = connection
    connection_ctx.__exit__.return_value = False

    connect_mock = MagicMock(return_value=connection_ctx)
    execute_values_mock = MagicMock()
    rows: list[tuple[object, ...]] = [("active", "rs1")]
    subject.execute_upsert(
        "postgres://test",
        rows,
        connect=connect_mock,
        execute_values_fn=execute_values_mock,
    )
    connect_mock.assert_called_once_with("postgres://test")
    execute_values_mock.assert_called_once()
    sql_arg = execute_values_mock.call_args.args[1]
    assert "running_style_model_bucket_evaluations" in sql_arg
    assert "cm_actual_nige_pred_nige_count" in sql_arg
    assert "log_loss_oikomi_count" in sql_arg
    assert "top2_hit_count" in sql_arg
    assert execute_values_mock.call_args.kwargs["template"] == subject.build_row_template()
    connection.commit.assert_called_once()


def test_main_reads_file_normalizes_rows_and_calls_execute_upsert(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    metrics_path = tmp_path / "metrics.json"
    metrics_path.write_text(
        json.dumps(
            [
                {
                    "source": "jra",
                    "keibajo_code": "05",
                    "kyori": 2400,
                    "kyoso_shubetsu_code": "12",
                    "race_count": 1,
                    "prediction_count": 16,
                    "cm_actual_nige_pred_nige_count": 1,
                    "log_loss_nige_sum": 1.0,
                    "log_loss_nige_count": 1,
                    "top2_hit_count": 14,
                }
            ]
        ),
        encoding="utf-8",
    )
    argv = [
        "--pg-url",
        "postgres://test",
        "--metrics-json",
        str(metrics_path),
        "--model-version",
        "jra-vX",
        "--running-style-feature-version",
        "v1",
        "--category",
        "jra",
        "--window-from",
        "20240101",
        "--window-to",
        "20251231",
    ]
    monkeypatch.setattr("sys.argv", ["insert_running_style_bucket_evaluation_row.py", *argv])
    execute_upsert_mock = MagicMock()
    monkeypatch.setattr(subject, "execute_upsert", execute_upsert_mock)
    subject.main()
    execute_upsert_mock.assert_called_once()
    pg_url_arg, tuples_arg = execute_upsert_mock.call_args.args
    assert pg_url_arg == "postgres://test"
    assert len(tuples_arg) == 1
    assert tuples_arg[0][0] == "jra-vX"
    assert tuples_arg[0][1] == "v1"
    assert tuples_arg[0][2] == "jra"


def test_default_execute_values_uses_executemany_not_psycopg_extras():
    fn = subject._default_execute_values()
    cursor_mock = MagicMock()
    sql = "INSERT INTO t (a, b, evaluated_at) VALUES %s ON CONFLICT (a) DO UPDATE SET b = excluded.b"
    template = "(%s, %s, now())"
    rows = [("v1", 1), ("v2", 2)]
    fn(cursor_mock, sql, rows, template=template)
    cursor_mock.executemany.assert_called_once()
    call_sql, call_rows = cursor_mock.executemany.call_args.args
    assert "VALUES %s" not in call_sql
    assert f"VALUES {template}" in call_sql
    assert call_rows == rows


def test_main_handles_metrics_json_with_empty_rows_field(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    metrics_path = tmp_path / "metrics.json"
    metrics_path.write_text(json.dumps({"rows": []}), encoding="utf-8")
    argv = [
        "--pg-url",
        "postgres://test",
        "--metrics-json",
        str(metrics_path),
        "--model-version",
        "jra-vX",
        "--running-style-feature-version",
        "v1",
        "--category",
        "jra",
        "--window-from",
        "20240101",
        "--window-to",
        "20251231",
    ]
    monkeypatch.setattr("sys.argv", ["insert_running_style_bucket_evaluation_row.py", *argv])
    execute_upsert_mock = MagicMock()
    monkeypatch.setattr(subject, "execute_upsert", execute_upsert_mock)
    subject.main()
    execute_upsert_mock.assert_called_once_with("postgres://test", [])
