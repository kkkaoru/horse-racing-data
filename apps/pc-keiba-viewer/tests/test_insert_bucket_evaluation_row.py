from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import insert_bucket_evaluation_row as subject


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
                "--finish-position-version",
                "fp1",
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
            "v3",
            "--running-style-feature-version",
            "rs1",
            "--finish-position-version",
            "fp1",
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
    assert namespace.model_version == "v3"
    assert namespace.running_style_feature_version == "rs1"
    assert namespace.finish_position_version == "fp1"
    assert namespace.category == "jra"
    assert namespace.window_from == "20240101"
    assert namespace.window_to == "20251231"


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
            "pair_score_pair_count": 120,
            "ndcg_at_3_race_count": 9,
            "top1_hit_sum": "1.5",
        }
    )
    assert normalized["kyori"] == 2400
    assert normalized["race_count"] == 10
    assert normalized["prediction_count"] == 160
    assert normalized["top1_hit_sum"] == 1.5


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
            "source": "jra",
            "kyoso_joken_code": "703",
            "condition_key": "未勝利",
            "track_code": "10",
            "grade_code": "A",
            "race_name": "有馬記念",
        }
    )
    assert normalized["kyoso_joken_code"] == "703"
    assert normalized["condition_key"] == "未勝利"
    assert normalized["track_code"] == "10"
    assert normalized["grade_code"] == "A"
    assert normalized["race_name"] == "有馬記念"


def test_normalize_row_coerces_non_string_to_str_for_optional_dim():
    normalized = subject.normalize_row({"source": "jra", "grade_code": 123})
    assert normalized["grade_code"] == "123"


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


def test_build_upsert_sql_contains_on_conflict_do_update():
    sql = subject.build_upsert_sql()
    assert "INSERT INTO model_prediction_bucket_evaluations" in sql
    assert "ON CONFLICT" in sql
    assert "DO UPDATE SET" in sql
    assert "race_count = excluded.race_count" in sql
    assert "evaluated_at = now()" in sql


def test_build_upsert_sql_includes_all_15_conflict_keys():
    sql = subject.build_upsert_sql()
    assert "coalesce(race_name,'')" in sql
    assert "coalesce(kyoso_joken_code,'')" in sql
    assert "coalesce(track_code,'')" in sql


def test_build_row_template_has_31_placeholders():
    template = subject.build_row_template()
    assert template.count("%s") == 30
    assert template.endswith(", now())")


def test_build_row_tuple_maps_into_30_column_order():
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
            "pair_score_pair_count": 120,
            "ndcg_at_3_race_count": 1,
            "top1_hit_sum": 1.0,
            "place1_hit_sum": 1.0,
            "place2_hit_sum": 0.0,
            "place3_hit_sum": 0.0,
            "top3_box_hit_sum": 1.0,
            "top3_exact_hit_sum": 0.0,
            "top3_winner_capture_sum": 1.0,
            "top5_winner_capture_sum": 1.0,
            "top3_place_relation_sum": 0.5,
            "pair_score_sum": 80.0,
            "ndcg_at_3_sum": 0.9,
        }
    )
    tup = subject.build_row_tuple(
        row,
        "model-v",
        "rs-v",
        "fp-v",
        "jra",
        "20240101",
        "20251231",
    )
    assert tup == (
        "model-v",
        "rs-v",
        "fp-v",
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
        120,
        1,
        1.0,
        1.0,
        0.0,
        0.0,
        1.0,
        0.0,
        1.0,
        1.0,
        0.5,
        80.0,
        0.9,
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
    rows: list[tuple[object, ...]] = [("active", "rs1", "fp1")]
    subject.execute_upsert(
        "postgres://test",
        rows,
        connect=connect_mock,
        execute_values_fn=execute_values_mock,
    )
    connect_mock.assert_called_once_with("postgres://test")
    execute_values_mock.assert_called_once()
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
                    "pair_score_pair_count": 120,
                    "ndcg_at_3_race_count": 1,
                    "top1_hit_sum": 1.0,
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
        "v3",
        "--running-style-feature-version",
        "rs1",
        "--finish-position-version",
        "fp1",
        "--category",
        "jra",
        "--window-from",
        "20240101",
        "--window-to",
        "20251231",
    ]
    monkeypatch.setattr("sys.argv", ["insert_bucket_evaluation_row.py", *argv])
    execute_upsert_mock = MagicMock()
    monkeypatch.setattr(subject, "execute_upsert", execute_upsert_mock)
    subject.main()
    execute_upsert_mock.assert_called_once()
    pg_url_arg, tuples_arg = execute_upsert_mock.call_args.args
    assert pg_url_arg == "postgres://test"
    assert len(tuples_arg) == 1
    assert tuples_arg[0][0] == "v3"
    assert tuples_arg[0][1] == "rs1"
    assert tuples_arg[0][2] == "fp1"


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


def test_main_handles_metrics_json_without_rows_field(
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
        "v3",
        "--running-style-feature-version",
        "rs1",
        "--finish-position-version",
        "fp1",
        "--category",
        "jra",
        "--window-from",
        "20240101",
        "--window-to",
        "20251231",
    ]
    monkeypatch.setattr("sys.argv", ["insert_bucket_evaluation_row.py", *argv])
    execute_upsert_mock = MagicMock()
    monkeypatch.setattr(subject, "execute_upsert", execute_upsert_mock)
    subject.main()
    execute_upsert_mock.assert_called_once_with("postgres://test", [])
