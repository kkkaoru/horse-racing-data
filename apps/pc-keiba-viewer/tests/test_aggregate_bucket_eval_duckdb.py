from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

import aggregate_bucket_eval_duckdb as subject


def test_sql_quote_literal_doubles_single_quotes():
    assert subject.sql_quote_literal("o'brien") == "o''brien"


def test_sql_quote_literal_leaves_plain_text_unchanged():
    assert subject.sql_quote_literal("plain") == "plain"


def test_build_condition_case_sql_uses_ascii_space_trim():
    sql = subject.build_condition_case_sql("ra.kyoso_joken_code", "ra.kyoso_joken_meisho")
    assert "split_part(trim(ra.kyoso_joken_meisho, ' '), ' ', 1)" in sql


def test_build_condition_case_sql_includes_open_class_label():
    sql = subject.build_condition_case_sql("col_a", "col_b")
    assert "when col_a = '999' then 'オープン'" in sql


def test_build_race_name_sql_uses_ascii_space_trim_and_grade_filter():
    sql = subject.build_race_name_sql("ra.grade_code", "ra.kyosomei_hondai")
    assert sql == "case when ra.grade_code in ('A','F') then trim(ra.kyosomei_hondai, ' ') else null end"


def test_resolve_category_meta_jra_reads_jvd_ra_with_no_filter():
    meta = subject.resolve_category_meta(subject.CATEGORY_JRA)
    assert meta["ra_table"] == "jvd_ra"
    assert meta["ra_filter"] == "true"
    assert meta["actuals_filter"] == "rec.source = 'jra'"
    assert meta["years"] == tuple(range(2007, 2027))


def test_resolve_category_meta_nar_excludes_banei_keibajo():
    meta = subject.resolve_category_meta(subject.CATEGORY_NAR)
    assert meta["ra_table"] == "nvd_ra"
    assert meta["ra_filter"] == "ra.keibajo_code <> '83'"
    assert meta["actuals_filter"] == "rec.source = 'nar' and rec.keibajo_code <> '83'"
    assert meta["years"] == tuple(range(2007, 2027))


def test_resolve_category_meta_banei_filters_to_keibajo_83():
    meta = subject.resolve_category_meta(subject.CATEGORY_BAN_EI)
    assert meta["ra_table"] == "nvd_ra"
    assert meta["ra_filter"] == "ra.keibajo_code = '83'"
    assert meta["actuals_filter"] == "rec.source = 'nar' and rec.keibajo_code = '83'"
    assert meta["years"] == tuple(range(2008, 2027))


def test_resolve_category_meta_rejects_unknown_category():
    with pytest.raises(ValueError):
        subject.resolve_category_meta("keiba")


def test_build_year_window_pads_january_and_december():
    assert subject.build_year_window(2020) == ("20200101", "20201231")


def test_build_plan_window_spans_first_to_last_year():
    assert subject.build_plan_window((2008, 2009, 2026)) == ("20080101", "20261231")


def test_build_plan_window_handles_empty_years():
    assert subject.build_plan_window(()) == ("00101", "01231")


def test_build_bucket_aggregate_sql_banei_nulls_track_and_joken():
    sql = subject.build_bucket_aggregate_sql(
        subject.to_aggregate_args(
            predictions_glob="g.parquet",
            model_version="banei-cb-v7-lineage-wf-21y",
            category=subject.CATEGORY_BAN_EI,
            from_date="20080101",
            to_date="20081231",
            running_style_feature_version="v3",
            finish_position_version="v1",
        )
    )
    assert "null::text as kyoso_joken_code" in sql
    assert "null::text as track_code" in sql
    assert "null::text as condition_key" in sql
    assert "ra.keibajo_code = '83'" in sql


def test_build_bucket_aggregate_sql_nar_uses_condition_case_and_track():
    sql = subject.build_bucket_aggregate_sql(
        subject.to_aggregate_args(
            predictions_glob="g.parquet",
            model_version="nar-xgb-v7-lineage-wf-21y",
            category=subject.CATEGORY_NAR,
            from_date="20200101",
            to_date="20201231",
            running_style_feature_version="v3",
            finish_position_version="v1",
        )
    )
    assert "ra.track_code as track_code" in sql
    assert "ra.kyoso_joken_code as kyoso_joken_code" in sql
    assert "split_part(trim(ra.kyoso_joken_meisho, ' '), ' ', 1)" in sql
    assert "rec.source = 'nar' and rec.keibajo_code <> '83'" in sql


def test_build_bucket_aggregate_sql_jra_casts_kyori_and_uses_pair_self_join():
    sql = subject.build_bucket_aggregate_sql(
        subject.to_aggregate_args(
            predictions_glob="g.parquet",
            model_version="jra-cb-v7-lineage-wf-21y",
            category=subject.CATEGORY_JRA,
            from_date="20200101",
            to_date="20201231",
            running_style_feature_version="v3",
            finish_position_version="v1",
        )
    )
    assert "cast(ra.kyori as integer) as kyori" in sql
    assert "j1.ketto_toroku_bango < j2.ketto_toroku_bango" in sql
    assert "(3 / ln(2 + 1) + 2 / ln(2 + 2) + 1 / ln(2 + 3)) ideal_dcg" in sql
    assert "rec.source = 'jra'" in sql


def test_build_bucket_aggregate_sql_filters_predictions_by_versions():
    sql = subject.build_bucket_aggregate_sql(
        subject.to_aggregate_args(
            predictions_glob="my/glob/**/*.parquet",
            model_version="m'1",
            category=subject.CATEGORY_JRA,
            from_date="20200101",
            to_date="20201231",
            running_style_feature_version="v3",
            finish_position_version="v1",
        )
    )
    assert "model_version = 'm''1'" in sql
    assert "running_style_feature_version = 'v3'" in sql
    assert "finish_position_version = 'v1'" in sql
    assert "read_parquet('my/glob/**/*.parquet', hive_partitioning=1)" in sql


def test_build_bucket_evaluations_ddl_creates_table_and_indexes():
    ddl = subject.build_bucket_evaluations_ddl()
    assert "create table if not exists model_prediction_bucket_evaluations" in ddl
    assert "create unique index if not exists model_prediction_bucket_evaluations_uq" in ddl
    assert "pair_score_pair_count         integer not null" in ddl


def test_build_global_evaluations_ddl_creates_table_with_primary_key():
    ddl = subject.build_global_evaluations_ddl()
    assert "create table if not exists model_prediction_evaluations" in ddl
    assert "primary key (model_version, category, evaluation_window_from, evaluation_window_to)" in ddl
    assert "add column if not exists top3_place_relation numeric" in ddl


def test_build_bucket_upsert_sql_sets_metric_columns_only():
    sql = subject.build_bucket_upsert_sql()
    assert "insert into model_prediction_bucket_evaluations" in sql
    assert "on conflict (model_version, running_style_feature_version" in sql
    assert "race_count = excluded.race_count" in sql
    assert "model_version = excluded.model_version" not in sql
    assert "evaluated_at = now()" in sql


def test_build_bucket_upsert_sql_has_thirty_placeholders_plus_now():
    sql = subject.build_bucket_upsert_sql()
    assert "values (" + ", ".join(["%s"] * 30) + ", now())" in sql


def test_build_global_upsert_sql_sets_accuracy_columns_only():
    sql = subject.build_global_upsert_sql()
    assert "insert into model_prediction_evaluations" in sql
    assert "on conflict (model_version, category, evaluation_window_from, evaluation_window_to)" in sql
    assert "top1_accuracy = excluded.top1_accuracy" in sql
    assert "model_version = excluded.model_version" not in sql


def test_build_global_upsert_sql_has_seventeen_placeholders_plus_now():
    sql = subject.build_global_upsert_sql()
    assert "values (" + ", ".join(["%s"] * 17) + ", now())" in sql


def test_build_bucket_upsert_row_prepends_version_and_window_dims():
    aggregate_row = tuple(range(24))
    row = subject.build_bucket_upsert_row(
        aggregate_row=aggregate_row,
        model_version="m",
        running_style_feature_version="v3",
        finish_position_version="v1",
        category="jra",
        window_from="20200101",
        window_to="20201231",
    )
    assert row[0] == "m"
    assert row[1] == "v3"
    assert row[2] == "v1"
    assert row[3] == "jra"
    assert row[4] == "20200101"
    assert row[5] == "20201231"
    assert row[6] == 0
    assert len(row) == 30


def test_compute_global_rollup_divides_sums_by_race_count():
    aggregate_row = (
        "nar",
        "83",
        200,
        "02",
        None,
        None,
        None,
        None,
        None,
        10,
        90,
        5.0,
        5.0,
        2.0,
        1.0,
        3.0,
        0.5,
        7.0,
        8.0,
        4.0,
        80.0,
        120,
        6.0,
        10,
    )
    rollup = subject.compute_global_rollup([aggregate_row])
    assert rollup["race_count"] == 10
    assert rollup["prediction_count"] == 90
    assert rollup["top1_accuracy"] == 0.5
    assert rollup["place2_accuracy"] == 0.2
    assert rollup["top3_winner_capture"] == 0.7
    assert rollup["pair_score"] == 80.0 / 120
    assert rollup["ndcg_at_3"] == 6.0 / 10


def test_compute_global_rollup_returns_none_for_empty_input():
    rollup = subject.compute_global_rollup([])
    assert rollup["race_count"] == 0
    assert rollup["top1_accuracy"] is None
    assert rollup["pair_score"] is None
    assert rollup["ndcg_at_3"] is None


def test_build_global_upsert_row_orders_accuracy_columns():
    rollup = subject.GlobalRollup(
        race_count=10,
        prediction_count=90,
        top1_accuracy=0.5,
        top3_box_accuracy=0.3,
        top3_exact_accuracy=0.1,
        place1_accuracy=0.5,
        place2_accuracy=0.2,
        place3_accuracy=0.1,
        top3_winner_capture=0.7,
        top5_winner_capture=0.8,
        pair_score=0.66,
        ndcg_at_3=0.6,
        top3_place_relation=0.4,
    )
    row = subject.build_global_upsert_row(
        rollup=rollup,
        model_version="m",
        category="jra",
        window_from="20070101",
        window_to="20261231",
    )
    assert row[0] == "m"
    assert row[6] == 0.5
    assert row[7] == 0.3
    assert row[16] == 0.4
    assert len(row) == 17


def test_chunk_rows_splits_into_batches():
    rows: list[tuple[object, ...]] = [(i,) for i in range(5)]
    chunks = subject.chunk_rows(rows, 2)
    assert chunks == [[(0,), (1,)], [(2,), (3,)], [(4,)]]


def test_chunk_rows_rejects_non_positive_size():
    with pytest.raises(ValueError):
        subject.chunk_rows([(1,)], 0)


def test_to_aggregate_args_builds_typed_dict():
    args = subject.to_aggregate_args(
        predictions_glob="g",
        model_version="m",
        category="jra",
        from_date="20200101",
        to_date="20201231",
        running_style_feature_version="v3",
        finish_position_version="v1",
    )
    assert args["predictions_glob"] == "g"
    assert args["model_version"] == "m"
    assert args["category"] == "jra"


def test_parse_args_uses_default_model_versions():
    namespace = subject.parse_args(
        [
            "--predictions-glob",
            "g",
            "--local-pg-url",
            "postgres://local",
            "--neon-url",
            "postgres://neon",
            "--running-style-feature-version",
            "v3",
            "--finish-position-version",
            "v1",
        ]
    )
    assert namespace.model_version_jra == "jra-cb-v7-lineage-wf-21y"
    assert namespace.model_version_nar == "nar-xgb-v7-lineage-wf-21y"
    assert namespace.model_version_banei == "banei-cb-v7-lineage-wf-21y"
    assert namespace.threads == 15


def test_parse_args_requires_neon_url():
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--predictions-glob",
                "g",
                "--local-pg-url",
                "postgres://local",
                "--running-style-feature-version",
                "v3",
                "--finish-position-version",
                "v1",
            ]
        )


def test_resolve_model_version_picks_per_category():
    namespace = subject.parse_args(
        [
            "--predictions-glob",
            "g",
            "--local-pg-url",
            "l",
            "--neon-url",
            "n",
            "--running-style-feature-version",
            "v3",
            "--finish-position-version",
            "v1",
            "--model-version-jra",
            "jra-x",
            "--model-version-nar",
            "nar-x",
            "--model-version-banei",
            "banei-x",
        ]
    )
    assert subject.resolve_model_version(namespace, subject.CATEGORY_JRA) == "jra-x"
    assert subject.resolve_model_version(namespace, subject.CATEGORY_NAR) == "nar-x"
    assert subject.resolve_model_version(namespace, subject.CATEGORY_BAN_EI) == "banei-x"


def test_aggregate_category_year_executes_year_window_sql():
    duck = MagicMock()
    duck.execute.return_value.fetchall.return_value = [("nar", "83")]
    rows = subject.aggregate_category_year(
        duck,
        predictions_glob="g.parquet",
        model_version="banei-cb-v7-lineage-wf-21y",
        category=subject.CATEGORY_BAN_EI,
        year=2008,
        running_style_feature_version="v3",
        finish_position_version="v1",
    )
    assert rows == [("nar", "83")]
    executed_sql = duck.execute.call_args.args[0]
    assert "rec.race_date between '20080101' and '20081231'" in executed_sql


def test_ensure_neon_tables_runs_only_ddl_and_commits():
    pg = MagicMock()
    cursor = MagicMock()
    pg.cursor.return_value.__enter__.return_value = cursor
    subject.ensure_neon_tables(pg)
    assert cursor.execute.call_count == 2
    assert pg.commit.call_count == 1


def test_upsert_bucket_rows_batches_by_chunk_size_and_commits():
    pg = MagicMock()
    cursor = MagicMock()
    pg.cursor.return_value.__enter__.return_value = cursor
    rows: list[tuple[object, ...]] = [(i,) for i in range(subject.UPSERT_BATCH_SIZE + 3)]
    subject.upsert_bucket_rows(pg, rows)
    assert cursor.executemany.call_count == 2
    assert pg.commit.call_count == 1


def test_upsert_global_rows_runs_single_batch_and_commits():
    pg = MagicMock()
    cursor = MagicMock()
    pg.cursor.return_value.__enter__.return_value = cursor
    subject.upsert_global_rows(pg, [("m", "jra")])
    assert cursor.executemany.call_count == 1
    assert pg.commit.call_count == 1


def test_collect_category_returns_bucket_rows_and_global_row():
    namespace = subject.parse_args(
        [
            "--predictions-glob",
            "g",
            "--local-pg-url",
            "l",
            "--neon-url",
            "n",
            "--running-style-feature-version",
            "v3",
            "--finish-position-version",
            "v1",
        ]
    )
    duck = MagicMock()
    aggregate_row = tuple([0] * 9 + [3, 30, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 9.0, 12, 2.0, 3])
    duck.execute.return_value.fetchall.return_value = [aggregate_row]
    bucket_rows, global_row = subject.collect_category(duck, namespace, subject.CATEGORY_BAN_EI)
    assert len(bucket_rows) == len(subject.BAN_EI_YEARS)
    assert global_row[1] == subject.CATEGORY_BAN_EI
    assert global_row[2] == "20080101"
    assert global_row[3] == "20261231"


def test_run_aggregation_collects_upserts_and_closes_connections():
    namespace = subject.parse_args(
        [
            "--predictions-glob",
            "g",
            "--local-pg-url",
            "l",
            "--neon-url",
            "n",
            "--running-style-feature-version",
            "v3",
            "--finish-position-version",
            "v1",
        ]
    )
    duck = MagicMock()
    aggregate_row = tuple([0] * 9 + [3, 30, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 9.0, 12, 2.0, 3])
    duck.execute.return_value.fetchall.return_value = [aggregate_row]
    pg = MagicMock()
    pg_cursor = MagicMock()
    pg.cursor.return_value.__enter__.return_value = pg_cursor
    result = subject.run_aggregation(
        namespace,
        connect_duckdb=lambda _url, _threads: duck,
        connect_pg=lambda _url: pg,
    )
    assert result["categories"] == 3
    assert result["rollups"] == 3
    assert result["bucket_rows"] == len(subject.JRA_YEARS) + len(subject.NAR_YEARS) + len(
        subject.BAN_EI_YEARS
    )
    assert duck.close.call_count == 1
    assert pg.close.call_count == 1


def test_coerce_int_coerces_decimal_to_int():
    assert subject.coerce_int(Decimal("662")) == 662


def test_coerce_float_coerces_decimal_to_float():
    assert subject.coerce_float(Decimal("167.0")) == 167.0


def testcoerce_int_coerces_bool_true_to_one():
    assert subject.coerce_int(True) == 1


def testcoerce_int_coerces_float_to_int():
    assert subject.coerce_int(3.9) == 3


def testcoerce_int_coerces_numeric_string():
    assert subject.coerce_int("42") == 42


def testcoerce_int_returns_zero_for_empty_string():
    assert subject.coerce_int("") == 0


def testcoerce_int_returns_zero_for_none():
    assert subject.coerce_int(None) == 0


def testcoerce_float_coerces_bool_false_to_zero():
    assert subject.coerce_float(False) == 0.0


def testcoerce_float_coerces_int_to_float():
    assert subject.coerce_float(7) == 7.0


def testcoerce_float_coerces_numeric_string():
    assert subject.coerce_float("1.5") == 1.5


def testcoerce_float_returns_zero_for_empty_string():
    assert subject.coerce_float("") == 0.0


def testcoerce_float_returns_zero_for_none():
    assert subject.coerce_float(None) == 0.0


def test_default_duckdb_connect_attaches_local_pg_read_only(monkeypatch: pytest.MonkeyPatch):
    con = MagicMock()
    fake_module = MagicMock()
    fake_module.connect.return_value = con
    import importlib as _importlib

    monkeypatch.setattr(_importlib, "import_module", lambda _name: fake_module)
    result = subject.default_duckdb_connect("postgres://local", 8)
    assert result is con
    executed = [call.args[0] for call in con.execute.call_args_list]
    assert "set threads=8;" in executed
    assert "attach 'postgres://local' as pg (type postgres, read_only)" in executed


def test_default_psycopg_connect_opens_connection(monkeypatch: pytest.MonkeyPatch):
    connection = MagicMock()
    fake_module = MagicMock()
    fake_module.connect.return_value = connection
    import importlib as _importlib

    monkeypatch.setattr(_importlib, "import_module", lambda _name: fake_module)
    result = subject.default_psycopg_connect("postgres://neon")
    assert result is connection
    fake_module.connect.assert_called_once_with("postgres://neon")


def test_main_prints_json_summary(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        subject,
        "run_aggregation",
        lambda _args: {"bucket_rows": 7, "rollups": 3, "categories": 3},
    )
    subject.main(
        [
            "--predictions-glob",
            "g",
            "--local-pg-url",
            "l",
            "--neon-url",
            "n",
            "--running-style-feature-version",
            "v3",
            "--finish-position-version",
            "v1",
        ]
    )
    captured = capsys.readouterr()
    assert '"bucket_rows": 7' in captured.out
