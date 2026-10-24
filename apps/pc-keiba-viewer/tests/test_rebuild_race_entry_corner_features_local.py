"""Tests for rebuild_race_entry_corner_features_local.py."""

from __future__ import annotations

import duckdb
import pytest

import _catalog_attach
import rebuild_race_entry_corner_features_local as subject


class FakeConnection:
    def __init__(self, results: list[list[tuple[object, ...]]]) -> None:
        self.statements: list[str] = []
        self._results: list[list[tuple[object, ...]]] = results
        self._last: list[tuple[object, ...]] = []

    def execute(self, query: str) -> FakeConnection:
        self.statements.append(query)
        self._last = self._results.pop(0) if "postgres_query" in query else []
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._last

    def close(self) -> None:
        self.statements.append("close")


def _seed_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema pg")
    se_columns = (
        "kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar, "
        "race_bango varchar, ketto_toroku_bango varchar, umaban varchar, bamei varchar, "
        "seibetsu_code varchar, barei varchar, futan_juryo varchar, "
        "kishumei_ryakusho varchar, chokyoshimei_ryakusho varchar, banushimei varchar, "
        "kakutei_chakujun varchar, tansho_ninkijun varchar, tansho_odds varchar, "
        "soha_time varchar, time_sa varchar, kohan_3f varchar, corner_1 varchar, "
        "corner_2 varchar, corner_3 varchar, corner_4 varchar"
    )
    ra_columns = (
        "kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar, "
        "race_bango varchar, track_code varchar, grade_code varchar, "
        "kyoso_shubetsu_code varchar, juryo_shubetsu_code varchar, "
        "kyoso_joken_code varchar, babajotai_code_shiba varchar, "
        "babajotai_code_dirt varchar, kyori varchar, shusso_tosu varchar"
    )
    con.execute(f"create table pg.jvd_se ({se_columns})")
    con.execute(f"create table pg.nvd_se ({se_columns})")
    con.execute(f"create table pg.jvd_ra ({ra_columns})")
    con.execute(f"create table pg.nvd_ra ({ra_columns})")
    se_row = "'H', '01', 'B', '1', '03', '560', 'J', 'T', 'O', '{finish}', '01', '0035', '1234', '0000', '350', '01', '01', '01', '01'"
    con.execute(
        "insert into pg.jvd_se values "
        f"('2025', '0601', '05', '11', {se_row.format(finish='01')}), "
        f"('2025', '0601', '05', '11', 'H2', '02', 'B', '1', '03', '560', 'J', 'T', 'O', '02', '02', '0050', '1234', '0001', '350', '02', '02', '02', '02'), "
        f"('2025', '0602', '44', '01', {se_row.format(finish='03')}), "
        f"('2025', '0702', '05', '01', {se_row.format(finish='01')})"
    )
    con.execute(
        "insert into pg.nvd_se values "
        f"('2025', '0602', '44', '01', {se_row.format(finish='03')})"
    )
    con.execute(
        "insert into pg.jvd_ra values "
        "('2025', '0601', '05', '11', '11', 'A', '13', '1', '010', '1', '0', '1600', '00'), "
        "('2025', '0602', '44', '01', '24', ' ', '13', '1', '010', '0', '1', '1200', '12'), "
        "('2025', '0702', '05', '01', '11', 'A', '13', '1', '010', '1', '0', '1600', '10')"
    )
    con.execute(
        "insert into pg.nvd_ra values "
        "('2025', '0602', '44', '01', '24', ' ', '13', '1', '010', '0', '1', '1200', '12')"
    )


def test_rebuild_select_drops_nar_venue_jra_rows_and_counts_field_size() -> None:
    con = duckdb.connect()
    _seed_raw_tables(con)
    rows = con.execute(
        "select source, keibajo_code, ketto_toroku_bango, shusso_tosu, finish_norm, "
        "corner1_norm, time_sa from ("
        + subject.rebuild_select_sql("20250601", "20250630")
        + ") order by source, keibajo_code, ketto_toroku_bango"
    ).fetchall()
    con.close()
    assert rows == [
        ("jra", "05", "H", 2, 0.0, 0.0, None),
        ("jra", "05", "H2", 2, 1.0, 1.0, 0.1),
        ("nar", "44", "H", 12, 2 / 11, 0.0, None),
    ]


def test_rebuild_select_without_dates_keeps_every_production_row() -> None:
    con = duckdb.connect()
    _seed_raw_tables(con)
    rows = con.execute(
        "select race_date, source, keibajo_code from ("
        + subject.rebuild_select_sql(None, None)
        + ") order by race_date, source, keibajo_code, ketto_toroku_bango"
    ).fetchall()
    con.close()
    assert rows == [
        ("20250601", "jra", "05"),
        ("20250601", "jra", "05"),
        ("20250602", "nar", "44"),
        ("20250702", "jra", "05"),
    ]


def test_production_select_sql_is_the_catalog_view_body() -> None:
    sql = subject.production_select_sql()
    assert sql.startswith("with raw_rows as (")
    assert "create view" not in sql


def test_production_select_sql_rejects_a_reshaped_view(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_catalog_attach, "_RAW_RACE_ENTRY_VIEW_SQL", "select 1")
    with pytest.raises(RuntimeError, match="changed shape"):
        subject.production_select_sql()


def test_parse_args_defaults_to_the_live_local_table() -> None:
    args = subject.parse_args([])
    assert args.target_table == "race_entry_corner_features"
    assert args.from_date is None
    assert args.to_date is None
    assert args.pg_url == "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"


def test_parse_args_rejects_unsafe_target_table() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(["--target-table", "x; drop table y"])


def test_parse_args_rejects_malformed_date() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(["--from-date", "2025-06-01"])


def test_postgres_execute_sql_escapes_quotes() -> None:
    assert subject.postgres_execute_sql("select 'a'") == (
        "call postgres_execute('pg', 'select ''a''')"
    )


def test_existing_index_definitions_reads_secondary_indexes() -> None:
    con = FakeConnection([[("CREATE INDEX a ON public.t (x)",)]])
    assert subject.existing_index_definitions(con, "t") == [
        "CREATE INDEX a ON public.t (x)"
    ]
    assert "indexname <> ''t_pkey''" in con.statements[0]


def test_swap_sql_replaces_target_and_restores_indexes_in_order() -> None:
    sql = subject.swap_sql("t", ["CREATE INDEX a ON public.t (x)"])
    statements = sql.split(";\n")
    assert statements[0] == "drop table if exists t__rebuilt"
    assert statements[1].startswith("create table t__rebuilt as select s.*, array[")
    assert statements[1].endswith(
        "]::vector(8) as feature_vector, now() as updated_at from t__staging s"
    )
    assert statements[2:] == [
        "drop table if exists t",
        "alter table t__rebuilt rename to t",
        "alter table t add primary key (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango, umaban)",
        "CREATE INDEX a ON public.t (x)",
        "drop table t__staging",
    ]


def test_rebuild_stages_swaps_and_counts() -> None:
    con = FakeConnection([[("CREATE INDEX a ON public.t (x)",)], [(42,)]])
    count = subject.rebuild(con, target_table="t", from_date="20250601", to_date=None)
    assert count == 42
    assert con.statements[0] == "drop table if exists pg.public.t__staging"
    assert con.statements[1].startswith("create table pg.public.t__staging as select * from (")
    assert con.statements[3].startswith("call postgres_execute('pg', 'drop table if exists t__rebuilt;")
    assert "CREATE INDEX a ON public.t (x)" in con.statements[3]
    assert con.statements[4] == (
        "select count from postgres_query('pg', 'select count(*) as count from t')"
    )


def test_main_attaches_writable_pg_and_reports_count(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    con = FakeConnection([[], [(7,)]])
    monkeypatch.setattr(duckdb, "connect", lambda: con)
    subject.main(["--target-table", "sample_t", "--from-date", "20250601"])
    assert con.statements[:3] == [
        "install postgres",
        "load postgres",
        "attach 'postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing' as pg (type postgres)",
    ]
    assert con.statements[-1] == "close"
    assert capsys.readouterr().out.startswith("sample_t=7 rows in ")
