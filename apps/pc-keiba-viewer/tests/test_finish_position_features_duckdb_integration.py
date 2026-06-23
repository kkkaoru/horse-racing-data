from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import cast

import duckdb
import pandas as pd
import pytest

import finish_position_features_duckdb as subject

REC_COLUMNS = [
    "source",
    "race_date",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "umaban",
    "kishumei_ryakusho",
    "chokyoshimei_ryakusho",
    "kyori",
    "track_code",
    "grade_code",
    "kyoso_joken_code",
    "shusso_tosu",
    "finish_position",
    "finish_norm",
    "time_sa",
    "kohan_3f",
    "corner1_norm",
    "corner3_norm",
    "corner4_norm",
    "babajotai_code_shiba",
    "babajotai_code_dirt",
    "tansho_ninkijun",
    "tansho_odds",
    "seibetsu_code",
]

REC_DATA: list[tuple[object, ...]] = [
    ("jra", "20180101", "2018", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 2, 0.5, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 2, 5.0, 1),
    ("jra", "20180601", "2018", "0601", "02", "01", "h001", 2, "j1", "t1", 1800, "11", "A", "005", 12, 1, 0.0, 0.0, 34.5, 0.2, 0.3, 0.4, "1", None, 1, 2.5, 1),
    ("jra", "20190101", "2019", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 0.0, 34.0, 0.2, 0.3, 0.4, "1", None, 1, 2.0, 1),
    ("jra", "20190101", "2019", "0101", "01", "01", "h002", 2, "j2", "t1", 1600, "10", "A", "000", 10, 2, 0.5, 1.0, 34.5, 0.3, 0.4, 0.5, "1", None, 2, 3.0, 2),
    ("jra", "20190601", "2019", "0601", "02", "01", "h001", 1, "j1", "t1", 1800, "11", "A", "005", 12, 3, 0.7, 1.5, 36.0, 0.3, 0.4, 0.5, "1", None, 3, 4.0, 1),
    ("jra", "20190601", "2019", "0601", "02", "01", "h002", 2, "j2", "t2", 1800, "11", "A", "005", 12, 1, 0.0, 0.0, 35.5, 0.2, 0.3, 0.4, "1", None, 1, 2.0, 2),
    ("jra", "20200101", "2020", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 0.0, 34.0, 0.2, 0.3, 0.4, "1", None, 1, 2.0, 1),
    ("jra", "20200101", "2020", "0101", "01", "01", "h002", 2, "j2", "t1", 1600, "10", "A", "000", 10, 2, 0.5, 1.0, 34.5, 0.3, 0.4, 0.5, "1", None, 2, 3.0, 2),
    ("nar", "20190601", "2019", "0601", "01", "02", "h003", 1, "j3", "t3", 1200, "20", "B", "000", 8, 1, 0.0, 0.0, 25.5, 0.3, 0.4, 0.5, None, "1", 1, 2.0, 3),
    ("nar", "20200601", "2020", "0601", "01", "02", "h003", 1, "j3", "t3", 1200, "20", "B", "000", 8, 1, 0.0, 0.0, 25.0, 0.3, 0.4, 0.5, None, "1", 1, 2.0, 3),
]


def _seed_rec(con: duckdb.DuckDBPyConnection) -> None:
    df = pd.DataFrame(REC_DATA, columns=REC_COLUMNS)
    con.register("rec_df", df)
    con.execute(
        """
        create or replace temp table rec as
        select source, race_date,
          strptime(race_date, '%Y%m%d')::date as race_dt,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban,
          kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code,
          shusso_tosu, finish_position, finish_norm,
          time_sa, kohan_3f, corner1_norm, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, seibetsu_code
        from rec_df
        """
    )


def _seed_horse_masters(con: duckdb.DuckDBPyConnection) -> None:
    jra_um = pd.DataFrame(
        [
            ("h001", "s001", "d001"),
            ("h002", "s002", "d002"),
        ],
        columns=["ketto_toroku_bango", "ketto_joho_01b", "ketto_joho_05b"],
    )
    nar_um = pd.DataFrame(
        [
            ("h003", "s003", "d003"),
        ],
        columns=["ketto_toroku_bango", "ketto_joho_01b", "ketto_joho_05b"],
    )
    con.register("jra_um_df", jra_um)
    con.register("nar_um_df", nar_um)
    con.execute("create or replace temp table jra_um as select * from jra_um_df")
    con.execute("create or replace temp table nar_um as select * from nar_um_df")


def _seed_weight_tables(con: duckdb.DuckDBPyConnection) -> None:
    jra_se = pd.DataFrame(
        [
            ("2018", "0101", "01", "01", "h001", "480", "000", "+"),
            ("2018", "0601", "02", "01", "h001", "478", "002", "-"),
            ("2019", "0101", "01", "01", "h001", "482", "004", "+"),
            ("2019", "0101", "01", "01", "h002", "470", "000", "+"),
            ("2019", "0601", "02", "01", "h001", "484", "002", "+"),
            ("2019", "0601", "02", "01", "h002", "472", "002", "+"),
            ("2020", "0101", "01", "01", "h001", "486", "002", "+"),
            ("2020", "0101", "01", "01", "h002", "474", "002", "+"),
        ],
        columns=[
            "kaisai_nen",
            "kaisai_tsukihi",
            "keibajo_code",
            "race_bango",
            "ketto_toroku_bango",
            "bataiju",
            "zogen_sa",
            "zogen_fugo",
        ],
    )
    nar_se = pd.DataFrame(
        [
            ("2019", "0601", "01", "02", "h003", "440", "000", "+"),
            ("2020", "0601", "01", "02", "h003", "442", "002", "+"),
        ],
        columns=[
            "kaisai_nen",
            "kaisai_tsukihi",
            "keibajo_code",
            "race_bango",
            "ketto_toroku_bango",
            "bataiju",
            "zogen_sa",
            "zogen_fugo",
        ],
    )
    con.register("jra_se_df", jra_se)
    con.register("nar_se_df", nar_se)
    con.execute("create or replace temp table jra_se as select * from jra_se_df")
    con.execute("create or replace temp table nar_se as select * from nar_se_df")


def _seed_weather_tables(con: duckdb.DuckDBPyConnection) -> None:
    jra_ra = pd.DataFrame(
        [
            ("2020", "0101", "01", "01", "1", None),
        ],
        columns=[
            "kaisai_nen",
            "kaisai_tsukihi",
            "keibajo_code",
            "race_bango",
            "tenko_code",
            "kyoso_joken_meisho",
        ],
    )
    nar_ra = pd.DataFrame(
        [
            ("2020", "0601", "01", "02", "2", "「　　　Ｃ２　」"),
        ],
        columns=[
            "kaisai_nen",
            "kaisai_tsukihi",
            "keibajo_code",
            "race_bango",
            "tenko_code",
            "kyoso_joken_meisho",
        ],
    )
    con.register("jra_ra_df", jra_ra)
    con.register("nar_ra_df", nar_ra)
    con.execute("create or replace temp table jra_ra as select * from jra_ra_df")
    con.execute("create or replace temp table nar_ra as select * from nar_ra_df")


@pytest.fixture
def seeded_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    _seed_rec(con)
    _seed_horse_masters(con)
    _seed_weight_tables(con)
    _seed_weather_tables(con)
    subject.build_target_table(con, "jra", "20200101", "20201231")
    subject.materialize_se_lookup(con)
    return con


def _silent_heartbeat() -> subject.Heartbeat:
    return subject.Heartbeat(0.0, None)


def test_non_negative_float_accepts_zero():
    assert subject.non_negative_float("0") == 0.0


def test_non_negative_float_accepts_positive():
    assert subject.non_negative_float("3.5") == 3.5


def test_non_negative_float_rejects_negative():
    with pytest.raises(argparse.ArgumentTypeError):
        subject.non_negative_float("-1")


def test_parse_args_supports_skip_count_and_keep_existing_output():
    args = subject.parse_args(["--skip-count", "--keep-existing-output", "--force-clean-output"])
    assert args.skip_count is True
    assert args.keep_existing_output is True
    assert args.force_clean_output is True


def test_parse_args_supports_temp_dir(tmp_path: Path):
    args = subject.parse_args(["--temp-dir", str(tmp_path / "duck-temp")])
    assert args.temp_dir == tmp_path / "duck-temp"


def test_parse_args_temp_dir_defaults_to_none():
    args = subject.parse_args([])
    assert args.temp_dir is None


def test_configure_duckdb_session_applies_temp_dir(tmp_path: Path):
    con = duckdb.connect(":memory:")
    target = tmp_path / "duck-temp"
    subject.configure_duckdb_session(con, 2, "1GB", target)
    assert target.exists()
    row = con.execute("select current_setting('temp_directory')").fetchone()
    assert row is not None
    assert row[0] == target.as_posix()


def test_run_staged_sql_emits_start_and_done_events(capsys: pytest.CaptureFixture[str]):
    con = duckdb.connect(":memory:")
    subject.run_staged_sql(con, "demo.stage", "create or replace temp table demo as select 1 as x")
    output_lines = capsys.readouterr().out.splitlines()
    payloads = [json.loads(line) for line in output_lines]
    statuses = [payload["status"] for payload in payloads]
    stages = [payload["stage"] for payload in payloads]
    assert statuses == ["start", "done"]
    assert stages == ["demo.stage", "demo.stage"]


class _ExecResultStub:
    def fetchone(self) -> tuple[int]:
        return (0,)


class _ExecCaptureCon:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> object:
        self.statements.append(sql)
        return _ExecResultStub()


def test_stage_rec_table_loads_columns_and_indexes(capsys: pytest.CaptureFixture[str]):
    captured = _ExecCaptureCon()
    subject.stage_rec_table(
        cast(duckdb.DuckDBPyConnection, captured), "20100101", "20251231", "jra"
    )
    joined = "\n".join(captured.statements)
    assert "race_date between '20100101' and '20251231'" in joined
    assert "create index rec_horse_date on rec" in joined
    assert "create index rec_jockey_date on rec" in joined
    assert "create index rec_trainer_date on rec" in joined
    assert "create index rec_keibajo_date on rec" in joined
    output = capsys.readouterr().out
    assert "source.rec" in output
    assert "source.rec.indexes" in output


def test_build_rec_select_sql_jra_unions_corner_features_and_ban_ei():
    sql = subject.build_rec_select_sql("jra", "20160101", "20251231")
    assert "from pg.race_entry_corner_features" in sql
    assert "union all" in sql
    assert "from pg.nvd_se" in sql
    assert "se.keibajo_code = '83'" in sql


def test_build_rec_select_sql_ban_ei_uses_only_nvd_se_path():
    sql = subject.build_rec_select_sql("ban-ei", "20160101", "20251231")
    assert "from pg.race_entry_corner_features" not in sql
    assert "from pg.nvd_se" in sql
    assert "se.keibajo_code = '83'" in sql
    assert "union all" not in sql


def test_stage_se_table_filters_by_concatenated_date(capsys: pytest.CaptureFixture[str]):
    captured = _ExecCaptureCon()
    subject.stage_se_table(
        cast(duckdb.DuckDBPyConnection, captured),
        "source.jra_se",
        "jra_se",
        "jvd_se",
        "20100101",
        "20251231",
    )
    joined = "\n".join(captured.statements)
    assert "from pg.jvd_se" in joined
    assert "(kaisai_nen || kaisai_tsukihi) between '20100101' and '20251231'" in joined
    assert "source.jra_se" in capsys.readouterr().out


def test_stage_um_table_selects_pedigree_columns(capsys: pytest.CaptureFixture[str]):
    captured = _ExecCaptureCon()
    subject.stage_um_table(
        cast(duckdb.DuckDBPyConnection, captured), "source.jra_um", "jra_um", "jvd_um"
    )
    joined = "\n".join(captured.statements)
    assert "from pg.jvd_um" in joined
    assert "ketto_joho_01b" in joined
    assert "source.jra_um" in capsys.readouterr().out


def test_stage_ra_table_uses_target_date_range(capsys: pytest.CaptureFixture[str]):
    captured = _ExecCaptureCon()
    subject.stage_ra_table(
        cast(duckdb.DuckDBPyConnection, captured),
        "source.jra_ra",
        "jra_ra",
        "jvd_ra",
        "20200101",
        "20201231",
    )
    joined = "\n".join(captured.statements)
    assert "from pg.jvd_ra" in joined
    assert "(kaisai_nen || kaisai_tsukihi) between '20200101' and '20201231'" in joined
    assert "kyoso_joken_meisho" in joined
    assert "source.jra_ra" in capsys.readouterr().out


def test_stage_source_tables_jra_orchestrates_rec_se_um_ra(capsys: pytest.CaptureFixture[str]):
    captured = _ExecCaptureCon()
    subject.stage_source_tables(
        cast(duckdb.DuckDBPyConnection, captured), "20200101", "20201231", "jra"
    )
    output = capsys.readouterr().out
    for stage in (
        "source.config",
        "source.rec",
        "source.jra_se",
        "source.nar_se",
        "source.jra_um",
        "source.nar_um",
        "source.jra_ra",
        "source.nar_ra",
    ):
        assert stage in output


def test_stage_source_tables_ban_ei_skips_jra_pg_reads(capsys: pytest.CaptureFixture[str]):
    captured = _ExecCaptureCon()
    subject.stage_source_tables(
        cast(duckdb.DuckDBPyConnection, captured), "20200101", "20201231", "ban-ei"
    )
    joined = "\n".join(captured.statements)
    assert "from pg.jvd_se" not in joined
    assert "from pg.jvd_ra" not in joined
    assert "from pg.jvd_um" not in joined
    assert "from pg.nvd_se" in joined
    assert "and keibajo_code = '83'" in joined
    assert "kyoso_joken_meisho" in joined
    output = capsys.readouterr().out
    assert "source.jra_se.skip" in output
    assert "source.jra_um.skip" in output
    assert "source.jra_ra.skip" in output


def test_stage_source_tables_nar_keeps_full_rec_for_history_precision(
    capsys: pytest.CaptureFixture[str],
):
    captured = _ExecCaptureCon()
    subject.stage_source_tables(
        cast(duckdb.DuckDBPyConnection, captured), "20200101", "20201231", "nar"
    )
    joined = "\n".join(captured.statements)
    assert "from pg.race_entry_corner_features" in joined
    assert "union all" in joined
    assert "from pg.jvd_se" in joined
    assert "from pg.nvd_se" in joined
    output = capsys.readouterr().out
    assert "source.jra_se" in output
    assert "source.nar_se" in output


def test_parse_args_supports_heartbeat_interval():
    args = subject.parse_args(["--heartbeat-interval", "2.5"])
    assert args.heartbeat_interval == 2.5


def test_parse_args_rejects_negative_heartbeat_interval():
    with pytest.raises(SystemExit):
        subject.parse_args(["--heartbeat-interval", "-0.5"])


def test_write_status_atomic_writes_json_and_replaces(tmp_path: Path):
    path = tmp_path / "status.json"
    subject.write_status_atomic(path, {"stage": "a", "rows": 10})
    assert json.loads(path.read_text()) == {"stage": "a", "rows": 10}


def test_write_status_atomic_skips_when_path_is_none():
    subject.write_status_atomic(None, {"ignored": True})


def test_read_runtime_snapshot_returns_two_floats():
    cpu, wall = subject.read_runtime_snapshot()
    assert isinstance(cpu, float)
    assert isinstance(wall, float)


def test_compute_runtime_stats_returns_expected_keys():
    cpu, wall = subject.read_runtime_snapshot()
    time.sleep(0.01)
    stats, new_cpu, new_wall = subject.compute_runtime_stats(cpu, wall)
    assert set(stats.keys()) == {"rss_mb", "cpu_percent", "cpu_user_seconds", "cpu_sys_seconds"}
    assert new_wall >= wall
    assert new_cpu >= cpu


def test_compute_runtime_stats_handles_zero_wall_delta():
    cpu, wall = subject.read_runtime_snapshot()
    stats, _, _ = subject.compute_runtime_stats(cpu, wall + 999.0)
    assert stats["cpu_percent"] == 0.0


def test_heartbeat_emits_stage_change_and_stop_without_thread(tmp_path: Path):
    status_path = tmp_path / "hb.json"
    heartbeat = subject.Heartbeat(0.0, status_path)
    heartbeat.start()
    heartbeat.set_stage("alpha")
    heartbeat.set_substage("part-1")
    heartbeat.stop()
    payload = json.loads(status_path.read_text())
    assert payload["kind"] == "stop"
    assert payload["stage"] == "alpha"


def test_heartbeat_runs_periodic_tick_when_interval_positive(tmp_path: Path):
    status_path = tmp_path / "hb.json"
    heartbeat = subject.Heartbeat(0.05, status_path)
    heartbeat.start()
    heartbeat.set_stage("running")
    time.sleep(0.15)
    heartbeat.stop()
    assert status_path.exists()


def test_get_target_years_returns_sorted_distinct(seeded_con: duckdb.DuckDBPyConnection):
    years = subject.get_target_years(seeded_con)
    assert years == [2020]


def test_get_target_years_returns_empty_when_target_empty():
    con = duckdb.connect(":memory:")
    con.execute("create temp table target (race_year integer)")
    assert subject.get_target_years(con) == []


def test_build_target_table_populates_target(seeded_con: duckdb.DuckDBPyConnection):
    row = seeded_con.execute("select count(*) from target").fetchone()
    assert row is not None
    assert row[0] == 2


def test_materialize_temp_table_creates_named_table():
    con = duckdb.connect(":memory:")
    con.execute("create temp table src (v integer)")
    con.execute("insert into src values (1), (2), (3)")
    rows = subject.materialize_temp_table(
        con,
        "src.copy",
        "src_copy",
        "src_filtered as (select v from src where v > 1)",
        "src_filtered",
    )
    assert rows == 2


def test_materialize_horse_history_base_filters_to_year(seeded_con: duckdb.DuckDBPyConnection):
    rows = subject.materialize_horse_history_base(seeded_con, "t.kaisai_nen = '2020'")
    assert rows > 0
    schema_row = seeded_con.execute(
        "select name from pragma_table_info('horse_history_base') where name in ('target_race_dt', 'history_race_dt', 'recent_rank')"
    ).fetchall()
    assert len(schema_row) == 3


def test_materialize_temp_table_by_year_creates_then_inserts(seeded_con: duckdb.DuckDBPyConnection):
    rows = subject.materialize_temp_table_by_year(
        seeded_con,
        "jockey_career",
        "jockey_career",
        subject.jockey_cte,
        "jockey_career",
        [2020],
        _silent_heartbeat(),
    )
    assert rows > 0


def test_stage_horse_history_derived_creates_four_tables(seeded_con: duckdb.DuckDBPyConnection):
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    for table in ("horse_career", "recent_form", "weight_agg", "legacy_features"):
        row = seeded_con.execute(f"select count(*) from {table}").fetchone()
        assert row is not None
        assert row[0] > 0


def test_materialize_pedigree_stats_creates_month_partitioned_stats(
    seeded_con: duckdb.DuckDBPyConnection,
):
    subject.materialize_pedigree_stats(seeded_con, "jra")
    for table in subject.PEDIGREE_STAT_TABLES:
        row = seeded_con.execute(
            f"select count(*) from pragma_table_info('{table}') where name = 'stats_year_month'"
        ).fetchone()
        assert row is not None
        assert row[0] == 1


def test_materialize_pedigree_stats_excludes_current_month_data(
    seeded_con: duckdb.DuckDBPyConnection,
):
    subject.materialize_pedigree_stats(seeded_con, "jra")
    row = seeded_con.execute(
        "select count(*) from sire_distance_stats where stats_year_month = 202001"
    ).fetchone()
    assert row is not None
    assert row[0] > 0


def test_materialize_pedigree_stats_includes_target_months_table(
    seeded_con: duckdb.DuckDBPyConnection,
):
    subject.materialize_pedigree_stats(seeded_con, "jra")
    row = seeded_con.execute("select count(*) from target_months").fetchone()
    assert row is not None
    assert row[0] == 1


def test_materialize_pedigree_stats_materializes_pedigree_rec_um(
    seeded_con: duckdb.DuckDBPyConnection,
):
    subject.materialize_pedigree_stats(seeded_con, "jra")
    row = seeded_con.execute("select count(*) from pedigree_rec_um").fetchone()
    assert row is not None
    assert row[0] > 0


def test_pedigree_rec_um_has_precomputed_race_year_month(
    seeded_con: duckdb.DuckDBPyConnection,
):
    seeded_con.execute(subject.target_pedigree_sql())
    seeded_con.execute(subject.target_months_sql())
    seeded_con.execute(subject.pedigree_rec_um_sql("jra"))
    row = seeded_con.execute(
        "select min(race_year_month), max(race_year_month) from pedigree_rec_um"
    ).fetchone()
    assert row is not None
    assert row[0] == 201801
    assert row[1] == 202001


def test_pedigree_stats_avg_uses_non_null_count_only(tmp_path: Path):
    con = duckdb.connect(":memory:")
    rows = [
        ("jra", "20180101", "2018", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 1, 2.0, 1),
        ("jra", "20180102", "2018", "0102", "01", "01", "h001", 2, "j1", "t1", 1600, "10", "A", "000", 10, 2, None, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 2, 3.0, 1),
        ("jra", "20200101", "2020", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 0.0, 34.0, 0.2, 0.3, 0.4, "1", None, 1, 2.0, 1),
    ]
    df = pd.DataFrame(rows, columns=REC_COLUMNS)
    con.register("rec_null_df", df)
    con.execute(
        """
        create or replace temp table rec as
        select source, race_date,
          strptime(race_date, '%Y%m%d')::date as race_dt,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban,
          kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code,
          shusso_tosu, finish_position,
          cast(finish_norm as double) as finish_norm,
          time_sa, kohan_3f, corner1_norm, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, seibetsu_code
        from rec_null_df
        """
    )
    _seed_horse_masters(con)
    _seed_weight_tables(con)
    _seed_weather_tables(con)
    subject.build_target_table(con, "jra", "20200101", "20201231")
    subject.materialize_pedigree_stats(con, "jra")
    row = con.execute(
        "select sire_avg_finish_at_distance_val from sire_distance_stats "
        "where stats_year_month = 202001"
    ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(0.0)


def test_materialize_race_context_builds_aggregates(seeded_con: duckdb.DuckDBPyConnection):
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_race_context(seeded_con)
    row = seeded_con.execute("select count(*) from race_field_aggregates").fetchone()
    assert row is not None
    assert row[0] > 0


def test_materialize_weather_lookup_joins_jra_ra(seeded_con: duckdb.DuckDBPyConnection):
    subject.materialize_venue_weather(seeded_con, None, [2020])
    subject.materialize_weather_lookup(seeded_con)
    row = seeded_con.execute(
        "select tenko_code from weather_lookup where ketto_toroku_bango = 'h001'"
    ).fetchone()
    assert row is not None
    assert row[0] == "1"


def test_materialize_weather_lookup_projects_nar_kyoso_joken_meisho_column():
    con = duckdb.connect(":memory:")
    _seed_rec(con)
    _seed_horse_masters(con)
    _seed_weight_tables(con)
    _seed_weather_tables(con)
    subject.build_target_table(con, "nar", "20200101", "20201231")
    subject.materialize_venue_weather(con, None, [2020])
    subject.materialize_weather_lookup(con)
    row = con.execute(
        "select nar_kyoso_joken_meisho from weather_lookup where ketto_toroku_bango = 'h003'"
    ).fetchone()
    assert row is not None
    assert row[0] == "「　　　Ｃ２　」"


def _write_venue_weather_db(path: Path, rows: list[tuple[object, ...]]) -> None:
    src = duckdb.connect(str(path))
    src.execute(
        "create table venue_weather ("
        "keibajo_code varchar, weather_date date, weather_hour integer, "
        "temperature double, precipitation double, wind_speed double, wind_gusts double)"
    )
    src.executemany(
        "insert into venue_weather values (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    src.close()


def test_materialize_weather_lookup_left_joins_venue_weather(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    _write_venue_weather_db(
        tmp_path / "venue_weather_2020.duckdb",
        [
            ("01", "2020-01-01", 8, 99.0, 99.0, 99.0, 99.0),
            ("01", "2020-01-01", 10, 10.0, 1.0, 5.0, 9.0),
            ("01", "2020-01-01", 14, 20.0, 3.0, 7.0, 12.0),
        ],
    )
    subject.materialize_venue_weather(seeded_con, tmp_path, [2020])
    subject.materialize_weather_lookup(seeded_con)
    row = seeded_con.execute(
        "select venue_temperature, venue_precipitation_total, "
        "venue_wind_speed_max, venue_wind_gusts_max from weather_lookup "
        "where ketto_toroku_bango = 'h001'"
    ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(15.0)
    assert row[1] == pytest.approx(4.0)
    assert row[2] == pytest.approx(7.0)
    assert row[3] == pytest.approx(12.0)


def test_materialize_weather_lookup_venue_columns_null_when_no_venue_data(
    seeded_con: duckdb.DuckDBPyConnection,
):
    subject.materialize_venue_weather(seeded_con, None, [2020])
    subject.materialize_weather_lookup(seeded_con)
    row = seeded_con.execute(
        "select venue_temperature, venue_precipitation_total, "
        "venue_wind_speed_max, venue_wind_gusts_max from weather_lookup "
        "where ketto_toroku_bango = 'h001'"
    ).fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1] is None
    assert row[2] is None
    assert row[3] is None


def test_base_features_select_emits_venue_weather_value(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    _write_venue_weather_db(
        tmp_path / "venue_weather_2020.duckdb",
        [
            ("01", "2020-01-01", 10, 12.0, 2.0, 6.0, 10.0),
            ("01", "2020-01-01", 15, 18.0, 4.0, 8.0, 13.0),
        ],
    )
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_pedigree_stats(seeded_con, "jra")
    subject.materialize_race_context(seeded_con)
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_venue_weather(seeded_con, tmp_path, [2020])
    subject.materialize_weather_lookup(seeded_con)
    base_sql = subject.base_features_select_sql("jra")
    row = seeded_con.execute(
        f"with final as ({base_sql}) "
        "select venue_temperature, venue_precipitation_total, "
        "venue_wind_speed_max, venue_wind_gusts_max from final "
        "where ketto_toroku_bango = 'h001'"
    ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(15.0)
    assert row[1] == pytest.approx(6.0)
    assert row[2] == pytest.approx(8.0)
    assert row[3] == pytest.approx(13.0)


def test_write_parquet_emits_venue_weather_columns(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    output_dir = tmp_path / "out"
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_pedigree_stats(seeded_con, "jra")
    subject.materialize_race_context(seeded_con)
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_venue_weather(seeded_con, None, [2020])
    subject.materialize_weather_lookup(seeded_con)
    subject.write_parquet(
        seeded_con,
        subject.assemble_final_select_from_temp_tables("jra"),
        output_dir,
        keep_existing=False,
        force_clean=True,
    )
    reader = duckdb.connect(":memory:")
    columns = [
        row[0]
        for row in reader.execute(
            f"describe select * from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')"
        ).fetchall()
    ]
    reader.close()
    assert "venue_temperature" in columns
    assert "venue_precipitation_total" in columns
    assert "venue_wind_speed_max" in columns
    assert "venue_wind_gusts_max" in columns


def test_stage_partner_features_creates_jockey_and_trainer(seeded_con: duckdb.DuckDBPyConnection):
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    for table in ("jockey_career", "trainer_career"):
        row = seeded_con.execute(f"select count(*) from {table}").fetchone()
        assert row is not None
        assert row[0] > 0


def test_stage_track_bias_creates_track_bias(seeded_con: duckdb.DuckDBPyConnection):
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
    row = seeded_con.execute("select count(*) from track_bias").fetchone()
    assert row is not None
    assert row[0] == 2


def test_configure_duckdb_session_applies_threads_and_memory():
    con = duckdb.connect(":memory:")
    subject.configure_duckdb_session(con, 2, "1GB")
    row = con.execute("select current_setting('threads')").fetchone()
    assert row is not None
    assert int(row[0]) == 2


def test_prepare_output_dir_removes_existing_partition(tmp_path: Path):
    leftover = tmp_path / "out" / "race_year=2024" / "data_0.parquet"
    leftover.parent.mkdir(parents=True)
    leftover.write_text("stale")
    subject.prepare_output_dir(tmp_path / "out", keep_existing=False, force_clean=False)
    assert not leftover.exists()
    assert (tmp_path / "out").exists()


UPCOMING_REC_DATA: list[tuple[object, ...]] = [
    ("jra", "20250101", "2025", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 1, 5.0, 1),
    ("jra", "20260603", "2026", "0603", "05", "11", "h001", 3, "j1", "t1", 1600, "10", "A", "000", 9, None, None, None, None, None, None, None, "1", None, None, None, 1),
]


def _seed_upcoming_rec(con: duckdb.DuckDBPyConnection) -> None:
    df = pd.DataFrame(UPCOMING_REC_DATA, columns=REC_COLUMNS)
    con.register("rec_upcoming_df", df)
    con.execute(
        """
        create or replace temp table rec as
        select source, race_date,
          strptime(race_date, '%Y%m%d')::date as race_dt,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban,
          kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code,
          shusso_tosu,
          cast(finish_position as integer) as finish_position,
          cast(finish_norm as double) as finish_norm,
          cast(time_sa as double) as time_sa, cast(kohan_3f as double) as kohan_3f,
          cast(corner1_norm as double) as corner1_norm,
          cast(corner3_norm as double) as corner3_norm,
          cast(corner4_norm as double) as corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          cast(tansho_ninkijun as integer) as tansho_ninkijun,
          cast(tansho_odds as double) as tansho_odds,
          cast(seibetsu_code as integer) as seibetsu_code
        from rec_upcoming_df
        """
    )


def test_build_target_table_includes_upcoming_null_finish_position_row():
    con = duckdb.connect(":memory:")
    _seed_upcoming_rec(con)
    subject.build_target_table(con, "jra", "20260603", "20260603")
    row = con.execute(
        "select finish_position from target "
        "where kaisai_nen = '2026' and kaisai_tsukihi = '0603' and race_bango = '11'"
    ).fetchone()
    assert row is not None
    assert row[0] is None


def test_build_target_table_target_date_window_excludes_prior_history_row():
    con = duckdb.connect(":memory:")
    _seed_upcoming_rec(con)
    subject.build_target_table(con, "jra", "20260603", "20260603")
    row = con.execute("select count(*) from target").fetchone()
    assert row is not None
    assert row[0] == 1


def test_prepare_output_dir_keeps_existing_when_requested(tmp_path: Path):
    leftover = tmp_path / "out" / "race_year=2024" / "data_0.parquet"
    leftover.parent.mkdir(parents=True)
    leftover.write_text("stale")
    subject.prepare_output_dir(tmp_path / "out", keep_existing=True, force_clean=False)
    assert leftover.exists()


def test_prepare_output_dir_refuses_unknown_entries_without_force(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    foreign = out_dir / "important_other_file.txt"
    foreign.write_text("do not delete")
    with pytest.raises(ValueError, match="refusing to clean"):
        subject.prepare_output_dir(out_dir, keep_existing=False, force_clean=False)
    assert foreign.exists()


def test_prepare_output_dir_force_clean_removes_unknown_entries(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "important_other_file.txt").write_text("will be deleted")
    subject.prepare_output_dir(out_dir, keep_existing=False, force_clean=True)
    assert not (out_dir / "important_other_file.txt").exists()
    assert out_dir.exists()


def test_directory_only_contains_partitions_detects_foreign_files(tmp_path: Path):
    (tmp_path / "race_year=2020").mkdir()
    assert subject.directory_only_contains_partitions(tmp_path) is True
    (tmp_path / "unexpected.txt").write_text("x")
    assert subject.directory_only_contains_partitions(tmp_path) is False


def test_write_parquet_cleans_output_dir(seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    stale = output_dir / "race_year=2099" / "data_0.parquet"
    stale.parent.mkdir(parents=True)
    stale.write_text("stale")
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_pedigree_stats(seeded_con, "jra")
    subject.materialize_race_context(seeded_con)
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_venue_weather(seeded_con, None, [2020])
    subject.materialize_weather_lookup(seeded_con)
    subject.write_parquet(
        seeded_con,
        subject.assemble_final_select_from_temp_tables("jra"),
        output_dir,
        keep_existing=False,
        force_clean=True,
    )
    assert not stale.exists()
    written = list(output_dir.glob("race_year=*/data_*.parquet"))
    assert written


def test_write_parquet_emits_nar_subclass_column(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    output_dir = tmp_path / "out"
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_pedigree_stats(seeded_con, "jra")
    subject.materialize_race_context(seeded_con)
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_venue_weather(seeded_con, None, [2020])
    subject.materialize_weather_lookup(seeded_con)
    subject.write_parquet(
        seeded_con,
        subject.assemble_final_select_from_temp_tables("jra"),
        output_dir,
        keep_existing=False,
        force_clean=True,
    )
    reader = duckdb.connect(":memory:")
    columns = [
        row[0]
        for row in reader.execute(
            f"describe select * from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')"
        ).fetchall()
    ]
    reader.close()
    assert "nar_subclass" in columns


def test_write_parquet_emits_kyoso_joken_code_column(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    output_dir = tmp_path / "out"
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_pedigree_stats(seeded_con, "jra")
    subject.materialize_race_context(seeded_con)
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_venue_weather(seeded_con, None, [2020])
    subject.materialize_weather_lookup(seeded_con)
    subject.write_parquet(
        seeded_con,
        subject.assemble_final_select_from_temp_tables("jra"),
        output_dir,
        keep_existing=False,
        force_clean=True,
    )
    reader = duckdb.connect(":memory:")
    columns = [
        row[0]
        for row in reader.execute(
            f"describe select * from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')"
        ).fetchall()
    ]
    reader.close()
    assert "kyoso_joken_code" in columns


def test_count_output_rows_counts_written_parquet(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    output_dir = tmp_path / "out"
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_pedigree_stats(seeded_con, "jra")
    subject.materialize_race_context(seeded_con)
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_venue_weather(seeded_con, None, [2020])
    subject.materialize_weather_lookup(seeded_con)
    subject.write_parquet(
        seeded_con,
        subject.assemble_final_select_from_temp_tables("jra"),
        output_dir,
        keep_existing=False,
        force_clean=True,
    )
    assert subject.count_output_rows(output_dir) == 2


def test_resolve_output_rows_skip_count_returns_target_rows(tmp_path: Path):
    args = subject.parse_args(["--skip-count", "--output-dir", str(tmp_path)])
    assert subject.resolve_output_rows(args, 7) == 7


def test_resolve_output_rows_falls_through_to_count(tmp_path: Path):
    args = subject.parse_args(["--output-dir", str(tmp_path / "missing")])
    assert subject.resolve_output_rows(args, 99) == 0


def test_build_empty_result_packs_output_path(tmp_path: Path):
    result = subject.build_empty_result(tmp_path, 1.5)
    assert result["rows_written"] == 0
    assert result["elapsed_seconds"] == 1.5
    assert result["output_dir"] == tmp_path.as_posix()


def test_log_event_emits_json_to_stdout(capsys: pytest.CaptureFixture[str]):
    subject.log_event("stage-x", "done", 1.23, rows=5)
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["stage"] == "stage-x"
    assert payload["rows"] == 5


def test_main_passes_skip_count_to_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    captured: list[argparse.Namespace] = []

    def fake_run(args: argparse.Namespace) -> subject.BuildResult:
        captured.append(args)
        return {"elapsed_seconds": 0.0, "output_dir": str(tmp_path), "rows_written": 0}

    monkeypatch.setattr(subject, "run", fake_run)
    monkeypatch.setattr("builtins.print", lambda _line: None)
    subject.main(
        [
            "--category",
            "jra",
            "--output-dir",
            str(tmp_path),
            "--skip-count",
            "--keep-existing-output",
            "--heartbeat-interval",
            "0",
        ]
    )
    assert captured
    assert captured[0].skip_count is True
    assert captured[0].keep_existing_output is True
    assert captured[0].heartbeat_interval == 0.0


def test_run_returns_empty_result_when_no_years(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    def fake_stage_source(con: duckdb.DuckDBPyConnection, *_args: object) -> None:
        con.execute("create or replace temp table rec (source varchar, race_date varchar)")
        con.execute("create or replace temp table jra_um (ketto_toroku_bango varchar, ketto_joho_01b varchar, ketto_joho_05b varchar)")
        con.execute("create or replace temp table nar_um (ketto_toroku_bango varchar, ketto_joho_01b varchar, ketto_joho_05b varchar)")
        con.execute("create or replace temp table jra_se (kaisai_nen varchar)")
        con.execute("create or replace temp table nar_se (kaisai_nen varchar)")
        con.execute("create or replace temp table jra_ra (kaisai_nen varchar)")
        con.execute("create or replace temp table nar_ra (kaisai_nen varchar)")

    def fake_stage_target(con: duckdb.DuckDBPyConnection, *_args: object) -> int:
        con.execute(
            "create or replace temp table target (race_year integer, kaisai_nen varchar)"
        )
        return 0

    monkeypatch.setattr(subject, "stage_source", fake_stage_source)
    monkeypatch.setattr(subject, "stage_target", fake_stage_target)
    args = subject.parse_args(
        [
            "--category",
            "jra",
            "--from-date",
            "20990101",
            "--to-date",
            "20991231",
            "--output-dir",
            str(tmp_path / "empty"),
            "--heartbeat-interval",
            "0",
        ]
    )
    result = subject.run(args)
    assert result["rows_written"] == 0
    assert result["output_dir"] == (tmp_path / "empty").as_posix()
    assert (tmp_path / "empty").exists()


def test_run_empty_years_cleans_stale_partitions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    def fake_stage_source(con: duckdb.DuckDBPyConnection, *_args: object) -> None:
        con.execute("create or replace temp table rec (source varchar)")
        con.execute("create or replace temp table jra_um (ketto_toroku_bango varchar)")
        con.execute("create or replace temp table nar_um (ketto_toroku_bango varchar)")
        con.execute("create or replace temp table jra_se (kaisai_nen varchar)")
        con.execute("create or replace temp table nar_se (kaisai_nen varchar)")
        con.execute("create or replace temp table jra_ra (kaisai_nen varchar)")
        con.execute("create or replace temp table nar_ra (kaisai_nen varchar)")

    def fake_stage_target(con: duckdb.DuckDBPyConnection, *_args: object) -> int:
        con.execute(
            "create or replace temp table target (race_year integer, kaisai_nen varchar)"
        )
        return 0

    monkeypatch.setattr(subject, "stage_source", fake_stage_source)
    monkeypatch.setattr(subject, "stage_target", fake_stage_target)
    output_dir = tmp_path / "out"
    stale = output_dir / "race_year=2099" / "data_0.parquet"
    stale.parent.mkdir(parents=True)
    stale.write_text("stale")
    args = subject.parse_args(
        [
            "--from-date",
            "20990101",
            "--to-date",
            "20991231",
            "--output-dir",
            str(output_dir),
            "--heartbeat-interval",
            "0",
        ]
    )
    subject.run(args)
    assert not stale.exists()


def test_execute_derived_stage_creates_then_inserts(seeded_con: duckdb.DuckDBPyConnection):
    subject.materialize_horse_history_base(seeded_con, "t.kaisai_nen = '2020'")
    spec: subject.DerivedStageSpec = {
        "name": "horse_career",
        "cte_builder": lambda _filter: subject.horse_career_cte(),
        "final_cte": "horse_career",
    }
    subject.execute_derived_stage(seeded_con, spec, "t.kaisai_nen = '2020'", is_first_year=True)
    row = seeded_con.execute("select count(*) from horse_career").fetchone()
    assert row is not None
    first_count = row[0]
    subject.execute_derived_stage(seeded_con, spec, "t.kaisai_nen = '2020'", is_first_year=False)
    row2 = seeded_con.execute("select count(*) from horse_career").fetchone()
    assert row2 is not None
    assert row2[0] == first_count * 2


def test_stage_target_returns_row_count(seeded_con: duckdb.DuckDBPyConnection):
    rows = subject.stage_target(seeded_con, "jra", "20200101", "20201231")
    assert rows == 2


def test_install_and_attach_pg_raises_on_invalid_url():
    con = duckdb.connect(":memory:")
    with pytest.raises(duckdb.Error):
        subject.install_and_attach_pg(con, "postgresql://bogus:5432/bogus")


def test_run_full_pipeline_with_seeded_sources(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    def fake_stage_source(con: duckdb.DuckDBPyConnection, *_args: object) -> None:
        _seed_rec(con)
        _seed_horse_masters(con)
        _seed_weight_tables(con)
        _seed_weather_tables(con)

    monkeypatch.setattr(subject, "stage_source", fake_stage_source)
    args = subject.parse_args(
        [
            "--category",
            "jra",
            "--from-date",
            "20200101",
            "--to-date",
            "20201231",
            "--output-dir",
            str(tmp_path / "out"),
            "--heartbeat-interval",
            "0",
            "--threads",
            "2",
            "--memory-limit",
            "1GB",
        ]
    )
    result = subject.run(args)
    assert result["rows_written"] == 2
    assert (tmp_path / "out").exists()


def test_run_full_pipeline_with_venue_weather_dir_populates_columns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    def fake_stage_source(con: duckdb.DuckDBPyConnection, *_args: object) -> None:
        _seed_rec(con)
        _seed_horse_masters(con)
        _seed_weight_tables(con)
        _seed_weather_tables(con)

    vw_dir = tmp_path / "venue-weather"
    vw_dir.mkdir()
    _write_venue_weather_db(
        vw_dir / "venue_weather_2020.duckdb",
        [
            ("01", "2020-01-01", 10, 14.0, 2.0, 6.0, 11.0),
            ("01", "2020-01-01", 16, 16.0, 4.0, 8.0, 13.0),
        ],
    )
    monkeypatch.setattr(subject, "stage_source", fake_stage_source)
    args = subject.parse_args(
        [
            "--category",
            "jra",
            "--from-date",
            "20200101",
            "--to-date",
            "20201231",
            "--output-dir",
            str(tmp_path / "out"),
            "--heartbeat-interval",
            "0",
            "--venue-weather-dir",
            str(vw_dir),
        ]
    )
    result = subject.run(args)
    assert result["rows_written"] == 2
    reader = duckdb.connect(":memory:")
    row = reader.execute(
        f"select venue_temperature, venue_precipitation_total from "
        f"read_parquet('{(tmp_path / 'out').as_posix()}/race_year=*/*.parquet') "
        "where ketto_toroku_bango = 'h001'"
    ).fetchone()
    reader.close()
    assert row is not None
    assert row[0] == pytest.approx(15.0)
    assert row[1] == pytest.approx(6.0)


def test_assemble_final_select_emits_race_internal_rank_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    captured_columns: dict[str, list[str]] = {}
    captured_values: dict[str, list[float]] = {}

    def fake_stage_source(con: duckdb.DuckDBPyConnection, *_args: object) -> None:
        _seed_rec(con)
        _seed_horse_masters(con)
        _seed_weight_tables(con)
        _seed_weather_tables(con)

    def capture_parquet(
        con: duckdb.DuckDBPyConnection,
        category: str,
        output_dir: Path,
        keep_existing: bool,
        force_clean: bool,
    ) -> None:
        sql = subject.assemble_final_select_from_temp_tables(category)
        rows = con.execute(f"with final as ({sql}) select * from final").fetchdf()
        captured_columns["names"] = [str(name) for name in rows.columns]
        captured_values["speed_rank"] = [float(value) for value in rows["speed_index_avg_5_rank_in_race"]]
        captured_values["jockey_diff"] = [
            float(value) for value in rows["jockey_recent_win_rate_diff_from_race_avg"]
        ]
        subject.prepare_output_dir(output_dir, keep_existing, force_clean)

    monkeypatch.setattr(subject, "stage_source", fake_stage_source)
    monkeypatch.setattr(subject, "write_parquet", capture_parquet)
    args = subject.parse_args(
        [
            "--category",
            "jra",
            "--from-date",
            "20200101",
            "--to-date",
            "20201231",
            "--output-dir",
            str(tmp_path / "out"),
            "--heartbeat-interval",
            "0",
            "--skip-count",
        ]
    )
    subject.run(args)
    for column in (
        "speed_index_avg_5_rank_in_race",
        "speed_index_best_5_rank_in_race",
        "jockey_recent_win_rate_rank_in_race",
        "trainer_career_win_rate_rank_in_race",
        "pedigree_score_for_race_rank_in_race",
        "same_distance_win_rate_rank_in_race",
        "speed_index_avg_5_diff_from_race_avg",
        "jockey_recent_win_rate_diff_from_race_avg",
        "pedigree_score_diff_from_race_avg",
    ):
        assert column in captured_columns["names"]
    assert sorted(captured_values["speed_rank"]) == [1.0, 2.0]


def test_heartbeat_supports_repeated_stage_changes():
    heartbeat = subject.Heartbeat(0.0, None)
    heartbeat.start()
    heartbeat.set_stage("first")
    heartbeat.set_stage("second")
    heartbeat.set_substage("step")
    heartbeat.stop()
    assert heartbeat.stage == "second"


# ---------------------------------------------------------------------------
# Signal4: sire_keibajo_stats / damsire_keibajo_stats — venue-bucketed
# pedigree win rate computed in DuckDB end-to-end.
# ---------------------------------------------------------------------------


def test_materialize_pedigree_stats_creates_sire_keibajo_stats_table():
    con = duckdb.connect(":memory:")
    rows = [
        ("jra", "20180101", "2018", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 1, 2.0, 1),
        ("jra", "20180201", "2018", "0201", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 1, 2.0, 1),
        ("jra", "20180301", "2018", "0301", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 2, 0.5, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 2, 3.0, 1),
        ("jra", "20180401", "2018", "0401", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 3, 0.7, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 3, 4.0, 1),
        ("jra", "20180501", "2018", "0501", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 4, 0.9, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 4, 5.0, 1),
        ("jra", "20200101", "2020", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 34.0, 0.2, 0.3, 0.4, "1", None, 1, 2.0, 1),
    ]
    df = pd.DataFrame(rows, columns=REC_COLUMNS)
    con.register("rec_keibajo_df", df)
    con.execute(
        """
        create or replace temp table rec as
        select source, race_date,
          strptime(race_date, '%Y%m%d')::date as race_dt,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban,
          kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code,
          shusso_tosu, finish_position,
          cast(finish_norm as double) as finish_norm,
          time_sa, kohan_3f, corner1_norm, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, seibetsu_code
        from rec_keibajo_df
        """
    )
    _seed_horse_masters(con)
    _seed_weight_tables(con)
    _seed_weather_tables(con)
    subject.build_target_table(con, "jra", "20200101", "20201231")
    subject.materialize_pedigree_stats(con, "jra")
    row = con.execute(
        "select sire, keibajo_code, sire_keibajo_win_rate_val, race_count "
        "from sire_keibajo_stats where stats_year_month = 202001"
    ).fetchone()
    assert row is not None
    assert row[0] == "s001"
    assert row[1] == "01"
    assert row[2] == pytest.approx(0.4)
    assert row[3] == 5


def test_materialize_pedigree_stats_computes_damsire_keibajo_win_rate():
    con = duckdb.connect(":memory:")
    rows = [
        ("jra", "20180101", "2018", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 1, 2.0, 1),
        ("jra", "20180201", "2018", "0201", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 1, 2.0, 1),
        ("jra", "20180301", "2018", "0301", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 2, 0.5, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 2, 3.0, 1),
        ("jra", "20180401", "2018", "0401", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 3, 0.7, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 3, 4.0, 1),
        ("jra", "20180501", "2018", "0501", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 4, 0.9, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 4, 5.0, 1),
        ("jra", "20200101", "2020", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 34.0, 0.2, 0.3, 0.4, "1", None, 1, 2.0, 1),
    ]
    df = pd.DataFrame(rows, columns=REC_COLUMNS)
    con.register("rec_damsire_keibajo_df", df)
    con.execute(
        """
        create or replace temp table rec as
        select source, race_date,
          strptime(race_date, '%Y%m%d')::date as race_dt,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban,
          kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code,
          shusso_tosu, finish_position,
          cast(finish_norm as double) as finish_norm,
          time_sa, kohan_3f, corner1_norm, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, seibetsu_code
        from rec_damsire_keibajo_df
        """
    )
    _seed_horse_masters(con)
    _seed_weight_tables(con)
    _seed_weather_tables(con)
    subject.build_target_table(con, "jra", "20200101", "20201231")
    subject.materialize_pedigree_stats(con, "jra")
    row = con.execute(
        "select damsire, keibajo_code, damsire_keibajo_win_rate_val, race_count "
        "from damsire_keibajo_stats where stats_year_month = 202001"
    ).fetchone()
    assert row is not None
    assert row[0] == "d001"
    assert row[1] == "01"
    assert row[2] == pytest.approx(0.4)
    assert row[3] == 5


def test_keibajo_win_rate_null_when_race_count_below_min_races():
    con = duckdb.connect(":memory:")
    # Only 3 history races at keibajo 01 (< PEDIGREE_MIN_RACES=5) → final value NULL.
    rows = [
        ("jra", "20180101", "2018", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 1, 2.0, 1),
        ("jra", "20180201", "2018", "0201", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 1, 2.0, 1),
        ("jra", "20180301", "2018", "0301", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 2, 0.5, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 2, 3.0, 1),
        ("jra", "20200101", "2020", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 34.0, 0.2, 0.3, 0.4, "1", None, 1, 2.0, 1),
    ]
    df = pd.DataFrame(rows, columns=REC_COLUMNS)
    con.register("rec_below_min_df", df)
    con.execute(
        """
        create or replace temp table rec as
        select source, race_date,
          strptime(race_date, '%Y%m%d')::date as race_dt,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban,
          kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code,
          shusso_tosu, finish_position,
          cast(finish_norm as double) as finish_norm,
          time_sa, kohan_3f, corner1_norm, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, seibetsu_code
        from rec_below_min_df
        """
    )
    _seed_horse_masters(con)
    _seed_weight_tables(con)
    _seed_weather_tables(con)
    subject.build_target_table(con, "jra", "20200101", "20201231")
    subject.materialize_se_lookup(con)
    subject.stage_horse_history_derived(con, [2020], subject.Heartbeat(0.0, None))
    subject.stage_partner_features(con, [2020], subject.Heartbeat(0.0, None))
    subject.materialize_pedigree_stats(con, "jra")
    subject.materialize_race_context(con)
    subject.stage_track_bias(con, [2020], subject.Heartbeat(0.0, None))
    subject.materialize_venue_weather(con, None, [2020])
    subject.materialize_weather_lookup(con)
    base_sql = subject.base_features_select_sql("jra")
    row = con.execute(
        f"with final as ({base_sql}) "
        "select sire_keibajo_win_rate, damsire_keibajo_win_rate from final"
    ).fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1] is None


def test_keibajo_win_rate_present_in_final_output_value():
    con = duckdb.connect(":memory:")
    # 5 history races at keibajo 01, 2 wins → win rate 0.4 surfaces in final output.
    rows = [
        ("jra", "20180101", "2018", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 1, 2.0, 1),
        ("jra", "20180201", "2018", "0201", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 1, 2.0, 1),
        ("jra", "20180301", "2018", "0301", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 2, 0.5, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 2, 3.0, 1),
        ("jra", "20180401", "2018", "0401", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 3, 0.7, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 3, 4.0, 1),
        ("jra", "20180501", "2018", "0501", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 4, 0.9, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 4, 5.0, 1),
        ("jra", "20200101", "2020", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 1.0, 34.0, 0.2, 0.3, 0.4, "1", None, 1, 2.0, 1),
    ]
    df = pd.DataFrame(rows, columns=REC_COLUMNS)
    con.register("rec_value_df", df)
    con.execute(
        """
        create or replace temp table rec as
        select source, race_date,
          strptime(race_date, '%Y%m%d')::date as race_dt,
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban,
          kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code,
          shusso_tosu, finish_position,
          cast(finish_norm as double) as finish_norm,
          time_sa, kohan_3f, corner1_norm, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, seibetsu_code
        from rec_value_df
        """
    )
    _seed_horse_masters(con)
    _seed_weight_tables(con)
    _seed_weather_tables(con)
    subject.build_target_table(con, "jra", "20200101", "20201231")
    subject.materialize_se_lookup(con)
    subject.stage_horse_history_derived(con, [2020], subject.Heartbeat(0.0, None))
    subject.stage_partner_features(con, [2020], subject.Heartbeat(0.0, None))
    subject.materialize_pedigree_stats(con, "jra")
    subject.materialize_race_context(con)
    subject.stage_track_bias(con, [2020], subject.Heartbeat(0.0, None))
    subject.materialize_venue_weather(con, None, [2020])
    subject.materialize_weather_lookup(con)
    base_sql = subject.base_features_select_sql("jra")
    row = con.execute(
        f"with final as ({base_sql}) "
        "select sire_keibajo_win_rate, damsire_keibajo_win_rate from final"
    ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(0.4)
    assert row[1] == pytest.approx(0.4)


def test_write_parquet_emits_keibajo_win_rate_columns(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    output_dir = tmp_path / "out"
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_pedigree_stats(seeded_con, "jra")
    subject.materialize_race_context(seeded_con)
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_venue_weather(seeded_con, None, [2020])
    subject.materialize_weather_lookup(seeded_con)
    subject.write_parquet(
        seeded_con,
        subject.assemble_final_select_from_temp_tables("jra"),
        output_dir,
        keep_existing=False,
        force_clean=True,
    )
    reader = duckdb.connect(":memory:")
    columns = [
        row[0]
        for row in reader.execute(
            f"describe select * from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')"
        ).fetchall()
    ]
    reader.close()
    assert "sire_keibajo_win_rate" in columns
    assert "damsire_keibajo_win_rate" in columns


def _active_controller(tmp_path: Path, incremental: bool = False) -> subject.CheckpointController:
    return subject.CheckpointController(
        active=True,
        incremental=incremental,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=tmp_path / "table_spill",
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )


def test_run_stage_horse_history_active_spills_and_records(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    controller = _active_controller(tmp_path)
    subject.run_stage_horse_history(
        seeded_con, controller, _silent_heartbeat(), [2020], "jra", tmp_path
    )
    manifest = subject.CheckpointManifest.load(tmp_path)
    assert manifest is not None
    assert manifest.stages["horse_history_derived"].tables == [
        "horse_career.parquet",
        "weight_agg.parquet",
        "recent_form.parquet",
        "legacy_features.parquet",
        "horse_running_style_history.parquet",
    ]
    view_type = seeded_con.execute(
        "select table_type from information_schema.tables where table_name = 'horse_career'"
    ).fetchone()
    assert view_type == ("VIEW",)


def test_run_stage_horse_history_active_restores_from_checkpoint(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    controller = _active_controller(tmp_path)
    subject.run_stage_horse_history(
        seeded_con, controller, _silent_heartbeat(), [2020], "jra", tmp_path
    )
    expected = seeded_con.execute("select count(*) from horse_career").fetchone()
    fresh_con = duckdb.connect(":memory:")
    restored_controller = _active_controller(tmp_path)
    restored_controller.manifest = cast(
        subject.CheckpointManifest, subject.CheckpointManifest.load(tmp_path)
    )
    subject.run_stage_horse_history(
        fresh_con, restored_controller, _silent_heartbeat(), [2020], "jra", tmp_path
    )
    restored = fresh_con.execute("select count(*) from horse_career").fetchone()
    fresh_con.close()
    assert restored == expected


def test_run_stage_partner_active_spills_and_records(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat(), "jra")
    controller = _active_controller(tmp_path)
    subject.run_stage_partner(seeded_con, controller, _silent_heartbeat(), [2020], tmp_path)
    manifest = subject.CheckpointManifest.load(tmp_path)
    assert manifest is not None
    assert manifest.stages["partner_features"].tables == [
        "jockey_career.parquet",
        "trainer_career.parquet",
    ]


def test_run_stage_partner_active_restores_from_checkpoint(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat(), "jra")
    controller = _active_controller(tmp_path)
    subject.run_stage_partner(seeded_con, controller, _silent_heartbeat(), [2020], tmp_path)
    fresh_con = duckdb.connect(":memory:")
    restored_controller = _active_controller(tmp_path)
    restored_controller.manifest = cast(
        subject.CheckpointManifest, subject.CheckpointManifest.load(tmp_path)
    )
    subject.run_stage_partner(
        fresh_con, restored_controller, _silent_heartbeat(), [2020], tmp_path
    )
    jockey_rows = fresh_con.execute(
        "select table_type from information_schema.tables where table_name = 'jockey_career'"
    ).fetchone()
    fresh_con.close()
    assert jockey_rows == ("VIEW",)


def test_run_stage_pedigree_active_spills_and_records(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    controller = _active_controller(tmp_path)
    subject.run_stage_pedigree(
        seeded_con, controller, _silent_heartbeat(), [2020], "jra", tmp_path
    )
    manifest = subject.CheckpointManifest.load(tmp_path)
    assert manifest is not None
    assert "target_pedigree.parquet" in manifest.stages["pedigree"].tables


def test_run_stage_race_context_active_records_then_restores(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat(), "jra")
    controller = _active_controller(tmp_path)
    subject.run_stage_race_context(seeded_con, controller, _silent_heartbeat(), [2020], tmp_path)
    fresh_con = duckdb.connect(":memory:")
    restored_controller = _active_controller(tmp_path)
    restored_controller.manifest = cast(
        subject.CheckpointManifest, subject.CheckpointManifest.load(tmp_path)
    )
    subject.run_stage_race_context(
        fresh_con, restored_controller, _silent_heartbeat(), [2020], tmp_path
    )
    aggregates = fresh_con.execute(
        "select table_type from information_schema.tables where table_name = 'race_field_aggregates'"
    ).fetchone()
    fresh_con.close()
    assert aggregates == ("VIEW",)


def test_run_stage_track_bias_active_spills_and_records(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    controller = _active_controller(tmp_path)
    subject.run_stage_track_bias(seeded_con, controller, _silent_heartbeat(), [2020], tmp_path)
    manifest = subject.CheckpointManifest.load(tmp_path)
    assert manifest is not None
    assert manifest.stages["track_bias"].tables == ["track_bias.parquet"]


def test_run_stage_weather_active_spills_and_records(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    controller = _active_controller(tmp_path)
    subject.run_stage_weather(
        seeded_con, controller, _silent_heartbeat(), [2020], None, tmp_path
    )
    manifest = subject.CheckpointManifest.load(tmp_path)
    assert manifest is not None
    assert manifest.stages["weather_lookup"].tables == ["weather_lookup.parquet"]


def test_run_stage_weather_active_restore_drops_rec_view(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    controller = _active_controller(tmp_path)
    subject.run_stage_weather(
        seeded_con, controller, _silent_heartbeat(), [2020], None, tmp_path
    )
    fresh_con = duckdb.connect(":memory:")
    fresh_con.execute("create table rec as select 1 as x")
    restored_controller = _active_controller(tmp_path)
    restored_controller.manifest = cast(
        subject.CheckpointManifest, subject.CheckpointManifest.load(tmp_path)
    )
    subject.run_stage_weather(
        fresh_con, restored_controller, _silent_heartbeat(), [2020], None, tmp_path
    )
    rec_present = fresh_con.execute(
        "select count(*) from information_schema.tables where table_name = 'rec'"
    ).fetchone()
    weather_type = fresh_con.execute(
        "select table_type from information_schema.tables where table_name = 'weather_lookup'"
    ).fetchone()
    fresh_con.close()
    assert rec_present == (0,)
    assert weather_type == ("VIEW",)


def test_run_resume_skips_recompute_on_second_pass(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_stage_source(con: duckdb.DuckDBPyConnection, *_args: object) -> None:
        _seed_rec(con)
        _seed_horse_masters(con)
        _seed_weight_tables(con)
        _seed_weather_tables(con)
        con.execute(
            "create or replace temp table nar_nu as "
            "select ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b from nar_um"
        )

    monkeypatch.setattr(subject, "stage_source", fake_stage_source)
    base_args = [
        "--category",
        "jra",
        "--from-date",
        "20200101",
        "--to-date",
        "20201231",
        "--output-dir",
        str(tmp_path / "out"),
        "--temp-dir",
        str(tmp_path / "tmp"),
        "--skip-count",
        "--resume",
    ]
    first = subject.run(subject.parse_args(base_args))
    assert first["rows_written"] > 0
    manifest = subject.CheckpointManifest.load(tmp_path / "tmp")
    assert manifest is not None
    assert "weather_lookup" in manifest.stages

    def exploding_stage_source(_con: duckdb.DuckDBPyConnection, *_args: object) -> None:
        raise AssertionError("source stage must not run again on resume")

    monkeypatch.setattr(subject, "stage_source", exploding_stage_source)
    second = subject.run(subject.parse_args(base_args))
    assert second["rows_written"] == first["rows_written"]


def test_base_features_select_emits_weather_interaction_values(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    _write_venue_weather_db(
        tmp_path / "venue_weather_2020.duckdb",
        [
            ("01", "2020-01-01", 10, 12.0, 2.0, 6.0, 10.0),
            ("01", "2020-01-01", 15, 18.0, 4.0, 8.0, 13.0),
        ],
    )
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_pedigree_stats(seeded_con, "jra")
    subject.materialize_race_context(seeded_con)
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_venue_weather(seeded_con, tmp_path, [2020])
    subject.materialize_weather_lookup(seeded_con)
    base_sql = subject.base_features_select_sql("jra")
    row = seeded_con.execute(
        f"with final as ({base_sql}) "
        "select rain_x_speed_decay, wind_x_field_size, cold_x_speed_effect, "
        "rain_x_track_condition, speed_index_avg_5 from final "
        "where ketto_toroku_bango = 'h001'"
    ).fetchone()
    assert row is not None
    speed = row[4]
    assert speed is not None
    assert row[0] == pytest.approx(6.0 * speed)
    assert row[1] == pytest.approx(8.0 * (10.0 / subject.MAX_FIELD_SIZE))
    assert row[2] == pytest.approx((20.0 - 15.0) * speed)
    assert row[3] == pytest.approx(0.0)


def test_base_features_pedigree_and_style_interactions_compute_and_null_guard(
    seeded_con: duckdb.DuckDBPyConnection,
):
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_pedigree_stats(seeded_con, "jra")
    subject.materialize_race_context(seeded_con)
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_venue_weather(seeded_con, None, [2020])
    subject.materialize_weather_lookup(seeded_con)
    # Override pedigree stats so the min-races guard passes with known values.
    # h001 links to sire s001 / keibajo '01' / kyori_band 4 / rs_bucket 0 / 202001.
    seeded_con.execute(
        "create or replace temp table sire_keibajo_stats as select * from (values "
        f"('s001', '01', 202001, 0.4, {subject.PEDIGREE_MIN_RACES})) "
        "as t(sire, keibajo_code, stats_year_month, sire_keibajo_win_rate_val, race_count)"
    )
    seeded_con.execute(
        "create or replace temp table sire_distance_stats as select * from (values "
        f"('s001', 4, 202001, 0.5, 2.0, {subject.PEDIGREE_MIN_RACES})) "
        "as t(sire, kyori_band, stats_year_month, sire_distance_win_rate_val, "
        "sire_avg_finish_at_distance_val, race_count)"
    )
    seeded_con.execute(
        "create or replace temp table sire_running_style_stats as select * from (values "
        f"('s001', 0, 202001, 0.2, 0.3, 0.4, 0.1, 0.5, {subject.PEDIGREE_MIN_RACES})) "
        "as t(sire, rs_bucket, stats_year_month, sire_nige_rate_val, sire_senkou_rate_val, "
        "sire_sashi_rate_val, sire_oikomi_rate_val, sire_corner_1_norm_avg_val, race_count)"
    )
    # h001 has past_nige_rate_self=0.0 (not null) → dot product branch taken.
    # h002 set to NULL nige rate → null-guard branch returns NULL.
    seeded_con.execute(
        "update horse_running_style_history set "
        "past_nige_rate_self = 0.5, past_senkou_rate_self = 0.3, "
        "past_sashi_rate_self = 0.1, past_oikomi_rate_self = 0.1 "
        "where ketto_toroku_bango = 'h001'"
    )
    seeded_con.execute(
        "update horse_running_style_history set "
        "past_nige_rate_self = NULL, past_senkou_rate_self = NULL, "
        "past_sashi_rate_self = NULL, past_oikomi_rate_self = NULL "
        "where ketto_toroku_bango = 'h002'"
    )
    base_sql = subject.base_features_select_sql("jra")
    rows = seeded_con.execute(
        f"with final as ({base_sql}) "
        "select ketto_toroku_bango, pedigree_venue_x_horse_venue, "
        "pedigree_distance_x_horse_distance, sire_style_x_horse_style_match "
        "from final where ketto_toroku_bango in ('h001', 'h002') "
        "order by ketto_toroku_bango"
    ).fetchall()
    horse_with_style = rows[0]
    horse_without_style = rows[1]
    assert horse_with_style[0] == "h001"
    assert horse_with_style[1] == pytest.approx(0.4 * 0.5)
    assert horse_with_style[2] == pytest.approx(0.5 * 0.5)
    assert horse_with_style[3] == pytest.approx(
        0.5 * 0.2 + 0.3 * 0.3 + 0.1 * 0.4 + 0.1 * 0.1
    )
    assert horse_without_style[0] == "h002"
    assert horse_without_style[3] is None
