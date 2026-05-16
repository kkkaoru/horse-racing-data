from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

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
]

REC_DATA: list[tuple[object, ...]] = [
    ("jra", "20180101", "2018", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 2, 0.5, 1.0, 35.0, 0.3, 0.4, 0.5, "1", None, 2, 5.0),
    ("jra", "20180601", "2018", "0601", "02", "01", "h001", 2, "j1", "t1", 1800, "11", "A", "005", 12, 1, 0.0, 0.0, 34.5, 0.2, 0.3, 0.4, "1", None, 1, 2.5),
    ("jra", "20190101", "2019", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 0.0, 34.0, 0.2, 0.3, 0.4, "1", None, 1, 2.0),
    ("jra", "20190101", "2019", "0101", "01", "01", "h002", 2, "j2", "t1", 1600, "10", "A", "000", 10, 2, 0.5, 1.0, 34.5, 0.3, 0.4, 0.5, "1", None, 2, 3.0),
    ("jra", "20190601", "2019", "0601", "02", "01", "h001", 1, "j1", "t1", 1800, "11", "A", "005", 12, 3, 0.7, 1.5, 36.0, 0.3, 0.4, 0.5, "1", None, 3, 4.0),
    ("jra", "20190601", "2019", "0601", "02", "01", "h002", 2, "j2", "t2", 1800, "11", "A", "005", 12, 1, 0.0, 0.0, 35.5, 0.2, 0.3, 0.4, "1", None, 1, 2.0),
    ("jra", "20200101", "2020", "0101", "01", "01", "h001", 1, "j1", "t1", 1600, "10", "A", "000", 10, 1, 0.0, 0.0, 34.0, 0.2, 0.3, 0.4, "1", None, 1, 2.0),
    ("jra", "20200101", "2020", "0101", "01", "01", "h002", 2, "j2", "t1", 1600, "10", "A", "000", 10, 2, 0.5, 1.0, 34.5, 0.3, 0.4, 0.5, "1", None, 2, 3.0),
    ("nar", "20190601", "2019", "0601", "01", "02", "h003", 1, "j3", "t3", 1200, "20", "B", "000", 8, 1, 0.0, 0.0, 25.5, 0.3, 0.4, 0.5, None, "1", 1, 2.0),
    ("nar", "20200601", "2020", "0601", "01", "02", "h003", 1, "j3", "t3", 1200, "20", "B", "000", 8, 1, 0.0, 0.0, 25.0, 0.3, 0.4, 0.5, None, "1", 1, 2.0),
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
          tansho_ninkijun, tansho_odds
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
            ("2018", "0101", "01", "01", "h001", "480"),
            ("2018", "0601", "02", "01", "h001", "478"),
            ("2019", "0101", "01", "01", "h001", "482"),
            ("2019", "0101", "01", "01", "h002", "470"),
            ("2019", "0601", "02", "01", "h001", "484"),
            ("2019", "0601", "02", "01", "h002", "472"),
            ("2020", "0101", "01", "01", "h001", "486"),
            ("2020", "0101", "01", "01", "h002", "474"),
        ],
        columns=["kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango", "ketto_toroku_bango", "bataiju"],
    )
    nar_se = pd.DataFrame(
        [
            ("2019", "0601", "01", "02", "h003", "440"),
            ("2020", "0601", "01", "02", "h003", "442"),
        ],
        columns=["kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango", "ketto_toroku_bango", "bataiju"],
    )
    con.register("jra_se_df", jra_se)
    con.register("nar_se_df", nar_se)
    con.execute("create or replace temp table jra_se as select * from jra_se_df")
    con.execute("create or replace temp table nar_se as select * from nar_se_df")


def _seed_weather_tables(con: duckdb.DuckDBPyConnection) -> None:
    jra_ra = pd.DataFrame(
        [
            ("2020", "0101", "01", "01", "1"),
        ],
        columns=["kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango", "tenko_code"],
    )
    nar_ra = pd.DataFrame(
        [
            ("2020", "0601", "01", "02", "2"),
        ],
        columns=["kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango", "tenko_code"],
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




def test_materialize_race_context_builds_aggregates(seeded_con: duckdb.DuckDBPyConnection):
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_race_context(seeded_con)
    row = seeded_con.execute("select count(*) from race_field_aggregates").fetchone()
    assert row is not None
    assert row[0] > 0


def test_materialize_weather_lookup_joins_jra_ra(seeded_con: duckdb.DuckDBPyConnection):
    subject.materialize_weather_lookup(seeded_con)
    row = seeded_con.execute(
        "select tenko_code from weather_lookup where ketto_toroku_bango = 'h001'"
    ).fetchone()
    assert row is not None
    assert row[0] == "1"


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


def test_count_output_rows_counts_written_parquet(
    seeded_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    output_dir = tmp_path / "out"
    subject.stage_horse_history_derived(seeded_con, [2020], _silent_heartbeat())
    subject.stage_partner_features(seeded_con, [2020], _silent_heartbeat())
    subject.materialize_pedigree_stats(seeded_con, "jra")
    subject.materialize_race_context(seeded_con)
    subject.stage_track_bias(seeded_con, [2020], _silent_heartbeat())
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


def test_heartbeat_supports_repeated_stage_changes():
    heartbeat = subject.Heartbeat(0.0, None)
    heartbeat.start()
    heartbeat.set_stage("first")
    heartbeat.set_stage("second")
    heartbeat.set_substage("step")
    heartbeat.stop()
    assert heartbeat.stage == "second"
