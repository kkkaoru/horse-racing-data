from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-workout-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_workout_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_workout_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_accepts_target_race(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--target-race",
            "10:02",
        ]
    )
    assert args.target_race == "10:02"


def test_stage_workout_raw_pushes_exact_horses_and_date_bounds_to_source() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    scope = subject.WorkoutScope(
        history_floor="20221003",
        history_ceiling="20230101",
        horse_ids=("horse-1", "horse'2"),
    )
    subject.stage_workout_raw(FakeConn(), scope)
    body = " ".join(captured)
    assert "from pg.jvd_hc" in body
    assert "chokyo_nengappi >= '20221003'" in body
    assert "chokyo_nengappi < '20230101'" in body
    assert "ketto_toroku_bango in ('horse-1', 'horse''2')" in body


def test_stage_workout_raw_empty_scope_avoids_source_rows() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_workout_raw(FakeConn(), None)
    body = " ".join(captured)
    assert "base_parquet" not in body
    assert "where false" in body


def test_stage_workout_raw_excludes_other_horses_and_out_of_window_rows() -> None:
    with duckdb.connect(":memory:") as con:
        con.execute(
            """
            create schema pg;
            create table pg.jvd_hc (
              ketto_toroku_bango varchar, chokyo_nengappi varchar,
              tracen_kubun varchar, lap_time_1f varchar, lap_time_2f varchar,
              lap_time_3f varchar, lap_time_4f varchar, time_gokei_4f varchar,
              time_gokei_3f varchar, time_gokei_2f varchar
            );
            insert into pg.jvd_hc values
              ('horse-1', '20221231', '1', '121', '250', '380', '510', '510', '380', '250'),
              ('horse-1', '20220901', '1', '122', '251', '381', '511', '511', '381', '251'),
              ('horse-2', '20221231', '1', '123', '252', '382', '512', '512', '382', '252'),
              ('horse-1', '20230101', '1', '124', '253', '383', '513', '513', '383', '253')
            """
        )
        subject.stage_workout_raw(
            con,
            subject.WorkoutScope(
                history_floor="20221003",
                history_ceiling="20230101",
                horse_ids=("horse-1",),
            ),
        )
        rows = con.execute(
            "select ketto_toroku_bango, chokyo_nengappi, lap_1f from workout_raw"
        ).fetchall()
        assert rows == [("horse-1", "20221231", 12.1)]


def test_workout_scope_uses_input_horses_and_exact_lookback() -> None:
    with duckdb.connect(":memory:") as con:
        con.execute(
            """
            create table base_parquet (race_date varchar, ketto_toroku_bango varchar);
            insert into base_parquet values
              ('20230101', 'horse-2'),
              ('20230102', 'horse-1'),
              ('20230102', 'horse-2'),
              (null, 'ignored')
            """
        )
        assert subject.workout_scope(con) == subject.WorkoutScope(
            history_floor="20221003",
            history_ceiling="20230102",
            horse_ids=("horse-1", "horse-2"),
        )


def test_workout_scope_returns_none_for_empty_input() -> None:
    with duckdb.connect(":memory:") as con:
        con.execute(
            "create table base_parquet (race_date varchar, ketto_toroku_bango varchar)"
        )
        assert subject.workout_scope(con) is None


def test_stage_workout_agg_counts_zero_when_horse_has_no_workout() -> None:
    with duckdb.connect(":memory:") as con:
        con.execute(
            """
            create table base_parquet (
              source varchar, race_date varchar, kaisai_nen varchar,
              kaisai_tsukihi varchar, keibajo_code varchar, race_bango varchar,
              ketto_toroku_bango varchar
            );
            insert into base_parquet values
              ('jra', '20230101', '2023', '0101', '05', '01', 'horse-1');
            create table workout_raw (
              ketto_toroku_bango varchar, workout_dt date, lap_1f double,
              lap_2f double, lap_3f double, lap_4f double, gokei_4f double,
              gokei_3f double, gokei_2f double, tracen_kubun varchar
            )
            """
        )
        subject.stage_workout_agg(con)
        row = con.execute(
            """
            select coalesce(a.workout_count_recent, 0),
              coalesce(a.workout_count_30d, 0)
            from base_parquet b
            left join workout_agg a using (
              source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
              ketto_toroku_bango
            )
            """
        ).fetchone()
        assert row == (0, 0)


def test_append_features_reuses_materialized_base_table() -> None:
    sql = subject.append_features_sql()
    assert "from base_parquet b" in sql
    assert "read_parquet" not in sql
