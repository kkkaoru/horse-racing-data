from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-sectional-and-weight-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location(
    "add_sectional_and_weight_features", MODULE_PATH
)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_sectional_and_weight_features"] = subject
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


def test_stage_history_focused_filters_bataiju_and_rec_to_base_horses() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_history(FakeConn(), "20230101", "20231231", focused_target=True)
    body = " ".join(captured)
    assert "from pg.jvd_se" in body
    assert "from pg.race_entry_corner_features rec" in body
    assert (
        "ketto_toroku_bango in (select distinct ketto_toroku_bango from base_v3)"
        in body
    )
    assert (
        "rec.ketto_toroku_bango in (select distinct ketto_toroku_bango from base_v3)"
        in body
    )


def test_stage_history_default_does_not_require_base_v3() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_history(FakeConn(), "20230101", "20231231")
    body = " ".join(captured)
    assert "base_v3" not in body


def test_stage_history_pushes_literal_horses_and_year_partitions() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_history(
        FakeConn(),
        "20230101",
        "20241231",
        horse_literals="'horse_a', 'horse_b'",
    )
    body = " ".join(captured)
    assert "ketto_toroku_bango in ('horse_a', 'horse_b')" in body
    assert "rec.ketto_toroku_bango in ('horse_a', 'horse_b')" in body
    assert (
        "(try_cast(nullif(trim(cast(rec.soha_time as varchar)), '0000') as bigint) // 1000) * 600"
        in body
    )
    assert "kaisai_nen between '2023' and '2024'" in body
    assert "rec.kaisai_nen between '2023' and '2024'" in body


def test_target_horse_literals_escapes_and_handles_empty() -> None:
    class Result:
        rows: list[tuple[str]]

        def __init__(self, rows: list[tuple[str]]) -> None:
            self.rows = rows

        def fetchall(self) -> list[tuple[str]]:
            return self.rows

    class FakeConn:
        rows: list[tuple[str]]

        def __init__(self, rows: list[tuple[str]]) -> None:
            self.rows = rows

        def execute(self, _sql: str) -> Result:
            return Result(self.rows)

    assert (
        subject.target_horse_literals(FakeConn([("a'b",), ("horse_c",)]))
        == "'a''b', 'horse_c'"
    )
    assert subject.target_horse_literals(FakeConn([])) == "null"


def test_stage_race_time_context_uses_same_race_zscore_and_prior_day_par() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create table rec_hist (
          source varchar, race_date varchar, keibajo_code varchar, race_bango varchar,
          ketto_toroku_bango varchar, track_code varchar, track_condition varchar,
          soha_time double, kyori int, bataiju double
        )
        """
    )
    con.execute(
        """
        insert into rec_hist values
          ('jra', '20240101', '06', '01', 'h1', '24', '1', 600, 1000, 470),
          ('jra', '20240101', '06', '01', 'h2', '24', '1', 620, 1000, 480),
          ('jra', '20240102', '06', '01', 'h1', '24', '1', 610, 1000, 471),
          ('jra', '20240102', '06', '01', 'h2', '24', '1', 630, 1000, 481)
        """
    )
    subject.stage_race_time_context(con)

    row = con.execute(
        """
        select
          min(prior_condition_time_per_meter) filter (where race_date = '20240101'),
          min(prior_condition_time_per_meter) filter (where race_date = '20240102'),
          min(race_relative_time_z) filter (
            where race_date = '20240102' and ketto_toroku_bango = 'h1'
          ),
          min(prior_condition_time_residual) filter (
            where race_date = '20240102' and ketto_toroku_bango = 'h1'
          )
        from rec_hist_context
        left join condition_day_par
          using (source, race_date, keibajo_code, track_code, track_condition, kyori)
        """
    ).fetchone()
    assert row is not None
    first_par, second_par, relative_z, residual = row

    assert first_par is None
    assert second_par == pytest.approx(0.61)
    assert relative_z == pytest.approx(-1.0)
    assert residual == pytest.approx(0.0)


def test_append_features_sql_includes_relative_and_par_time_features() -> None:
    sql = subject.append_features_sql("/tmp/input/race_year=*/*.parquet")
    assert "hive_partitioning=true" in sql
    assert "recent_race_relative_time_z_avg5" in sql
    assert "recent_condition_par_time_residual_avg5" in sql


def test_append_features_sql_does_not_duplicate_existing_features() -> None:
    sql = subject.append_features_sql(
        "/tmp/input/race_year=*/*.parquet",
        {"weight_trend_5", "weight_volatility_5"},
    )
    assert "h.weight_trend_5" not in sql
    assert "h.weight_volatility_5" not in sql
    assert "h.recent_soha_time_per_meter_avg5" in sql
