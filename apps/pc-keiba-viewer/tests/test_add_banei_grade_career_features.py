from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-banei-grade-career-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_banei_grade_career_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_banei_grade_career_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_banei_keibajo_constant() -> None:
    assert subject.BAN_EI_KEIBAJO == "83"


def test_grade_rank_sql_orders_p_highest() -> None:
    sql = subject.GRADE_RANK_SQL
    assert "'P' then 6" in sql
    assert "'Q' then 5" in sql
    assert "'E' then 1" in sql


def test_append_features_sql_pivots_six_grades() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    for letter in ("E", "T", "S", "R", "Q", "P"):
        assert f"horse_grade_{letter}_career_starts" in sql
        assert f"horse_grade_{letter}_career_win_rate" in sql


def test_append_features_sql_includes_current_grade_aggregates() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "current_race_grade_letter" in sql
    assert "horse_current_grade_career_win_rate" in sql
    assert "horse_current_grade_career_starts" in sql
    assert "horse_career_starts_minus_field" in sql
    assert "field_avg_career_starts" in sql


def test_target_grade_careers_align_latest_strictly_prior_rows() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create table banei_targets as
        select 'nar'::varchar as source, '2026'::varchar as kaisai_nen,
          '0826'::varchar as kaisai_tsukihi, '83'::varchar as keibajo_code,
          '01'::varchar as race_bango, 'HORSE'::varchar as ketto_toroku_bango,
          '20260826'::varchar as race_date
        """
    )
    con.execute(
        """
        create table current_race_grade as
        select 'nar'::varchar as source, '2026'::varchar as kaisai_nen,
          '0826'::varchar as kaisai_tsukihi, '83'::varchar as keibajo_code,
          '01'::varchar as race_bango, 'E'::varchar as grade_letter,
          1::integer as grade_rank
        """
    )
    con.execute(
        """
        create table horse_grade_cumul as
        select 'nar'::varchar as source, 'HORSE'::varchar as ketto_toroku_bango,
          'E'::varchar as grade_letter, 1::integer as grade_rank,
          '20260820'::varchar as race_date, 4::hugeint as past_starts,
          2::hugeint as past_wins
        """
    )
    con.execute(
        """
        create table horse_grade_total_career as
        select 'nar'::varchar as source, 'HORSE'::varchar as ketto_toroku_bango,
          '20260820'::varchar as race_date, 7::hugeint as total_past_starts,
          3::hugeint as total_past_wins
        """
    )

    subject.stage_field_career_avg(con)
    subject.stage_target_grade_careers(con)

    grade_row = con.execute(
        "select e_starts, e_wins from target_grade_career"
    ).fetchone()
    current_row = con.execute(
        "select past_starts, past_wins from target_current_grade_career"
    ).fetchone()
    total_row = con.execute(
        "select total_past_starts, total_past_wins from target_total_career"
    ).fetchone()
    field_row = con.execute(
        "select field_avg_career_starts from banei_field_career_avg"
    ).fetchone()
    assert grade_row == (4, 2)
    assert current_row == (4, 2)
    assert total_row == (7, 3)
    assert field_row == (7.0,)
    con.close()


def test_grade_staging_scopes_history_and_aligns_upcoming_race() -> None:
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        "create table pg.nvd_se (kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar, kakutei_chakujun varchar)"
    )
    con.execute(
        "create table pg.nvd_ra (kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar, race_bango varchar, grade_code varchar)"
    )
    con.execute(
        """
        create table banei_targets as
        select 'nar'::varchar as source, '2026'::varchar as kaisai_nen,
          '0826'::varchar as kaisai_tsukihi, '83'::varchar as keibajo_code,
          '01'::varchar as race_bango, 'HORSE'::varchar as ketto_toroku_bango,
          '20260826'::varchar as race_date
        """
    )
    con.execute(
        "insert into pg.nvd_se values ('2026', '0820', '83', '01', 'HORSE', '01')"
    )
    con.execute(
        "insert into pg.nvd_se values ('2026', '0820', '83', '01', 'OTHER', '01')"
    )
    con.execute("insert into pg.nvd_ra values ('2026', '0820', '83', '01', 'E')")
    con.execute("insert into pg.nvd_ra values ('2026', '0826', '83', '01', 'Q')")

    subject.stage_banei_grade_history(con, "20200101")
    subject.stage_horse_grade_cumul(con)
    subject.stage_horse_total_career(con)
    subject.stage_current_race_grade(con)
    subject.stage_field_career_avg(con)
    subject.stage_target_grade_careers(con)

    history_rows = con.execute(
        "select ketto_toroku_bango, grade_letter, finish_position from banei_grade_history"
    ).fetchall()
    current_rows = con.execute(
        "select grade_letter, grade_rank from current_race_grade"
    ).fetchall()
    e_career = con.execute(
        "select e_starts, e_wins from target_grade_career"
    ).fetchone()
    current_career = con.execute(
        "select past_starts, past_wins from target_current_grade_career"
    ).fetchone()
    assert history_rows == [("HORSE", "E", 1)]
    assert current_rows == [("Q", 5)]
    assert e_career == (1, 1)
    assert current_career == (None, None)
    con.close()
