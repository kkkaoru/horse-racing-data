from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-head-to-head-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_head_to_head_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_head_to_head_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_parse_args_accepts_target_race(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--target-race",
            "30:01",
        ]
    )
    assert args.target_race == "30:01"


def test_append_features_sql_contains_h2h_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "h2h_encounter_count" in sql
    assert "h2h_win_count_vs_field" in sql
    assert "h2h_loss_count_vs_field" in sql
    assert "h2h_win_rate_vs_field" in sql
    assert "h2h_avg_finish_diff_vs_field" in sql
    assert "h2h_unique_rivals_count" in sql


def test_append_features_sql_left_joins_summary() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join h2h_horse_summary" in sql
    assert "s.self_horse = b.ketto_toroku_bango" in sql


def test_race_partition_constant() -> None:
    assert subject.RACE_PARTITION == "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"


def _make_target_parquet(tmp_path: Path) -> str:
    """Write a minimal parquet file with hive partitioning and return the glob."""
    race_year_dir = tmp_path / "race_year=2026"
    race_year_dir.mkdir(parents=True)
    table = pa.table(
        {
            "source": pa.array(["nar"], pa.string()),
            "kaisai_nen": pa.array(["2026"], pa.string()),
            "kaisai_tsukihi": pa.array(["0619"], pa.string()),
            "keibajo_code": pa.array(["30"], pa.string()),
            "race_bango": pa.array(["01"], pa.string()),
            "race_date": pa.array(["2026-06-19"], pa.string()),
            "ketto_toroku_bango": pa.array(["HORSE1"], pa.string()),
            "finish_position": pa.array([None], pa.float64()),
        }
    )
    pq.write_table(table, race_year_dir / "data.parquet")
    return f"{tmp_path.as_posix()}/race_year=*/*.parquet"


def test_stage_target_races_loads_distinct_race_keys(tmp_path: Path) -> None:
    """stage_target_races must extract unique race keys from the input parquet."""
    input_glob = _make_target_parquet(tmp_path)
    con = duckdb.connect(":memory:")
    subject.stage_target_races(con, input_glob)
    rows = con.execute(
        "SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango FROM target_races"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0] == ("nar", "2026", "0619", "30", "01")


def test_stage_target_horses_loads_distinct_input_horses(tmp_path: Path) -> None:
    input_glob = _make_target_parquet(tmp_path)
    con = duckdb.connect(":memory:")
    subject.stage_target_horses(con, input_glob)
    rows = con.execute(
        "select source, ketto_toroku_bango from target_horses"
    ).fetchall()
    con.close()
    assert rows == [("nar", "HORSE1")]


def test_sql_literal_escapes_single_quotes() -> None:
    assert subject.sql_literal("A'B") == "'A''B'"


def test_target_horses_by_source_groups_non_null_horses() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table target_horses(source varchar, ketto_toroku_bango varchar);
        insert into target_horses values
          ('jra', 'JRA2'), ('jra', 'JRA1'), ('nar', 'NAR1'),
          ('nar', 'NAR1'), (null, 'IGNORED'), ('nar', null)
        """
    )

    horses = subject.target_horses_by_source(con)

    con.close()
    assert horses == {"jra": {"JRA1", "JRA2"}, "nar": {"NAR1"}}


def test_race_history_select_sql_empty_target_has_typed_empty_result() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        "create temp table target_horses(source varchar, ketto_toroku_bango varchar)"
    )

    sql = subject.race_history_select_sql(con, "20200101")

    con.close()
    assert "cast(null as varchar) as source" in sql
    assert "where false" in sql
    assert "pg.jvd_se" not in sql
    assert "pg.nvd_se" not in sql


def test_race_history_select_sql_pushes_filters_into_raw_source_branches() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table target_horses(source varchar, ketto_toroku_bango varchar);
        insert into target_horses values
          ('jra', 'JRA2'), ('jra', 'JRA1'), ('nar', 'NAR1')
        """
    )

    sql = subject.race_history_select_sql(con, "20200101")

    con.close()
    assert "from pg.jvd_se se" in sql
    assert "inner join pg.jvd_ra ra" in sql
    assert "from pg.nvd_se se" in sql
    assert "inner join pg.nvd_ra ra" in sql
    assert "se.kaisai_nen >= '2020'" in sql
    assert "se.kaisai_nen || se.kaisai_tsukihi >= '20200101'" in sql
    assert "se.ketto_toroku_bango in ('JRA1', 'JRA2')" in sql
    assert "se.ketto_toroku_bango in ('NAR1')" in sql
    assert "se.kakutei_chakujun" in sql
    assert "pg.race_entry_corner_features" not in sql


def test_stage_race_history_pushes_source_horse_and_year_filters() -> None:
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.nvd_se(
          kaisai_nen varchar,
          kaisai_tsukihi varchar,
          keibajo_code varchar,
          race_bango varchar,
          ketto_toroku_bango varchar,
          umaban varchar,
          kakutei_chakujun varchar
        );
        insert into pg.nvd_se values
          ('2025', '0101', '30', '01', 'HORSE1', '01', '01'),
          ('2025', '0101', '30', '01', 'UNUSED', '02', '02'),
          ('2019', '0101', '30', '01', 'HORSE1', '03', '03'),
          ('2025', '0102', '30', '01', 'HORSE1', '04', '00');
        create table pg.nvd_ra(
          kaisai_nen varchar,
          kaisai_tsukihi varchar,
          keibajo_code varchar,
          race_bango varchar,
          kyori varchar
        );
        insert into pg.nvd_ra values
          ('2025', '0101', '30', '01', '1200'),
          ('2019', '0101', '30', '01', '1200'),
          ('2025', '0102', '30', '01', '1200')
        """
    )
    con.execute(
        """
        create temp table target_horses(source varchar, ketto_toroku_bango varchar);
        insert into target_horses values ('nar', 'HORSE1')
        """
    )

    subject.stage_race_history(con, "20200101")

    rows = con.execute(
        "select source, kaisai_nen, ketto_toroku_bango from race_history"
    ).fetchall()
    con.close()
    assert rows == [("nar", "2025", "HORSE1")]


def test_target_pair_filter_sql_uses_target_horses() -> None:
    sql = subject.target_pair_filter_sql()
    assert "target_horses" in sql
    assert "h1.ketto_toroku_bango" in sql
    assert "h2.ketto_toroku_bango" in sql


def test_stage_pair_history_keeps_only_pairs_needed_by_whole_day_input() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table target_horses(source varchar, ketto_toroku_bango varchar);
        insert into target_horses values ('nar', 'HORSE1'), ('nar', 'HORSE2');
        create temp table race_history as
        select 'nar' as source, '2025-06-01' as race_date,
               '2025' as kaisai_nen, '0601' as kaisai_tsukihi,
               '30' as keibajo_code, '01' as race_bango,
               'HORSE1' as ketto_toroku_bango, 1 as finish_position
        union all
        select 'nar', '2025-06-01', '2025', '0601', '30', '01', 'HORSE2', 2
        union all
        select 'nar', '2025-06-01', '2025', '0601', '30', '01', 'UNUSED', 3
        """
    )

    subject.stage_pair_history(con)

    rows = con.execute(
        "select horse_a, horse_b, finish_diff_a_minus_b from pair_history"
    ).fetchall()
    con.close()
    assert rows == [("HORSE1", "HORSE2", -1)]


def test_stage_target_races_multiple_races(tmp_path: Path) -> None:
    """stage_target_races deduplicates across horses in the same race."""
    race_year_dir = tmp_path / "race_year=2026"
    race_year_dir.mkdir(parents=True)
    # 3 horses in same race + 1 horse in different race
    table = pa.table(
        {
            "source": pa.array(["nar", "nar", "nar", "nar"], pa.string()),
            "kaisai_nen": pa.array(["2026", "2026", "2026", "2026"], pa.string()),
            "kaisai_tsukihi": pa.array(["0619", "0619", "0619", "0619"], pa.string()),
            "keibajo_code": pa.array(["30", "30", "30", "31"], pa.string()),
            "race_bango": pa.array(["01", "01", "01", "01"], pa.string()),
            "race_date": pa.array(
                ["2026-06-19", "2026-06-19", "2026-06-19", "2026-06-19"], pa.string()
            ),
            "ketto_toroku_bango": pa.array(
                ["HORSE1", "HORSE2", "HORSE3", "HORSE4"], pa.string()
            ),
            "finish_position": pa.array([None, None, None, None], pa.float64()),
        }
    )
    pq.write_table(table, race_year_dir / "data.parquet")
    input_glob = f"{tmp_path.as_posix()}/race_year=*/*.parquet"
    con = duckdb.connect(":memory:")
    subject.stage_target_races(con, input_glob)
    row = con.execute("SELECT count(*) FROM target_races").fetchone()
    assert row is not None
    count = row[0]
    assert count == 2  # 2 distinct (keibajo_code, race_bango) combos


def test_stage_current_pair_aggregates_filters_to_target_races(tmp_path: Path) -> None:
    """current_pair_aggregates must only compute pairs for races in target_races."""
    input_glob = _make_target_parquet(tmp_path)
    con = duckdb.connect(":memory:")

    # race_history: 2 races — only race 01 is in target_races
    con.execute(
        """
        CREATE TEMP TABLE race_history AS
        SELECT
          'nar' AS source,
          '2026' AS kaisai_nen,
          '0619' AS kaisai_tsukihi,
          '30' AS keibajo_code,
          '01' AS race_bango,
          'HORSE1' AS ketto_toroku_bango,
          CAST('2026-06-19' AS DATE) AS race_date,
          1 AS finish_position
        UNION ALL
        SELECT 'nar','2026','0619','30','01','HORSE2','2026-06-19'::DATE, 2
        UNION ALL
        -- race 02 is NOT in target_races — should be excluded
        SELECT 'nar','2026','0619','30','02','HORSE3','2026-06-19'::DATE, 1
        UNION ALL
        SELECT 'nar','2026','0619','30','02','HORSE4','2026-06-19'::DATE, 2
        """
    )
    # pair_history: empty (no prior encounters)
    con.execute(
        """
        CREATE TEMP TABLE pair_history AS
        SELECT
          'nar' AS source,
          CAST('2026-06-01' AS DATE) AS race_date,
          'HORSE_X' AS horse_a,
          'HORSE_Y' AS horse_b,
          -1 AS finish_diff_a_minus_b
        WHERE false
        """
    )
    subject.stage_target_races(con, input_glob)
    subject.stage_current_pair_aggregates(con)

    rows = con.execute(
        "SELECT race_bango, horse_a, horse_b FROM current_pair_aggregates ORDER BY race_bango"
    ).fetchall()
    # Only race 01 pair (HORSE1, HORSE2) should appear; race 02 excluded
    assert len(rows) == 1
    assert rows[0][0] == "01"
    assert set(rows[0][1:]) == {"HORSE1", "HORSE2"}
