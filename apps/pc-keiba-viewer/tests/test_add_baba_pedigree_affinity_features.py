from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-baba-pedigree-affinity-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_baba_pedigree_affinity_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_baba_pedigree_affinity_features"] = subject
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
            "10:02",
        ]
    )
    assert args.target_race == "10:02"


def test_append_features_sql_contains_baba_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "current_baba_condition" in sql
    assert "horse_baba_win_rate" in sql
    assert "horse_baba_career_starts" in sql
    assert "sire_baba_win_rate" in sql
    assert "damsire_baba_win_rate" in sql
    assert "sire_horse_baba_combined_score" in sql


def test_append_features_sql_joins_pedigree_and_baba() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join horse_pedigree" in sql
    assert "left join horse_baba_cumul" in sql
    assert "left join sire_baba_cumul" in sql
    assert "left join damsire_baba_cumul" in sql


def test_append_features_sql_uses_asof_join_for_baba_cumul_tables() -> None:
    # Regression: the old exact-date equality join (hbc.race_date =
    # bwp.race_date) required horse_baba_cumul/sire_baba_cumul/
    # damsire_baba_cumul to carry a row keyed at exactly the target's own
    # race_date, which only a COMPLETED race can produce (race_history filters
    # finish_position is not null) -- so every upcoming target race silently
    # got NULL for all 7 baba-affinity columns. The ASOF join with a strict
    # inequality resolves against the latest actual prior race in the same
    # baba_cond instead.
    sql = subject.append_features_sql("dummy.parquet")
    assert "asof left join horse_baba_cumul hbc" in sql
    assert "bwp.race_date > hbc.race_date" in sql
    assert "hbc.race_date = bwp.race_date" not in sql
    assert "asof left join sire_baba_cumul sbc" in sql
    assert "bwp.race_date > sbc.race_date" in sql
    assert "sbc.race_date = bwp.race_date" not in sql
    assert "asof left join damsire_baba_cumul dbc" in sql
    assert "bwp.race_date > dbc.race_date" in sql
    assert "dbc.race_date = bwp.race_date" not in sql


def test_stage_horse_baba_cumul_window_is_inclusive_of_current_row() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_horse_baba_cumul(RecordingConn())
    body = " ".join(sql_calls)
    assert "rows between unbounded preceding and current row" in body
    assert "1 preceding" not in body


def test_stage_sire_baba_cumul_window_is_inclusive_of_current_row() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_sire_baba_cumul(RecordingConn())
    body = " ".join(sql_calls)
    assert "rows between unbounded preceding and current row" in body
    assert "1 preceding" not in body


def test_stage_damsire_baba_cumul_window_is_inclusive_of_current_row() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_damsire_baba_cumul(RecordingConn())
    body = " ".join(sql_calls)
    assert "rows between unbounded preceding and current row" in body
    assert "1 preceding" not in body


def test_upcoming_target_race_resolves_baba_pedigree_features_via_asof(
    tmp_path: Path,
) -> None:
    """End-to-end regression for the serve-time defect.

    horse_a ran 3 COMPLETED races in baba_cond=1 (good going): a win on
    2024-01-01, a loss on 2024-01-08, a win on 2024-01-15. sire_a (horse_a's
    sire) is used so sire_baba_win_rate resolves too. The TARGET race
    (2024-01-22, also baba_cond=1) is genuinely upcoming: it is not, and never
    was, in pg.jvd_ra / pg.race_entry_corner_features. The old exact-date join
    could never resolve this target; the ASOF fix must resolve it against
    horse_a's/sire_a's latest actual prior race in the same baba_cond
    (2024-01-15).
    """
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.jvd_ra as
        select * from (
          values
            ('2024','0101','06','11','1','0'),
            ('2024','0108','06','12','1','0'),
            ('2024','0115','06','13','1','0'),
            ('2024','0122','06','14','1','0')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               babajotai_code_shiba, babajotai_code_dirt)
        """
    )
    con.execute(
        """
        create table pg.nvd_ra as
        select * from (values (cast(null as varchar), cast(null as varchar),
          cast(null as varchar), cast(null as varchar), cast(null as varchar),
          cast(null as varchar))) as v(
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          babajotai_code_shiba, babajotai_code_dirt
        ) where false
        """
    )
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra','20240101','2024','0101','06','11','horse_a',1),
            ('jra','20240108','2024','0108','06','12','horse_a',2),
            ('jra','20240115','2024','0115','06','13','horse_a',1)
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, finish_position)
        """
    )
    con.execute(
        """
        create temp table horse_pedigree as
        select * from (values ('horse_a', 'sire_a', 'damsire_a'))
          as v(ketto_toroku_bango, sire_id, damsire_id)
        """
    )

    subject.stage_race_baba(con, "20000101")
    subject.stage_race_history_with_baba(con, "20000101")
    subject.stage_horse_baba_cumul(con)
    subject.stage_sire_baba_cumul(con)
    subject.stage_damsire_baba_cumul(con)

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values ('jra', '2024', '0122', '06', '14', 'horse_a', '20240122', 2024)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    sql = subject.append_features_sql(input_glob)
    row = con.execute(
        f"""
        select current_baba_condition, horse_baba_career_starts, horse_baba_win_rate,
               sire_baba_career_starts, sire_baba_win_rate,
               damsire_baba_career_starts, sire_horse_baba_combined_score
        from ({sql})
        """
    ).fetchone()
    con.close()

    assert row is not None
    # The target race itself resolves baba_cond=1 (good going) via jvd_ra's own
    # identity-keyed join (not date-based, so this was already correct).
    assert row[0] == 1
    # 3 career starts in baba_cond=1, 2 wins (0101, 0115) -> win_rate = 2/3.
    assert row[1] == 3
    assert row[2] == pytest.approx(2.0 / 3.0)
    assert row[3] == 3
    assert row[4] == pytest.approx(2.0 / 3.0)
    # damsire_a never won in baba_cond=1 in this fixture (all 3 races are
    # attributed via sire_a/horse_a only; damsire is separately tracked) --
    # damsire_baba_career_starts still reflects the same 3 starts because
    # horse_a IS damsire_a's tracked offspring here too.
    assert row[5] == 3
    # combined score averages horse (2/3) and sire (2/3) -> 2/3.
    assert row[6] == pytest.approx(2.0 / 3.0)


def test_stage_base_input_reads_target_horses_from_input_parquet() -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, query: str) -> None:
            self.sql.append(query)

    conn = FakeConn()
    subject.stage_base_input(conn, "/tmp/x/race_year=*/*.parquet")
    joined = "\n".join(conn.sql)
    assert "create or replace temp table base_input" in joined
    assert "from read_parquet('/tmp/x/race_year=*/*.parquet'" in joined
    assert "ketto_toroku_bango is not null" in joined


def test_stage_target_pedigree_context_builds_horse_and_pedigree_filters() -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, query: str) -> None:
            self.sql.append(query)

    conn = FakeConn()
    subject.stage_target_pedigree_context(conn)
    joined = "\n".join(conn.sql)
    assert "create or replace temp table target_horses" in joined
    assert "create or replace temp table target_pedigree_ids" in joined
    assert "left join horse_pedigree hp using (ketto_toroku_bango)" in joined


def test_stage_race_history_focused_filters_to_target_pedigree_context() -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, query: str) -> None:
            self.sql.append(query)

    conn = FakeConn()
    subject.stage_race_history_with_baba(conn, "20100101", focused_target=True)
    joined = "\n".join(conn.sql)
    assert "rec.ketto_toroku_bango in (select ketto_toroku_bango from target_horses)" in joined
    assert "join target_pedigree_ids t" in joined
    assert "rec.finish_position is not null" in joined
