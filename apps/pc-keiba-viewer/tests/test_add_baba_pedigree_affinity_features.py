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

_spec = importlib.util.spec_from_file_location(
    "add_baba_pedigree_affinity_features", MODULE_PATH
)
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


def test_append_features_sql_uses_scoped_live_current_baba() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join horse_pedigree" in sql
    assert "left join horse_baba_cumul" in sql
    assert "left join sire_baba_cumul" in sql
    assert "left join damsire_baba_cumul" in sql
    assert "left join race_baba rb" in sql
    assert "rb.baba_cond as current_baba_condition" in sql
    assert "jvd_ra" not in sql
    assert "nvd_ra" not in sql


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


def test_carried_baba_condition_matches_legacy_ra_for_all_categories_and_edge_cases() -> (
    None
):
    """The carried columns must be byte-for-byte equivalent to the old RA join.

    The fixture covers JRA turf/dirt, NAR, Ban-ei (NAR source + venue 83),
    mixed/invalid/NULL condition codes, and a scratched row represented by a
    NULL finish. The latter remains excluded by the unchanged history filter.
    """
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.jvd_ra as
        select * from (
          values
            ('2024','0101','06','01','10','1','0'),
            ('2024','0102','06','02','23','0','2'),
            ('2024','0103','06','03','10','3','4'),
            ('2024','0104','06','04','23','x','4'),
            ('2024','0105','06','05','23',null,'3'),
            ('2024','0106','06','06','10','0','0')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               track_code, babajotai_code_shiba, babajotai_code_dirt)
        """
    )
    con.execute(
        """
        create table pg.nvd_ra as
        select * from (
          values
            ('2024','0107','83','07','21',null,'4'),
            ('2024','0108','30','08','11','2',null),
            ('2024','0109','30','09','11','1','0'),
            ('2024','0110','30','10','21',null,null)
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               track_code, babajotai_code_shiba, babajotai_code_dirt)
        """
    )
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra','20240101','2024','0101','06','01','jra_turf',1,'10','1','0'),
            ('jra','20240102','2024','0102','06','02','jra_dirt',2,'23','0','2'),
            ('jra','20240103','2024','0103','06','03','jra_both',3,'10','3','4'),
            ('jra','20240104','2024','0104','06','04','jra_invalid',4,'23','x','4'),
            ('jra','20240105','2024','0105','06','05','jra_null_turf',5,'23',null,'3'),
            ('jra','20240106','2024','0106','06','06','jra_zero',6,'10','0','0'),
            ('nar','20240107','2024','0107','83','07','banei',1,'21',null,'4'),
            ('nar','20240108','2024','0108','30','08','nar',2,'11','2',null),
            ('nar','20240109','2024','0109','30','09','scratched',null,'11','1','0'),
            ('nar','20240110','2024','0110','30','10','nar_null',3,'21',null,null)
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, finish_position, track_code,
               babajotai_code_shiba, babajotai_code_dirt)
        """
    )
    con.execute(
        """
        create temp table legacy_race_baba as
        select 'jra' as source, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango,
               coalesce(
                 try_cast(nullif(babajotai_code_shiba, '0') as int),
                 try_cast(nullif(babajotai_code_dirt, '0') as int)
               ) as baba_cond
        from pg.jvd_ra
        where kaisai_nen >= substring('20000101', 1, 4)
        union all
        select 'nar' as source, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango,
               coalesce(
                 try_cast(nullif(babajotai_code_shiba, '0') as int),
                 try_cast(nullif(babajotai_code_dirt, '0') as int)
               ) as baba_cond
        from pg.nvd_ra
        where kaisai_nen >= substring('20000101', 1, 4)
        """
    )
    legacy_rows = con.execute(
        """
        select rec.source, rec.race_date, rec.keibajo_code,
               rec.ketto_toroku_bango, rec.finish_position, rb.baba_cond
        from pg.race_entry_corner_features rec
        left join legacy_race_baba rb
          on rb.source = rec.source
          and rb.kaisai_nen = rec.kaisai_nen
          and rb.kaisai_tsukihi = rec.kaisai_tsukihi
          and rb.keibajo_code = rec.keibajo_code
          and rb.race_bango = rec.race_bango
        where rec.race_date >= '20000101'
          and rec.finish_position is not null
          and rec.ketto_toroku_bango is not null
          and rb.baba_cond is not null
        order by rec.source, rec.race_date, rec.ketto_toroku_bango
        """
    ).fetchall()

    subject.stage_race_history_with_baba(con, "20000101")
    carried_rows = con.execute(
        """
        select source, race_date, keibajo_code, ketto_toroku_bango,
               finish_position, baba_cond
        from race_history
        order by source, race_date, ketto_toroku_bango
        """
    ).fetchall()
    con.close()

    assert carried_rows == legacy_rows
    assert carried_rows == [
        ("jra", "20240101", "06", "jra_turf", 1, 1),
        ("jra", "20240102", "06", "jra_dirt", 2, 2),
        ("jra", "20240103", "06", "jra_both", 3, 3),
        ("jra", "20240104", "06", "jra_invalid", 4, 4),
        ("jra", "20240105", "06", "jra_null_turf", 5, 3),
        ("nar", "20240107", "83", "banei", 1, 4),
        ("nar", "20240108", "30", "nar", 2, 2),
    ]


def test_current_baba_live_lookup_is_scoped_to_input_races_for_jra_nar_and_banei() -> (
    None
):
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create temp table base_input as
        select * from (
          values
            ('jra','2024','0201','05','01'),
            ('jra','1999','0201','05','04'),
            ('nar','2024','0201','30','02'),
            ('nar','2024','0201','83','03')
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        """
    )
    con.execute(
        """
        create table pg.jvd_ra as
        select * from (
          values
            ('2024','0201','05','01','1','0'),
            ('1999','0201','05','04','3','0'),
            ('2024','0201','05','12','0','4')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               babajotai_code_shiba, babajotai_code_dirt)
        """
    )
    con.execute(
        """
        create table pg.nvd_ra as
        select * from (
          values
            ('2024','0201','30','02','0','2'),
            ('2024','0201','83','03',null,'4'),
            ('2024','0201','30','11','3','0')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               babajotai_code_shiba, babajotai_code_dirt)
        """
    )

    subject.stage_current_race_baba(con, "20000101")
    rows = con.execute(
        """
        select source, keibajo_code, race_bango, baba_cond
        from race_baba
        order by source, keibajo_code, race_bango
        """
    ).fetchall()
    con.close()

    assert rows == [
        ("jra", "05", "01", 1),
        ("nar", "30", "02", 2),
        ("nar", "83", "03", 4),
    ]


def test_upcoming_target_race_resolves_baba_pedigree_features_via_asof(
    tmp_path: Path,
) -> None:
    """End-to-end regression for the serve-time defect.

    horse_a ran 3 COMPLETED races in baba_cond=1 (good going): a win on
    2024-01-01, a loss on 2024-01-08, a win on 2024-01-15. sire_a (horse_a's
    sire) is used so sire_baba_win_rate resolves too. The TARGET race
    (2024-01-22, also baba_cond=1) is genuinely upcoming and absent from
    pg.race_entry_corner_features. Its input parquet already carries the RA
    baba columns. The ASOF lookup must resolve it against horse_a's/sire_a's
    latest actual prior race in the same baba_cond (2024-01-15).
    """
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.jvd_ra as
        select * from (
          values ('2024','0122','06','14','1','0')
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
            ('jra','20240101','2024','0101','06','11','horse_a',1,'1','0'),
            ('jra','20240108','2024','0108','06','12','horse_a',2,'1','0'),
            ('jra','20240115','2024','0115','06','13','horse_a',1,'1','0')
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, finish_position,
               babajotai_code_shiba, babajotai_code_dirt)
        """
    )
    con.execute(
        """
        create temp table horse_pedigree as
        select * from (values ('horse_a', 'sire_a', 'damsire_a'))
          as v(ketto_toroku_bango, sire_id, damsire_id)
        """
    )

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values ('jra', '2024', '0122', '06', '14', 'horse_a', '20240122',
                  2024, '2', '0')
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year,
               babajotai_code_shiba, babajotai_code_dirt)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    subject.stage_base_input(con, input_glob)
    subject.stage_current_race_baba(con, "20000101")
    subject.stage_race_history_with_baba(con, "20000101")
    subject.stage_horse_baba_cumul(con)
    subject.stage_sire_baba_cumul(con)
    subject.stage_damsire_baba_cumul(con)

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
    # The input deliberately carries stale baba_cond=2. The scoped live RA
    # lookup resolves the updated baba_cond=1 without scanning unrelated RA
    # rows, preserving the pre-day-base-split freshness contract.
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
    assert (
        "rec.ketto_toroku_bango in (select ketto_toroku_bango from target_horses)"
        in joined
    )
    assert "join target_pedigree_ids t" in joined
    assert "rec.finish_position is not null" in joined
    assert "rec.babajotai_code_shiba" in joined
    assert "rec.babajotai_code_dirt" in joined
    assert "race_baba" not in joined
    assert "jvd_ra" not in joined
    assert "nvd_ra" not in joined
