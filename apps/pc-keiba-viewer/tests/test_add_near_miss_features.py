from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-near-miss-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_near_miss_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_near_miss_features"] = subject
_spec.loader.exec_module(subject)


class FakeConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)


def test_parse_args_accepts_target_race(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--target-race",
            "05:11",
        ]
    )
    assert args.target_race == "05:11"


def test_append_features_sql_excludes_base_meta_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "exclude (kishumei_ryakusho, tansho_ninkijun, shusso_tosu)" in sql


def test_append_features_sql_reemits_canonical_null_shusso_tosu() -> None:
    # The EXCLUDE drops the populated base ``shusso_tosu`` (the rh re-join keeps
    # the colliding copy as ``shusso_tosu_1``), so the canonical ``shusso_tosu``
    # has to be re-emitted as an all-NULL BIGINT to match the trained NAR parquet
    # layout (feature index 2 was constant-NULL there).
    sql = subject.append_features_sql("dummy.parquet")
    assert "cast(null as bigint) as shusso_tosu" in sql


def test_append_features_sql_keeps_horse_popularity_vs_field() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "b.tansho_ninkijun::double / nullif(b.shusso_tosu, 0)" in sql
    assert "as horse_popularity_vs_field" in sql


def _seed_base_parquet(parquet_dir: Path) -> str:
    """Write a 2-row synthetic base parquet carrying the meta columns the
    near-miss layer re-joins (so the rename / exclude path is exercised).

    horse_a: ninki=1, odds=3.0 (favourite)
    horse_b: ninki=2, odds=6.0 (second choice)
    Expected field_dominant_favorite_indicator = 3.0 / 6.0 = 0.5
    """
    parquet_dir.mkdir(parents=True, exist_ok=True)
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('nar', '2025', '0415', '54', '11', 'horse_a', '20250415', 2025,
              'JOCKEY_A'::varchar, 1::integer, 12::integer, 3.0::double),
            ('nar', '2025', '0415', '54', '11', 'horse_b', '20250415', 2025,
              'JOCKEY_B'::varchar, 2::integer, 12::integer, 6.0::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year,
          kishumei_ryakusho, tansho_ninkijun, shusso_tosu, tansho_odds
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    return f"{parquet_dir.as_posix()}/race_year=*/*.parquet"


def _seed_base_parquet_without_jockey(parquet_dir: Path) -> str:
    parquet_dir.mkdir(parents=True, exist_ok=True)
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('nar', '2025', '0415', '35', '01', 'horse_a', '20250415', 2025,
              1200::integer, '1'::varchar),
            ('nar', '2025', '0415', '35', '01', 'horse_b', '20250415', 2025,
              1200::integer, '1'::varchar)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year, kyori, track_code
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    return f"{parquet_dir.as_posix()}/race_year=*/*.parquet"


def _seed_pg_race_entry_corner_features(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('nar', '2025', '0415', '54', '11', 'horse_a', 'PG_JOCKEY_A'::varchar),
            ('nar', '2025', '0415', '54', '11', 'horse_b', 'PG_JOCKEY_B'::varchar),
            ('nar', '2025', '0415', '35', '01', 'horse_a', 'PG_JOCKEY_A'::varchar),
            ('nar', '2025', '0415', '35', '01', 'horse_b', 'PG_JOCKEY_B'::varchar),
            ('nar', '2025', '0415', '35', '02', 'horse_a', 'OTHER_JOCKEY'::varchar)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, kishumei_ryakusho
        )
        """
    )


def _seed_join_temps(con: duckdb.DuckDBPyConnection) -> None:
    """Create the six staging temp tables the joined CTE LEFT JOINs against.

    Only ``race_history`` carries a row for horse_a (so its rh re-join supplies a
    populated ``shusso_tosu_1``); the per-horse / per-jockey / per-race aggregate
    temps are seeded empty so their LEFT JOINs emit NULL — the documented
    "no eligible history" path."""
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values ('nar', '2025', '0415', '54', '11', 'horse_a',
                  'JOCKEY_A'::varchar, 1::integer, 9::integer, 0.7::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, kishumei_ryakusho, tansho_ninkijun, shusso_tosu,
          tansho_odds
        )
        """
    )
    con.execute(
        """
        create or replace temp table horse_near_miss(
          source varchar, ketto_toroku_bango varchar, race_date varchar,
          past_starts integer, past_p1_count integer, past_p2_count integer,
          past_p2_avg_timesa double, recent_p2_count_5 integer,
          recent_p2_avg_timesa_5 double
        )
        """
    )
    con.execute(
        """
        create or replace temp table horse_context(
          source varchar, ketto_toroku_bango varchar, race_date varchar,
          same_keibajo_starts integer, same_keibajo_p2 integer,
          same_distance_starts integer, same_distance_p2 integer,
          same_track_starts integer, same_track_p2 integer,
          pair_starts integer, pair_p2 integer
        )
        """
    )
    con.execute(
        """
        create or replace temp table horse_pedigree_context(
          source varchar, ketto_toroku_bango varchar, race_date varchar,
          sire_distance_starts integer, sire_distance_p2 integer,
          sire_grade_starts integer, sire_grade_p2 integer,
          damsire_distance_starts integer, damsire_distance_p2 integer
        )
        """
    )
    con.execute(
        """
        create or replace temp table horse_distance_grade(
          source varchar, ketto_toroku_bango varchar, race_date varchar,
          dg_starts integer, dg_p2 integer
        )
        """
    )
    con.execute(
        """
        create or replace temp table jockey_near_miss(
          source varchar, kishumei_ryakusho varchar, race_date varchar,
          past_rides integer, past_jockey_p2_count integer
        )
        """
    )
def test_append_features_sql_output_has_canonical_and_suffixed_shusso(
    tmp_path: Path,
) -> None:
    con = duckdb.connect(":memory:")
    glob = _seed_base_parquet(tmp_path / "input")
    _seed_join_temps(con)
    sql = subject.append_features_sql(glob)
    cols = [row[0] for row in con.execute(f"describe {sql}").fetchall()]
    con.close()
    # Both the canonical ``shusso_tosu`` and the rh-join ``shusso_tosu_1`` are
    # present so the NAR ensemble members (which reference BOTH) are covered.
    assert "shusso_tosu" in cols
    assert "shusso_tosu_1" in cols


def test_append_features_sql_canonical_shusso_tosu_is_all_null(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    glob = _seed_base_parquet(tmp_path / "input")
    _seed_join_temps(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango, shusso_tosu, shusso_tosu_1
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # Canonical ``shusso_tosu`` is constant-NULL (matches the trained parquet's
    # index-2 column); horse_a's ``shusso_tosu_1`` comes from the race_history
    # re-join (=9), horse_b has no history row so it is NULL.
    assert rows[0] == ("horse_a", None, 9)
    assert rows[1] == ("horse_b", None, None)


def test_append_features_sql_canonical_shusso_tosu_is_bigint(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    glob = _seed_base_parquet(tmp_path / "input")
    _seed_join_temps(con)
    sql = subject.append_features_sql(glob)
    described = con.execute(
        f"describe select shusso_tosu from ({sql})"
    ).fetchall()
    con.close()
    # BIGINT cast mirrors the trained parquet dtype for the index-2 column.
    assert described[0][0] == "shusso_tosu"
    assert described[0][1] == "BIGINT"


def test_append_features_sql_field_dominant_favorite_from_base_parquet(
    tmp_path: Path,
) -> None:
    """field_dominant_favorite_indicator is computed from the base parquet (not
    race_history), so upcoming races without a finish_position row still get a
    non-NULL value.  With horse_a (ninki=1, odds=3.0) and horse_b (ninki=2,
    odds=6.0), the ratio should be 3.0/6.0 = 0.5 for both rows in the race."""
    con = duckdb.connect(":memory:")
    glob = _seed_base_parquet(tmp_path / "input")
    _seed_join_temps(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"select ketto_toroku_bango, field_dominant_favorite_indicator"
        f" from ({sql}) order by ketto_toroku_bango"
    ).fetchall()
    con.close()
    assert rows[0][0] == "horse_a"
    assert rows[0][1] == pytest.approx(0.5)
    assert rows[1][0] == "horse_b"
    assert rows[1][1] == pytest.approx(0.5)


def test_append_features_sql_uses_asof_join_for_horse_near_miss() -> None:
    # Regression: the old exact-date equality join (h.race_date = b.race_date)
    # required horse_near_miss to carry a row keyed at exactly the target's own
    # race_date, which only a COMPLETED race can produce -- so every upcoming
    # target race silently got NULL for all 5 horse-side near-miss columns. The
    # ASOF join with a strict inequality resolves against the horse's latest
    # actual prior race instead.
    sql = subject.append_features_sql("dummy.parquet")
    assert "asof left join horse_near_miss h" in sql
    assert "b.race_date > h.race_date" in sql
    assert "h.race_date = b.race_date" not in sql


def test_append_features_sql_uses_asof_join_for_jockey_near_miss() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "asof left join jockey_near_miss j" in sql
    assert "b.race_date > j.race_date" in sql
    assert "j.race_date = b.race_date" not in sql


def test_stage_horse_near_miss_window_is_inclusive_of_current_row() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_horse_near_miss(RecordingConn())
    body = " ".join(sql_calls)
    # Inclusive windows (current row included) let the ASOF join in
    # append_features_sql treat the matched row's cumulative as "everything
    # through and including the horse's latest actual race" -- the old
    # exclusive ("1 preceding") window relied on being joined at exactly the
    # target's own race_date, which an upcoming race can never provide.
    assert "rows between unbounded preceding and current row" in body
    assert "rows between 4 preceding and current row" in body
    assert "1 preceding" not in body


def test_stage_jockey_near_miss_window_is_inclusive_of_current_row() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_jockey_near_miss(RecordingConn())
    body = " ".join(sql_calls)
    assert "rows between unbounded preceding and current row" in body
    assert "1 preceding" not in body


def test_upcoming_target_race_resolves_near_miss_features_via_asof(
    tmp_path: Path,
) -> None:
    """End-to-end regression for the serve-time defect.

    horse_a ran 3 COMPLETED races (2024-01-01, 2024-01-08 [P2], 2024-01-15).
    jockey J1 rode horse_a in all 3. The TARGET race (2024-01-22) is
    genuinely upcoming: it is not, and never was, in
    pg.race_entry_corner_features (finish_position is null forever, by
    construction). The old exact-date join could never resolve this target;
    the ASOF fix must resolve it against horse_a's/jockey J1's latest actual
    prior race (2024-01-15).
    """
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra','20240101','2024','0101','06','11','horse_a',1,1,150.0,'J1'),
            ('jra','20240108','2024','0108','06','12','horse_a',1,2,80.0,'J1'),
            ('jra','20240115','2024','0115','06','13','horse_a',1,1,200.0,'J1')
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, tansho_ninkijun, finish_position, time_sa, kishumei_ryakusho)
        """
    )
    for col, typ in (
        ("shusso_tosu", "integer"),
        ("kyori", "integer"),
        ("track_code", "varchar"),
        ("grade_code", "varchar"),
        ("tansho_odds", "double"),
        ("chokyoshimei_ryakusho", "varchar"),
        ("banushimei", "varchar"),
    ):
        con.execute(f"alter table pg.race_entry_corner_features add column {col} {typ}")

    subject.stage_race_history(con, "20000101")
    subject.stage_horse_near_miss(con)
    subject.stage_jockey_near_miss(con)

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values ('jra', '2024', '0122', '06', '14', 'horse_a', '20240122', 2024,
                  1::integer, 3.0::double)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year, tansho_ninkijun, tansho_odds)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    con.execute(
        """
        create or replace temp table horse_context(
          source varchar, ketto_toroku_bango varchar, race_date varchar,
          same_keibajo_starts integer, same_keibajo_p2 integer,
          same_distance_starts integer, same_distance_p2 integer,
          same_track_starts integer, same_track_p2 integer,
          pair_starts integer, pair_p2 integer
        )
        """
    )
    con.execute(
        """
        create or replace temp table horse_pedigree_context(
          source varchar, ketto_toroku_bango varchar, race_date varchar,
          sire_distance_starts integer, sire_distance_p2 integer,
          sire_grade_starts integer, sire_grade_p2 integer,
          damsire_distance_starts integer, damsire_distance_p2 integer
        )
        """
    )
    con.execute(
        """
        create or replace temp table horse_distance_grade(
          source varchar, ketto_toroku_bango varchar, race_date varchar,
          dg_starts integer, dg_p2 integer
        )
        """
    )

    sql = subject.append_features_sql(input_glob)
    row = con.execute(
        f"""
        select career_place2_rate, career_place2_to_win_ratio,
               career_avg_2nd_margin_decisec, recent_place2_count_5,
               recent_2nd_margin_avg_5, jockey_career_place2_rate
        from ({sql})
        """
    ).fetchone()
    con.close()

    assert row is not None
    # 3 career starts, 1 P2 (the 2024-01-08 race) -> career_place2_rate = 1/3.
    assert row[0] == pytest.approx(1.0 / 3.0)
    # 2 wins (2024-01-01, 2024-01-15) out of 3 starts -> win_rate = 2/3.
    # place2_to_win_ratio = (1/3) / (2/3) = 0.5.
    assert row[1] == pytest.approx(0.5)
    # The single P2 race (2024-01-08) had time_sa = 80.
    assert row[2] == pytest.approx(80.0)
    # All 3 prior races fall within the "recent 5" window -> same P2 count/avg.
    assert row[3] == 1
    assert row[4] == pytest.approx(80.0)
    # jockey_career_place2_rate is a SEPARATE, NOT-yet-fixed gap: its join key
    # (b.kishumei_ryakusho) is sourced entirely from the rh (race_history)
    # re-join in base_with_meta, which requires the exact race to be COMPLETED
    # (same defect family) -- and unlike tansho_ninkijun/tansho_odds/
    # shusso_tosu, the base feature parquet never carries its own
    # kishumei_ryakusho column (confirmed: absent from
    # finish_position_features_duckdb.py::base_features_select_sql), so there
    # is no populated fallback for the "*.* exclude(...)" collision-rename
    # trick to fall back to. Fixing this needs a genuinely new PG source
    # (jvd_se/nvd_se read directly, since race_entry_corner_features itself can
    # be entirely absent for an upcoming race, independent of any
    # finish_position filter) -- out of scope for this ASOF-join fix; flagged
    # separately rather than silently left broken.
    assert row[5] is None


def test_append_features_sql_computes_fav_dominance_sql_from_base_cte() -> None:
    """The SQL string must reference fav_ranked / fav_pivoted / race_favorite_dominance
    CTEs sourced from base — not from a pre-built race_favorite_dominance temp table."""
    sql = subject.append_features_sql("dummy.parquet")
    assert "fav_ranked" in sql
    assert "fav_pivoted" in sql
    assert "race_favorite_dominance" in sql
    assert "from base" in sql.lower()


def test_race_history_focus_filter_sql_unfocused_is_empty() -> None:
    assert subject.race_history_focus_filter_sql(False) == ""


def test_race_history_focus_filter_sql_uses_target_entities_and_pedigree() -> None:
    sql = subject.race_history_focus_filter_sql(True)
    assert "target_entities" in sql
    assert "rec.ketto_toroku_bango" in sql
    assert "rec.kishumei_ryakusho" in sql
    assert "horse_pedigree" in sql
    assert "te.sire_id" in sql
    assert "te.damsire_id" in sql


def test_stage_race_history_focused_appends_entity_filter() -> None:
    conn = FakeConn()
    subject.stage_race_history(conn, "20240101", focused_target=True)
    body = " ".join(conn.statements)
    assert "from pg.race_entry_corner_features rec" in body
    assert "rec.race_date >= '20240101'" in body
    assert "target_entities" in body


def test_stage_target_entities_extracts_input_horse_jockey_and_pedigree(
    tmp_path: Path,
) -> None:
    con = duckdb.connect(":memory:")
    input_glob = _seed_base_parquet(tmp_path / "input")
    _seed_pg_race_entry_corner_features(con)
    con.execute(
        """
        create temp table horse_pedigree as
        select * from (
          values
            ('horse_a', 'sire_a', 'damsire_a'),
            ('horse_b', 'sire_b', 'damsire_b')
        ) as v(ketto_toroku_bango, sire_id, damsire_id)
        """
    )
    subject.stage_target_entities(con, input_glob)
    rows = con.execute(
        """
        select ketto_toroku_bango, kishumei_ryakusho, sire_id, damsire_id
        from target_entities
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    assert rows == [
        ("horse_a", "PG_JOCKEY_A", "sire_a", "damsire_a"),
        ("horse_b", "PG_JOCKEY_B", "sire_b", "damsire_b"),
    ]


def test_stage_target_entities_accepts_scoped_input_without_jockey_column(
    tmp_path: Path,
) -> None:
    con = duckdb.connect(":memory:")
    input_glob = _seed_base_parquet_without_jockey(tmp_path / "input")
    _seed_pg_race_entry_corner_features(con)
    con.execute(
        """
        create temp table horse_pedigree as
        select * from (
          values
            ('horse_a', 'sire_a', 'damsire_a'),
            ('horse_b', 'sire_b', 'damsire_b')
        ) as v(ketto_toroku_bango, sire_id, damsire_id)
        """
    )
    subject.stage_target_entities(con, input_glob)
    rows = con.execute(
        """
        select ketto_toroku_bango, kishumei_ryakusho, sire_id, damsire_id
        from target_entities
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    assert rows == [
        ("horse_a", "PG_JOCKEY_A", "sire_a", "damsire_a"),
        ("horse_b", "PG_JOCKEY_B", "sire_b", "damsire_b"),
    ]
