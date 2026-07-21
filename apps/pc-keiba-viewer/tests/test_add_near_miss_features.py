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
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
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
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
          sire_distance_starts integer, sire_distance_p2 integer,
          sire_grade_starts integer, sire_grade_p2 integer,
          damsire_distance_starts integer, damsire_distance_p2 integer
        )
        """
    )
    con.execute(
        """
        create or replace temp table horse_distance_grade(
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
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
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
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
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
          sire_distance_starts integer, sire_distance_p2 integer,
          sire_grade_starts integer, sire_grade_p2 integer,
          damsire_distance_starts integer, damsire_distance_p2 integer
        )
        """
    )
    con.execute(
        """
        create or replace temp table horse_distance_grade(
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
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


# --- target-aware horse_context parity repair -------------------------------
#
# horse_context previously self-joined ONLY finished-only race_history for
# BOTH sides (curr and past), so an upcoming target race -- which can never
# have a settled row -- silently produced a join-NULL curr context instead of
# an honest zero-support value. stage_target_context resolves curr
# independently of settlement (from the target parquet's own rows in focused
# mode, or by reusing the already-staged race_history in offline mode), and
# stage_horse_context / append_features_sql now join on the FULL race key
# (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, horse)
# instead of the old race_date-only key.


def test_stage_target_context_offline_reuses_race_history_no_new_pg_scan() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_target_context(RecordingConn(), "dummy.parquet", False)
    body = " ".join(sql_calls)
    # Offline mode reuses the already-staged race_history table directly --
    # no new PostgreSQL query (no ``pg.`` reference at all) and no re-read of
    # the input parquet.
    assert "from race_history rh" in body
    assert "pg." not in body
    assert "read_parquet" not in body


def test_stage_target_context_focused_uses_scoped_point_lookup_not_historical_scan() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_target_context(RecordingConn(), "dummy.parquet", True)
    body = " ".join(sql_calls)
    assert "from read_parquet" in body
    assert "left join pg.race_entry_corner_features rec" in body
    assert "rec.ketto_toroku_bango = b.ketto_toroku_bango" in body
    # A narrow point lookup keyed on the exact target race(s) already present
    # in the input parquet -- not a second *historical* scan (no date-range
    # filter, no finish_position filter, unlike stage_race_history).
    assert "race_date >=" not in body
    assert "finish_position is not null" not in body


def test_main_calls_stage_race_history_and_stage_target_context_each_once() -> None:
    import inspect

    source = inspect.getsource(subject.main)
    assert source.count("stage_race_history(") == 1
    assert source.count("stage_target_context(") == 1


def test_stage_target_context_offline_builds_from_race_history_and_pedigree() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            ('jra', '20240101', '2024', '0101', '06', '11', 'horse_a',
             'J1'::varchar, 1200::integer, '24'::varchar, 'A'::varchar)
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, kishumei_ryakusho, kyori, track_code,
               grade_code)
        """
    )
    con.execute(
        """
        create temp table horse_pedigree as
        select * from (values ('horse_a', 'sire_a', 'damsire_a'))
        as v(ketto_toroku_bango, sire_id, damsire_id)
        """
    )
    subject.stage_target_context(con, "unused.parquet", False)
    row = con.execute(
        """
        select source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, kyori, track_code, grade_code,
               kishumei_ryakusho, sire_id, damsire_id
        from target_context
        """
    ).fetchone()
    con.close()
    assert row == (
        "jra", "20240101", "2024", "0101", "06", "11", "horse_a",
        1200, "24", "A", "J1", "sire_a", "damsire_a",
    )


def test_stage_target_context_focused_resolves_jockey_for_unsettled_target_row(
    tmp_path: Path,
) -> None:
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra', '2024', '0122', '06', '14', 'horse_a', 'J9'::varchar,
             cast(null as integer))
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, kishumei_ryakusho, finish_position)
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
          values ('jra', '2024', '0122', '06', '14', 'horse_a', '20240122', 2024,
                  1300::integer, '24'::varchar, 'A'::varchar)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year, kyori, track_code, grade_code)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    subject.stage_target_context(con, input_glob, True)
    row = con.execute(
        "select kyori, track_code, kishumei_ryakusho, sire_id, damsire_id from target_context"
    ).fetchone()
    con.close()
    # The target race's own finish_position in pg is NULL (genuinely
    # unsettled -- it has never been run) yet the jockey still resolves,
    # because the lookup is not filtered by finish_position.
    assert row == (1300, "24", "J9", "sire_a", "damsire_a")


def test_stage_target_context_focused_blank_jockey_resolves_to_null(
    tmp_path: Path,
) -> None:
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra', '2024', '0122', '06', '14', 'horse_a', ''::varchar,
             cast(null as integer))
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, kishumei_ryakusho, finish_position)
        """
    )
    con.execute(
        "create temp table horse_pedigree(ketto_toroku_bango varchar, sire_id varchar, damsire_id varchar)"
    )
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values ('jra', '2024', '0122', '06', '14', 'horse_a', '20240122', 2024,
                  1300::integer, '24'::varchar, 'A'::varchar)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year, kyori, track_code, grade_code)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    subject.stage_target_context(con, input_glob, True)
    row = con.execute("select kishumei_ryakusho from target_context").fetchone()
    con.close()
    assert row == (None,)


def test_stage_horse_context_known_zero_support_yields_zero_not_null() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table target_context as
        select * from (
          values ('jra', '20240201', '2024', '0201', '09', '05', 'horse_a',
                  1600::integer, '24'::varchar, 'J1'::varchar,
                  cast(null as varchar), cast(null as varchar))
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, kyori, track_code,
               kishumei_ryakusho, sire_id, damsire_id)
        """
    )
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values ('jra', '20240101', 'horse_a', '06', 1600::integer,
                  '24'::varchar, 'J1'::varchar, 3::integer)
        ) as v(source, race_date, ketto_toroku_bango, keibajo_code, kyori,
               track_code, kishumei_ryakusho, finish_position)
        """
    )
    subject.stage_horse_context(con)
    row = con.execute(
        "select same_keibajo_starts, same_keibajo_p2 from horse_context"
    ).fetchone()
    con.close()
    # The horse's only past race was at a DIFFERENT keibajo (06) than the
    # target's own (09) -- known context (target_context has a row for this
    # curr key), zero qualifying prior rows -> denominator 0, not NULL.
    assert row == (0, 0)


def test_stage_horse_context_strict_prior_date_only_excludes_same_day() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table target_context as
        select * from (
          values ('jra', '20240301', '2024', '0301', '06', '01', 'horse_a',
                  1600::integer, '24'::varchar, 'J1'::varchar,
                  cast(null as varchar), cast(null as varchar))
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, kyori, track_code,
               kishumei_ryakusho, sire_id, damsire_id)
        """
    )
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            -- same-day past row (race_date equal to curr's) must NOT count.
            ('jra', '20240301', 'horse_a', '06', 1600::integer, '24'::varchar, 'J1'::varchar, 2::integer),
            -- one day earlier must count.
            ('jra', '20240229', 'horse_a', '06', 1600::integer, '24'::varchar, 'J1'::varchar, 1::integer)
        ) as v(source, race_date, ketto_toroku_bango, keibajo_code, kyori,
               track_code, kishumei_ryakusho, finish_position)
        """
    )
    subject.stage_horse_context(con)
    row = con.execute(
        "select same_keibajo_starts, same_keibajo_p2 from horse_context"
    ).fetchone()
    con.close()
    assert row == (1, 0)


def test_stage_horse_context_blank_track_never_matches_even_when_both_blank() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table target_context as
        select * from (
          values ('jra', '20240301', '2024', '0301', '06', '01', 'horse_a',
                  1600::integer, cast(null as varchar), 'J1'::varchar,
                  cast(null as varchar), cast(null as varchar))
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, kyori, track_code,
               kishumei_ryakusho, sire_id, damsire_id)
        """
    )
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values ('jra', '20240101', 'horse_a', '06', 1600::integer,
                  cast(null as varchar), 'J1'::varchar, 2::integer)
        ) as v(source, race_date, ketto_toroku_bango, keibajo_code, kyori,
               track_code, kishumei_ryakusho, finish_position)
        """
    )
    subject.stage_horse_context(con)
    row = con.execute(
        "select same_track_starts, same_track_p2 from horse_context"
    ).fetchone()
    con.close()
    # Both curr and past track_code are blank -- the old
    # ``coalesce(track_code, '')`` comparison let '' = '' count as a match;
    # the fix requires BOTH sides non-blank before comparing.
    assert row == (0, 0)


def test_stage_horse_context_same_track_matches_when_both_populated_share_first_char() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table target_context as
        select * from (
          values ('jra', '20240301', '2024', '0301', '06', '01', 'horse_a',
                  1600::integer, '24'::varchar, 'J1'::varchar,
                  cast(null as varchar), cast(null as varchar))
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, kyori, track_code,
               kishumei_ryakusho, sire_id, damsire_id)
        """
    )
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values ('jra', '20240101', 'horse_a', '06', 1600::integer,
                  '23'::varchar, 'J1'::varchar, 2::integer)
        ) as v(source, race_date, ketto_toroku_bango, keibajo_code, kyori,
               track_code, kishumei_ryakusho, finish_position)
        """
    )
    subject.stage_horse_context(con)
    row = con.execute(
        "select same_track_starts, same_track_p2 from horse_context"
    ).fetchone()
    con.close()
    # '24' and '23' share the first character '2' (surface) -- a genuine
    # match despite differing full codes. Positive control proving the
    # nonblank guard doesn't also break legitimate matches.
    assert row == (1, 1)


def test_stage_horse_context_pair_requires_nonblank_curr_jockey() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table target_context as
        select * from (
          values
            ('jra', '20240301', '2024', '0301', '06', '01', 'horse_a',
              1600::integer, '24'::varchar, cast(null as varchar),
              cast(null as varchar), cast(null as varchar)),
            ('jra', '20240301', '2024', '0301', '06', '02', 'horse_a',
              1600::integer, '24'::varchar, 'J1'::varchar,
              cast(null as varchar), cast(null as varchar))
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, kyori, track_code,
               kishumei_ryakusho, sire_id, damsire_id)
        """
    )
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values ('jra', '20240101', 'horse_a', '06', 1600::integer,
                  '24'::varchar, 'J1'::varchar, 2::integer)
        ) as v(source, race_date, ketto_toroku_bango, keibajo_code, kyori,
               track_code, kishumei_ryakusho, finish_position)
        """
    )
    subject.stage_horse_context(con)
    rows = con.execute(
        "select race_bango, pair_starts, pair_p2 from horse_context order by race_bango"
    ).fetchall()
    con.close()
    # race_bango=01's curr (target-row) jockey is blank/unknown -> pair_starts
    # stays 0 even though race_history has a matching-jockey past row.
    # race_bango=02's curr jockey (J1) is populated -> the same past row
    # counts. Same race_date/horse for both -- proves the jockey guard, not
    # a date/horse difference, drives the split.
    assert rows == [("01", 0, 0), ("02", 1, 1)]


def test_stage_horse_context_pair_trims_past_jockey_name_before_comparing() -> None:
    """Regression test: raw JVD jockey names are full/half-width-space-padded
    (e.g. '　佐藤　', with U+3000 IDEOGRAPHIC SPACE padding). curr's jockey
    comes from target_context, which is always trimmed
    (nullif(trim(rec.kishumei_ryakusho), '')). past comes straight from
    race_history, which is NEVER trimmed (stage_race_history selects
    rec.kishumei_ryakusho as-is). Comparing curr's trimmed value against
    past's raw, still-padded value without also trimming past would silently
    fail to match every padded name, even though it is the exact same
    jockey -- pre-fix, curr and past were both literally the SAME untrimmed
    race_history table, so raw=raw happened to match; that symmetry broke
    once curr moved to target_context's already-trimmed value, so past must
    be trimmed inline to restore the match."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table target_context as
        select * from (
          values ('jra', '20240201', '2024', '0201', '06', '09', 'horse_a',
                  1600::integer, '24'::varchar, '佐藤'::varchar,
                  cast(null as varchar), cast(null as varchar))
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, kyori, track_code,
               kishumei_ryakusho, sire_id, damsire_id)
        """
    )
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values ('jra', '20240101', 'horse_a', '06', 1600::integer,
                  '24'::varchar, '　佐藤　'::varchar, 2::integer)
        ) as v(source, race_date, ketto_toroku_bango, keibajo_code, kyori,
               track_code, kishumei_ryakusho, finish_position)
        """
    )
    subject.stage_horse_context(con)
    row = con.execute("select pair_starts, pair_p2 from horse_context").fetchone()
    con.close()
    assert row == (1, 1)


def test_stage_horse_context_full_race_key_isolation_no_cross_race_join() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table target_context as
        select * from (
          values
            ('jra', '20240301', '2024', '0301', '06', '01', 'horse_a',
              1600::integer, '24'::varchar, 'J1'::varchar,
              cast(null as varchar), cast(null as varchar)),
            ('jra', '20240301', '2024', '0301', '08', '02', 'horse_a',
              1600::integer, '31'::varchar, 'J1'::varchar,
              cast(null as varchar), cast(null as varchar))
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
               race_bango, ketto_toroku_bango, kyori, track_code,
               kishumei_ryakusho, sire_id, damsire_id)
        """
    )
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            ('jra', '20240101', 'horse_a', '06', 1600::integer, '24'::varchar, 'J1'::varchar, 2::integer),
            ('jra', '20240108', 'horse_a', '08', 1600::integer, '31'::varchar, 'J1'::varchar, 2::integer)
        ) as v(source, race_date, ketto_toroku_bango, keibajo_code, kyori,
               track_code, kishumei_ryakusho, finish_position)
        """
    )
    subject.stage_horse_context(con)
    rows = con.execute(
        "select race_bango, same_keibajo_starts, same_keibajo_p2 "
        "from horse_context order by race_bango"
    ).fetchall()
    con.close()
    # Two curr rows share the SAME horse and SAME race_date (the old
    # exact-date-only join's key) but have DIFFERENT keibajo_code/race_bango.
    # race_bango=01 (keibajo=06) must match only the 20240101 past race
    # (also keibajo=06); race_bango=02 (keibajo=08) must match only the
    # 20240108 past race (also keibajo=08). Neither leaks into the other.
    assert rows == [("01", 1, 1), ("02", 1, 1)]


def test_append_features_sql_hc_join_uses_full_race_key_not_race_date_only() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "hc.kaisai_nen = b.kaisai_nen" in sql
    assert "hc.kaisai_tsukihi = b.kaisai_tsukihi" in sql
    assert "hc.keibajo_code = b.keibajo_code" in sql
    assert "hc.race_bango = b.race_bango" in sql
    assert "hc.ketto_toroku_bango = b.ketto_toroku_bango" in sql
    assert "hc.race_date = b.race_date" not in sql


def test_append_features_sql_log1p_columns_present() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "as log1p_same_keibajo_starts" in sql
    assert "as log1p_same_distance_starts" in sql
    assert "as log1p_same_track_starts" in sql
    assert "as log1p_pair_starts" in sql


def test_append_features_sql_hp_hdg_joins_use_full_race_key_not_race_date_only() -> None:
    """stage_horse_pedigree_context / stage_horse_distance_grade had the SAME
    settled-only curr-resolution defect as the old horse_context (their
    ``pedigree_target``/``target`` CTEs sourced from race_history), plus the
    same race_date-only join in ``joined``. Both are fixed the same way:
    curr resolved via target_context, joined on the full race key."""
    sql = subject.append_features_sql("dummy.parquet")
    assert "hp.kaisai_nen = b.kaisai_nen" in sql
    assert "hp.kaisai_tsukihi = b.kaisai_tsukihi" in sql
    assert "hp.keibajo_code = b.keibajo_code" in sql
    assert "hp.race_bango = b.race_bango" in sql
    assert "hp.ketto_toroku_bango = b.ketto_toroku_bango" in sql
    assert "hp.race_date = b.race_date" not in sql
    assert "hdg.kaisai_nen = b.kaisai_nen" in sql
    assert "hdg.kaisai_tsukihi = b.kaisai_tsukihi" in sql
    assert "hdg.keibajo_code = b.keibajo_code" in sql
    assert "hdg.race_bango = b.race_bango" in sql
    assert "hdg.ketto_toroku_bango = b.ketto_toroku_bango" in sql
    assert "hdg.race_date = b.race_date" not in sql


def test_stage_horse_pedigree_context_sources_curr_from_target_context() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_horse_pedigree_context(RecordingConn())
    # pedigree_target must be built specifically from target_context (the
    # first executed statement), not from a fresh join against race_history
    # -- isolate that one statement rather than string-searching the whole
    # concatenated body, since "race_history" is a legitimate substring
    # elsewhere in this function's other statements (sire_kyori_cumul etc.
    # are consumed, not rebuilt, here).
    pedigree_target_sql = sql_calls[0]
    assert "create or replace temp table pedigree_target as" in pedigree_target_sql
    assert "from target_context" in pedigree_target_sql
    assert "race_history" not in pedigree_target_sql


def test_stage_horse_distance_grade_target_cte_sources_from_target_context() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_horse_distance_grade(RecordingConn())
    body = " ".join(sql_calls)
    assert "from target_context\n          where kyori is not null" in body


def test_upcoming_target_race_resolves_pedigree_and_distance_grade_context(
    tmp_path: Path,
) -> None:
    """End-to-end proof that stage_horse_pedigree_context /
    stage_horse_distance_grade resolve for a genuinely upcoming (never
    settled) target race, via the SAME target_context table horse_context
    uses -- the identical fix pattern extended to the other two
    exact-date-equality joins team-lead identified as the same defect
    family.

    horse_a (sire_id='sire_x', damsire NULL) has 3 COMPLETED historical
    races. The TARGET race (2024-02-01) is genuinely upcoming: its own row
    in pg.race_entry_corner_features has finish_position NULL.
    """
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra','20240101','2024','0101','06','11','horse_a','J1',2,1600,'24','A'),
            ('jra','20240108','2024','0108','06','12','horse_a','J1',1,1600,'24','A'),
            ('jra','20240115','2024','0115','06','13','horse_a','J1',2,2400,'24','B'),
            ('jra','20240201','2024','0201','06','09','horse_a','J1',cast(null as integer),1600,'24','A')
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, kishumei_ryakusho, finish_position, kyori, track_code,
               grade_code)
        """
    )
    for col, typ in (
        ("tansho_odds", "double"),
        ("tansho_ninkijun", "integer"),
        ("shusso_tosu", "integer"),
        ("time_sa", "double"),
        ("chokyoshimei_ryakusho", "varchar"),
        ("banushimei", "varchar"),
    ):
        con.execute(f"alter table pg.race_entry_corner_features add column {col} {typ}")
    con.execute(
        """
        create temp table horse_pedigree as
        select * from (values ('horse_a', 'sire_x', cast(null as varchar)))
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
          values ('jra', '2024', '0201', '06', '09', 'horse_a', '20240201', 2024,
                  1::integer, 3.0::double, 1600::integer, '24'::varchar, 'A'::varchar)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year,
               tansho_ninkijun, tansho_odds, kyori, track_code, grade_code)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    subject.stage_target_entities(con, input_glob)
    subject.stage_race_history(con, "20000101", True)
    subject.stage_horse_near_miss(con)
    subject.stage_target_context(con, input_glob, True)
    subject.stage_horse_context(con)
    subject.stage_pedigree_cumulatives(con)
    subject.stage_horse_pedigree_context(con)
    subject.stage_horse_distance_grade(con)
    subject.stage_jockey_near_miss(con)

    sql = subject.append_features_sql(input_glob)
    row = con.execute(
        f"""
        select sire_distance_place2_rate, sire_grade_place2_rate,
          horse_distance_grade_place2_rate
        from ({sql})
        """
    ).fetchone()
    con.close()

    assert row is not None
    # sire_x's races within +/-200m of the target's kyori=1600: the
    # 20240101 race (P2) and 20240108 race (not P2) -- the 20240115 race
    # (kyori=2400) is excluded by distance tolerance. 2 starts, 1 P2 -> 0.5.
    assert row[0] == pytest.approx(0.5)
    # sire_x's races with the target's exact grade='A': the same two races
    # -- the 20240115 race (grade='B') is excluded. 2 starts, 1 P2 -> 0.5.
    assert row[1] == pytest.approx(0.5)
    # horse_a's OWN (kyori,grade) pair history mirrors the sire's in this
    # fixture (horse_a is sire_x's only contributor): same 2 starts, 1 P2.
    assert row[2] == pytest.approx(0.5)


def test_unknown_target_context_join_failure_yields_null_not_zero(
    tmp_path: Path,
) -> None:
    """A base row whose race key has no corresponding horse_context row
    (target-context/join failure) must propagate NULL for all four
    rates/log1p columns -- never silently coalesced to the known-zero-support
    value of 0.0. The SAME query also carries a horse with a resolved,
    known-zero-support (pair) feature, so both halves of the zero-vs-NULL
    distinction are exercised together."""
    con = duckdb.connect(":memory:")
    con.execute(
        "create or replace temp table race_history(source varchar, kaisai_nen varchar, "
        "kaisai_tsukihi varchar, keibajo_code varchar, race_bango varchar, "
        "ketto_toroku_bango varchar, kishumei_ryakusho varchar, "
        "tansho_ninkijun integer, shusso_tosu integer, race_date varchar)"
    )
    con.execute(
        """
        create or replace temp table horse_context as
        select * from (
          values ('jra', '2024', '0201', '06', '09', 'horse_a',
                  2::integer, 1::integer, 1::integer, 1::integer,
                  1::integer, 1::integer, 1::integer, 0::integer)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango,
               same_keibajo_starts, same_keibajo_p2,
               same_distance_starts, same_distance_p2,
               same_track_starts, same_track_p2,
               pair_starts, pair_p2)
        """
    )
    con.execute(
        "create temp table horse_near_miss(source varchar, ketto_toroku_bango varchar, "
        "race_date varchar, past_starts integer, past_p1_count integer, "
        "past_p2_count integer, past_p2_avg_timesa double, "
        "recent_p2_count_5 integer, recent_p2_avg_timesa_5 double)"
    )
    con.execute(
        "create temp table horse_pedigree_context(source varchar, kaisai_nen varchar, "
        "kaisai_tsukihi varchar, keibajo_code varchar, race_bango varchar, "
        "ketto_toroku_bango varchar, sire_distance_starts integer, sire_distance_p2 integer, "
        "sire_grade_starts integer, sire_grade_p2 integer, "
        "damsire_distance_starts integer, damsire_distance_p2 integer)"
    )
    con.execute(
        "create temp table horse_distance_grade(source varchar, kaisai_nen varchar, "
        "kaisai_tsukihi varchar, keibajo_code varchar, race_bango varchar, "
        "ketto_toroku_bango varchar, dg_starts integer, dg_p2 integer)"
    )
    con.execute(
        "create temp table jockey_near_miss(source varchar, kishumei_ryakusho varchar, "
        "race_date varchar, past_rides integer, past_jockey_p2_count integer)"
    )

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2024', '0201', '06', '09', 'horse_a', '20240201', 2024,
              1::integer, 3.0::double),
            ('jra', '2024', '0201', '06', '10', 'horse_b', '20240201', 2024,
              2::integer, 5.0::double)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year,
               tansho_ninkijun, tansho_odds)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    sql = subject.append_features_sql(input_glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
          same_keibajo_place2_rate, log1p_same_keibajo_starts,
          same_distance_place2_rate, jockey_horse_pair_place2_rate
        from ({sql}) order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()

    assert rows[0][0] == "horse_a"
    assert rows[0][1] == pytest.approx(0.5)
    assert rows[0][2] == pytest.approx(1.0986122886681098)
    assert rows[0][3] == pytest.approx(1.0)
    assert rows[0][4] == pytest.approx(0.0)

    assert rows[1][0] == "horse_b"
    assert rows[1][1] is None
    assert rows[1][2] is None
    assert rows[1][3] is None
    assert rows[1][4] is None


def test_upcoming_target_race_resolves_horse_context_via_target_context(
    tmp_path: Path,
) -> None:
    """End-to-end proof for the target-aware parity repair, exercising the
    full staging chain in the same order as main().

    horse_a has 5 COMPLETED historical races at various keibajo/kyori/track/
    jockey combinations. The TARGET race (2024-02-01, keibajo=06, kyori=1600,
    track='24', jockey='J1') is genuinely upcoming: its own row in
    pg.race_entry_corner_features has finish_position NULL (never settled).
    The old exact-date-equality horse_context join could never resolve this
    target; the fix resolves curr's own keibajo/kyori/track/jockey directly
    from the target race itself, independent of settlement, and produces
    honest, arithmetically-verified rates for all four features plus their
    log1p support columns.
    """
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra','20240101','2024','0101','06','11','horse_a','J1',2,1600,'24'),
            ('jra','20240108','2024','0108','07','12','horse_a','J2',2,1801,'24'),
            ('jra','20240115','2024','0115','06','13','horse_a','J1',1,2400,'10'),
            ('jra','20240122','2024','0122','06','14','horse_a','J3',2,1550,'24'),
            ('jra','20240129','2024','0129','09','15','horse_a','J1',2,1900,'31'),
            ('jra','20240201','2024','0201','06','09','horse_a','J1',cast(null as integer),1600,'24')
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, kishumei_ryakusho, finish_position, kyori, track_code)
        """
    )
    for col, typ in (
        ("grade_code", "varchar"),
        ("tansho_odds", "double"),
        ("tansho_ninkijun", "integer"),
        ("shusso_tosu", "integer"),
        ("time_sa", "double"),
        ("chokyoshimei_ryakusho", "varchar"),
        ("banushimei", "varchar"),
    ):
        con.execute(f"alter table pg.race_entry_corner_features add column {col} {typ}")
    con.execute(
        "create temp table horse_pedigree(ketto_toroku_bango varchar, sire_id varchar, damsire_id varchar)"
    )

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values ('jra', '2024', '0201', '06', '09', 'horse_a', '20240201', 2024,
                  1::integer, 3.0::double, 1600::integer, '24'::varchar, 'A'::varchar)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year,
               tansho_ninkijun, tansho_odds, kyori, track_code, grade_code)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    subject.stage_target_entities(con, input_glob)
    subject.stage_race_history(con, "20000101", True)
    subject.stage_horse_near_miss(con)
    subject.stage_target_context(con, input_glob, True)
    subject.stage_horse_context(con)
    subject.stage_pedigree_cumulatives(con)
    subject.stage_horse_pedigree_context(con)
    subject.stage_horse_distance_grade(con)
    subject.stage_jockey_near_miss(con)

    sql = subject.append_features_sql(input_glob)
    row = con.execute(
        f"""
        select
          same_keibajo_place2_rate, log1p_same_keibajo_starts,
          same_distance_place2_rate, log1p_same_distance_starts,
          same_track_place2_rate, log1p_same_track_starts,
          jockey_horse_pair_place2_rate, log1p_pair_starts
        from ({sql})
        """
    ).fetchone()
    con.close()

    assert row is not None
    # same_keibajo (target keibajo=06): matches R1(06),R3(06),R4(06) = 3
    # starts; of these R1(fp2) is P2 and R4(fp2) is P2, R3(fp1) is not -> 2/3.
    assert row[0] == pytest.approx(2.0 / 3.0)
    assert row[1] == pytest.approx(1.3862943611198906)
    # same_distance (target kyori=1600, tol 200): matches R1(1600),R4(1550);
    # R2(1801, diff201>200) excluded. Both matches are P2 -> rate 1.0.
    assert row[2] == pytest.approx(1.0)
    assert row[3] == pytest.approx(1.0986122886681098)
    # same_track (target track='24' -> '2'): matches R1,R2,R4 (all '24');
    # all three happen to be P2 -> rate 1.0.
    assert row[4] == pytest.approx(1.0)
    assert row[5] == pytest.approx(1.3862943611198906)
    # pair (target jockey='J1'): matches R1,R3,R5; R1(P2)+R5(P2) are P2,
    # R3(fp1) is not -> rate 2/3.
    assert row[6] == pytest.approx(2.0 / 3.0)
    assert row[7] == pytest.approx(1.3862943611198906)


def test_target_context_wins_over_horse_last_finished_row_when_differ(
    tmp_path: Path,
) -> None:
    """curr's OWN race key (keibajo_code) must come from the TARGET race
    itself, not be inherited from the horse's last actual (finished) race.
    horse_a's entire history is at keibajo=01; the TARGET race is at a
    DIFFERENT keibajo=08 the horse has never run at. If curr's keibajo were
    (incorrectly) carried over from the horse's last finished row,
    same_keibajo_starts would show 3 (all history matches keibajo=01);
    resolved correctly from the target's own row, it must show 0.
    """
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra','20240101','2024','0101','01','11','horse_a','J1',2,1600,'24'),
            ('jra','20240108','2024','0108','01','12','horse_a','J1',1,1600,'24'),
            ('jra','20240115','2024','0115','01','13','horse_a','J1',2,1600,'24'),
            ('jra','20240201','2024','0201','08','09','horse_a','J1',cast(null as integer),1600,'24')
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, kishumei_ryakusho, finish_position, kyori, track_code)
        """
    )
    for col, typ in (
        ("grade_code", "varchar"),
        ("tansho_odds", "double"),
        ("tansho_ninkijun", "integer"),
        ("shusso_tosu", "integer"),
        ("time_sa", "double"),
        ("chokyoshimei_ryakusho", "varchar"),
        ("banushimei", "varchar"),
    ):
        con.execute(f"alter table pg.race_entry_corner_features add column {col} {typ}")
    con.execute(
        "create temp table horse_pedigree(ketto_toroku_bango varchar, sire_id varchar, damsire_id varchar)"
    )

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values ('jra', '2024', '0201', '08', '09', 'horse_a', '20240201', 2024,
                  1::integer, 3.0::double, 1600::integer, '24'::varchar, 'A'::varchar)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year,
               tansho_ninkijun, tansho_odds, kyori, track_code, grade_code)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    subject.stage_target_entities(con, input_glob)
    subject.stage_race_history(con, "20000101", True)
    subject.stage_horse_near_miss(con)
    subject.stage_target_context(con, input_glob, True)
    subject.stage_horse_context(con)
    subject.stage_pedigree_cumulatives(con)
    subject.stage_horse_pedigree_context(con)
    subject.stage_horse_distance_grade(con)
    subject.stage_jockey_near_miss(con)

    sql = subject.append_features_sql(input_glob)
    row = con.execute(
        f"select same_keibajo_place2_rate, log1p_same_keibajo_starts from ({sql})"
    ).fetchone()
    con.close()

    assert row is not None
    assert row[0] == pytest.approx(0.0)
    assert row[1] == pytest.approx(0.0)
