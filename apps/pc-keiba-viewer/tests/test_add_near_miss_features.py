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

    def execute(self, sql: str) -> FakeConn:
        self.statements.append(sql)
        return self

    def fetchall(self) -> list[tuple[str]]:
        return [("tansho_ninkijun",), ("shusso_tosu",)]


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


def test_parse_args_accepts_bounded_target_period(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--target-from-date",
            "20250101",
            "--target-to-date",
            "20250131",
        ]
    )
    assert args.target_from_date == "20250101"
    assert args.target_to_date == "20250131"


@pytest.mark.parametrize("value", ["20250229", "2025-01-01", "2025011"])
def test_validate_yyyymmdd_rejects_invalid_dates(value: str) -> None:
    with pytest.raises(ValueError, match="valid YYYYMMDD"):
        subject.validate_yyyymmdd(value, "--target-from-date")


def test_target_date_filter_sql_is_inclusive() -> None:
    assert subject.target_date_filter_sql("b", "20250101", "20250131") == (
        "b.race_date >= '20250101' and b.race_date <= '20250131'"
    )
    assert subject.target_date_filter_sql("b", None, None) == "true"


def test_offline_bulk_requires_bounded_target_period() -> None:
    with pytest.raises(ValueError, match="offline bulk mode requires"):
        subject.require_bounded_bulk(False, None, None)
    subject.require_bounded_bulk(False, "20250101", "20250131")
    subject.require_bounded_bulk(True, None, None)


def test_drop_temp_tables_releases_only_named_intermediates() -> None:
    con = duckdb.connect(":memory:")
    con.execute("create temp table obsolete as select 1 as value")
    con.execute("create temp table retained as select 2 as value")
    subject.drop_temp_tables(con, ("obsolete", "already_absent"))
    assert con.execute("select value from retained").fetchone() == (2,)
    with pytest.raises(duckdb.CatalogException):
        con.execute("select * from obsolete")


def test_optional_parquet_column_sql_uses_typed_null_for_missing_column(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "minimal.parquet"
    con = duckdb.connect(":memory:")
    con.execute(f"copy (select 1 as present) to '{parquet_path.as_posix()}'")
    assert (
        subject.optional_parquet_column_sql(
            con, parquet_path.as_posix(), "missing", "bigint"
        )
        == "cast(null as bigint) as missing"
    )
    assert (
        subject.optional_parquet_column_sql(
            con, parquet_path.as_posix(), "present", "integer"
        )
        == "b.present"
    )


def test_append_features_sql_excludes_base_meta_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "exclude (kishumei_ryakusho, tansho_ninkijun, shusso_tosu)" in sql


def test_append_features_sql_bounded_base_defines_filter_alias() -> None:
    sql = subject.append_features_sql(
        "dummy.parquet",
        target_from_date="20250101",
        target_to_date="20250131",
    )
    assert "hive_partitioning=true) b" in sql
    assert "where b.race_date >= '20250101'" in sql


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
              'JOCKEY_A'::varchar, 1::integer, 12::integer, 3.0::double,
              1200::integer, '1'::varchar, 'C'::varchar),
            ('nar', '2025', '0415', '54', '11', 'horse_b', '20250415', 2025,
              'JOCKEY_B'::varchar, 2::integer, 12::integer, 6.0::double,
              1200::integer, '1'::varchar, 'C'::varchar)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year,
          kishumei_ryakusho, tansho_ninkijun, shusso_tosu, tansho_odds,
          kyori, track_code, grade_code
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
              1200::integer, '1'::varchar, 'C'::varchar),
            ('nar', '2025', '0415', '35', '01', 'horse_b', '20250415', 2025,
              1200::integer, '1'::varchar, 'C'::varchar)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year, kyori, track_code, grade_code
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


def test_append_features_sql_bulk_uses_exact_shifted_history_joins() -> None:
    sql = subject.append_features_sql("dummy.parquet", focused_target=False)
    assert "left join horse_near_miss_target h" in sql
    assert "b.race_date = h.race_date" in sql
    assert "left join jockey_near_miss_target j" in sql
    assert "b.race_date = j.race_date" in sql
    assert "asof left join horse_near_miss h" not in sql
    assert "asof left join jockey_near_miss j" not in sql


def test_bulk_prior_date_lookups_match_strict_asof_with_same_day_rows() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table horse_near_miss as
        select * from (values
          ('nar','horse','20240101',1,0,1,null,0,null),
          ('nar','horse','20240101',2,1,1,2.0,1,2.0),
          ('nar','horse','20240105',3,1,2,2.0,0,null),
          ('nar','horse','20240110',4,2,2,1.5,2,1.5)
        ) v(source,ketto_toroku_bango,race_date,past_starts,past_p2_count,
            past_p1_count,past_p2_avg_timesa,recent_p2_count_5,
            recent_p2_avg_timesa_5)
        """
    )
    con.execute(
        """
        create temp table jockey_near_miss as
        select * from (values
          ('nar','J','20240101',2,1),
          ('nar','J','20240105',3,1),
          ('nar','J','20240110',4,2)
        ) v(source,kishumei_ryakusho,race_date,past_rides,past_jockey_p2_count)
        """
    )
    subject.stage_bulk_prior_date_lookups(con)
    horse = con.execute(
        """select race_date,past_starts,past_p2_count,recent_p2_count_5,
          recent_p2_avg_timesa_5
        from horse_near_miss_target order by race_date"""
    ).fetchall()
    jockey = con.execute(
        """select race_date,past_rides,past_jockey_p2_count
        from jockey_near_miss_target order by race_date"""
    ).fetchall()
    con.close()
    assert horse == [
        ("20240105", 2, 1, 1, 2.0),
        ("20240110", 3, 1, 0, None),
    ]
    assert jockey == [("20240105", 2, 1), ("20240110", 3, 1)]


def test_append_features_sql_does_not_share_unknown_horse_history() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    guard = (
        "and nullif(trim(b.ketto_toroku_bango), '0000000000') is not null"
    )
    assert sql.count(guard) == 4


def test_append_features_sql_unknown_horse_keeps_one_row_and_null_horse_stats(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "unknown-horse"
    input_dir.mkdir()
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table seed as
        select
          'nar'::varchar as source, '2025'::varchar as kaisai_nen,
          '0406'::varchar as kaisai_tsukihi, '46'::varchar as keibajo_code,
          '01'::varchar as race_bango, '0000000000'::varchar as ketto_toroku_bango,
          '20250406'::varchar as race_date, 2025::integer as race_year,
          'JOCKEY_ZERO'::varchar as kishumei_ryakusho,
          1::integer as tansho_ninkijun, 10::integer as shusso_tosu,
          3.0::double as tansho_odds, 1300::integer as kyori,
          '1'::varchar as track_code, 'C'::varchar as grade_code
        """
    )
    con.execute(
        f"copy seed to '{input_dir.as_posix()}' "
        "(format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"
    _seed_join_temps(con)
    con.execute(
        """
        insert into race_history values
          ('nar', '2025', '0406', '46', '01', '0000000000',
           'JOCKEY_ZERO', 1, 10, 3.0)
        """
    )
    con.execute(
        """
        insert into horse_near_miss values
          ('nar', '0000000000', '20250405', 20, 4, 6, 0.2, 3, 0.1)
        """
    )
    con.execute(
        """
        insert into horse_context values
          ('nar', '2025', '0406', '46', '01', '0000000000',
           10, 5, 10, 5, 10, 5, 10, 5)
        """
    )
    con.execute(
        """
        insert into horse_pedigree_context values
          ('nar', '2025', '0406', '46', '01', '0000000000', 10, 5, 10, 5, 10, 5)
        """
    )
    con.execute(
        """
        insert into horse_distance_grade values
          ('nar', '2025', '0406', '46', '01', '0000000000', 10, 5)
        """
    )
    con.execute(
        """
        insert into jockey_near_miss values
          ('nar', 'JOCKEY_ZERO', '20250405', 10, 2)
        """
    )
    rows = con.execute(
        f"""
        select career_place2_rate, same_keibajo_place2_rate,
               sire_distance_place2_rate, horse_distance_grade_place2_rate,
               jockey_career_place2_rate
        from ({subject.append_features_sql(glob)})
        """
    ).fetchall()
    con.close()
    assert rows == [(None, None, None, None, 0.2)]


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
    assert "rec.source in (select distinct source from target_entities)" in sql
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
    assert "rec.kaisai_nen >= substring('20240101', 1, 4)" in body
    assert "rec.source in (select distinct source from target_entities)" in body
    assert "target_entities" in body


def test_stage_race_history_bulk_prunes_unrequested_source() -> None:
    conn = FakeConn()
    subject.stage_race_history(
        conn, "20240101", offline_sources=frozenset({"nar"})
    )
    body = " ".join(conn.statements)
    assert "and rec.source in ('nar')" in body


def test_stage_race_history_bulk_rejects_invalid_or_empty_source_scope() -> None:
    conn = FakeConn()
    with pytest.raises(ValueError, match="source scope cannot be empty"):
        subject.stage_race_history(conn, "20240101", offline_sources=frozenset())
    with pytest.raises(ValueError, match="unsupported offline race-history sources"):
        subject.stage_race_history(
            conn, "20240101", offline_sources=frozenset({"other"})
        )


def test_raw_catalog_race_history_sql_pushes_filters_into_source_branches() -> None:
    sql = subject.raw_catalog_race_history_sql("20100101")
    assert "from pg.jvd_se se" in sql
    assert "from pg.nvd_se se" in sql
    assert sql.count("se.kaisai_nen >= substring('20100101', 1, 4)") == 2
    assert sql.count("< (select max(race_date) from target_current)") == 2
    assert sql.count("te.ketto_toroku_bango = se.ketto_toroku_bango") == 2
    assert sql.count("nullif(trim(se.kishumei_ryakusho), '')") == 2
    assert sql.count("hp.ketto_toroku_bango = se.ketto_toroku_bango") == 2
    assert "from pg.race_entry_corner_features" not in sql
    assert "count_se" not in sql
    assert sql.count("cast(null as integer) as shusso_tosu") == 2


def test_raw_catalog_race_history_sql_only_reads_target_source_branch() -> None:
    jra_sql = subject.raw_catalog_race_history_sql("20100101", frozenset({"jra"}))
    nar_sql = subject.raw_catalog_race_history_sql("20100101", frozenset({"nar"}))

    assert "from pg.jvd_se se" in jra_sql
    assert "from pg.nvd_se se" not in jra_sql
    assert "from pg.nvd_se se" in nar_sql
    assert "from pg.jvd_se se" not in nar_sql


def test_raw_catalog_race_history_sql_empty_source_scope_is_fail_safe() -> None:
    sql = subject.raw_catalog_race_history_sql("20100101", frozenset())

    assert "from pg.jvd_se se" in sql
    assert "from pg.nvd_se se" in sql


def test_stage_race_history_raw_catalog_filters_entities_and_future_rows() -> None:
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.jvd_se as
        select * from (
          values
            ('2024','0101','06','01','target','01','01','0010','0030','J0'),
            ('2024','0102','06','02','jockey_horse','02','02','0020','0040','J1'),
            ('2024','0103','06','03','sire_child','03','02','0030','0050','J2'),
            ('2024','0104','06','04','irrelevant','04','01','0040','0060','J3'),
            ('2024','0201','06','05','target','01','00','0000','0000','J1'),
            ('2024','0202','06','06','target','01','01','0010','0030','J1')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, umaban, kakutei_chakujun,
               time_sa, tansho_odds, kishumei_ryakusho)
        """
    )
    con.execute("alter table pg.jvd_se add column tansho_ninkijun varchar default '01'")
    con.execute(
        """
        create table pg.jvd_ra as
        select kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               '04'::varchar as shusso_tosu, '1600'::varchar as kyori,
               '1'::varchar as track_code, 'A'::varchar as grade_code
        from pg.jvd_se
        """
    )
    con.execute("create table pg.nvd_se as select * from pg.jvd_se where false")
    con.execute("create table pg.nvd_ra as select * from pg.jvd_ra where false")
    con.execute(
        """
        create temp table target_current as
        select '20240201'::varchar as race_date
        """
    )
    con.execute(
        """
        create temp table target_entities as
        select 'jra'::varchar as source, 'target'::varchar as ketto_toroku_bango,
               'J1'::varchar as kishumei_ryakusho, 'sire_x'::varchar as sire_id,
               cast(null as varchar) as damsire_id
        """
    )
    con.execute(
        """
        create temp table horse_pedigree as
        select * from (
          values
            ('target', 'sire_x', null),
            ('jockey_horse', 'sire_y', null),
            ('sire_child', 'sire_x', null),
            ('irrelevant', 'sire_z', null)
        ) as v(ketto_toroku_bango, sire_id, damsire_id)
        """
    )

    subject.stage_race_history(con, "20100101", True, True)
    rows = con.execute(
        "select ketto_toroku_bango from race_history order by race_date"
    ).fetchall()
    con.close()

    assert rows == [("target",), ("jockey_horse",), ("sire_child",)]


def test_stage_target_entities_raw_catalog_reads_jockey_from_se(
    tmp_path: Path,
) -> None:
    con = duckdb.connect(":memory:")
    input_glob = _seed_base_parquet_without_jockey(tmp_path / "input")
    _seed_pg_race_entry_corner_features(con)
    con.execute(
        """
        create table pg.jvd_se(
          kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
          race_bango varchar, ketto_toroku_bango varchar,
          kishumei_ryakusho varchar
        )
        """
    )
    con.execute(
        """
        create table pg.nvd_se as
        select * from (
          values
            ('2025','0415','35','01','horse_a',' RAW_JOCKEY_A '),
            ('2025','0415','35','01','horse_b','RAW_JOCKEY_B')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, kishumei_ryakusho)
        """
    )
    con.execute(
        """
        create temp table horse_pedigree as
        select * from (
          values ('horse_a', 'sire_a', 'dam_a'), ('horse_b', 'sire_b', 'dam_b')
        ) as v(ketto_toroku_bango, sire_id, damsire_id)
        """
    )

    subject.stage_target_entities(con, input_glob, True)
    rows = con.execute(
        "select ketto_toroku_bango, kishumei_ryakusho "
        "from target_entities order by ketto_toroku_bango"
    ).fetchall()
    con.close()

    assert rows == [("horse_a", "RAW_JOCKEY_A"), ("horse_b", "RAW_JOCKEY_B")]


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


def test_stage_target_context_focused_reuses_materialized_current_rows() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_target_context(RecordingConn(), "dummy.parquet", True)
    body = " ".join(sql_calls)
    assert "from target_current" in body
    assert "read_parquet" not in body
    assert "pg." not in body


def test_focused_target_staging_uses_one_current_catalog_lookup() -> None:
    conn = FakeConn()
    subject.stage_target_entities(conn, "dummy.parquet")
    subject.stage_target_context(conn, "dummy.parquet", True)
    body = " ".join(conn.statements)
    assert body.count("pg.race_entry_corner_features") == 1


def test_stage_race_history_pushes_inclusive_upper_date_into_pg_scan() -> None:
    conn = FakeConn()
    subject.stage_race_history(
        conn,
        "20100101",
        offline_sources=frozenset({"nar"}),
        to_date="20250131",
    )
    body = " ".join(conn.statements)
    assert "rec.race_date <= '20250131'" in body
    assert "rec.kaisai_nen <= substring('20250131', 1, 4)" in body
    assert "rec.source in ('nar')" in body


def test_postgres_entity_scoped_query_pushes_source_entities_and_dates() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table target_entities as select * from (
          values
            ('nar', 'horse_n', 'O''Brien', 'sire_n', cast(null as varchar)),
            ('jra', 'horse_j', cast(null as varchar), cast(null as varchar), 'dam_j')
        ) as v(source, ketto_toroku_bango, kishumei_ryakusho, sire_id, damsire_id)
        """
    )
    sql = subject.postgres_entity_scoped_race_history_query(
        con, "20100101", "20250131"
    )
    assert "rec.race_date >= '20100101'" in sql
    assert "rec.race_date <= '20250131'" in sql
    assert "rec.source = 'nar'" in sql
    assert "rec.source = 'jra'" in sql
    assert "rec.ketto_toroku_bango in ('horse_n')" in sql
    assert "rec.kishumei_ryakusho in ('O''Brien')" in sql
    assert "hp.damsire_id in ('dam_j')" in sql


def test_postgres_entity_scoped_query_rejects_empty_or_unknown_source() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table target_entities(
          source varchar, ketto_toroku_bango varchar,
          kishumei_ryakusho varchar, sire_id varchar, damsire_id varchar
        )
        """
    )
    with pytest.raises(ValueError, match="target cannot be empty"):
        subject.postgres_entity_scoped_race_history_query(
            con, "20100101", "20250131"
        )
    con.execute(
        "insert into target_entities values ('other', 'horse', null, null, null)"
    )
    with pytest.raises(ValueError, match="unsupported entity-scoped"):
        subject.postgres_entity_scoped_race_history_query(
            con, "20100101", "20250131"
        )


def test_postgres_query_table_sql_escapes_remote_query_literal() -> None:
    sql = subject.postgres_query_table_sql("select 'quoted' as value")
    assert "postgres_query('pg', 'select ''quoted'' as value')" in sql


def test_project_scoped_race_history_drops_output_only_columns() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table race_history as select
          'nar'::varchar as source, '20250101'::varchar as race_date,
          '35'::varchar as keibajo_code, 'horse'::varchar as ketto_toroku_bango,
          'jockey'::varchar as kishumei_ryakusho, 2::integer as finish_position,
          0.1::double as time_sa, 1200::integer as kyori,
          '11'::varchar as track_code, 'A'::varchar as grade_code,
          '2025'::varchar as kaisai_nen, '0101'::varchar as kaisai_tsukihi,
          '01'::varchar as race_bango, 3.2::double as tansho_odds,
          1::integer as tansho_ninkijun, 12::integer as shusso_tosu
        """
    )
    subject.project_scoped_race_history(con)
    columns = [row[0] for row in con.execute("describe race_history").fetchall()]
    assert columns == [
        "source",
        "race_date",
        "keibajo_code",
        "ketto_toroku_bango",
        "kishumei_ryakusho",
        "finish_position",
        "time_sa",
        "kyori",
        "track_code",
        "grade_code",
    ]


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

    subject.stage_target_entities(con, input_glob)
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

    subject.stage_target_entities(con, input_glob)
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


def test_stage_horse_context_cumulative_asof_matches_reference_inequality_join() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table race_history as
        select * from (values
          ('nar','20240101','2024','0101','01','01','horse_a',' J1 ',2,1.0,1,1,8,1200,'24','A'),
          ('nar','20240105','2024','0105','02','02','horse_a','J2',1,1.0,1,1,8,1400,'14','A'),
          ('nar','20240110','2024','0110','01','03','horse_a','J1',2,1.0,1,1,8,1600,'25','B'),
          ('nar','20240115','2024','0115','01','04','horse_a','J1',1,1.0,1,1,8,1400,'24','A'),
          ('nar','20240115','2024','0115','02','05','horse_b',null,2,1.0,1,1,8,1800,'','A')
        ) v(source,race_date,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,
            ketto_toroku_bango,kishumei_ryakusho,finish_position,time_sa,
            tansho_odds,tansho_ninkijun,shusso_tosu,kyori,track_code,grade_code)
        """
    )
    con.execute(
        """
        create temp table target_context as
        select * from (values
          ('nar','20240115','2024','0115','01','04','horse_a',1400,'24','A','J1',null,null),
          ('nar','20240115','2024','0115','02','05','horse_b',1800,'','A',null,null,null)
        ) v(source,race_date,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,
            ketto_toroku_bango,kyori,track_code,grade_code,kishumei_ryakusho,
            sire_id,damsire_id)
        """
    )
    expected = con.execute(
        """
        select curr.race_bango,
          count(case when past.keibajo_code = curr.keibajo_code then 1 end),
          sum(case when past.keibajo_code = curr.keibajo_code
                    and past.finish_position = 2 then 1 else 0 end),
          count(case when curr.kyori is not null and past.kyori is not null
                       and abs(past.kyori - curr.kyori) <= 200 then 1 end),
          sum(case when curr.kyori is not null and past.kyori is not null
                    and abs(past.kyori - curr.kyori) <= 200
                    and past.finish_position = 2 then 1 else 0 end),
          count(case when nullif(trim(curr.track_code), '') is not null
                       and nullif(trim(past.track_code), '') is not null
                       and left(curr.track_code, 1) = left(past.track_code, 1)
                     then 1 end),
          sum(case when nullif(trim(curr.track_code), '') is not null
                    and nullif(trim(past.track_code), '') is not null
                    and left(curr.track_code, 1) = left(past.track_code, 1)
                    and past.finish_position = 2 then 1 else 0 end),
          count(case when curr.kishumei_ryakusho is not null
                       and nullif(trim(past.kishumei_ryakusho), '') = curr.kishumei_ryakusho
                     then 1 end),
          sum(case when curr.kishumei_ryakusho is not null
                    and nullif(trim(past.kishumei_ryakusho), '') = curr.kishumei_ryakusho
                    and past.finish_position = 2 then 1 else 0 end)
        from target_context curr
        left join race_history past
          on past.source = curr.source
          and past.ketto_toroku_bango = curr.ketto_toroku_bango
          and past.race_date < curr.race_date
        group by curr.race_bango
        order by curr.race_bango
        """
    ).fetchall()

    subject.stage_horse_context(con)
    actual = con.execute(
        """
        select race_bango, same_keibajo_starts, same_keibajo_p2,
          same_distance_starts, same_distance_p2,
          same_track_starts, same_track_p2, pair_starts, pair_p2
        from horse_context order by race_bango
        """
    ).fetchall()

    subject.stage_horse_context(con, focused_target=False)
    bulk_actual = con.execute(
        """
        select race_bango, same_keibajo_starts, same_keibajo_p2,
          same_distance_starts, same_distance_p2,
          same_track_starts, same_track_p2, pair_starts, pair_p2
        from horse_context order by race_bango
        """
    ).fetchall()
    con.close()

    assert actual == expected
    assert bulk_actual == expected


def test_stage_horse_context_bulk_uses_shifted_exact_joins() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_horse_context(RecordingConn(), focused_target=False)
    body = " ".join(sql_calls)
    assert "left join venue_shifted venue" in body
    assert "left join distance_shifted distance" in body
    assert "left join track_shifted track" in body
    assert "left join pair_shifted pair" in body
    assert "asof left join venue_cumulative venue" not in body


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


def test_bulk_pedigree_timeline_is_strict_and_carries_across_date_gaps() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table pedigree_target as
        select * from (values
          ('nar', date '2024-01-05', '2024', '0105', '01', '01', 'horse_gap',
           1600, 'A', 'sire_x', 'damsire_x'),
          ('nar', date '2024-01-10', '2024', '0110', '01', '02', 'horse_same_day',
           1600, 'A', 'sire_x', 'damsire_x')
        ) as v(source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code,
          race_bango, ketto_toroku_bango, kyori, grade_code, sire_id, damsire_id)
        """
    )
    con.execute(
        """
        create temp table sire_kyori_cumul as
        select * from (values
          ('sire_x', 1600, date '2024-01-01', 1::hugeint, 1::hugeint),
          ('sire_x', 1600, date '2024-01-10', 2::hugeint, 1::hugeint)
        ) as v(sire_id, kyori, race_date, cum_starts, cum_p2)
        """
    )
    con.execute(
        """
        create temp table sire_grade_cumul as
        select * from (values
          ('sire_x', 'A', date '2024-01-01', 1::hugeint, 1::hugeint),
          ('sire_x', 'A', date '2024-01-10', 2::hugeint, 1::hugeint)
        ) as v(sire_id, grade_code, race_date, cum_starts, cum_p2)
        """
    )
    con.execute(
        """
        create temp table damsire_kyori_cumul as
        select * from (values
          ('damsire_x', 1600, date '2024-01-01', 1::hugeint, 1::hugeint),
          ('damsire_x', 1600, date '2024-01-10', 2::hugeint, 1::hugeint)
        ) as v(damsire_id, kyori, race_date, cum_starts, cum_p2)
        """
    )
    con.execute(
        "create temp table distance_bridge as select 1600 as target_kyori, "
        "1600 as past_kyori"
    )

    subject._stage_horse_pedigree_context_bulk(con)
    rows = con.execute(
        """
        select sd.ketto_toroku_bango, sd.sire_distance_starts,
          sd.sire_distance_p2, sg.sire_grade_starts, dd.damsire_distance_starts
        from sire_distance_stats sd
        join sire_grade_stats sg using (
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango
        )
        join damsire_distance_stats dd using (
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango
        )
        order by sd.ketto_toroku_bango
        """
    ).fetchall()
    con.close()

    assert rows == [
        ("horse_gap", 1, 1, 1, 1),
        ("horse_same_day", 1, 1, 1, 1),
    ]


def test_bulk_distance_grade_lag_matches_strict_prior_date() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create temp table race_history as
        select * from (values
          ('nar', 'horse_a', 1600, 'A', date '2024-01-01', 2),
          ('nar', 'horse_a', 1600, 'A', date '2024-01-10', 1)
        ) as v(source, ketto_toroku_bango, kyori, grade_code, race_date,
          finish_position)
        """
    )
    con.execute(
        """
        create temp table target_context as
        select 'nar' as source, date '2024-01-10' as race_date,
          '2024' as kaisai_nen, '0110' as kaisai_tsukihi, '01' as keibajo_code,
          '01' as race_bango, 'horse_a' as ketto_toroku_bango,
          1600 as kyori, 'A' as grade_code
        """
    )

    subject.stage_distance_bridge(con)
    subject.stage_horse_distance_grade(con, focused_target=False)
    row = con.execute(
        "select dg_starts, dg_p2 from horse_distance_grade"
    ).fetchone()
    con.close()

    assert row == (1, 1)


def test_bulk_pedigree_and_distance_grade_sql_contains_no_asof() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_horse_pedigree_context(RecordingConn(), focused_target=False)
    subject.stage_horse_distance_grade(RecordingConn(), focused_target=False)
    body = " ".join(sql_calls)
    assert "asof left join" not in body
    assert "last_value(cum_starts ignore nulls)" in body
    assert "lag(cum_starts) over history" in body


def test_distance_tolerance_is_resolved_by_small_bridge_before_entity_joins() -> None:
    sql_calls: list[str] = []

    class RecordingConn:
        def execute(self, query: str) -> None:
            sql_calls.append(query)

    subject.stage_horse_context(RecordingConn())
    subject.stage_horse_pedigree_context(RecordingConn())
    subject.stage_horse_distance_grade(RecordingConn())
    body = " ".join(sql_calls)

    # The range comparison is isolated in a separate physical table built
    # from two already-materialized DISTINCT distance domains. Every large
    # target/entity query consumes that table through equality joins only.
    assert "create or replace temp table target_distance_domain" in body
    assert "create or replace temp table history_distance_domain" in body
    assert body.count("abs(history.past_kyori - target.target_kyori)") == 1
    assert "distance_bridge as materialized" not in body
    assert "cross join unnest(range(" not in body
    assert "abs(past.kyori - target.target_kyori)" not in body
    assert "bridge.past_kyori = past.kyori" in body
    assert "abs(sk.kyori - t.kyori)" not in body
    assert "abs(dk.kyori - t.kyori)" not in body
    assert "abs(hk.kyori - t.kyori)" not in body
    assert "sk.kyori = bridge.past_kyori" in body
    assert "dk.kyori = bridge.past_kyori" in body
    assert "hk.kyori = bridge.past_kyori" in body


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
