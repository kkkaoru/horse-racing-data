from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-futan-juryo-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_futan_juryo_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_futan_juryo_features"] = subject
_spec.loader.exec_module(subject)


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


def test_parse_args_requires_input_and_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_parse_args_defaults(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert "127.0.0.1" in args.pg_url
    assert args.from_date == "20100101"
    assert args.to_date == "20991231"


def test_parse_args_accepts_custom_dates(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--from-date",
            "20230101",
            "--to-date",
            "20231231",
        ]
    )
    assert args.from_date == "20230101"
    assert args.to_date == "20231231"


# ---------------------------------------------------------------------------
# install_and_attach_pg
# ---------------------------------------------------------------------------


def test_install_and_attach_pg_executes_three_statements() -> None:
    executed: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            executed.append(sql)

    subject.install_and_attach_pg(FakeConn(), "postgresql://stub/horse_racing")
    assert executed[0] == "install postgres"
    assert executed[1] == "load postgres"
    assert executed[2].startswith("attach 'postgresql://stub/horse_racing'")
    assert "type postgres" in executed[2]
    assert "read_only" in executed[2]


# ---------------------------------------------------------------------------
# stage_futan_juryo — SQL-structure check
# ---------------------------------------------------------------------------


def test_stage_futan_juryo_sql_references_se_table_and_race_entry() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_futan_juryo(FakeConn(), "20230101", "20231231", "pg.jvd_se")
    body = " ".join(captured)
    assert "pg.race_entry_corner_features" in body
    assert "pg.jvd_se" in body
    assert "futan_raw" in body
    assert "coalesce" in body.lower()
    assert "20230101" in body
    assert "20231231" in body


# ---------------------------------------------------------------------------
# Helper: seed jvd_se and race_entry_corner_features in an in-memory DuckDB
# ---------------------------------------------------------------------------


def _seed_pg_schema(
    con: duckdb.DuckDBPyConnection,
    *,
    rec_rows: list[tuple[str, ...]],
    se_rows: list[tuple[str, ...]],
    se_table: str = "jvd_se",
) -> None:
    """Create minimal pg schema with race_entry_corner_features and jvd_se."""
    con.execute("create schema if not exists pg")
    con.execute(
        """
        create or replace table pg.race_entry_corner_features (
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
          race_date varchar, futan_juryo varchar
        )
        """
    )
    if rec_rows:
        placeholders = ", ".join(
            f"('{r[0]}','{r[1]}','{r[2]}','{r[3]}','{r[4]}','{r[5]}','{r[6]}','{r[7]}')"
            for r in rec_rows
        )
        con.execute(
            f"insert into pg.race_entry_corner_features values {placeholders}"
        )

    con.execute(
        f"""
        create or replace table pg.{se_table} (
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
          race_date varchar, futan_juryo varchar
        )
        """
    )
    if se_rows:
        placeholders = ", ".join(
            f"('{r[0]}','{r[1]}','{r[2]}','{r[3]}','{r[4]}','{r[5]}','{r[6]}','{r[7]}')"
            for r in se_rows
        )
        con.execute(f"insert into pg.{se_table} values {placeholders}")


# ---------------------------------------------------------------------------
# stage_futan_juryo — end-to-end: upcoming-race path (PG empty)
# ---------------------------------------------------------------------------


def test_stage_futan_juryo_uses_se_fallback_when_rec_missing() -> None:
    """When race_entry_corner_features has no row for the upcoming race, the
    jvd_se futan_juryo (0.1 kg units) must be divided by 10 and used.
    """
    con = duckdb.connect(":memory:")
    # rec is empty (simulates stale race_entry_corner_features for upcoming race)
    # se has the upcoming race with futan_juryo '540' = 54.0 kg
    _seed_pg_schema(
        con,
        rec_rows=[],
        se_rows=[
            ("jra", "2026", "0607", "05", "11", "horse_a", "20260607", "540"),
            ("jra", "2026", "0607", "05", "11", "horse_b", "20260607", "560"),
        ],
    )
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    rows = con.execute(
        "select ketto_toroku_bango, futan_juryo from futan_raw order by ketto_toroku_bango"
    ).fetchall()
    con.close()
    assert len(rows) == 2
    assert rows[0] == ("horse_a", pytest.approx(54.0))
    assert rows[1] == ("horse_b", pytest.approx(56.0))


def test_stage_futan_juryo_filters_null_futan_se_rows() -> None:
    """Rows where both rec and se futan_juryo are NULL must be excluded."""
    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features (
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
          race_date varchar, futan_juryo varchar
        )
        """
    )
    con.execute(
        """
        create table pg.jvd_se (
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
          race_date varchar, futan_juryo varchar
        )
        """
    )
    # horse_a has futan; horse_b has NULL in both rec and se
    con.execute(
        "insert into pg.jvd_se values ('jra','2026','0607','05','11','horse_a','20260607','540')"
    )
    con.execute(
        "insert into pg.jvd_se values ('jra','2026','0607','05','11','horse_b','20260607',NULL)"
    )
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    rows = con.execute(
        "select ketto_toroku_bango from futan_raw order by ketto_toroku_bango"
    ).fetchall()
    con.close()
    assert len(rows) == 1
    assert rows[0][0] == "horse_a"


# ---------------------------------------------------------------------------
# stage_futan_juryo — PG value wins over se fallback (historical path)
# ---------------------------------------------------------------------------


def test_stage_futan_juryo_pg_wins_over_se_for_historical_race() -> None:
    """For historical rows present in race_entry_corner_features, the PG value
    (stored in 0.1 kg units × 10) takes priority over jvd_se.
    """
    con = duckdb.connect(":memory:")
    # rec has authoritative historical value '560' = 56.0 kg
    # se has a different value '990' = 99.0 kg
    _seed_pg_schema(
        con,
        rec_rows=[("jra", "2024", "0415", "05", "11", "horse_a", "20240415", "560")],
        se_rows=[("jra", "2024", "0415", "05", "11", "horse_a", "20240415", "990")],
    )
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    rows = con.execute(
        "select ketto_toroku_bango, futan_juryo from futan_raw"
    ).fetchall()
    con.close()
    assert len(rows) == 1
    assert rows[0] == ("horse_a", pytest.approx(56.0))


def test_stage_futan_juryo_mixed_historical_and_upcoming() -> None:
    """Historical horse uses rec (PG); upcoming horse absent from rec uses se."""
    con = duckdb.connect(":memory:")
    _seed_pg_schema(
        con,
        rec_rows=[
            # historical horse in rec: '560' = 56.0 kg
            ("jra", "2024", "0415", "05", "11", "horse_hist", "20240415", "560")
        ],
        se_rows=[
            # same historical horse in se: different value ('990' = 99.0 kg)
            ("jra", "2024", "0415", "05", "11", "horse_hist", "20240415", "990"),
            # upcoming horse: only in se ('540' = 54.0 kg)
            ("jra", "2026", "0607", "05", "11", "horse_upcoming", "20260607", "540"),
        ],
    )
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    rows = con.execute(
        "select ketto_toroku_bango, futan_juryo from futan_raw order by ketto_toroku_bango"
    ).fetchall()
    con.close()
    hist = next(r for r in rows if r[0] == "horse_hist")
    upcoming = next(r for r in rows if r[0] == "horse_upcoming")
    # historical: rec value (56.0) wins over se (99.0)
    assert hist[1] == pytest.approx(56.0)
    # upcoming: se fallback (54.0) used because rec has no row
    assert upcoming[1] == pytest.approx(54.0)


# ---------------------------------------------------------------------------
# stage_horse_history
# ---------------------------------------------------------------------------


def test_stage_horse_history_sql_check() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_horse_history(FakeConn())
    body = " ".join(captured)
    assert "horse_futan_hist" in body
    assert "futan_raw" in body
    assert "past_futan_juryo_avg5" in body
    assert "past_high_futan_share" in body


# ---------------------------------------------------------------------------
# append_features_sql — SQL-structure checks
# ---------------------------------------------------------------------------


def test_append_features_sql_uses_futan_raw() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "futan_raw" in sql


def test_append_features_sql_contains_futan_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "futan_juryo_rank_in_race" in sql
    assert "futan_juryo_diff_from_race_avg" in sql
    assert "past_futan_juryo_diff" in sql
    assert "past_futan_juryo_avg5" in sql
    assert "futan_weight_class" in sql
    assert "past_high_futan_share" in sql


def test_append_features_sql_preserves_base_select_star() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "b.*" in sql


def test_append_features_sql_rank_window_uses_nulls_last() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "nulls last" in sql


def test_append_features_sql_uses_input_glob(tmp_path: Path) -> None:
    glob = f"{tmp_path}/race_year=*/*.parquet"
    sql = subject.append_features_sql(glob)
    assert glob in sql


# ---------------------------------------------------------------------------
# Helpers for end-to-end feature computation tests
# ---------------------------------------------------------------------------


def _seed_upcoming_parquet(parquet_dir: Path) -> str:
    """Write a synthetic upcoming-race parquet with three horses.

    The parquet does NOT contain futan_juryo (as in production); that is
    sourced via stage_futan_juryo from jvd_se.
    """
    parquet_dir.mkdir(parents=True, exist_ok=True)
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2026', '0607', '05', '11', 'horse_light', '20260607', 2026),
            ('jra', '2026', '0607', '05', '11', 'horse_mid',   '20260607', 2026),
            ('jra', '2026', '0607', '05', '11', 'horse_heavy', '20260607', 2026)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    return f"{parquet_dir.as_posix()}/race_year=*/*.parquet"


def _seed_pg_for_upcoming(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Seed minimal PG: rec empty (upcoming), se has futan for all three horses."""
    con.execute("create schema if not exists pg")
    con.execute(
        """
        create or replace table pg.race_entry_corner_features (
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
          race_date varchar, futan_juryo varchar
        )
        """
    )
    # rec is empty — simulates stale race_entry_corner_features
    con.execute(
        """
        create or replace table pg.jvd_se (
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
          race_date varchar, futan_juryo varchar
        )
        """
    )
    # horse_light=540 (54.0 kg), horse_mid=560 (56.0 kg), horse_heavy=580 (58.0 kg)
    con.execute(
        """
        insert into pg.jvd_se values
          ('jra','2026','0607','05','11','horse_light','20260607','540'),
          ('jra','2026','0607','05','11','horse_mid',  '20260607','560'),
          ('jra','2026','0607','05','11','horse_heavy','20260607','580')
        """
    )


# ---------------------------------------------------------------------------
# Upcoming-race path: core features non-null (fix verification)
# ---------------------------------------------------------------------------


def test_upcoming_race_futan_juryo_non_null(tmp_path: Path) -> None:
    """Fix verification: futan_juryo must not be NULL for upcoming races when
    the jvd_se fallback supplies it.
    """
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_pg_for_upcoming(con)
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    subject.stage_horse_history(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango, futan_juryo
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    for row in rows:
        assert row[1] is not None, f"{row[0]} futan_juryo is NULL"


def test_upcoming_race_rank_and_diff_non_null(tmp_path: Path) -> None:
    """Fix verification: rank_in_race and diff_from_race_avg must not be NULL
    for upcoming races once the se fallback supplies futan_juryo.
    """
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_pg_for_upcoming(con)
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    subject.stage_horse_history(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
               futan_juryo_rank_in_race,
               futan_juryo_diff_from_race_avg
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    for row in rows:
        assert row[1] is not None, f"{row[0]} futan_juryo_rank_in_race is NULL"
        assert row[2] is not None, f"{row[0]} futan_juryo_diff_from_race_avg is NULL"


def test_upcoming_race_rank_not_all_one(tmp_path: Path) -> None:
    """Ranks must be distinct (1,2,3) when futan_juryo values differ."""
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_pg_for_upcoming(con)
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    subject.stage_horse_history(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango, futan_juryo_rank_in_race
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # heavy (58.0) → rank 1, mid (56.0) → rank 2, light (54.0) → rank 3
    heavy = next(r for r in rows if r[0] == "horse_heavy")
    mid = next(r for r in rows if r[0] == "horse_mid")
    light = next(r for r in rows if r[0] == "horse_light")
    assert heavy[1] == 1
    assert mid[1] == 2
    assert light[1] == 3


def test_upcoming_race_futan_weight_class_non_null(tmp_path: Path) -> None:
    """futan_weight_class must not be NULL when futan_juryo is non-null."""
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_pg_for_upcoming(con)
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    subject.stage_horse_history(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango, futan_weight_class
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    for row in rows:
        assert row[1] is not None, f"{row[0]} futan_weight_class is NULL"


def test_upcoming_race_diff_from_race_avg_sums_to_zero(tmp_path: Path) -> None:
    """futan_juryo_diff_from_race_avg must sum to 0 within a race."""
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_pg_for_upcoming(con)
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    subject.stage_horse_history(con)
    sql = subject.append_features_sql(glob)
    row = con.execute(
        f"select round(sum(futan_juryo_diff_from_race_avg), 10) from ({sql})"
    ).fetchone()
    con.close()
    assert row is not None
    assert row[0] == pytest.approx(0.0, abs=1e-9)


def test_upcoming_race_debutant_past_diff_is_zero(tmp_path: Path) -> None:
    """For debutants (only the upcoming race in futan_raw, no prior races):
    past_futan_juryo_avg5 = futan of the upcoming race itself (rn=1 is the only
    row in the window), and past_futan_juryo_diff = futan - avg5 = 0.
    This is consistent with how training data computed these values (rec included
    the target race in the window).
    """
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_pg_for_upcoming(con)
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    subject.stage_horse_history(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango, futan_juryo, past_futan_juryo_avg5, past_futan_juryo_diff
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # Each horse has only one race (upcoming); avg5 = that futan; diff = 0
    for row in rows:
        assert row[2] is not None, f"{row[0]} past_futan_juryo_avg5 should not be NULL (has se row)"
        assert row[3] is not None, f"{row[0]} past_futan_juryo_diff should not be NULL"
        assert row[3] == pytest.approx(0.0, abs=1e-9), (
            f"{row[0]} past_futan_juryo_diff should be 0 for debutant (diff = futan - avg5 = futan - futan)"
        )


def test_upcoming_race_with_history_past_diff_non_null(tmp_path: Path) -> None:
    """For established horses with past races, past_futan_juryo_diff must be
    non-null once the jvd_se fallback supplies current-race futan_juryo.

    futan_raw contains two rows for horse_est:
      rn=1: 0607 (upcoming, 56.0 kg from se)
      rn=2: 0601 (historical, 54.0 kg from rec — rec wins over se for that row)

    past_futan_juryo_avg5 = avg(rn 1..5) = avg(56.0, 54.0) = 55.0
    past_futan_juryo_diff = 56.0 - 55.0 = 1.0

    This is consistent with training-data behaviour where race_entry_corner_features
    included the target race in the window.
    """
    parquet_dir = tmp_path / "input"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    # Seed input parquet: one upcoming race only (no historical row in parquet)
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values ('jra','2026','0607','05','11','horse_est','20260607',2026)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    glob = f"{parquet_dir.as_posix()}/race_year=*/*.parquet"

    con = duckdb.connect(":memory:")
    # rec: has a PAST race for horse_est (not upcoming)
    # se: has both the past race AND the upcoming race
    _seed_pg_schema(
        con,
        rec_rows=[
            # past race in rec (historical): futan '540' = 54.0 kg
            ("jra", "2026", "0601", "05", "11", "horse_est", "20260601", "540")
        ],
        se_rows=[
            # past race in se (matches rec — rec wins for this row)
            ("jra", "2026", "0601", "05", "11", "horse_est", "20260601", "990"),
            # upcoming race in se only: futan '560' = 56.0 kg
            ("jra", "2026", "0607", "05", "11", "horse_est", "20260607", "560"),
        ],
    )
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    subject.stage_horse_history(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango, futan_juryo,
               past_futan_juryo_avg5, past_futan_juryo_diff
        from ({sql})
        where kaisai_tsukihi = '0607'
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == "horse_est"
    # upcoming race futan comes from se fallback: 560/10 = 56.0 kg
    assert row[1] == pytest.approx(56.0)
    # past_futan_juryo_avg5 = avg(rn 1..5) = avg(56.0, 54.0) = 55.0
    assert row[2] is not None, "past_futan_juryo_avg5 must be non-null for established horse"
    assert row[2] == pytest.approx(55.0, abs=1e-6)
    # past_futan_juryo_diff = 56.0 - 55.0 = 1.0
    assert row[3] is not None, "past_futan_juryo_diff must be non-null for established horse"
    assert row[3] == pytest.approx(1.0, abs=1e-6)


# ---------------------------------------------------------------------------
# Historical path: PG rec rows present → values unchanged (regression guard)
# ---------------------------------------------------------------------------


def _seed_historical_parquet(parquet_dir: Path) -> str:
    """Write a synthetic historical parquet with two horses."""
    parquet_dir.mkdir(parents=True, exist_ok=True)
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2024', '0415', '05', '11', 'horse_a', '20240415', 2024),
            ('jra', '2024', '0415', '05', '11', 'horse_b', '20240415', 2024)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    return f"{parquet_dir.as_posix()}/race_year=*/*.parquet"


def test_historical_race_pg_rec_wins_over_se(tmp_path: Path) -> None:
    """For historical rows present in race_entry_corner_features, the PG rec
    value must win over the se value (no regression).
    """
    parquet_dir = tmp_path / "input"
    glob = _seed_historical_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    # rec has authoritative values; se has different (wrong) values
    _seed_pg_schema(
        con,
        rec_rows=[
            ("jra", "2024", "0415", "05", "11", "horse_a", "20240415", "560"),
            ("jra", "2024", "0415", "05", "11", "horse_b", "20240415", "540"),
        ],
        se_rows=[
            ("jra", "2024", "0415", "05", "11", "horse_a", "20240415", "990"),
            ("jra", "2024", "0415", "05", "11", "horse_b", "20240415", "880"),
        ],
    )
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    subject.stage_horse_history(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango, futan_juryo, futan_juryo_rank_in_race
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    horse_a = next(r for r in rows if r[0] == "horse_a")
    horse_b = next(r for r in rows if r[0] == "horse_b")
    # rec values (56.0/54.0) must be used, not se (99.0/88.0)
    assert horse_a[1] == pytest.approx(56.0)
    assert horse_b[1] == pytest.approx(54.0)
    # horse_a is heavier → rank 1; horse_b → rank 2
    assert horse_a[2] == 1
    assert horse_b[2] == 2


def test_historical_race_diff_from_avg_non_null_rec_path(tmp_path: Path) -> None:
    """With rec futan values the diff_from_race_avg must still be non-null."""
    parquet_dir = tmp_path / "input"
    glob = _seed_historical_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_pg_schema(
        con,
        rec_rows=[
            ("jra", "2024", "0415", "05", "11", "horse_a", "20240415", "560"),
            ("jra", "2024", "0415", "05", "11", "horse_b", "20240415", "540"),
        ],
        se_rows=[
            ("jra", "2024", "0415", "05", "11", "horse_a", "20240415", "560"),
            ("jra", "2024", "0415", "05", "11", "horse_b", "20240415", "540"),
        ],
    )
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    subject.stage_horse_history(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango, futan_juryo_diff_from_race_avg
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    for row in rows:
        assert row[1] is not None, f"{row[0]} futan_juryo_diff_from_race_avg is NULL"


# ---------------------------------------------------------------------------
# write_partitioned
# ---------------------------------------------------------------------------


def _seed_for_write(tmp_path: Path) -> tuple[str, duckdb.DuckDBPyConnection]:
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_pg_for_upcoming(con)
    subject.stage_futan_juryo(con, "20100101", "20991231", "pg.jvd_se")
    subject.stage_horse_history(con)
    return glob, con


def test_write_partitioned_produces_parquet_with_futan_columns(tmp_path: Path) -> None:
    glob, con = _seed_for_write(tmp_path)
    sql = subject.append_features_sql(glob)
    out_dir = tmp_path / "output"
    subject.write_partitioned(con, sql, out_dir)
    verify_con = duckdb.connect(":memory:")
    col_names = [
        c[0]
        for c in verify_con.execute(
            f"describe select * from read_parquet('{out_dir.as_posix()}/race_year=*/*.parquet')"
        ).fetchall()
    ]
    verify_con.close()
    con.close()
    assert "futan_juryo" in col_names
    assert "futan_juryo_rank_in_race" in col_names
    assert "futan_juryo_diff_from_race_avg" in col_names
    assert "past_futan_juryo_diff" in col_names
    assert "futan_weight_class" in col_names


def test_write_partitioned_row_count_preserved(tmp_path: Path) -> None:
    glob, con = _seed_for_write(tmp_path)
    sql = subject.append_features_sql(glob)
    out_dir = tmp_path / "output"
    subject.write_partitioned(con, sql, out_dir)
    verify_con = duckdb.connect(":memory:")
    row = verify_con.execute(
        f"select count(*) from read_parquet('{out_dir.as_posix()}/race_year=*/*.parquet')"
    ).fetchone()
    verify_con.close()
    con.close()
    assert row == (3,)


def test_write_partitioned_overwrites_existing_dir(tmp_path: Path) -> None:
    """output_dir pre-existing must be wiped and rewritten cleanly."""
    glob, con = _seed_for_write(tmp_path)
    sql = subject.append_features_sql(glob)
    out_dir = tmp_path / "output"
    out_dir.mkdir()  # pre-create to exercise the shutil.rmtree branch
    subject.write_partitioned(con, sql, out_dir)
    verify_con = duckdb.connect(":memory:")
    row = verify_con.execute(
        f"select count(*) from read_parquet('{out_dir.as_posix()}/race_year=*/*.parquet')"
    ).fetchone()
    verify_con.close()
    con.close()
    assert row == (3,)


# ---------------------------------------------------------------------------
# main() end-to-end with stubbed PG (no real DB required)
# ---------------------------------------------------------------------------


def test_main_upcoming_race_produces_non_null_futan_features(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Full main() path with upcoming race: rec empty → jvd_se fallback →
    non-null futan_juryo, rank_in_race, diff_from_race_avg in output parquet.
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    _seed_upcoming_parquet(input_dir)

    def _fake_install_and_attach(con: duckdb.DuckDBPyConnection, _pg_url: str) -> None:
        con.execute("create schema pg")
        # race_entry_corner_features is empty (simulates lag for upcoming race)
        con.execute(
            """
            create table pg.race_entry_corner_features (
              source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
              keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
              race_date varchar, futan_juryo varchar
            )
            """
        )
        con.execute(
            """
            create table pg.jvd_se (
              source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
              keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
              race_date varchar, futan_juryo varchar
            )
            """
        )
        con.execute(
            """
            insert into pg.jvd_se values
              ('jra','2026','0607','05','11','horse_light','20260607','540'),
              ('jra','2026','0607','05','11','horse_mid',  '20260607','560'),
              ('jra','2026','0607','05','11','horse_heavy','20260607','580')
            """
        )

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_futan_juryo_features",
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
        ],
    )
    subject.main()

    verify_con = duckdb.connect(":memory:")
    rows = verify_con.execute(
        f"""
        select ketto_toroku_bango,
               futan_juryo,
               futan_juryo_rank_in_race,
               futan_juryo_diff_from_race_avg,
               futan_weight_class
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        order by ketto_toroku_bango
        """
    ).fetchall()
    verify_con.close()
    assert len(rows) == 3
    for row in rows:
        assert row[1] is not None, f"{row[0]} futan_juryo is NULL"
        assert row[2] is not None, f"{row[0]} futan_juryo_rank_in_race is NULL"
        assert row[3] is not None, f"{row[0]} futan_juryo_diff_from_race_avg is NULL"
        assert row[4] is not None, f"{row[0]} futan_weight_class is NULL"
    # Ranks must not all be 1 — horse_heavy (58.0) should be rank 1
    heavy = next(r for r in rows if r[0] == "horse_heavy")
    light = next(r for r in rows if r[0] == "horse_light")
    assert heavy[2] == 1
    assert light[2] == 3


def test_main_historical_race_pg_rec_values_used(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Full main() path with historical race: rec has rows → rec values used."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    _seed_historical_parquet(input_dir)

    def _fake_install_and_attach(con: duckdb.DuckDBPyConnection, _pg_url: str) -> None:
        con.execute("create schema pg")
        con.execute(
            """
            create table pg.race_entry_corner_features as
            select * from (
              values
                ('jra', '2024', '0415', '05', '11', 'horse_a', '20240415', '560'),
                ('jra', '2024', '0415', '05', '11', 'horse_b', '20240415', '540')
            ) as v(
              source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
              ketto_toroku_bango, race_date, futan_juryo
            )
            """
        )
        con.execute(
            """
            create table pg.jvd_se as
            select * from (
              values
                ('jra', '2024', '0415', '05', '11', 'horse_a', '20240415', '990'),
                ('jra', '2024', '0415', '05', '11', 'horse_b', '20240415', '880')
            ) as v(
              source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
              ketto_toroku_bango, race_date, futan_juryo
            )
            """
        )

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_futan_juryo_features",
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
        ],
    )
    subject.main()

    verify_con = duckdb.connect(":memory:")
    rows = verify_con.execute(
        f"""
        select ketto_toroku_bango, futan_juryo, futan_juryo_rank_in_race
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        order by ketto_toroku_bango
        """
    ).fetchall()
    verify_con.close()
    # rec stored '560' → 560.0/10 = 56.0, '540' → 54.0
    horse_a = next(r for r in rows if r[0] == "horse_a")
    horse_b = next(r for r in rows if r[0] == "horse_b")
    assert horse_a[1] == pytest.approx(56.0)
    assert horse_b[1] == pytest.approx(54.0)
    # horse_a (56.0) heavier → rank 1; horse_b (54.0) → rank 2
    assert horse_a[2] == 1
    assert horse_b[2] == 2
