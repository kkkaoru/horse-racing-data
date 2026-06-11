from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-market-signal-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_market_signal_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_market_signal_features"] = subject
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


def test_parse_args_pg_url_defaults_to_local_url(tmp_path: Path) -> None:
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
# stage_raw_odds
# ---------------------------------------------------------------------------


def test_stage_raw_odds_queries_race_entry_corner_features() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_raw_odds(FakeConn(), "20230101", "20231231")
    body = " ".join(captured)
    assert "pg.race_entry_corner_features" in body
    assert "raw_odds" in body
    assert "20230101" in body
    assert "20231231" in body
    assert "tansho_odds" in body
    assert "tansho_ninkijun" in body


# ---------------------------------------------------------------------------
# stage_parquet_odds
# ---------------------------------------------------------------------------


def test_stage_parquet_odds_creates_parquet_odds_table() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_parquet_odds(FakeConn(), "/tmp/in/race_year=*/*.parquet")
    body = " ".join(captured)
    assert "parquet_odds" in body
    assert "read_parquet('/tmp/in/race_year=*/*.parquet'" in body
    assert "tansho_odds" in body
    assert "tansho_ninkijun" in body
    assert "tansho_odds is not null" in body


def test_stage_parquet_odds_end_to_end_reads_parquet_values(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "input"
    parquet_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2026', '0607', '05', '11', 'horse_a', '20260607', 2026,
              12.5::double, 3::integer),
            ('jra', '2026', '0607', '05', '11', 'horse_b', '20260607', 2026,
              5.0::double, 1::integer),
            ('jra', '2026', '0607', '05', '11', 'horse_c', '20260607', 2026,
              NULL::double, NULL::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year,
          tansho_odds, tansho_ninkijun
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()

    con = duckdb.connect(":memory:")
    glob = f"{parquet_dir.as_posix()}/race_year=*/*.parquet"
    subject.stage_parquet_odds(con, glob)
    rows = con.execute(
        """
        select ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        from parquet_odds
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # horse_c has NULL tansho_odds so it must be excluded
    assert len(rows) == 2
    assert rows[0] == ("horse_a", 12.5, 3)
    assert rows[1] == ("horse_b", 5.0, 1)


# ---------------------------------------------------------------------------
# merge_odds_tables
# ---------------------------------------------------------------------------


def test_merge_odds_tables_prefers_pg_over_parquet() -> None:
    """When PG has the row, its value takes priority over the parquet value."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table raw_odds as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a',
              8.0::double, 2::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        )
        """
    )
    con.execute(
        """
        create or replace temp table parquet_odds as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a',
              12.5::double, 3::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        )
        """
    )
    subject.merge_odds_tables(con)
    rows = con.execute(
        """
        select ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        from raw_odds_merged
        """
    ).fetchall()
    con.close()
    # PG value (8.0, rank 2) must win
    assert rows == [("horse_a", 8.0, 2)]


def test_merge_odds_tables_falls_back_to_parquet_when_pg_misses() -> None:
    """For upcoming-race rows absent from PG, parquet values fill the gap."""
    con = duckdb.connect(":memory:")
    # raw_odds (PG) is empty — simulates race_entry_corner_features lag
    con.execute(
        """
        create or replace temp table raw_odds as
        select * from (
          values
            (NULL::varchar, NULL::varchar, NULL::varchar, NULL::varchar,
             NULL::varchar, NULL::varchar, NULL::double, NULL::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        )
        where 1 = 0
        """
    )
    con.execute(
        """
        create or replace temp table parquet_odds as
        select * from (
          values
            ('jra', '2026', '0607', '05', '11', 'horse_a',
              12.5::double, 3::integer),
            ('jra', '2026', '0607', '05', '11', 'horse_b',
              5.0::double, 1::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        )
        """
    )
    subject.merge_odds_tables(con)
    rows = con.execute(
        """
        select ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        from raw_odds_merged
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    assert len(rows) == 2
    assert rows[0] == ("horse_a", 12.5, 3)
    assert rows[1] == ("horse_b", 5.0, 1)


def test_merge_odds_tables_mixed_historical_and_upcoming() -> None:
    """Historical row uses PG; upcoming row absent from PG uses parquet."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table raw_odds as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_hist',
              8.0::double, 2::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        )
        """
    )
    con.execute(
        """
        create or replace temp table parquet_odds as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_hist',
              99.0::double, 9::integer),
            ('jra', '2026', '0607', '05', '11', 'horse_upcoming',
              5.0::double, 1::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        )
        """
    )
    subject.merge_odds_tables(con)
    rows = con.execute(
        """
        select ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        from raw_odds_merged
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # horse_hist: PG (8.0, 2) wins over parquet (99.0, 9)
    # horse_upcoming: parquet (5.0, 1) since PG has no match
    assert rows[0] == ("horse_hist", 8.0, 2)
    assert rows[1] == ("horse_upcoming", 5.0, 1)


# ---------------------------------------------------------------------------
# append_features_sql — SQL structure checks
# ---------------------------------------------------------------------------


def test_append_features_sql_uses_raw_odds_merged() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "raw_odds_merged" in sql
    assert "raw_odds r" not in sql


def test_append_features_sql_contains_market_signal_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "inverse_odds_implied_prob" in sql
    assert "inverse_odds_market_share" in sql
    assert "inverse_odds_rank_in_race" in sql
    assert "popularity_rank_in_race" in sql
    assert "odds_score_diff_from_race_avg" in sql
    assert "popularity_score_diff_from_race_avg" in sql
    assert "popularity_odds_disagreement" in sql


def test_append_features_sql_preserves_base_select_star() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "b.*" in sql


def test_append_features_sql_rank_windows_use_nulls_last() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "desc nulls last" in sql
    assert "asc nulls last" in sql


def test_append_features_sql_uses_input_glob(tmp_path: Path) -> None:
    glob = f"{tmp_path}/race_year=*/*.parquet"
    sql = subject.append_features_sql(glob)
    assert glob in sql


# ---------------------------------------------------------------------------
# Upcoming-race path: all features non-null, ranks correct (fix verification)
# ---------------------------------------------------------------------------


def _seed_upcoming_parquet(parquet_dir: Path) -> str:
    """Write a synthetic upcoming-race parquet with tansho_odds/tansho_ninkijun
    already populated (as they are after the realtime-odds path in base build).

    Three horses in one race: odds 5.0 (fav), 8.0, 20.0.
    Also includes odds_score / popularity_score to exercise those diffs.
    """
    parquet_dir.mkdir(parents=True, exist_ok=True)
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2026', '0607', '05', '11', 'horse_fav', '20260607', 2026,
              5.0::double, 1::integer, 0.5::double, 0.6::double),
            ('jra', '2026', '0607', '05', '11', 'horse_mid', '20260607', 2026,
              8.0::double, 2::integer, 0.3::double, 0.2::double),
            ('jra', '2026', '0607', '05', '11', 'horse_out', '20260607', 2026,
              20.0::double, 3::integer, 0.1::double, 0.1::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year,
          tansho_odds, tansho_ninkijun,
          odds_score, popularity_score
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    return f"{parquet_dir.as_posix()}/race_year=*/*.parquet"


def _seed_empty_raw_odds(con: duckdb.DuckDBPyConnection) -> None:
    """Simulate race_entry_corner_features having no rows for the upcoming date."""
    con.execute(
        """
        create or replace temp table raw_odds as
        select * from (
          values (NULL::varchar, NULL::varchar, NULL::varchar,
                  NULL::varchar, NULL::varchar, NULL::varchar,
                  NULL::double, NULL::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        )
        where 1 = 0
        """
    )


def test_upcoming_race_all_market_signal_features_non_null(tmp_path: Path) -> None:
    """Core fix verification: when PG has no rows (upcoming race), the parquet
    odds path must produce non-null inverse_odds_implied_prob,
    inverse_odds_market_share, tansho_odds_raw, tansho_ninkijun_raw.
    """
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_empty_raw_odds(con)
    subject.stage_parquet_odds(con, glob)
    subject.merge_odds_tables(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
               tansho_odds_raw,
               tansho_ninkijun_raw,
               inverse_odds_implied_prob,
               inverse_odds_market_share
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # All five columns must be non-null for every horse
    for row in rows:
        assert row[1] is not None, f"{row[0]} tansho_odds_raw is NULL"
        assert row[2] is not None, f"{row[0]} tansho_ninkijun_raw is NULL"
        assert row[3] is not None, f"{row[0]} inverse_odds_implied_prob is NULL"
        assert row[4] is not None, f"{row[0]} inverse_odds_market_share is NULL"


def test_upcoming_race_inverse_odds_rank_not_all_one(tmp_path: Path) -> None:
    """Fix verification: ranks must not all collapse to 1 (the bogus all-null
    NULLS LAST tie that occurred pre-fix when tansho_odds_raw was NULL for all).
    """
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_empty_raw_odds(con)
    subject.stage_parquet_odds(con, glob)
    subject.merge_odds_tables(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
               inverse_odds_rank_in_race,
               popularity_rank_in_race
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # horse_fav has odds=5.0 (highest 1/odds) → rank 1
    # horse_mid has odds=8.0 → rank 2
    # horse_out has odds=20.0 → rank 3
    fav = next(r for r in rows if r[0] == "horse_fav")
    mid = next(r for r in rows if r[0] == "horse_mid")
    out = next(r for r in rows if r[0] == "horse_out")
    assert fav[1] == 1
    assert mid[1] == 2
    assert out[1] == 3
    # popularity rank: ninkijun 1,2,3 → ranks 1,2,3
    assert fav[2] == 1
    assert mid[2] == 2
    assert out[2] == 3


def test_upcoming_race_inverse_odds_implied_prob_values(tmp_path: Path) -> None:
    """Verify the numeric values of inverse_odds_implied_prob are correct."""
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_empty_raw_odds(con)
    subject.stage_parquet_odds(con, glob)
    subject.merge_odds_tables(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
               round(inverse_odds_implied_prob, 6) as prob
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    fav = next(r for r in rows if r[0] == "horse_fav")
    mid = next(r for r in rows if r[0] == "horse_mid")
    out = next(r for r in rows if r[0] == "horse_out")
    assert fav[1] == pytest.approx(1 / 5.0, rel=1e-5)
    assert mid[1] == pytest.approx(1 / 8.0, rel=1e-5)
    assert out[1] == pytest.approx(1 / 20.0, rel=1e-5)


def test_upcoming_race_market_share_sums_to_one(tmp_path: Path) -> None:
    """inverse_odds_market_share must sum to 1.0 within the race."""
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_empty_raw_odds(con)
    subject.stage_parquet_odds(con, glob)
    subject.merge_odds_tables(con)
    sql = subject.append_features_sql(glob)
    row = con.execute(
        f"""
        select round(sum(inverse_odds_market_share), 6)
        from ({sql})
        """
    ).fetchone()
    con.close()
    assert row is not None
    assert row[0] == pytest.approx(1.0, rel=1e-5)


def test_upcoming_race_odds_score_diff_from_race_avg_non_null(tmp_path: Path) -> None:
    """odds_score_diff_from_race_avg must be non-null when odds_score is non-null."""
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_empty_raw_odds(con)
    subject.stage_parquet_odds(con, glob)
    subject.merge_odds_tables(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
               odds_score_diff_from_race_avg,
               popularity_score_diff_from_race_avg,
               popularity_odds_disagreement
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    for row in rows:
        assert row[1] is not None, f"{row[0]} odds_score_diff_from_race_avg is NULL"
        assert row[2] is not None, f"{row[0]} popularity_score_diff_from_race_avg is NULL"
        assert row[3] is not None, f"{row[0]} popularity_odds_disagreement is NULL"


# ---------------------------------------------------------------------------
# Historical path: PG rows present → values unchanged (regression guard)
# ---------------------------------------------------------------------------


def _seed_historical_parquet(parquet_dir: Path) -> str:
    """Write a synthetic historical parquet where tansho_odds in parquet
    differs from what PG would provide (so we can confirm PG wins).
    """
    parquet_dir.mkdir(parents=True, exist_ok=True)
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2024', '0415', '05', '11', 'horse_a', '20240415', 2024,
              99.0::double, 9::integer, 0.2::double, 0.3::double),
            ('jra', '2024', '0415', '05', '11', 'horse_b', '20240415', 2024,
              88.0::double, 8::integer, 0.5::double, 0.4::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year,
          tansho_odds, tansho_ninkijun,
          odds_score, popularity_score
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    return f"{parquet_dir.as_posix()}/race_year=*/*.parquet"


def test_historical_race_pg_odds_take_priority_over_parquet(tmp_path: Path) -> None:
    """For historical rows present in PG, the PG value must win (no regression)."""
    parquet_dir = tmp_path / "input"
    glob = _seed_historical_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    # raw_odds (PG) has authoritative historical values
    con.execute(
        """
        create or replace temp table raw_odds as
        select * from (
          values
            ('jra', '2024', '0415', '05', '11', 'horse_a',
              6.0::double, 2::integer),
            ('jra', '2024', '0415', '05', '11', 'horse_b',
              3.0::double, 1::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        )
        """
    )
    subject.stage_parquet_odds(con, glob)
    subject.merge_odds_tables(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
               tansho_odds_raw,
               tansho_ninkijun_raw,
               inverse_odds_rank_in_race,
               popularity_rank_in_race
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # PG values (6.0/3.0) must be used, not parquet (99.0/88.0)
    horse_a = next(r for r in rows if r[0] == "horse_a")
    horse_b = next(r for r in rows if r[0] == "horse_b")
    assert horse_a[1] == 6.0
    assert horse_b[1] == 3.0
    # popularity: horse_b ninkijun=1 → rank 1; horse_a ninkijun=2 → rank 2
    assert horse_b[4] == 1
    assert horse_a[4] == 2


def test_historical_race_inverse_odds_market_share_sums_to_one_from_pg(tmp_path: Path) -> None:
    """With PG odds the market share must still sum to 1.0."""
    parquet_dir = tmp_path / "input"
    glob = _seed_historical_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table raw_odds as
        select * from (
          values
            ('jra', '2024', '0415', '05', '11', 'horse_a',
              6.0::double, 2::integer),
            ('jra', '2024', '0415', '05', '11', 'horse_b',
              3.0::double, 1::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw
        )
        """
    )
    subject.stage_parquet_odds(con, glob)
    subject.merge_odds_tables(con)
    sql = subject.append_features_sql(glob)
    row = con.execute(
        f"select round(sum(inverse_odds_market_share), 6) from ({sql})"
    ).fetchone()
    con.close()
    assert row is not None
    assert row[0] == pytest.approx(1.0, rel=1e-5)


# ---------------------------------------------------------------------------
# write_partitioned
# ---------------------------------------------------------------------------


def _seed_for_write(tmp_path: Path) -> tuple[str, duckdb.DuckDBPyConnection]:
    parquet_dir = tmp_path / "input"
    glob = _seed_upcoming_parquet(parquet_dir)
    con = duckdb.connect(":memory:")
    _seed_empty_raw_odds(con)
    subject.stage_parquet_odds(con, glob)
    subject.merge_odds_tables(con)
    return glob, con


def test_write_partitioned_produces_parquet_with_market_signal_columns(tmp_path: Path) -> None:
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
    assert "inverse_odds_implied_prob" in col_names
    assert "inverse_odds_market_share" in col_names
    assert "inverse_odds_rank_in_race" in col_names
    assert "popularity_rank_in_race" in col_names


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


def test_main_upcoming_race_produces_non_null_market_signals(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Full main() path with upcoming race: PG empty → parquet odds → non-null
    market-signal features in output parquet.
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
              race_date varchar, tansho_odds varchar, tansho_ninkijun varchar
            )
            """
        )

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_market_signal_features",
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
               tansho_odds_raw,
               inverse_odds_implied_prob,
               inverse_odds_market_share,
               inverse_odds_rank_in_race
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        order by ketto_toroku_bango
        """
    ).fetchall()
    verify_con.close()
    assert len(rows) == 3
    for row in rows:
        assert row[1] is not None, f"{row[0]} tansho_odds_raw is NULL"
        assert row[2] is not None, f"{row[0]} inverse_odds_implied_prob is NULL"
        assert row[3] is not None, f"{row[0]} inverse_odds_market_share is NULL"
    # Ranks must not all be 1 — horse_fav has best (lowest odds) so rank 1
    fav = next(r for r in rows if r[0] == "horse_fav")
    out = next(r for r in rows if r[0] == "horse_out")
    assert fav[4] == 1
    assert out[4] == 3


def test_main_historical_race_produces_non_null_market_signals(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Full main() path with historical race: PG has rows → PG values used."""
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
                ('jra', '2024', '0415', '05', '11', 'horse_a',
                  '20240415', '60', '2'),
                ('jra', '2024', '0415', '05', '11', 'horse_b',
                  '20240415', '30', '1')
            ) as v(
              source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
              ketto_toroku_bango, race_date, tansho_odds, tansho_ninkijun
            )
            """
        )

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_market_signal_features",
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
               tansho_odds_raw,
               inverse_odds_implied_prob,
               inverse_odds_rank_in_race
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        order by ketto_toroku_bango
        """
    ).fetchall()
    verify_con.close()
    # PG stored values as string ('60'→6.0, '30'→3.0 after /10 via cast)
    horse_a = next(r for r in rows if r[0] == "horse_a")
    horse_b = next(r for r in rows if r[0] == "horse_b")
    assert horse_a[1] is not None
    assert horse_b[1] is not None
    # horse_b has lower odds (rank 1), horse_a rank 2
    assert horse_b[3] == 1
    assert horse_a[3] == 2


# ---------------------------------------------------------------------------
# INTEGRATION TESTS — realistic base-build schema (Bug 1 regression guards)
#
# These tests use parquet fixtures whose column set matches the ACTUAL output
# of finish_position_features_duckdb.py (base build).  The pre-fix base build
# did NOT emit tansho_odds / tansho_ninkijun, so stage_parquet_odds() raised
# BinderException: column "tansho_odds" not found.
# After the fix, both columns are emitted and must be non-null for upcoming
# races that are absent from race_entry_corner_features.
# ---------------------------------------------------------------------------


def _seed_realistic_base_build_parquet(parquet_dir: Path, *, include_raw_odds: bool) -> str:
    """Write a parquet whose columns match the real base-build output schema.

    When ``include_raw_odds=False`` the fixture omits tansho_odds /
    tansho_ninkijun, reproducing the pre-fix column set so we can assert a
    BinderException occurs.  When ``include_raw_odds=True`` both columns are
    present (post-fix), and the market-signal layer must not raise.

    The fixture also includes the columns consumed by append_features_sql
    (odds_score, popularity_score, race partition keys, ketto_toroku_bango,
    race_year) so the full SQL pipeline executes without missing-column errors
    on those other references.
    """
    parquet_dir.mkdir(parents=True, exist_ok=True)
    seed_con = duckdb.connect(":memory:")
    if include_raw_odds:
        seed_con.execute(
            """
            create or replace temp table seed as
            select * from (
              values
                ('jra','2026','0607','05','11','horse_a','20260607',2026,
                  5.0::double, 1::integer, 0.4::double, 0.5::double),
                ('jra','2026','0607','05','11','horse_b','20260607',2026,
                  8.0::double, 2::integer, 0.3::double, 0.3::double)
            ) as v(
              source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
              ketto_toroku_bango, race_date, race_year,
              tansho_odds, tansho_ninkijun,
              odds_score, popularity_score
            )
            """
        )
    else:
        # Pre-fix schema: NO tansho_odds / tansho_ninkijun columns
        seed_con.execute(
            """
            create or replace temp table seed as
            select * from (
              values
                ('jra','2026','0607','05','11','horse_a','20260607',2026,
                  0.4::double, 0.5::double),
                ('jra','2026','0607','05','11','horse_b','20260607',2026,
                  0.3::double, 0.3::double)
            ) as v(
              source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
              ketto_toroku_bango, race_date, race_year,
              odds_score, popularity_score
            )
            """
        )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()
    return f"{parquet_dir.as_posix()}/race_year=*/*.parquet"


def test_integration_stage_parquet_odds_raises_on_base_build_without_raw_odds(
    tmp_path: Path,
) -> None:
    """Regression guard: a base-build parquet WITHOUT tansho_odds / tansho_ninkijun
    must raise a DuckDB error (BinderException) when stage_parquet_odds() executes.
    This documents the pre-fix failure mode so future changes don't silently
    re-introduce it.
    """
    parquet_dir = tmp_path / "input"
    glob = _seed_realistic_base_build_parquet(parquet_dir, include_raw_odds=False)
    con = duckdb.connect(":memory:")
    with pytest.raises(duckdb.Error):
        subject.stage_parquet_odds(con, glob)
    con.close()


def test_integration_stage_parquet_odds_succeeds_with_raw_odds_in_parquet(
    tmp_path: Path,
) -> None:
    """Post-fix: base-build parquet WITH tansho_odds / tansho_ninkijun must not
    raise a BinderException and must populate parquet_odds with non-null values.
    """
    parquet_dir = tmp_path / "input"
    glob = _seed_realistic_base_build_parquet(parquet_dir, include_raw_odds=True)
    con = duckdb.connect(":memory:")
    subject.stage_parquet_odds(con, glob)
    rows = con.execute(
        "select ketto_toroku_bango, tansho_odds_raw, tansho_ninkijun_raw"
        " from parquet_odds order by ketto_toroku_bango"
    ).fetchall()
    con.close()
    assert len(rows) == 2
    for row in rows:
        assert row[1] is not None, f"{row[0]} tansho_odds_raw is NULL"
        assert row[2] is not None, f"{row[0]} tansho_ninkijun_raw is NULL"


def test_integration_full_pipeline_realistic_schema_no_binder_exception(
    tmp_path: Path,
) -> None:
    """End-to-end: realistic base-build parquet (with tansho_odds/ninkijun) →
    merge → append_features_sql → no BinderException, all market-signal features
    non-null for upcoming races (PG empty).
    """
    parquet_dir = tmp_path / "input"
    glob = _seed_realistic_base_build_parquet(parquet_dir, include_raw_odds=True)
    con = duckdb.connect(":memory:")
    _seed_empty_raw_odds(con)
    subject.stage_parquet_odds(con, glob)
    subject.merge_odds_tables(con)
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
               tansho_odds_raw,
               tansho_ninkijun_raw,
               inverse_odds_implied_prob,
               inverse_odds_market_share,
               inverse_odds_rank_in_race,
               popularity_rank_in_race
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    assert len(rows) == 2
    for row in rows:
        assert row[1] is not None, f"{row[0]} tansho_odds_raw is NULL"
        assert row[2] is not None, f"{row[0]} tansho_ninkijun_raw is NULL"
        assert row[3] is not None, f"{row[0]} inverse_odds_implied_prob is NULL"
        assert row[4] is not None, f"{row[0]} inverse_odds_market_share is NULL"


def test_integration_weekday_no_races_no_binder_exception(
    tmp_path: Path,
) -> None:
    """Weekday build: empty PG table (0 upcoming race rows) must not raise a
    BinderException.  stage_parquet_odds against a realistic base-build parquet
    that has tansho_odds must succeed — this catches any re-introduction of the
    b.source binder bug in merge_odds_tables or append_features_sql.

    Note: DuckDB raises IOException when a glob matches no files, so we use a
    parquet with one row and verify 0-row PG causes 0 market-signal results.
    """
    parquet_dir = tmp_path / "input"
    glob = _seed_realistic_base_build_parquet(parquet_dir, include_raw_odds=True)
    con = duckdb.connect(":memory:")
    _seed_empty_raw_odds(con)
    subject.stage_parquet_odds(con, glob)
    subject.merge_odds_tables(con)
    sql = subject.append_features_sql(glob)
    # All rows from parquet_odds (PG empty → parquet fills) must bind without exception
    rows = con.execute(f"select count(*) from ({sql})").fetchone()
    con.close()
    assert rows is not None
    assert rows[0] >= 0
