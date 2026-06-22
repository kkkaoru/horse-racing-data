"""Unit tests for add_exotic_odds_features.py.

All tests are pure-function or use DuckDB in-memory; no network or PG I/O.
"""
from __future__ import annotations

import math
from pathlib import Path
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import add_exotic_odds_features as subject


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


def test_parse_args_requires_input_and_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_parse_args_default_category_is_nar(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.category == "nar"


def test_parse_args_banei_category(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir", str(tmp_path / "in"),
            "--output-dir", str(tmp_path / "out"),
            "--category", "ban-ei",
        ]
    )
    assert args.category == "ban-ei"


def test_parse_args_default_dates(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.from_date == "20100101"
    assert args.to_date == "20991231"


def test_parse_args_custom_dates(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir", str(tmp_path / "in"),
            "--output-dir", str(tmp_path / "out"),
            "--from-date", "20230101",
            "--to-date", "20241231",
        ]
    )
    assert args.from_date == "20230101"
    assert args.to_date == "20241231"


def test_parse_args_pg_url(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir", str(tmp_path / "in"),
            "--output-dir", str(tmp_path / "out"),
            "--pg-url", "postgresql://test/db",
        ]
    )
    assert args.pg_url == "postgresql://test/db"


# ---------------------------------------------------------------------------
# keibajo_filter_sql
# ---------------------------------------------------------------------------


def test_keibajo_filter_sql_banei() -> None:
    sql = subject.keibajo_filter_sql("ban-ei")
    assert "83" in sql
    assert "keibajo_code" in sql


def test_keibajo_filter_sql_nar_includes_30_range() -> None:
    sql = subject.keibajo_filter_sql("nar")
    assert "IN (" in sql
    assert "'30'" in sql
    assert "'44'" in sql
    assert "'83'" not in sql


def test_keibajo_filter_sql_nar_includes_50_range() -> None:
    sql = subject.keibajo_filter_sql("nar")
    assert "'50'" in sql


# ---------------------------------------------------------------------------
# install_and_attach_pg
# ---------------------------------------------------------------------------


def test_install_and_attach_pg_executes_three_statements() -> None:
    executed: list[str] = []

    class FakeConn:
        def execute(self, query: str) -> None:
            executed.append(query)

    subject.install_and_attach_pg(FakeConn(), "postgresql://stub/horse_racing")
    assert executed[0] == "install postgres"
    assert executed[1] == "load postgres"
    assert "attach" in executed[2]
    assert "postgresql://stub/horse_racing" in executed[2]
    assert "postgres" in executed[2]
    assert "read_only" in executed[2]


# ---------------------------------------------------------------------------
# decode_sanrenpuku_to_horse_inv_probs
# ---------------------------------------------------------------------------


def _make_o5_entry(h1: int, h2: int, h3: int, odds_raw: int) -> str:
    """Build a single 15-char sanrenpuku entry."""
    return f"{h1:02d}{h2:02d}{h3:02d}{odds_raw:05d}0000"


def test_decode_sanrenpuku_empty_string() -> None:
    result = subject.decode_sanrenpuku_to_horse_inv_probs("")
    assert result == {}


def test_decode_sanrenpuku_single_triple() -> None:
    # odds_raw=100 → odds=10.0x → inv_prob=0.1 per triple member
    entry = _make_o5_entry(1, 2, 3, 100)
    result = subject.decode_sanrenpuku_to_horse_inv_probs(entry)
    assert set(result.keys()) == {1, 2, 3}
    assert math.isclose(result[1], 0.1, rel_tol=1e-6)
    assert math.isclose(result[2], 0.1, rel_tol=1e-6)
    assert math.isclose(result[3], 0.1, rel_tol=1e-6)


def test_decode_sanrenpuku_two_triples_share_horse() -> None:
    # horse 1 appears in both triples → its inv_prob is doubled
    entry = _make_o5_entry(1, 2, 3, 100) + _make_o5_entry(1, 4, 5, 200)
    result = subject.decode_sanrenpuku_to_horse_inv_probs(entry)
    # triple 1: inv=0.1; triple 2: inv=0.05
    assert math.isclose(result[1], 0.1 + 0.05, rel_tol=1e-6)
    assert math.isclose(result[2], 0.1, rel_tol=1e-6)
    assert math.isclose(result[4], 0.05, rel_tol=1e-6)


def test_decode_sanrenpuku_skips_zero_odds() -> None:
    entry = _make_o5_entry(0, 0, 0, 0) + _make_o5_entry(1, 2, 3, 100)
    result = subject.decode_sanrenpuku_to_horse_inv_probs(entry)
    assert 0 not in result
    assert 1 in result


def test_decode_sanrenpuku_skips_zero_horse() -> None:
    # horse number 0 is invalid
    entry = _make_o5_entry(0, 2, 3, 100)
    result = subject.decode_sanrenpuku_to_horse_inv_probs(entry)
    assert 0 not in result


# ---------------------------------------------------------------------------
# decode_wide_to_horse_inv_probs
# ---------------------------------------------------------------------------


def _make_o3_entry(h1: int, h2: int, lo: int, hi: int) -> str:
    """Build a single 17-char wide entry."""
    return f"{h1:02d}{h2:02d}{lo:05d}{hi:05d}000"


def test_decode_wide_empty_string() -> None:
    result = subject.decode_wide_to_horse_inv_probs("")
    assert result == {}


def test_decode_wide_single_pair() -> None:
    # lo=100 (10.0x), hi=200 (20.0x) → mid_odds=15.0x → inv=1/15
    entry = _make_o3_entry(1, 2, 100, 200)
    result = subject.decode_wide_to_horse_inv_probs(entry)
    assert set(result.keys()) == {1, 2}
    expected = 1.0 / 15.0
    assert math.isclose(result[1], expected, rel_tol=1e-6)
    assert math.isclose(result[2], expected, rel_tol=1e-6)


def test_decode_wide_skips_zero_lo() -> None:
    entry = _make_o3_entry(1, 2, 0, 200)
    result = subject.decode_wide_to_horse_inv_probs(entry)
    assert result == {}


def test_decode_wide_skips_zero_horse() -> None:
    entry = _make_o3_entry(0, 2, 100, 200)
    result = subject.decode_wide_to_horse_inv_probs(entry)
    assert 0 not in result


def test_decode_wide_two_pairs_share_horse() -> None:
    entry = _make_o3_entry(1, 2, 100, 200) + _make_o3_entry(1, 3, 200, 300)
    result = subject.decode_wide_to_horse_inv_probs(entry)
    inv1 = 1.0 / 15.0
    inv2 = 1.0 / 25.0
    assert math.isclose(result[1], inv1 + inv2, rel_tol=1e-6)
    assert math.isclose(result[2], inv1, rel_tol=1e-6)
    assert math.isclose(result[3], inv2, rel_tol=1e-6)


# ---------------------------------------------------------------------------
# decode_umaren_to_horse_inv_probs
# ---------------------------------------------------------------------------


def _make_o2_entry(h1: int, h2: int, odds_raw: int) -> str:
    """Build a single 13-char umaren entry."""
    return f"{h1:02d}{h2:02d}{odds_raw:05d}0000"


def test_decode_umaren_empty_string() -> None:
    result = subject.decode_umaren_to_horse_inv_probs("")
    assert result == {}


def test_decode_umaren_single_pair() -> None:
    # odds_raw=150 → odds=15.0x → inv=1/15
    entry = _make_o2_entry(1, 2, 150)
    result = subject.decode_umaren_to_horse_inv_probs(entry)
    assert set(result.keys()) == {1, 2}
    expected = 1.0 / 15.0
    assert math.isclose(result[1], expected, rel_tol=1e-6)
    assert math.isclose(result[2], expected, rel_tol=1e-6)


def test_decode_umaren_skips_zero_odds() -> None:
    entry = _make_o2_entry(1, 2, 0)
    result = subject.decode_umaren_to_horse_inv_probs(entry)
    assert result == {}


def test_decode_umaren_skips_zero_horse() -> None:
    entry = _make_o2_entry(0, 2, 150)
    result = subject.decode_umaren_to_horse_inv_probs(entry)
    assert 0 not in result


def test_decode_umaren_two_pairs_share_horse() -> None:
    entry = _make_o2_entry(1, 2, 100) + _make_o2_entry(1, 3, 200)
    result = subject.decode_umaren_to_horse_inv_probs(entry)
    inv1 = 1.0 / 10.0
    inv2 = 1.0 / 20.0
    assert math.isclose(result[1], inv1 + inv2, rel_tol=1e-6)
    assert math.isclose(result[2], inv1, rel_tol=1e-6)
    assert math.isclose(result[3], inv2, rel_tol=1e-6)


# ---------------------------------------------------------------------------
# normalize_inv_probs
# ---------------------------------------------------------------------------


def test_normalize_inv_probs_empty() -> None:
    assert subject.normalize_inv_probs({}) == {}


def test_normalize_inv_probs_zero_total() -> None:
    assert subject.normalize_inv_probs({1: 0.0, 2: 0.0}) == {}


def test_normalize_inv_probs_sums_to_one() -> None:
    inv = {1: 0.3, 2: 0.5, 3: 0.2}
    normed = subject.normalize_inv_probs(inv)
    assert math.isclose(sum(normed.values()), 1.0, rel_tol=1e-9)


def test_normalize_inv_probs_preserves_rank_order() -> None:
    inv = {1: 0.6, 2: 0.3, 3: 0.1}
    normed = subject.normalize_inv_probs(inv)
    assert normed[1] > normed[2] > normed[3]


def test_normalize_inv_probs_single_horse() -> None:
    normed = subject.normalize_inv_probs({5: 0.8})
    assert math.isclose(normed[5], 1.0, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# append_features_sql (SQL structure check — no DB I/O)
# ---------------------------------------------------------------------------


def test_append_features_sql_contains_expected_columns() -> None:
    sql = subject.append_features_sql("/path/to/race_year=*/*.parquet")
    assert "exotic_sanrenpuku_p3" in sql
    assert "exotic_wide_p3" in sql
    assert "exotic_umaren_p2" in sql
    assert "left join exotic_o5" in sql
    assert "left join exotic_o3" in sql
    assert "left join exotic_o2" in sql


def test_append_features_sql_joins_on_race_keys() -> None:
    sql = subject.append_features_sql("/path/race_year=*/*.parquet")
    assert "kaisai_nen" in sql
    assert "kaisai_tsukihi" in sql
    assert "keibajo_code" in sql
    assert "race_bango" in sql
    assert "umaban" in sql


# ---------------------------------------------------------------------------
# stage_exotic_features + write_partitioned (integration via DuckDB in-memory)
# ---------------------------------------------------------------------------


def _build_in_memory_pg_tables(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Create stub nvd_o5/o3/o2 tables in a 'pg' schema for testing."""
    con.execute("create schema if not exists pg")

    # Build a minimal 2-horse sanrenpuku packed string:
    # Only combo 1-2-3 with odds=100 (10.0x)
    o5_entry = f"{'01':s}{'02':s}{'03':s}{'00100':s}{'0000':s}"
    o5_packed = o5_entry + "0" * (subject.O5_MAX_COMBOS - 1) * subject.O5_STRIDE

    con.execute(
        f"""
        create or replace table pg.nvd_o5 as
        select
            '2023' as kaisai_nen,
            '0601' as kaisai_tsukihi,
            '44' as keibajo_code,
            '01' as race_bango,
            '5' as data_kubun,
            '{o5_packed}' as odds_sanrenpuku
        """
    )

    o3_entry = f"{'01':s}{'02':s}{'00100':s}{'00200':s}{'000':s}"
    o3_packed = o3_entry + "0" * (subject.O3_MAX_COMBOS - 1) * subject.O3_STRIDE

    con.execute(
        f"""
        create or replace table pg.nvd_o3 as
        select
            '2023' as kaisai_nen,
            '0601' as kaisai_tsukihi,
            '44' as keibajo_code,
            '01' as race_bango,
            '5' as data_kubun,
            '{o3_packed}' as odds_wide
        """
    )

    o2_entry = f"{'01':s}{'02':s}{'00150':s}{'0000':s}"
    o2_packed = o2_entry + "0" * (subject.O2_MAX_COMBOS - 1) * subject.O2_STRIDE

    con.execute(
        f"""
        create or replace table pg.nvd_o2 as
        select
            '2023' as kaisai_nen,
            '0601' as kaisai_tsukihi,
            '44' as keibajo_code,
            '01' as race_bango,
            '5' as data_kubun,
            '{o2_packed}' as odds_umaren
        """
    )


def test_stage_exotic_features_nar_creates_three_tables(
    tmp_path: Path,
) -> None:
    con = duckdb.connect(":memory:")
    _build_in_memory_pg_tables(con)
    subject.stage_exotic_features(con, "nar", "20230101", "20991231")
    tables = {row[0] for row in con.execute("show tables").fetchall()}
    assert "exotic_o5" in tables
    assert "exotic_o3" in tables
    assert "exotic_o2" in tables
    con.close()


def test_stage_exotic_features_o5_has_expected_horses(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _build_in_memory_pg_tables(con)
    subject.stage_exotic_features(con, "nar", "20230101", "20991231")
    rows = con.execute("select umaban from exotic_o5 order by umaban").fetchall()
    umabans = [r[0] for r in rows]
    assert 1 in umabans
    assert 2 in umabans
    assert 3 in umabans
    con.close()


def test_stage_exotic_features_banei_filters_keibajo(tmp_path: Path) -> None:
    """Ban-ei filter (keibajo='83') excludes the stub NAR keibajo='44'."""
    con = duckdb.connect(":memory:")
    _build_in_memory_pg_tables(con)
    subject.stage_exotic_features(con, "ban-ei", "20230101", "20991231")
    count = con.execute("select count(*) from exotic_o5").fetchone()
    assert count is not None
    assert count[0] == 0  # keibajo='44' is NAR, not Ban-ei
    con.close()


def test_stage_exotic_features_date_filter_excludes_old(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _build_in_memory_pg_tables(con)
    # from_date after the test row date
    subject.stage_exotic_features(con, "nar", "20240101", "20991231")
    count = con.execute("select count(*) from exotic_o5").fetchone()
    assert count is not None
    assert count[0] == 0
    con.close()


def test_stage_exotic_features_normalization_sums_to_one(tmp_path: Path) -> None:
    """All horses in a race should have exotic_sanrenpuku_p3 summing to 1.0."""
    con = duckdb.connect(":memory:")
    _build_in_memory_pg_tables(con)
    subject.stage_exotic_features(con, "nar", "20230101", "20991231")
    total = con.execute(
        "select sum(exotic_sanrenpuku_p3) from exotic_o5"
    ).fetchone()
    assert total is not None
    assert total[0] is not None
    assert math.isclose(float(total[0]), 1.0, rel_tol=1e-6)
    con.close()


# ---------------------------------------------------------------------------
# append_features_sql + write_partitioned end-to-end
# ---------------------------------------------------------------------------


def _write_stub_parquet(tmp_path: Path) -> Path:
    """Write a minimal stub feature parquet with race_year partition.

    kaisai_tsukihi uses MMDD format (e.g. '0601') matching the warehouse and
    real feature parquets.
    """
    year_dir = tmp_path / "race_year=2023"
    year_dir.mkdir(parents=True)
    table = pa.table(
        {
            "race_year": pa.array([2023, 2023, 2023], type=pa.int32()),
            "source": pa.array(["nar", "nar", "nar"], type=pa.string()),
            "kaisai_nen": pa.array(["2023", "2023", "2023"], type=pa.string()),
            "kaisai_tsukihi": pa.array(
                ["0601", "0601", "0601"], type=pa.string()
            ),
            "keibajo_code": pa.array(["44", "44", "44"], type=pa.string()),
            "race_bango": pa.array(["01", "01", "01"], type=pa.string()),
            "umaban": pa.array([1, 2, 3], type=pa.int32()),
            "finish_position": pa.array([1.0, 2.0, 3.0], type=pa.float32()),
        }
    )
    pq.write_table(table, str(year_dir / "data.parquet"))
    return tmp_path


def test_write_partitioned_appends_exotic_columns(tmp_path: Path) -> None:
    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    _write_stub_parquet(in_dir)

    con = duckdb.connect(":memory:")
    _build_in_memory_pg_tables(con)
    subject.stage_exotic_features(con, "nar", "20230101", "20991231")

    input_glob = f"{in_dir.as_posix()}/race_year=*/*.parquet"
    sql = subject.append_features_sql(input_glob)
    subject.write_partitioned(con, sql, out_dir)
    con.close()

    written = list(out_dir.rglob("*.parquet"))
    assert len(written) >= 1

    # Read individual parquet files to avoid pyarrow hive-partition schema
    # merging conflicts (race_year is stored in both the column data and the
    # folder name, which causes ArrowTypeError on schema merge).
    df = pd.concat([pd.read_parquet(p) for p in written], ignore_index=True)
    assert "exotic_sanrenpuku_p3" in df.columns
    assert "exotic_wide_p3" in df.columns
    assert "exotic_umaren_p2" in df.columns
    # Original columns preserved
    assert "finish_position" in df.columns
    assert "umaban" in df.columns
    # race_year is stored in the file data (not only in the folder name)
    assert "race_year" in df.columns


def test_write_partitioned_null_where_no_odds(tmp_path: Path) -> None:
    """Horses missing from the odds tables get NULL exotic columns."""
    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    # Stub parquet with keibajo='99' (not in the test odds table)
    year_dir = in_dir / "race_year=2023"
    year_dir.mkdir(parents=True)
    table = pa.table(
        {
            "race_year": pa.array([2023], type=pa.int32()),
            "source": pa.array(["nar"], type=pa.string()),
            "kaisai_nen": pa.array(["2023"], type=pa.string()),
            "kaisai_tsukihi": pa.array(["0601"], type=pa.string()),
            "keibajo_code": pa.array(["99"], type=pa.string()),
            "race_bango": pa.array(["01"], type=pa.string()),
            "umaban": pa.array([1], type=pa.int32()),
            "finish_position": pa.array([1.0], type=pa.float32()),
        }
    )
    pq.write_table(table, str(year_dir / "data.parquet"))

    con = duckdb.connect(":memory:")
    _build_in_memory_pg_tables(con)
    subject.stage_exotic_features(con, "nar", "20230101", "20991231")
    input_glob = f"{in_dir.as_posix()}/race_year=*/*.parquet"
    sql = subject.append_features_sql(input_glob)
    subject.write_partitioned(con, sql, out_dir)
    con.close()

    written = list(out_dir.rglob("*.parquet"))
    assert len(written) >= 1
    df = pd.concat([pd.read_parquet(p) for p in written], ignore_index=True)
    assert df["exotic_sanrenpuku_p3"].isna().all()
    assert df["exotic_wide_p3"].isna().all()
    assert df["exotic_umaren_p2"].isna().all()


def test_write_partitioned_overwrites_existing_output(tmp_path: Path) -> None:
    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    _write_stub_parquet(in_dir)

    # Pre-create a stale output directory
    out_dir.mkdir(parents=True)
    stale = out_dir / "stale.txt"
    stale.write_text("old")

    con = duckdb.connect(":memory:")
    _build_in_memory_pg_tables(con)
    subject.stage_exotic_features(con, "nar", "20230101", "20991231")
    input_glob = f"{in_dir.as_posix()}/race_year=*/*.parquet"
    sql = subject.append_features_sql(input_glob)
    subject.write_partitioned(con, sql, out_dir)
    con.close()

    # Stale file should be gone
    assert not stale.exists()


# ---------------------------------------------------------------------------
# o5_decode_sql / o3_decode_sql / o2_decode_sql SQL content checks
# ---------------------------------------------------------------------------


def test_o5_decode_sql_references_nvd_o5() -> None:
    sql = subject.o5_decode_sql()
    assert "nvd_o5" in sql
    assert "odds_sanrenpuku" in sql


def test_o3_decode_sql_references_nvd_o3() -> None:
    sql = subject.o3_decode_sql()
    assert "nvd_o3" in sql
    assert "odds_wide" in sql


def test_o2_decode_sql_references_nvd_o2() -> None:
    sql = subject.o2_decode_sql()
    assert "nvd_o2" in sql
    assert "odds_umaren" in sql


def test_o5_stride_constant_matches_schema() -> None:
    assert subject.O5_STRIDE == 15


def test_o3_stride_constant_matches_schema() -> None:
    assert subject.O3_STRIDE == 17


def test_o2_stride_constant_matches_schema() -> None:
    assert subject.O2_STRIDE == 13


# ---------------------------------------------------------------------------
# ValueError branches in pure-Python decoders
# ---------------------------------------------------------------------------


def test_decode_sanrenpuku_skips_non_numeric_entry() -> None:
    # Corrupt horse number: 'XX' is not parseable as int
    entry = "XX0203001000000"
    assert len(entry) == subject.O5_STRIDE
    result = subject.decode_sanrenpuku_to_horse_inv_probs(entry)
    assert result == {}


def test_decode_wide_skips_non_numeric_entry() -> None:
    # Corrupt lo field: 'XXXXX'
    entry = "01" + "02" + "XXXXX" + "00200" + "000"
    assert len(entry) == subject.O3_STRIDE
    result = subject.decode_wide_to_horse_inv_probs(entry)
    assert result == {}


def test_decode_umaren_skips_non_numeric_entry() -> None:
    # Corrupt odds field
    entry = "01" + "02" + "XXXXX" + "0000"
    assert len(entry) == subject.O2_STRIDE
    result = subject.decode_umaren_to_horse_inv_probs(entry)
    assert result == {}


def test_decode_sanrenpuku_truncated_string_breaks() -> None:
    # String shorter than one stride → loop never enters first slot
    short = "01020300"  # < 15 chars
    result = subject.decode_sanrenpuku_to_horse_inv_probs(short)
    assert result == {}


def test_decode_wide_truncated_string_breaks() -> None:
    short = "010200100"  # < 17 chars
    result = subject.decode_wide_to_horse_inv_probs(short)
    assert result == {}


def test_decode_umaren_truncated_string_breaks() -> None:
    short = "010200"  # < 13 chars
    result = subject.decode_umaren_to_horse_inv_probs(short)
    assert result == {}


# ---------------------------------------------------------------------------
# main() smoke test (mocked I/O)
# ---------------------------------------------------------------------------


def test_main_runs_without_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() must run end-to-end with a mocked pg connection."""
    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    _write_stub_parquet(in_dir)

    calls: list[str] = []

    class StubCon:
        def execute(self, sql: str) -> "StubCon":
            calls.append(sql)
            return self

        def close(self) -> None:
            pass

    import duckdb as _duckdb

    real_connect = _duckdb.connect

    def mock_connect(dbpath: str) -> duckdb.DuckDBPyConnection:
        # Use real in-memory DuckDB but pre-populate pg tables
        con = real_connect(":memory:")
        _build_in_memory_pg_tables(con)
        return con

    monkeypatch.setattr(_duckdb, "connect", mock_connect)
    monkeypatch.setattr(subject, "install_and_attach_pg", lambda con, url: None)

    monkeypatch.setattr(
        "sys.argv",
        [
            "add_exotic_odds_features",
            "--input-dir", str(in_dir),
            "--output-dir", str(out_dir),
            "--category", "nar",
        ],
    )
    subject.main()
    assert out_dir.exists()
