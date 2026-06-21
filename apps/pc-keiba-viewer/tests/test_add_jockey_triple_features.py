"""Tests for add_jockey_triple_features.py."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add_jockey_triple_features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_jockey_triple_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_jockey_triple_features"] = subject
_spec.loader.exec_module(subject)


class FakeConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, query: str) -> object:
        self.statements.append(query)
        return None


# ── parse_args ─────────────────────────────────────────────────────────────────


def test_parse_args_requires_input_and_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_parse_args_missing_required_raises_system_exit() -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(["--input-dir", "/tmp/only-input"])


def test_parse_args_pg_url_default_contains_localhost(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert "127.0.0.1" in args.pg_url


def test_parse_args_history_from_year_default_is_2005(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.history_from_year == 2005


def test_parse_args_history_from_year_override(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--history-from-year",
            "2010",
        ]
    )
    assert args.history_from_year == 2010


def test_parse_args_accepts_resource_args(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--threads",
            "4",
            "--memory-limit",
            "6GB",
        ]
    )
    assert args.threads == 4
    assert args.memory_limit == "6GB"


# ── constants ──────────────────────────────────────────────────────────────────


def test_jra_keibajo_regexp_matches_only_jra_codes() -> None:
    con = duckdb.connect(":memory:")
    rows = con.execute(
        f"""
        with v(code) as (values ('01'), ('05'), ('10'), ('11'), ('30'), ('83'))
        select code, regexp_matches(code, '{subject.JRA_KEIBAJO_REGEXP}') from v order by code
        """
    ).fetchall()
    con.close()
    matched = {row[0]: row[1] for row in rows}
    assert matched["01"] is True
    assert matched["05"] is True
    assert matched["10"] is True
    assert matched["11"] is False
    assert matched["30"] is False
    assert matched["83"] is False


def test_distance_band_boundaries() -> None:
    assert subject.SPRINT_MAX == 1400
    assert subject.MILE_MAX == 1800
    assert subject.INTERMEDIATE_MAX == 2200


def test_surface_track_code_ranges() -> None:
    assert subject.TURF_TRACK_CODE_MIN == 10
    assert subject.TURF_TRACK_CODE_MAX == 22
    assert subject.DIRT_TRACK_CODE_MIN == 23
    assert subject.DIRT_TRACK_CODE_MAX == 29


# ── dband_case_sql ─────────────────────────────────────────────────────────────


def test_dband_case_sql_assigns_correct_bands_in_duckdb() -> None:
    con = duckdb.connect(":memory:")
    sql = subject.dband_case_sql("kyori")
    rows = con.execute(
        f"""
        with v(kyori) as (values (1200), (1400), (1600), (1800), (2000), (2200), (2400), (3600))
        select kyori, {sql} as dband from v order by kyori
        """
    ).fetchall()
    con.close()
    by_kyori = {row[0]: row[1] for row in rows}
    assert by_kyori[1200] == "sprint"
    assert by_kyori[1400] == "sprint"
    assert by_kyori[1600] == "mile"
    assert by_kyori[1800] == "mile"
    assert by_kyori[2000] == "intermediate"
    assert by_kyori[2200] == "intermediate"
    assert by_kyori[2400] == "long"
    assert by_kyori[3600] == "long"


# ── surface_case_sql ──────────────────────────────────────────────────────────


def test_surface_case_sql_assigns_correct_surfaces_in_duckdb() -> None:
    con = duckdb.connect(":memory:")
    sql = subject.surface_case_sql("tc")
    rows = con.execute(
        f"""
        with v(tc) as (values (10), (15), (22), (23), (25), (29), (30), (51))
        select tc, {sql} as surface from v order by tc
        """
    ).fetchall()
    con.close()
    by_tc = {row[0]: row[1] for row in rows}
    assert by_tc[10] == "turf"
    assert by_tc[15] == "turf"
    assert by_tc[22] == "turf"
    assert by_tc[23] == "dirt"
    assert by_tc[25] == "dirt"
    assert by_tc[29] == "dirt"
    assert by_tc[30] == "other"
    assert by_tc[51] == "other"


# ── install_and_attach_pg ──────────────────────────────────────────────────────


def test_install_and_attach_pg_executes_three_statements() -> None:
    conn = FakeConn()
    subject.install_and_attach_pg(conn, "postgresql://stub/horse_racing")
    assert conn.statements[0] == "install postgres"
    assert conn.statements[1] == "load postgres"
    assert conn.statements[2].startswith("attach 'postgresql://stub/horse_racing'")
    assert "read_only" in conn.statements[2]


# ── stage_jockey_history ───────────────────────────────────────────────────────


def test_stage_jockey_history_sql_joins_jvd_ra_on_race_tuple() -> None:
    conn = FakeConn()
    subject.stage_jockey_history(conn, 2005)
    body = " ".join(conn.statements)
    assert "from pg.jvd_se se" in body
    assert "join pg.jvd_ra ra" in body
    assert "ra.kaisai_nen" in body and "se.kaisai_nen" in body
    assert "ra.kaisai_tsukihi" in body and "se.kaisai_tsukihi" in body
    assert "ra.keibajo_code" in body and "se.keibajo_code" in body
    assert "ra.race_bango" in body and "se.race_bango" in body


def test_stage_jockey_history_sql_filters_jra_year_and_valid_finish() -> None:
    conn = FakeConn()
    subject.stage_jockey_history(conn, 2010)
    body = " ".join(conn.statements)
    assert "regexp_matches(se.keibajo_code, '^0[1-9]$|^10$')" in body
    assert "cast(se.kaisai_nen as integer) >= 2010" in body
    assert "se.kishu_code is not null" in body
    assert ">= 1" in body


def test_stage_jockey_history_sql_contains_dband_and_surface_case() -> None:
    conn = FakeConn()
    subject.stage_jockey_history(conn, 2005)
    body = " ".join(conn.statements)
    assert "sprint" in body
    assert "mile" in body
    assert "intermediate" in body
    assert "long" in body
    assert "turf" in body
    assert "dirt" in body


# ── append_features_sql ────────────────────────────────────────────────────────


def test_append_features_sql_contains_three_jc_columns() -> None:
    sql = subject.append_features_sql("/tmp/x/race_year=*/*.parquet")
    assert "jt.jc_win_rate" in sql
    assert "jt.jc_avg_finish" in sql
    assert "jt.jc_support_n" in sql


def test_append_features_sql_left_joins_on_race_and_horse() -> None:
    sql = subject.append_features_sql("/tmp/x/race_year=*/*.parquet")
    assert "left join jockey_triple_agg jt" in sql
    assert "jt.source" in sql and "b.source" in sql
    assert "jt.kaisai_nen" in sql and "b.kaisai_nen" in sql
    assert "jt.kaisai_tsukihi" in sql and "b.kaisai_tsukihi" in sql
    assert "jt.keibajo_code" in sql and "b.keibajo_code" in sql
    assert "jt.race_bango" in sql and "b.race_bango" in sql
    assert "jt.ketto_toroku_bango" in sql and "b.ketto_toroku_bango" in sql


def test_append_features_sql_reads_parquet_glob() -> None:
    sql = subject.append_features_sql("/tmp/x/race_year=*/*.parquet")
    assert "read_parquet('/tmp/x/race_year=*/*.parquet'" in sql


# ── write_partitioned ──────────────────────────────────────────────────────────


def test_write_partitioned_creates_output_dir_and_emits_copy(tmp_path: Path) -> None:
    conn = FakeConn()
    out_dir = tmp_path / "fresh-out"
    subject.write_partitioned(conn, "select 1 as race_year", out_dir)
    assert out_dir.exists()
    assert conn.statements[0].startswith("copy (select 1 as race_year) to ")
    assert "partition_by (race_year)" in conn.statements[0]


def test_write_partitioned_removes_preexisting_output_dir(tmp_path: Path) -> None:
    conn = FakeConn()
    out_dir = tmp_path / "stale-out"
    stale_file = out_dir / "race_year=1999" / "data_0.parquet"
    stale_file.parent.mkdir(parents=True)
    stale_file.write_text("stale")
    subject.write_partitioned(conn, "select 1 as race_year", out_dir)
    assert out_dir.exists()
    assert not stale_file.exists()


# ── functional: staging pipeline against in-memory DuckDB ──────────────────────


def _seed_pg_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Seed minimal jvd_se + jvd_ra rows for the functional test.

    Target race: 2025/0415 at venue=06, race=11.
    horse_a ridden by J01; horse_b ridden by J02; horse_c has no jockey entry.
    J01 has 3 prior rides at venue=06/1600m/turf (mile, turf) before 20250415.
    J02 has 1 prior ride at the same combo.
    """
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.jvd_se as
        select * from (
          values
            -- J01 history at venue=06 (prior rides, strictly before 20250415)
            ('2025', '0301', '06', '01', 'J01', 'horse_x', '1'),
            ('2025', '0201', '06', '01', 'J01', 'horse_y', '2'),
            ('2024', '1101', '06', '01', 'J01', 'horse_z', '3'),
            -- J01 at venue=05 (different venue — must NOT count for venue=06 agg)
            ('2025', '0310', '05', '01', 'J01', 'horse_w', '1'),
            -- J02 history at venue=06 (1 prior ride)
            ('2025', '0301', '06', '01', 'J02', 'horse_q', '2'),
            -- Target race entries (20250415, venue=06, race=11)
            ('2025', '0415', '06', '11', 'J01', 'horse_a', null),
            ('2025', '0415', '06', '11', 'J02', 'horse_b', null),
            -- horse_c has no jockey row in the target race (no kishu_code available)
            -- NAR row excluded by JRA keibajo regexp
            ('2025', '0301', '30', '01', 'J01', 'horse_n', '1')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               kishu_code, ketto_toroku_bango, kakutei_chakujun)
        """
    )
    # jvd_ra: all venue=06 races 1600m turf (track_code=17 → mile dband, turf surface)
    con.execute(
        """
        create table pg.jvd_ra as
        select * from (
          values
            ('2025', '0301', '06', '01', '1600', '17'),
            ('2025', '0201', '06', '01', '1600', '17'),
            ('2024', '1101', '06', '01', '1600', '17'),
            ('2025', '0310', '05', '01', '1600', '17'),
            ('2025', '0415', '06', '11', '1600', '17'),
            ('2025', '0301', '30', '01', '1600', '17')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kyori, track_code)
        """
    )


def _seed_base_parquet(input_dir: Path) -> None:
    """Write a minimal feature parquet: 3 JRA entries + 1 NAR entry."""
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '06', '11', 'horse_a', '20250415', 2025, '1600', '17'),
            ('jra', '2025', '0415', '06', '11', 'horse_b', '20250415', 2025, '1600', '17'),
            ('jra', '2025', '0415', '06', '11', 'horse_c', '20250415', 2025, '1600', '17'),
            ('nar', '2025', '0415', '30', '01', 'horse_n', '20250415', 2025, '1600', '17')
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year, kyori, track_code)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()


def test_staging_pipeline_computes_jockey_triple_aggregates(tmp_path: Path) -> None:
    """Functional test: staging helpers produce correct causal aggregates."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)
    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    con = duckdb.connect(":memory:")
    _seed_pg_schema(con)
    subject.stage_jockey_history(con, 2005)
    subject.stage_base_entries(con, input_glob)
    subject.stage_jockey_triple_agg(con)

    rows = con.execute(
        """
        select ketto_toroku_bango, jc_win_rate, jc_avg_finish, jc_support_n
        from jockey_triple_agg order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()

    by_horse = {row[0]: row for row in rows}
    # horse_a is ridden by J01 at venue=06/mile/turf.
    # J01 prior rides at that combo (strictly before 20250415):
    #   horse_x(1), horse_y(2), horse_z(3) → n=3, wins=1
    assert by_horse["horse_a"][3] == 3         # jc_support_n
    assert by_horse["horse_a"][1] == pytest.approx(1.0 / 3.0)   # jc_win_rate
    assert by_horse["horse_a"][2] == pytest.approx((1.0 + 2.0 + 3.0) / 3.0)  # jc_avg_finish

    # horse_b is ridden by J02 at venue=06/mile/turf.
    # J02 prior rides at that combo: horse_q(2) → n=1, wins=0
    assert by_horse["horse_b"][3] == 1
    assert by_horse["horse_b"][1] == pytest.approx(0.0)
    assert by_horse["horse_b"][2] == pytest.approx(2.0)

    # horse_c has no jockey row in target race (not in jvd_se for race=11) → agg absent,
    # LEFT JOIN in append_features_sql will emit NULL.
    assert "horse_c" not in by_horse


def test_main_end_to_end_appends_three_jc_columns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)

    def _fake_install_and_attach(con: duckdb.DuckDBPyConnection, _pg_url: str) -> None:
        _seed_pg_schema(con)

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_jockey_triple_features",
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--threads",
            "2",
            "--memory-limit",
            "1GB",
        ],
    )
    subject.main()

    verify_con = duckdb.connect(":memory:")
    rows = verify_con.execute(
        f"""
        select source, ketto_toroku_bango, jc_win_rate, jc_avg_finish, jc_support_n
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        order by source, ketto_toroku_bango
        """
    ).fetchall()
    verify_con.close()

    by_key = {(row[0], row[1]): row for row in rows}
    assert len(by_key) == 4

    # horse_a (J01): 3 prior rides at venue=06/mile/turf — wins=1, avg_finish=2.0
    assert by_key[("jra", "horse_a")][4] == 3
    assert by_key[("jra", "horse_a")][2] == pytest.approx(1.0 / 3.0)
    assert by_key[("jra", "horse_a")][3] == pytest.approx(2.0)

    # horse_b (J02): 1 prior ride at venue=06/mile/turf — no win, avg_finish=2.0
    assert by_key[("jra", "horse_b")][4] == 1
    assert by_key[("jra", "horse_b")][2] == pytest.approx(0.0)
    assert by_key[("jra", "horse_b")][3] == pytest.approx(2.0)

    # horse_c: no history → all NULL
    assert by_key[("jra", "horse_c")][2] is None
    assert by_key[("jra", "horse_c")][3] is None
    assert by_key[("jra", "horse_c")][4] is None

    # NAR row passes through with NULL (not in jockey_triple_agg)
    assert by_key[("nar", "horse_n")][2] is None
    assert by_key[("nar", "horse_n")][3] is None
    assert by_key[("nar", "horse_n")][4] is None
