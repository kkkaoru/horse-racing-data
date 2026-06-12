from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add_kohan3f_going_features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_kohan3f_going_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_kohan3f_going_features"] = subject
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


def test_parse_args_pg_url_override(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--pg-url",
            "postgresql://stub:stub@db.example:15432/horse_racing",
        ]
    )
    assert args.pg_url == "postgresql://stub:stub@db.example:15432/horse_racing"


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


def test_recent_going_window_size_constant() -> None:
    assert subject.RECENT_GOING_WINDOW_SIZE == 5


def test_firm_and_soft_going_codes() -> None:
    assert subject.FIRM_GOING_CODES == (1, 2)
    assert subject.SOFT_GOING_CODES == (3, 4)


def test_track_code_ranges() -> None:
    assert subject.TURF_TRACK_CODE_RANGE == (10, 29)
    assert subject.DIRT_TRACK_CODE_RANGE == (51, 69)


def test_race_partition_constant() -> None:
    assert subject.RACE_PARTITION == "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"


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


# ── going_code_case_sql ────────────────────────────────────────────────────────


def test_going_code_case_sql_mentions_both_surface_columns() -> None:
    sql = subject.going_code_case_sql()
    assert "babajotai_code_shiba" in sql
    assert "babajotai_code_dirt" in sql
    assert "between 10 and 29" in sql
    assert "between 51 and 69" in sql


def test_going_code_case_sql_selects_surface_specific_column_in_duckdb() -> None:
    con = duckdb.connect(":memory:")
    sql = subject.going_code_case_sql()
    rows = con.execute(
        f"""
        with ra(track_code, babajotai_code_shiba, babajotai_code_dirt) as (
          values
            ('17', '1', '3'),   -- turf: reads shiba=1
            ('52', '1', '3'),   -- dirt: reads dirt=3
            ('40', '1', '3'),   -- neither range: NULL
            ('17', '', '3'),    -- turf with empty shiba: NULL
            ('52', '1', '0')    -- dirt with code 0: parses to 0
        )
        select {sql} as going_code from ra
        """
    ).fetchall()
    con.close()
    assert [row[0] for row in rows] == [1, 3, None, None, 0]


# ── install_and_attach_pg ──────────────────────────────────────────────────────


def test_install_and_attach_pg_executes_three_statements() -> None:
    conn = FakeConn()
    subject.install_and_attach_pg(conn, "postgresql://stub/horse_racing")
    assert conn.statements[0] == "install postgres"
    assert conn.statements[1] == "load postgres"
    assert conn.statements[2].startswith("attach 'postgresql://stub/horse_racing'")
    assert "read_only" in conn.statements[2]


# ── stage_kohan3f_history ──────────────────────────────────────────────────────


def test_stage_kohan3f_history_sql_joins_jvd_ra_on_race_tuple() -> None:
    conn = FakeConn()
    subject.stage_kohan3f_history(conn, 2005)
    body = " ".join(conn.statements)
    assert "from pg.jvd_se se" in body
    assert "join pg.jvd_ra ra" in body
    assert "ra.kaisai_nen = se.kaisai_nen" in body
    assert "ra.kaisai_tsukihi = se.kaisai_tsukihi" in body
    assert "ra.keibajo_code = se.keibajo_code" in body
    assert "ra.race_bango = se.race_bango" in body


def test_stage_kohan3f_history_sql_filters_jra_year_and_valid_kohan() -> None:
    conn = FakeConn()
    subject.stage_kohan3f_history(conn, 2010)
    body = " ".join(conn.statements)
    assert "regexp_matches(se.keibajo_code, '^0[1-9]$|^10$')" in body
    assert "cast(se.kaisai_nen as integer) >= 2010" in body
    assert "se.ketto_toroku_bango is not null" in body
    assert "as double) > 0" in body


def test_stage_kohan3f_history_sql_builds_hist_race_date_concat() -> None:
    conn = FakeConn()
    subject.stage_kohan3f_history(conn, 2005)
    body = " ".join(conn.statements)
    assert "se.kaisai_nen || se.kaisai_tsukihi as hist_race_date" in body


# ── stage_base_races ───────────────────────────────────────────────────────────


def test_stage_base_races_reads_parquet_glob_and_filters_jra() -> None:
    conn = FakeConn()
    subject.stage_base_races(conn, "/tmp/x/race_year=*/*.parquet")
    body = " ".join(conn.statements)
    assert "read_parquet('/tmp/x/race_year=*/*.parquet'" in body
    assert "where source = 'jra'" in body
    assert "ketto_toroku_bango" in body
    assert "race_date" in body


# ── stage_going_conditional_agg ────────────────────────────────────────────────


def test_stage_going_conditional_agg_sql_uses_strict_date_and_valid_codes() -> None:
    conn = FakeConn()
    subject.stage_going_conditional_agg(conn)
    body = " ".join(conn.statements)
    assert "h.hist_race_date < b.race_date" in body
    assert "h.going_code in (1, 2, 3, 4)" in body
    assert "case when h.going_code in (1, 2) then 1 else 0 end as is_firm" in body


def test_stage_going_conditional_agg_sql_windows_and_filters_last5() -> None:
    conn = FakeConn()
    subject.stage_going_conditional_agg(conn)
    body = " ".join(conn.statements)
    assert "row_number() over" in body
    assert "order by h.hist_race_date desc" in body
    assert "prior_rank <= 5 and is_firm = 1" in body
    assert "prior_rank <= 5 and is_firm = 0" in body
    assert "kohan3f_firm_avg5" in body
    assert "kohan3f_soft_avg5" in body


# ── append_features_sql ────────────────────────────────────────────────────────


def test_append_features_sql_contains_three_columns_and_diff_expression() -> None:
    sql = subject.append_features_sql("/tmp/x/race_year=*/*.parquet")
    assert "g.kohan3f_firm_avg5" in sql
    assert "g.kohan3f_soft_avg5" in sql
    assert "g.kohan3f_firm_avg5 - g.kohan3f_soft_avg5 as kohan3f_going_diff" in sql


def test_append_features_sql_left_joins_on_race_tuple_and_horse() -> None:
    sql = subject.append_features_sql("/tmp/x/race_year=*/*.parquet")
    assert "left join going_cond_agg g" in sql
    assert "g.source = b.source" in sql
    assert "g.kaisai_nen = b.kaisai_nen" in sql
    assert "g.kaisai_tsukihi = b.kaisai_tsukihi" in sql
    assert "g.keibajo_code = b.keibajo_code" in sql
    assert "g.race_bango = b.race_bango" in sql
    assert "g.ketto_toroku_bango = b.ketto_toroku_bango" in sql
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
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.jvd_se as
        select * from (
          values
            -- horse_a history: 5 going-coded + 1 older (window overflow)
            ('2025', '0301', '05', '11', 'horse_a', '0345'),
            ('2025', '0201', '05', '11', 'horse_a', '0350'),
            ('2025', '0101', '05', '11', 'horse_a', '0360'),
            ('2024', '1201', '05', '11', 'horse_a', '0370'),
            ('2024', '1101', '05', '11', 'horse_a', '0340'),
            ('2024', '1001', '05', '11', 'horse_a', '9999'),
            -- horse_b: one valid firm + same-date-as-target + going-0 + zero kohan
            ('2025', '0301', '06', '01', 'horse_b', '0333'),
            ('2025', '0415', '05', '11', 'horse_b', '0311'),
            ('2025', '0310', '06', '01', 'horse_b', '0322'),
            ('2025', '0305', '06', '01', 'horse_b', '0000'),
            -- NAR keibajo row must be excluded by the JRA filter
            ('2025', '0301', '30', '01', 'horse_b', '0301')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, kohan_3f)
        """
    )
    con.execute(
        """
        create table pg.jvd_ra as
        select * from (
          values
            -- horse_a races: turf firm(1), turf firm(2), turf soft(3), turf soft(4),
            -- dirt firm(1) via babajotai_code_dirt, turf firm(1) (overflow row)
            ('2025', '0301', '05', '11', '17', '1', ''),
            ('2025', '0201', '05', '11', '17', '2', ''),
            ('2025', '0101', '05', '11', '17', '3', ''),
            ('2024', '1201', '05', '11', '17', '4', ''),
            ('2024', '1101', '05', '11', '52', '', '1'),
            ('2024', '1001', '05', '11', '17', '1', ''),
            -- horse_b races: valid firm turf; target-day race; going code 0; zero-kohan day
            ('2025', '0301', '06', '01', '17', '1', ''),
            ('2025', '0415', '05', '11', '17', '1', ''),
            ('2025', '0310', '06', '01', '17', '0', ''),
            ('2025', '0305', '06', '01', '17', '1', ''),
            ('2025', '0301', '30', '01', '17', '1', '')
        ) as v(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               track_code, babajotai_code_shiba, babajotai_code_dirt)
        """
    )


def _seed_base_parquet(input_dir: Path) -> None:
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a', '20250415', 2025),
            ('jra', '2025', '0415', '05', '11', 'horse_b', '20250415', 2025),
            ('jra', '2025', '0415', '05', '11', 'horse_c', '20250415', 2025),
            ('nar', '2025', '0415', '30', '01', 'horse_n', '20250415', 2025)
        ) as v(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year)
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()


def test_staging_pipeline_computes_going_conditional_averages(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _seed_base_parquet(input_dir)

    con = duckdb.connect(":memory:")
    _seed_pg_schema(con)
    subject.stage_kohan3f_history(con, 2005)
    subject.stage_base_races(con, f"{input_dir.as_posix()}/race_year=*/*.parquet")
    subject.stage_going_conditional_agg(con)
    rows = con.execute(
        """
        select ketto_toroku_bango, kohan3f_firm_avg5, kohan3f_soft_avg5
        from going_cond_agg order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()

    by_horse = {row[0]: row for row in rows}
    # horse_a last 5 going-coded priors: 0345(F),0350(F),0360(S),0370(S),0340(F dirt).
    # The 6th (9999) falls outside the window.
    assert by_horse["horse_a"][1] == pytest.approx((345.0 + 350.0 + 340.0) / 3.0)
    assert by_horse["horse_a"][2] == pytest.approx((360.0 + 370.0) / 2.0)
    # horse_b: one valid firm prior (0333). Same-date row (strict <), going-0 row,
    # and zero kohan row are all excluded. Soft side has no history -> NULL.
    assert by_horse["horse_b"][1] == pytest.approx(333.0)
    assert by_horse["horse_b"][2] is None
    # horse_c never raced -> no agg row at all (LEFT JOIN later emits NULLs).
    assert "horse_c" not in by_horse


def test_main_end_to_end_appends_three_columns(
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
            "add_kohan3f_going_features",
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
        select source, ketto_toroku_bango,
               kohan3f_firm_avg5, kohan3f_soft_avg5, kohan3f_going_diff
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        order by source, ketto_toroku_bango
        """
    ).fetchall()
    verify_con.close()

    by_horse = {(row[0], row[1]): row for row in rows}
    assert len(by_horse) == 4
    firm_a = (345.0 + 350.0 + 340.0) / 3.0
    soft_a = (360.0 + 370.0) / 2.0
    assert by_horse[("jra", "horse_a")][2] == pytest.approx(firm_a)
    assert by_horse[("jra", "horse_a")][3] == pytest.approx(soft_a)
    assert by_horse[("jra", "horse_a")][4] == pytest.approx(firm_a - soft_a)
    # horse_b: soft NULL -> diff NULL (no imputation).
    assert by_horse[("jra", "horse_b")][2] == pytest.approx(333.0)
    assert by_horse[("jra", "horse_b")][3] is None
    assert by_horse[("jra", "horse_b")][4] is None
    # horse_c: no history -> all NULL.
    assert by_horse[("jra", "horse_c")][2] is None
    assert by_horse[("jra", "horse_c")][3] is None
    assert by_horse[("jra", "horse_c")][4] is None
    # NAR row passes through with NULLs (base_races filters to jra for the agg).
    assert by_horse[("nar", "horse_n")][2] is None
    assert by_horse[("nar", "horse_n")][3] is None
    assert by_horse[("nar", "horse_n")][4] is None
