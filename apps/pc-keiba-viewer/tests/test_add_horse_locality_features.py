"""Tests for add-horse-locality-features.py.

NOT-DRY by design: each test is fully self-contained with fixed literals.
Covers:
  - locally-anchored horse (high pct_career_at_keibajo)
  - travelling horse (low pct_career_at_keibajo)
  - debut horse (zero career races → n_career_races_total=0, pct NULL)
  - leak-free guarantee (target race excluded from its own aggregate)
  - rs_features_null_flag correctness (all-NULL vs partially-non-NULL)
  - n_distinct_keibajo across multiple venues
  - stage_race_history SQL contains expected tokens
  - stage_locality_aggregates emits expected columns
  - append_features_sql contains expected join tokens
  - write_partitioned creates output parquet
  - main() end-to-end with stubbed PG
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-horse-locality-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_horse_locality_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_horse_locality_features"] = subject
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


def test_parse_args_custom_from_date(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir", str(tmp_path / "in"),
            "--output-dir", str(tmp_path / "out"),
            "--from-date", "20150101",
        ]
    )
    assert args.from_date == "20150101"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_race_partition_constant() -> None:
    assert subject.RACE_PARTITION == "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"


def test_rs_null_trigger_cols_constant() -> None:
    assert subject.RS_NULL_TRIGGER_COLS == (
        "past_nige_rate_self",
        "past_senkou_rate_self",
        "past_sashi_rate_self",
        "past_oikomi_rate_self",
    )


# ---------------------------------------------------------------------------
# stage_race_history SQL tokens
# ---------------------------------------------------------------------------


def test_stage_race_history_queries_race_entry_corner_features() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_race_history(FakeConn(), "20150101")
    body = " ".join(captured)
    assert "pg.race_entry_corner_features" in body
    assert "race_date >= '20150101'" in body
    assert "ketto_toroku_bango is not null" in body


def test_stage_race_history_creates_index() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_race_history(FakeConn(), "20150101")
    index_stmts = [s for s in captured if "create index" in s.lower()]
    assert len(index_stmts) == 1
    assert "ketto_toroku_bango" in index_stmts[0]


# ---------------------------------------------------------------------------
# stage_base_input SQL tokens
# ---------------------------------------------------------------------------


def test_stage_base_input_reads_parquet_and_sets_rs_null_flag() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_base_input(FakeConn(), "/tmp/x/race_year=*/*.parquet")
    body = " ".join(captured)
    assert "read_parquet('/tmp/x/race_year=*/*.parquet'" in body
    assert "rs_features_null_flag" in body
    assert "past_nige_rate_self is null" in body
    assert "past_senkou_rate_self is null" in body
    assert "past_sashi_rate_self is null" in body
    assert "past_oikomi_rate_self is null" in body


# ---------------------------------------------------------------------------
# append_features_sql SQL tokens
# ---------------------------------------------------------------------------


def test_append_features_sql_contains_locality_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "pct_career_at_keibajo" in sql
    assert "n_career_races_at_keibajo" in sql
    assert "n_career_races_total" in sql
    assert "n_distinct_keibajo" in sql
    assert "rs_features_null_flag" in sql


def test_append_features_sql_left_joins_locality_aggregates() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join locality_aggregates" in sql
    assert "la.ketto_toroku_bango = b.ketto_toroku_bango" in sql


def test_append_features_sql_preserves_base_select_star() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "b.*" in sql


def test_append_features_sql_uses_input_glob(tmp_path: Path) -> None:
    glob = f"{tmp_path}/race_year=*/*.parquet"
    sql = subject.append_features_sql(glob)
    assert glob in sql


def test_append_features_sql_coalesces_counts_to_zero() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "coalesce(la.n_career_races_total, 0)" in sql
    assert "coalesce(la.n_career_races_at_keibajo, 0)" in sql
    assert "coalesce(la.n_distinct_keibajo, 0)" in sql


# ---------------------------------------------------------------------------
# install_and_attach_pg SQL tokens
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
# End-to-end DuckDB tests (no PG attach)
# ---------------------------------------------------------------------------


def _seed_base_input(
    con: duckdb.DuckDBPyConnection,
    *,
    horse_id: str,
    keibajo_code: str,
    race_date: str,
    nige_rate: float | None = 0.4,
    senkou_rate: float | None = 0.3,
    sashi_rate: float | None = 0.2,
    oikomi_rate: float | None = 0.1,
) -> None:
    """Seed base_input with a single horse × race row."""
    nige_lit = "NULL" if nige_rate is None else str(nige_rate)
    senkou_lit = "NULL" if senkou_rate is None else str(senkou_rate)
    sashi_lit = "NULL" if sashi_rate is None else str(sashi_rate)
    oikomi_lit = "NULL" if oikomi_rate is None else str(oikomi_rate)
    rs_flag = 1 if (nige_rate is None and senkou_rate is None and sashi_rate is None and oikomi_rate is None) else 0
    con.execute(
        f"""
        create or replace temp table base_input as
        select * from (
          values (
            'nar', '2025', '0601', '{keibajo_code}', '11',
            '{race_date}',
            '{horse_id}',
            {nige_lit}::double,
            {senkou_lit}::double,
            {sashi_lit}::double,
            {oikomi_lit}::double,
            {rs_flag}::integer
          )
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          race_date, ketto_toroku_bango,
          past_nige_rate_self, past_senkou_rate_self,
          past_sashi_rate_self, past_oikomi_rate_self,
          rs_features_null_flag
        )
        """
    )
    con.execute(
        "create index base_input_locality_idx on base_input (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def _seed_race_history(
    con: duckdb.DuckDBPyConnection,
    rows: list[tuple[str, str, str]],
) -> None:
    """Seed race_history with (horse_id, race_date, keibajo_code) rows."""
    if not rows:
        con.execute(
            """
            create or replace temp table race_history (
              source varchar, race_date varchar, keibajo_code varchar,
              ketto_toroku_bango varchar
            )
            """
        )
        return
    values = ", ".join(
        f"('nar', '{rd}', '{kc}', '{hid}')"
        for hid, rd, kc in rows
    )
    con.execute(
        f"""
        create or replace temp table race_history as
        select * from (
          values {values}
        ) as v(source, race_date, keibajo_code, ketto_toroku_bango)
        """
    )
    con.execute(
        "create index race_history_locality_idx on race_history (ketto_toroku_bango, race_date)"
    )


def test_locally_anchored_horse_gets_high_pct_career_at_keibajo() -> None:
    """A horse with 9 of 10 prior races at venue 43 → pct_career_at_keibajo ≈ 0.9."""
    con = duckdb.connect(":memory:")
    # 9 prior races at venue 43 + 1 at venue 46 = 10 total.
    _seed_race_history(
        con,
        [
            ("horse_local", "20240101", "43"),
            ("horse_local", "20240201", "43"),
            ("horse_local", "20240301", "43"),
            ("horse_local", "20240401", "43"),
            ("horse_local", "20240501", "43"),
            ("horse_local", "20240601", "43"),
            ("horse_local", "20240701", "43"),
            ("horse_local", "20240801", "43"),
            ("horse_local", "20240901", "43"),
            ("horse_local", "20241001", "46"),  # one away race
        ],
    )
    _seed_base_input(
        con, horse_id="horse_local", keibajo_code="43", race_date="20250601"
    )
    subject.stage_locality_aggregates(con)
    row = con.execute(
        """
        select n_career_races_total, n_career_races_at_keibajo,
               cast(round(pct_career_at_keibajo, 2) as double) as pct,
               n_distinct_keibajo
        from locality_aggregates
        where ketto_toroku_bango = 'horse_local'
        """
    ).fetchone()
    con.close()
    assert row == (10, 9, 0.9, 2)


def test_travelling_horse_gets_low_pct_career_at_keibajo() -> None:
    """A horse that only visited venue 43 once in 8 prior races → pct ≈ 0.125."""
    con = duckdb.connect(":memory:")
    # 1 race at venue 43, 7 at other venues.
    _seed_race_history(
        con,
        [
            ("horse_travel", "20240101", "43"),
            ("horse_travel", "20240201", "46"),
            ("horse_travel", "20240301", "46"),
            ("horse_travel", "20240401", "47"),
            ("horse_travel", "20240501", "47"),
            ("horse_travel", "20240601", "50"),
            ("horse_travel", "20240701", "54"),
            ("horse_travel", "20240801", "55"),
        ],
    )
    _seed_base_input(
        con, horse_id="horse_travel", keibajo_code="43", race_date="20250601"
    )
    subject.stage_locality_aggregates(con)
    row = con.execute(
        """
        select n_career_races_total, n_career_races_at_keibajo,
               cast(round(pct_career_at_keibajo, 4) as double) as pct,
               n_distinct_keibajo
        from locality_aggregates
        where ketto_toroku_bango = 'horse_travel'
        """
    ).fetchone()
    con.close()
    assert row == (8, 1, 0.125, 6)


def test_debut_horse_gets_zero_counts_and_null_pct() -> None:
    """A horse with no prior races → n_career_races_total=0, pct_career_at_keibajo=NULL."""
    con = duckdb.connect(":memory:")
    _seed_race_history(con, [])  # no history
    _seed_base_input(
        con, horse_id="horse_debut", keibajo_code="43", race_date="20250601"
    )
    subject.stage_locality_aggregates(con)
    row = con.execute(
        """
        select n_career_races_total, n_career_races_at_keibajo,
               pct_career_at_keibajo, n_distinct_keibajo
        from locality_aggregates
        where ketto_toroku_bango = 'horse_debut'
        """
    ).fetchone()
    con.close()
    assert row == (0, 0, None, 0)


def test_leak_free_excludes_target_race_itself() -> None:
    """Race rows on the SAME date as the current race must be excluded (strict <)."""
    con = duckdb.connect(":memory:")
    # Target race is on 20250601 at venue 43.
    # History has two prior races (before target) + one on the same date.
    _seed_race_history(
        con,
        [
            ("horse_x", "20250401", "43"),  # prior: included
            ("horse_x", "20250501", "43"),  # prior: included
            ("horse_x", "20250601", "43"),  # SAME date as target: must be excluded
        ],
    )
    _seed_base_input(
        con, horse_id="horse_x", keibajo_code="43", race_date="20250601"
    )
    subject.stage_locality_aggregates(con)
    row = con.execute(
        """
        select n_career_races_total, n_career_races_at_keibajo,
               cast(round(pct_career_at_keibajo, 2) as double) as pct
        from locality_aggregates
        where ketto_toroku_bango = 'horse_x'
        """
    ).fetchone()
    con.close()
    # Only 2 prior races (the same-date row is excluded).
    assert row == (2, 2, 1.0)


def test_leak_free_excludes_future_races() -> None:
    """Future race rows (race_date > current race_date) are excluded by the strict <."""
    con = duckdb.connect(":memory:")
    _seed_race_history(
        con,
        [
            ("horse_y", "20250401", "43"),  # prior: included
            ("horse_y", "20250701", "43"),  # FUTURE: must be excluded
            ("horse_y", "20250801", "43"),  # FUTURE: must be excluded
        ],
    )
    _seed_base_input(
        con, horse_id="horse_y", keibajo_code="43", race_date="20250601"
    )
    subject.stage_locality_aggregates(con)
    row = con.execute(
        """
        select n_career_races_total, n_career_races_at_keibajo
        from locality_aggregates
        where ketto_toroku_bango = 'horse_y'
        """
    ).fetchone()
    con.close()
    # Only 1 prior race; the 2 future rows are excluded.
    assert row == (1, 1)


def test_rs_features_null_flag_one_when_all_four_rs_cols_null() -> None:
    """rs_features_null_flag must be 1 when all 4 RS rate columns are NULL."""
    con = duckdb.connect(":memory:")
    _seed_race_history(con, [])
    _seed_base_input(
        con,
        horse_id="horse_null_rs",
        keibajo_code="43",
        race_date="20250601",
        nige_rate=None,
        senkou_rate=None,
        sashi_rate=None,
        oikomi_rate=None,
    )
    row = con.execute(
        "select rs_features_null_flag from base_input where ketto_toroku_bango = 'horse_null_rs'"
    ).fetchone()
    con.close()
    assert row == (1,)


def test_rs_features_null_flag_zero_when_any_rs_col_non_null() -> None:
    """rs_features_null_flag must be 0 when at least one RS rate column is non-NULL."""
    con = duckdb.connect(":memory:")
    _seed_race_history(con, [])
    _seed_base_input(
        con,
        horse_id="horse_partial_rs",
        keibajo_code="43",
        race_date="20250601",
        nige_rate=0.5,       # non-NULL
        senkou_rate=None,
        sashi_rate=None,
        oikomi_rate=None,
    )
    row = con.execute(
        "select rs_features_null_flag from base_input where ketto_toroku_bango = 'horse_partial_rs'"
    ).fetchone()
    con.close()
    assert row == (0,)


def test_rs_features_null_flag_zero_when_all_rs_cols_non_null() -> None:
    """rs_features_null_flag must be 0 when all 4 RS rate columns are non-NULL."""
    con = duckdb.connect(":memory:")
    _seed_race_history(con, [])
    _seed_base_input(
        con,
        horse_id="horse_full_rs",
        keibajo_code="44",
        race_date="20250601",
        nige_rate=0.4,
        senkou_rate=0.3,
        sashi_rate=0.2,
        oikomi_rate=0.1,
    )
    row = con.execute(
        "select rs_features_null_flag from base_input where ketto_toroku_bango = 'horse_full_rs'"
    ).fetchone()
    con.close()
    assert row == (0,)


def test_n_distinct_keibajo_counts_prior_venues_only() -> None:
    """n_distinct_keibajo reflects how many DISTINCT venues appeared before target race."""
    con = duckdb.connect(":memory:")
    # Horse raced at 43, 44, 46, 47 — all before the target date.
    _seed_race_history(
        con,
        [
            ("horse_broad", "20240101", "43"),
            ("horse_broad", "20240201", "44"),
            ("horse_broad", "20240301", "46"),
            ("horse_broad", "20240401", "47"),
            ("horse_broad", "20240501", "43"),  # duplicate venue 43
        ],
    )
    _seed_base_input(
        con, horse_id="horse_broad", keibajo_code="44", race_date="20250601"
    )
    subject.stage_locality_aggregates(con)
    row = con.execute(
        """
        select n_distinct_keibajo, n_career_races_total, n_career_races_at_keibajo
        from locality_aggregates
        where ketto_toroku_bango = 'horse_broad'
        """
    ).fetchone()
    con.close()
    # 5 prior races at 4 distinct venues; target venue=44 was visited once.
    assert row == (4, 5, 1)


def _write_parquet(
    tmp_path: Path,
    *,
    horse_id: str,
    keibajo_code: str,
    race_date: str,
    nige_rate: float | None,
    senkou_rate: float | None,
    sashi_rate: float | None,
    oikomi_rate: float | None,
) -> str:
    """Write a single-row parquet and return the glob path."""
    parquet_dir = tmp_path / "input"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    seed = duckdb.connect(":memory:")
    nige_lit = "NULL" if nige_rate is None else str(nige_rate)
    senkou_lit = "NULL" if senkou_rate is None else str(senkou_rate)
    sashi_lit = "NULL" if sashi_rate is None else str(sashi_rate)
    oikomi_lit = "NULL" if oikomi_rate is None else str(oikomi_rate)
    seed.execute(
        f"""
        create or replace temp table seed as
        select * from (
          values (
            'nar', '{race_date}', '2025', '0601', '{keibajo_code}', '11',
            '{horse_id}', 2025,
            {nige_lit}::double, {senkou_lit}::double,
            {sashi_lit}::double, {oikomi_lit}::double
          )
        ) as v(
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_year,
          past_nige_rate_self, past_senkou_rate_self,
          past_sashi_rate_self, past_oikomi_rate_self
        )
        """
    )
    seed.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed.close()
    return f"{parquet_dir.as_posix()}/race_year=*/*.parquet"


def test_append_features_sql_end_to_end_locally_anchored_horse(tmp_path: Path) -> None:
    """append_features_sql adds correct locality columns for a locally-anchored horse."""
    glob = _write_parquet(
        tmp_path,
        horse_id="horse_local_e2e",
        keibajo_code="43",
        race_date="20250601",
        nige_rate=None,
        senkou_rate=None,
        sashi_rate=None,
        oikomi_rate=None,
    )
    con = duckdb.connect(":memory:")
    # 8 prior races at venue 43 + 2 at other venues.
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            ('nar', '20240101', '43', 'horse_local_e2e'),
            ('nar', '20240201', '43', 'horse_local_e2e'),
            ('nar', '20240301', '43', 'horse_local_e2e'),
            ('nar', '20240401', '43', 'horse_local_e2e'),
            ('nar', '20240501', '43', 'horse_local_e2e'),
            ('nar', '20240601', '43', 'horse_local_e2e'),
            ('nar', '20240701', '43', 'horse_local_e2e'),
            ('nar', '20240801', '43', 'horse_local_e2e'),
            ('nar', '20240901', '46', 'horse_local_e2e'),
            ('nar', '20241001', '47', 'horse_local_e2e')
        ) as v(source, race_date, keibajo_code, ketto_toroku_bango)
        """
    )
    con.execute(
        "create index race_history_locality_idx on race_history (ketto_toroku_bango, race_date)"
    )
    subject.stage_base_input(con, glob)
    subject.stage_locality_aggregates(con)
    sql = subject.append_features_sql(glob)
    row = con.execute(
        f"""
        select ketto_toroku_bango,
               n_career_races_total,
               n_career_races_at_keibajo,
               cast(round(pct_career_at_keibajo, 2) as double) as pct,
               n_distinct_keibajo,
               rs_features_null_flag
        from ({sql})
        """
    ).fetchone()
    con.close()
    # 8 of 10 prior races at venue 43; all RS cols NULL → flag=1.
    assert row == ("horse_local_e2e", 10, 8, 0.8, 3, 1)


def test_append_features_sql_end_to_end_debut_horse(tmp_path: Path) -> None:
    """Debut horse → n_total=0, n_at_venue=0, pct=NULL, n_distinct=0, flag=0."""
    glob = _write_parquet(
        tmp_path,
        horse_id="horse_debut_e2e",
        keibajo_code="43",
        race_date="20250601",
        nige_rate=0.5,
        senkou_rate=0.3,
        sashi_rate=0.1,
        oikomi_rate=0.1,
    )
    con = duckdb.connect(":memory:")
    # No history at all.
    con.execute(
        """
        create or replace temp table race_history (
          source varchar, race_date varchar, keibajo_code varchar,
          ketto_toroku_bango varchar
        )
        """
    )
    subject.stage_base_input(con, glob)
    subject.stage_locality_aggregates(con)
    sql = subject.append_features_sql(glob)
    row = con.execute(
        f"""
        select ketto_toroku_bango,
               n_career_races_total,
               n_career_races_at_keibajo,
               pct_career_at_keibajo,
               n_distinct_keibajo,
               rs_features_null_flag
        from ({sql})
        """
    ).fetchone()
    con.close()
    # Debut horse: counts are 0 (coalesced from NULL), pct is NULL, flag=0.
    assert row == ("horse_debut_e2e", 0, 0, None, 0, 0)


def test_append_features_sql_end_to_end_travelling_horse(tmp_path: Path) -> None:
    """Travelling horse with RS data → pct low, flag=0."""
    glob = _write_parquet(
        tmp_path,
        horse_id="horse_travel_e2e",
        keibajo_code="43",
        race_date="20250601",
        nige_rate=0.3,
        senkou_rate=0.4,
        sashi_rate=0.2,
        oikomi_rate=0.1,
    )
    con = duckdb.connect(":memory:")
    # 1 race at 43, 6 at other venues.
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            ('nar', '20240101', '43', 'horse_travel_e2e'),
            ('nar', '20240201', '46', 'horse_travel_e2e'),
            ('nar', '20240301', '47', 'horse_travel_e2e'),
            ('nar', '20240401', '50', 'horse_travel_e2e'),
            ('nar', '20240501', '54', 'horse_travel_e2e'),
            ('nar', '20240601', '55', 'horse_travel_e2e'),
            ('nar', '20240701', '35', 'horse_travel_e2e')
        ) as v(source, race_date, keibajo_code, ketto_toroku_bango)
        """
    )
    subject.stage_base_input(con, glob)
    subject.stage_locality_aggregates(con)
    sql = subject.append_features_sql(glob)
    row = con.execute(
        f"""
        select ketto_toroku_bango,
               n_career_races_total,
               n_career_races_at_keibajo,
               cast(round(pct_career_at_keibajo, 6) as double) as pct,
               n_distinct_keibajo,
               rs_features_null_flag
        from ({sql})
        """
    ).fetchone()
    con.close()
    # 1/7 ≈ 0.142857; 7 distinct venues (43, 46, 47, 50, 54, 55, 35); RS non-NULL → flag=0.
    assert row is not None
    assert row[0] == "horse_travel_e2e"
    assert row[1] == 7
    assert row[2] == 1
    assert abs(row[3] - (1.0 / 7.0)) < 1e-5
    assert row[4] == 7
    assert row[5] == 0


def test_write_partitioned_creates_parquet_files(tmp_path: Path) -> None:
    glob = _write_parquet(
        tmp_path / "src",
        horse_id="horse_write",
        keibajo_code="43",
        race_date="20250601",
        nige_rate=None,
        senkou_rate=None,
        sashi_rate=None,
        oikomi_rate=None,
    )
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table race_history (
          source varchar, race_date varchar, keibajo_code varchar,
          ketto_toroku_bango varchar
        )
        """
    )
    subject.stage_base_input(con, glob)
    subject.stage_locality_aggregates(con)
    sql = subject.append_features_sql(glob)
    out_dir = tmp_path / "output"
    out_dir.mkdir()
    subject.write_partitioned(con, sql, out_dir)
    files = list(out_dir.glob("race_year=2025/*.parquet"))
    verify = duckdb.connect(":memory:")
    cnt = verify.execute(
        f"select count(*) from read_parquet('{out_dir.as_posix()}/race_year=*/*.parquet')"
    ).fetchone()
    verify.close()
    con.close()
    assert len(files) >= 1
    assert cnt == (1,)


def test_write_partitioned_overwrites_existing_output(tmp_path: Path) -> None:
    """write_partitioned must remove and re-create the output dir (no orphan files)."""
    glob = _write_parquet(
        tmp_path / "src",
        horse_id="horse_ow",
        keibajo_code="43",
        race_date="20250601",
        nige_rate=0.5,
        senkou_rate=0.3,
        sashi_rate=0.1,
        oikomi_rate=0.1,
    )
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table race_history (
          source varchar, race_date varchar, keibajo_code varchar,
          ketto_toroku_bango varchar
        )
        """
    )
    subject.stage_base_input(con, glob)
    subject.stage_locality_aggregates(con)
    sql = subject.append_features_sql(glob)
    out_dir = tmp_path / "output"
    out_dir.mkdir()
    # Pre-create a stale file that should be wiped.
    (out_dir / "stale.parquet").write_text("stale")
    subject.write_partitioned(con, sql, out_dir)
    stale_gone = not (out_dir / "stale.parquet").exists()
    con.close()
    assert stale_gone


def test_main_end_to_end_with_stubbed_pg(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """main() runs end-to-end with stubbed install_and_attach_pg and stage_race_history."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values (
            'nar', '20250601', '2025', '0601', '43', '11',
            'horse_main', 2025,
            NULL::double, NULL::double, NULL::double, NULL::double
          )
        ) as v(
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_year,
          past_nige_rate_self, past_senkou_rate_self,
          past_sashi_rate_self, past_oikomi_rate_self
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()

    def _fake_install_and_attach(con: duckdb.DuckDBPyConnection, _pg_url: str) -> None:
        pass

    def _fake_stage_race_history(
        con: duckdb.DuckDBPyConnection, _from_date: str
    ) -> None:
        # 5 prior races at venue 43 for horse_main.
        con.execute(
            """
            create or replace temp table race_history as
            select * from (
              values
                ('nar', '20240101', '43', 'horse_main'),
                ('nar', '20240201', '43', 'horse_main'),
                ('nar', '20240301', '43', 'horse_main'),
                ('nar', '20240401', '43', 'horse_main'),
                ('nar', '20240501', '43', 'horse_main')
            ) as v(source, race_date, keibajo_code, ketto_toroku_bango)
            """
        )
        con.execute(
            "create index race_history_locality_idx on race_history (ketto_toroku_bango, race_date)"
        )

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(subject, "stage_race_history", _fake_stage_race_history)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_horse_locality_features",
            "--input-dir", str(input_dir),
            "--output-dir", str(output_dir),
        ],
    )
    subject.main()

    verify = duckdb.connect(":memory:")
    row = verify.execute(
        f"""
        select ketto_toroku_bango,
               n_career_races_total,
               n_career_races_at_keibajo,
               cast(round(pct_career_at_keibajo, 2) as double) as pct,
               n_distinct_keibajo,
               rs_features_null_flag
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        """
    ).fetchone()
    verify.close()
    # 5 prior races all at venue 43 → pct=1.0, distinct=1, all-NULL RS → flag=1.
    assert row == ("horse_main", 5, 5, 1.0, 1, 1)
