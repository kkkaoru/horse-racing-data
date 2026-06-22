from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-class-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_class_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_class_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_and_output(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"
    assert args.category == "jra"


def test_parse_args_accepts_nar_category(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "nar",
        ]
    )
    assert args.category == "nar"


def test_parse_args_rejects_invalid_category(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--input-dir",
                str(tmp_path / "in"),
                "--output-dir",
                str(tmp_path / "out"),
                "--category",
                "ban-ei",
            ]
        )


def test_parse_args_pg_url_defaults_to_local_url(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert "127.0.0.1" in args.pg_url
    assert args.from_date == "20100101"


def test_jra_class_levels_constant_matches_ts_builder() -> None:
    assert subject.JRA_CLASS_LEVELS == {
        "000": 0,
        "005": 1,
        "010": 2,
        "016": 3,
        "701": 4,
        "703": 5,
        "999": 6,
    }


def test_hiraba_kyoso_joken_codes_constant_matches_ts_builder() -> None:
    assert subject.HIRABA_KYOSO_JOKEN_CODES == ("000", "005", "010", "016")


def test_hiraba_in_clause_sql_uses_canonical_codes() -> None:
    sql = subject.hiraba_in_clause_sql("kyoso_joken_code")
    assert sql == "kyoso_joken_code in ('000', '005', '010', '016')"


def test_hiraba_in_clause_sql_supports_qualified_column() -> None:
    sql = subject.hiraba_in_clause_sql("rh.kyoso_joken_code")
    assert sql == "rh.kyoso_joken_code in ('000', '005', '010', '016')"


def test_class_variance_window_constants() -> None:
    assert subject.CLASS_VARIANCE_WINDOW == 5
    assert subject.CLASS_VARIANCE_MIN_RACES == 2


def test_promotion_level_buffer_constant() -> None:
    assert subject.PROMOTION_LEVEL_BUFFER == 1


def test_race_partition_constant() -> None:
    assert subject.RACE_PARTITION == "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"


def test_class_level_case_sql_includes_all_codes() -> None:
    sql = subject.class_level_case_sql("rec.kyoso_joken_code")
    assert "when '000' then 0" in sql
    assert "when '005' then 1" in sql
    assert "when '010' then 2" in sql
    assert "when '016' then 3" in sql
    assert "when '701' then 4" in sql
    assert "when '703' then 5" in sql
    assert "when '999' then 6" in sql
    assert "else null" in sql


def test_class_level_case_sql_uses_provided_expression() -> None:
    sql = subject.class_level_case_sql("foo.bar_code")
    assert sql.startswith("case foo.bar_code ")


def test_source_filter_sql_jra() -> None:
    assert subject.source_filter_sql("jra") == "rec.source = 'jra'"


def test_source_filter_sql_nar_excludes_banei() -> None:
    sql = subject.source_filter_sql("nar")
    assert "rec.source = 'nar'" in sql
    assert "rec.keibajo_code <> '83'" in sql


def test_se_table_for_jra() -> None:
    assert subject.se_table_for("jra") == "pg.jvd_se"


def test_se_table_for_nar() -> None:
    assert subject.se_table_for("nar") == "pg.nvd_se"


def test_append_features_sql_contains_three_iter18_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "class_promotion_velocity" in sql
    assert "trainer_hiraba_win_rate" in sql
    assert "horse_recent_class_variance" in sql


def test_append_features_sql_left_joins_three_staging_tables() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join class_promotion" in sql
    assert "left join trainer_hiraba" in sql
    assert "left join horse_class_variance" in sql


def test_append_features_sql_preserves_base_select_star() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "b.*" in sql


def test_append_features_sql_uses_input_glob(tmp_path: Path) -> None:
    glob = f"{tmp_path}/race_year=*/*.parquet"
    sql = subject.append_features_sql(glob)
    assert glob in sql


def _seed_history_and_base(
    con: duckdb.DuckDBPyConnection, *, target_class_level: int | None
) -> None:
    """Build base_input + race_history temp tables directly (no PG attach).

    base_input holds a single target race for horse_a on 2025-04-15. race_history
    holds three earlier rows for the same horse (different class levels + wins).
    """
    level_literal = "NULL" if target_class_level is None else str(target_class_level)
    con.execute(
        f"""
        create or replace temp table base_input as
        select * from (
          values
            (
              'jra', '2025', '0415', '05', '11',
              'horse_a', '20250415', 2025, {level_literal}::integer
            )
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year, target_class_level
        )
        """
    )
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            -- past win at class level 3 (eligible promotion if target >= 4)
            ('jra', '20250215', '2025', '0215', '05', '11',
              'horse_a', 1, '', '016', 3, 'trainer_x'),
            -- past win at class level 2 (eligible if target == 3 (>=2))
            ('jra', '20241215', '2024', '1215', '05', '11',
              'horse_a', 1, '', '010', 2, 'trainer_x'),
            -- past run at class level 1 (not a win)
            ('jra', '20240801', '2024', '0801', '05', '11',
              'horse_a', 5, '', '005', 1, 'trainer_x'),
            -- another non-win at class level 4 (kyoso_joken_code '701' -> not hiraba)
            ('jra', '20240601', '2024', '0601', '05', '11',
              'horse_a', 8, '', '701', 4, 'trainer_x')
        ) as v(
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, finish_position, grade_code, kyoso_joken_code,
          class_level, chokyoshi_code
        )
        """
    )


def test_stage_class_promotion_picks_most_recent_eligible_win(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _seed_history_and_base(con, target_class_level=4)
    subject.stage_class_promotion(con)
    row = con.execute(
        """
        select ketto_toroku_bango,
               cast(class_promotion_velocity as integer) as velocity_days
        from class_promotion
        where ketto_toroku_bango = 'horse_a'
        """
    ).fetchone()
    con.close()
    # target = 2025-04-15, most recent eligible win = 2025-02-15 => 59 days
    assert row == ("horse_a", 59)


def test_stage_class_promotion_emits_no_row_when_no_eligible_history(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _seed_history_and_base(con, target_class_level=6)
    subject.stage_class_promotion(con)
    rows = con.execute("select count(*) from class_promotion").fetchone()
    con.close()
    # target level 6 with buffer 1 needs class >= 5; no past wins satisfy that
    assert rows == (0,)


def test_stage_class_promotion_skips_when_target_class_level_null(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _seed_history_and_base(con, target_class_level=None)
    subject.stage_class_promotion(con)
    rows = con.execute("select count(*) from class_promotion").fetchone()
    con.close()
    assert rows == (0,)


def test_stage_class_promotion_skips_when_target_class_level_zero(tmp_path: Path) -> None:
    """target_class_level=0 produces class_level >= -1 (always true) without the guard.

    The fix adds `and bi.target_class_level > 0` so class-0 target races are excluded,
    preventing every historical win from matching regardless of class level.
    """
    con = duckdb.connect(":memory:")
    _seed_history_and_base(con, target_class_level=0)
    subject.stage_class_promotion(con)
    rows = con.execute("select count(*) from class_promotion").fetchone()
    con.close()
    assert rows == (0,)


def test_stage_horse_class_variance_computes_stddev_over_window(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _seed_history_and_base(con, target_class_level=4)
    subject.stage_horse_class_variance(con)
    row = con.execute(
        """
        select ketto_toroku_bango,
               cast(round(horse_recent_class_variance, 6) as double) as variance
        from horse_class_variance
        where ketto_toroku_bango = 'horse_a'
        """
    ).fetchone()
    con.close()
    # 4 past races with class levels [3, 2, 1, 4] (sorted by recency desc) — within window of 5.
    # population stddev_pop([3,2,1,4]) = sqrt(((3-2.5)^2+(2-2.5)^2+(1-2.5)^2+(4-2.5)^2)/4)
    # = sqrt((0.25+0.25+2.25+2.25)/4) = sqrt(5/4) = sqrt(1.25) ≈ 1.118034
    assert row is not None
    assert row[0] == "horse_a"
    assert row[1] == pytest.approx(1.118034, rel=1e-4)


def test_stage_horse_class_variance_null_when_below_min_races(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    # 1 history row only
    con.execute(
        """
        create or replace temp table base_input as
        select * from (
          values ('jra', '2025', '0415', '05', '11', 'horse_b',
                  '20250415', 2025, 4::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year, target_class_level
        )
        """
    )
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            ('jra', '20250215', '2025', '0215', '05', '11',
              'horse_b', 1, '', '016', 3, 'trainer_z')
        ) as v(
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, finish_position, grade_code, kyoso_joken_code,
          class_level, chokyoshi_code
        )
        """
    )
    subject.stage_horse_class_variance(con)
    row = con.execute(
        """
        select horse_recent_class_variance
        from horse_class_variance
        where ketto_toroku_bango = 'horse_b'
        """
    ).fetchone()
    con.close()
    # only one history row with non-null class_level -> below min -> NULL
    assert row == (None,)


def test_class_level_case_sql_evaluates_correctly_in_duckdb() -> None:
    con = duckdb.connect(":memory:")
    case_sql = subject.class_level_case_sql("code")
    row = con.execute(
        f"""
        with v(code) as (values ('010'), ('999'), ('xyz'))
        select code, {case_sql} as lvl from v order by code
        """
    ).fetchall()
    con.close()
    assert row == [("010", 2), ("999", 6), ("xyz", None)]


def _seed_for_append(con: duckdb.DuckDBPyConnection, parquet_dir: Path) -> str:
    """Write a 5-row, 5-column synthetic parquet + the 3 staging temps."""
    parquet_dir.mkdir(parents=True, exist_ok=True)
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a', '20250415', 2025),
            ('jra', '2025', '0415', '05', '11', 'horse_b', '20250415', 2025),
            ('jra', '2025', '0415', '05', '11', 'horse_c', '20250415', 2025),
            ('jra', '2025', '0415', '05', '11', 'horse_d', '20250415', 2025),
            ('jra', '2025', '0415', '05', '11', 'horse_e', '20250415', 2025)
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

    # Seed staging tables on the main connection with values for some horses, NULL for others
    con.execute(
        """
        create or replace temp table class_promotion as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a', 59),
            ('jra', '2025', '0415', '05', '11', 'horse_c', 120)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, class_promotion_velocity
        )
        """
    )
    con.execute(
        """
        create or replace temp table trainer_hiraba as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a', 0.25::double),
            ('jra', '2025', '0415', '05', '11', 'horse_b', 0.10::double),
            ('jra', '2025', '0415', '05', '11', 'horse_d', 0.40::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, trainer_hiraba_win_rate
        )
        """
    )
    con.execute(
        """
        create or replace temp table horse_class_variance as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a', 1.12::double),
            ('jra', '2025', '0415', '05', '11', 'horse_e', 0.50::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, horse_recent_class_variance
        )
        """
    )
    return f"{parquet_dir.as_posix()}/race_year=*/*.parquet"


def test_append_features_sql_join_preserves_input_columns_and_adds_three(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    glob = _seed_for_append(con, tmp_path / "input")
    sql = subject.append_features_sql(glob)
    cols = con.execute(f"describe {sql}").fetchall()
    con.close()
    col_names = [c[0] for c in cols]
    # 8 base columns + 3 new = 11 total
    assert "source" in col_names
    assert "kaisai_nen" in col_names
    assert "ketto_toroku_bango" in col_names
    assert "race_year" in col_names
    assert "class_promotion_velocity" in col_names
    assert "trainer_hiraba_win_rate" in col_names
    assert "horse_recent_class_variance" in col_names
    assert len(col_names) == 11


def test_append_features_sql_join_handles_nulls_for_missing_horses(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    glob = _seed_for_append(con, tmp_path / "input")
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
               class_promotion_velocity,
               cast(round(trainer_hiraba_win_rate, 6) as double) as thr,
               cast(round(horse_recent_class_variance, 6) as double) as hcv
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # horse_a: all three set
    assert rows[0] == ("horse_a", 59, 0.25, 1.12)
    # horse_b: only trainer_hiraba_win_rate set
    assert rows[1] == ("horse_b", None, 0.1, None)
    # horse_c: only class_promotion_velocity set
    assert rows[2] == ("horse_c", 120, None, None)
    # horse_d: only trainer_hiraba_win_rate set
    assert rows[3] == ("horse_d", None, 0.4, None)
    # horse_e: only horse_recent_class_variance set
    assert rows[4] == ("horse_e", None, None, 0.5)


def test_write_partitioned_writes_parquet_files(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    glob = _seed_for_append(con, tmp_path / "input")
    sql = subject.append_features_sql(glob)
    out_dir = tmp_path / "output"
    out_dir.mkdir()  # exercise the existing-dir branch (will be wiped)
    subject.write_partitioned(con, sql, out_dir)
    files = list(out_dir.glob("race_year=2025/*.parquet"))
    rows = con.execute(
        f"select count(*) from read_parquet('{out_dir.as_posix()}/race_year=*/*.parquet')"
    ).fetchone()
    con.close()
    assert len(files) >= 1
    assert rows == (5,)


def test_install_and_attach_pg_executes_three_statements() -> None:
    """install / load / attach must each be called exactly once."""

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


def test_stage_race_history_uses_jra_se_table_for_jra() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_race_history(FakeConn(), "20240101", "jra")
    body = " ".join(captured)
    assert "pg.jvd_se" in body
    assert "pg.nvd_se" not in body
    assert "rec.source = 'jra'" in body
    assert "race_date >= '20240101'" in body


def test_stage_race_history_exposes_kyoso_joken_code_column() -> None:
    """stage_trainer_hiraba relies on race_history.kyoso_joken_code, so the
    column must be projected by the staging query (sourced from
    race_entry_corner_features).
    """
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_race_history(FakeConn(), "20240101", "jra")
    body = " ".join(captured)
    assert "rec.kyoso_joken_code" in body


def test_stage_race_history_uses_nvd_se_for_nar() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_race_history(FakeConn(), "20240101", "nar")
    body = " ".join(captured)
    assert "pg.nvd_se" in body
    assert "pg.jvd_se" not in body
    assert "rec.source = 'nar'" in body


def test_stage_base_input_calls_read_parquet_with_glob() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_base_input(FakeConn(), "/tmp/x/race_year=*/*.parquet")
    body = " ".join(captured)
    assert "read_parquet('/tmp/x/race_year=*/*.parquet'" in body
    assert "target_class_level" in body


def test_stage_base_input_left_joins_race_entry_corner_features_for_kyoso_joken_code() -> None:
    """iter14 parquet does not carry kyoso_joken_code; stage_base_input must
    LEFT JOIN against pg.race_entry_corner_features (alias rec) on the
    standard race-entry composite key and project kyoso_joken_code from it.
    target_class_level then maps rec.kyoso_joken_code via JRA_CLASS_LEVELS.
    """
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_base_input(FakeConn(), "/tmp/x/race_year=*/*.parquet")
    body = " ".join(captured)
    assert "left join pg.race_entry_corner_features rec" in body
    assert "rec.source = b.source" in body
    assert "rec.kaisai_nen = b.kaisai_nen" in body
    assert "rec.kaisai_tsukihi = b.kaisai_tsukihi" in body
    assert "rec.keibajo_code = b.keibajo_code" in body
    assert "rec.race_bango = b.race_bango" in body
    assert "rec.umaban = b.umaban" in body
    assert "rec.kyoso_joken_code" in body
    # target_class_level CASE must map rec.kyoso_joken_code, not b.kyoso_joken_code.
    assert "case rec.kyoso_joken_code" in body
    assert "case b.kyoso_joken_code" not in body


def test_stage_base_input_end_to_end_projects_kyoso_joken_code_from_pg_join(
    tmp_path: Path,
) -> None:
    """Drive stage_base_input against a real DuckDB connection with a synthetic
    in-memory pg.race_entry_corner_features and a synthetic parquet. Verify the
    resulting base_input projects kyoso_joken_code (and target_class_level)
    from the JOIN.
    """
    parquet_dir = tmp_path / "input"
    parquet_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 1::integer, 'horse_a',
              '20250415', 2025),
            ('jra', '2025', '0415', '05', '11', 2::integer, 'horse_b',
              '20250415', 2025)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          ketto_toroku_bango, race_date, race_year
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()

    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 1::integer, '010'),
            ('jra', '2025', '0415', '05', '11', 2::integer, '999')
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          kyoso_joken_code
        )
        """
    )
    glob = f"{parquet_dir.as_posix()}/race_year=*/*.parquet"
    subject.stage_base_input(con, glob)
    rows = con.execute(
        """
        select ketto_toroku_bango, kyoso_joken_code, target_class_level
        from base_input
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # horse_a row kyoso_joken_code='010' -> class level 2.
    # horse_b row kyoso_joken_code='999' -> class level 6.
    assert rows == [("horse_a", "010", 2), ("horse_b", "999", 6)]


def test_stage_base_input_end_to_end_emits_null_target_class_level_when_pg_missing(
    tmp_path: Path,
) -> None:
    """When the LEFT JOIN does not find a matching pg row, kyoso_joken_code and
    therefore target_class_level must be NULL — this is the documented
    "row without history" path that downstream stages tolerate.
    """
    parquet_dir = tmp_path / "input"
    parquet_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 7::integer, 'horse_z',
              '20250415', 2025)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          ketto_toroku_bango, race_date, race_year
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{parquet_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()

    con = duckdb.connect(":memory:")
    con.execute("create schema pg")
    con.execute(
        """
        create table pg.race_entry_corner_features (
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, umaban integer,
          kyoso_joken_code varchar
        )
        """
    )
    glob = f"{parquet_dir.as_posix()}/race_year=*/*.parquet"
    subject.stage_base_input(con, glob)
    rows = con.execute(
        """
        select ketto_toroku_bango, kyoso_joken_code, target_class_level
        from base_input
        """
    ).fetchall()
    con.close()
    assert rows == [("horse_z", None, None)]


def test_stage_trainer_hiraba_jra_filters_to_canonical_kyoso_joken_codes() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_trainer_hiraba(FakeConn(), "jra")
    body = " ".join(captured)
    # The hiraba_history CTE filters to the canonical hiraba kyoso_joken_code
    # set — mirrors build-trainer-hiraba-sql.ts. The legacy grade_code-based
    # filter must no longer be present.
    assert "kyoso_joken_code in ('000', '005', '010', '016')" in body
    assert "grade_code is null" not in body
    assert "trim(grade_code) = ''" not in body
    assert "pg.jvd_se" in body


def test_stage_trainer_hiraba_nar_uses_nvd_se() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_trainer_hiraba(FakeConn(), "nar")
    body = " ".join(captured)
    assert "pg.nvd_se" in body
    assert "pg.jvd_se" not in body


def test_stage_trainer_hiraba_end_to_end_computes_rate(tmp_path: Path) -> None:
    """Build base_input + race_history + base_with_trainer manually, then
    inline the same hiraba_history + agg CTE that ``stage_trainer_hiraba``
    emits so we exercise the kyoso_joken_code-based filter end-to-end without
    requiring a PG attach.
    """
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            -- trainer_x: 4 hiraba past races (kyoso_joken_code in canonical set),
            -- 2 wins among them.
            ('jra', '20250215', '2025', '0215', '05', '11',
              'horse_a', 1, '', '016', 3, 'trainer_x'),
            ('jra', '20241215', '2024', '1215', '05', '11',
              'horse_a', 5, '', '010', 2, 'trainer_x'),
            ('jra', '20240801', '2024', '0801', '05', '11',
              'horse_a', 1, '', '005', 1, 'trainer_x'),
            ('jra', '20240601', '2024', '0601', '05', '11',
              'horse_a', 7, '', '000', 0, 'trainer_x'),
            -- One non-hiraba race (kyoso_joken_code='701' / open) — must be excluded
            -- even though it was a win, otherwise the rate would become 3/5.
            ('jra', '20240501', '2024', '0501', '05', '11',
              'horse_a', 1, '', '701', 4, 'trainer_x')
        ) as v(
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, finish_position, grade_code, kyoso_joken_code,
          class_level, chokyoshi_code
        )
        """
    )
    con.execute(
        """
        create or replace temp table base_with_trainer as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a', '20250415', 'trainer_x'),
            ('jra', '2025', '0415', '05', '11', 'horse_b', '20250415', NULL),
            ('jra', '2025', '0415', '05', '11', 'horse_c', '20250415', '   ')
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, chokyoshi_code
        )
        """
    )
    hiraba_filter = subject.hiraba_in_clause_sql("kyoso_joken_code")
    con.execute(
        f"""
        create or replace temp table trainer_hiraba as
        with hiraba_history as (
          select source, chokyoshi_code, race_date, finish_position
          from race_history
          where chokyoshi_code is not null
            and trim(chokyoshi_code) <> ''
            and {hiraba_filter}
        ),
        agg as (
          select
            bwt.source,
            bwt.kaisai_nen,
            bwt.kaisai_tsukihi,
            bwt.keibajo_code,
            bwt.race_bango,
            bwt.ketto_toroku_bango,
            count(hh.race_date) as past_starts,
            sum(case when hh.finish_position = 1 then 1 else 0 end) as past_wins
          from base_with_trainer bwt
          left join hiraba_history hh
            on hh.source = bwt.source
            and hh.chokyoshi_code = bwt.chokyoshi_code
            and hh.race_date < bwt.race_date
          where bwt.chokyoshi_code is not null
            and trim(bwt.chokyoshi_code) <> ''
          group by bwt.source, bwt.kaisai_nen, bwt.kaisai_tsukihi,
                   bwt.keibajo_code, bwt.race_bango, bwt.ketto_toroku_bango
        )
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango,
               case when past_starts > 0
                    then past_wins::double / past_starts
                    else null end as trainer_hiraba_win_rate
        from agg
        """
    )
    rows = con.execute(
        """
        select ketto_toroku_bango,
               cast(round(trainer_hiraba_win_rate, 6) as double) as rate
        from trainer_hiraba
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # trainer_x: 4 hiraba past races (kyoso_joken_code in canonical set),
    # 2 wins => 0.5. The kyoso_joken_code='701' race is excluded.
    assert rows == [("horse_a", 0.5)]


def test_stage_trainer_hiraba_excludes_non_hiraba_kyoso_joken_codes(tmp_path: Path) -> None:
    """A trainer whose past races are all at non-hiraba kyoso_joken_codes
    (e.g., '030' is hypothetical or any code outside '000'/'005'/'010'/'016')
    contributes 0 hiraba past starts → trainer_hiraba_win_rate is NULL.
    """
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            -- All past races outside the canonical hiraba set (701/703/999).
            ('jra', '20250215', '2025', '0215', '05', '11',
              'horse_a', 1, '', '701', 4, 'trainer_g'),
            ('jra', '20241215', '2024', '1215', '05', '11',
              'horse_a', 1, '', '703', 5, 'trainer_g'),
            ('jra', '20240801', '2024', '0801', '05', '11',
              'horse_a', 3, '', '999', 6, 'trainer_g')
        ) as v(
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, finish_position, grade_code, kyoso_joken_code,
          class_level, chokyoshi_code
        )
        """
    )
    con.execute(
        """
        create or replace temp table base_with_trainer as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a', '20250415', 'trainer_g')
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, chokyoshi_code
        )
        """
    )
    hiraba_filter = subject.hiraba_in_clause_sql("kyoso_joken_code")
    con.execute(
        f"""
        create or replace temp table trainer_hiraba as
        with hiraba_history as (
          select source, chokyoshi_code, race_date, finish_position
          from race_history
          where chokyoshi_code is not null
            and trim(chokyoshi_code) <> ''
            and {hiraba_filter}
        ),
        agg as (
          select
            bwt.source, bwt.kaisai_nen, bwt.kaisai_tsukihi, bwt.keibajo_code,
            bwt.race_bango, bwt.ketto_toroku_bango,
            count(hh.race_date) as past_starts,
            sum(case when hh.finish_position = 1 then 1 else 0 end) as past_wins
          from base_with_trainer bwt
          left join hiraba_history hh
            on hh.source = bwt.source
            and hh.chokyoshi_code = bwt.chokyoshi_code
            and hh.race_date < bwt.race_date
          where bwt.chokyoshi_code is not null
            and trim(bwt.chokyoshi_code) <> ''
          group by bwt.source, bwt.kaisai_nen, bwt.kaisai_tsukihi,
                   bwt.keibajo_code, bwt.race_bango, bwt.ketto_toroku_bango
        )
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango,
               case when past_starts > 0
                    then past_wins::double / past_starts
                    else null end as trainer_hiraba_win_rate
        from agg
        """
    )
    rows = con.execute(
        """
        select ketto_toroku_bango, trainer_hiraba_win_rate
        from trainer_hiraba
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # All past races outside canonical hiraba set -> 0 past_starts -> NULL rate.
    assert rows == [("horse_a", None)]


def test_stage_trainer_hiraba_ignores_graded_grade_code_when_kyoso_joken_in_set(
    tmp_path: Path,
) -> None:
    """Under the new semantics the filter is purely on kyoso_joken_code: even
    when grade_code is non-empty (e.g., 'A' / 'B' / 'L'), a past race whose
    kyoso_joken_code IS in the canonical set still counts as hiraba. This
    mirrors how the TS PG UPDATE builder picks up the same row set.
    """
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            -- Race with kyoso_joken_code='005' (hiraba set) and grade_code='A'.
            -- Under the new semantics this row IS counted (old grade_code-based
            -- filter would have skipped it).
            ('jra', '20250215', '2025', '0215', '05', '11',
              'horse_a', 1, 'A', '005', 1, 'trainer_h'),
            -- Plus another straightforward hiraba past race for context.
            ('jra', '20241215', '2024', '1215', '05', '11',
              'horse_a', 4, '', '010', 2, 'trainer_h')
        ) as v(
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, finish_position, grade_code, kyoso_joken_code,
          class_level, chokyoshi_code
        )
        """
    )
    con.execute(
        """
        create or replace temp table base_with_trainer as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a', '20250415', 'trainer_h')
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, chokyoshi_code
        )
        """
    )
    hiraba_filter = subject.hiraba_in_clause_sql("kyoso_joken_code")
    con.execute(
        f"""
        create or replace temp table trainer_hiraba as
        with hiraba_history as (
          select source, chokyoshi_code, race_date, finish_position
          from race_history
          where chokyoshi_code is not null
            and trim(chokyoshi_code) <> ''
            and {hiraba_filter}
        ),
        agg as (
          select
            bwt.source, bwt.kaisai_nen, bwt.kaisai_tsukihi, bwt.keibajo_code,
            bwt.race_bango, bwt.ketto_toroku_bango,
            count(hh.race_date) as past_starts,
            sum(case when hh.finish_position = 1 then 1 else 0 end) as past_wins
          from base_with_trainer bwt
          left join hiraba_history hh
            on hh.source = bwt.source
            and hh.chokyoshi_code = bwt.chokyoshi_code
            and hh.race_date < bwt.race_date
          where bwt.chokyoshi_code is not null
            and trim(bwt.chokyoshi_code) <> ''
          group by bwt.source, bwt.kaisai_nen, bwt.kaisai_tsukihi,
                   bwt.keibajo_code, bwt.race_bango, bwt.ketto_toroku_bango
        )
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango,
               case when past_starts > 0
                    then past_wins::double / past_starts
                    else null end as trainer_hiraba_win_rate
        from agg
        """
    )
    rows = con.execute(
        """
        select ketto_toroku_bango,
               cast(round(trainer_hiraba_win_rate, 6) as double) as rate
        from trainer_hiraba
        """
    ).fetchall()
    con.close()
    # 2 hiraba past races (both in canonical set, grade_code irrelevant),
    # 1 win => 0.5.
    assert rows == [("horse_a", 0.5)]


def test_main_runs_end_to_end_with_stubbed_pg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """End-to-end main(): we stub install_and_attach_pg + race_history seeding so
    no real PG is required, and verify the output parquet has the expected
    columns + row count.
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    # NOTE: the seed parquet must include ``umaban`` so that stage_base_input's
    # LEFT JOIN against pg.race_entry_corner_features can resolve. The iter14
    # base parquet itself includes umaban — we mirror that here.
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 1::integer, 'horse_a', '20250415', 2025),
            ('jra', '2025', '0415', '05', '11', 2::integer, 'horse_b', '20250415', 2025)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          ketto_toroku_bango, race_date, race_year
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()

    def _fake_install_and_attach(con: duckdb.DuckDBPyConnection, _pg_url: str) -> None:
        # The real script `attach`-es the PG instance as schema `pg`. We bypass
        # PG entirely by creating a local `pg` schema with a stand-in
        # race_entry_corner_features table so stage_base_input's JOIN resolves.
        con.execute("create schema pg")
        con.execute(
            """
            create table pg.race_entry_corner_features as
            select * from (
              values
                ('jra', '2025', '0415', '05', '11', 1::integer, 'horse_a', '010'),
                ('jra', '2025', '0415', '05', '11', 2::integer, 'horse_b', '999')
            ) as v(
              source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
              ketto_toroku_bango, kyoso_joken_code
            )
            """
        )
        con.execute(
            """
            create or replace temp table race_history as
            select * from (
              values
                ('jra', '20250215', '2025', '0215', '05', '11', 'horse_a', 1, '', '016', 3, 'trainer_x'),
                ('jra', '20241215', '2024', '1215', '05', '11', 'horse_a', 1, '', '010', 2, 'trainer_x'),
                ('jra', '20240601', '2024', '0601', '05', '11', 'horse_a', 5, '', '005', 1, 'trainer_x')
            ) as v(
              source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
              ketto_toroku_bango, finish_position, grade_code, kyoso_joken_code,
              class_level, chokyoshi_code
            )
            """
        )

    def _fake_stage_race_history(*_args: object, **_kwargs: object) -> None:
        return None

    def _fake_stage_trainer_hiraba(con: duckdb.DuckDBPyConnection, _category: str) -> None:
        con.execute(
            """
            create or replace temp table trainer_hiraba as
            select * from (
              values
                ('jra', '2025', '0415', '05', '11', 'horse_a', 0.50::double)
            ) as v(
              source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
              ketto_toroku_bango, trainer_hiraba_win_rate
            )
            """
        )

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(subject, "stage_race_history", _fake_stage_race_history)
    monkeypatch.setattr(subject, "stage_trainer_hiraba", _fake_stage_trainer_hiraba)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_class_features",
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--category",
            "jra",
        ],
    )
    subject.main()

    verify_con = duckdb.connect(":memory:")
    rows = verify_con.execute(
        f"""
        select ketto_toroku_bango,
               class_promotion_velocity,
               cast(round(trainer_hiraba_win_rate, 6) as double) as thr,
               horse_recent_class_variance
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        order by ketto_toroku_bango
        """
    ).fetchall()
    verify_con.close()
    # horse_a: target=2025-04-15 with kyoso_joken_code 010 -> target_class_level=2
    #   eligible past wins (class >= 1): 2025-02-15 (class 3) and 2024-12-15 (class 2)
    #   most recent: 2025-02-15 => 59 days
    # variance over [3,2,1]: stddev_pop = sqrt(((3-2)^2+(2-2)^2+(1-2)^2)/3) = sqrt(2/3) ≈ 0.8165
    assert rows[0][0] == "horse_a"
    assert rows[0][1] == 59
    assert rows[0][2] == 0.5
    assert rows[0][3] == pytest.approx(0.816497, rel=1e-4)
    # horse_b has no matching history -> all three NULL
    assert rows[1] == ("horse_b", None, None, None)
