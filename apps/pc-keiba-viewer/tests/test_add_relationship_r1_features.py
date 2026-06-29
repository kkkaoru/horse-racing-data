from __future__ import annotations

import importlib.util
import math
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "src" / "scripts" / "finish-position-features"
MODULE_PATH = SCRIPTS_DIR / "add-relationship-r1-features.py"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location("add_relationship_r1_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_relationship_r1_features"] = subject
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


def test_parse_args_accepts_banei_category(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "ban-ei",
        ]
    )
    assert args.category == "ban-ei"


def test_parse_args_accepts_all_category(tmp_path: Path) -> None:
    args = subject.parse_args(
        [
            "--input-dir",
            str(tmp_path / "in"),
            "--output-dir",
            str(tmp_path / "out"),
            "--category",
            "all",
        ]
    )
    assert args.category == "all"


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


def test_parse_args_rejects_invalid_category(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        subject.parse_args(
            [
                "--input-dir",
                str(tmp_path / "in"),
                "--output-dir",
                str(tmp_path / "out"),
                "--category",
                "trotting",
            ]
        )


def test_parse_args_pg_url_defaults_to_local_url(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")]
    )
    assert "127.0.0.1" in args.pg_url
    assert args.from_date == "20100101"


def test_recent_history_window_size_constant() -> None:
    assert subject.RECENT_HISTORY_WINDOW_SIZE == 5


def test_history_lookback_days_constant() -> None:
    assert subject.HISTORY_LOOKBACK_DAYS == 100000


def test_race_partition_constant() -> None:
    assert subject.RACE_PARTITION == "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"


def test_source_filter_sql_jra() -> None:
    assert subject.source_filter_sql("jra") == "rec.source = 'jra'"


def test_source_filter_sql_nar_excludes_banei() -> None:
    sql = subject.source_filter_sql("nar")
    assert sql == "rec.source = 'nar' and rec.keibajo_code <> '83'"


def test_source_filter_sql_banei_targets_keibajo_83() -> None:
    sql = subject.source_filter_sql("ban-ei")
    assert sql == "rec.source = 'nar' and rec.keibajo_code = '83'"


def test_source_filter_sql_all_returns_true() -> None:
    assert subject.source_filter_sql("all") == "true"


def test_se_table_for_jra() -> None:
    assert subject.se_table_for("jra") == "pg.jvd_se"


def test_se_table_for_nar() -> None:
    assert subject.se_table_for("nar") == "pg.nvd_se"


def test_se_table_for_banei() -> None:
    assert subject.se_table_for("ban-ei") == "pg.nvd_se"


def test_safe_bataiju_cast_sql_uses_provided_alias() -> None:
    sql = subject.safe_bataiju_cast_sql("se")
    assert "se.bataiju" in sql
    assert "~ '^-?[0-9]+$'" in sql
    assert "else null end" in sql


def test_safe_bataiju_cast_sql_returns_null_for_non_numeric_in_duckdb() -> None:
    con = duckdb.connect(":memory:")
    sql = subject.safe_bataiju_cast_sql("v")
    rows = con.execute(
        f"""
        with v(bataiju) as (values ('480'), (' 472 '), ('---'), (NULL))
        select bataiju, {sql} as parsed from v order by bataiju nulls last
        """
    ).fetchall()
    con.close()
    # ' 472 ' -> 472, '480' -> 480, '---' -> NULL, NULL -> NULL
    parsed_map = {row[0]: row[1] for row in rows}
    assert parsed_map[" 472 "] == 472
    assert parsed_map["480"] == 480
    assert parsed_map["---"] is None
    assert parsed_map[None] is None


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


def test_stage_base_input_calls_read_parquet_with_glob() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_base_input(FakeConn(), "/tmp/x/race_year=*/*.parquet", "jra")
    body = " ".join(captured)
    assert "read_parquet('/tmp/x/race_year=*/*.parquet'" in body
    assert "kyori" in body
    assert "futan_juryo" in body
    assert "barei" in body
    assert "bataiju" in body


def test_stage_base_input_left_joins_race_entry_corner_features_on_umaban() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_base_input(FakeConn(), "/tmp/x/race_year=*/*.parquet", "jra")
    body = " ".join(captured)
    assert "left join pg.race_entry_corner_features rec" in body
    assert "rec.umaban = b.umaban" in body
    # kyori / futan_juryo / barei use COALESCE with se fallback for upcoming races.
    assert "rec.kyori" in body
    assert "rec.futan_juryo" in body
    assert "rec.barei" in body
    assert "coalesce" in body.lower()
    assert "se.futan_juryo" in body
    assert "se.barei" in body


def test_stage_base_input_left_joins_jvd_se_for_bataiju_when_jra() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_base_input(FakeConn(), "/tmp/x/race_year=*/*.parquet", "jra")
    body = " ".join(captured)
    assert "left join pg.jvd_se se" in body
    assert "pg.nvd_se" not in body
    # bataiju is sourced from se table with safe cast.
    assert "se.bataiju" in body


def test_stage_base_input_left_joins_nvd_se_for_bataiju_when_nar() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_base_input(FakeConn(), "/tmp/x/race_year=*/*.parquet", "nar")
    body = " ".join(captured)
    assert "left join pg.nvd_se se" in body
    assert "pg.jvd_se" not in body


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


def test_race_history_focus_filter_sql_false_is_empty() -> None:
    assert subject.race_history_focus_filter_sql(False) == ""


def test_race_history_focus_filter_sql_true_uses_base_input_horses() -> None:
    sql = subject.race_history_focus_filter_sql(True)
    assert "base_input bi" in sql
    assert "bi.source = rec.source" in sql
    assert "bi.ketto_toroku_bango = rec.ketto_toroku_bango" in sql


def test_stage_race_history_focused_filters_to_base_input_horses() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_race_history(FakeConn(), "20240101", "jra", focused_target=True)
    body = " ".join(captured)
    assert "base_input bi" in body
    assert "bi.ketto_toroku_bango = rec.ketto_toroku_bango" in body


def test_stage_race_history_filters_soha_time_and_kyori_for_leak_guard() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_race_history(FakeConn(), "20240101", "jra")
    body = " ".join(captured)
    assert "rec.finish_position is not null" in body
    assert "rec.kyori is not null" in body
    assert "rec.soha_time is not null" in body


def test_stage_race_relative_sql_contains_all_seven_columns() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_race_relative(FakeConn())
    body = " ".join(captured)
    assert "bataiju_futan_ratio" in body
    assert "futan_per_barei" in body
    assert "bataiju_per_kyori_log" in body
    assert "bataiju_diff_from_race_mean" in body
    assert "bataiju_rank_in_race" in body
    assert "futan_minus_bataiju_zscore_in_race" in body
    assert "barei_diff_from_race_mean" in body


def test_stage_race_relative_sql_partitions_window_on_race_columns() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_race_relative(FakeConn())
    body = " ".join(captured)
    assert (
        "partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
        in body
    )
    # zscore guard against zero-stddev division.
    assert "joint_ratio_stddev = 0" in body


def test_stage_history_normalized_sql_contains_all_five_columns() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_history_normalized(FakeConn())
    body = " ".join(captured)
    assert "past_speed_kg_normalized_avg5" in body
    assert "past_speed_futan_normalized_avg5" in body
    assert "past_speed_age_adjusted_avg5" in body
    assert "past_speed_volatility_5" in body
    assert "past_finish_position_volatility_5" in body


def test_stage_history_normalized_sql_uses_strict_past_race_join_and_window() -> None:
    captured: list[str] = []

    class FakeConn:
        def execute(self, sql: str) -> None:
            captured.append(sql)

    subject.stage_history_normalized(FakeConn())
    body = " ".join(captured)
    # leak guard
    assert "rh.race_date < bi.race_date" in body
    # window size literal
    assert "recent_rank <= 5" in body
    # nullif guard for divide-by-zero on barei
    assert "nullif(hist_barei, 0)" in body


def test_append_features_sql_contains_twelve_iter26_columns() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "bataiju_futan_ratio" in sql
    assert "futan_per_barei" in sql
    assert "bataiju_per_kyori_log" in sql
    assert "bataiju_diff_from_race_mean" in sql
    assert "bataiju_rank_in_race" in sql
    assert "futan_minus_bataiju_zscore_in_race" in sql
    assert "barei_diff_from_race_mean" in sql
    assert "past_speed_kg_normalized_avg5" in sql
    assert "past_speed_futan_normalized_avg5" in sql
    assert "past_speed_age_adjusted_avg5" in sql
    assert "past_speed_volatility_5" in sql
    assert "past_finish_position_volatility_5" in sql


def test_append_features_sql_left_joins_two_staging_tables() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "left join race_relative" in sql
    assert "left join history_normalized" in sql


def test_append_features_sql_preserves_base_select_star() -> None:
    sql = subject.append_features_sql("dummy.parquet")
    assert "b.*" in sql


def test_append_features_sql_uses_input_glob(tmp_path: Path) -> None:
    glob = f"{tmp_path}/race_year=*/*.parquet"
    sql = subject.append_features_sql(glob)
    assert glob in sql


def _seed_base_input_with_three_horses(con: duckdb.DuckDBPyConnection) -> None:
    """Seed base_input with 3 horses sharing one race so within-race windows
    have meaningful spread. horse_a: heavy 480kg / 56kg / barei 5;
    horse_b: 460kg / 54kg / barei 4; horse_c: 470kg / 55kg / barei 6."""
    con.execute(
        """
        create or replace temp table base_input as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a', '20250415', 2025,
              1600.0::double, 56.0::double, 5.0::double, 480.0::double),
            ('jra', '2025', '0415', '05', '11', 'horse_b', '20250415', 2025,
              1600.0::double, 54.0::double, 4.0::double, 460.0::double),
            ('jra', '2025', '0415', '05', '11', 'horse_c', '20250415', 2025,
              1600.0::double, 55.0::double, 6.0::double, 470.0::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year,
          kyori, futan_juryo, barei, bataiju
        )
        """
    )


def test_stage_race_relative_computes_within_row_features(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _seed_base_input_with_three_horses(con)
    subject.stage_race_relative(con)
    rows = con.execute(
        """
        select ketto_toroku_bango,
               bataiju_futan_ratio,
               futan_per_barei,
               bataiju_per_kyori_log
        from race_relative
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # horse_a: 56/480, 56/5, 480/ln(1601)
    assert rows[0][0] == "horse_a"
    assert rows[0][1] == pytest.approx(56.0 / 480.0, rel=1e-6)
    assert rows[0][2] == pytest.approx(56.0 / 5.0, rel=1e-6)
    assert rows[0][3] == pytest.approx(480.0 / math.log(1601.0), rel=1e-6)


def test_stage_race_relative_computes_within_race_features(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _seed_base_input_with_three_horses(con)
    subject.stage_race_relative(con)
    rows = con.execute(
        """
        select ketto_toroku_bango,
               bataiju_diff_from_race_mean,
               bataiju_rank_in_race,
               barei_diff_from_race_mean
        from race_relative
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # mean bataiju = (480+460+470)/3 = 470. mean barei = (5+4+6)/3 = 5.
    # rank desc: horse_a (480) = 1, horse_c (470) = 2, horse_b (460) = 3.
    horse_map = {r[0]: r for r in rows}
    assert horse_map["horse_a"][1] == pytest.approx(10.0, rel=1e-6)
    assert horse_map["horse_a"][2] == 1
    assert horse_map["horse_a"][3] == pytest.approx(0.0, rel=1e-6)
    assert horse_map["horse_b"][1] == pytest.approx(-10.0, rel=1e-6)
    assert horse_map["horse_b"][2] == 3
    assert horse_map["horse_b"][3] == pytest.approx(-1.0, rel=1e-6)
    assert horse_map["horse_c"][1] == pytest.approx(0.0, rel=1e-6)
    assert horse_map["horse_c"][2] == 2
    assert horse_map["horse_c"][3] == pytest.approx(1.0, rel=1e-6)


def test_stage_race_relative_zscore_is_null_when_stddev_zero(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    # 1 horse only -> joint_ratio_stddev = 0 -> zscore must be null.
    con.execute(
        """
        create or replace temp table base_input as
        select * from (
          values ('jra', '2025', '0415', '05', '11', 'horse_solo',
                  '20250415', 2025, 1600.0::double, 56.0::double, 5.0::double, 480.0::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year,
          kyori, futan_juryo, barei, bataiju
        )
        """
    )
    subject.stage_race_relative(con)
    row = con.execute(
        "select futan_minus_bataiju_zscore_in_race from race_relative"
    ).fetchone()
    con.close()
    assert row == (None,)


def test_stage_race_relative_zscore_nonzero_when_stddev_positive(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _seed_base_input_with_three_horses(con)
    subject.stage_race_relative(con)
    rows = con.execute(
        """
        select ketto_toroku_bango, futan_minus_bataiju_zscore_in_race
        from race_relative
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # Three different joint_ratios -> non-zero stddev -> all three zscores defined.
    for _, z in rows:
        assert z is not None


def _seed_history_for_horse_a(con: duckdb.DuckDBPyConnection) -> None:
    """Seed base_input (target row) + race_history (past races) for horse_a."""
    con.execute(
        """
        create or replace temp table base_input as
        select * from (
          values ('jra', '2025', '0415', '05', '11', 'horse_a',
                  '20250415', 2025, 1600.0::double, 56.0::double, 5.0::double, 480.0::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year,
          kyori, futan_juryo, barei, bataiju
        )
        """
    )
    # 3 past races: identical 1600m kyori, soha_time varies, bataiju varies.
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            ('jra', '20250215', '2025', '0215', '05', '11', 'horse_a',
              1.0::double, 1600.0::double, 95.0::double, 56.0::double, 5.0::double, 478.0::double),
            ('jra', '20241215', '2024', '1215', '05', '11', 'horse_a',
              3.0::double, 1600.0::double, 96.0::double, 55.0::double, 4.0::double, 482.0::double),
            ('jra', '20240801', '2024', '0801', '05', '11', 'horse_a',
              5.0::double, 1600.0::double, 97.0::double, 54.0::double, 3.0::double, 480.0::double)
        ) as v(
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, finish_position, kyori, soha_time, futan_juryo, barei, bataiju
        )
        """
    )


def test_stage_history_normalized_computes_speed_kg_avg(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _seed_history_for_horse_a(con)
    subject.stage_history_normalized(con)
    row = con.execute(
        """
        select ketto_toroku_bango, past_speed_kg_normalized_avg5
        from history_normalized
        where ketto_toroku_bango = 'horse_a'
        """
    ).fetchone()
    con.close()
    # avg(soha_time/kyori * bataiju):
    # (95/1600*478 + 96/1600*482 + 97/1600*480) / 3
    expected = (95.0 / 1600.0 * 478.0 + 96.0 / 1600.0 * 482.0 + 97.0 / 1600.0 * 480.0) / 3.0
    assert row is not None
    assert row[0] == "horse_a"
    assert row[1] == pytest.approx(expected, rel=1e-6)


def test_stage_history_normalized_computes_volatility(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _seed_history_for_horse_a(con)
    subject.stage_history_normalized(con)
    row = con.execute(
        """
        select past_speed_volatility_5, past_finish_position_volatility_5
        from history_normalized
        where ketto_toroku_bango = 'horse_a'
        """
    ).fetchone()
    con.close()
    # stddev_pop of soha_time/kyori = stddev_pop([95/1600, 96/1600, 97/1600])
    speed = [95.0 / 1600.0, 96.0 / 1600.0, 97.0 / 1600.0]
    mean_speed = sum(speed) / len(speed)
    expected_speed_stddev = math.sqrt(sum((x - mean_speed) ** 2 for x in speed) / len(speed))
    # stddev_pop of finish_position = stddev_pop([1, 3, 5]) = sqrt(((1-3)^2+(3-3)^2+(5-3)^2)/3)
    expected_fp_stddev = math.sqrt(((1 - 3) ** 2 + (3 - 3) ** 2 + (5 - 3) ** 2) / 3.0)
    assert row is not None
    assert row[0] == pytest.approx(expected_speed_stddev, rel=1e-6)
    assert row[1] == pytest.approx(expected_fp_stddev, rel=1e-6)


def test_stage_history_normalized_speed_age_adjusted_skips_zero_barei(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table base_input as
        select * from (
          values ('jra', '2025', '0415', '05', '11', 'horse_b',
                  '20250415', 2025, 1600.0::double, 56.0::double, 5.0::double, 480.0::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year,
          kyori, futan_juryo, barei, bataiju
        )
        """
    )
    # Two past races: one with barei=0 (must be filtered to NULL via nullif),
    # one with barei=4 -> the avg should be soha_time/kyori/4 (single value).
    con.execute(
        """
        create or replace temp table race_history as
        select * from (
          values
            ('jra', '20250215', '2025', '0215', '05', '11', 'horse_b',
              1.0::double, 1600.0::double, 96.0::double, 56.0::double, 0.0::double, 480.0::double),
            ('jra', '20241215', '2024', '1215', '05', '11', 'horse_b',
              2.0::double, 1600.0::double, 100.0::double, 55.0::double, 4.0::double, 482.0::double)
        ) as v(
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, finish_position, kyori, soha_time, futan_juryo, barei, bataiju
        )
        """
    )
    subject.stage_history_normalized(con)
    row = con.execute(
        """
        select past_speed_age_adjusted_avg5
        from history_normalized
        where ketto_toroku_bango = 'horse_b'
        """
    ).fetchone()
    con.close()
    # barei=0 contributes NULL (nullif); only barei=4 entry: (100/1600)/4
    expected = (100.0 / 1600.0) / 4.0
    assert row is not None
    assert row[0] == pytest.approx(expected, rel=1e-6)


def test_stage_history_normalized_no_past_races_emits_no_row(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        create or replace temp table base_input as
        select * from (
          values ('jra', '2025', '0415', '05', '11', 'horse_new',
                  '20250415', 2025, 1600.0::double, 56.0::double, 4.0::double, 470.0::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date, race_year,
          kyori, futan_juryo, barei, bataiju
        )
        """
    )
    con.execute(
        """
        create or replace temp table race_history(
          source varchar, race_date varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, ketto_toroku_bango varchar,
          finish_position double, kyori double, soha_time double, futan_juryo double,
          barei double, bataiju double
        )
        """
    )
    subject.stage_history_normalized(con)
    rows = con.execute("select count(*) from history_normalized").fetchone()
    con.close()
    assert rows == (0,)


def _seed_for_append(con: duckdb.DuckDBPyConnection, parquet_dir: Path) -> str:
    """Write a 4-row, 8-column synthetic parquet + the 2 staging temps."""
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
            ('jra', '2025', '0415', '05', '11', 'horse_d', '20250415', 2025)
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
    con.execute(
        """
        create or replace temp table race_relative as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a',
              0.117::double, 11.2::double, 65.2::double, 10.0::double,
              1::integer, 1.2::double, 0.5::double),
            ('jra', '2025', '0415', '05', '11', 'horse_c',
              0.121::double, 10.5::double, 64.0::double, -5.0::double,
              2::integer, -0.3::double, -1.0::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          bataiju_futan_ratio, futan_per_barei, bataiju_per_kyori_log,
          bataiju_diff_from_race_mean, bataiju_rank_in_race,
          futan_minus_bataiju_zscore_in_race, barei_diff_from_race_mean
        )
        """
    )
    con.execute(
        """
        create or replace temp table history_normalized as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 'horse_a',
              28.5::double, 3.5::double, 0.012::double, 0.001::double, 1.5::double),
            ('jra', '2025', '0415', '05', '11', 'horse_b',
              29.0::double, 3.6::double, 0.013::double, 0.002::double, 1.2::double)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          past_speed_kg_normalized_avg5, past_speed_futan_normalized_avg5,
          past_speed_age_adjusted_avg5, past_speed_volatility_5,
          past_finish_position_volatility_5
        )
        """
    )
    return f"{parquet_dir.as_posix()}/race_year=*/*.parquet"


def test_append_features_sql_join_preserves_input_and_adds_twelve(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    glob = _seed_for_append(con, tmp_path / "input")
    sql = subject.append_features_sql(glob)
    cols = con.execute(f"describe {sql}").fetchall()
    con.close()
    col_names = [c[0] for c in cols]
    # 8 base columns + 12 new = 20 total
    assert "source" in col_names
    assert "ketto_toroku_bango" in col_names
    assert "race_year" in col_names
    assert "bataiju_futan_ratio" in col_names
    assert "futan_per_barei" in col_names
    assert "bataiju_per_kyori_log" in col_names
    assert "bataiju_diff_from_race_mean" in col_names
    assert "bataiju_rank_in_race" in col_names
    assert "futan_minus_bataiju_zscore_in_race" in col_names
    assert "barei_diff_from_race_mean" in col_names
    assert "past_speed_kg_normalized_avg5" in col_names
    assert "past_speed_futan_normalized_avg5" in col_names
    assert "past_speed_age_adjusted_avg5" in col_names
    assert "past_speed_volatility_5" in col_names
    assert "past_finish_position_volatility_5" in col_names
    assert len(col_names) == 20


def test_append_features_sql_join_handles_nulls_for_missing_horses(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    glob = _seed_for_append(con, tmp_path / "input")
    sql = subject.append_features_sql(glob)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
               bataiju_rank_in_race,
               past_speed_kg_normalized_avg5
        from ({sql})
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # horse_a: both staging tables have rows
    assert rows[0] == ("horse_a", 1, 28.5)
    # horse_b: only history_normalized has a row
    assert rows[1] == ("horse_b", None, 29.0)
    # horse_c: only race_relative has a row
    assert rows[2] == ("horse_c", 2, None)
    # horse_d: neither
    assert rows[3] == ("horse_d", None, None)


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
    assert rows == (4,)


def test_stage_base_input_end_to_end_projects_columns_from_pg_join(tmp_path: Path) -> None:
    """Drive stage_base_input against a real DuckDB connection with synthetic
    in-memory pg.race_entry_corner_features + pg.jvd_se and a synthetic parquet.
    rec is present -> rec wins (COALESCE keeps rec value unchanged for completed races)."""
    parquet_dir = tmp_path / "input"
    parquet_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 1::integer, 'horse_a',
              '20250415', 2025, 1600::integer),
            ('jra', '2025', '0415', '05', '11', 2::integer, 'horse_b',
              '20250415', 2025, 1600::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          ketto_toroku_bango, race_date, race_year, kyori
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
    # rec has futan_juryo=56.0 (kg) and barei=5 (age) as stored in the corner table.
    con.execute(
        """
        create table pg.race_entry_corner_features as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 1::integer, 'horse_a',
              1600::integer, 56.0::double, 5::integer),
            ('jra', '2025', '0415', '05', '11', 2::integer, 'horse_b',
              1600::integer, 54.0::double, 4::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          ketto_toroku_bango, kyori, futan_juryo, barei
        )
        """
    )
    # se has futan_juryo in 0.1kg units (560=56.0kg) and barei as zero-padded string.
    # COALESCE must pick rec first, so output should still be 56.0/54.0 (not 560/540).
    con.execute(
        """
        create table pg.jvd_se as
        select * from (
          values
            ('2025', '0415', '05', '11', 'horse_a', '480'::varchar, '560'::varchar, '05'::varchar),
            ('2025', '0415', '05', '11', 'horse_b', '   460  '::varchar, '540'::varchar, '04'::varchar)
        ) as v(
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, bataiju, futan_juryo, barei
        )
        """
    )
    glob = f"{parquet_dir.as_posix()}/race_year=*/*.parquet"
    subject.stage_base_input(con, glob, "jra")
    rows = con.execute(
        """
        select ketto_toroku_bango, kyori, futan_juryo, barei, bataiju
        from base_input
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # rec is present: rec wins, futan_juryo = rec's kg value (not se's raw 0.1kg units)
    assert rows == [
        ("horse_a", 1600.0, 56.0, 5.0, 480.0),
        ("horse_b", 1600.0, 54.0, 4.0, 460.0),
    ]


def test_stage_base_input_se_fallback_for_upcoming_race_nar(tmp_path: Path) -> None:
    """For upcoming NAR races rec is absent (LEFT JOIN miss). futan_juryo and barei
    must fall back to nvd_se with the correct unit conversion: futan_juryo/10 (0.1kg->kg)
    and barei cast from zero-padded string ('05'->5.0)."""
    parquet_dir = tmp_path / "input"
    parquet_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('nar', '2026', '0611', '30', '01', 1::integer, 'horse_upcoming',
              '20260611', 2026, 1400::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          ketto_toroku_bango, race_date, race_year, kyori
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
    # rec is empty: simulates upcoming race not yet in race_entry_corner_features
    con.execute(
        """
        create table pg.race_entry_corner_features(
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, umaban integer,
          ketto_toroku_bango varchar, kyori integer, futan_juryo varchar, barei integer
        )
        """
    )
    # nvd_se has futan_juryo='520' (0.1kg units = 52.0kg) and barei='03' (3 years old)
    con.execute(
        """
        create table pg.nvd_se as
        select * from (
          values
            ('2026', '0611', '30', '01', 'horse_upcoming',
             '   '::varchar, '520'::varchar, '03'::varchar)
        ) as v(
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, bataiju, futan_juryo, barei
        )
        """
    )
    glob = f"{parquet_dir.as_posix()}/race_year=*/*.parquet"
    subject.stage_base_input(con, glob, "nar")
    rows = con.execute(
        """
        select ketto_toroku_bango, kyori, futan_juryo, barei, bataiju
        from base_input
        """
    ).fetchall()
    con.close()
    # rec absent: se fallback used. futan_juryo = 520/10 = 52.0, barei = '03' -> 3.0
    # kyori falls back to b.kyori (from parquet) = 1400.0
    assert len(rows) == 1
    assert rows[0][0] == "horse_upcoming"
    assert rows[0][1] == pytest.approx(1400.0)
    assert rows[0][2] == pytest.approx(52.0)
    assert rows[0][3] == pytest.approx(3.0)
    # bataiju is blank -> NULL
    assert rows[0][4] is None


def test_stage_base_input_se_fallback_for_upcoming_race_jra(tmp_path: Path) -> None:
    """For upcoming JRA races rec is absent. futan_juryo and barei must fall back
    to jvd_se with the same unit convention as NAR (both use 0.1kg / zero-padded)."""
    parquet_dir = tmp_path / "input"
    parquet_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2026', '0614', '05', '01', 1::integer, 'horse_jra_up',
              '20260614', 2026, 2000::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          ketto_toroku_bango, race_date, race_year, kyori
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
        create table pg.race_entry_corner_features(
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, umaban integer,
          ketto_toroku_bango varchar, kyori integer, futan_juryo varchar, barei integer
        )
        """
    )
    # jvd_se has futan_juryo='565' (0.1kg = 56.5kg) and barei='04' (4 years old)
    con.execute(
        """
        create table pg.jvd_se as
        select * from (
          values
            ('2026', '0614', '05', '01', 'horse_jra_up',
             '464'::varchar, '565'::varchar, '04'::varchar)
        ) as v(
          kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, bataiju, futan_juryo, barei
        )
        """
    )
    glob = f"{parquet_dir.as_posix()}/race_year=*/*.parquet"
    subject.stage_base_input(con, glob, "jra")
    rows = con.execute(
        """
        select ketto_toroku_bango, kyori, futan_juryo, barei, bataiju
        from base_input
        """
    ).fetchall()
    con.close()
    assert len(rows) == 1
    assert rows[0][0] == "horse_jra_up"
    assert rows[0][1] == pytest.approx(2000.0)
    assert rows[0][2] == pytest.approx(56.5)
    assert rows[0][3] == pytest.approx(4.0)
    assert rows[0][4] == pytest.approx(464.0)


def test_stage_base_input_all_null_when_rec_and_se_both_absent(tmp_path: Path) -> None:
    """When both rec and se have no matching row, futan_juryo and barei are NULL."""
    parquet_dir = tmp_path / "input"
    parquet_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('nar', '2026', '0611', '30', '01', 1::integer, 'horse_ghost',
              '20260611', 2026, 1600::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          ketto_toroku_bango, race_date, race_year, kyori
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
        create table pg.race_entry_corner_features(
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, umaban integer,
          ketto_toroku_bango varchar, kyori integer, futan_juryo varchar, barei integer
        )
        """
    )
    con.execute(
        """
        create table pg.nvd_se(
          kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
          race_bango varchar, ketto_toroku_bango varchar,
          bataiju varchar, futan_juryo varchar, barei varchar
        )
        """
    )
    glob = f"{parquet_dir.as_posix()}/race_year=*/*.parquet"
    subject.stage_base_input(con, glob, "nar")
    rows = con.execute(
        """
        select ketto_toroku_bango, futan_juryo, barei, bataiju
        from base_input
        """
    ).fetchall()
    con.close()
    assert len(rows) == 1
    assert rows[0][0] == "horse_ghost"
    assert rows[0][1] is None
    assert rows[0][2] is None
    assert rows[0][3] is None


def test_stage_base_input_end_to_end_emits_null_when_pg_rows_missing(tmp_path: Path) -> None:
    """LEFT JOIN must emit NULL when there is no PG row for the parquet row.

    This is the documented "row without history" path that downstream stages
    tolerate (group A / B compute NULL because of nullif divisors).
    Both rec and se have no matching row -> futan_juryo/barei/bataiju all NULL.
    kyori falls back to b.kyori (parquet) since rec is absent.
    """
    parquet_dir = tmp_path / "input"
    parquet_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 9::integer, 'horse_z',
              '20250415', 2025, 1800::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          ketto_toroku_bango, race_date, race_year, kyori
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
        create table pg.race_entry_corner_features(
          source varchar, kaisai_nen varchar, kaisai_tsukihi varchar,
          keibajo_code varchar, race_bango varchar, umaban integer,
          ketto_toroku_bango varchar, kyori integer, futan_juryo varchar,
          barei integer
        )
        """
    )
    # jvd_se must include futan_juryo and barei columns (accessed by COALESCE fallback)
    con.execute(
        """
        create table pg.jvd_se(
          kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar,
          race_bango varchar, ketto_toroku_bango varchar,
          bataiju varchar, futan_juryo varchar, barei varchar
        )
        """
    )
    glob = f"{parquet_dir.as_posix()}/race_year=*/*.parquet"
    subject.stage_base_input(con, glob, "jra")
    rows = con.execute(
        """
        select ketto_toroku_bango, kyori, futan_juryo, barei, bataiju
        from base_input
        """
    ).fetchall()
    con.close()
    # rec absent: kyori falls back to b.kyori=1800.0; futan/barei/bataiju NULL (se also absent)
    assert len(rows) == 1
    assert rows[0][0] == "horse_z"
    assert rows[0][1] == pytest.approx(1800.0)
    assert rows[0][2] is None
    assert rows[0][3] is None
    assert rows[0][4] is None


def test_main_runs_end_to_end_with_stubbed_pg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """End-to-end main(): we stub install_and_attach_pg so no real PG is
    required, and verify the output parquet has the expected new columns +
    row count. rec is present for both horses -> rec wins COALESCE.
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    # seed parquet must include umaban and kyori (b.kyori is the COALESCE fallback).
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0415', '05', '11', 1::integer, 'horse_a',
              '20250415', 2025, 1600::integer),
            ('jra', '2025', '0415', '05', '11', 2::integer, 'horse_b',
              '20250415', 2025, 1600::integer)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban,
          ketto_toroku_bango, race_date, race_year, kyori
        )
        """
    )
    seed_con.execute(
        f"copy (select * from seed) to '{input_dir.as_posix()}'"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )
    seed_con.close()

    def _fake_install_and_attach(con: duckdb.DuckDBPyConnection, _pg_url: str) -> None:
        con.execute("create schema pg")
        con.execute(
            """
            create table pg.race_entry_corner_features as
            select * from (
              values
                -- Target rows (current race 2025-04-15): futan_juryo in kg (56.0/54.0)
                ('jra', '2025', '0415', '05', '11', 1::integer, 'horse_a',
                  '20250415'::varchar, 1600::integer, 56.0::double, 5::integer,
                  NULL::double, NULL::double),
                ('jra', '2025', '0415', '05', '11', 2::integer, 'horse_b',
                  '20250415'::varchar, 1600::integer, 54.0::double, 4::integer,
                  NULL::double, NULL::double),
                -- Past rows for horse_a (eligible history)
                ('jra', '2025', '0215', '05', '11', 1::integer, 'horse_a',
                  '20250215'::varchar, 1600::integer, 56.0::double, 5::integer,
                  1.0::double, 95.0::double),
                ('jra', '2024', '1215', '05', '11', 1::integer, 'horse_a',
                  '20241215'::varchar, 1600::integer, 55.0::double, 4::integer,
                  3.0::double, 96.0::double)
            ) as v(
              source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
              umaban, ketto_toroku_bango, race_date, kyori, futan_juryo, barei,
              finish_position, soha_time
            )
            """
        )
        # jvd_se must include futan_juryo and barei columns (used by COALESCE fallback).
        # rec is present for all rows so se values won't be used, but columns must exist.
        con.execute(
            """
            create table pg.jvd_se as
            select * from (
              values
                ('2025', '0415', '05', '11', 'horse_a', '480'::varchar, '560'::varchar, '05'::varchar),
                ('2025', '0415', '05', '11', 'horse_b', '460'::varchar, '540'::varchar, '04'::varchar),
                ('2025', '0215', '05', '11', 'horse_a', '478'::varchar, '560'::varchar, '05'::varchar),
                ('2024', '1215', '05', '11', 'horse_a', '482'::varchar, '550'::varchar, '04'::varchar)
            ) as v(
              kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
              ketto_toroku_bango, bataiju, futan_juryo, barei
            )
            """
        )

    monkeypatch.setattr(subject, "install_and_attach_pg", _fake_install_and_attach)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "add_relationship_r1_features",
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
               bataiju_futan_ratio,
               bataiju_rank_in_race,
               past_speed_kg_normalized_avg5
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        order by ketto_toroku_bango
        """
    ).fetchall()
    verify_con.close()
    horse_map = {r[0]: r for r in rows}
    # horse_a: bataiju=480, futan_juryo=56.0 (rec value, kg) -> ratio = 56.0/480.0
    assert horse_map["horse_a"][1] == pytest.approx(56.0 / 480.0, rel=1e-6)
    # horse_a has past races -> past_speed_kg_normalized_avg5 is defined
    assert horse_map["horse_a"][3] is not None
    # horse_b: bataiju=460, futan_juryo=54.0 (rec value, kg) -> ratio = 54.0/460.0
    assert horse_map["horse_b"][1] == pytest.approx(54.0 / 460.0, rel=1e-6)
    # horse_b has no past races -> past_speed_kg_normalized_avg5 is NULL
    assert horse_map["horse_b"][3] is None
