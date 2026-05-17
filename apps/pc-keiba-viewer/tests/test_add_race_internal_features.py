from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "src" / "scripts" / "finish-position-features" / "add-race-internal-features.py"

_spec = importlib.util.spec_from_file_location("add_race_internal_features", MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
subject = importlib.util.module_from_spec(_spec)
sys.modules["add_race_internal_features"] = subject
_spec.loader.exec_module(subject)


def test_parse_args_requires_input_and_output_dirs(tmp_path: Path):
    args = subject.parse_args(["--input-dir", str(tmp_path / "in"), "--output-dir", str(tmp_path / "out")])
    assert args.input_dir == tmp_path / "in"
    assert args.output_dir == tmp_path / "out"


def test_pressure_weights_match_plan():
    assert subject.NIGE_PRESSURE_WEIGHT == 2.0
    assert subject.SENKOU_PRESSURE_WEIGHT == 1.0


def test_nige_candidate_threshold_is_documented():
    assert subject.NIGE_CANDIDATE_THRESHOLD == 0.4


def test_append_features_sql_contains_field_pressure_columns():
    sql = subject.append_features_sql("dummy.parquet")
    assert "field_nige_pressure" in sql
    assert "field_senkou_pressure" in sql
    assert "field_sashi_pressure" in sql
    assert "field_oikomi_pressure" in sql
    assert "field_pace_index" in sql


def test_append_features_sql_emits_intra_race_peer_aggregates():
    sql = subject.append_features_sql("dummy.parquet")
    assert "field_nige_candidate_count" in sql
    assert "self_nige_rate_minus_field_avg" in sql
    assert "umaban_x_nige_history" in sql
    assert "field_avg_speed_index" in sql
    assert "field_top_speed_index" in sql
    assert "field_avg_career_win_rate" in sql


def test_append_features_sql_keeps_existing_speed_diff_column():
    sql = subject.append_features_sql("dummy.parquet")
    assert "speed_index_avg_5_diff_from_race_avg" in sql


def test_append_features_sql_uses_race_partition_window():
    sql = subject.append_features_sql("dummy.parquet")
    assert "race_partition as (partition by" in sql
    assert "b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango" in sql


def test_field_nige_pressure_excludes_self_via_subtraction():
    """field_*_pressure must subtract self from race-wide sum to avoid self-loop."""
    sql = subject.append_features_sql("dummy.parquet")
    assert (
        "sum(coalesce(b.past_nige_rate_self, 0)) over race_partition - coalesce(b.past_nige_rate_self, 0)"
        in sql
    )


def test_field_pace_index_weights_nige_higher_than_senkou():
    sql = subject.append_features_sql("dummy.parquet")
    assert f") * {subject.NIGE_PRESSURE_WEIGHT}" in sql
    assert f") * {subject.SENKOU_PRESSURE_WEIGHT}" in sql


def test_appended_features_via_duckdb_match_expected_values(tmp_path: Path):
    import duckdb

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed as
        select * from (
          values
            ('jra', '2025', '0101', '05', '01', 1, 'horse_a', 0.10, 0.20, 0.40, 0.30, 0.5, 1.0, 0.2, 0.1, 0),
            ('jra', '2025', '0101', '05', '01', 2, 'horse_b', 0.50, 0.20, 0.20, 0.10, 0.3, 1.5, 0.4, 0.2, 1),
            ('jra', '2025', '0101', '05', '01', 3, 'horse_c', 0.00, 0.40, 0.40, 0.20, 0.8, 1.2, 0.3, 0.3, 2)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          umaban, ketto_toroku_bango,
          past_nige_rate_self, past_senkou_rate_self, past_sashi_rate_self, past_oikomi_rate_self,
          umaban_norm, speed_index_avg_5, speed_index_best_5, career_win_rate, race_year
        )
        """
    )
    seed_con.execute(
        "copy (select * from seed) to ?"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)",
        [input_dir.as_posix()],
    )
    seed_con.close()

    input_glob = f"{input_dir.as_posix()}/race_year=*/*.parquet"

    # add stub columns that the post-processor expects but seed doesn't have
    extended_sql = subject.append_features_sql(input_glob).replace(
        "select\n      b.*,",
        "select\n      b.*,\n      cast(null as double) as pedigree_score_for_race_stub,",
    )
    # we cannot inject the expected columns easily; instead, build a richer parquet
    # via a second seed file that includes all columns referenced by append_features_sql.
    richer_dir = tmp_path / "input_full"
    richer_dir.mkdir()
    seed_con = duckdb.connect(":memory:")
    seed_con.execute(
        """
        create or replace temp table seed_full as
        select * from (
          values
            ('jra', '2025', '0101', '05', '01', 1, 'horse_a',
              0.10, 0.20, 0.40, 0.30, 0.5, 1.0, 0.2, 0.1,
              0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0),
            ('jra', '2025', '0101', '05', '01', 2, 'horse_b',
              0.50, 0.20, 0.20, 0.10, 0.3, 1.5, 0.4, 0.2,
              0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 1),
            ('jra', '2025', '0101', '05', '01', 3, 'horse_c',
              0.00, 0.40, 0.40, 0.20, 0.8, 1.2, 0.3, 0.3,
              0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 2)
        ) as v(
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          umaban, ketto_toroku_bango,
          past_nige_rate_self, past_senkou_rate_self, past_sashi_rate_self, past_oikomi_rate_self,
          umaban_norm, speed_index_avg_5, speed_index_best_5, career_win_rate,
          jockey_recent_win_rate, trainer_career_win_rate, pedigree_score_for_race,
          same_distance_win_rate, kohan3f_avg_5, corner_pass_avg_5,
          field_strength_avg_speed, field_strength_top3_speed, finish_position, race_year
        )
        """
    )
    seed_con.execute(
        "copy (select * from seed_full) to ?"
        " (format parquet, partition_by (race_year), overwrite_or_ignore true)",
        [richer_dir.as_posix()],
    )
    seed_con.close()

    con = duckdb.connect(":memory:")
    glob = f"{richer_dir.as_posix()}/race_year=*/*.parquet"
    sql = subject.append_features_sql(glob)
    subject.write_partitioned(con, sql, output_dir)
    rows = con.execute(
        f"""
        select ketto_toroku_bango,
          cast(round(field_nige_pressure, 3) as double) as f_nige_p,
          cast(round(field_pace_index, 3) as double) as pace,
          cast(field_nige_candidate_count as int) as candidates,
          cast(round(self_nige_rate_minus_field_avg, 3) as double) as self_minus,
          cast(round(umaban_x_nige_history, 3) as double) as umaban_nige
        from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet')
        order by ketto_toroku_bango
        """
    ).fetchall()
    con.close()
    # For horse_a (past_nige=0.10): field sum of others = 0.50 + 0.00 = 0.50
    # field_pace_index = 0.5 * 2 + (0.20+0.40)*1 = 1.0 + 0.60 = 1.60
    # nige_candidate_count (others above 0.4): horse_b yes => 1
    # self_nige_rate_minus_field_avg = 0.10 - (0.50/2) = 0.10 - 0.25 = -0.15
    # umaban_x_nige = 0.5 * 0.10 = 0.05
    assert rows[0] == ("horse_a", 0.5, 1.6, 1, -0.15, 0.05)
    # For horse_b (past_nige=0.50): field sum of others = 0.10 + 0.00 = 0.10
    # field_pace_index = 0.1 * 2 + (0.20+0.40)*1 = 0.2 + 0.6 = 0.8
    # nige_candidate_count = 0 (horse_a=0.10, horse_c=0.00, neither > 0.4)
    # self_minus = 0.50 - 0.05 = 0.45
    # umaban_x_nige = 0.3 * 0.50 = 0.15
    assert rows[1] == ("horse_b", 0.1, 0.8, 0, 0.45, 0.15)
    # For horse_c (past_nige=0.00): field sum of others = 0.10 + 0.50 = 0.60
    # field_pace_index = 0.6 * 2 + (0.20+0.20)*1 = 1.2 + 0.4 = 1.6
    # nige_candidate_count = 1 (horse_b > 0.4)
    # self_minus = 0.00 - 0.30 = -0.30
    # umaban_x_nige = 0.8 * 0.00 = 0.00
    assert rows[2] == ("horse_c", 0.6, 1.6, 1, -0.3, 0.0)
