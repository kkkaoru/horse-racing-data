from __future__ import annotations

from pathlib import Path

import pytest

import finish_position_features_duckdb as subject


def test_parse_args_defaults():
    args = subject.parse_args([])
    assert args.category == "jra"
    assert args.from_date == "20160101"
    assert args.to_date == "20251231"
    assert args.output_dir == subject.DEFAULT_OUTPUT_DIR


def test_parse_args_full_set():
    args = subject.parse_args(
        [
            "--category",
            "nar",
            "--from-date",
            "20200101",
            "--to-date",
            "20251231",
            "--output-dir",
            "tmp/x",
            "--pg-url",
            "postgresql://u:p@h/db",
            "--threads",
            "4",
            "--memory-limit",
            "2GB",
        ]
    )
    assert args.category == "nar"
    assert args.from_date == "20200101"
    assert args.pg_url == "postgresql://u:p@h/db"
    assert args.threads == 4


def test_parse_args_rejects_unknown_category():
    with pytest.raises(SystemExit):
        subject.parse_args(["--category", "bogus"])


def test_resolve_pg_url_uses_cli_when_provided():
    assert subject.resolve_pg_url("postgresql://cli/db") == "postgresql://cli/db"


def test_resolve_pg_url_falls_back_to_env_local(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABASE_URL_LOCAL", "postgresql://env-local/db")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert subject.resolve_pg_url(None) == "postgresql://env-local/db"


def test_resolve_pg_url_falls_back_to_env_database_url(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("DATABASE_URL_LOCAL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://env-fallback/db")
    assert subject.resolve_pg_url(None) == "postgresql://env-fallback/db"


def test_resolve_pg_url_uses_default_when_nothing_set(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("DATABASE_URL_LOCAL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert subject.resolve_pg_url(None) == subject.DEFAULT_PG_URL


def test_resolve_pg_url_empty_string_treated_as_unset(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("DATABASE_URL_LOCAL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert subject.resolve_pg_url("") == subject.DEFAULT_PG_URL


def test_category_source_filter_jra():
    assert subject.category_source_filter("jra", "rec") == "rec.source = 'jra'"


def test_category_source_filter_nar_excludes_ban_ei():
    assert (
        subject.category_source_filter("nar", "rec")
        == "rec.source = 'nar' and rec.keibajo_code <> '83'"
    )


def test_category_source_filter_ban_ei():
    assert (
        subject.category_source_filter("ban-ei", "rec")
        == "rec.source = 'nar' and rec.keibajo_code = '83'"
    )


def test_category_source_filter_all_passes_through():
    assert subject.category_source_filter("all", "rec") == "true"


def test_category_expression_returns_literal_when_pinned():
    assert subject.category_expression("jra") == "'jra'"
    assert subject.category_expression("nar") == "'nar'"
    assert subject.category_expression("ban-ei") == "'ban-ei'"


def test_category_expression_uses_case_when_all():
    expr = subject.category_expression("all")
    assert "case" in expr
    assert "ban-ei" in expr


def test_horse_career_cte_reads_from_horse_history_base():
    cte = subject.horse_career_cte()
    assert "from horse_history_base" in cte
    assert "speed_index_avg_5" in cte
    assert "days_since_last_race" in cte


def test_horse_history_base_select_defines_strict_history_window():
    select_text = subject.HORSE_HISTORY_BASE_SELECT
    assert "row_number() over" in select_text
    assert "target_race_dt" in select_text
    assert "history_race_dt" in select_text
    assert "target_class_level" in select_text
    assert "history_class_level" in select_text


def test_compute_history_start_subtracts_years_from_yyyy_prefix():
    assert subject.compute_history_start("20160101", 10) == "20060101"
    assert subject.compute_history_start("20251231", 5) == "20201231"


def test_jockey_cte_filters_on_kishumei_ryakusho():
    cte = subject.jockey_cte()
    assert "h.kishumei_ryakusho = t.kishumei_ryakusho" in cte
    assert "jockey_career_win_rate" in cte


def test_trainer_cte_filters_on_chokyoshimei_ryakusho():
    cte = subject.trainer_cte()
    assert "h.chokyoshimei_ryakusho = t.chokyoshimei_ryakusho" in cte
    assert "trainer_career_win_rate" in cte


def test_pedigree_monthly_stat_sql_includes_target_months_join():
    spec = subject.PEDIGREE_STAT_SPECS[0]
    sql = subject.pedigree_monthly_stat_sql(spec)
    assert "join monthly m on m.race_year_month < tm.stats_year_month" in sql
    assert "stats_year_month" in sql
    assert "race_year_month" in sql


def test_pedigree_monthly_stat_sql_reads_from_pedigree_rec_um():
    spec = subject.PEDIGREE_STAT_SPECS[0]
    sql = subject.pedigree_monthly_stat_sql(spec)
    assert "from pedigree_rec_um" in sql
    assert "ketto_joho_01b as sire" in sql
    assert "kyori_band" in sql


def test_pedigree_stat_specs_cover_four_tables():
    table_names = [spec["table"] for spec in subject.PEDIGREE_STAT_SPECS]
    assert table_names == [
        "sire_distance_stats",
        "sire_track_stats",
        "damsire_distance_stats",
        "damsire_track_stats",
    ]


def test_sire_distance_stats_uses_finish_norm_count_in_denominator():
    spec = subject.PEDIGREE_STAT_SPECS[0]
    sql = subject.pedigree_monthly_stat_sql(spec)
    assert "count(finish_norm) as finish_norm_count" in sql
    assert "nullif(sum(m.finish_norm_count), 0)" in sql


def test_damsire_track_stats_uses_finish_norm_count_in_denominator():
    spec = subject.PEDIGREE_STAT_SPECS[3]
    sql = subject.pedigree_monthly_stat_sql(spec)
    assert "count(finish_norm) as finish_norm_count" in sql
    assert "nullif(sum(m.finish_norm_count), 0)" in sql


def test_win_rate_specs_still_use_race_count_in_denominator():
    sire_track = subject.PEDIGREE_STAT_SPECS[1]
    damsire_distance = subject.PEDIGREE_STAT_SPECS[2]
    sire_track_sql = subject.pedigree_monthly_stat_sql(sire_track)
    damsire_distance_sql = subject.pedigree_monthly_stat_sql(damsire_distance)
    assert "sire_track_win_rate_val" in sire_track_sql
    assert "nullif(sum(m.race_count), 0) as sire_track_win_rate_val" in sire_track_sql
    assert "nullif(sum(m.race_count), 0) as dam_sire_distance_win_rate_val" in damsire_distance_sql


def test_pedigree_rec_um_sql_projects_required_columns():
    sql = subject.pedigree_rec_um_sql("jra")
    assert "create or replace temp table pedigree_rec_um as" in sql
    assert "race_year_month" in sql
    assert "ketto_joho_01b" in sql
    assert "ketto_joho_05b" in sql


def test_pedigree_rec_um_sql_uses_category_specific_join():
    jra_sql = subject.pedigree_rec_um_sql("jra")
    all_sql = subject.pedigree_rec_um_sql("all")
    assert "jra_um" in jra_sql
    assert "union all" in all_sql


def test_target_pedigree_sql_joins_both_horse_masters():
    sql = subject.target_pedigree_sql()
    assert "left join jra_um" in sql
    assert "left join nar_um" in sql
    assert "coalesce(j_um.ketto_joho_01b, n_um.ketto_joho_01b)" in sql


def test_target_months_sql_groups_distinct_year_months():
    sql = subject.target_months_sql()
    assert "distinct" in sql
    assert "stats_year_month" in sql


def test_pedigree_rec_um_subquery_returns_distinct_clauses_per_category():
    assert "where rec.source = 'jra'" in subject.pedigree_rec_um_subquery("jra")
    assert (
        "where rec.source = 'nar' and rec.keibajo_code <> '83'"
        in subject.pedigree_rec_um_subquery("nar")
    )
    assert (
        "where rec.source = 'nar' and rec.keibajo_code = '83'"
        in subject.pedigree_rec_um_subquery("ban-ei")
    )
    assert "union all" in subject.pedigree_rec_um_subquery("all")


def test_race_context_cte_ranks_top_three_by_lowest_time_sa():
    cte = subject.race_context_cte()
    assert "order by speed_index_best_5 asc nulls last" in cte
    assert "rk <= 3" in cte


def test_track_bias_cte_defines_inside_and_front_thresholds():
    cte = subject.track_bias_cte()
    assert "h.umaban * 2 <= h.shusso_tosu + 1" in cte
    assert f"<= {subject.FRONT_CORNER_THRESHOLD}" in cte


def test_weight_cte_aggregates_baked_bataiju_from_horse_history_base():
    cte = subject.weight_cte()
    assert "from horse_history_base" in cte
    assert "target_current_bataiju" in cte
    assert "history_bataiju" in cte
    assert "weight_avg_5" in cte
    # se tables are not joined inside weight_cte anymore (bataiju is baked in)
    assert "jra_se" not in cte
    assert "nar_se" not in cte


def test_recent_form_cte_uses_regr_slope_with_three_race_guard():
    cte = subject.recent_form_cte()
    assert "regr_slope(finish_norm" in cte
    assert f">= {subject.TREND_MIN_RACES}" in cte


def test_legacy_five_cte_emits_popularity_and_odds_scoring():
    cte = subject.legacy_five_cte()
    assert "popularity_score" in cte
    assert "odds_score" in cte
    assert "(t.ninkijun - 1)" in cte


def test_assemble_final_select_from_temp_tables_contains_all_feature_groups():
    sql = subject.assemble_final_select_from_temp_tables("jra")
    expected_keywords = [
        "speed_index_avg_5",
        "jockey_career_win_rate",
        "trainer_career_win_rate",
        "sire_distance_win_rate",
        "field_strength_avg_speed",
        "track_bias_inside",
        "weight_avg_5",
        "last_race_finish_norm",
        "weather_normalized",
        "popularity_score",
        "race_year",
        "race_id",
    ]
    for keyword in expected_keywords:
        assert keyword in sql


def test_assemble_final_select_appends_race_internal_relative_features():
    sql = subject.assemble_final_select_from_temp_tables("jra")
    expected_new_columns = [
        "speed_index_avg_5_rank_in_race",
        "speed_index_best_5_rank_in_race",
        "jockey_recent_win_rate_rank_in_race",
        "trainer_career_win_rate_rank_in_race",
        "pedigree_score_for_race_rank_in_race",
        "same_distance_win_rate_rank_in_race",
        "speed_index_avg_5_diff_from_race_avg",
        "jockey_recent_win_rate_diff_from_race_avg",
        "pedigree_score_diff_from_race_avg",
    ]
    for column in expected_new_columns:
        assert column in sql


def test_assemble_final_select_uses_race_partition_window():
    sql = subject.assemble_final_select_from_temp_tables("jra")
    assert "with base_features as" in sql
    assert "partition by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango" in sql
    assert "order by b.speed_index_avg_5 asc nulls last" in sql
    assert "order by b.jockey_recent_win_rate desc nulls last" in sql


def test_base_features_select_sql_contains_existing_columns():
    sql = subject.base_features_select_sql("jra")
    assert "speed_index_avg_5" in sql
    assert "pedigree_score_for_race" in sql
    assert "weight_diff_from_avg" in sql


def test_count_output_rows_returns_zero_for_missing_dir(tmp_path: Path):
    assert subject.count_output_rows(tmp_path / "missing") == 0


def test_main_invokes_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda line: captured.append(line))
    monkeypatch.setattr(
        subject,
        "run",
        lambda args: {
            "elapsed_seconds": 0.0,
            "output_dir": str(tmp_path),
            "rows_written": 0,
        },
    )
    subject.main(["--category", "jra", "--output-dir", str(tmp_path)])
    assert captured
    assert "rows_written" in captured[0]


def test_running_style_class_thresholds_are_documented():
    assert subject.RUNNING_STYLE_SENKOU_THRESHOLD == 0.30
    assert subject.RUNNING_STYLE_SASHI_THRESHOLD == 0.70
    assert subject.RUNNING_STYLE_CLASS_NIGE == 0
    assert subject.RUNNING_STYLE_CLASS_SENKOU == 1
    assert subject.RUNNING_STYLE_CLASS_SASHI == 2
    assert subject.RUNNING_STYLE_CLASS_OIKOMI == 3


def _classify_running_style(corner1_norm: float | None) -> int | None:
    """Reference Python implementation mirroring the SQL CASE in build_target_table()."""
    if corner1_norm is None:
        return None
    if corner1_norm == 0:
        return subject.RUNNING_STYLE_CLASS_NIGE
    if corner1_norm <= subject.RUNNING_STYLE_SENKOU_THRESHOLD:
        return subject.RUNNING_STYLE_CLASS_SENKOU
    if corner1_norm <= subject.RUNNING_STYLE_SASHI_THRESHOLD:
        return subject.RUNNING_STYLE_CLASS_SASHI
    return subject.RUNNING_STYLE_CLASS_OIKOMI


def test_running_style_label_nige_when_corner_1_first():
    assert _classify_running_style(0.0) == 0


def test_running_style_label_senkou_when_corner_1_norm_within_first_30_percent():
    assert _classify_running_style(0.05) == 1
    assert _classify_running_style(0.30) == 1


def test_running_style_label_sashi_when_corner_1_norm_in_middle():
    assert _classify_running_style(0.31) == 2
    assert _classify_running_style(0.50) == 2
    assert _classify_running_style(0.70) == 2


def test_running_style_label_oikomi_when_corner_1_norm_exceeds_70_percent():
    assert _classify_running_style(0.71) == 3
    assert _classify_running_style(1.00) == 3


def test_running_style_label_null_when_corner_1_norm_missing():
    assert _classify_running_style(None) is None


def test_build_target_table_emits_running_style_label_via_duckdb():
    import duckdb

    con = duckdb.connect()
    con.execute(
        """
        create or replace temp table rec as
        select * from (
          values
            ('jra', '20250101', date '2025-01-01', '2025', '0101', '05', '01',
              '2020100001', 3, 'jockey_a', 'trainer_a',
              1600, '11', 'A', '99', 16, 1, 1.0/16,
              'name_a', 'fukudai_a',
              null::double, null::double, null::double, null::double,
              '1', '1', 1, 50.0, null::int, 0.00),
            ('jra', '20250101', date '2025-01-01', '2025', '0101', '05', '01',
              '2020100002', 5, 'jockey_b', 'trainer_b',
              1600, '11', 'A', '99', 16, 2, 2.0/16,
              'name_b', 'fukudai_b',
              null::double, null::double, null::double, null::double,
              '1', '1', 2, 100.0, null::int, 0.20),
            ('jra', '20250101', date '2025-01-01', '2025', '0101', '05', '01',
              '2020100003', 8, 'jockey_c', 'trainer_c',
              1600, '11', 'A', '99', 16, 10, 10.0/16,
              'name_c', 'fukudai_c',
              null::double, null::double, null::double, null::double,
              '1', '1', 5, 500.0, null::int, 0.50),
            ('jra', '20250101', date '2025-01-01', '2025', '0101', '05', '01',
              '2020100004', 12, 'jockey_d', 'trainer_d',
              1600, '11', 'A', '99', 16, 15, 15.0/16,
              'name_d', 'fukudai_d',
              null::double, null::double, null::double, null::double,
              '1', '1', 12, 1500.0, null::int, 0.95),
            ('jra', '20250101', date '2025-01-01', '2025', '0101', '05', '01',
              '2020100005', 16, 'jockey_e', 'trainer_e',
              1600, '11', 'A', '99', 16, null::int, null::double,
              'name_e', 'fukudai_e',
              null::double, null::double, null::double, null::double,
              '1', '1', 16, 2000.0, null::int, null::double)
        ) as v(
          source, race_date, race_dt, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban, kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code, shusso_tosu, finish_position, finish_norm,
          kyosomei_hondai, kyosomei_fukudai,
          time_sa, kohan_3f, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, bataiju, corner1_norm
        )
        """
    )
    subject.build_target_table(con, "jra", "20250101", "20251231")
    rows = con.execute(
        """
        select ketto_toroku_bango, target_corner_1_norm, target_running_style_class
        from target order by ketto_toroku_bango
        """
    ).fetchall()
    assert rows == [
        ("2020100001", 0.0, 0),
        ("2020100002", 0.2, 1),
        ("2020100003", 0.5, 2),
        ("2020100004", 0.95, 3),
        ("2020100005", None, None),
    ]


def test_horse_running_style_history_cte_aggregates_corner_1_norm():
    cte = subject.horse_running_style_history_cte()
    assert "past_corner_1_norm_avg_5" in cte
    assert "past_corner_1_norm_std_5" in cte
    assert "past_corner_1_norm_best_5" in cte
    assert "past_corner_1_norm_worst_5" in cte
    assert "past_nige_rate_self" in cte
    assert "past_senkou_rate_self" in cte
    assert "past_sashi_rate_self" in cte
    assert "past_oikomi_rate_self" in cte
    assert "last_race_corner_1_norm" in cte
    assert "last_race_corner_progression" in cte
    assert "horse_distance_corner_1_norm_avg" in cte
    assert "horse_track_corner_1_norm_avg" in cte
    assert "from horse_history_base" in cte


def test_horse_running_style_history_cte_uses_recent_window_filter():
    cte = subject.horse_running_style_history_cte()
    assert f"recent_rank <= {subject.RECENT_WINDOW_SIZE}" in cte


def test_horse_running_style_history_cte_emits_style_win_rates():
    cte = subject.horse_running_style_history_cte()
    assert "past_nige_win_rate_self" in cte
    assert "past_senkou_win_rate_self" in cte
    assert "past_sashi_win_rate_self" in cte
    assert "past_oikomi_win_rate_self" in cte


def test_horse_running_style_history_cte_emits_iqr_and_grade_counts():
    cte = subject.horse_running_style_history_cte()
    assert "past_corner_1_norm_iqr_5" in cte
    assert "quantile_cont(b.corner1_norm, 0.75)" in cte
    assert "quantile_cont(b.corner1_norm, 0.25)" in cte
    assert "top1_count_in_grade_races" in cte
    assert "place_count_in_grade_races" in cte
    assert "experience_in_g1_race" in cte


def test_horse_running_style_history_cte_emits_recent_streak_proxies():
    cte = subject.horse_running_style_history_cte()
    assert "recent_win_count_5" in cte
    assert "recent_top3_count_5" in cte
    assert "past_dominant_label_consistency_5" in cte


def test_jockey_cte_emits_running_style_aggregates():
    cte = subject.jockey_cte()
    assert "jockey_nige_rate" in cte
    assert "jockey_senkou_rate" in cte
    assert "jockey_sashi_rate" in cte
    assert "jockey_oikomi_rate" in cte
    assert "jockey_corner_1_norm_avg" in cte
    assert "jockey_horse_corner_1_norm_avg" in cte


def test_partner_history_cte_propagates_corner_1_norm():
    cte = subject.partner_history_cte("jockey_history", "kishumei_ryakusho", "jockey_career")
    assert "cast(h.corner1_norm as double) as corner1_norm" in cte


def test_base_features_select_sql_includes_jockey_running_style():
    sql = subject.base_features_select_sql("jra")
    assert "jc.jockey_nige_rate" in sql
    assert "jc.jockey_senkou_rate" in sql
    assert "jc.jockey_sashi_rate" in sql
    assert "jc.jockey_oikomi_rate" in sql
    assert "jc.jockey_corner_1_norm_avg" in sql
    assert "jc.jockey_horse_corner_1_norm_avg" in sql


def test_base_features_select_sql_includes_extended_horse_features():
    sql = subject.base_features_select_sql("jra")
    assert "rsh.past_nige_win_rate_self" in sql
    assert "rsh.past_corner_1_norm_iqr_5" in sql
    assert "rsh.experience_in_g1_race" in sql
    assert "rsh.recent_win_count_5" in sql
    assert "rsh.past_dominant_label_consistency_5" in sql


def test_per_year_specs_registers_horse_running_style_history():
    names = [spec["name"] for spec in subject.PER_YEAR_SPECS]
    assert "horse_running_style_history" in names


def test_kyori_band_constants_partition_distance_range():
    assert subject.KYORI_BAND_SPRINT_MAX < subject.KYORI_BAND_MILE_MAX
    assert subject.KYORI_BAND_MILE_MAX < subject.KYORI_BAND_INTERMEDIATE_MAX
    assert subject.KYORI_BAND_SPRINT == 0
    assert subject.KYORI_BAND_LONG == 3


def test_season_band_constants_distinct_values():
    assert subject.SEASON_SPRING == 0
    assert subject.SEASON_SUMMER == 1
    assert subject.SEASON_AUTUMN == 2
    assert subject.SEASON_WINTER == 3


def test_base_features_select_sql_includes_running_style_history_columns():
    sql = subject.base_features_select_sql("jra")
    assert "rsh.past_corner_1_norm_avg_5" in sql
    assert "rsh.past_nige_rate_self" in sql
    assert "rsh.last_race_corner_1_norm" in sql
    assert "rsh.horse_distance_corner_1_norm_avg" in sql
    assert "rsh.horse_track_corner_1_norm_avg" in sql
    assert "left join horse_running_style_history rsh" in sql


def test_base_features_select_sql_includes_direct_target_features():
    sql = subject.base_features_select_sql("jra")
    assert "umaban_norm" in sql
    assert "is_newcomer_race" in sql
    assert "kyori_band" in sql
    assert "season_band" in sql


def test_build_target_table_keeps_finish_position_intact():
    """Regression: adding target_* columns must not change existing finish_position output."""
    import duckdb

    con = duckdb.connect()
    con.execute(
        """
        create or replace temp table rec as
        select * from (
          values
            ('jra', '20250101', date '2025-01-01', '2025', '0101', '05', '01',
              '2020999999', 7, 'jockey_x', 'trainer_x',
              1800, '11', 'A', '99', 12, 3, 3.0/12,
              'name_x', 'fukudai_x',
              null::double, null::double, null::double, null::double,
              '1', '1', 3, 250.0, null::int, 0.18)
        ) as v(
          source, race_date, race_dt, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban, kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code, shusso_tosu, finish_position, finish_norm,
          kyosomei_hondai, kyosomei_fukudai,
          time_sa, kohan_3f, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, bataiju, corner1_norm
        )
        """
    )
    subject.build_target_table(con, "jra", "20250101", "20251231")
    row = con.execute(
        "select finish_position, finish_norm from target"
    ).fetchone()
    assert row == (3, 3.0 / 12.0)
