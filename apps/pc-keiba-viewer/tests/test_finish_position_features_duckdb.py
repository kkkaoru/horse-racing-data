from __future__ import annotations

import argparse
import json
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
    assert (
        subject.category_source_filter("jra", "rec")
        == "rec.source = 'jra' and rec.keibajo_code in "
        "('01', '02', '03', '04', '05', '06', '07', '08', '09', '10')"
    )


def test_jra_keibajo_codes_sql_lists_central_venues_only():
    assert subject.JRA_KEIBAJO_CODES == (
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
    )
    assert (
        subject.JRA_KEIBAJO_CODES_SQL
        == "('01', '02', '03', '04', '05', '06', '07', '08', '09', '10')"
    )


def test_upcoming_target_union_sql_jra_restricts_to_central_keibajo():
    sql = subject.upcoming_target_union_sql("jra", "20260101", "20260131")
    assert (
        "se.keibajo_code in ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10')"
        in sql
    )


def test_category_source_filter_nar_excludes_ban_ei():
    assert (
        subject.category_source_filter("nar", "rec")
        == "rec.source = 'nar' and (rec.keibajo_code is null or rec.keibajo_code <> '83')"
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


def test_parse_args_target_date_and_days_ahead():
    args = subject.parse_args(["--target-date", "20260603", "--days-ahead", "2"])
    assert args.target_date == "20260603"
    assert args.days_ahead == 2


def test_parse_args_target_date_defaults_days_ahead_zero():
    args = subject.parse_args(["--target-date", "20260603"])
    assert args.target_date == "20260603"
    assert args.days_ahead == 0


def test_parse_args_defaults_target_date_none():
    args = subject.parse_args([])
    assert args.target_date is None
    assert args.days_ahead == 0


def test_parse_args_allow_empty_targets_defaults_false():
    args = subject.parse_args([])
    assert args.allow_empty_targets is False


def test_parse_args_allow_empty_targets_sets_true():
    args = subject.parse_args(["--allow-empty-targets"])
    assert args.allow_empty_targets is True


def test_target_date_arg_accepts_valid_yyyymmdd():
    assert subject.target_date_arg("20260603") == "20260603"


def test_target_date_arg_rejects_malformed():
    with pytest.raises(argparse.ArgumentTypeError):
        subject.target_date_arg("2026-06-03")


def test_target_date_arg_rejects_impossible_calendar_date():
    with pytest.raises(argparse.ArgumentTypeError):
        subject.target_date_arg("20260631")


def test_non_negative_int_accepts_zero():
    assert subject.non_negative_int("0") == 0


def test_non_negative_int_rejects_negative():
    with pytest.raises(argparse.ArgumentTypeError):
        subject.non_negative_int("-1")


def test_add_days_advances_across_month_boundary():
    assert subject.add_days("20260603", 0) == "20260603"
    assert subject.add_days("20260603", 2) == "20260605"
    assert subject.add_days("20260630", 1) == "20260701"


def test_resolve_date_range_uses_from_to_when_no_target_date():
    args = subject.parse_args(["--from-date", "20200101", "--to-date", "20211231"])
    assert subject.resolve_date_range(args) == ("20200101", "20211231")


def test_resolve_date_range_uses_target_date_window_when_set():
    args = subject.parse_args(["--target-date", "20260603", "--days-ahead", "2"])
    assert subject.resolve_date_range(args) == ("20260603", "20260605")


def test_resolve_date_range_single_day_when_days_ahead_zero():
    args = subject.parse_args(["--target-date", "20260603"])
    assert subject.resolve_date_range(args) == ("20260603", "20260603")


def test_resolve_upcoming_window_none_without_target_date():
    args = subject.parse_args(["--from-date", "20200101", "--to-date", "20211231"])
    assert subject.resolve_upcoming_window(args, "20200101", "20211231") is None


def test_resolve_upcoming_window_returns_window_with_target_date():
    args = subject.parse_args(["--target-date", "20260603"])
    assert subject.resolve_upcoming_window(args, "20260603", "20260603") == (
        "20260603",
        "20260603",
    )


def test_upcoming_target_union_sql_jra_reads_jvd_tables_and_nulls_corners():
    sql = subject.upcoming_target_union_sql("jra", "20260603", "20260603")
    assert "pg.jvd_se se" in sql
    assert "pg.jvd_ra ra" in sql
    assert "cast(null as double) as corner1_norm" in sql
    assert "'jra' as source" in sql


def test_upcoming_target_union_sql_nullifies_unrun_finish_position():
    sql = subject.upcoming_target_union_sql("nar", "20260603", "20260603")
    assert "nullif(nullif(trim(se.kakutei_chakujun), ''), '00') as int) as finish_position" in sql


def test_upcoming_target_union_sql_requires_numeric_umaban():
    sql = subject.upcoming_target_union_sql("jra", "20260603", "20260603")
    assert "try_cast(nullif(trim(se.umaban), '') as int) is not null" in sql


def test_upcoming_target_union_sql_nar_excludes_ban_ei_keibajo():
    sql = subject.upcoming_target_union_sql("nar", "20260603", "20260603")
    assert "pg.nvd_se se" in sql
    assert "(se.keibajo_code is null or se.keibajo_code <> '83')" in sql
    assert "'nar' as source" in sql


def test_upcoming_target_union_sql_ban_ei_filters_to_ban_ei_keibajo():
    sql = subject.upcoming_target_union_sql("ban-ei", "20260603", "20260603")
    assert "pg.nvd_se se" in sql
    assert "se.keibajo_code = '83'" in sql


def test_upcoming_target_union_sql_all_unions_three_categories():
    sql = subject.upcoming_target_union_sql("all", "20260603", "20260603")
    assert "pg.jvd_se se" in sql
    assert "(se.keibajo_code is null or se.keibajo_code <> '83')" in sql
    assert "se.keibajo_code = '83'" in sql


def test_build_rec_select_sql_without_upcoming_window_keeps_corner_source():
    sql = subject.build_rec_select_sql("nar", "20100101", "20251231")
    assert "from pg.race_entry_corner_features" in sql
    assert "_rec_priority" not in sql


def test_build_rec_select_sql_with_upcoming_window_dedupes_and_adds_direct_source():
    sql = subject.build_rec_select_sql(
        "nar", "20100101", "20260603", ("20260603", "20260603")
    )
    assert "from pg.race_entry_corner_features" in sql
    assert "pg.nvd_se se" in sql
    assert "_rec_priority" in sql
    assert "row_number() over" in sql


def test_build_rec_select_sql_ban_ei_with_upcoming_window_uses_ban_ei_history():
    sql = subject.build_rec_select_sql(
        "ban-ei", "20100101", "20260603", ("20260603", "20260603")
    )
    assert "from pg.nvd_se se" in sql
    assert "se.keibajo_code = '83'" in sql
    assert "_rec_priority" in sql


def test_jockey_cte_filters_on_kishumei_ryakusho():
    cte = subject.jockey_cte()
    assert "h.kishumei_ryakusho = t.kishumei_ryakusho" in cte
    assert "jockey_career_win_rate" in cte


def test_trainer_cte_filters_on_chokyoshimei_ryakusho():
    cte = subject.trainer_cte()
    assert "h.chokyoshimei_ryakusho = t.chokyoshimei_ryakusho" in cte
    assert "trainer_career_win_rate" in cte


def test_pedigree_monthly_stat_sql_uses_asof_strictly_prior_month():
    spec = subject.PEDIGREE_STAT_SPECS[0]
    sql = subject.pedigree_monthly_stat_sql(spec)
    assert "asof join cumulative c" in sql
    assert "c.race_year_month < tm.stats_year_month" in sql
    assert "cross join stat_keys k" in sql
    assert "stats_year_month" in sql
    assert "race_year_month" in sql


def test_pedigree_monthly_stat_sql_builds_cumulative_window_once():
    spec = subject.PEDIGREE_STAT_SPECS[0]
    sql = subject.pedigree_monthly_stat_sql(spec)
    assert "rows between unbounded preceding and current row" in sql
    assert "sum(m.win_count) over w as cum_win_count" in sql
    assert "sum(m.race_count) over w as cum_race_count" in sql
    assert "c.cum_race_count as race_count" in sql


def test_pedigree_monthly_stat_sql_reads_from_pedigree_rec_um():
    spec = subject.PEDIGREE_STAT_SPECS[0]
    sql = subject.pedigree_monthly_stat_sql(spec)
    assert "from pedigree_rec_um" in sql
    assert "ketto_joho_01b as sire" in sql
    assert "kyori_band" in sql


def test_pedigree_stat_base_columns_extracts_aliases_plus_race_count():
    spec = subject.PEDIGREE_STAT_SPECS[0]
    assert subject.pedigree_stat_base_columns(spec) == [
        "win_count",
        "finish_norm_sum",
        "finish_norm_count",
        "race_count",
    ]


def test_pedigree_stat_base_columns_for_win_rate_spec():
    spec = subject.PEDIGREE_STAT_SPECS[1]
    assert subject.pedigree_stat_base_columns(spec) == ["win_count", "race_count"]


def test_pedigree_stat_cumulative_select_emits_running_sums():
    spec = subject.PEDIGREE_STAT_SPECS[1]
    assert subject.pedigree_stat_cumulative_select(spec) == (
        "sum(m.win_count) over w as cum_win_count,\n"
        "        sum(m.race_count) over w as cum_race_count"
    )


def test_pedigree_stat_accum_from_cumulative_rewrites_sum_to_cum():
    spec = subject.PEDIGREE_STAT_SPECS[1]
    assert subject.pedigree_stat_accum_from_cumulative(spec) == (
        "c.cum_win_count::double / nullif(c.cum_race_count, 0) as sire_track_win_rate_val"
    )


def test_pedigree_stat_specs_cover_seven_tables():
    table_names = [spec["table"] for spec in subject.PEDIGREE_STAT_SPECS]
    assert table_names == [
        "sire_distance_stats",
        "sire_track_stats",
        "damsire_distance_stats",
        "damsire_track_stats",
        "sire_keibajo_stats",
        "damsire_keibajo_stats",
        "sire_running_style_stats",
    ]


def test_sire_distance_stats_uses_finish_norm_count_in_denominator():
    spec = subject.PEDIGREE_STAT_SPECS[0]
    sql = subject.pedigree_monthly_stat_sql(spec)
    assert "count(finish_norm) as finish_norm_count" in sql
    assert "nullif(c.cum_finish_norm_count, 0)" in sql


def test_damsire_track_stats_uses_finish_norm_count_in_denominator():
    spec = subject.PEDIGREE_STAT_SPECS[3]
    sql = subject.pedigree_monthly_stat_sql(spec)
    assert "count(finish_norm) as finish_norm_count" in sql
    assert "nullif(c.cum_finish_norm_count, 0)" in sql


def test_win_rate_specs_still_use_race_count_in_denominator():
    sire_track = subject.PEDIGREE_STAT_SPECS[1]
    damsire_distance = subject.PEDIGREE_STAT_SPECS[2]
    sire_track_sql = subject.pedigree_monthly_stat_sql(sire_track)
    damsire_distance_sql = subject.pedigree_monthly_stat_sql(damsire_distance)
    assert "sire_track_win_rate_val" in sire_track_sql
    assert "nullif(c.cum_race_count, 0) as sire_track_win_rate_val" in sire_track_sql
    assert "nullif(c.cum_race_count, 0) as dam_sire_distance_win_rate_val" in damsire_distance_sql


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
    assert "left join nar_nu" in sql
    assert "coalesce(j_um.ketto_joho_01b, n_um.ketto_joho_01b, n_nu.ketto_joho_01b)" in sql
    assert "coalesce(j_um.ketto_joho_05b, n_um.ketto_joho_05b, n_nu.ketto_joho_05b)" in sql


def test_target_months_sql_groups_distinct_year_months():
    sql = subject.target_months_sql()
    assert "distinct" in sql
    assert "stats_year_month" in sql


def test_pedigree_rec_um_subquery_returns_distinct_clauses_per_category():
    assert "where rec.source = 'jra'" in subject.pedigree_rec_um_subquery("jra")
    assert (
        "where rec.source = 'nar' and (rec.keibajo_code is null or rec.keibajo_code <> '83')"
        in subject.pedigree_rec_um_subquery("nar")
    )
    assert (
        "where rec.source = 'nar' and rec.keibajo_code = '83'"
        in subject.pedigree_rec_um_subquery("ban-ei")
    )
    assert "union all" in subject.pedigree_rec_um_subquery("all")


def test_pedigree_rec_um_subquery_nar_joins_both_nar_masters():
    sql = subject.pedigree_rec_um_subquery("nar")
    assert "left join nar_um um using (ketto_toroku_bango)" in sql
    assert "left join nar_nu nu using (ketto_toroku_bango)" in sql
    assert "coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) as ketto_joho_01b" in sql
    assert "coalesce(um.ketto_joho_05b, nu.ketto_joho_05b) as ketto_joho_05b" in sql
    assert "and coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) is not null" in sql


def test_pedigree_rec_um_subquery_banei_joins_both_nar_masters():
    sql = subject.pedigree_rec_um_subquery("ban-ei")
    assert "left join nar_um um using (ketto_toroku_bango)" in sql
    assert "left join nar_nu nu using (ketto_toroku_bango)" in sql
    assert "coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) as ketto_joho_01b" in sql
    assert "and coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) is not null" in sql


def test_pedigree_rec_um_subquery_all_joins_both_nar_masters():
    sql = subject.pedigree_rec_um_subquery("all")
    assert "left join nar_um um using (ketto_toroku_bango)" in sql
    assert "left join nar_nu nu using (ketto_toroku_bango)" in sql
    assert "coalesce(um.ketto_joho_01b, nu.ketto_joho_01b) as ketto_joho_01b" in sql


def test_race_context_cte_ranks_top_three_by_lowest_time_sa():
    cte = subject.race_context_cte()
    assert "order by speed_index_best_5 asc nulls last" in cte
    assert "rk <= 3" in cte


def test_track_bias_cte_defines_inside_and_front_thresholds():
    cte = subject.track_bias_cte()
    assert "h.umaban * 2 <= h.shusso_tosu + 1" in cte
    assert f"<= {subject.FRONT_CORNER_THRESHOLD}" in cte


def test_track_bias_cte_guards_null_shusso_tosu():
    cte = subject.track_bias_cte()
    assert "h.shusso_tosu is not null" in cte


def test_track_bias_cte_guards_null_corner1_norm():
    cte = subject.track_bias_cte()
    assert "h.corner1_norm is not null" in cte


def test_pedigree_score_for_race_respects_min_races_guard():
    sql = subject.base_features_select_sql("jra")
    assert f"race_count >= {subject.PEDIGREE_MIN_RACES}" in sql
    assert "pedigree_score_for_race" in sql
    assert "sire_distance_win_rate_val else null end, 0)" in sql


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
              '1', '1', 1, 50.0, null::int, 0.00, 1, 3),
            ('jra', '20250101', date '2025-01-01', '2025', '0101', '05', '01',
              '2020100002', 5, 'jockey_b', 'trainer_b',
              1600, '11', 'A', '99', 16, 2, 2.0/16,
              'name_b', 'fukudai_b',
              null::double, null::double, null::double, null::double,
              '1', '1', 2, 100.0, null::int, 0.20, 1, 4),
            ('jra', '20250101', date '2025-01-01', '2025', '0101', '05', '01',
              '2020100003', 8, 'jockey_c', 'trainer_c',
              1600, '11', 'A', '99', 16, 10, 10.0/16,
              'name_c', 'fukudai_c',
              null::double, null::double, null::double, null::double,
              '1', '1', 5, 500.0, null::int, 0.50, 1, 5),
            ('jra', '20250101', date '2025-01-01', '2025', '0101', '05', '01',
              '2020100004', 12, 'jockey_d', 'trainer_d',
              1600, '11', 'A', '99', 16, 15, 15.0/16,
              'name_d', 'fukudai_d',
              null::double, null::double, null::double, null::double,
              '1', '1', 12, 1500.0, null::int, 0.95, 1, 6),
            ('jra', '20250101', date '2025-01-01', '2025', '0101', '05', '01',
              '2020100005', 16, 'jockey_e', 'trainer_e',
              1600, '11', 'A', '99', 16, null::int, null::double,
              'name_e', 'fukudai_e',
              null::double, null::double, null::double, null::double,
              '1', '1', 16, 2000.0, null::int, null::double, 1, 7)
        ) as v(
          source, race_date, race_dt, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban, kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code, shusso_tosu, finish_position, finish_norm,
          kyosomei_hondai, kyosomei_fukudai,
          time_sa, kohan_3f, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, bataiju, corner1_norm, seibetsu_code, barei
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


def test_jockey_cte_emits_recent_90d_window_aggregates():
    cte = subject.jockey_cte()
    assert "jockey_recent_corner_1_norm_avg_90d" in cte
    assert "jockey_recent_nige_rate_90d" in cte
    assert f"target_race_dt - {subject.JOCKEY_RECENT_DAYS}" in cte


def test_horse_running_style_history_cte_emits_last_3_kohan_3f():
    cte = subject.horse_running_style_history_cte()
    assert "last_3_avg_kohan_3f" in cte


def test_base_features_select_sql_includes_recent_jockey_and_kohan():
    sql = subject.base_features_select_sql("jra")
    assert "jc.jockey_recent_corner_1_norm_avg_90d" in sql
    assert "jc.jockey_recent_nige_rate_90d" in sql
    assert "rsh.last_3_avg_kohan_3f" in sql


def test_pedigree_stat_specs_includes_sire_running_style():
    names = [spec["table"] for spec in subject.PEDIGREE_STAT_SPECS]
    assert "sire_running_style_stats" in names


def test_pedigree_rec_um_sql_propagates_corner_1_norm():
    sql = subject.pedigree_rec_um_sql("jra")
    assert "cast(corner1_norm as double) as corner1_norm" in sql


def test_target_pedigree_sql_includes_running_style_bucket():
    sql = subject.target_pedigree_sql()
    assert "0 as rs_bucket" in sql


def test_base_features_select_sql_includes_sire_running_style():
    sql = subject.base_features_select_sql("jra")
    assert "sire_nige_rate" in sql
    assert "sire_senkou_rate" in sql
    assert "sire_sashi_rate" in sql
    assert "sire_oikomi_rate" in sql
    assert "sire_corner_1_norm_avg" in sql
    assert "left join sire_running_style_stats srs" in subject.pedigree_features_sql()


def test_trainer_cte_emits_running_style_aggregates():
    cte = subject.trainer_cte()
    assert "trainer_nige_rate" in cte
    assert "trainer_senkou_rate" in cte
    assert "trainer_sashi_rate" in cte
    assert "trainer_oikomi_rate" in cte
    assert "trainer_corner_1_norm_avg" in cte


def test_base_features_select_sql_includes_trainer_running_style():
    sql = subject.base_features_select_sql("jra")
    assert "tc.trainer_nige_rate" in sql
    assert "tc.trainer_senkou_rate" in sql
    assert "tc.trainer_sashi_rate" in sql
    assert "tc.trainer_oikomi_rate" in sql
    assert "tc.trainer_corner_1_norm_avg" in sql


def test_horse_running_style_history_cte_emits_multi_window_aggregates():
    cte = subject.horse_running_style_history_cte()
    assert "past_corner_1_norm_avg_3" in cte
    assert "past_corner_1_norm_avg_10" in cte
    assert "past_corner_progression_avg_5" in cte
    assert "recent_rank <= 3" in cte
    assert "recent_rank <= 10" in cte


def test_base_features_select_sql_includes_multi_window_corner_avgs():
    sql = subject.base_features_select_sql("jra")
    assert "rsh.past_corner_1_norm_avg_3" in sql
    assert "rsh.past_corner_1_norm_avg_10" in sql
    assert "rsh.past_corner_progression_avg_5" in sql


def test_base_features_select_sql_includes_extended_horse_features():
    sql = subject.base_features_select_sql("jra")
    assert "rsh.past_nige_win_rate_self" in sql
    assert "rsh.past_corner_1_norm_iqr_5" in sql
    assert "rsh.experience_in_g1_race" in sql
    assert "rsh.recent_win_count_5" in sql
    assert "rsh.past_dominant_label_consistency_5" in sql


def test_per_year_specs_registers_horse_running_style_history():
    names = [spec["name"] for spec in subject.build_per_year_specs(subject.CATEGORY_JRA)]
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
              '1', '1', 3, 250.0, null::int, 0.18, 1, 4)
        ) as v(
          source, race_date, race_dt, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban, kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code, shusso_tosu, finish_position, finish_norm,
          kyosomei_hondai, kyosomei_fukudai,
          time_sa, kohan_3f, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, bataiju, corner1_norm, seibetsu_code, barei
        )
        """
    )
    subject.build_target_table(con, "jra", "20250101", "20251231")
    row = con.execute(
        "select finish_position, finish_norm from target"
    ).fetchone()
    assert row == (3, 3.0 / 12.0)


def test_nar_subclass_named_classes_constant():
    assert subject.NAR_NAMED_CLASSES == (
        "OP",
        "NEW",
        "MUKATSU",
        "A",
        "B",
        "C",
        "2YO",
        "3YO",
        "other",
    )


def test_nar_subclass_case_sql_emits_meisho_regex_matches_with_null_for_non_nar():
    sql = subject.nar_subclass_case_sql("t.source", "t.keibajo_code", "wl.nar_kyoso_joken_meisho")
    assert "when t.source <> 'nar' or t.keibajo_code = '83' then null" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, 'ＯＰ')" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, '新馬')" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, '未勝利|未出走')" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, '２歳|2歳')" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, '３歳|3歳')" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, 'Ａ')" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, 'Ｂ')" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, 'Ｃ')" in sql
    assert "else 'other'" in sql


def test_base_features_select_sql_includes_nar_subclass_alias():
    sql = subject.base_features_select_sql("nar")
    assert "as nar_subclass" in sql
    assert "wl.nar_kyoso_joken_meisho" in sql


def test_base_features_select_sql_includes_kyoso_joken_code_passthrough():
    sql = subject.base_features_select_sql("jra")
    assert "t.kyoso_joken_code as kyoso_joken_code" in sql


def _eval_nar_subclass(
    source: str, keibajo_code: str, meisho: str | None
) -> str | None:
    import duckdb

    con = duckdb.connect()
    case_sql = subject.nar_subclass_case_sql("v.source", "v.keibajo_code", "v.meisho")
    rows = con.execute(
        f"""
        with v(source, keibajo_code, meisho) as (
          values (?::varchar, ?::varchar, ?::varchar)
        )
        select {case_sql} from v
        """,
        (source, keibajo_code, meisho),
    ).fetchone()
    assert rows is not None
    return rows[0]


def test_nar_subclass_returns_null_for_jra_row():
    assert _eval_nar_subclass("jra", "05", "「３歳　　　　」") is None


def test_nar_subclass_returns_null_for_ban_ei_row():
    assert _eval_nar_subclass("nar", "83", "「　　　Ｃ２　」") is None


def test_nar_subclass_returns_op_for_meisho_matching_op():
    assert _eval_nar_subclass("nar", "30", "「　　　ＯＰ　」") == "OP"


def test_nar_subclass_returns_new_for_meisho_matching_shinba():
    assert _eval_nar_subclass("nar", "30", "「新馬　　　　」") == "NEW"


def test_nar_subclass_returns_mukatsu_for_meisho_matching_mishori():
    assert _eval_nar_subclass("nar", "30", "「　未勝利　　」") == "MUKATSU"


def test_nar_subclass_returns_mukatsu_for_meisho_matching_mishusso():
    assert _eval_nar_subclass("nar", "30", "「　未出走　　」") == "MUKATSU"


def test_nar_subclass_returns_a_for_meisho_matching_zenkaku_a():
    assert _eval_nar_subclass("nar", "30", "「　　　Ａ１　」") == "A"


def test_nar_subclass_returns_b_for_meisho_matching_zenkaku_b():
    assert _eval_nar_subclass("nar", "30", "「　　　Ｂ２　」") == "B"


def test_nar_subclass_returns_c_for_meisho_matching_zenkaku_c():
    assert _eval_nar_subclass("nar", "30", "「　　　Ｃ２　」") == "C"


def test_nar_subclass_returns_3yo_for_meisho_with_fullwidth_3_sai():
    assert _eval_nar_subclass("nar", "30", "「３歳　　　　」") == "3YO"


def test_nar_subclass_returns_other_for_null_meisho():
    assert _eval_nar_subclass("nar", "30", None) == "other"


def test_nar_subclass_precedence_op_wins_over_a():
    assert _eval_nar_subclass("nar", "30", "「ＯＰ　　Ａ１」") == "OP"


# ---------------------------------------------------------------------------
# --realtime-odds argument parsing
# ---------------------------------------------------------------------------


def test_parse_args_realtime_odds_defaults_to_none() -> None:
    args = subject.parse_args(["--category", "nar"])
    assert args.realtime_odds is None


def test_parse_args_realtime_odds_accepts_path() -> None:
    args = subject.parse_args(["--realtime-odds", "/tmp/odds.parquet"])
    assert args.realtime_odds == Path("/tmp/odds.parquet")


# ---------------------------------------------------------------------------
# stage_realtime_odds_table — parquet load + row count
# ---------------------------------------------------------------------------


def _make_realtime_odds_parquet(tmp_path: Path) -> Path:
    """Write a minimal realtime-odds parquet to tmp_path and return the path."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {
            "keibajo_code": pa.array(["44", "44"], type=pa.string()),
            "race_bango": pa.array(["01", "01"], type=pa.string()),
            "umaban": pa.array([1, 2], type=pa.int32()),
            "tansho_odds_realtime": pa.array([7.3, 12.5], type=pa.float64()),
            "ninkijun_realtime": pa.array([1, 2], type=pa.int32()),
            "bataiju_realtime": pa.array([447, None], type=pa.int32()),
        }
    )
    path = tmp_path / "realtime_odds.parquet"
    pq.write_table(table, str(path))
    return path


def test_stage_realtime_odds_table_loads_parquet(tmp_path: Path) -> None:
    import duckdb

    con = duckdb.connect()
    path = _make_realtime_odds_parquet(tmp_path)
    rc = subject.stage_realtime_odds_table(con, path)
    assert rc == 2
    rows = con.execute(
        "select keibajo_code, race_bango, umaban, tansho_odds_realtime, ninkijun_realtime, bataiju_realtime"
        f" from {subject.REALTIME_ODDS_TABLE} order by umaban"
    ).fetchall()
    assert rows == [("44", "01", 1, 7.3, 1, 447), ("44", "01", 2, 12.5, 2, None)]


def test_stage_realtime_odds_table_empty_parquet_returns_zero(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import duckdb

    table = pa.table(
        {
            "keibajo_code": pa.array([], type=pa.string()),
            "race_bango": pa.array([], type=pa.string()),
            "umaban": pa.array([], type=pa.int32()),
            "tansho_odds_realtime": pa.array([], type=pa.float64()),
            "ninkijun_realtime": pa.array([], type=pa.int32()),
            "bataiju_realtime": pa.array([], type=pa.int32()),
        }
    )
    path = tmp_path / "empty.parquet"
    pq.write_table(table, str(path))
    con = duckdb.connect()
    rc = subject.stage_realtime_odds_table(con, path)
    assert rc == 0


def test_create_empty_realtime_odds_stub_creates_zero_row_table() -> None:
    import duckdb

    con = duckdb.connect()
    subject.create_empty_realtime_odds_stub(con)
    rc_row = con.execute(f"select count(*) from {subject.REALTIME_ODDS_TABLE}").fetchone()
    assert rc_row is not None
    assert rc_row[0] == 0
    # Verify bataiju_realtime column is present so COALESCE references resolve.
    cols = con.execute(
        f"select column_name from information_schema.columns"
        f" where table_name = '{subject.REALTIME_ODDS_TABLE}'"
    ).fetchall()
    col_names = [c[0] for c in cols]
    assert "bataiju_realtime" in col_names


# ---------------------------------------------------------------------------
# stage_source_tables — realtime_odds_path routing
# ---------------------------------------------------------------------------


def test_stage_source_tables_creates_stub_when_no_realtime_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When realtime_odds_path=None a zero-row stub table is created (no error)."""
    import duckdb

    con = duckdb.connect()

    # Stub out the PG-reading stage calls so we exercise only the realtime
    # table setup without needing a real Postgres.
    monkeypatch.setattr(subject, "install_and_attach_pg", lambda *_: None)
    monkeypatch.setattr(subject, "stage_rec_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_se_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_um_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_ra_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "_stage_empty_jra_stubs", lambda _: None)

    subject.stage_source_tables(con, "20260610", "20260610", "nar", None, None)

    rc_row = con.execute(f"select count(*) from {subject.REALTIME_ODDS_TABLE}").fetchone()
    assert rc_row is not None
    assert rc_row[0] == 0


def test_stage_source_tables_loads_realtime_odds_when_path_provided(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When realtime_odds_path is set stage_realtime_odds_table is called."""
    import duckdb

    con = duckdb.connect()
    path = _make_realtime_odds_parquet(tmp_path)

    monkeypatch.setattr(subject, "install_and_attach_pg", lambda *_: None)
    monkeypatch.setattr(subject, "stage_rec_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_se_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_um_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_ra_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "_stage_empty_jra_stubs", lambda _: None)

    subject.stage_source_tables(con, "20260610", "20260610", "nar", None, path)

    rc_row = con.execute(f"select count(*) from {subject.REALTIME_ODDS_TABLE}").fetchone()
    assert rc_row is not None
    assert rc_row[0] == 2


# ---------------------------------------------------------------------------
# COALESCE behaviour: realtime first, nvd_se fallback, NULL when both absent
# ---------------------------------------------------------------------------


def _eval_coalesce_in_duckdb(
    se_tansho_odds: str | None,
    se_tansho_ninkijun: str | None,
    rt_odds: float | None,
    rt_rank: int | None,
    se_bataiju: str | None = None,
    rt_bataiju: int | None = None,
) -> tuple[float | None, int | None, int | None]:
    """Run the COALESCE SQL used in _rec_select_from_se_ra via in-memory DuckDB."""
    import duckdb

    con = duckdb.connect()
    # Replicate the realtime_odds_rt stub with one row including bataiju_realtime.
    con.execute(
        f"""
        create temp table {subject.REALTIME_ODDS_TABLE} as
        select
          cast(?::varchar as varchar) as keibajo_code,
          cast(?::varchar as varchar) as race_bango,
          cast(?::int as int) as umaban,
          cast(?::double as double) as tansho_odds_realtime,
          cast(?::int as int) as ninkijun_realtime,
          cast(?::int as int) as bataiju_realtime
        """,
        ("44", "01", 1, rt_odds, rt_rank, rt_bataiju),
    )
    # Replicate the COALESCE expressions from _rec_select_from_se_ra.
    rows = con.execute(
        f"""
        select
          coalesce(
            rt.tansho_odds_realtime,
            try_cast(nullif(trim(?::varchar), '') as double) / 10
          ) as tansho_odds,
          coalesce(
            rt.ninkijun_realtime,
            try_cast(nullif(trim(?::varchar), '') as int)
          ) as tansho_ninkijun,
          coalesce(
            rt.bataiju_realtime,
            try_cast(nullif(trim(?::varchar), '') as int)
          ) as bataiju
        from (select 1 as umaban, '44' as keibajo_code, '01' as race_bango) se
        left join {subject.REALTIME_ODDS_TABLE} rt
          on rt.keibajo_code = se.keibajo_code
          and rt.race_bango = se.race_bango
          and rt.umaban = se.umaban
        """,
        (se_tansho_odds, se_tansho_ninkijun, se_bataiju),
    ).fetchone()
    assert rows is not None
    return rows[0], rows[1], rows[2]


def test_coalesce_uses_realtime_odds_when_present() -> None:
    # Realtime = 7.3x (rank 1); nvd_se has stale '0000' (NULL → fallback stays 7.3)
    odds, rank, _ = _eval_coalesce_in_duckdb("0000", "00", 7.3, 1)
    assert odds == pytest.approx(7.3)
    assert rank == 1


def test_coalesce_falls_back_to_se_when_realtime_absent() -> None:
    # No realtime row for this horse → rt cols are NULL → se '0073' / 10 = 7.3
    odds, rank, _ = _eval_coalesce_in_duckdb("0073", "01", None, None)
    assert odds == pytest.approx(7.3)
    assert rank == 1


def test_coalesce_returns_zero_from_se_when_both_realtime_absent_and_se_all_zeros() -> None:
    # No realtime AND se has '0000' (JV-Link "no odds" sentinel = 0.0 after /10).
    # The COALESCE itself produces 0.0; the downstream legacy_five_cte formula
    # guards with ``odds_value > 0`` so odds_score is still NULL at feature time.
    odds, rank, _ = _eval_coalesce_in_duckdb("0000", None, None, None)
    assert odds == pytest.approx(0.0)
    assert rank is None


def test_coalesce_returns_null_when_se_is_empty_string_and_realtime_absent() -> None:
    # Truly absent odds in se → empty/NULL → COALESCE yields NULL.
    odds, rank, _ = _eval_coalesce_in_duckdb("", None, None, None)
    assert odds is None
    assert rank is None


def test_coalesce_units_realtime_is_direct_multiplier() -> None:
    # Realtime 1.4x → odds_score = ln(1.4)/ln(300) ≈ 0.059. Verify units are
    # NOT divided by 10 (which would give 0.14x → different score).
    import math

    odds, _, _ = _eval_coalesce_in_duckdb("0014", "01", 1.4, 1)
    assert odds == pytest.approx(1.4)
    # odds_score formula: ln(max(odds,1))/ln(300), clamped [0,1]
    expected_score = math.log(max(1.4, 1.0)) / math.log(300)
    assert expected_score == pytest.approx(math.log(1.4) / math.log(300))


def test_coalesce_partial_realtime_coverage_uses_realtime_for_present_horses() -> None:
    # Realtime covers umaban=1 but not umaban=2.
    import duckdb

    con = duckdb.connect()
    con.execute(
        f"""
        create temp table {subject.REALTIME_ODDS_TABLE} as
        select * from (
          values
            ('44'::varchar, '01'::varchar, 1::int, 7.3::double, 1::int, 447::int)
        ) t(keibajo_code, race_bango, umaban, tansho_odds_realtime, ninkijun_realtime, bataiju_realtime)
        """
    )
    # Horse 1: realtime present
    row1 = con.execute(
        f"""
        select coalesce(rt.tansho_odds_realtime,
                        try_cast(nullif(trim('0073'), '') as double) / 10)
        from (select 1 as umaban, '44' as keibajo_code, '01' as race_bango) se
        left join {subject.REALTIME_ODDS_TABLE} rt
          on rt.keibajo_code = se.keibajo_code
          and rt.race_bango = se.race_bango
          and rt.umaban = se.umaban
        """
    ).fetchone()
    assert row1 is not None
    assert row1[0] == pytest.approx(7.3)

    # Horse 2: no realtime row → fallback '0125' / 10 = 12.5
    row2 = con.execute(
        f"""
        select coalesce(rt.tansho_odds_realtime,
                        try_cast(nullif(trim('0125'), '') as double) / 10)
        from (select 2 as umaban, '44' as keibajo_code, '01' as race_bango) se
        left join {subject.REALTIME_ODDS_TABLE} rt
          on rt.keibajo_code = se.keibajo_code
          and rt.race_bango = se.race_bango
          and rt.umaban = se.umaban
        """
    ).fetchone()
    assert row2 is not None
    assert row2[0] == pytest.approx(12.5)


# ---------------------------------------------------------------------------
# COALESCE bataiju: realtime_realtime first, nvd_se fallback, NULL when both absent
# ---------------------------------------------------------------------------


def test_coalesce_bataiju_uses_realtime_when_present() -> None:
    _, _, bataiju = _eval_coalesce_in_duckdb("0000", "00", 7.3, 1, "450", 447)
    assert bataiju == 447


def test_coalesce_bataiju_falls_back_to_se_when_realtime_absent() -> None:
    _, _, bataiju = _eval_coalesce_in_duckdb("0073", "01", None, None, "450", None)
    assert bataiju == 450


def test_coalesce_bataiju_returns_null_when_both_absent() -> None:
    _, _, bataiju = _eval_coalesce_in_duckdb("0073", "01", None, None, "", None)
    assert bataiju is None


def test_coalesce_bataiju_realtime_overrides_se_value() -> None:
    # Realtime weight (490) should override stale se weight (450).
    _, _, bataiju = _eval_coalesce_in_duckdb("0073", "01", 7.3, 1, "450", 490)
    assert bataiju == 490


# ---------------------------------------------------------------------------
# Fix 0a-2: stage_um_table and stage_ra_table must be called exactly once for JRA
# ---------------------------------------------------------------------------


def test_stage_source_tables_calls_stage_um_table_exactly_once_for_jra(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Duplicate-staging bug fix: jra_um must be staged only once per call."""
    import duckdb

    con = duckdb.connect()
    um_calls: list[tuple[object, ...]] = []

    def capture_um(*args: object, **_kwargs: object) -> None:
        um_calls.append(args)

    monkeypatch.setattr(subject, "install_and_attach_pg", lambda *_: None)
    monkeypatch.setattr(subject, "stage_rec_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_se_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_um_table", capture_um)
    monkeypatch.setattr(subject, "stage_ra_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "_stage_empty_jra_stubs", lambda _: None)

    subject.stage_source_tables(con, "20260610", "20260610", "jra", None, None)

    jra_um_calls = [c for c in um_calls if len(c) >= 3 and c[2] == "jra_um"]
    assert len(jra_um_calls) == 1


def test_stage_source_tables_calls_stage_ra_table_exactly_once_for_jra(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Duplicate-staging bug fix: jra_ra must be staged only once per call."""
    import duckdb

    con = duckdb.connect()
    ra_calls: list[tuple[object, ...]] = []

    def capture_ra(*args: object, **_kwargs: object) -> None:
        ra_calls.append(args)

    monkeypatch.setattr(subject, "install_and_attach_pg", lambda *_: None)
    monkeypatch.setattr(subject, "stage_rec_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_se_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_um_table", lambda *_, **__: None)
    monkeypatch.setattr(subject, "stage_ra_table", capture_ra)
    monkeypatch.setattr(subject, "_stage_empty_jra_stubs", lambda _: None)

    subject.stage_source_tables(con, "20260610", "20260610", "jra", None, None)

    jra_ra_calls = [c for c in ra_calls if len(c) >= 3 and c[2] == "jra_ra"]
    assert len(jra_ra_calls) == 1


# ---------------------------------------------------------------------------
# Fix 0b: legacy_five_cte — popularity_score / odds_score median fallback
# ---------------------------------------------------------------------------


def _eval_legacy_scores_in_duckdb(
    ninkijun: int | None,
    odds_value: float | None,
    runner_count: int,
    category: str,
) -> tuple[float | None, float | None]:
    """Evaluate popularity_score and odds_score from legacy_five_cte SQL.

    Exercises the COALESCE median fallback by setting up a minimal DuckDB
    environment with a single-row legacy_target and empty legacy_horse_avg.
    """
    import duckdb
    import math

    con = duckdb.connect()

    # Build the CTE text for the given category.
    cte_text = subject.legacy_five_cte("true", category)

    # horse_history_base: one row with all needed columns (no history → avg = NULL).
    con.execute(
        """
        create temp table horse_history_base as
        select
          'jra'::varchar as source,
          '2026'::varchar as kaisai_nen,
          '0101'::varchar as kaisai_tsukihi,
          '01'::varchar as keibajo_code,
          '01'::varchar as race_bango,
          '0000000001'::varchar as ketto_toroku_bango,
          0.5::double as finish_norm,
          1::int as recent_rank
        limit 0
        """
    )
    # target: one row for the horse being scored.
    con.execute(
        """
        create temp table target as
        select
          'jra'::varchar as source,
          '2026'::varchar as kaisai_nen,
          '0101'::varchar as kaisai_tsukihi,
          '01'::varchar as keibajo_code,
          '01'::varchar as race_bango,
          '0000000001'::varchar as ketto_toroku_bango
        """
    )
    ninkijun_val = f"{ninkijun}::int" if ninkijun is not None else "null::int"
    odds_val = f"{odds_value}::double" if odds_value is not None else "null::double"
    runner_val = f"{runner_count}::int"
    # rec: single row supplying tansho_ninkijun, tansho_odds, shusso_tosu.
    con.execute(
        f"""
        create temp table rec as
        select
          'jra'::varchar as source,
          '2026'::varchar as kaisai_nen,
          '0101'::varchar as kaisai_tsukihi,
          '01'::varchar as keibajo_code,
          '01'::varchar as race_bango,
          '0000000001'::varchar as ketto_toroku_bango,
          {ninkijun_val} as tansho_ninkijun,
          {odds_val} as tansho_odds,
          {runner_val} as shusso_tosu
        """
    )
    row = con.execute(
        f"with {cte_text} select popularity_score, odds_score from legacy_features"
    ).fetchone()
    assert row is not None
    pop: float | None = row[0]
    odds: float | None = row[1]
    _ = math  # suppress unused import warning
    return pop, odds


def test_legacy_five_cte_popularity_score_uses_computed_value_when_present_jra() -> None:
    # ninkijun=1 out of 8 runners → score = (1-1)/(8-1) = 0.0
    pop, _ = _eval_legacy_scores_in_duckdb(1, 5.0, 8, subject.CATEGORY_JRA)
    assert pop == pytest.approx(0.0)


def test_legacy_five_cte_popularity_score_uses_median_fallback_when_null_jra() -> None:
    # ninkijun=NULL (odds not yet posted) → COALESCE yields JRA median
    pop, _ = _eval_legacy_scores_in_duckdb(None, None, 8, subject.CATEGORY_JRA)
    assert pop == pytest.approx(subject.POPULARITY_SCORE_MEDIAN_JRA)


def test_legacy_five_cte_odds_score_uses_computed_value_when_present_jra() -> None:
    import math

    # odds_value=5.0 → score = ln(5)/ln(300) ≈ 0.356
    _, odds = _eval_legacy_scores_in_duckdb(1, 5.0, 8, subject.CATEGORY_JRA)
    expected = math.log(5.0) / math.log(300.0)
    assert odds == pytest.approx(expected)


def test_legacy_five_cte_odds_score_uses_median_fallback_when_null_jra() -> None:
    # odds_value=NULL → COALESCE yields JRA median
    _, odds = _eval_legacy_scores_in_duckdb(None, None, 8, subject.CATEGORY_JRA)
    assert odds == pytest.approx(subject.ODDS_SCORE_MEDIAN_JRA)


def test_legacy_five_cte_popularity_score_uses_median_fallback_when_null_nar() -> None:
    # ninkijun=NULL for NAR race → COALESCE yields NAR median
    pop, _ = _eval_legacy_scores_in_duckdb(None, None, 8, subject.CATEGORY_NAR)
    assert pop == pytest.approx(subject.POPULARITY_SCORE_MEDIAN_NAR)


def test_legacy_five_cte_odds_score_uses_median_fallback_when_null_nar() -> None:
    # odds_value=NULL for NAR race → COALESCE yields NAR median
    _, odds = _eval_legacy_scores_in_duckdb(None, None, 8, subject.CATEGORY_NAR)
    assert odds == pytest.approx(subject.ODDS_SCORE_MEDIAN_NAR)


def test_legacy_five_cte_odds_score_uses_median_fallback_when_null_banei() -> None:
    # Ban-ei shares NAR medians
    _, odds = _eval_legacy_scores_in_duckdb(None, None, 8, subject.CATEGORY_BAN_EI)
    assert odds == pytest.approx(subject.ODDS_SCORE_MEDIAN_NAR)


def test_legacy_five_cte_popularity_score_not_null_for_any_category() -> None:
    # Median fallback ensures popularity_score is never NULL even when ninkijun absent
    for cat in (subject.CATEGORY_JRA, subject.CATEGORY_NAR, subject.CATEGORY_BAN_EI):
        pop, _ = _eval_legacy_scores_in_duckdb(None, None, 8, cat)
        assert pop is not None


def test_legacy_five_cte_odds_score_not_null_for_any_category() -> None:
    # Median fallback ensures odds_score is never NULL even when odds absent
    for cat in (subject.CATEGORY_JRA, subject.CATEGORY_NAR, subject.CATEGORY_BAN_EI):
        _, odds = _eval_legacy_scores_in_duckdb(None, None, 8, cat)
        assert odds is not None


# ---------------------------------------------------------------------------
# Fix 0b: _build_per_year_specs — legacy_features cte_builder uses correct category
# ---------------------------------------------------------------------------


def test_build_per_year_specs_legacy_features_uses_jra_median() -> None:
    specs = subject.build_per_year_specs(subject.CATEGORY_JRA)
    legacy_spec = next(s for s in specs if s["name"] == "legacy_features")
    cte_text = legacy_spec["cte_builder"]("true")
    assert str(subject.ODDS_SCORE_MEDIAN_JRA) in cte_text


def test_build_per_year_specs_legacy_features_uses_nar_median() -> None:
    specs = subject.build_per_year_specs(subject.CATEGORY_NAR)
    legacy_spec = next(s for s in specs if s["name"] == "legacy_features")
    cte_text = legacy_spec["cte_builder"]("true")
    assert str(subject.ODDS_SCORE_MEDIAN_NAR) in cte_text


# ---------------------------------------------------------------------------
# Fix B001: Ban-ei historical builder uses double-nullif for '00' DQ rows
# ---------------------------------------------------------------------------


def test_build_rec_select_sql_ban_ei_uses_double_nullif_for_finish_position() -> None:
    """B001: Ban-ei rec SQL must use double nullif so '00' → NULL (not 0)."""
    sql = subject.build_rec_select_sql("ban-ei", "20160101", "20251231", None)
    assert "nullif(nullif(trim(se.kakutei_chakujun), ''), '00') as int) as finish_position" in sql


def test_build_rec_select_sql_ban_ei_uses_double_nullif_in_finish_norm_case() -> None:
    """B001: finish_norm CASE expression must also exclude '00' DQ rows.

    finish_position (1) + CASE when-condition (1) + CASE then-expression (1) = 3 total.
    """
    sql = subject.build_rec_select_sql("ban-ei", "20160101", "20251231", None)
    # finish_position line + two CASE expression references = 3 total occurrences.
    assert sql.count("nullif(nullif(trim(se.kakutei_chakujun), ''), '00')") == 3


def test_ban_ei_double_nullif_drops_00_finish_position_via_duckdb() -> None:
    """B001: DuckDB confirms try_cast('00' via single-nullif)=0 and double-nullif=NULL."""
    import duckdb

    con = duckdb.connect()
    # Single nullif: '00' is not '' so passes through, try_cast('00' as int) = 0.
    single_row = con.execute(
        "select try_cast(nullif(trim('00'), '') as int)"
    ).fetchone()
    assert single_row is not None
    assert single_row[0] == 0  # This was the bug: 0 leaks into training.

    # Double nullif: '00' is explicitly excluded → NULL → filtered by IS NOT NULL.
    double_row = con.execute(
        "select try_cast(nullif(nullif(trim('00'), ''), '00') as int)"
    ).fetchone()
    assert double_row is not None
    assert double_row[0] is None  # Fix: NULL → row excluded from training.


def test_ban_ei_double_nullif_preserves_valid_finish_positions_via_duckdb() -> None:
    """B001: valid positions like '01', '02', '16' are unaffected by the fix."""
    import duckdb

    con = duckdb.connect()
    rows = con.execute(
        """
        select
          val,
          try_cast(nullif(nullif(trim(val), ''), '00') as int) as fixed_pos
        from (values ('01'), ('02'), ('16'), ('  3  ')) t(val)
        order by val
        """
    ).fetchall()
    assert rows == [("  3  ", 3), ("01", 1), ("02", 2), ("16", 16)]


# ---------------------------------------------------------------------------
# Fix F001: NAR 2YO / 3YO subclass arms
# ---------------------------------------------------------------------------


def test_nar_subclass_constants_2yo_and_3yo_defined() -> None:
    assert subject.NAR_SUBCLASS_2YO == "2YO"
    assert subject.NAR_SUBCLASS_3YO == "3YO"


def test_nar_subclass_returns_2yo_for_fullwidth_2_sai() -> None:
    assert _eval_nar_subclass("nar", "30", "２歳　　　　　　　　　") == "2YO"


def test_nar_subclass_returns_3yo_for_fullwidth_3_sai() -> None:
    assert _eval_nar_subclass("nar", "30", "３歳　　　　　　　　　") == "3YO"


def test_nar_subclass_returns_3yo_for_3sai_with_dash_variant() -> None:
    # e.g. "３歳　　　－３　　　　"
    assert _eval_nar_subclass("nar", "30", "３歳　　　－３　　　　") == "3YO"


def test_nar_subclass_returns_2yo_for_ascii_2_sai() -> None:
    # Halfwidth "2歳" variant should also match.
    assert _eval_nar_subclass("nar", "30", "2歳　　　　　　　　　") == "2YO"


def test_nar_subclass_returns_3yo_for_ascii_3_sai() -> None:
    assert _eval_nar_subclass("nar", "30", "3歳　　　　　　　　　") == "3YO"


def test_nar_subclass_2yo_does_not_match_ban_ei_row() -> None:
    # Ban-ei (keibajo '83') must still return NULL regardless of meisho.
    assert _eval_nar_subclass("nar", "83", "２歳　　　　") is None


def test_nar_subclass_2yo_does_not_match_jra_row() -> None:
    assert _eval_nar_subclass("jra", "05", "２歳　　　　") is None


def test_nar_subclass_op_still_takes_precedence_over_age_arms() -> None:
    # Hypothetical meisho containing both "ＯＰ" and "２歳" — OP wins (higher in CASE).
    assert _eval_nar_subclass("nar", "30", "ＯＰ２歳") == "OP"


def test_nar_subclass_other_still_routes_non_age_non_class_meisho() -> None:
    # A meisho that genuinely has no class / age marker falls to 'other'.
    assert _eval_nar_subclass("nar", "30", "　　　　　　　　　　　") == "other"


def test_nar_subclass_2yo_3yo_route_to_global_fallback_without_ensemble() -> None:
    """F001 backward-safety: 2YO/3YO without a registered per-class ensemble should
    route the same as 'other' today — both fall through to the global model.
    This test verifies that NAR_SUBCLASS_2YO and NAR_SUBCLASS_3YO are recognised
    values in NAR_NAMED_CLASSES (not unknown tokens) so routing logic can handle them.
    """
    assert subject.NAR_SUBCLASS_2YO in subject.NAR_NAMED_CLASSES
    assert subject.NAR_SUBCLASS_3YO in subject.NAR_NAMED_CLASSES


# ---------------------------------------------------------------------------
# Bug 1 fix — build_target_table and base_features_select_sql must emit
# tansho_odds and tansho_ninkijun so the market-signal post-processor layer
# can read them from the base-build parquet without a BinderException.
# ---------------------------------------------------------------------------


def test_base_features_select_sql_includes_tansho_odds_and_ninkijun() -> None:
    """base_features_select_sql() must reference t.tansho_odds and
    t.tansho_ninkijun so the market-signal layer can read those columns from
    the output parquet without a BinderException.
    """
    sql = subject.base_features_select_sql("jra")
    assert "t.tansho_odds" in sql, "t.tansho_odds missing from base_features_select_sql"
    assert "t.tansho_ninkijun" in sql, "t.tansho_ninkijun missing from base_features_select_sql"


def test_build_target_table_emits_tansho_odds_and_ninkijun() -> None:
    """build_target_table() must project tansho_odds and tansho_ninkijun from
    rec into the target table so that base_features_select_sql can reference
    them via t.tansho_odds / t.tansho_ninkijun.

    This test catches any regression where the columns are removed from the
    target table definition, which would re-introduce Bug 1.
    """
    import duckdb

    con = duckdb.connect()
    con.execute(
        """
        create or replace temp table rec as
        select * from (
          values
            ('jra', '20260607', date '2026-06-07', '2026', '0607', '05', '11',
              'horse_a', 3, 'jockey_a', 'trainer_a',
              1600, '11', 'A', '99', 12, null::int, null::double,
              'name_a', 'fukudai_a',
              null::double, null::double, null::double, null::double,
              '1', '1', 2, 5.0, null::int, null::double, '1', 3),
            ('jra', '20260607', date '2026-06-07', '2026', '0607', '05', '11',
              'horse_b', 5, 'jockey_b', 'trainer_b',
              1600, '11', 'A', '99', 12, null::int, null::double,
              'name_b', 'fukudai_b',
              null::double, null::double, null::double, null::double,
              '1', '1', 1, 8.0, null::int, null::double, '2', 5)
        ) as v(
          source, race_date, race_dt, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, umaban, kishumei_ryakusho, chokyoshimei_ryakusho,
          kyori, track_code, grade_code, kyoso_joken_code, shusso_tosu,
          finish_position, finish_norm,
          kyosomei_hondai, kyosomei_fukudai,
          time_sa, kohan_3f, corner3_norm, corner4_norm,
          babajotai_code_shiba, babajotai_code_dirt,
          tansho_ninkijun, tansho_odds, bataiju, corner1_norm, seibetsu_code, barei
        )
        """
    )
    subject.build_target_table(con, "jra", "20260607", "20260607")
    col_names = [
        c[0]
        for c in con.execute("describe target").fetchall()
    ]
    assert "tansho_odds" in col_names, "tansho_odds not in target — market-signal layer will crash"
    assert "tansho_ninkijun" in col_names, "tansho_ninkijun not in target — market-signal layer will crash"
    # Values must be non-null and correct
    rows = con.execute(
        "select ketto_toroku_bango, tansho_odds, tansho_ninkijun from target order by ketto_toroku_bango"
    ).fetchall()
    assert rows[0] == ("horse_a", 5.0, 2)
    assert rows[1] == ("horse_b", 8.0, 1)
    con.close()


def test_category_source_filter_nar_includes_null_keibajo() -> None:
    """NAR filter must include rows with NULL keibajo_code (not just non-'83' rows).

    `keibajo_code <> '83'` yields NULL (not TRUE) when keibajo_code IS NULL under
    SQL three-valued logic, silently dropping valid NAR rows with missing keibajo.
    """
    sql = subject.category_source_filter("nar", "t")
    assert "keibajo_code is null" in sql.lower() or "is null" in sql.lower(), (
        "NAR filter does not handle NULL keibajo_code — rows with missing keibajo are silently dropped"
    )
    import duckdb
    con = duckdb.connect()
    con.execute(
        """
        create or replace temp table t as
        select * from (values
          ('nar', null::varchar),
          ('nar', '83'),
          ('nar', '01')
        ) as v(source, keibajo_code)
        """
    )
    rows = con.execute(
        f"select source, keibajo_code from t where {sql}"
    ).fetchall()
    keibajo_codes = [r[1] for r in rows]
    assert None in keibajo_codes, "NULL keibajo_code NAR row must be included in NAR filter"
    assert "83" not in keibajo_codes, "keibajo_code='83' (ban-ei) must be excluded from NAR filter"
    assert "01" in keibajo_codes, "Non-ban-ei NAR row must be included"
    con.close()


def test_pedigree_score_is_null_when_all_components_missing() -> None:
    """pedigree_score_for_race must be NULL (not 0) when all three pedigree components lack data."""
    sql = subject.base_features_select_sql("jra")
    assert "pedigree_score_for_race" in sql
    # Dynamic divisor pattern: nullif(..., 0)::double should appear for pedigree score
    assert "nullif(" in sql.lower(), "pedigree_score_for_race must use dynamic divisor via NULLIF"


def test_pedigree_score_uses_available_component_count_as_divisor() -> None:
    """When only 1 of 3 pedigree components has data, the score must equal that component's value.

    Dividing by the fixed constant 3 would give sire_distance_win_rate / 3 ≈ 0.04
    instead of the true score 0.12, systematically underestimating pedigree strength.
    """
    sql = subject.base_features_select_sql("jra")
    # The fixed-divisor pattern (/ 3::double) must NOT appear in the pedigree score expression.
    # Instead, the count of non-null components must be the divisor.
    import re
    # Extract the pedigree_score_for_race expression from the SQL
    m = re.search(
        r"([\s\S]+?)\s+as\s+pedigree_score_for_race",
        sql,
        re.IGNORECASE,
    )
    assert m is not None, "pedigree_score_for_race expression not found in SQL"
    expr = m.group(1)
    # Must NOT use the constant 3 as a bare divisor (fixed divisor)
    assert f"/ {subject.PEDIGREE_COMPOSITE_DIVISOR}::" not in expr, (
        "pedigree_score_for_race still uses fixed divisor — partial-data scores will be underestimated"
    )


def test_sire_running_style_stats_uses_corner_count_as_denominator() -> None:
    """Nige/senkou/sashi/oikomi rates must divide by corner1_norm_count, not race_count.

    race_count = count(*) includes races with NULL corner1_norm (no corner data recorded).
    Dividing style counts by race_count inflates the denominator and systematically
    underestimates running-style rates for sires whose offspring have incomplete corner data.
    """
    srs_spec = next(
        s for s in subject.PEDIGREE_STAT_SPECS if s["table"] == "sire_running_style_stats"
    )
    accum = srs_spec["accum_metrics_select"]
    # Each rate must use corner1_norm_count, not race_count, as denominator
    for metric in ("nige", "senkou", "sashi", "oikomi"):
        assert f"sum(m.{metric}_count)::double / nullif(sum(m.corner1_norm_count), 0)" in accum, (
            f"sire_{metric}_rate_val uses race_count denominator — NULL-corner races inflate denominator"
        )


# ---------------------------------------------------------------------------
# sire_keibajo_stats / damsire_keibajo_stats — keibajo_code-bucketed pedigree
# win rates (Signal4 venue-aware pedigree features)
# ---------------------------------------------------------------------------


def test_pedigree_stat_specs_includes_sire_keibajo_stats() -> None:
    sks_spec = next(
        s for s in subject.PEDIGREE_STAT_SPECS if s["table"] == "sire_keibajo_stats"
    )
    assert sks_spec["key_column"] == "ketto_joho_01b"
    assert sks_spec["key_alias"] == "sire"
    assert sks_spec["bucket_expr"] == "keibajo_code"
    assert sks_spec["bucket_alias"] == "keibajo_code"
    assert (
        sks_spec["monthly_metrics_select"]
        == "sum(case when finish_position = 1 then 1 else 0 end) as win_count"
    )
    assert (
        sks_spec["accum_metrics_select"]
        == "sum(m.win_count)::double / nullif(sum(m.race_count), 0) as sire_keibajo_win_rate_val"
    )


def test_pedigree_stat_specs_includes_damsire_keibajo_stats() -> None:
    dks_spec = next(
        s for s in subject.PEDIGREE_STAT_SPECS if s["table"] == "damsire_keibajo_stats"
    )
    assert dks_spec["key_column"] == "ketto_joho_05b"
    assert dks_spec["key_alias"] == "damsire"
    assert dks_spec["bucket_expr"] == "keibajo_code"
    assert dks_spec["bucket_alias"] == "keibajo_code"
    assert (
        dks_spec["monthly_metrics_select"]
        == "sum(case when finish_position = 1 then 1 else 0 end) as win_count"
    )
    assert (
        dks_spec["accum_metrics_select"]
        == "sum(m.win_count)::double / nullif(sum(m.race_count), 0) as damsire_keibajo_win_rate_val"
    )


def test_pedigree_monthly_stat_sql_sire_keibajo() -> None:
    sks_spec = next(
        s for s in subject.PEDIGREE_STAT_SPECS if s["table"] == "sire_keibajo_stats"
    )
    sql = subject.pedigree_monthly_stat_sql(sks_spec)
    assert "create or replace temp table sire_keibajo_stats as" in sql
    assert "ketto_joho_01b as sire" in sql
    assert "keibajo_code as keibajo_code" in sql
    assert "sum(case when finish_position = 1 then 1 else 0 end) as win_count" in sql
    assert "c.cum_win_count::double / nullif(c.cum_race_count, 0) as sire_keibajo_win_rate_val" in sql
    assert "from pedigree_rec_um" in sql


def test_pedigree_monthly_stat_sql_damsire_keibajo() -> None:
    dks_spec = next(
        s for s in subject.PEDIGREE_STAT_SPECS if s["table"] == "damsire_keibajo_stats"
    )
    sql = subject.pedigree_monthly_stat_sql(dks_spec)
    assert "create or replace temp table damsire_keibajo_stats as" in sql
    assert "ketto_joho_05b as damsire" in sql
    assert "keibajo_code as keibajo_code" in sql
    assert "sum(case when finish_position = 1 then 1 else 0 end) as win_count" in sql
    assert "c.cum_win_count::double / nullif(c.cum_race_count, 0) as damsire_keibajo_win_rate_val" in sql
    assert "from pedigree_rec_um" in sql


def test_base_features_select_sql_includes_sire_keibajo_win_rate() -> None:
    sql = subject.base_features_select_sql("jra")
    assert "pf.sire_keibajo_win_rate_val else null end as sire_keibajo_win_rate" in sql


def test_base_features_select_sql_includes_damsire_keibajo_win_rate() -> None:
    sql = subject.base_features_select_sql("jra")
    assert "pf.damsire_keibajo_win_rate_val else null end as damsire_keibajo_win_rate" in sql


# ---------------------------------------------------------------------------
# Signal4 (additional coverage): PEDIGREE_STAT_TABLES registration,
# target_keibajo_code projection, min-races guard, and join clauses.
# ---------------------------------------------------------------------------


def test_pedigree_stat_tables_includes_both_keibajo_stats() -> None:
    assert "sire_keibajo_stats" in subject.PEDIGREE_STAT_TABLES
    assert "damsire_keibajo_stats" in subject.PEDIGREE_STAT_TABLES


def test_target_pedigree_sql_projects_target_keibajo_code() -> None:
    sql = subject.target_pedigree_sql()
    assert "t.keibajo_code as target_keibajo_code" in sql


def test_base_features_select_sql_guards_keibajo_win_rate_by_min_races() -> None:
    sql = subject.base_features_select_sql("jra")
    assert (
        f"case when pf.sks_race_count >= {subject.PEDIGREE_MIN_RACES} then pf.sire_keibajo_win_rate_val else null end as sire_keibajo_win_rate"
        in sql
    )
    assert (
        f"case when pf.dks_race_count >= {subject.PEDIGREE_MIN_RACES} then pf.damsire_keibajo_win_rate_val else null end as damsire_keibajo_win_rate"
        in sql
    )


def test_base_features_select_sql_joins_keibajo_stats_on_sire_and_keibajo() -> None:
    sql = subject.pedigree_features_sql()
    assert (
        "left join sire_keibajo_stats sks on sks.sire = tp.target_sire and sks.keibajo_code = tp.target_keibajo_code"
        in sql
    )
    assert (
        "left join damsire_keibajo_stats dks on dks.damsire = tp.target_damsire and dks.keibajo_code = tp.target_keibajo_code"
        in sql
    )


def test_spill_tables_lists_all_final_join_temp_tables() -> None:
    assert subject.SPILL_TABLES == (
        "target",
        "horse_career",
        "jockey_career",
        "trainer_career",
        "pedigree_features",
        "race_field_aggregates",
        "race_top3_speed",
        "track_bias",
        "weight_agg",
        "recent_form",
        "legacy_features",
        "weather_lookup",
        "horse_running_style_history",
    )


def test_spill_temp_tables_to_disk_replaces_tables_with_views_preserving_data(
    tmp_path: Path,
) -> None:
    import duckdb

    con = duckdb.connect()
    for spill_table in subject.SPILL_TABLES:
        con.execute(f"create table {spill_table} as select 1 as k, '{spill_table}' as label")
    subject.spill_temp_tables_to_disk(con, tmp_path)
    table_rows = con.execute(
        "select table_name from information_schema.tables where table_type = 'BASE TABLE'"
    ).fetchall()
    assert table_rows == []
    view_rows = con.execute(
        "select table_name from information_schema.tables where table_type = 'VIEW' order by table_name"
    ).fetchall()
    view_names = sorted(r[0] for r in view_rows)
    assert view_names == sorted(subject.SPILL_TABLES)
    track_bias_data = con.execute("select k, label from track_bias").fetchall()
    assert track_bias_data == [(1, "track_bias")]


def test_spill_temp_tables_to_disk_writes_parquet_into_temp_dir_subfolder(
    tmp_path: Path,
) -> None:
    import duckdb

    con = duckdb.connect()
    for spill_table in subject.SPILL_TABLES:
        con.execute(f"create table {spill_table} as select 42 as v")
    subject.spill_temp_tables_to_disk(con, tmp_path)
    written = sorted(p.name for p in (tmp_path / "table_spill").glob("*.parquet"))
    assert written == sorted(f"{t}.parquet" for t in subject.SPILL_TABLES)


def test_spill_temp_tables_to_disk_defaults_to_tmp_when_temp_dir_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import duckdb

    monkeypatch.setattr(subject, "Path", lambda _: tmp_path)
    con = duckdb.connect()
    for spill_table in subject.SPILL_TABLES:
        con.execute(f"create table {spill_table} as select 1 as v")
    subject.spill_temp_tables_to_disk(con, None)
    written = sorted(p.name for p in (tmp_path / "table_spill").glob("*.parquet"))
    assert written == sorted(f"{t}.parquet" for t in subject.SPILL_TABLES)


def test_parse_args_supports_venue_weather_dir(tmp_path: Path) -> None:
    args = subject.parse_args(["--venue-weather-dir", str(tmp_path / "vw")])
    assert args.venue_weather_dir == tmp_path / "vw"


def test_parse_args_venue_weather_dir_defaults_to_none() -> None:
    args = subject.parse_args([])
    assert args.venue_weather_dir is None


def test_venue_weather_files_for_years_only_returns_existing(tmp_path: Path) -> None:
    (tmp_path / "venue_weather_2020.duckdb").write_text("x")
    (tmp_path / "venue_weather_2022.duckdb").write_text("x")
    found = subject.venue_weather_files_for_years(tmp_path, [2020, 2021, 2022])
    assert found == [
        (2020, tmp_path / "venue_weather_2020.duckdb"),
        (2022, tmp_path / "venue_weather_2022.duckdb"),
    ]


def test_chunk_years_splits_into_batches() -> None:
    assert subject.chunk_years([2018, 2019, 2020, 2021, 2022], 2) == [
        [2018, 2019],
        [2020, 2021],
        [2022],
    ]


def test_chunk_years_batch_size_one_is_singletons() -> None:
    assert subject.chunk_years([2019, 2020], 1) == [[2019], [2020]]


def test_chunk_years_zero_batch_size_degrades_to_one() -> None:
    assert subject.chunk_years([2019, 2020], 0) == [[2019], [2020]]


def test_chunk_years_negative_batch_size_degrades_to_one() -> None:
    assert subject.chunk_years([2019, 2020], -3) == [[2019], [2020]]


def test_chunk_years_empty_list_returns_empty() -> None:
    assert subject.chunk_years([], 2) == []


def test_year_in_filter_builds_in_list() -> None:
    assert subject.year_in_filter([2019, 2020]) == "t.kaisai_nen in ('2019', '2020')"


def test_year_in_filter_single_year() -> None:
    assert subject.year_in_filter([2020]) == "t.kaisai_nen in ('2020')"


def test_venue_weather_empty_agg_sql_has_expected_columns() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(subject.venue_weather_empty_agg_sql())
    columns = [
        row[0] for row in con.execute("describe venue_weather_agg").fetchall()
    ]
    assert columns == [
        "keibajo_code",
        "weather_date",
        "weather_date_yyyymmdd",
        "venue_temperature",
        "venue_precipitation_total",
        "venue_wind_speed_max",
        "venue_wind_gusts_max",
    ]


def test_materialize_venue_weather_creates_empty_agg_when_dir_none() -> None:
    import duckdb

    con = duckdb.connect()
    subject.materialize_venue_weather(con, None, [2020])
    row = con.execute("select count(*) from venue_weather_agg").fetchone()
    assert row is not None
    assert row[0] == 0


def test_materialize_venue_weather_creates_empty_agg_when_no_matching_files(
    tmp_path: Path,
) -> None:
    import duckdb

    con = duckdb.connect()
    subject.materialize_venue_weather(con, tmp_path, [2099])
    row = con.execute("select count(*) from venue_weather_agg").fetchone()
    assert row is not None
    assert row[0] == 0


def test_materialize_venue_weather_aggregates_race_hours_from_attached_db(
    tmp_path: Path,
) -> None:
    import duckdb

    src_path = tmp_path / "venue_weather_2020.duckdb"
    src = duckdb.connect(str(src_path))
    src.execute(
        "create table venue_weather ("
        "keibajo_code varchar, weather_date date, weather_hour integer, "
        "temperature double, precipitation double, wind_speed double, wind_gusts double)"
    )
    src.execute(
        "insert into venue_weather values "
        "('01', date '2020-01-01', 8, 100.0, 100.0, 100.0, 100.0), "
        "('01', date '2020-01-01', 10, 10.0, 1.0, 5.0, 9.0), "
        "('01', date '2020-01-01', 12, 20.0, 2.0, 7.0, 11.0)"
    )
    src.close()
    con = duckdb.connect(":memory:")
    subject.materialize_venue_weather(con, tmp_path, [2020])
    row = con.execute(
        "select venue_temperature, venue_precipitation_total, "
        "venue_wind_speed_max, venue_wind_gusts_max from venue_weather_agg "
        "where keibajo_code = '01' and weather_date = date '2020-01-01'"
    ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(15.0)
    assert row[1] == pytest.approx(3.0)
    assert row[2] == pytest.approx(7.0)
    assert row[3] == pytest.approx(11.0)


def test_materialize_venue_weather_detaches_so_source_db_is_not_locked(
    tmp_path: Path,
) -> None:
    import duckdb

    src_path = tmp_path / "venue_weather_2020.duckdb"
    src = duckdb.connect(str(src_path))
    src.execute(
        "create table venue_weather ("
        "keibajo_code varchar, weather_date date, weather_hour integer, "
        "temperature double, precipitation double, wind_speed double, wind_gusts double)"
    )
    src.execute(
        "insert into venue_weather values ('01', date '2020-01-01', 10, 10.0, 1.0, 5.0, 9.0)"
    )
    src.close()
    con = duckdb.connect(":memory:")
    subject.materialize_venue_weather(con, tmp_path, [2020])
    attached = con.execute(
        "select database_name from duckdb_databases() where database_name like 'vw_%'"
    ).fetchall()
    assert attached == []


def test_spill_groups_partition_spill_tables_without_gaps_or_duplicates() -> None:
    groups = (
        subject.SPILL_AFTER_HORSE_HISTORY
        + subject.SPILL_AFTER_PARTNER
        + subject.SPILL_AFTER_PEDIGREE
        + subject.SPILL_AFTER_RACE_CONTEXT
        + subject.SPILL_AFTER_TRACK_BIAS
        + subject.SPILL_AFTER_WEATHER
        + subject.SPILL_BEFORE_PARQUET
    )
    assert sorted(groups) == sorted(subject.SPILL_TABLES)
    assert len(groups) == len(set(groups))


def test_spill_before_parquet_keeps_target_until_the_end() -> None:
    assert subject.SPILL_BEFORE_PARQUET == (
        "target",
        "weight_agg",
        "recent_form",
        "legacy_features",
    )


def test_spill_temp_tables_to_disk_only_spills_requested_subset(
    tmp_path: Path,
) -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("create table horse_career as select 1 as k")
    con.execute("create table target as select 2 as k")
    subject.spill_temp_tables_to_disk(con, tmp_path, ("horse_career",))
    base_tables = con.execute(
        "select table_name from information_schema.tables "
        "where table_type = 'BASE TABLE' order by table_name"
    ).fetchall()
    views = con.execute(
        "select table_name from information_schema.tables "
        "where table_type = 'VIEW' order by table_name"
    ).fetchall()
    assert [r[0] for r in base_tables] == ["target"]
    assert [r[0] for r in views] == ["horse_career"]
    written = sorted(p.name for p in (tmp_path / "table_spill").glob("*.parquet"))
    assert written == ["horse_career.parquet"]


def test_parse_args_resume_and_incremental_default_false() -> None:
    args = subject.parse_args([])
    assert args.resume is False
    assert args.incremental is False


def test_parse_args_supports_resume_flag() -> None:
    args = subject.parse_args(["--resume"])
    assert args.resume is True
    assert args.incremental is False


def test_parse_args_supports_incremental_flag() -> None:
    args = subject.parse_args(["--incremental"])
    assert args.resume is False
    assert args.incremental is True


def test_checkpoint_manifest_path_is_under_table_spill(tmp_path: Path) -> None:
    assert subject.CheckpointManifest.path(tmp_path) == tmp_path / "table_spill" / "checkpoint.json"


def test_checkpoint_manifest_load_returns_none_when_missing(tmp_path: Path) -> None:
    assert subject.CheckpointManifest.load(tmp_path) is None


def test_checkpoint_manifest_load_returns_none_on_corrupt_json(tmp_path: Path) -> None:
    manifest_path = subject.CheckpointManifest.path(tmp_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("{not valid json")
    assert subject.CheckpointManifest.load(tmp_path) is None


def test_checkpoint_manifest_load_returns_none_when_top_level_not_dict(tmp_path: Path) -> None:
    manifest_path = subject.CheckpointManifest.path(tmp_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("[1, 2, 3]")
    assert subject.CheckpointManifest.load(tmp_path) is None


def test_checkpoint_manifest_save_then_load_round_trips(tmp_path: Path) -> None:
    manifest = subject.CheckpointManifest(category="jra", from_date="20200101", to_date="20201231")
    manifest.stages["source"] = subject.StageCheckpoint(
        status="done",
        tables=["rec.parquet"],
        row_counts={"rec.parquet": 7},
        query_hash="abc",
        timestamp=12.5,
    )
    manifest.save(tmp_path)
    loaded = subject.CheckpointManifest.load(tmp_path)
    assert loaded is not None
    assert loaded.category == "jra"
    assert loaded.from_date == "20200101"
    assert loaded.stages["source"].tables == ["rec.parquet"]
    assert loaded.stages["source"].row_counts == {"rec.parquet": 7}
    assert loaded.stages["source"].query_hash == "abc"


def test_checkpoint_manifest_save_is_atomic_no_tmp_left_behind(tmp_path: Path) -> None:
    manifest = subject.CheckpointManifest(category="nar")
    manifest.save(tmp_path)
    leftovers = sorted(p.name for p in (tmp_path / "table_spill").glob("*.tmp"))
    assert leftovers == []


def test_checkpoint_manifest_load_ignores_non_dict_stages(tmp_path: Path) -> None:
    manifest_path = subject.CheckpointManifest.path(tmp_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"version": 1, "stages": ["not", "a", "dict"]}))
    loaded = subject.CheckpointManifest.load(tmp_path)
    assert loaded is not None
    assert loaded.stages == {}


def test_checkpoint_manifest_load_skips_non_dict_stage_payload(tmp_path: Path) -> None:
    manifest_path = subject.CheckpointManifest.path(tmp_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps({"version": 1, "stages": {"source": "bogus", "target": {"status": "done"}}})
    )
    loaded = subject.CheckpointManifest.load(tmp_path)
    assert loaded is not None
    assert "source" not in loaded.stages
    assert loaded.stages["target"].status == "done"


def test_checkpoint_manifest_mark_done_writes_stage_and_persists(tmp_path: Path) -> None:
    manifest = subject.CheckpointManifest()
    manifest.mark_done("source", ["rec.parquet"], {"rec.parquet": 3}, "hash1", tmp_path)
    reloaded = subject.CheckpointManifest.load(tmp_path)
    assert reloaded is not None
    assert reloaded.stages["source"].status == "done"
    assert reloaded.stages["source"].query_hash == "hash1"


def test_checkpoint_manifest_invalidate_removes_stage_and_persists(tmp_path: Path) -> None:
    manifest = subject.CheckpointManifest()
    manifest.mark_done("source", ["rec.parquet"], {"rec.parquet": 3}, "hash1", tmp_path)
    manifest.invalidate("source", tmp_path)
    reloaded = subject.CheckpointManifest.load(tmp_path)
    assert reloaded is not None
    assert "source" not in reloaded.stages


def test_checkpoint_manifest_invalidate_absent_stage_is_noop(tmp_path: Path) -> None:
    manifest = subject.CheckpointManifest()
    manifest.invalidate("never_recorded", tmp_path)
    assert subject.CheckpointManifest.load(tmp_path) is None


def test_is_stage_valid_false_when_stage_absent(tmp_path: Path) -> None:
    manifest = subject.CheckpointManifest()
    assert manifest.is_stage_valid("source", "hash1", tmp_path) is False


def test_is_stage_valid_false_when_status_not_done(tmp_path: Path) -> None:
    manifest = subject.CheckpointManifest()
    manifest.stages["source"] = subject.StageCheckpoint(
        status="pending", tables=[], row_counts={}, query_hash="hash1", timestamp=1.0
    )
    assert manifest.is_stage_valid("source", "hash1", tmp_path) is False


def test_is_stage_valid_false_when_hash_mismatch(tmp_path: Path) -> None:
    (tmp_path / "rec.parquet").write_text("x")
    manifest = subject.CheckpointManifest()
    manifest.stages["source"] = subject.StageCheckpoint(
        status="done", tables=["rec.parquet"], row_counts={}, query_hash="old", timestamp=1.0
    )
    assert manifest.is_stage_valid("source", "new", tmp_path) is False


def test_is_stage_valid_false_when_parquet_missing(tmp_path: Path) -> None:
    manifest = subject.CheckpointManifest()
    manifest.stages["source"] = subject.StageCheckpoint(
        status="done", tables=["rec.parquet"], row_counts={}, query_hash="hash1", timestamp=1.0
    )
    assert manifest.is_stage_valid("source", "hash1", tmp_path) is False


def test_is_stage_valid_true_when_done_hash_matches_files_present(tmp_path: Path) -> None:
    (tmp_path / "rec.parquet").write_text("x")
    (tmp_path / "jra_se.parquet").write_text("x")
    manifest = subject.CheckpointManifest()
    manifest.stages["source"] = subject.StageCheckpoint(
        status="done",
        tables=["rec.parquet", "jra_se.parquet"],
        row_counts={},
        query_hash="hash1",
        timestamp=1.0,
    )
    assert manifest.is_stage_valid("source", "hash1", tmp_path) is True


def test_build_target_fingerprint_differs_by_category() -> None:
    jra = subject.build_target_fingerprint("jra")
    nar = subject.build_target_fingerprint("nar")
    assert jra != nar


def test_compute_stage_hash_is_deterministic() -> None:
    first = subject.compute_stage_hash("source", "jra", "20200101", "20201231", [2020, 2021])
    second = subject.compute_stage_hash("source", "jra", "20200101", "20201231", [2020, 2021])
    assert first == second


def test_compute_stage_hash_changes_with_category() -> None:
    jra = subject.compute_stage_hash("source", "jra", "20200101", "20201231", [2020])
    nar = subject.compute_stage_hash("source", "nar", "20200101", "20201231", [2020])
    assert jra != nar


def test_compute_stage_hash_changes_with_years() -> None:
    one = subject.compute_stage_hash("source", "jra", "20200101", "20201231", [2020])
    two = subject.compute_stage_hash("source", "jra", "20200101", "20201231", [2020, 2021])
    assert one != two


def test_compute_stage_hash_changes_with_date_window() -> None:
    early = subject.compute_stage_hash("source", "jra", "20200101", "20201231", [2020])
    late = subject.compute_stage_hash("source", "jra", "20210101", "20211231", [2020])
    assert early != late


def test_compute_stage_hash_changes_with_extra() -> None:
    without = subject.compute_stage_hash("weather_lookup", "jra", "20200101", "20201231", [2020])
    with_dir = subject.compute_stage_hash(
        "weather_lookup", "jra", "20200101", "20201231", [2020], "/vw"
    )
    assert without != with_dir


def test_compute_stage_hash_differs_per_stage() -> None:
    source = subject.compute_stage_hash("source", "jra", "20200101", "20201231", [2020])
    target = subject.compute_stage_hash("target", "jra", "20200101", "20201231", [2020])
    pedigree = subject.compute_stage_hash("pedigree", "jra", "20200101", "20201231", [2020])
    assert len({source, target, pedigree}) == 3


def test_stage_sql_fingerprint_covers_every_known_stage() -> None:
    seen = {
        subject._stage_sql_fingerprint(stage, "jra", None)
        for stage in subject.CHECKPOINT_STAGE_ORDER
    }
    assert len(seen) == len(subject.CHECKPOINT_STAGE_ORDER)


def test_stage_sql_fingerprint_unknown_stage_returns_name() -> None:
    assert subject._stage_sql_fingerprint("mystery_stage", "jra", None) == "mystery_stage"


def test_stage_sql_fingerprint_horse_history_includes_base_select() -> None:
    fingerprint = subject._stage_sql_fingerprint("horse_history_derived", "jra", None)
    assert subject.HORSE_HISTORY_BASE_SELECT in fingerprint


def test_stage_sql_fingerprint_partner_includes_jockey_and_trainer() -> None:
    fingerprint = subject._stage_sql_fingerprint("partner_features", "jra", None)
    assert "jockey_career" in fingerprint
    assert "trainer_career" in fingerprint


def test_stage_sql_fingerprint_pedigree_includes_target_pedigree() -> None:
    fingerprint = subject._stage_sql_fingerprint("pedigree", "jra", None)
    assert "target_pedigree" in fingerprint


def test_stage_sql_fingerprint_weather_reflects_venue_presence() -> None:
    without = subject._stage_sql_fingerprint("weather_lookup", "jra", None)
    with_dir = subject._stage_sql_fingerprint("weather_lookup", "jra", Path("/vw"))
    assert without != with_dir


def test_spilled_table_files_appends_parquet_suffix() -> None:
    assert subject.spilled_table_files(Path("/tmp"), ("target", "rec")) == [
        "target.parquet",
        "rec.parquet",
    ]


def test_spilled_row_counts_reports_per_table_counts() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("create table target as select * from (values (1), (2), (3)) t(x)")
    con.execute("create table rec as select * from (values (1)) t(x)")
    counts = subject.spilled_row_counts(con, ("target", "rec"))
    assert counts == {"target.parquet": 3, "rec.parquet": 1}


def test_restore_stage_from_spill_creates_views_and_returns_true(tmp_path: Path) -> None:
    import duckdb

    writer = duckdb.connect()
    writer.execute("create table horse_career as select 11 as k")
    parquet_path = (tmp_path / "horse_career.parquet").as_posix()
    writer.execute(f"copy horse_career to '{parquet_path}' (format parquet)")
    writer.close()
    con = duckdb.connect()
    checkpoint = subject.StageCheckpoint(
        status="done",
        tables=["horse_career.parquet"],
        row_counts={"horse_career.parquet": 1},
        query_hash="h",
        timestamp=1.0,
    )
    restored = subject.restore_stage_from_spill(con, "horse_history_derived", checkpoint, tmp_path)
    assert restored is True
    row = con.execute("select k from horse_career").fetchone()
    assert row == (11,)


def test_restore_stage_from_spill_returns_false_when_file_missing(tmp_path: Path) -> None:
    import duckdb

    con = duckdb.connect()
    checkpoint = subject.StageCheckpoint(
        status="done",
        tables=["absent.parquet"],
        row_counts={},
        query_hash="h",
        timestamp=1.0,
    )
    assert subject.restore_stage_from_spill(con, "source", checkpoint, tmp_path) is False


def test_restore_stage_from_spill_returns_false_when_parquet_unreadable(tmp_path: Path) -> None:
    import duckdb

    bad_path = tmp_path / "horse_career.parquet"
    bad_path.write_text("this is not parquet")
    con = duckdb.connect()
    checkpoint = subject.StageCheckpoint(
        status="done",
        tables=["horse_career.parquet"],
        row_counts={},
        query_hash="h",
        timestamp=1.0,
    )
    assert subject.restore_stage_from_spill(con, "source", checkpoint, tmp_path) is False


def test_drop_view_or_table_drops_a_view() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("create view target as select 1 as x")
    subject.drop_view_or_table(con, "target")
    remaining = con.execute(
        "select count(*) from information_schema.tables where table_name = 'target'"
    ).fetchone()
    assert remaining == (0,)


def test_drop_view_or_table_drops_a_base_table() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("create table jra_se as select 1 as x")
    subject.drop_view_or_table(con, "jra_se")
    remaining = con.execute(
        "select count(*) from information_schema.tables where table_name = 'jra_se'"
    ).fetchone()
    assert remaining == (0,)


def test_drop_view_or_table_drops_a_temp_table() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("create temp table horse_history_base as select 1 as x")
    subject.drop_view_or_table(con, "horse_history_base")
    remaining = con.execute(
        "select count(*) from information_schema.tables where table_name = 'horse_history_base'"
    ).fetchone()
    assert remaining == (0,)


def test_drop_view_or_table_noop_when_absent() -> None:
    import duckdb

    con = duckdb.connect()
    subject.drop_view_or_table(con, "never_created")
    remaining = con.execute(
        "select count(*) from information_schema.tables where table_name = 'never_created'"
    ).fetchone()
    assert remaining == (0,)


def test_target_rows_from_target_counts_rows() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("create table target as select * from (values (1), (2)) t(x)")
    assert subject.target_rows_from_target(con) == 2


def test_extract_years_from_target_reads_distinct_race_years() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        "create table target as select * from (values (2020), (2020), (2021)) t(race_year)"
    )
    assert subject.extract_years_from_target(con) == [2020, 2021]


def test_controller_should_skip_false_when_inactive(tmp_path: Path) -> None:
    controller = subject.CheckpointController(
        active=False,
        incremental=False,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=tmp_path / "table_spill",
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )
    controller.manifest.stages["source"] = subject.StageCheckpoint(
        status="done", tables=[], row_counts={}, query_hash="ignored", timestamp=1.0
    )
    assert controller.should_skip("source", []) is False


def test_controller_should_skip_true_when_valid(tmp_path: Path) -> None:
    spill_dir = tmp_path / "table_spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    (spill_dir / "target.parquet").write_text("x")
    controller = subject.CheckpointController(
        active=True,
        incremental=False,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=spill_dir,
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )
    valid_hash = controller.stage_hash("target", [2020])
    controller.manifest.stages["target"] = subject.StageCheckpoint(
        status="done",
        tables=["target.parquet"],
        row_counts={"target.parquet": 1},
        query_hash=valid_hash,
        timestamp=1.0,
    )
    assert controller.should_skip("target", [2020]) is True


def test_controller_spill_and_record_is_noop_when_inactive(tmp_path: Path) -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("create table target as select 1 as x")
    controller = subject.CheckpointController(
        active=False,
        incremental=False,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=tmp_path / "table_spill",
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )
    controller.spill_and_record(con, "target", [2020])
    assert subject.CheckpointManifest.load(tmp_path) is None
    base_tables = con.execute(
        "select count(*) from information_schema.tables "
        "where table_name = 'target' and table_type = 'BASE TABLE'"
    ).fetchone()
    assert base_tables == (1,)


def test_controller_spill_and_record_spills_and_writes_manifest(tmp_path: Path) -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("create table target as select * from (values (1), (2)) t(x)")
    controller = subject.CheckpointController(
        active=True,
        incremental=False,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=tmp_path / "table_spill",
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )
    controller.spill_and_record(con, "target", [2020])
    reloaded = subject.CheckpointManifest.load(tmp_path)
    assert reloaded is not None
    assert reloaded.stages["target"].tables == ["target.parquet"]
    assert reloaded.stages["target"].row_counts == {"target.parquet": 2}
    view_rows = con.execute(
        "select table_type from information_schema.tables where table_name = 'target'"
    ).fetchone()
    assert view_rows == ("VIEW",)


def test_controller_try_restore_false_when_skip_false(tmp_path: Path) -> None:
    import duckdb

    con = duckdb.connect()
    controller = subject.CheckpointController(
        active=True,
        incremental=False,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=tmp_path / "table_spill",
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )
    assert controller.try_restore(con, "target", [2020]) is False


def test_controller_try_restore_true_when_spill_present(tmp_path: Path) -> None:
    import duckdb

    spill_dir = tmp_path / "table_spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    writer = duckdb.connect()
    writer.execute("create table target as select 5 as race_year")
    writer.execute(f"copy target to '{(spill_dir / 'target.parquet').as_posix()}' (format parquet)")
    writer.close()
    con = duckdb.connect()
    controller = subject.CheckpointController(
        active=True,
        incremental=False,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=spill_dir,
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )
    valid_hash = controller.stage_hash("target", [2020])
    controller.manifest.stages["target"] = subject.StageCheckpoint(
        status="done",
        tables=["target.parquet"],
        row_counts={"target.parquet": 1},
        query_hash=valid_hash,
        timestamp=1.0,
    )
    assert controller.try_restore(con, "target", [2020]) is True
    row = con.execute("select race_year from target").fetchone()
    assert row == (5,)


def test_controller_try_restore_invalidates_when_file_vanished(tmp_path: Path) -> None:
    import duckdb

    spill_dir = tmp_path / "table_spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    controller = subject.CheckpointController(
        active=True,
        incremental=False,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=spill_dir,
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )
    valid_hash = controller.stage_hash("target", [2020])
    controller.manifest.stages["target"] = subject.StageCheckpoint(
        status="done",
        tables=["target.parquet"],
        row_counts={"target.parquet": 1},
        query_hash=valid_hash,
        timestamp=1.0,
    )
    monkey_patched = pytest.MonkeyPatch()
    monkey_patched.setattr(controller.manifest, "is_stage_valid", lambda *_a, **_k: True)
    assert controller.try_restore(con, "target", [2020]) is False
    monkey_patched.undo()
    assert "target" not in controller.manifest.stages


def test_controller_cascade_invalidate_noop_when_not_incremental(tmp_path: Path) -> None:
    controller = subject.CheckpointController(
        active=True,
        incremental=False,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=tmp_path / "table_spill",
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )
    controller.manifest.stages["pedigree"] = subject.StageCheckpoint(
        status="done", tables=[], row_counts={}, query_hash="h", timestamp=1.0
    )
    controller.cascade_invalidate_from("source")
    assert "pedigree" in controller.manifest.stages


def test_controller_cascade_invalidate_drops_downstream_when_incremental(tmp_path: Path) -> None:
    controller = subject.CheckpointController(
        active=True,
        incremental=True,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=tmp_path / "table_spill",
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )
    controller.manifest.stages["source"] = subject.StageCheckpoint(
        status="done", tables=[], row_counts={}, query_hash="h", timestamp=1.0
    )
    controller.manifest.stages["pedigree"] = subject.StageCheckpoint(
        status="done", tables=[], row_counts={}, query_hash="h", timestamp=1.0
    )
    controller.cascade_invalidate_from("target")
    assert "source" in controller.manifest.stages
    assert "pedigree" not in controller.manifest.stages


def test_controller_cascade_invalidate_unknown_stage_is_noop(tmp_path: Path) -> None:
    controller = subject.CheckpointController(
        active=True,
        incremental=True,
        manifest=subject.CheckpointManifest(),
        temp_dir=tmp_path,
        spill_dir=tmp_path / "table_spill",
        category="jra",
        from_date="20200101",
        to_date="20201231",
        venue_weather_extra="",
    )
    controller.manifest.stages["pedigree"] = subject.StageCheckpoint(
        status="done", tables=[], row_counts={}, query_hash="h", timestamp=1.0
    )
    controller.cascade_invalidate_from("not_a_real_stage")
    assert "pedigree" in controller.manifest.stages


def test_make_checkpoint_controller_inactive_without_flags(tmp_path: Path) -> None:
    args = subject.parse_args(["--temp-dir", str(tmp_path)])
    controller = subject.make_checkpoint_controller(args)
    assert controller.active is False
    assert controller.incremental is False


def test_make_checkpoint_controller_active_with_resume(tmp_path: Path) -> None:
    args = subject.parse_args(["--resume", "--temp-dir", str(tmp_path)])
    controller = subject.make_checkpoint_controller(args)
    assert controller.active is True
    assert controller.incremental is False
    assert controller.temp_dir == tmp_path


def test_make_checkpoint_controller_active_with_incremental(tmp_path: Path) -> None:
    args = subject.parse_args(["--incremental", "--temp-dir", str(tmp_path)])
    controller = subject.make_checkpoint_controller(args)
    assert controller.active is True
    assert controller.incremental is True


def test_make_checkpoint_controller_defaults_temp_dir_when_none() -> None:
    args = subject.parse_args(["--resume"])
    controller = subject.make_checkpoint_controller(args)
    assert controller.temp_dir == Path("/tmp/duckdb-spill")


def test_make_checkpoint_controller_loads_existing_manifest(tmp_path: Path) -> None:
    seed = subject.CheckpointManifest(category="old")
    seed.stages["source"] = subject.StageCheckpoint(
        status="done", tables=["rec.parquet"], row_counts={}, query_hash="h", timestamp=1.0
    )
    seed.save(tmp_path)
    args = subject.parse_args(["--resume", "--category", "jra", "--temp-dir", str(tmp_path)])
    controller = subject.make_checkpoint_controller(args)
    assert "source" in controller.manifest.stages
    assert controller.manifest.category == "jra"


def test_make_checkpoint_controller_records_venue_weather_extra(tmp_path: Path) -> None:
    args = subject.parse_args(
        ["--resume", "--temp-dir", str(tmp_path), "--venue-weather-dir", str(tmp_path / "vw")]
    )
    controller = subject.make_checkpoint_controller(args)
    assert controller.venue_weather_extra == (tmp_path / "vw").as_posix()


def test_checkpoint_stage_tables_partition_all_intermediate_tables() -> None:
    owned: list[str] = []
    for stage in subject.CHECKPOINT_STAGE_ORDER:
        owned.extend(subject.CHECKPOINT_STAGE_TABLES.get(stage, ()))
    intermediate = [t for t in subject.SPILL_TABLES]
    assert sorted(t for t in owned if t in intermediate) == sorted(intermediate)


# ---------------------------------------------------------------------------
# Interaction features (weather x horse, pedigree x horse, sire-style x horse).
# All eight must appear as computed aliases in base_features_select_sql().
# ---------------------------------------------------------------------------


def test_base_features_select_sql_includes_all_interaction_columns() -> None:
    sql = subject.base_features_select_sql("jra")
    assert "as rain_x_speed_decay" in sql
    assert "as wind_x_front_runner" in sql
    assert "as pedigree_venue_x_horse_venue" in sql
    assert "as pedigree_distance_x_horse_distance" in sql
    assert "as sire_style_x_horse_style_match" in sql
    assert "as wind_x_field_size" in sql
    assert "as rain_x_track_condition" in sql
    assert "as cold_x_speed_effect" in sql


def test_interaction_columns_flow_through_assemble_final_select() -> None:
    sql = subject.assemble_final_select_from_temp_tables("jra")
    assert "as rain_x_speed_decay" in sql
    assert "as wind_x_front_runner" in sql
    assert "as pedigree_venue_x_horse_venue" in sql
    assert "as pedigree_distance_x_horse_distance" in sql
    assert "as sire_style_x_horse_style_match" in sql
    assert "as wind_x_field_size" in sql
    assert "as rain_x_track_condition" in sql
    assert "as cold_x_speed_effect" in sql


def test_pedigree_interaction_columns_respect_min_races_guard() -> None:
    sql = subject.base_features_select_sql("jra")
    assert (
        f"coalesce(case when pf.sks_race_count >= {subject.PEDIGREE_MIN_RACES} then pf.sire_keibajo_win_rate_val else null end, 0) * coalesce(hc.same_keibajo_win_rate, 0) as pedigree_venue_x_horse_venue"
        in sql
    )
    assert (
        f"coalesce(case when pf.sds_race_count >= {subject.PEDIGREE_MIN_RACES} then pf.sire_distance_win_rate_val else null end, 0) * coalesce(hc.same_distance_win_rate, 0) as pedigree_distance_x_horse_distance"
        in sql
    )


def test_sire_style_match_returns_null_when_horse_style_unknown() -> None:
    sql = subject.base_features_select_sql("jra")
    assert "when rsh.past_nige_rate_self is null then null" in sql
    assert f"pf.srs_race_count >= {subject.PEDIGREE_MIN_RACES} then pf.sire_nige_rate_val" in sql


def test_parse_args_log_file_defaults_none() -> None:
    args = subject.parse_args([])
    assert args.log_file is None


def test_parse_args_log_file_sets_path(tmp_path: Path) -> None:
    args = subject.parse_args(["--log-file", str(tmp_path / "build.log")])
    assert args.log_file == tmp_path / "build.log"


def test_set_log_file_none_clears_handle() -> None:
    try:
        subject.set_log_file(None)
        assert subject._log_file is None
    finally:
        subject.close_log_file()


def test_set_log_file_creates_parent_and_appends(tmp_path: Path) -> None:
    log_path = tmp_path / "nested" / "build.log"
    try:
        subject.set_log_file(log_path)
        subject.emit_log_line("first")
        subject.close_log_file()
        subject.set_log_file(log_path)
        subject.emit_log_line("second")
    finally:
        subject.close_log_file()
    assert log_path.read_text(encoding="utf-8") == "first\nsecond\n"


def test_emit_log_line_writes_stdout_and_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    log_path = tmp_path / "build.log"
    try:
        subject.set_log_file(log_path)
        subject.emit_log_line("hello")
    finally:
        subject.close_log_file()
    assert capsys.readouterr().out == "hello\n"
    assert log_path.read_text(encoding="utf-8") == "hello\n"


def test_emit_log_line_writes_stdout_only_when_no_file(
    capsys: pytest.CaptureFixture[str],
) -> None:
    subject.close_log_file()
    subject.emit_log_line("stdout-only")
    assert capsys.readouterr().out == "stdout-only\n"


def test_log_event_appends_json_to_log_file(tmp_path: Path) -> None:
    log_path = tmp_path / "build.log"
    try:
        subject.set_log_file(log_path)
        subject.log_event("source.stage", "done", 1.234, rows=42)
    finally:
        subject.close_log_file()
    record = json.loads(log_path.read_text(encoding="utf-8").strip())
    assert record["stage"] == "source.stage"
    assert record["status"] == "done"
    assert record["rows"] == 42
    assert record["elapsed_seconds"] == 1.23


def test_log_event_omits_rows_when_none(tmp_path: Path) -> None:
    log_path = tmp_path / "build.log"
    try:
        subject.set_log_file(log_path)
        subject.log_event("run", "start", 0.0)
    finally:
        subject.close_log_file()
    record = json.loads(log_path.read_text(encoding="utf-8").strip())
    assert "rows" not in record


def test_heartbeat_emit_writes_to_log_file(tmp_path: Path) -> None:
    log_path = tmp_path / "build.log"
    try:
        subject.set_log_file(log_path)
        heartbeat = subject.Heartbeat(0.0, None)
        heartbeat.set_stage("parquet.write")
    finally:
        subject.close_log_file()
    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line]
    record = json.loads(lines[-1])
    assert record["type"] == "heartbeat"
    assert record["stage"] == "parquet.write"


def test_close_log_file_is_idempotent() -> None:
    subject.close_log_file()
    subject.close_log_file()
    assert subject._log_file is None


def test_jockey_cte_emits_season_keibajo_distance_aggregates():
    cte = subject.jockey_cte()
    assert "jockey_season_win_rate" in cte
    assert "jockey_season_keibajo_win_rate" in cte
    assert "jockey_keibajo_distance_win_rate" in cte
    assert "jockey_season_keibajo_distance_win_rate" in cte
    assert "jockey_season_keibajo_distance_count" in cte
    assert "(cast(month(history_race_dt) as int) + 9) % 12 // 3" in cte
    assert "(cast(month(target_race_dt) as int) + 9) % 12 // 3" in cte
    assert "abs(history_kyori - target_kyori) <= 200" in cte


def test_trainer_cte_emits_class_surface_season_aggregates():
    cte = subject.trainer_cte()
    assert "trainer_grade_win_rate" in cte
    assert "trainer_class_surface_season_win_rate" in cte
    assert "trainer_class_surface_season_count" in cte
    assert "coalesce(history_grade_code, '') = coalesce(target_grade_code, '')" in cte
    assert "left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1)" in cte
    assert "(cast(month(history_race_dt) as int) + 9) % 12 // 3" in cte
    assert "(cast(month(target_race_dt) as int) + 9) % 12 // 3" in cte


def test_weight_cte_emits_trend_and_volatility_aggregates():
    cte = subject.weight_cte()
    assert "weight_trend_5" in cte
    assert "weight_volatility_5" in cte
    assert "regr_slope(b.history_bataiju" in cte
    assert "(-b.recent_rank)::double" in cte
    assert "stddev_pop(b.history_bataiju)" in cte


def test_weight_cte_guards_trend_against_single_point_nan():
    cte = subject.weight_cte()
    assert subject.WEIGHT_TREND_MIN_RACES == 2
    assert (
        f"case when count(b.history_bataiju) filter (where b.recent_rank <= {subject.RECENT_WINDOW_SIZE}) >= {subject.WEIGHT_TREND_MIN_RACES}"
        in cte
    )
    assert "else null end as weight_trend_5" in cte


def test_base_features_select_sql_registers_weight_zscore_and_new_partner_features():
    sql = subject.base_features_select_sql("jra")
    assert "weight_zscore" in sql
    assert "wa.weight_trend_5" in sql
    assert "wa.weight_volatility_5" in sql
    assert (
        f"least(greatest((cast(wa.current_bataiju_kept as double) - wa.weight_avg_5) / nullif(greatest(wa.weight_volatility_5, {subject.WEIGHT_ZSCORE_MIN_VOLATILITY}), 0), -{subject.WEIGHT_ZSCORE_CLAMP}), {subject.WEIGHT_ZSCORE_CLAMP}) as weight_zscore"
        in sql
    )
    assert "jc.jockey_season_win_rate" in sql
    assert "jc.jockey_season_keibajo_distance_count" in sql
    assert "tc.trainer_grade_win_rate" in sql
    assert "tc.trainer_class_surface_season_count" in sql


def test_weight_zscore_clamp_constants_floor_volatility_and_bound_magnitude():
    assert subject.WEIGHT_ZSCORE_MIN_VOLATILITY == 1.0
    assert subject.WEIGHT_ZSCORE_CLAMP == 5.0


def test_weight_zscore_clamps_extreme_values_and_floors_near_zero_volatility():
    import duckdb

    con = duckdb.connect()
    con.execute(
        f"""
        create table wa as
        select * from (values
          (520.0, 500.0, 0.001),
          (480.0, 500.0, 0.001),
          (502.0, 500.0, 8.0)
        ) as t(current_bataiju_kept, weight_avg_5, weight_volatility_5)
        """
    )
    rows = con.execute(
        f"""
        select least(greatest((cast(wa.current_bataiju_kept as double) - wa.weight_avg_5)
          / nullif(greatest(wa.weight_volatility_5, {subject.WEIGHT_ZSCORE_MIN_VOLATILITY}), 0),
          -{subject.WEIGHT_ZSCORE_CLAMP}), {subject.WEIGHT_ZSCORE_CLAMP}) as weight_zscore
        from wa
        order by current_bataiju_kept
        """
    ).fetchall()
    con.close()
    assert rows[0][0] == -5.0
    assert rows[1][0] == 0.25
    assert rows[2][0] == 5.0


def test_weight_trend_5_is_null_for_single_history_point():
    import duckdb

    con = duckdb.connect()
    con.execute(
        f"""
        create table horse_history_base as
        select * from (values
          ('jra', 480.0, 1),
          ('jra', 484.0, 2),
          ('nar', 500.0, 1)
        ) as t(source, history_bataiju, recent_rank)
        """
    )
    rows = con.execute(
        f"""
        select source,
          case when count(history_bataiju) filter (where recent_rank <= {subject.RECENT_WINDOW_SIZE}) >= {subject.WEIGHT_TREND_MIN_RACES}
               then regr_slope(history_bataiju, (-recent_rank)::double) filter (where recent_rank <= {subject.RECENT_WINDOW_SIZE})
               else null end as weight_trend_5
        from horse_history_base
        group by source
        order by source
        """
    ).fetchall()
    con.close()
    assert rows[0][0] == "jra"
    assert rows[0][1] == -4.0
    assert rows[1][0] == "nar"
    assert rows[1][1] is None


def test_write_parquet_writes_per_year_from_target_table(tmp_path: Path) -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        "create temp table target (race_year int, race_id text, horse_id text, val double)"
    )
    con.execute("insert into target values (2023, 'R1', 'H1', 1.0)")
    con.execute("insert into target values (2023, 'R2', 'H2', 2.0)")
    con.execute("insert into target values (2024, 'R3', 'H3', 3.0)")

    final_query = "select race_year, race_id, horse_id, val from target"
    output_dir = tmp_path / "parquet_out"

    subject.write_parquet(con, final_query, output_dir, False, False)

    parquet_2023 = list((output_dir / "race_year=2023").glob("*.parquet"))
    parquet_2024 = list((output_dir / "race_year=2024").glob("*.parquet"))
    assert len(parquet_2023) == 1
    assert len(parquet_2024) == 1

    rows = con.execute(
        f"select race_year, race_id, horse_id, val from read_parquet('{output_dir.as_posix()}/race_year=*/*.parquet', hive_partitioning=true) order by val"
    ).fetchall()
    assert rows == [(2023, "R1", "H1", 1.0), (2023, "R2", "H2", 2.0), (2024, "R3", "H3", 3.0)]

    con.close()


def test_write_parquet_restores_threads_after_write(tmp_path: Path) -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("set threads = 4")
    con.execute(
        "create temp table target (race_year int, race_id text, val double)"
    )
    con.execute("insert into target values (2025, 'R1', 10.0)")

    final_query = "select race_year, race_id, val from target"
    output_dir = tmp_path / "parquet_threads"

    subject.write_parquet(con, final_query, output_dir, False, False)

    threads_after = con.execute("select current_setting('threads')").fetchone()
    assert threads_after is not None
    assert int(threads_after[0]) == 4
    con.close()


def test_write_parquet_no_staging_table_leak(tmp_path: Path) -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        "create temp table target (race_year int, race_id text, val double)"
    )
    con.execute("insert into target values (2023, 'R1', 1.0)")
    con.execute("insert into target values (2024, 'R2', 2.0)")

    final_query = "select race_year, race_id, val from target"
    output_dir = tmp_path / "parquet_leak"

    subject.write_parquet(con, final_query, output_dir, False, False)

    tables = con.execute(
        "select table_name from information_schema.tables where table_name = '_parquet_staging'"
    ).fetchall()
    assert tables == []
    con.close()


def test_window_query_from_base_table_produces_valid_sql_with_expected_columns() -> None:
    sql = subject._window_query_from_base_table("my_base_table")
    assert "from my_base_table b" in sql
    assert "speed_index_avg_5_rank_in_race" in sql
    assert "speed_index_best_5_rank_in_race" in sql
    assert "jockey_recent_win_rate_rank_in_race" in sql
    assert "trainer_career_win_rate_rank_in_race" in sql
    assert "pedigree_score_for_race_rank_in_race" in sql
    assert "same_distance_win_rate_rank_in_race" in sql
    assert "speed_index_avg_5_diff_from_race_avg" in sql
    assert "jockey_recent_win_rate_diff_from_race_avg" in sql
    assert "pedigree_score_diff_from_race_avg" in sql
    assert "race_partition" in sql
    assert "race_by_speed_avg_asc" in sql


def test_window_query_from_base_table_uses_race_partition_columns() -> None:
    sql = subject._window_query_from_base_table("tbl")
    assert "b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango" in sql


def test_window_query_from_base_table_is_executable_in_duckdb() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("""
        create temp table _test_base (
            source text, kaisai_nen text, kaisai_tsukihi text, keibajo_code text,
            race_bango text, race_year int, ketto_toroku_bango text, umaban int,
            speed_index_avg_5 double, speed_index_best_5 double,
            jockey_recent_win_rate double, trainer_career_win_rate double,
            pedigree_score_for_race double, same_distance_win_rate double
        )
    """)
    con.execute("""
        insert into _test_base values
        ('jra','2024','0601','01','01',2024,'H1',1,50.0,55.0,0.2,0.3,0.4,0.5),
        ('jra','2024','0601','01','01',2024,'H2',2,45.0,48.0,0.15,0.25,0.35,0.45)
    """)

    query = subject._window_query_from_base_table("_test_base")
    rows = con.execute(query).fetchall()
    assert len(rows) == 2
    con.close()


def test_stage_parquet_write_prematerializes_and_cleans_up(tmp_path: Path) -> None:
    import duckdb

    con = duckdb.connect()
    # Create minimal temp tables that base_features_select_sql references.
    # Instead of setting up all 17 tables, we mock base_features_select_sql
    # and _window_query_from_base_table via monkeypatching.
    con.execute("""
        create temp table target (
            race_year int, source text, kaisai_nen text, kaisai_tsukihi text,
            keibajo_code text, race_bango text, ketto_toroku_bango text
        )
    """)
    con.execute("""
        insert into target values
        (2023, 'jra', '2023', '0601', '01', '01', 'H1'),
        (2024, 'jra', '2024', '0601', '01', '01', 'H2')
    """)

    # Track calls to verify the flow
    calls: list[str] = []

    def mock_base_sql(category: str) -> str:
        calls.append(f"base_sql:{category}")
        return """
            select 'jra' as source, '2023' as kaisai_nen, '0601' as kaisai_tsukihi,
                   '01' as keibajo_code, '01' as race_bango, 2023 as race_year,
                   50.0 as speed_index_avg_5, 55.0 as speed_index_best_5,
                   0.2 as jockey_recent_win_rate, 0.3 as trainer_career_win_rate,
                   0.4 as pedigree_score_for_race, 0.5 as same_distance_win_rate
            union all
            select 'jra', '2024', '0601', '01', '01', 2024,
                   45.0, 48.0, 0.15, 0.25, 0.35, 0.45
        """

    import unittest.mock

    output_dir = tmp_path / "parquet_stage"
    with unittest.mock.patch.object(subject, "base_features_select_sql", mock_base_sql):
        subject.stage_parquet_write(con, "jra", output_dir, False, False)

    assert "base_sql:jra" in calls

    # _base_features_all should be cleaned up
    tables = con.execute(
        "select table_name from information_schema.tables where table_name = '_base_features_all'"
    ).fetchall()
    assert tables == []

    # Parquet files should exist
    parquet_2023 = list((output_dir / "race_year=2023").glob("*.parquet"))
    parquet_2024 = list((output_dir / "race_year=2024").glob("*.parquet"))
    assert len(parquet_2023) == 1
    assert len(parquet_2024) == 1

    con.close()


# ---------------------------------------------------------------------------
# Pedigree consolidation: the 8 per-row LEFT JOINs in base_features_select_sql
# are replaced by a single LEFT JOIN on the pre-joined pedigree_features table,
# with composite indexes added on the stats temp tables to speed the build.
# ---------------------------------------------------------------------------


def test_pedigree_natural_key_is_the_target_row_grain() -> None:
    assert subject.PEDIGREE_NATURAL_KEY == (
        "source",
        "kaisai_nen",
        "kaisai_tsukihi",
        "keibajo_code",
        "race_bango",
        "ketto_toroku_bango",
    )


def test_pedigree_features_table_name_constant() -> None:
    assert subject.PEDIGREE_FEATURES_TABLE == "pedigree_features"


def test_pedigree_join_specs_cover_all_seven_stats_tables() -> None:
    tables = [spec["table"] for spec in subject.PEDIGREE_JOIN_SPECS]
    assert tables == [
        "sire_distance_stats",
        "sire_track_stats",
        "damsire_distance_stats",
        "damsire_track_stats",
        "sire_running_style_stats",
        "sire_keibajo_stats",
        "damsire_keibajo_stats",
    ]


def test_pedigree_join_specs_aliases_match_legacy_base_sql_aliases() -> None:
    aliases = [spec["alias"] for spec in subject.PEDIGREE_JOIN_SPECS]
    assert aliases == ["sds", "sts", "dsd", "dst", "srs", "sks", "dks"]


def test_pedigree_target_key_column_uses_target_sire_for_sire_specs() -> None:
    sds_spec = next(s for s in subject.PEDIGREE_JOIN_SPECS if s["alias"] == "sds")
    assert subject.pedigree_target_key_column(sds_spec) == "target_sire"


def test_pedigree_target_key_column_uses_target_damsire_for_damsire_specs() -> None:
    dks_spec = next(s for s in subject.PEDIGREE_JOIN_SPECS if s["alias"] == "dks")
    assert subject.pedigree_target_key_column(dks_spec) == "target_damsire"


def test_pedigree_stats_index_sql_indexes_composite_probe_key() -> None:
    spec = next(s for s in subject.PEDIGREE_STAT_SPECS if s["table"] == "sire_distance_stats")
    sql = subject.pedigree_stats_index_sql(spec)
    assert (
        sql
        == "create index if not exists idx_sire_distance_stats on sire_distance_stats (sire, kyori_band, stats_year_month)"
    )


def test_pedigree_stats_index_sql_for_keibajo_bucketed_spec() -> None:
    spec = next(s for s in subject.PEDIGREE_STAT_SPECS if s["table"] == "sire_keibajo_stats")
    sql = subject.pedigree_stats_index_sql(spec)
    assert (
        sql
        == "create index if not exists idx_sire_keibajo_stats on sire_keibajo_stats (sire, keibajo_code, stats_year_month)"
    )


def test_target_pedigree_index_sql_indexes_the_natural_key() -> None:
    sql = subject.target_pedigree_index_sql()
    assert (
        sql
        == "create index if not exists idx_target_pedigree on target_pedigree (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def test_pedigree_features_sql_creates_consolidated_temp_table() -> None:
    sql = subject.pedigree_features_sql()
    assert "create or replace temp table pedigree_features as" in sql
    assert "from target_pedigree tp" in sql


def test_pedigree_features_sql_joins_all_seven_stats_tables() -> None:
    sql = subject.pedigree_features_sql()
    assert "left join sire_distance_stats sds" in sql
    assert "left join sire_track_stats sts" in sql
    assert "left join damsire_distance_stats dsd" in sql
    assert "left join damsire_track_stats dst" in sql
    assert "left join sire_running_style_stats srs" in sql
    assert "left join sire_keibajo_stats sks" in sql
    assert "left join damsire_keibajo_stats dks" in sql


def test_pedigree_features_sql_projects_per_table_race_count_aliases() -> None:
    sql = subject.pedigree_features_sql()
    assert "sds.race_count as sds_race_count" in sql
    assert "srs.race_count as srs_race_count" in sql
    assert "dks.race_count as dks_race_count" in sql


def test_pedigree_features_sql_projects_every_val_column() -> None:
    sql = subject.pedigree_features_sql()
    assert "sds.sire_distance_win_rate_val as sire_distance_win_rate_val" in sql
    assert "sds.sire_avg_finish_at_distance_val as sire_avg_finish_at_distance_val" in sql
    assert "srs.sire_corner_1_norm_avg_val as sire_corner_1_norm_avg_val" in sql
    assert "dks.damsire_keibajo_win_rate_val as damsire_keibajo_win_rate_val" in sql


def test_pedigree_features_sql_projects_the_natural_key() -> None:
    sql = subject.pedigree_features_sql()
    assert "tp.source" in sql
    assert "tp.kaisai_nen" in sql
    assert "tp.ketto_toroku_bango" in sql


def test_base_features_select_sql_joins_pedigree_features_once() -> None:
    sql = subject.base_features_select_sql("jra")
    assert (
        "left join pedigree_features pf using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
        in sql
    )
    # The 8 per-row pedigree joins must no longer live in the final SELECT.
    assert "left join sire_distance_stats" not in sql
    assert "left join target_pedigree" not in sql


def test_pedigree_features_consolidation_preserves_stats_values() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        """
        create temp table target_pedigree as
        select 'jra' as source, '2023' as kaisai_nen, '0601' as kaisai_tsukihi,
               '01' as keibajo_code, '01' as race_bango, 'H1' as ketto_toroku_bango,
               3 as kyori_band, '1' as surface, '01' as target_keibajo_code,
               0 as rs_bucket, 'SIRE' as target_sire, 'DAM' as target_damsire
        """
    )
    con.execute(
        """
        create temp table sire_distance_stats as
        select 'SIRE' as sire, 3 as kyori_band, 202306 as stats_year_month,
               0.21::double as sire_distance_win_rate_val, 0.42::double as sire_avg_finish_at_distance_val,
               9 as race_count
        """
    )
    con.execute(
        """
        create temp table sire_track_stats as
        select 'SIRE' as sire, '1' as surface, 202306 as stats_year_month,
               0.11::double as sire_track_win_rate_val, 7 as race_count
        """
    )
    con.execute(
        """
        create temp table damsire_distance_stats as
        select 'DAM' as damsire, 3 as kyori_band, 202306 as stats_year_month,
               0.13::double as dam_sire_distance_win_rate_val, 6 as race_count
        """
    )
    con.execute(
        """
        create temp table damsire_track_stats as
        select 'DAM' as damsire, '1' as surface, 202306 as stats_year_month,
               0.31::double as damsire_avg_finish_at_track_val, 5 as race_count
        """
    )
    con.execute(
        """
        create temp table sire_running_style_stats as
        select 'SIRE' as sire, 0 as rs_bucket, 202306 as stats_year_month,
               0.4::double as sire_nige_rate_val, 0.3::double as sire_senkou_rate_val,
               0.2::double as sire_sashi_rate_val, 0.1::double as sire_oikomi_rate_val,
               1.5::double as sire_corner_1_norm_avg_val, 8 as race_count
        """
    )
    con.execute(
        """
        create temp table sire_keibajo_stats as
        select 'SIRE' as sire, '01' as keibajo_code, 202306 as stats_year_month,
               0.18::double as sire_keibajo_win_rate_val, 10 as race_count
        """
    )
    con.execute(
        """
        create temp table damsire_keibajo_stats as
        select 'DAM' as damsire, '01' as keibajo_code, 202306 as stats_year_month,
               0.09::double as damsire_keibajo_win_rate_val, 4 as race_count
        """
    )
    con.execute(subject.pedigree_features_sql())
    row = con.execute(
        """
        select sire_distance_win_rate_val, sds_race_count,
               sire_track_win_rate_val, sts_race_count,
               dam_sire_distance_win_rate_val, dsd_race_count,
               damsire_avg_finish_at_track_val, dst_race_count,
               sire_nige_rate_val, sire_corner_1_norm_avg_val, srs_race_count,
               sire_keibajo_win_rate_val, sks_race_count,
               damsire_keibajo_win_rate_val, dks_race_count
        from pedigree_features
        """
    ).fetchone()
    assert row == (
        0.21,
        9,
        0.11,
        7,
        0.13,
        6,
        0.31,
        5,
        0.4,
        1.5,
        8,
        0.18,
        10,
        0.09,
        4,
    )
    con.close()


def test_pedigree_features_consolidation_keeps_unmatched_lineage_null() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        """
        create temp table target_pedigree as
        select 'jra' as source, '2023' as kaisai_nen, '0601' as kaisai_tsukihi,
               '01' as keibajo_code, '01' as race_bango, 'H1' as ketto_toroku_bango,
               3 as kyori_band, '1' as surface, '01' as target_keibajo_code,
               0 as rs_bucket, 'UNKNOWN' as target_sire, 'UNKNOWN' as target_damsire
        """
    )
    con.execute(
        """
        create temp table sire_distance_stats as
        select 'SIRE' as sire, 3 as kyori_band, 202306 as stats_year_month,
               0.21 as sire_distance_win_rate_val, 0.42 as sire_avg_finish_at_distance_val,
               9 as race_count
        """
    )
    con.execute(
        "create temp table sire_track_stats as select 'SIRE' as sire, '1' as surface, 202306 as stats_year_month, 0.11 as sire_track_win_rate_val, 7 as race_count"
    )
    con.execute(
        "create temp table damsire_distance_stats as select 'DAM' as damsire, 3 as kyori_band, 202306 as stats_year_month, 0.13 as dam_sire_distance_win_rate_val, 6 as race_count"
    )
    con.execute(
        "create temp table damsire_track_stats as select 'DAM' as damsire, '1' as surface, 202306 as stats_year_month, 0.31 as damsire_avg_finish_at_track_val, 5 as race_count"
    )
    con.execute(
        "create temp table sire_running_style_stats as select 'SIRE' as sire, 0 as rs_bucket, 202306 as stats_year_month, 0.4 as sire_nige_rate_val, 0.3 as sire_senkou_rate_val, 0.2 as sire_sashi_rate_val, 0.1 as sire_oikomi_rate_val, 1.5 as sire_corner_1_norm_avg_val, 8 as race_count"
    )
    con.execute(
        "create temp table sire_keibajo_stats as select 'SIRE' as sire, '01' as keibajo_code, 202306 as stats_year_month, 0.18 as sire_keibajo_win_rate_val, 10 as race_count"
    )
    con.execute(
        "create temp table damsire_keibajo_stats as select 'DAM' as damsire, '01' as keibajo_code, 202306 as stats_year_month, 0.09 as damsire_keibajo_win_rate_val, 4 as race_count"
    )
    con.execute(subject.pedigree_features_sql())
    row = con.execute(
        "select ketto_toroku_bango, sire_distance_win_rate_val, sds_race_count from pedigree_features"
    ).fetchone()
    assert row == ("H1", None, None)
    con.close()


def test_materialize_pedigree_stats_leaves_only_pedigree_features(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        """
        create temp table target as
        select 'jra' as source, '2023' as kaisai_nen, '0601' as kaisai_tsukihi,
               '01' as keibajo_code, '01' as race_bango, 'H1' as ketto_toroku_bango,
               1800 as kyori, '10' as track_code
        """
    )
    con.execute(
        "create temp table jra_um as select 'H1' as ketto_toroku_bango, 'SIRE' as ketto_joho_01b, 'DAM' as ketto_joho_05b"
    )
    con.execute("create temp table nar_um as select '' as ketto_toroku_bango, '' as ketto_joho_01b, '' as ketto_joho_05b where false")
    con.execute("create temp table nar_nu as select '' as ketto_toroku_bango, '' as ketto_joho_01b, '' as ketto_joho_05b where false")
    con.execute(
        """
        create temp table pedigree_rec_um as
        select 'jra' as source, '20230101' as race_date, 202301 as race_year_month,
               'H0' as ketto_toroku_bango, 1800 as kyori, '10' as track_code,
               1 as finish_position, 0.1 as finish_norm, '01' as keibajo_code,
               'SIRE' as ketto_joho_01b, 'DAM' as ketto_joho_05b, 0.0 as corner1_norm
        """
    )

    monkeypatch.setattr(subject, "pedigree_rec_um_sql", lambda category: "create or replace temp table pedigree_rec_um as select * from pedigree_rec_um")
    subject.materialize_pedigree_stats(con, "jra")

    remaining = {
        r[0]
        for r in con.execute(
            "select table_name from information_schema.tables where table_name in ("
            "'pedigree_features', 'target_pedigree', 'sire_distance_stats', 'sire_keibajo_stats')"
        ).fetchall()
    }
    assert remaining == {"pedigree_features"}
    feature_rows = con.execute("select count(*) from pedigree_features").fetchone()
    assert feature_rows == (1,)
    con.close()


def test_pedigree_stats_index_sql_creates_a_usable_index() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        """
        create temp table sire_distance_stats as
        select 'SIRE' as sire, 3 as kyori_band, 202306 as stats_year_month,
               0.21::double as sire_distance_win_rate_val, 0.42::double as sire_avg_finish_at_distance_val,
               9 as race_count
        """
    )
    spec = next(s for s in subject.PEDIGREE_STAT_SPECS if s["table"] == "sire_distance_stats")
    con.execute(subject.pedigree_stats_index_sql(spec))
    index_names = {
        r[0] for r in con.execute("select index_name from duckdb_indexes()").fetchall()
    }
    assert "idx_sire_distance_stats" in index_names
    con.close()


def test_target_pedigree_index_sql_creates_a_usable_index() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        """
        create temp table target_pedigree as
        select 'jra' as source, '2023' as kaisai_nen, '0601' as kaisai_tsukihi,
               '01' as keibajo_code, '01' as race_bango, 'H1' as ketto_toroku_bango
        """
    )
    con.execute(subject.target_pedigree_index_sql())
    index_names = {
        r[0] for r in con.execute("select index_name from duckdb_indexes()").fetchall()
    }
    assert "idx_target_pedigree" in index_names
    con.close()


def test_stage_sql_fingerprint_pedigree_includes_consolidation_sql() -> None:
    fingerprint = subject._stage_sql_fingerprint(subject.CHECKPOINT_PEDIGREE, "jra", None)
    assert "create or replace temp table pedigree_features as" in fingerprint


def test_checkpoint_pedigree_stage_owns_pedigree_features_only() -> None:
    assert subject.CHECKPOINT_STAGE_TABLES[subject.CHECKPOINT_PEDIGREE] == ("pedigree_features",)


def test_spill_after_pedigree_is_just_pedigree_features() -> None:
    assert subject.SPILL_AFTER_PEDIGREE == ("pedigree_features",)


# ---------------------------------------------------------------------------
# _build_horse_filter_from_rec / _build_race_filter_from_rec
# ---------------------------------------------------------------------------


def test_build_horse_filter_from_rec_returns_filter_when_rec_has_horses() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        "create temp table rec as select * from (values "
        "('jra', '2020100001'), ('jra', '2020100002'), ('jra', null)"
        ") as v(source, ketto_toroku_bango)"
    )
    result = subject._build_horse_filter_from_rec(con)
    con.close()
    assert "ketto_toroku_bango in (" in result
    assert "'2020100001'" in result
    assert "'2020100002'" in result
    assert result.startswith(" and ")


def test_build_horse_filter_from_rec_returns_empty_when_rec_has_no_horses() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        "create temp table rec as select cast(null as varchar) as ketto_toroku_bango, "
        "cast(null as varchar) as source where false"
    )
    result = subject._build_horse_filter_from_rec(con)
    con.close()
    assert result == ""


def test_build_race_filter_from_rec_returns_filter() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        "create temp table rec as select * from (values "
        "('jra', '05', '01', '2026', '0628'), "
        "('jra', '05', '02', '2026', '0628')"
        ") as v(source, keibajo_code, race_bango, kaisai_nen, kaisai_tsukihi)"
    )
    result = subject._build_race_filter_from_rec(con, "jra")
    con.close()
    assert "(keibajo_code, race_bango, kaisai_nen, kaisai_tsukihi)" in result
    assert "('05', '01', '2026', '0628')" in result
    assert "('05', '02', '2026', '0628')" in result
    assert result.startswith(" and ")


def test_build_race_filter_from_rec_returns_empty_when_no_rows() -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(
        "create temp table rec as select cast(null as varchar) as source, "
        "cast(null as varchar) as keibajo_code, cast(null as varchar) as race_bango, "
        "cast(null as varchar) as kaisai_nen, cast(null as varchar) as kaisai_tsukihi "
        "where false"
    )
    result = subject._build_race_filter_from_rec(con, "jra")
    con.close()
    assert result == ""


# ---------------------------------------------------------------------------
# stage_se_table entity_filter
# ---------------------------------------------------------------------------


def test_stage_se_table_uses_postgres_query_when_entity_filter_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import duckdb

    captured_sql: list[str] = []

    def capture_run(con_inner: object, stage: object, sql: str, **kw: object) -> None:
        captured_sql.append(sql)

    con = duckdb.connect()
    # Create a dummy table so the index creation after run_staged_sql succeeds.
    con.execute(
        "create temp table nar_se("
        "kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar, "
        "race_bango varchar, ketto_toroku_bango varchar)"
    )
    monkeypatch.setattr(subject, "run_staged_sql", capture_run)
    subject.stage_se_table(
        con,
        "source.nar_se",
        "nar_se",
        "nvd_se",
        "20060101",
        "20260628",
        entity_filter=" and ketto_toroku_bango in ('2020100001')",
    )
    con.close()
    assert any("postgres_query" in sql for sql in captured_sql)


def test_stage_se_table_uses_pg_dot_when_no_entity_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import duckdb

    captured_sql: list[str] = []

    def capture_run(con_inner: object, stage: object, sql: str, **kw: object) -> None:
        captured_sql.append(sql)

    con = duckdb.connect()
    con.execute(
        "create temp table nar_se("
        "kaisai_nen varchar, kaisai_tsukihi varchar, keibajo_code varchar, "
        "race_bango varchar, ketto_toroku_bango varchar)"
    )
    monkeypatch.setattr(subject, "run_staged_sql", capture_run)
    subject.stage_se_table(
        con,
        "source.nar_se",
        "nar_se",
        "nvd_se",
        "20060101",
        "20260628",
    )
    con.close()
    assert any("pg.nvd_se" in sql for sql in captured_sql)
    assert not any("postgres_query" in sql for sql in captured_sql)


# ---------------------------------------------------------------------------
# stage_um_table entity_filter
# ---------------------------------------------------------------------------


def test_stage_um_table_uses_postgres_query_when_entity_filter_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import duckdb

    captured_sql: list[str] = []

    def capture_run(con_inner: object, stage: object, sql: str, **kw: object) -> None:
        captured_sql.append(sql)

    con = duckdb.connect()
    monkeypatch.setattr(subject, "run_staged_sql", capture_run)
    subject.stage_um_table(
        con,
        "source.nar_um",
        "nar_um",
        "nvd_um",
        entity_filter=" and ketto_toroku_bango in ('2020100001')",
    )
    con.close()
    assert any("postgres_query" in sql for sql in captured_sql)


def test_stage_um_table_uses_pg_dot_when_no_entity_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import duckdb

    captured_sql: list[str] = []

    def capture_run(con_inner: object, stage: object, sql: str, **kw: object) -> None:
        captured_sql.append(sql)

    con = duckdb.connect()
    monkeypatch.setattr(subject, "run_staged_sql", capture_run)
    subject.stage_um_table(
        con,
        "source.nar_um",
        "nar_um",
        "nvd_um",
    )
    con.close()
    assert any("pg.nvd_um" in sql for sql in captured_sql)
    assert not any("postgres_query" in sql for sql in captured_sql)


# ---------------------------------------------------------------------------
# stage_ra_table entity_filter
# ---------------------------------------------------------------------------


def test_stage_ra_table_uses_postgres_query_when_entity_filter_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import duckdb

    captured_sql: list[str] = []

    def capture_run(con_inner: object, stage: object, sql: str, **kw: object) -> None:
        captured_sql.append(sql)

    con = duckdb.connect()
    monkeypatch.setattr(subject, "run_staged_sql", capture_run)
    subject.stage_ra_table(
        con,
        "source.nar_ra",
        "nar_ra",
        "nvd_ra",
        "20260628",
        "20260628",
        entity_filter=" and (keibajo_code, race_bango, kaisai_nen, kaisai_tsukihi) in (('83', '01', '2026', '0628'))",
    )
    con.close()
    assert any("postgres_query" in sql for sql in captured_sql)


# ---------------------------------------------------------------------------
# stage_source_tables with target_race
# ---------------------------------------------------------------------------


def test_stage_source_tables_passes_entity_filter_when_target_race_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import duckdb

    con = duckdb.connect()
    captured_se: list[str] = []
    captured_um: list[str] = []
    captured_ra: list[str] = []

    def fake_stage_rec_table(*args: object, **kwargs: object) -> None:
        con.execute(
            "create temp table rec as select * from (values "
            "('jra', '2020100001', '05', '01', '2026', '0628'), "
            "('nar', '2020100003', '44', '03', '2026', '0628')"
            ") as v(source, ketto_toroku_bango, keibajo_code, race_bango, kaisai_nen, kaisai_tsukihi)"
        )

    def fake_stage_se(
        con_: object,
        stage: object,
        table: object,
        pg_table: object,
        hs: object,
        td: object,
        kf: object = None,
        entity_filter: str = "",
    ) -> None:
        captured_se.append(entity_filter)

    def fake_stage_um(
        con_: object,
        stage: object,
        table: object,
        pg_table: object,
        entity_filter: str = "",
    ) -> None:
        captured_um.append(entity_filter)

    def fake_stage_ra(
        con_: object,
        stage: object,
        table: object,
        pg_table: object,
        fd: object,
        td: object,
        kf: object = None,
        entity_filter: str = "",
    ) -> None:
        captured_ra.append(entity_filter)

    monkeypatch.setattr(subject, "install_and_attach_pg", lambda *_: None)
    monkeypatch.setattr(subject, "stage_rec_table", fake_stage_rec_table)
    monkeypatch.setattr(subject, "stage_se_table", fake_stage_se)
    monkeypatch.setattr(subject, "stage_um_table", fake_stage_um)
    monkeypatch.setattr(subject, "stage_ra_table", fake_stage_ra)
    subject.stage_source_tables(
        con, "20260628", "20260628", "jra", ("20260628", "20260628"), None, ("05", "01"),
    )
    con.close()
    assert all(f != "" for f in captured_se)
    assert all(f != "" for f in captured_um)
    assert all(f != "" for f in captured_ra)


def test_stage_source_tables_passes_empty_filter_when_target_race_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import duckdb

    con = duckdb.connect()
    captured_se: list[str] = []
    captured_um: list[str] = []

    def fake_stage_rec_table(*args: object, **kwargs: object) -> None:
        pass

    def fake_stage_se(
        con_: object,
        stage: object,
        table: object,
        pg_table: object,
        hs: object,
        td: object,
        kf: object = None,
        entity_filter: str = "",
    ) -> None:
        captured_se.append(entity_filter)

    def fake_stage_um(
        con_: object,
        stage: object,
        table: object,
        pg_table: object,
        entity_filter: str = "",
    ) -> None:
        captured_um.append(entity_filter)

    def fake_stage_ra(
        con_: object,
        stage: object,
        table: object,
        pg_table: object,
        fd: object,
        td: object,
        kf: object = None,
        entity_filter: str = "",
    ) -> None:
        pass

    monkeypatch.setattr(subject, "install_and_attach_pg", lambda *_: None)
    monkeypatch.setattr(subject, "stage_rec_table", fake_stage_rec_table)
    monkeypatch.setattr(subject, "stage_se_table", fake_stage_se)
    monkeypatch.setattr(subject, "stage_um_table", fake_stage_um)
    monkeypatch.setattr(subject, "stage_ra_table", fake_stage_ra)
    subject.stage_source_tables(
        con, "20260628", "20260628", "jra", ("20260628", "20260628"), None, None,
    )
    con.close()
    assert all(f == "" for f in captured_se)
    assert all(f == "" for f in captured_um)


# ---------------------------------------------------------------------------
# _rec_select_from_corner_features entity_filter
# ---------------------------------------------------------------------------


def test_rec_select_from_corner_features_uses_postgres_query_with_entity_filter() -> None:
    sql = subject._rec_select_from_corner_features(
        "20060101",
        "20260628",
        entity_filter=" and ketto_toroku_bango in ('2020100001')",
    )
    assert "postgres_query" in sql
    assert "race_entry_corner_features" in sql
    assert "ketto_toroku_bango in ('2020100001')" in sql


def test_rec_select_from_corner_features_uses_pg_dot_without_entity_filter() -> None:
    sql = subject._rec_select_from_corner_features("20060101", "20260628")
    assert "pg.race_entry_corner_features" in sql
    assert "postgres_query" not in sql


# ---------------------------------------------------------------------------
# _rec_select_from_ban_ei entity_filter
# ---------------------------------------------------------------------------


def test_rec_select_from_ban_ei_uses_postgres_query_with_entity_filter() -> None:
    sql = subject._rec_select_from_ban_ei(
        "20060101",
        "20260628",
        entity_filter=" and ketto_toroku_bango in ('2020100001')",
    )
    assert "postgres_query" in sql
    assert "nvd_se" in sql
    assert "nvd_ra" in sql
    assert "ketto_toroku_bango in ('2020100001')" in sql


def test_rec_select_from_ban_ei_uses_pg_dot_without_entity_filter() -> None:
    sql = subject._rec_select_from_ban_ei("20060101", "20260628")
    assert "pg.nvd_se" in sql
    assert "postgres_query" not in sql
