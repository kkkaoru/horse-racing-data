from __future__ import annotations

import argparse
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
    assert "se.keibajo_code <> '83'" in sql
    assert "'nar' as source" in sql


def test_upcoming_target_union_sql_ban_ei_filters_to_ban_ei_keibajo():
    sql = subject.upcoming_target_union_sql("ban-ei", "20260603", "20260603")
    assert "pg.nvd_se se" in sql
    assert "se.keibajo_code = '83'" in sql


def test_upcoming_target_union_sql_all_unions_three_categories():
    sql = subject.upcoming_target_union_sql("all", "20260603", "20260603")
    assert "pg.jvd_se se" in sql
    assert "se.keibajo_code <> '83'" in sql
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


def test_pedigree_stat_specs_cover_five_tables():
    table_names = [spec["table"] for spec in subject.PEDIGREE_STAT_SPECS]
    assert table_names == [
        "sire_distance_stats",
        "sire_track_stats",
        "damsire_distance_stats",
        "damsire_track_stats",
        "sire_running_style_stats",
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
    assert "left join sire_running_style_stats srs" in sql


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


def test_nar_subclass_named_classes_constant():
    assert subject.NAR_NAMED_CLASSES == (
        "OP",
        "NEW",
        "MUKATSU",
        "A",
        "B",
        "C",
        "other",
    )


def test_nar_subclass_case_sql_emits_meisho_regex_matches_with_null_for_non_nar():
    sql = subject.nar_subclass_case_sql("t.source", "t.keibajo_code", "wl.nar_kyoso_joken_meisho")
    assert "when t.source <> 'nar' or t.keibajo_code = '83' then null" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, 'ＯＰ')" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, '新馬')" in sql
    assert "regexp_matches(wl.nar_kyoso_joken_meisho, '未勝利|未出走')" in sql
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


def test_nar_subclass_returns_other_for_meisho_with_no_match():
    assert _eval_nar_subclass("nar", "30", "「３歳　　　　」") == "other"


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
