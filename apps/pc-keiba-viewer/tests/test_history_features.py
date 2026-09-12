from __future__ import annotations

import duckdb
import pytest
from learning.history_features import build_history_feature_sql


def test_rejects_unsafe_table_identifier() -> None:
    with pytest.raises(ValueError, match="simple SQL identifier"):
        build_history_feature_sql("history; drop table history")


def test_history_features_decode_clocks_and_exclude_same_day_and_future() -> None:
    with duckdb.connect() as con:
        con.execute("""
            create table history as
            select source,race_date,keibajo_code,race_bango,ketto_toroku_bango,
              soha_time,finish_position, '01' as umaban, '1200' as kyori,
              '10' as shusso_tosu,'4' as barei,'24' as track_code,
              '1' as seibetsu_code,'1' as babajotai_code_dirt,
              'jockey' as kishumei_ryakusho,'trainer' as chokyoshimei_ryakusho
            from (values
              ('jra','20230101','05','01','h1','1000','1'),
              ('nar','20240101','54','01','h1','2000','3'),
              ('nar','20240101','54','02','h1','3000','5'),
              ('nar','20240102','55','01','h1','1234','2'),
              ('nar','20250101','55','01','h1','9599','9')
            ) t(source,race_date,keibajo_code,race_bango,ketto_toroku_bango,
                soha_time,finish_position)
        """)
        con.execute("create table features as " + build_history_feature_sql())
        first = con.execute("""
            select past_runs,past_speed_mean,past_finish_mean,days_since
            from features where race_date='20230101'
        """).fetchall()
        same_day = con.execute("""
            select past_runs,past_speed_mean,past_speed_365d,days_since
            from features where race_date='20240101' order by race_id
        """).fetchall()
        next_day = con.execute("""
            select clock_seconds,past_runs,round(past_speed_mean,6),
              round(past_speed_365d,6),days_since
            from features where race_date='20240102'
        """).fetchall()
    assert first == [(0, None, None, None)]
    assert same_day == [(1, 20.0, 20.0, 365), (1, 20.0, 20.0, 365)]
    assert next_day == [(83.4, 3, 12.222222, 8.333333, 1)]


def test_invalid_clocks_and_unfinished_results_are_not_speed_observations() -> None:
    with duckdb.connect() as con:
        con.execute("""
            create table input_rows as
            select 'nar' as source,race_date,'83' as keibajo_code,'01' as race_bango,
              'h1' as ketto_toroku_bango,soha_time,finish_position,
              '01' as umaban,'200' as kyori,'10' as shusso_tosu,'4' as barei,
              '90' as track_code,'1' as seibetsu_code,'1' as babajotai_code_dirt,
              'j' as kishumei_ryakusho,'t' as chokyoshimei_ryakusho
            from (values ('20240101','1600','1'),('20240102','1234','0'),
              ('20240103','0000',NULL)) t(race_date,soha_time,finish_position)
        """)
        result = con.execute(
            "select category,clock_seconds,past_runs,past_speed_mean,finish from ("
            + build_history_feature_sql("input_rows")
            + ") order by race_date"
        ).fetchall()
    assert result == [
        ("ban-ei", None, 0, None, 1),
        ("ban-ei", 83.4, 1, None, None),
        ("ban-ei", None, 1, None, None),
    ]
