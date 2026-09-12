"""Causal, normalized history features for local-PG ranker experiments.

Historical race outcomes are observations, never current-race predictors.
All windows end the day before the target race, excluding same-day results.
"""

from __future__ import annotations

import re

from learning.race_time import encoded_race_time_seconds_sql


def build_history_feature_sql(table: str = "history") -> str:
    """Return a DuckDB query over the exported local-PG history relation.

    Keep cross-venue/source records through this transform; select training
    scopes afterwards. Legacy speed is an explicitly named diagnostic arm,
    never the canonical unit. Invalid or unfinished results provide no label.
    """
    if re.fullmatch(r"[a-zA-Z_][a-zA-Z_0-9]*", table) is None:
        raise ValueError("History table must be a simple SQL identifier")
    seconds = encoded_race_time_seconds_sql("soha_time")
    return f"""
    with normalized as (
      select *,
        strptime(race_date, '%Y%m%d')::date as observed_date,
        source || '-' || race_date || '-' || keibajo_code || '-' || race_bango as race_id,
        ketto_toroku_bango as horse_id,
        keibajo_code as venue,
        case when source='nar' and keibajo_code='83' then 'ban-ei' else source end as category,
        try_cast(kyori as double) as distance_m,
        try_cast(shusso_tosu as double) as field_size,
        try_cast(barei as double) as age,
        case when try_cast(finish_position as int)>0
          then try_cast(finish_position as int) end as finish,
        {seconds} as clock_seconds
      from {table}
    ), outcomes as (
      select *,
        case when finish is not null then (finish=1)::int end as won,
        case when finish is not null and field_size>1
          then (finish-1)/(field_size-1) end as finish_fraction,
        case when finish is not null and distance_m>0
          then distance_m / nullif(clock_seconds,0) end as speed_mps,
        case when finish is not null and distance_m>0 and clock_seconds is not null
          then distance_m / (try_cast(soha_time as double)/10.0) end as legacy_speed
      from normalized
    )
    select race_id, horse_id, race_date, observed_date, category, venue,
      try_cast(umaban as int) as horse_number, finish,
      distance_m, field_size, age,
      try_cast(track_code as int) as track_code,
      try_cast(seibetsu_code as int) as sex_code,
      try_cast(babajotai_code_dirt as int) as going,
      extract(month from observed_date) as month,
      extract(year from observed_date) as year,
      clock_seconds,
      count(finish) over horse_past as past_runs,
      avg(finish_fraction) over horse_past as past_finish_mean,
      avg(won) over horse_past as past_win_rate,
      avg(speed_mps) over horse_past as past_speed_mean,
      avg(speed_mps) over horse_year as past_speed_365d,
      avg(legacy_speed) over horse_past as past_legacy_speed_mean,
      avg(legacy_speed) over horse_year as past_legacy_speed_365d,
      date_diff('day',max(observed_date) over horse_past,observed_date) as days_since,
      avg(won) over jockey_past as jockey_past_win_rate,
      avg(won) over trainer_past as trainer_past_win_rate
    from outcomes
    window
      horse_past as (partition by horse_id order by observed_date
        range between unbounded preceding and interval 1 day preceding),
      horse_year as (partition by horse_id order by observed_date
        range between interval 365 day preceding and interval 1 day preceding),
      jockey_past as (partition by source,kishumei_ryakusho order by observed_date
        range between unbounded preceding and interval 1 day preceding),
      trainer_past as (partition by source,chokyoshimei_ryakusho order by observed_date
        range between unbounded preceding and interval 1 day preceding)
    """
