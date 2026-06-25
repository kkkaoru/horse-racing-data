#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append similar-race context features (v8 add-on layer).

For each TARGET race the script finds *similar past races* — matching on
(source, keibajo_code, surface, kyori_band, season_band, class_group) with a
hierarchical fallback so every target race gets a populated similar-race pool:

  level 1 (exact)        : all 6 dims match            (use if >= MIN_SIMILAR)
  level 2 (relax season) : drop season_band            (use if >= MIN_SIMILAR)
  level 3 (relax venue)  : drop keibajo_code + season  (use if >= MIN_SIMILAR)
  level 4 (broad)        : surface + kyori_band only    (always enough)

Two phases of features are appended:

Phase 1 — race-level dynamic odds signals (every horse in a target race shares
the same value because they describe the race environment, not the horse):
  - sim_odds_rank_correlation     : time-decay weighted mean corr(popularity, finish)
  - sim_fav_win_rate              : weighted mean of "did the favourite win?"
  - sim_odds_correlation_variance : variance of the per-race odds-finish corr
  - sim_race_count                : number of similar races found
  - sim_match_level               : the similarity level used (1=exact .. 4=broad)

Phase 2 — per-horse entity stats inside the same similar-race pool:
  jockey  : sim_jockey_win_rate / sim_jockey_place_rate / sim_jockey_ride_count
  trainer : sim_trainer_win_rate / sim_trainer_place_rate / sim_trainer_ride_count
  sire    : sim_sire_win_rate / sim_sire_place_rate / sim_sire_offspring_count
  damsire : sim_damsire_win_rate / sim_damsire_offspring_count
  owner   : sim_owner_win_rate / sim_owner_race_count
  umaban  : sim_umaban_zone_win_rate (same inner/middle/outer tercile)

Data-leakage prevention: every similar race used for a target has a
``race_date`` strictly less than the target's ``race_date`` and within
HISTORY_LOOKBACK_YEARS years.  Time decay ``exp(-0.001 * days_diff)`` down-weights
old races.

Run with::

  uv run python src/scripts/finish-position-features/add-similar-race-features.py \\
    --input-dir tmp/feat-jra-v9-new \\
    --output-dir tmp/feat-jra-v9-similar \\
    --category jra \\
    --pg-url postgresql://horse_racing:***@127.0.0.1:15432/horse_racing
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path
from typing import Protocol

import duckdb

from _resource_defaults import add_resource_args, apply_to_connection


class _DuckDBConnectionLike(Protocol):
    def execute(self, query: str) -> object: ...


DEFAULT_PG_URL: str = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"

# Minimum number of similar races a level must yield before it is accepted;
# below this the next (broader) fallback level is used.
MIN_SIMILAR: int = 30

# Only look back this many years from the target race when pooling similar races.
HISTORY_LOOKBACK_YEARS: int = 10

# Time-decay rate per day between target and similar race (exp(-k * days)).
TIME_DECAY_RATE: float = 0.001

# kyori_band boundaries (metres) — must match finish_position_features_duckdb.
KYORI_BAND_SPRINT_MAX: int = 1300
KYORI_BAND_MILE_MAX: int = 1700
KYORI_BAND_INTERMEDIATE_MAX: int = 2200

# Ban-ei is always keibajo_code '83'; venue matching is therefore trivial.
BAN_EI_KEIBAJO_CODE: str = "83"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_similar_race_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--category",
        choices=("jra", "nar", "ban-ei"),
        default="jra",
        help="jra -> pg.jvd_se, nar/ban-ei -> pg.nvd_se",
    )
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20000101")
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: _DuckDBConnectionLike, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def surface_sql(track_code_col: str) -> str:
    """left(track_code, 1): '1'=turf, '2'=dirt, '3'=obstacle (Ban-ei -> '0')."""
    return f"left(coalesce({track_code_col}, ''), 1)"


def kyori_band_sql(kyori_col: str) -> str:
    """Map kyori (metres) to a 0-3 distance band matching the base pipeline."""
    return (
        f"case "
        f"when {kyori_col} is null then null "
        f"when {kyori_col} <= {KYORI_BAND_SPRINT_MAX} then 0 "
        f"when {kyori_col} <= {KYORI_BAND_MILE_MAX} then 1 "
        f"when {kyori_col} <= {KYORI_BAND_INTERMEDIATE_MAX} then 2 "
        f"else 3 end"
    )


def season_band_sql(tsukihi_col: str) -> str:
    """((month + 9) % 12) // 3 -> 0..3 season band from kaisai_tsukihi 'MMDD'."""
    return (
        f"case when {tsukihi_col} is null or length({tsukihi_col}) < 2 then null "
        f"else ((cast(substr({tsukihi_col}, 1, 2) as int) + 9) % 12) // 3 end"
    )


def class_group_sql(category: str, joken_col: str, meisho_col: str) -> str:
    """Class grouping key.

    JRA / Ban-ei carry a usable kyoso_joken_code, so we use it directly. NAR
    reports kyoso_joken_code='000' for every race (the real class lives in the
    free-text meisho), so for NAR we derive a coarse class token from the
    kyoso_joken_meisho via the dominant class character.
    """
    if category == "nar":
        return (
            f"case "
            f"when regexp_matches({meisho_col}, 'ＯＰ') then 'OP' "
            f"when regexp_matches({meisho_col}, '新馬') then 'NEW' "
            f"when regexp_matches({meisho_col}, '未勝利|未出走') then 'MUKATSU' "
            f"when regexp_matches({meisho_col}, '２歳|2歳') then '2YO' "
            f"when regexp_matches({meisho_col}, '３歳|3歳') then '3YO' "
            f"when regexp_matches({meisho_col}, 'Ａ') then 'A' "
            f"when regexp_matches({meisho_col}, 'Ｂ') then 'B' "
            f"when regexp_matches({meisho_col}, 'Ｃ') then 'C' "
            f"else 'other' end"
        )
    return f"coalesce(nullif(trim({joken_col}), ''), '000')"


def stage_similar_history(
    con: _DuckDBConnectionLike, from_date: str, category: str
) -> None:
    """Stage the per-entry historical race rows used to build the similar-race pool.

    One row per (race, horse) carrying the race-similarity key, the finish
    outcome, the popularity rank, and the entity codes (jockey/trainer/owner/
    sire/damsire/umaban) used by the Phase-2 per-horse aggregates.
    """
    ra_table = "pg.jvd_ra" if category == "jra" else "pg.nvd_ra"
    um_table = "pg.jvd_um" if category == "jra" else "pg.nvd_um"
    source_value = "jra" if category == "jra" else "nar"
    if category == "ban-ei":
        keibajo_predicate = f"rec.keibajo_code = '{BAN_EI_KEIBAJO_CODE}'"
    elif category == "nar":
        keibajo_predicate = (
            f"(rec.keibajo_code is null or rec.keibajo_code <> '{BAN_EI_KEIBAJO_CODE}')"
        )
    else:
        keibajo_predicate = "true"
    class_group = class_group_sql(category, "rec.kyoso_joken_code", "ra.kyoso_joken_meisho")
    con.execute(
        f"""
        create or replace temp table similar_history as
        select
          rec.source,
          rec.race_date,
          rec.kaisai_nen,
          rec.kaisai_tsukihi,
          rec.keibajo_code,
          rec.race_bango,
          rec.ketto_toroku_bango,
          rec.finish_position,
          rec.shusso_tosu,
          cast(rec.tansho_ninkijun as int) as tansho_ninkijun,
          rec.umaban,
          {surface_sql("rec.track_code")} as surface,
          {kyori_band_sql("rec.kyori")} as kyori_band,
          {season_band_sql("rec.kaisai_tsukihi")} as season_band,
          {class_group} as class_group,
          nullif(trim(rec.kishumei_ryakusho), '') as kishumei_ryakusho,
          nullif(trim(rec.chokyoshimei_ryakusho), '') as chokyoshimei_ryakusho,
          nullif(trim(rec.banushimei), '') as banushimei,
          nullif(trim(um.ketto_joho_01b), '') as sire,
          nullif(trim(um.ketto_joho_05b), '') as damsire,
          case
            when rec.umaban is null or rec.shusso_tosu is null or rec.shusso_tosu < 1 then null
            when cast(rec.umaban as double) <= cast(rec.shusso_tosu as double) / 3.0 then 0
            when cast(rec.umaban as double) <= 2.0 * cast(rec.shusso_tosu as double) / 3.0 then 1
            else 2
          end as umaban_zone
        from pg.race_entry_corner_features rec
        left join {ra_table} ra
          on ra.kaisai_nen = rec.kaisai_nen
          and ra.kaisai_tsukihi = rec.kaisai_tsukihi
          and ra.keibajo_code = rec.keibajo_code
          and ra.race_bango = rec.race_bango
        left join {um_table} um
          on um.ketto_toroku_bango = rec.ketto_toroku_bango
        where rec.source = '{source_value}'
          and rec.race_date >= '{from_date}'
          and rec.finish_position is not null
          and {keibajo_predicate}
        """
    )
    con.execute(
        "create index similar_history_idx on similar_history "
        "(source, keibajo_code, surface, kyori_band, season_band, class_group, race_date)"
    )


def stage_race_summary(con: _DuckDBConnectionLike) -> None:
    """One row per historical race: the similarity key plus per-race odds stats."""
    con.execute(
        """
        create or replace temp table race_summary as
        select
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          any_value(surface) as surface,
          any_value(kyori_band) as kyori_band,
          any_value(season_band) as season_band,
          any_value(class_group) as class_group,
          case when isnan(corr(cast(tansho_ninkijun as double), cast(finish_position as double)))
               then null
               else corr(cast(tansho_ninkijun as double), cast(finish_position as double))
          end as odds_rank_corr,
          max(case when tansho_ninkijun = 1 and finish_position = 1 then 1 else 0 end) as fav_won,
          count(*) as field_size
        from similar_history
        where tansho_ninkijun is not null
        group by source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
        """
    )
    con.execute(
        "create index race_summary_idx on race_summary "
        "(source, keibajo_code, surface, kyori_band, season_band, class_group, race_date)"
    )


def stage_target_races(con: _DuckDBConnectionLike, input_glob: str) -> None:
    """Distinct target races from the feature parquet, carrying the similarity key.

    The parquet already exposes kyori_band / season_band / track_code / source /
    race_date, but the parquet's class_group and surface are recomputed here from
    the base columns so the key derivation is identical to the historical side.
    """
    con.execute(
        f"""
        create or replace temp table target_races as
        select distinct
          rs.source,
          rs.race_date,
          rs.kaisai_nen,
          rs.kaisai_tsukihi,
          rs.keibajo_code,
          rs.race_bango,
          rs.surface,
          rs.kyori_band,
          rs.season_band,
          rs.class_group
        from race_summary rs
        join (
          select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
          from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)
        ) p
          on p.source = rs.source
          and p.kaisai_nen = rs.kaisai_nen
          and p.kaisai_tsukihi = rs.kaisai_tsukihi
          and p.keibajo_code = rs.keibajo_code
          and p.race_bango = rs.race_bango
        """
    )
    con.execute(
        "create index target_races_idx on target_races "
        "(source, keibajo_code, surface, kyori_band, season_band, class_group, race_date)"
    )


def _level_match_predicate(level: int) -> str:
    """ON-clause predicate (history `h` vs target `t`) for a similarity level.

    All levels require source, surface, kyori_band match, a strictly-earlier
    race_date, and a within-lookback window. Higher levels relax dims.
    """
    base = (
        "h.source = t.source "
        "and h.surface = t.surface "
        "and h.kyori_band = t.kyori_band "
        "and h.race_date < t.race_date "
        f"and cast(h.kaisai_nen as int) >= cast(t.kaisai_nen as int) - {HISTORY_LOOKBACK_YEARS}"
    )
    if level == 1:
        return (
            base
            + " and h.keibajo_code = t.keibajo_code"
            + " and h.season_band = t.season_band"
            + " and h.class_group = t.class_group"
        )
    if level == 2:
        return (
            base
            + " and h.keibajo_code = t.keibajo_code"
            + " and h.class_group = t.class_group"
        )
    if level == 3:
        return base + " and h.class_group = t.class_group"
    return base


def stage_target_match_level(con: _DuckDBConnectionLike) -> None:
    """Resolve, per target race, the coarsest similarity level meeting MIN_SIMILAR.

    Counts distinct similar races at each level (1..3); the first level with
    >= MIN_SIMILAR wins, otherwise level 4 (broad) is used.
    """
    counts = ",\n          ".join(
        f"""count(distinct case when {_level_match_predicate(level)}
              then h.source || h.race_date || h.keibajo_code || h.race_bango end) as n{level}"""
        for level in (1, 2, 3)
    )
    con.execute(
        f"""
        create or replace temp table target_level_counts as
        select
          t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
          {counts}
        from target_races t
        left join race_summary h
          on h.source = t.source
          and h.surface = t.surface
          and h.kyori_band = t.kyori_band
          and h.race_date < t.race_date
          and cast(h.kaisai_nen as int) >= cast(t.kaisai_nen as int) - {HISTORY_LOOKBACK_YEARS}
        group by t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango
        """
    )
    con.execute(
        f"""
        create or replace temp table target_match_level as
        select
          source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          case
            when n1 >= {MIN_SIMILAR} then 1
            when n2 >= {MIN_SIMILAR} then 2
            when n3 >= {MIN_SIMILAR} then 3
            else 4
          end as sim_match_level
        from target_level_counts
        """
    )
    con.execute(
        "create index target_match_level_idx on target_match_level "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)"
    )


def _similar_pool_join_predicate() -> str:
    """ON-clause selecting the similar-race summary rows for a target's chosen level.

    `t` is target_races joined with its resolved sim_match_level (tml). The level
    is materialised as `t.sim_match_level`; the predicate fans out to the matching
    level's dims via OR-by-level so a single join serves all four levels.
    """
    return (
        "h.source = t.source "
        "and h.surface = t.surface "
        "and h.kyori_band = t.kyori_band "
        "and h.race_date < t.race_date "
        f"and cast(h.kaisai_nen as int) >= cast(t.kaisai_nen as int) - {HISTORY_LOOKBACK_YEARS} "
        "and ("
        "  (t.sim_match_level = 1 and h.keibajo_code = t.keibajo_code "
        "     and h.season_band = t.season_band and h.class_group = t.class_group)"
        "  or (t.sim_match_level = 2 and h.keibajo_code = t.keibajo_code "
        "     and h.class_group = t.class_group)"
        "  or (t.sim_match_level = 3 and h.class_group = t.class_group)"
        "  or (t.sim_match_level = 4)"
        ")"
    )


def stage_similar_pool(con: _DuckDBConnectionLike) -> None:
    """Materialise the (target race -> similar historical race) edges with weights.

    Each edge carries the time-decay weight and the historical race's odds stats,
    so Phase-1 race-level aggregates reduce to a weighted reduction over edges.
    """
    pred = _similar_pool_join_predicate()
    con.execute(
        f"""
        create or replace temp table similar_pool as
        select
          t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
          t.sim_match_level,
          h.race_date as sim_race_date,
          h.kaisai_nen as sim_kaisai_nen,
          h.kaisai_tsukihi as sim_kaisai_tsukihi,
          h.keibajo_code as sim_keibajo_code,
          h.race_bango as sim_race_bango,
          h.odds_rank_corr,
          h.fav_won,
          exp(
            -{TIME_DECAY_RATE} * (
              cast(strptime(t.race_date, '%Y%m%d') as date)
              - cast(strptime(h.race_date, '%Y%m%d') as date)
            )
          ) as decay_weight
        from (
          select tr.*, tml.sim_match_level
          from target_races tr
          join target_match_level tml
            on tml.source = tr.source
            and tml.kaisai_nen = tr.kaisai_nen
            and tml.kaisai_tsukihi = tr.kaisai_tsukihi
            and tml.keibajo_code = tr.keibajo_code
            and tml.race_bango = tr.race_bango
        ) t
        join race_summary h on {pred}
        """
    )
    con.execute(
        "create index similar_pool_idx on similar_pool "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)"
    )
    con.execute(
        "create index similar_pool_sim_idx on similar_pool "
        "(source, sim_kaisai_nen, sim_kaisai_tsukihi, sim_keibajo_code, sim_race_bango)"
    )


def stage_race_level_features(con: _DuckDBConnectionLike) -> None:
    """Phase-1 race-level odds features: one row per target race."""
    con.execute(
        """
        create or replace temp table sim_race_features as
        select
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          any_value(sim_match_level) as sim_match_level,
          count(*) as sim_race_count,
          sum(odds_rank_corr * decay_weight) / nullif(sum(case when odds_rank_corr is not null then decay_weight else 0 end), 0) as sim_odds_rank_correlation,
          sum(cast(fav_won as double) * decay_weight) / nullif(sum(decay_weight), 0) as sim_fav_win_rate,
          var_pop(odds_rank_corr) as sim_odds_correlation_variance
        from similar_pool
        group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
        """
    )
    con.execute(
        "create index sim_race_features_idx on sim_race_features "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)"
    )


def stage_entity_features(con: _DuckDBConnectionLike) -> None:
    """Phase-2 per-horse entity stats from the similar-race pool.

    The similar_pool gives, per target race, the set of similar historical races.
    Joining those back to similar_history yields every horse-result inside the
    pool; we then aggregate by the target horse's entity code (jockey/trainer/
    sire/damsire/owner/umaban_zone). One row per (target race, horse).
    """
    con.execute(
        """
        create or replace temp table pool_results as
        select
          sp.source, sp.kaisai_nen, sp.kaisai_tsukihi, sp.keibajo_code, sp.race_bango,
          sh.kishumei_ryakusho, sh.chokyoshimei_ryakusho, sh.banushimei,
          sh.sire, sh.damsire, sh.umaban_zone,
          sh.finish_position
        from similar_pool sp
        join similar_history sh
          on sh.source = sp.source
          and sh.kaisai_nen = sp.sim_kaisai_nen
          and sh.kaisai_tsukihi = sp.sim_kaisai_tsukihi
          and sh.keibajo_code = sp.sim_keibajo_code
          and sh.race_bango = sp.sim_race_bango
        """
    )
    for entity_col, table, win_alias, place_alias, count_alias, with_place in (
        ("kishumei_ryakusho", "sim_jockey_stats", "sim_jockey_win_rate", "sim_jockey_place_rate", "sim_jockey_ride_count", True),
        ("chokyoshimei_ryakusho", "sim_trainer_stats", "sim_trainer_win_rate", "sim_trainer_place_rate", "sim_trainer_ride_count", True),
        ("sire", "sim_sire_stats", "sim_sire_win_rate", "sim_sire_place_rate", "sim_sire_offspring_count", True),
        ("damsire", "sim_damsire_stats", "sim_damsire_win_rate", "sim_damsire_place_rate", "sim_damsire_offspring_count", True),
        ("banushimei", "sim_owner_stats", "sim_owner_win_rate", "sim_owner_place_rate", "sim_owner_race_count", False),
        ("umaban_zone", "sim_umaban_zone_stats", "sim_umaban_zone_win_rate", "sim_umaban_zone_place_rate", "sim_umaban_zone_count", False),
    ):
        place_select = (
            f", avg(case when finish_position between 1 and 3 then 1.0 else 0.0 end) as {place_alias}"
            if with_place
            else ""
        )
        con.execute(
            f"""
            create or replace temp table {table} as
            select
              source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, {entity_col},
              avg(case when finish_position = 1 then 1.0 else 0.0 end) as {win_alias},
              count(*) as {count_alias}
              {place_select}
            from pool_results
            where {entity_col} is not null
            group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, {entity_col}
            """
        )
        con.execute(
            f"create index {table}_idx on {table} "
            f"(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, {entity_col})"
        )


def stage_target_entities(con: _DuckDBConnectionLike, from_date: str, category: str) -> None:
    """The target horses' own entity codes (for joining the Phase-2 stats back).

    Read straight from PG (rec + um) WITHOUT a finish_position filter so that
    UPCOMING target races (finish_position still NULL) still receive their
    jockey / trainer / owner / sire / damsire / umaban_zone codes and therefore
    their Phase-2 entity stats.
    """
    se_table = "pg.jvd_um" if category == "jra" else "pg.nvd_um"
    source_value = "jra" if category == "jra" else "nar"
    if category == "ban-ei":
        keibajo_predicate = f"rec.keibajo_code = '{BAN_EI_KEIBAJO_CODE}'"
    elif category == "nar":
        keibajo_predicate = (
            f"(rec.keibajo_code is null or rec.keibajo_code <> '{BAN_EI_KEIBAJO_CODE}')"
        )
    else:
        keibajo_predicate = "true"
    con.execute(
        f"""
        create or replace temp table target_entities as
        select
          rec.source, rec.kaisai_nen, rec.kaisai_tsukihi, rec.keibajo_code, rec.race_bango,
          rec.ketto_toroku_bango,
          nullif(trim(rec.kishumei_ryakusho), '') as kishumei_ryakusho,
          nullif(trim(rec.chokyoshimei_ryakusho), '') as chokyoshimei_ryakusho,
          nullif(trim(rec.banushimei), '') as banushimei,
          nullif(trim(um.ketto_joho_01b), '') as sire,
          nullif(trim(um.ketto_joho_05b), '') as damsire,
          case
            when rec.umaban is null or rec.shusso_tosu is null or rec.shusso_tosu < 1 then null
            when cast(rec.umaban as double) <= cast(rec.shusso_tosu as double) / 3.0 then 0
            when cast(rec.umaban as double) <= 2.0 * cast(rec.shusso_tosu as double) / 3.0 then 1
            else 2
          end as umaban_zone
        from pg.race_entry_corner_features rec
        left join {se_table} um
          on um.ketto_toroku_bango = rec.ketto_toroku_bango
        where rec.source = '{source_value}'
          and rec.race_date >= '{from_date}'
          and {keibajo_predicate}
        """
    )
    con.execute(
        "create index target_entities_idx on target_entities "
        "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str) -> str:
    """LEFT JOIN all Phase-1 + Phase-2 columns onto the base parquet."""
    race_key = (
        "{a}.source = {b}.source "
        "and {a}.kaisai_nen = {b}.kaisai_nen "
        "and {a}.kaisai_tsukihi = {b}.kaisai_tsukihi "
        "and {a}.keibajo_code = {b}.keibajo_code "
        "and {a}.race_bango = {b}.race_bango"
    )
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)
    ),
    with_entities as (
      select b.*,
        te.kishumei_ryakusho as _sim_kishu,
        te.chokyoshimei_ryakusho as _sim_chokyoshi,
        te.banushimei as _sim_banushi,
        te.sire as _sim_sire,
        te.damsire as _sim_damsire,
        te.umaban_zone as _sim_umaban_zone
      from base b
      left join target_entities te
        on te.source = b.source
        and te.kaisai_nen = b.kaisai_nen
        and te.kaisai_tsukihi = b.kaisai_tsukihi
        and te.keibajo_code = b.keibajo_code
        and te.race_bango = b.race_bango
        and te.ketto_toroku_bango = b.ketto_toroku_bango
    )
    select
      we.* exclude (_sim_kishu, _sim_chokyoshi, _sim_banushi, _sim_sire, _sim_damsire, _sim_umaban_zone),
      srf.sim_odds_rank_correlation,
      srf.sim_fav_win_rate,
      srf.sim_odds_correlation_variance,
      coalesce(srf.sim_race_count, 0) as sim_race_count,
      srf.sim_match_level,
      js.sim_jockey_win_rate,
      js.sim_jockey_place_rate,
      js.sim_jockey_ride_count,
      ts.sim_trainer_win_rate,
      ts.sim_trainer_place_rate,
      ts.sim_trainer_ride_count,
      ss.sim_sire_win_rate,
      ss.sim_sire_place_rate,
      ss.sim_sire_offspring_count,
      ds.sim_damsire_win_rate,
      ds.sim_damsire_offspring_count,
      os.sim_owner_win_rate,
      os.sim_owner_race_count,
      uz.sim_umaban_zone_win_rate
    from with_entities we
    left join sim_race_features srf on {race_key.format(a="srf", b="we")}
    left join sim_jockey_stats js
      on {race_key.format(a="js", b="we")} and js.kishumei_ryakusho = we._sim_kishu
    left join sim_trainer_stats ts
      on {race_key.format(a="ts", b="we")} and ts.chokyoshimei_ryakusho = we._sim_chokyoshi
    left join sim_sire_stats ss
      on {race_key.format(a="ss", b="we")} and ss.sire = we._sim_sire
    left join sim_damsire_stats ds
      on {race_key.format(a="ds", b="we")} and ds.damsire = we._sim_damsire
    left join sim_owner_stats os
      on {race_key.format(a="os", b="we")} and os.banushimei = we._sim_banushi
    left join sim_umaban_zone_stats uz
      on {race_key.format(a="uz", b="we")} and uz.umaban_zone = we._sim_umaban_zone
    """


def write_partitioned(con: _DuckDBConnectionLike, sql: str, output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"copy ({sql}) to '{output_dir.as_posix()}' "
        "(format parquet, partition_by (race_year), overwrite_or_ignore true)"
    )


def main() -> None:
    args = parse_args()
    input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    install_and_attach_pg(con, args.pg_url)
    stage_similar_history(con, args.from_date, args.category)
    stage_race_summary(con)
    stage_target_races(con, input_glob)
    stage_target_match_level(con)
    stage_similar_pool(con)
    stage_race_level_features(con)
    stage_entity_features(con)
    stage_target_entities(con, args.from_date, args.category)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
