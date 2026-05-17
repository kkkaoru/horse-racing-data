#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append race-internal relative features to an existing finish-position
feature parquet directory without re-running the full DuckDB pipeline.

Run with:
  .venv/bin/python src/scripts/finish-position-features/add-race-internal-features.py \
    --input-dir tmp/finish-position-features-parquet \
    --output-dir tmp/finish-position-features-parquet-jra-v2
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
NIGE_PRESSURE_WEIGHT = 2.0
SENKOU_PRESSURE_WEIGHT = 1.0
NIGE_CANDIDATE_THRESHOLD = 0.4
PURE_NIGE_THRESHOLD = 0.7
LAYOFF_DAYS_THRESHOLD = 90


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_race_internal_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base_features as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    )
    select
      b.*,
      rank() over race_by_speed_avg_asc as speed_index_avg_5_rank_in_race,
      rank() over race_by_speed_best_asc as speed_index_best_5_rank_in_race,
      rank() over race_by_jockey_recent_desc as jockey_recent_win_rate_rank_in_race,
      rank() over race_by_trainer_career_desc as trainer_career_win_rate_rank_in_race,
      rank() over race_by_pedigree_desc as pedigree_score_for_race_rank_in_race,
      rank() over race_by_same_distance_desc as same_distance_win_rate_rank_in_race,
      b.speed_index_avg_5 - avg(b.speed_index_avg_5) over race_partition
        as speed_index_avg_5_diff_from_race_avg,
      b.jockey_recent_win_rate - avg(b.jockey_recent_win_rate) over race_partition
        as jockey_recent_win_rate_diff_from_race_avg,
      b.pedigree_score_for_race - avg(b.pedigree_score_for_race) over race_partition
        as pedigree_score_diff_from_race_avg,
      sum(coalesce(b.past_nige_rate_self, 0)) over race_partition - coalesce(b.past_nige_rate_self, 0)
        as field_nige_pressure,
      sum(coalesce(b.past_senkou_rate_self, 0)) over race_partition - coalesce(b.past_senkou_rate_self, 0)
        as field_senkou_pressure,
      sum(coalesce(b.past_sashi_rate_self, 0)) over race_partition - coalesce(b.past_sashi_rate_self, 0)
        as field_sashi_pressure,
      sum(coalesce(b.past_oikomi_rate_self, 0)) over race_partition - coalesce(b.past_oikomi_rate_self, 0)
        as field_oikomi_pressure,
      (sum(coalesce(b.past_nige_rate_self, 0)) over race_partition - coalesce(b.past_nige_rate_self, 0)) * {NIGE_PRESSURE_WEIGHT}
        + (sum(coalesce(b.past_senkou_rate_self, 0)) over race_partition - coalesce(b.past_senkou_rate_self, 0)) * {SENKOU_PRESSURE_WEIGHT}
        as field_pace_index,
      (sum(case when b.past_nige_rate_self > {NIGE_CANDIDATE_THRESHOLD} then 1 else 0 end) over race_partition)
        - case when b.past_nige_rate_self > {NIGE_CANDIDATE_THRESHOLD} then 1 else 0 end
        as field_nige_candidate_count,
      b.past_nige_rate_self - (
        (sum(coalesce(b.past_nige_rate_self, 0)) over race_partition - coalesce(b.past_nige_rate_self, 0))
        / nullif(count(b.past_nige_rate_self) over race_partition - case when b.past_nige_rate_self is null then 0 else 1 end, 0)
      ) as self_nige_rate_minus_field_avg,
      b.umaban_norm * coalesce(b.past_nige_rate_self, 0) as umaban_x_nige_history,
      avg(b.speed_index_avg_5) over race_partition as field_avg_speed_index,
      max(b.speed_index_best_5) over race_partition as field_top_speed_index,
      avg(b.career_win_rate) over race_partition as field_avg_career_win_rate,
      max(b.past_corner_1_norm_avg_5) over race_partition as field_max_past_corner_1_norm,
      min(b.past_corner_1_norm_avg_5) over race_partition as field_min_past_corner_1_norm,
      max(b.past_corner_1_norm_avg_5) over race_partition
        - min(b.past_corner_1_norm_avg_5) over race_partition as field_spread_past_corner_1_norm,
      case
        when (
          sum(case when b.past_nige_rate_self > {PURE_NIGE_THRESHOLD} then 1 else 0 end) over race_partition
          - case when b.past_nige_rate_self > {PURE_NIGE_THRESHOLD} then 1 else 0 end
        ) > 0 then 1 else 0
      end as field_has_pure_nige_horse,
      case
        when b.days_since_last_race is null then null
        when b.days_since_last_race > {LAYOFF_DAYS_THRESHOLD} then 1 else 0
      end as is_returning_from_layoff,
      case
        when b.days_since_last_race is null then null
        else ln(cast(b.days_since_last_race as double) + 1.0)
      end as days_since_last_race_log
    from base_features b
    window
      race_partition as (partition by b.{RACE_PARTITION.replace(", ", ", b.")}),
      race_by_speed_avg_asc as (
        partition by b.{RACE_PARTITION.replace(", ", ", b.")}
        order by b.speed_index_avg_5 asc nulls last
      ),
      race_by_speed_best_asc as (
        partition by b.{RACE_PARTITION.replace(", ", ", b.")}
        order by b.speed_index_best_5 asc nulls last
      ),
      race_by_jockey_recent_desc as (
        partition by b.{RACE_PARTITION.replace(", ", ", b.")}
        order by b.jockey_recent_win_rate desc nulls last
      ),
      race_by_trainer_career_desc as (
        partition by b.{RACE_PARTITION.replace(", ", ", b.")}
        order by b.trainer_career_win_rate desc nulls last
      ),
      race_by_pedigree_desc as (
        partition by b.{RACE_PARTITION.replace(", ", ", b.")}
        order by b.pedigree_score_for_race desc nulls last
      ),
      race_by_same_distance_desc as (
        partition by b.{RACE_PARTITION.replace(", ", ", b.")}
        order by b.same_distance_win_rate desc nulls last
      )
    """


def write_partitioned(con: duckdb.DuckDBPyConnection, sql: str, output_dir: Path) -> None:
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
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
