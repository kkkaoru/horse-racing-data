#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Race-internal aggregation features for ban-ei v2 parquet → v3.

For each high-impact numeric feature, compute per-horse:
  - rank within race
  - diff from race average
  - z-score within race

This is the equivalent of add-race-internal-features.py for JRA/NAR, applied
to ban-ei v1 which was missing this layer. Adds ~90 columns (30 features ×
3 aggregations).

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/finish-position-features/add-ban-ei-internal-features.py \\
    --input-dir tmp/feat-ban-ei-v2 \\
    --output-dir tmp/feat-ban-ei-v3
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb

RACE_PARTITION_BY = "b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango"

# 30 high-signal features (selected to keep parquet bloat manageable)
TARGET_FEATURES: tuple[str, ...] = (
    "speed_index_avg_5",
    "speed_index_best_5",
    "kohan3f_avg_5",
    "corner_pass_avg_5",
    "career_win_rate",
    "career_place_rate",
    "career_top1_count",
    "same_keibajo_win_rate",
    "same_distance_win_rate",
    "same_grade_win_rate",
    "jockey_career_win_rate",
    "jockey_recent_win_rate",
    "jockey_keibajo_win_rate",
    "jockey_distance_win_rate",
    "jockey_horse_pair_win_rate",
    "trainer_career_win_rate",
    "trainer_keibajo_win_rate",
    "trainer_distance_win_rate",
    "trainer_horse_win_rate",
    "sire_distance_win_rate",
    "sire_track_win_rate",
    "dam_sire_distance_win_rate",
    "pedigree_score_for_race",
    "popularity_score",
    "odds_score",
    "weight_avg_5",
    "last_race_finish_norm",
    "last_3_avg_finish_norm",
    "finish_trend_5",
    "days_since_last_race",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_ban_ei_internal_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def build_aggregation_sql(features: tuple[str, ...]) -> str:
    """Generate window aggregation SQL clauses for each feature."""
    chunks: list[str] = []
    for feat in features:
        chunks.append(
            f"rank() over (partition by {RACE_PARTITION_BY} order by b.{feat} desc nulls last) "
            f"as {feat}_rank_in_race"
        )
        chunks.append(
            f"b.{feat} - avg(b.{feat}) over (partition by {RACE_PARTITION_BY}) "
            f"as {feat}_diff_from_race_avg"
        )
        chunks.append(
            f"(b.{feat} - avg(b.{feat}) over (partition by {RACE_PARTITION_BY})) "
            f"/ nullif(stddev_samp(b.{feat}) over (partition by {RACE_PARTITION_BY}), 0) "
            f"as {feat}_zscore_in_race"
        )
    return ",\n      ".join(chunks)


def append_features_sql(input_glob: str, features: tuple[str, ...]) -> str:
    aggregations = build_aggregation_sql(features)
    return f"""
    select
      b.*,
      {aggregations}
    from read_parquet('{input_glob}', hive_partitioning=true) b
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
    con.execute("SET memory_limit='16GB'")
    con.execute("SET threads TO 6")
    con.execute("SET preserve_insertion_order=false")
    write_partitioned(con, append_features_sql(input_glob, TARGET_FEATURES), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
