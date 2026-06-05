#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append static course numerical features (v8 iter14 layer, JRA-only).

Motivation:
  Iter 14 (v8 JRA) introduced seven course-level numerical attributes that
  encode the physical / historical layout of each (keibajo, kyori, track)
  course: final straight length, elevation diff, distance to first corner,
  corner count, full-gate count, and good/heavy-track nige rentai rates.
  These are static per-course constants, joined via a baked lookup parquet so
  no PG read is needed. Iter 14 tested at +0.16pp on JRA top1/place3 (first
  JRA accept since iter 9) per
  ``docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V8_PRODUCTION.md``.

Features added (per horse x race) — exactly 7 columns:
  - course_elevation_diff_m
  - course_final_straight_m
  - course_dist_to_first_corner_m
  - course_corner_count
  - course_full_gate_count
  - course_good_track_nige_rentai_rate_pct
  - course_heavy_track_nige_rentai_rate_pct

Lookup parquet (``--course-lookup``) carries 119 (keibajo, kyori, track) rows.
Join is many-to-one on those three keys; rows whose (keibajo, kyori, track)
have no lookup entry get NULL for all seven columns (XGBoost/CatBoost handle
NULL as missing). The trained iter14 model never saw rs_*/course features
populated for NAR rows (NAR was scored by iter12, not iter14), so this layer
only runs for JRA in the production chain — see
``predict_lib.pipeline_args.LAYER_CHAIN``.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb

from _resource_defaults import add_resource_args, apply_to_connection

COURSE_FEATURE_NAMES: tuple[str, ...] = (
    "course_elevation_diff_m",
    "course_final_straight_m",
    "course_dist_to_first_corner_m",
    "course_corner_count",
    "course_full_gate_count",
    "course_good_track_nige_rentai_rate_pct",
    "course_heavy_track_nige_rentai_rate_pct",
)
JOIN_KEYS: tuple[str, ...] = ("keibajo_code", "kyori", "track_code")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_course_numerical_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--course-lookup",
        type=Path,
        required=True,
        help="Path to the baked course-numerical-features lookup parquet.",
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def stage_course_lookup(con: duckdb.DuckDBPyConnection, lookup_path: Path) -> None:
    """Load + dedupe the lookup parquet on (keibajo_code, kyori, track_code).

    The raw lookup has 119 rows that include a couple of NaN-vs-filled splits
    for the same key; ``any_value(col)`` (DuckDB) already skips NULLs by
    default, so it collapses each key to a single non-null per attribute
    lossly when both halves are non-conflicting (validated in iter14 build).
    The earlier ``ignore nulls`` modifier is not accepted by DuckDB's parser
    for non-window aggregates (RESPECT/IGNORE NULLS is window-only) and is
    therefore omitted.
    """
    agg_cols = ", ".join(
        f"any_value({name}) as {name}" for name in COURSE_FEATURE_NAMES
    )
    join_cols = ", ".join(JOIN_KEYS)
    con.execute(
        f"""
        create or replace temp table course_lookup as
        select {join_cols}, {agg_cols}
        from read_parquet('{lookup_path.as_posix()}')
        group by {join_cols}
        """
    )
    con.execute(
        f"create index course_lookup_idx on course_lookup ({join_cols})"
    )


def append_features_sql(input_glob: str) -> str:
    """Left-join the lookup, preserving every input row.

    JOIN keys are cast to STRING / INT32 on both sides so a parquet schema with
    LargeString or Int64 still joins cleanly with the lookup's StringDtype +
    int32 dtypes (matches iter14_build_features.py defensive casts).
    """
    selected_attrs = ", ".join(f"cl.{name}" for name in COURSE_FEATURE_NAMES)
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    base_typed as (
      select
        b.*,
        cast(b.keibajo_code as varchar) as join_keibajo_code,
        cast(b.kyori as integer) as join_kyori,
        cast(b.track_code as varchar) as join_track_code
      from base b
    ),
    lookup_typed as (
      select
        cast(cl.keibajo_code as varchar) as join_keibajo_code,
        cast(cl.kyori as integer) as join_kyori,
        cast(cl.track_code as varchar) as join_track_code,
        {selected_attrs}
      from course_lookup cl
    ),
    joined as (
      select
        bt.* exclude (join_keibajo_code, join_kyori, join_track_code),
        {", ".join(f"lt.{name}" for name in COURSE_FEATURE_NAMES)}
      from base_typed bt
      left join lookup_typed lt
        on lt.join_keibajo_code = bt.join_keibajo_code
        and lt.join_kyori = bt.join_kyori
        and lt.join_track_code = bt.join_track_code
    )
    select * from joined
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
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    stage_course_lookup(con, args.course_lookup)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
