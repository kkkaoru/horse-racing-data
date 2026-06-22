"""Append jockey × venue × distance_band × surface triple-interaction features
(JRA only — venue=06 Nakayama signal, all JRA rows included).

Adds two new per-horse-per-race columns computed from the horse's jockey's
prior ride history segmented by (keibajo_code, distance_band, surface):

- ``jc_win_rate``   : jockey career win rate at (venue × dband × surface)
                      prior to the target race date (strictly causal).
- ``jc_avg_finish`` : jockey career average finish position at the same
                      triple combination.
- ``jc_support_n``  : number of prior finishes in the combination
                      (non-NULL denominator for downstream reliability check).

All three are NULL when the jockey has no prior rides in the combination
(new jockey, new venue, debut season, etc.).  GBDT boosters (CatBoost,
XGBoost) treat NULL inputs via their native missing-value routing, so no
imputation is applied here.

Distance bands (mirrors jockey_triple_build.py):
  sprint       : kyori <= 1400 m
  mile         : 1401-1800 m
  intermediate : 1801-2200 m
  long         : > 2200 m

Surface (track_code from jvd_ra):
  turf  : track_code 10-22
  dirt  : track_code 23-29
  other : all other codes (obstacle / rare codes — feature will be NULL
          for practical purposes because the combination has few priors)

Data leakage prevention: the ``jvd_se`` history rows are aggregated with
a strictly-before-date window (``hist_date < target race_date``).
Same-day races from OTHER events are also excluded (the aggregation groups
by ``ketto_toroku_bango`` + race tuple, so same-day own-race is excluded
by the strict < check; cross-race same-day would still be included but
is negligible for jockey aggregate stats).

JRA only: ``jvd_se`` / ``jvd_ra`` source tables.  NAR / Ban-ei have no
verified signal for this feature (jockey_triple_subgroup_eval probed
JRA only).  NAR and Ban-ei rows in the input pass through unchanged
(all three columns will be NULL for them).

Usage
-----
Run as a post-processor after the existing v8 JRA chain::

    uv run python src/scripts/finish-position-features/add_jockey_triple_features.py \\
        --input-dir tmp/feat-jra-v8-iter22-etop2 \\
        --output-dir tmp/feat-jra-v8-iter23-jockey-triple \\
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

# JRA keibajo_code range (01-10).
JRA_KEIBAJO_REGEXP: str = "^0[1-9]$|^10$"

# Distance band boundaries (metres).
SPRINT_MAX: int = 1400
MILE_MAX: int = 1800
INTERMEDIATE_MAX: int = 2200

# Surface track_code ranges (matching jockey_triple_build.py).
TURF_TRACK_CODE_MIN: int = 10
TURF_TRACK_CODE_MAX: int = 22
DIRT_TRACK_CODE_MIN: int = 23
DIRT_TRACK_CODE_MAX: int = 29


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_jockey_triple_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument(
        "--history-from-year",
        type=int,
        default=2005,
        help="stage jvd_se history from this kaisai_nen (>= 2 years before "
        "the oldest target year so early targets have a populated window)",
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: _DuckDBConnectionLike, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def dband_case_sql(kyori_col: str) -> str:
    """CASE expression mapping kyori (metres) to distance_band label."""
    return (
        f"case "
        f"when {kyori_col} <= {SPRINT_MAX} then 'sprint' "
        f"when {kyori_col} <= {MILE_MAX} then 'mile' "
        f"when {kyori_col} <= {INTERMEDIATE_MAX} then 'intermediate' "
        f"else 'long' end"
    )


def surface_case_sql(track_code_col: str) -> str:
    """CASE expression mapping track_code to surface label."""
    return (
        f"case "
        f"when {track_code_col} between {TURF_TRACK_CODE_MIN} and {TURF_TRACK_CODE_MAX} then 'turf' "
        f"when {track_code_col} between {DIRT_TRACK_CODE_MIN} and {DIRT_TRACK_CODE_MAX} then 'dirt' "
        f"else 'other' end"
    )


def stage_jockey_history(con: _DuckDBConnectionLike, history_from_year: int) -> None:
    """Stage per-jockey per-ride history rows from jvd_se + jvd_ra.

    Keeps only JRA races with a valid numeric finish (kakutei_chakujun),
    a non-empty kishu_code, and parseable kyori + track_code from jvd_ra.
    Each row contributes one ride to the jockey's expanding-window aggregate.
    """
    dband_sql = dband_case_sql("try_cast(nullif(trim(ra.kyori), '') as integer)")
    surface_sql = surface_case_sql("try_cast(nullif(trim(ra.track_code), '') as integer)")
    con.execute(
        f"""
        create or replace temp table jockey_hist as
        select
          nullif(trim(se.kishu_code), '')                                  as kishu_code,
          se.keibajo_code                                                   as keibajo_code,
          {dband_sql}                                                       as dband,
          {surface_sql}                                                     as surface,
          se.kaisai_nen || se.kaisai_tsukihi                               as hist_date,
          try_cast(nullif(trim(se.kakutei_chakujun), '') as integer)        as finish
        from pg.jvd_se se
        join pg.jvd_ra ra
          on ra.kaisai_nen      = se.kaisai_nen
         and ra.kaisai_tsukihi  = se.kaisai_tsukihi
         and ra.keibajo_code    = se.keibajo_code
         and ra.race_bango      = se.race_bango
        where regexp_matches(se.keibajo_code, '{JRA_KEIBAJO_REGEXP}')
          and cast(se.kaisai_nen as integer) >= {history_from_year}
          and se.kishu_code is not null
          and trim(se.kishu_code) != ''
          and try_cast(nullif(trim(se.kakutei_chakujun), '') as integer) >= 1
          and try_cast(nullif(trim(ra.kyori), '') as integer) > 0
          and try_cast(nullif(trim(ra.track_code), '') as integer) is not null
        """
    )
    con.execute(
        "create index jockey_hist_idx on jockey_hist (kishu_code, keibajo_code, dband, surface, hist_date)"
    )


def stage_base_entries(con: _DuckDBConnectionLike, input_glob: str) -> None:
    """Stage target race-entry identifiers + jockey codes from the feature parquet.

    Joins jvd_se to recover kishu_code for the target races (the feature
    store does not carry kishu_code — it is an entity identifier, not a
    feature, so the trainer layer drops it with ``exclude``).
    """
    con.execute(
        f"""
        create or replace temp table base_entries as
        select
          b.source,
          b.kaisai_nen,
          b.kaisai_tsukihi,
          b.keibajo_code,
          b.race_bango,
          b.ketto_toroku_bango,
          b.race_date,
          b.race_year,
          nullif(trim(se.kishu_code), '') as kishu_code,
          b.kyori,
          b.track_code
        from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true) b
        left join pg.jvd_se se
          on se.kaisai_nen          = b.kaisai_nen
         and se.kaisai_tsukihi      = b.kaisai_tsukihi
         and se.keibajo_code        = b.keibajo_code
         and se.race_bango          = b.race_bango
         and se.ketto_toroku_bango  = b.ketto_toroku_bango
        where b.source = 'jra'
        """
    )
    con.execute(
        "create index base_entries_idx on base_entries (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def stage_jockey_triple_agg(con: _DuckDBConnectionLike) -> None:
    """Compute causal jockey triple stats per target entry.

    For each target entry the aggregate counts only jockey_hist rows whose
    hist_date is strictly before the target race_date (causal window).
    """
    dband_sql = dband_case_sql("try_cast(nullif(trim(b.kyori), '') as integer)")
    surface_sql = surface_case_sql("try_cast(nullif(trim(b.track_code), '') as integer)")
    con.execute(
        f"""
        create or replace temp table jockey_triple_agg as
        select
          b.source,
          b.kaisai_nen,
          b.kaisai_tsukihi,
          b.keibajo_code,
          b.race_bango,
          b.ketto_toroku_bango,
          count(h.finish)                                                    as jc_support_n,
          case when count(h.finish) > 0
               then sum(case when h.finish = 1 then 1.0 else 0.0 end) / count(h.finish)
               else null end                                                 as jc_win_rate,
          case when count(h.finish) > 0
               then avg(h.finish::double)
               else null end                                                 as jc_avg_finish
        from base_entries b
        left join jockey_hist h
          on h.kishu_code    = b.kishu_code
         and h.keibajo_code  = b.keibajo_code
         and h.dband         = {dband_sql}
         and h.surface       = {surface_sql}
         and h.hist_date     < b.race_date
        where b.kishu_code is not null
        group by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango
        """
    )
    con.execute(
        "create index jockey_triple_agg_idx on jockey_triple_agg (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str) -> str:
    """LEFT JOIN the three jc_* columns onto the full base parquet.

    Non-JRA rows (NAR / Ban-ei) pass through with NULL values for all three
    new columns because ``jockey_triple_agg`` contains only JRA entries.
    JRA entries without a kishu_code (should not happen in production but
    handled defensively) also emit NULL.
    """
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)
    )
    select
      b.*,
      jt.jc_win_rate,
      jt.jc_avg_finish,
      jt.jc_support_n
    from base b
    left join jockey_triple_agg jt
      on jt.source               = b.source
     and jt.kaisai_nen           = b.kaisai_nen
     and jt.kaisai_tsukihi       = b.kaisai_tsukihi
     and jt.keibajo_code         = b.keibajo_code
     and jt.race_bango           = b.race_bango
     and jt.ketto_toroku_bango   = b.ketto_toroku_bango
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
    stage_jockey_history(con, args.history_from_year)
    stage_base_entries(con, input_glob)
    stage_jockey_triple_agg(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
