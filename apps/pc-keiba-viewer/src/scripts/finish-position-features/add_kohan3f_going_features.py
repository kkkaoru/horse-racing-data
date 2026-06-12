"""Append iter19 going-conditional kohan_3f features to a JRA finish-position
feature parquet directory (v8 iter19 layer, 241 -> 244 numeric features).

Adds three new per-horse columns derived from the horse's recent sectional
history (``jvd_se.kohan_3f``) conditioned on track going
(``jvd_ra.babajotai_code_shiba`` / ``babajotai_code_dirt``):

- ``kohan3f_firm_avg5``: average ``kohan_3f`` over the FIRM-going starts
  (going codes 1-2) among the horse's last 5 prior going-coded starts.
- ``kohan3f_soft_avg5``: same for SOFT going (codes 3-4).
- ``kohan3f_going_diff``: ``firm_avg5 - soft_avg5`` (NULL when either side is
  NULL — SQL subtraction propagates NULL; no imputation).

Precise window semantics (must stay bit-identical to the verified model
``iter19-jra-cb-kohan3f-going-v8``): ``prior_rank`` is a row_number over ALL
of the horse's prior going-coded starts ordered by date descending, and each
average filters ``prior_rank <= 5`` AND the going side.  I.e. the unit is
"the last 5 going-coded starts", split by going — NOT "the last 5 firm starts
ever".  A horse whose last 5 starts were all firm gets a NULL soft average.

Going derivation: turf races (``track_code`` 10-29) read
``babajotai_code_shiba``; dirt races (51-69) read ``babajotai_code_dirt``.
Code 0 (unknown) and non-1-4 codes are excluded from history entirely.

Data leakage prevention: history join uses strict
``hist_race_date < target race_date`` so the target row never contributes to
its own aggregate.

JRA only: source tables are ``pg.jvd_se`` / ``pg.jvd_ra`` and the keibajo
filter is the JRA range (01-10).  NAR / Ban-ei have no verified signal here
(the probe and the model judge were JRA-scoped).

Usage
-----
Run as a post-processor over the iter14 production store::

    uv run python src/scripts/finish-position-features/add_kohan3f_going_features.py \\
        --input-dir tmp/feat-jra-v8-iter14-course \\
        --output-dir tmp/feat-jra-v8-iter19-kohan3f-going \\
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


RACE_PARTITION: str = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
DEFAULT_PG_URL: str = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"

# JRA keibajo_code range (01-10).
JRA_KEIBAJO_REGEXP: str = "^0[1-9]$|^10$"

# How many of the most recent going-coded past starts contribute.
RECENT_GOING_WINDOW_SIZE: int = 5

# Going codes: 1=firm(良)/2=slightly-soft(稍重) treated FIRM; 3=soft(重)/4=heavy(不良) SOFT.
FIRM_GOING_CODES: tuple[int, ...] = (1, 2)
SOFT_GOING_CODES: tuple[int, ...] = (3, 4)

# track_code ranges selecting which babajotai column applies.
TURF_TRACK_CODE_RANGE: tuple[int, int] = (10, 29)
DIRT_TRACK_CODE_RANGE: tuple[int, int] = (51, 69)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_kohan3f_going_features")
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
        help="stage kohan_3f history from this kaisai_nen (>= 2 years before "
        "the oldest target year so early targets have a populated window)",
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: _DuckDBConnectionLike, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def going_code_case_sql() -> str:
    """CASE expression deriving the going code from the surface-specific column.

    Turf rows read babajotai_code_shiba, dirt rows babajotai_code_dirt; any
    other track_code (obstacle variations outside both ranges) yields NULL.
    """
    turf_lo, turf_hi = TURF_TRACK_CODE_RANGE
    dirt_lo, dirt_hi = DIRT_TRACK_CODE_RANGE
    return (
        "case "
        f"when cast(ra.track_code as int) between {turf_lo} and {turf_hi} "
        "then try_cast(nullif(trim(ra.babajotai_code_shiba::text), '') as int) "
        f"when cast(ra.track_code as int) between {dirt_lo} and {dirt_hi} "
        "then try_cast(nullif(trim(ra.babajotai_code_dirt::text), '') as int) "
        "else null end"
    )


def stage_kohan3f_history(con: _DuckDBConnectionLike, history_from_year: int) -> None:
    """Stage per-horse going-coded kohan_3f history rows from PG.

    Keeps only rows with a parseable positive ``kohan_3f`` (4-char string in
    tenths of seconds) and a non-empty ``ketto_toroku_bango``.  The going code
    itself is filtered later at the join (must be 1-4).
    """
    going_sql = going_code_case_sql()
    con.execute(
        f"""
        create or replace temp table kohan3f_hist as
        select
          se.ketto_toroku_bango,
          try_cast(nullif(trim(se.kohan_3f), '') as double) as kohan_3f_val,
          {going_sql} as going_code,
          se.kaisai_nen || se.kaisai_tsukihi as hist_race_date
        from pg.jvd_se se
        join pg.jvd_ra ra
          on ra.kaisai_nen = se.kaisai_nen
         and ra.kaisai_tsukihi = se.kaisai_tsukihi
         and ra.keibajo_code = se.keibajo_code
         and ra.race_bango = se.race_bango
        where regexp_matches(se.keibajo_code, '{JRA_KEIBAJO_REGEXP}')
          and cast(se.kaisai_nen as integer) >= {history_from_year}
          and se.ketto_toroku_bango is not null
          and se.ketto_toroku_bango != ''
          and try_cast(nullif(trim(se.kohan_3f), '') as double) is not null
          and try_cast(nullif(trim(se.kohan_3f), '') as double) > 0
        """
    )


def stage_base_races(con: _DuckDBConnectionLike, input_glob: str) -> None:
    """Stage target race-entry identifiers from the existing feature parquet."""
    con.execute(
        f"""
        create or replace temp table base_races as
        select
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango, race_date
        from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)
        where source = 'jra'
        """
    )


def stage_going_conditional_agg(con: _DuckDBConnectionLike) -> None:
    """Rank prior going-coded starts per target row and aggregate firm/soft.

    ``prior_rank`` counts ALL going-coded prior starts (codes 1-4) newest
    first; the firm / soft averages then filter within the last
    RECENT_GOING_WINDOW_SIZE of them.  Strict ``<`` date join is the leak
    guard.  ``kohan3f_going_diff`` relies on SQL NULL propagation.
    """
    firm_codes = ", ".join(str(c) for c in FIRM_GOING_CODES)
    valid_codes = ", ".join(str(c) for c in FIRM_GOING_CODES + SOFT_GOING_CODES)
    con.execute(
        f"""
        create or replace temp table going_cond_ranked as
        select
          b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango,
          b.ketto_toroku_bango,
          h.kohan_3f_val,
          case when h.going_code in ({firm_codes}) then 1 else 0 end as is_firm,
          row_number() over (
            partition by b.ketto_toroku_bango, b.kaisai_nen, b.kaisai_tsukihi,
                         b.keibajo_code, b.race_bango
            order by h.hist_race_date desc
          ) as prior_rank
        from base_races b
        join kohan3f_hist h
          on h.ketto_toroku_bango = b.ketto_toroku_bango
         and h.hist_race_date < b.race_date
         and h.going_code in ({valid_codes})
        """
    )
    con.execute(
        f"""
        create or replace temp table going_cond_agg as
        select
          {RACE_PARTITION}, ketto_toroku_bango,
          avg(kohan_3f_val)
            filter (where prior_rank <= {RECENT_GOING_WINDOW_SIZE} and is_firm = 1)
            as kohan3f_firm_avg5,
          avg(kohan_3f_val)
            filter (where prior_rank <= {RECENT_GOING_WINDOW_SIZE} and is_firm = 0)
            as kohan3f_soft_avg5
        from going_cond_ranked
        group by {RACE_PARTITION}, ketto_toroku_bango
        """
    )


def append_features_sql(input_glob: str) -> str:
    """LEFT JOIN the three new columns onto the base parquet (schema extension).

    Rows without any eligible going-coded history emit NULL for all three.
    """
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true, union_by_name=true)
    )
    select
      b.*,
      g.kohan3f_firm_avg5,
      g.kohan3f_soft_avg5,
      g.kohan3f_firm_avg5 - g.kohan3f_soft_avg5 as kohan3f_going_diff
    from base b
    left join going_cond_agg g
      on g.source = b.source
      and g.kaisai_nen = b.kaisai_nen
      and g.kaisai_tsukihi = b.kaisai_tsukihi
      and g.keibajo_code = b.keibajo_code
      and g.race_bango = b.race_bango
      and g.ketto_toroku_bango = b.ketto_toroku_bango
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
    stage_kohan3f_history(con, args.history_from_year)
    stage_base_races(con, input_glob)
    stage_going_conditional_agg(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
