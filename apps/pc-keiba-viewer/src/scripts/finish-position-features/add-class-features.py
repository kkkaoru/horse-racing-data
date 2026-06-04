#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append iter18 class-level horse / trainer features (v8 iter18 layer).

Motivation:
  Iter 18 (v8 JRA) introduces three signals that capture horse class movement
  and trainer venue specialisation. These features extend the v7-lineage stack
  by exploiting kyoso_joken_code (class level) trajectories that prior layers
  did not model directly.

  Sibling TS builders (run via PG UPDATE for the production
  ``race_finish_position_features`` table) live next to this script:
    - build-class-promotion-velocity-sql.ts
    - build-horse-class-variance-sql.ts
    - build-trainer-hiraba-sql.ts (parallel SubAgent — may not yet be present)

  This Python script is the offline / parquet-side mirror: it joins iter 14's
  241-feature parquet against the same PG history rows but produces the three
  new columns directly so the training dataset can be rebuilt without an
  intermediate PG round-trip.

Features added (per horse x race) — exactly 3 columns:
  - class_promotion_velocity      (days between target race and the most recent
                                   past win at >= target class level - 1)
  - trainer_hiraba_win_rate       (trainer career win rate in "hiraba" (平場)
                                   races prior to the target race, where
                                   hiraba is defined by the canonical
                                   kyoso_joken_code set ``{'000','005','010',
                                   '016'}`` — mirrors
                                   ``build-trainer-hiraba-sql.ts
                                   HIRABA_KYOSO_JOKEN_CODES``)
  - horse_recent_class_variance   (population stddev of the horse's last 5
                                   races' class levels; NULL when <2 valid
                                   class levels observed)

Class level mapping mirrors ``build-recent-form-sql.ts JRA_CLASS_LEVELS`` so
that iter18 numbers reconcile bit-for-bit with the TS builders:
  ``000 -> 0, 005 -> 1, 010 -> 2, 016 -> 3, 701 -> 4, 703 -> 5, 999 -> 6``.

Data leakage prevention: every past-race aggregate uses strict ``race_date <
target_race_date`` so the target row is never included in its own history.
Variance / velocity use only horse's own history; trainer hiraba uses only the
trainer's own past races.

Rows without a populated history (new horse / new trainer / nothing meeting the
class-promotion criterion) emit NULL — gradient boosters treat missing inputs
as a learned default so no imputation is applied here.
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

from _resource_defaults import add_resource_args, apply_to_connection

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"

# Class-level mapping — mirrors build-recent-form-sql.ts / build-horse-class-variance-sql.ts.
JRA_CLASS_LEVELS: dict[str, int] = {
    "000": 0,
    "005": 1,
    "010": 2,
    "016": 3,
    "701": 4,
    "703": 5,
    "999": 6,
}

# Canonical hiraba (平場) kyoso_joken_code set — mirrors
# build-trainer-hiraba-sql.ts HIRABA_KYOSO_JOKEN_CODES so the Python parquet
# pipeline emits the same row subset as the TS PG UPDATE path.
HIRABA_KYOSO_JOKEN_CODES: tuple[str, ...] = ("000", "005", "010", "016")

# How many of the most recent races count toward horse_recent_class_variance.
CLASS_VARIANCE_WINDOW = 5
CLASS_VARIANCE_MIN_RACES = 2

# Promotion buffer: a past win at (target_class_level - 1) still counts as a
# promotion-eligible run (mirrors build-class-promotion-velocity-sql.ts).
PROMOTION_LEVEL_BUFFER = 1


def hiraba_in_clause_sql(column_expr: str) -> str:
    """SQL `IN` clause restricting ``column_expr`` to the canonical hiraba set."""
    codes = ", ".join(f"'{code}'" for code in HIRABA_KYOSO_JOKEN_CODES)
    return f"{column_expr} in ({codes})"


def class_level_case_sql(code_expr: str) -> str:
    """SQL CASE that maps kyoso_joken_code text -> integer class level.

    Returns ``NULL`` for codes not in ``JRA_CLASS_LEVELS`` so that downstream
    aggregates can safely filter on ``is not null``.
    """
    branches = " ".join(
        f"when '{code}' then {level}" for code, level in JRA_CLASS_LEVELS.items()
    )
    return f"case {code_expr} {branches} else null end"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_class_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--category",
        choices=("jra", "nar"),
        default="jra",
        help="jra -> pg.jvd_se source filter; nar -> pg.nvd_se source filter",
    )
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20100101")
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def source_filter_sql(category: str) -> str:
    """SQL fragment filtering ``race_entry_corner_features`` to one category."""
    if category == "jra":
        return "rec.source = 'jra'"
    return "rec.source = 'nar' and rec.keibajo_code <> '83'"


def se_table_for(category: str) -> str:
    return "pg.jvd_se" if category == "jra" else "pg.nvd_se"


def stage_race_history(
    con: duckdb.DuckDBPyConnection, from_date: str, category: str
) -> None:
    """horse past races + class_level + chokyoshi_code + finish_position.

    Joins ``pg.race_entry_corner_features`` against the per-category se_table to
    pick up the trainer code (chokyoshi_code) which is not on the corner table.
    """
    se_table = se_table_for(category)
    class_case = class_level_case_sql("rec.kyoso_joken_code")
    src_filter = source_filter_sql(category)
    con.execute(
        f"""
        create or replace temp table race_history as
        select
          rec.source,
          rec.race_date,
          rec.kaisai_nen,
          rec.kaisai_tsukihi,
          rec.keibajo_code,
          rec.race_bango,
          rec.ketto_toroku_bango,
          rec.finish_position,
          rec.grade_code,
          rec.kyoso_joken_code,
          {class_case} as class_level,
          se.chokyoshi_code
        from pg.race_entry_corner_features rec
        left join {se_table} se
          on se.kaisai_nen = rec.kaisai_nen
          and se.kaisai_tsukihi = rec.kaisai_tsukihi
          and se.keibajo_code = rec.keibajo_code
          and se.race_bango = rec.race_bango
          and se.ketto_toroku_bango = rec.ketto_toroku_bango
        where rec.race_date >= '{from_date}'
          and rec.finish_position is not null
          and {src_filter}
        """
    )
    con.execute(
        "create index race_history_horse_idx on race_history (source, ketto_toroku_bango, race_date)"
    )
    con.execute(
        "create index race_history_trainer_idx on race_history (source, chokyoshi_code, race_date)"
    )


def stage_base_input(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """Read iter14 parquet keys + class context needed for join logic.

    The iter14 base parquet does NOT propagate ``kyoso_joken_code`` itself, so
    we LEFT JOIN against ``pg.race_entry_corner_features`` (alias ``rec``) on
    the standard race-entry composite key ``(source, kaisai_nen, kaisai_tsukihi,
    keibajo_code, race_bango, umaban)`` to pull the current race's class code.
    The projected ``kyoso_joken_code`` column then feeds:
      - ``target_class_level`` directly via JRA_CLASS_LEVELS mapping (used by
        ``stage_class_promotion``)
      - downstream stages (``stage_horse_class_variance``,
        ``stage_trainer_hiraba``) which key on past races, not the current row.
    """
    class_case = class_level_case_sql("rec.kyoso_joken_code")
    con.execute(
        f"""
        create or replace temp table base_input as
        select
          b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango,
          b.ketto_toroku_bango, b.race_date, b.race_year,
          rec.kyoso_joken_code,
          {class_case} as target_class_level
        from read_parquet('{input_glob}', hive_partitioning=true) b
        left join pg.race_entry_corner_features rec
          on rec.source = b.source
          and rec.kaisai_nen = b.kaisai_nen
          and rec.kaisai_tsukihi = b.kaisai_tsukihi
          and rec.keibajo_code = b.keibajo_code
          and rec.race_bango = b.race_bango
          and rec.umaban = b.umaban
        """
    )
    con.execute(
        f"create index base_input_idx on base_input ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def stage_class_promotion(con: duckdb.DuckDBPyConnection) -> None:
    """class_promotion_velocity = days from target race to most recent win at
    >= (target_class_level - PROMOTION_LEVEL_BUFFER).
    """
    con.execute(
        f"""
        create or replace temp table class_promotion as
        with eligible as (
          select
            bi.source,
            bi.kaisai_nen,
            bi.kaisai_tsukihi,
            bi.keibajo_code,
            bi.race_bango,
            bi.ketto_toroku_bango,
            bi.race_date as target_race_date,
            rh.race_date as history_race_date,
            row_number() over (
              partition by bi.source, bi.kaisai_nen, bi.kaisai_tsukihi,
                           bi.keibajo_code, bi.race_bango, bi.ketto_toroku_bango
              order by rh.race_date desc
            ) as recency_rank
          from base_input bi
          join race_history rh
            on rh.source = bi.source
            and rh.ketto_toroku_bango = bi.ketto_toroku_bango
            and rh.race_date < bi.race_date
            and rh.finish_position = 1
            and rh.class_level is not null
            and bi.target_class_level is not null
            and rh.class_level >= bi.target_class_level - {PROMOTION_LEVEL_BUFFER}
        )
        select
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          date_diff(
            'day',
            strptime(history_race_date, '%Y%m%d')::date,
            strptime(target_race_date, '%Y%m%d')::date
          ) as class_promotion_velocity
        from eligible
        where recency_rank = 1
        """
    )
    con.execute(
        f"create index class_promotion_idx on class_promotion ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def stage_horse_class_variance(con: duckdb.DuckDBPyConnection) -> None:
    """horse_recent_class_variance = stddev_pop(last N class levels) (window N)."""
    con.execute(
        f"""
        create or replace temp table horse_class_variance as
        with ranked as (
          select
            bi.source,
            bi.kaisai_nen,
            bi.kaisai_tsukihi,
            bi.keibajo_code,
            bi.race_bango,
            bi.ketto_toroku_bango,
            rh.class_level,
            row_number() over (
              partition by bi.source, bi.kaisai_nen, bi.kaisai_tsukihi,
                           bi.keibajo_code, bi.race_bango, bi.ketto_toroku_bango
              order by rh.race_date desc
            ) as recency_rank
          from base_input bi
          join race_history rh
            on rh.source = bi.source
            and rh.ketto_toroku_bango = bi.ketto_toroku_bango
            and rh.race_date < bi.race_date
            and rh.class_level is not null
        )
        select
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          case
            when count(*) filter (where recency_rank <= {CLASS_VARIANCE_WINDOW})
                 >= {CLASS_VARIANCE_MIN_RACES}
            then stddev_pop(class_level::double)
                   filter (where recency_rank <= {CLASS_VARIANCE_WINDOW})
            else null
          end as horse_recent_class_variance
        from ranked
        group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
                 ketto_toroku_bango
        """
    )
    con.execute(
        f"create index horse_class_variance_idx on horse_class_variance ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def stage_trainer_hiraba(
    con: duckdb.DuckDBPyConnection, category: str
) -> None:
    """trainer_hiraba_win_rate = trainer career win rate in hiraba races.

    "hiraba" (平場) is defined by the canonical kyoso_joken_code set
    ``HIRABA_KYOSO_JOKEN_CODES`` = ``('000', '005', '010', '016')`` which
    mirrors ``build-trainer-hiraba-sql.ts``. Graded races (G1/G2/G3 / L) and
    other class codes (701/703/999) are explicitly excluded. Joining
    base_input with the trainer's own past hiraba races yields
    per-(target_race, trainer) cumulative starts + wins.
    """
    se_table = se_table_for(category)
    hiraba_filter = hiraba_in_clause_sql("kyoso_joken_code")
    con.execute(
        f"""
        create or replace temp table base_with_trainer as
        select
          bi.source, bi.kaisai_nen, bi.kaisai_tsukihi, bi.keibajo_code,
          bi.race_bango, bi.ketto_toroku_bango, bi.race_date,
          se.chokyoshi_code
        from base_input bi
        left join {se_table} se
          on se.kaisai_nen = bi.kaisai_nen
          and se.kaisai_tsukihi = bi.kaisai_tsukihi
          and se.keibajo_code = bi.keibajo_code
          and se.race_bango = bi.race_bango
          and se.ketto_toroku_bango = bi.ketto_toroku_bango
        """
    )
    con.execute(
        "create index base_with_trainer_idx on base_with_trainer "
        "(source, chokyoshi_code, race_date)"
    )
    con.execute(
        f"""
        create or replace temp table trainer_hiraba as
        with hiraba_history as (
          select source, chokyoshi_code, race_date, finish_position
          from race_history
          where chokyoshi_code is not null
            and trim(chokyoshi_code) <> ''
            and {hiraba_filter}
        ),
        agg as (
          select
            bwt.source,
            bwt.kaisai_nen,
            bwt.kaisai_tsukihi,
            bwt.keibajo_code,
            bwt.race_bango,
            bwt.ketto_toroku_bango,
            count(hh.race_date) as past_starts,
            sum(case when hh.finish_position = 1 then 1 else 0 end) as past_wins
          from base_with_trainer bwt
          left join hiraba_history hh
            on hh.source = bwt.source
            and hh.chokyoshi_code = bwt.chokyoshi_code
            and hh.race_date < bwt.race_date
          where bwt.chokyoshi_code is not null
            and trim(bwt.chokyoshi_code) <> ''
          group by bwt.source, bwt.kaisai_nen, bwt.kaisai_tsukihi,
                   bwt.keibajo_code, bwt.race_bango, bwt.ketto_toroku_bango
        )
        select
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          ketto_toroku_bango,
          case when past_starts > 0
               then past_wins::double / past_starts
               else null end as trainer_hiraba_win_rate
        from agg
        """
    )
    con.execute(
        f"create index trainer_hiraba_idx on trainer_hiraba ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str) -> str:
    """Left-join the three iter18 columns onto the iter14 parquet base.

    All three staging tables are keyed on the full
    (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    ketto_toroku_bango) tuple — matches one horse x race row. ``left join``
    preserves all 241 input columns and emits NULL for rows that do not have a
    staging row (no eligible history / no trainer / etc.).
    """
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    joined as (
      select
        b.*,
        cp.class_promotion_velocity,
        th.trainer_hiraba_win_rate,
        hcv.horse_recent_class_variance
      from base b
      left join class_promotion cp
        on cp.source = b.source
        and cp.kaisai_nen = b.kaisai_nen
        and cp.kaisai_tsukihi = b.kaisai_tsukihi
        and cp.keibajo_code = b.keibajo_code
        and cp.race_bango = b.race_bango
        and cp.ketto_toroku_bango = b.ketto_toroku_bango
      left join trainer_hiraba th
        on th.source = b.source
        and th.kaisai_nen = b.kaisai_nen
        and th.kaisai_tsukihi = b.kaisai_tsukihi
        and th.keibajo_code = b.keibajo_code
        and th.race_bango = b.race_bango
        and th.ketto_toroku_bango = b.ketto_toroku_bango
      left join horse_class_variance hcv
        on hcv.source = b.source
        and hcv.kaisai_nen = b.kaisai_nen
        and hcv.kaisai_tsukihi = b.kaisai_tsukihi
        and hcv.keibajo_code = b.keibajo_code
        and hcv.race_bango = b.race_bango
        and hcv.ketto_toroku_bango = b.ketto_toroku_bango
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
    install_and_attach_pg(con, args.pg_url)
    stage_race_history(con, args.from_date, args.category)
    stage_base_input(con, input_glob)
    stage_class_promotion(con)
    stage_horse_class_variance(con)
    stage_trainer_hiraba(con, args.category)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
