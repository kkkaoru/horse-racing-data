#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append market-signal features (odds & popularity composite signals) to an
existing v2 finish-position feature parquet directory, producing v3.

This is a post-processor that:
  - reads the v2 parquet (already includes race-internal rank/diff features)
  - joins with PG `race_entry_corner_features` to pull raw tansho_odds,
    tansho_ninkijun (not present in v2)
  - computes 7 new market-signal features per-horse with race-internal ranks
  - writes a new v3 parquet partitioned by race_year

Run with:
  .venv/bin/python src/scripts/finish-position-features/add-market-signal-features.py \
    --input-dir tmp/finish-position-features-parquet-jra-v2 \
    --output-dir tmp/finish-position-features-parquet-jra-v3 \
    --pg-url postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
RACE_PARTITION_BY = "b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango"
PG_RAW_ODDS_COLUMNS = (
    "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,"
    " cast(tansho_odds as double) as tansho_odds_raw,"
    " cast(tansho_ninkijun as int) as tansho_ninkijun_raw"
)
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_market_signal_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20100101")
    parser.add_argument("--to-date", type=str, default="20991231")
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def stage_raw_odds(
    con: duckdb.DuckDBPyConnection, from_date: str, to_date: str
) -> None:
    con.execute(
        f"""
        create or replace temp table raw_odds as
        select {PG_RAW_ODDS_COLUMNS}
        from pg.race_entry_corner_features
        where race_date between '{from_date}' and '{to_date}'
        """
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base_v2 as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    joined as (
      select b.*,
        r.tansho_odds_raw,
        r.tansho_ninkijun_raw
      from base_v2 b
      left join raw_odds r using ({RACE_PARTITION}, ketto_toroku_bango)
    )
    select
      b.*,
      case when b.tansho_odds_raw is not null and b.tansho_odds_raw > 0
           then 1.0 / b.tansho_odds_raw
           else null end as inverse_odds_implied_prob,
      case when b.tansho_odds_raw is not null and b.tansho_odds_raw > 0
           then (1.0 / b.tansho_odds_raw)
                / nullif(sum(case when b.tansho_odds_raw > 0 then 1.0 / b.tansho_odds_raw else 0 end)
                    over (partition by {RACE_PARTITION_BY}), 0)
           else null end as inverse_odds_market_share,
      rank() over race_by_inverse_odds_desc as inverse_odds_rank_in_race,
      rank() over race_by_popularity_asc as popularity_rank_in_race,
      b.odds_score - avg(b.odds_score) over (partition by {RACE_PARTITION_BY})
        as odds_score_diff_from_race_avg,
      b.popularity_score - avg(b.popularity_score) over (partition by {RACE_PARTITION_BY})
        as popularity_score_diff_from_race_avg,
      case when b.popularity_score is not null and b.odds_score is not null
           then abs(b.popularity_score - b.odds_score)
           else null end as popularity_odds_disagreement
    from joined b
    window
      race_by_inverse_odds_desc as (
        partition by {RACE_PARTITION_BY}
        order by case when b.tansho_odds_raw > 0 then 1.0 / b.tansho_odds_raw else null end desc nulls last
      ),
      race_by_popularity_asc as (
        partition by {RACE_PARTITION_BY}
        order by b.tansho_ninkijun_raw asc nulls last
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
    install_and_attach_pg(con, args.pg_url)
    stage_raw_odds(con, args.from_date, args.to_date)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
