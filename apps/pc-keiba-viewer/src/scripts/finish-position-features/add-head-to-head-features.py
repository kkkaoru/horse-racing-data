#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append head-to-head (h2h) features (v7 layer).

Motivation:
  同じ重賞・条件戦に繰り返し出走する馬同士の過去対戦記録は、現 model が活用していない
  pair-wise signal。例: 過去 5 戦中 4 戦で B 馬を負かしている A 馬は、B 馬同居レースで
  着順前に来やすい。

Features added (per horse × race):
  - h2h_encounter_count       : 当該レース他出走馬と過去同居した race 数 (重複 OK)
  - h2h_win_count_vs_field    : 過去対戦時に勝った (= 自分が前) 回数の合計
  - h2h_loss_count_vs_field   : 過去対戦時に負けた回数の合計
  - h2h_win_rate_vs_field     : win / (win + loss); NULL if no encounters
  - h2h_avg_finish_diff_vs_field : avg (self_finish - other_finish); 負=自分が前
  - h2h_unique_rivals_count   : current field 中で過去対戦経験のある馬の数

Data leakage 防止: pair_history は race_date < current_race_date のみ。

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/finish-position-features/add-head-to-head-features.py \\
    --input-dir tmp/feat-jra-v7-lineage \\
    --output-dir tmp/feat-jra-v7-h2h
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_head_to_head_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20100101")
    parser.add_argument(
        "--target-race",
        type=str,
        default=None,
        help=(
            "Focused production mode keibajo_code:race_bango. The input parquet "
            "is already race-scoped; this switches pair-history staging to target horses."
        ),
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def stage_race_history(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    con.execute(
        f"""
        create or replace temp table race_history as
        select
          source,
          race_date,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          ketto_toroku_bango,
          finish_position
        from pg.race_entry_corner_features
        where race_date >= '{from_date}'
          and finish_position is not null
          and ketto_toroku_bango is not null
        """
    )
    con.execute(
        f"create index race_history_idx on race_history ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def stage_target_horses(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """Distinct target-field horses from the already scoped input parquet."""
    con.execute(
        f"""
        create or replace temp table target_horses as
        select distinct source, ketto_toroku_bango
        from read_parquet('{input_glob}', hive_partitioning=true)
        where ketto_toroku_bango is not null
        """
    )
    con.execute(
        "create index target_horses_idx on target_horses (source, ketto_toroku_bango)"
    )


def target_pair_filter_sql(focused_target: bool) -> str:
    if focused_target:
        return """
        where exists (
          select 1 from target_horses th
          where th.source = h1.source
            and th.ketto_toroku_bango = h1.ketto_toroku_bango
        )
          and exists (
          select 1 from target_horses th
          where th.source = h2.source
            and th.ketto_toroku_bango = h2.ketto_toroku_bango
        )
        """
    return ""


def stage_pair_history(
    con: duckdb.DuckDBPyConnection, focused_target: bool = False
) -> None:
    """過去同 race で走った unique 馬 pair (a < b) の finish_diff を materialize。

    Cardinality 制御: ketto_toroku_bango > 比較で重複除去。1 race あたり N×(N-1)/2 pairs。
    JRA 全期間で約 150M 行想定 (DuckDB 24GB で持つ)。
    focused_target=True の production single-race mode では input parquet の
    target field horses 同士に限定して同じ historical pair を作る。
    """
    target_filter = target_pair_filter_sql(focused_target)
    con.execute(
        f"""
        create or replace temp table pair_history as
        select
          h1.source,
          h1.race_date,
          h1.ketto_toroku_bango as horse_a,
          h2.ketto_toroku_bango as horse_b,
          h1.finish_position - h2.finish_position as finish_diff_a_minus_b
        from race_history h1
        inner join race_history h2
          on h2.source = h1.source
          and h2.kaisai_nen = h1.kaisai_nen
          and h2.kaisai_tsukihi = h1.kaisai_tsukihi
          and h2.keibajo_code = h1.keibajo_code
          and h2.race_bango = h1.race_bango
          and h2.ketto_toroku_bango > h1.ketto_toroku_bango
        {target_filter}
        """
    )
    con.execute(
        "create index pair_history_a_idx on pair_history (source, horse_a, horse_b, race_date)"
    )
    con.execute(
        "create index pair_history_b_idx on pair_history (source, horse_b, horse_a, race_date)"
    )


def stage_target_races(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """入力 parquet から (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    race_date) のユニーク race key を抽出し target_races テーブルに格納する。

    stage_current_pair_aggregates で current_field をこの race key に絞り込むことで、
    推論時 (upcoming races, 数十行) の処理量を劇的に削減できる。
    訓練時 (全期間 parquet, 数百万行) も同じ絞り込みを行うが、その場合 target_races
    は race_history のほぼ全 race を含むため挙動は変わらない。
    """
    con.execute(
        f"""
        create or replace temp table target_races as
        select distinct
          source,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          race_date
        from read_parquet('{input_glob}', hive_partitioning=true)
        """
    )
    con.execute(
        f"create index target_races_idx on target_races ({RACE_PARTITION})"
    )


def stage_current_pair_aggregates(con: duckdb.DuckDBPyConnection) -> None:
    """各 (current race, horse_a, horse_b) pair について、過去対戦時の集計。

    current_field を target_races (入力 parquet の race key セット) に絞り込む。
    これにより推論時は全 race_history ではなくその日の数十 race だけが対象になり、
    O(全期間²) → O(当日レース²) の劇的なメモリ・時間削減を実現する。

    current_pairs: 当該レース内の unique pair (a < b)
    × pair_history (race_date < current_race_date)
    → pair 単位の集計
    """
    con.execute(
        """
        create or replace temp table current_pair_aggregates as
        with current_field as (
          select rh.source, rh.kaisai_nen, rh.kaisai_tsukihi, rh.keibajo_code, rh.race_bango,
                 rh.ketto_toroku_bango, rh.race_date as current_date
          from race_history rh
          inner join target_races tr
            on tr.source = rh.source
            and tr.kaisai_nen = rh.kaisai_nen
            and tr.kaisai_tsukihi = rh.kaisai_tsukihi
            and tr.keibajo_code = rh.keibajo_code
            and tr.race_bango = rh.race_bango
        ),
        current_pairs as (
          select
            cf1.source, cf1.kaisai_nen, cf1.kaisai_tsukihi, cf1.keibajo_code, cf1.race_bango,
            cf1.current_date,
            cf1.ketto_toroku_bango as horse_a,
            cf2.ketto_toroku_bango as horse_b
          from current_field cf1
          inner join current_field cf2
            on cf2.source = cf1.source
            and cf2.kaisai_nen = cf1.kaisai_nen
            and cf2.kaisai_tsukihi = cf1.kaisai_tsukihi
            and cf2.keibajo_code = cf1.keibajo_code
            and cf2.race_bango = cf1.race_bango
            and cf2.ketto_toroku_bango > cf1.ketto_toroku_bango
        )
        select
          cp.source, cp.kaisai_nen, cp.kaisai_tsukihi, cp.keibajo_code, cp.race_bango,
          cp.horse_a, cp.horse_b,
          count(ph.finish_diff_a_minus_b) as enc_count,
          sum(case when ph.finish_diff_a_minus_b < 0 then 1 else 0 end) as a_wins,
          sum(case when ph.finish_diff_a_minus_b > 0 then 1 else 0 end) as b_wins,
          avg(ph.finish_diff_a_minus_b) as avg_diff_a_minus_b
        from current_pairs cp
        left join pair_history ph
          on ph.source = cp.source
          and ph.horse_a = cp.horse_a
          and ph.horse_b = cp.horse_b
          and ph.race_date < cp.current_date
        group by all
        """
    )


def stage_h2h_horse_summary(con: duckdb.DuckDBPyConnection) -> None:
    """current_pair_aggregates を horse-level に集約 (self_horse 視点で勝敗逆転して足す)。"""
    con.execute(
        """
        create or replace temp table h2h_horse_summary as
        with horse_a_view as (
          select
            source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            horse_a as self_horse,
            horse_b as other_horse,
            enc_count,
            a_wins as self_wins,
            b_wins as self_losses,
            avg_diff_a_minus_b as self_avg_diff
          from current_pair_aggregates
        ),
        horse_b_view as (
          select
            source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
            horse_b as self_horse,
            horse_a as other_horse,
            enc_count,
            b_wins as self_wins,
            a_wins as self_losses,
            -avg_diff_a_minus_b as self_avg_diff
          from current_pair_aggregates
        ),
        combined as (
          select * from horse_a_view union all select * from horse_b_view
        )
        select
          source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, self_horse,
          sum(enc_count) as h2h_encounter_count,
          sum(self_wins) as h2h_win_count_vs_field,
          sum(self_losses) as h2h_loss_count_vs_field,
          sum(self_avg_diff * enc_count) / nullif(sum(enc_count), 0) as h2h_avg_finish_diff_vs_field,
          sum(case when enc_count > 0 then 1 else 0 end) as h2h_unique_rivals_count
        from combined
        group by all
        """
    )
    con.execute(
        f"create index h2h_horse_summary_idx on h2h_horse_summary ({RACE_PARTITION}, self_horse)"
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    joined as (
      select
        b.*,
        coalesce(s.h2h_encounter_count, 0) as h2h_encounter_count,
        coalesce(s.h2h_win_count_vs_field, 0) as h2h_win_count_vs_field,
        coalesce(s.h2h_loss_count_vs_field, 0) as h2h_loss_count_vs_field,
        case
          when coalesce(s.h2h_win_count_vs_field, 0) + coalesce(s.h2h_loss_count_vs_field, 0) > 0
          then s.h2h_win_count_vs_field::double
               / (s.h2h_win_count_vs_field + s.h2h_loss_count_vs_field)
          else null
        end as h2h_win_rate_vs_field,
        s.h2h_avg_finish_diff_vs_field,
        coalesce(s.h2h_unique_rivals_count, 0) as h2h_unique_rivals_count
      from base b
      left join h2h_horse_summary s
        on s.source = b.source
        and s.kaisai_nen = b.kaisai_nen
        and s.kaisai_tsukihi = b.kaisai_tsukihi
        and s.keibajo_code = b.keibajo_code
        and s.race_bango = b.race_bango
        and s.self_horse = b.ketto_toroku_bango
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
    stage_race_history(con, args.from_date)
    stage_target_races(con, input_glob)
    if args.target_race is not None:
        stage_target_horses(con, input_glob)
    stage_pair_history(con, args.target_race is not None)
    stage_current_pair_aggregates(con)
    stage_h2h_horse_summary(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
