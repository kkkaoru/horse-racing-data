#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append grade-race lineage / trial-race lineage features (v7 layer).

Motivation:
  G1 race (オークス、ダービー、有馬記念 等) では、過去の好走馬が踏んだ特定の
  trial race (皐月賞、青葉賞、桜花賞 等) での連対歴が強い signal になる傾向がある。
  本 script は target G1 race に該当する出走馬の trial race 連対歴を直接 encode する。

  例: ダービー (東京優駿) では、皐月賞・青葉賞・京都新聞杯・プリンシパルS で
  好走した馬が好走する傾向 → これらの trial race での 1着/3着以内回数を feature 化。

Features added (per horse × race):
  - target_race_id                            : 該当 target_race id (NULL なら非対象)
  - target_grade_trial_count                  : trial race 出走回数 (lookback 内)
  - target_grade_trial_top1_count             : trial 1 着回数
  - target_grade_trial_top3_count             : trial 3 着以内回数
  - target_grade_trial_best_finish            : trial best finish (出走なし → NULL)
  - target_grade_trial_avg_top2_margin_decisec: trial 連対時の avg time_sa (decisec)
  - target_grade_has_trial_history            : boolean (trial 経験有無)

非 target race (一般戦) では全 trial 関連カラム NULL / 0、target_race_id NULL。

Data leakage 防止: race_date strictly less than current race_date のみを集計。
trial の lookback_days は target_race 定義毎に異なる (45-365 日)。

Run with:
  apps/pc-keiba-viewer/.venv/bin/python apps/pc-keiba-viewer/src/scripts/finish-position-features/add-grade-race-lineage-features.py \\
    --input-dir tmp/feat-jra-v6 \\
    --output-dir tmp/feat-jra-v7-lineage \\
    --config apps/pc-keiba-viewer/src/scripts/finish-position-features/lineage-races/jra.json
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb

from _resource_defaults import add_resource_args, apply_to_connection

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_grade_race_lineage_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True, help="Path to lineage-races/{jra,nar,ban-ei}.json")
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20100101")
    parser.add_argument("--to-date", type=str, default="20991231")
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def load_config(config_path: Path) -> dict:
    with config_path.open() as f:
        data = json.load(f)
    if "target_races" not in data or not isinstance(data["target_races"], list):
        raise ValueError(f"Config {config_path} missing 'target_races' list")
    return data


def stage_race_meta(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """race_meta: jvd_ra + nvd_ra から kyosomei_norm / kyori_int / month / race coords を取得。

    JRA は jvd_ra、NAR/Ban-ei は nvd_ra から取る。両方 union all で対応。
    """
    con.execute(
        f"""
        create or replace temp table race_meta as
        select
          'jra' as source,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          replace(replace(coalesce(kyosomei_hondai, ''), '　', ''), ' ', '') as kyosomei_norm,
          try_cast(nullif(trim(kyori), '') as int) as kyori_int,
          grade_code,
          cast(substring(kaisai_tsukihi, 1, 2) as int) as month
        from pg.jvd_ra
        where kaisai_nen >= substring('{from_date}', 1, 4)
        union all
        select
          'nar' as source,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          replace(replace(coalesce(kyosomei_hondai, ''), '　', ''), ' ', '') as kyosomei_norm,
          try_cast(nullif(trim(kyori), '') as int) as kyori_int,
          grade_code,
          cast(substring(kaisai_tsukihi, 1, 2) as int) as month
        from pg.nvd_ra
        where kaisai_nen >= substring('{from_date}', 1, 4)
        """
    )
    con.execute(
        f"create index race_meta_idx on race_meta ({RACE_PARTITION})"
    )


def build_target_classify_sql(config: dict) -> str:
    """Build a CASE WHEN expression that maps each race to target_race_id (or NULL)."""
    branches: list[str] = []
    for tr in config["target_races"]:
        conds: list[str] = []
        m = tr.get("match", {})
        if "kyosomei_equals" in m:
            v = m["kyosomei_equals"].replace("'", "''")
            conds.append(f"kyosomei_norm = '{v}'")
        if "kyosomei_contains" in m:
            v = m["kyosomei_contains"].replace("'", "''")
            conds.append(f"kyosomei_norm like '%{v}%'")
        if "keibajo_code" in m:
            conds.append(f"keibajo_code = '{m['keibajo_code']}'")
        if "kyori" in m:
            conds.append(f"kyori_int = {int(m['kyori'])}")
        if "month" in m:
            conds.append(f"month = {int(m['month'])}")
        if "grade_code" in m:
            v = m["grade_code"].replace("'", "''")
            conds.append(f"grade_code = '{v}'")
        if not conds:
            continue
        rid = tr["id"].replace("'", "''")
        branches.append(f"when {' and '.join(conds)} then '{rid}'")
    if not branches:
        raise ValueError("No target race classifications built")
    return "case " + " ".join(branches) + " else null end"


def build_trial_defs_values(config: dict) -> str:
    """Build a VALUES list of (target_race_id, trial_label, match_type, match_value, lookback_days)."""
    rows: list[str] = []
    for tr in config["target_races"]:
        rid = tr["id"].replace("'", "''")
        for tdef in tr.get("trials", []):
            label = tdef.get("name", "").replace("'", "''")
            tm = tdef.get("match", {})
            lookback = int(tdef.get("lookback_days", 90))
            if "kyosomei_equals" in tm:
                rows.append(
                    f"('{rid}', '{label}', 'equals', '{tm['kyosomei_equals'].replace(chr(39), chr(39) * 2)}', {lookback})"
                )
            elif "kyosomei_contains" in tm:
                rows.append(
                    f"('{rid}', '{label}', 'contains', '{tm['kyosomei_contains'].replace(chr(39), chr(39) * 2)}', {lookback})"
                )
            else:
                continue
    if not rows:
        raise ValueError("No trial definitions built")
    return ",\n          ".join(rows)


def stage_target_classifications(con: duckdb.DuckDBPyConnection, classify_sql: str) -> None:
    """race_target: race_meta + target_race_id (target 該当 race だけ)。"""
    con.execute(
        f"""
        create or replace temp table race_target as
        select * exclude (kyosomei_norm, kyori_int, grade_code, month),
          ({classify_sql}) as target_race_id
        from race_meta
        """
    )
    con.execute(
        f"create index race_target_idx on race_target ({RACE_PARTITION})"
    )


def stage_trial_definitions(con: duckdb.DuckDBPyConnection, trial_values: str) -> None:
    """trial_defs: target_race_id → trial 識別 (一対多)。"""
    con.execute(
        f"""
        create or replace temp table trial_defs as
        select * from (
          values
          {trial_values}
        ) as t(target_race_id, trial_label, match_type, match_value, lookback_days);
        """
    )


def stage_race_serves_as_trial(con: duckdb.DuckDBPyConnection) -> None:
    """race_serves_as_trial: race_meta × trial_defs join。

    各 race が「どの target_race の trial として機能するか」と lookback_days を持つ。
    1 race が複数 target の trial になり得る (例: 桜花賞 → オークス・秋華賞)。
    """
    con.execute(
        """
        create or replace temp table race_serves_as_trial as
        select
          rm.source,
          rm.kaisai_nen,
          rm.kaisai_tsukihi,
          rm.keibajo_code,
          rm.race_bango,
          td.target_race_id,
          td.trial_label,
          td.lookback_days
        from race_meta rm
        cross join trial_defs td
        where
          (td.match_type = 'equals' and rm.kyosomei_norm = td.match_value)
          or (td.match_type = 'contains' and rm.kyosomei_norm like '%' || td.match_value || '%')
        """
    )
    con.execute(
        f"create index race_serves_as_trial_idx on race_serves_as_trial ({RACE_PARTITION})"
    )


def stage_race_history(con: duckdb.DuckDBPyConnection, from_date: str) -> None:
    """horse 単位の過去レース成績 (finish_position, time_sa を含む)。"""
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
          finish_position,
          time_sa
        from pg.race_entry_corner_features
        where race_date >= '{from_date}'
          and finish_position is not null
        """
    )
    con.execute(
        "create index race_history_idx on race_history (source, ketto_toroku_bango, race_date)"
    )


def stage_horse_trial_history(con: duckdb.DuckDBPyConnection) -> None:
    """horse の trial race 出走履歴を target_race_id ごとに pre-aggregate (race_date 別)。

    Cardinality 制御: race_history × race_serves_as_trial の inner join。
    多数の race_serves_as_trial 行があるが、horse が実際に走ったレースとの inner join
    なので最終 row 数は horse × race_history_count × matched_targets で抑えられる。
    """
    con.execute(
        """
        create or replace temp table horse_trial_runs as
        select
          rh.source,
          rh.ketto_toroku_bango,
          rh.race_date as trial_race_date,
          rh.finish_position,
          rh.time_sa,
          rsat.target_race_id,
          rsat.lookback_days
        from race_history rh
        inner join race_serves_as_trial rsat
          on rsat.source = rh.source
          and rsat.kaisai_nen = rh.kaisai_nen
          and rsat.kaisai_tsukihi = rh.kaisai_tsukihi
          and rsat.keibajo_code = rh.keibajo_code
          and rsat.race_bango = rh.race_bango
        """
    )
    con.execute(
        "create index horse_trial_runs_idx on horse_trial_runs (source, ketto_toroku_bango, target_race_id, trial_race_date)"
    )


def stage_horse_target_race_trial_summary(con: duckdb.DuckDBPyConnection) -> None:
    """各 (current race, horse) の中で target_race に該当する row について、
    horse の lookback 内 trial 履歴を aggregate。
    """
    con.execute(
        """
        create or replace temp table horse_target_trial_summary as
        with current_target as (
          select
            rt.source,
            rt.kaisai_nen,
            rt.kaisai_tsukihi,
            rt.keibajo_code,
            rt.race_bango,
            rt.target_race_id,
            rh.race_date as current_race_date,
            rh.ketto_toroku_bango
          from race_target rt
          inner join race_history rh
            on rh.source = rt.source
            and rh.kaisai_nen = rt.kaisai_nen
            and rh.kaisai_tsukihi = rt.kaisai_tsukihi
            and rh.keibajo_code = rt.keibajo_code
            and rh.race_bango = rt.race_bango
          where rt.target_race_id is not null
        )
        select
          ct.source,
          ct.kaisai_nen,
          ct.kaisai_tsukihi,
          ct.keibajo_code,
          ct.race_bango,
          ct.ketto_toroku_bango,
          ct.target_race_id,
          count(htr.trial_race_date) as trial_count,
          sum(case when htr.finish_position = 1 then 1 else 0 end) as top1_count,
          sum(case when htr.finish_position <= 3 then 1 else 0 end) as top3_count,
          min(htr.finish_position) as best_finish,
          avg(case when htr.finish_position <= 2 then htr.time_sa else null end) as avg_top2_margin
        from current_target ct
        left join horse_trial_runs htr
          on htr.source = ct.source
          and htr.ketto_toroku_bango = ct.ketto_toroku_bango
          and htr.target_race_id = ct.target_race_id
          and htr.trial_race_date < ct.current_race_date
          and date_diff(
                'day',
                strptime(htr.trial_race_date, '%Y%m%d')::date,
                strptime(ct.current_race_date, '%Y%m%d')::date
              ) <= htr.lookback_days
        group by all
        """
    )
    con.execute(
        f"create index horse_target_trial_summary_idx on horse_target_trial_summary ({RACE_PARTITION}, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str) -> str:
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    base_target as (
      select b.*, rt.target_race_id
      from base b
      left join race_target rt
        on rt.source = b.source
        and rt.kaisai_nen = b.kaisai_nen
        and rt.kaisai_tsukihi = b.kaisai_tsukihi
        and rt.keibajo_code = b.keibajo_code
        and rt.race_bango = b.race_bango
    ),
    joined as (
      select
        bt.*,
        coalesce(s.trial_count, 0) as target_grade_trial_count,
        coalesce(s.top1_count, 0) as target_grade_trial_top1_count,
        coalesce(s.top3_count, 0) as target_grade_trial_top3_count,
        s.best_finish as target_grade_trial_best_finish,
        s.avg_top2_margin as target_grade_trial_avg_top2_margin_decisec,
        case when coalesce(s.trial_count, 0) > 0 then 1 else 0 end as target_grade_has_trial_history
      from base_target bt
      left join horse_target_trial_summary s
        on s.source = bt.source
        and s.kaisai_nen = bt.kaisai_nen
        and s.kaisai_tsukihi = bt.kaisai_tsukihi
        and s.keibajo_code = bt.keibajo_code
        and s.race_bango = bt.race_bango
        and s.ketto_toroku_bango = bt.ketto_toroku_bango
        and s.target_race_id = bt.target_race_id
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
    config = load_config(args.config)
    classify_sql = build_target_classify_sql(config)
    trial_values = build_trial_defs_values(config)

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    install_and_attach_pg(con, args.pg_url)
    stage_race_meta(con, args.from_date)
    stage_target_classifications(con, classify_sql)
    stage_trial_definitions(con, trial_values)
    stage_race_serves_as_trial(con)
    stage_race_history(con, args.from_date)
    stage_horse_trial_history(con)
    stage_horse_target_race_trial_summary(con)
    write_partitioned(con, append_features_sql(input_glob), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
