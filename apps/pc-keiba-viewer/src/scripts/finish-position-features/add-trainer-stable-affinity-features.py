#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append trainer × grade / target_race affinity features (v7 layer).

Motivation:
  既存 v6 stack には trainer_career_win_rate / trainer_keibajo_win_rate /
  trainer_distance_win_rate / trainer_horse_win_rate がある。本 script は
  「trainer の grade_code 別成績」と「trainer の target_race 別成績 (G1 特化)」
  を追加投入する。これは horse の前走成績に依存しない純非前走系 signal。

Features added (per horse × race):
  - trainer_grade_career_starts        : 同 grade_code (G1=A/G2=B/G3=C) での 過去出走数
  - trainer_grade_win_rate             : 同 grade での win rate
  - trainer_grade_top3_rate            : 同 grade での 3 着以内 rate
  - trainer_target_race_career_count   : target_race_id 該当 race の 過去出走数
  - trainer_target_race_top3_count     : target_race での 3 着以内 count
  - trainer_target_race_win_count      : target_race での 1 着 count
  - trainer_target_race_has_history    : boolean (target_race 出走経験)

ターゲット race 該当時 (target_race_id not null) のみ target 関連カラムが non-null、
それ以外は NULL/0。

Data leakage 防止: race_date strictly less than current race_date のみを集計。
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
from _catalog_attach import attach_source_catalog
from _resource_defaults import add_resource_args, apply_to_connection

RACE_PARTITION = "source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_trainer_stable_affinity_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--category",
        choices=("jra", "nar"),
        default="jra",
        help="jra → pg.jvd_se / 'jra' source filter; nar → pg.nvd_se / 'nar' source filter",
    )
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
            "Compatibility marker for a focused keibajo_code:race_bango input. "
            "History staging is always narrowed to trainers present in the "
            "input parquet."
        ),
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    attach_source_catalog(con, pg_url)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def target_race_entry_filter_sql(
    con: duckdb.DuckDBPyConnection, category: str
) -> str:
    """Build exact input-race predicates that the remote SE scan can push down."""
    rows = con.execute(
        """
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango
        from base_input
        where source is not null and ketto_toroku_bango is not null
        order by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
                 ketto_toroku_bango
        """
    ).fetchall()
    horses_by_race: dict[tuple[str, str, str, str], set[str]] = {}
    for raw_source, raw_year, raw_date, raw_venue, raw_race, raw_horse in rows:
        if str(raw_source) != category:
            continue
        race_key = (str(raw_year), str(raw_date), str(raw_venue), str(raw_race))
        horses_by_race.setdefault(race_key, set()).add(str(raw_horse))
    if not horses_by_race:
        return "false"

    race_predicates: list[str] = []
    for race_key in sorted(horses_by_race):
        year, date, venue, race = race_key
        horse_literals = ", ".join(
            sql_literal(horse) for horse in sorted(horses_by_race[race_key])
        )
        race_predicates.append(
            f"(se.kaisai_nen = {sql_literal(year)} "
            f"and se.kaisai_tsukihi = {sql_literal(date)} "
            f"and se.keibajo_code = {sql_literal(venue)} "
            f"and se.race_bango = {sql_literal(race)} "
            f"and se.ketto_toroku_bango in ({horse_literals}))"
        )
    return " or ".join(race_predicates)


def target_trainer_filter_sql(con: duckdb.DuckDBPyConnection) -> str:
    """Build a constant trainer predicate that remote SE scans can push down."""
    rows = con.execute(
        """
        select chokyoshi_code
        from target_trainers
        where chokyoshi_code is not null and trim(chokyoshi_code) != ''
        order by chokyoshi_code
        """
    ).fetchall()
    trainer_literals = ", ".join(
        sql_literal(trainer)
        for trainer in sorted({str(raw_trainer) for (raw_trainer,) in rows})
    )
    if not trainer_literals:
        return "false"
    return f"se.chokyoshi_code in ({trainer_literals})"


def stage_target_trainers(con: duckdb.DuckDBPyConnection, category: str) -> None:
    """Stage trainer ids present in the target input parquet.

    The feature values still come from strictly prior race history; this table
    only narrows which trainers' history needs to be staged for focused and
    whole-day inputs alike.
    """
    se_table = "pg.jvd_se" if category == "jra" else "pg.nvd_se"
    target_filter = target_race_entry_filter_sql(con, category)
    con.execute(
        f"""
        create or replace temp table target_trainers as
        select distinct se.chokyoshi_code
        from {se_table} se
        where se.chokyoshi_code is not null and trim(se.chokyoshi_code) != ''
          and ({target_filter})
        """
    )
    con.execute("create index target_trainers_idx on target_trainers (chokyoshi_code)")


def stage_race_history_with_trainer(
    con: duckdb.DuckDBPyConnection,
    from_date: str,
    category: str,
) -> None:
    """horse の過去レース成績 + trainer (chokyoshi_code) + grade_code を取得。

    category=jra → pg.jvd_se / source='jra' filter
    category=nar → pg.nvd_se / source='nar' filter (ban-ei 含む全 nvd_se rows)
    """
    se_table = "pg.jvd_se" if category == "jra" else "pg.nvd_se"
    ra_table = "pg.jvd_ra" if category == "jra" else "pg.nvd_ra"
    source_filter = "jra" if category == "jra" else "nar"
    trainer_filter = target_trainer_filter_sql(con)
    from_date_literal = sql_literal(from_date)
    from_year_literal = sql_literal(from_date[:4])
    source_literal = sql_literal(source_filter)
    con.execute(
        f"""
        create or replace temp table race_history as
        select
          {source_literal} as source,
          se.kaisai_nen || se.kaisai_tsukihi as race_date,
          se.kaisai_nen,
          se.kaisai_tsukihi,
          se.keibajo_code,
          se.race_bango,
          se.ketto_toroku_bango,
          try_cast(nullif(trim(se.kakutei_chakujun), '00') as integer)
            as finish_position,
          ra.grade_code,
          se.chokyoshi_code
        from {se_table} se
        inner join {ra_table} ra
          using (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
        where se.kaisai_nen || se.kaisai_tsukihi >= {from_date_literal}
          and se.kaisai_nen >= {from_year_literal}
          and try_cast(nullif(trim(se.kakutei_chakujun), '00') as integer)
            is not null
          and nullif(trim(se.ketto_toroku_bango), '') is not null
          and try_cast(nullif(trim(se.umaban), '') as integer) is not null
          and se.chokyoshi_code is not null and trim(se.chokyoshi_code) != ''
          and ({trainer_filter})
        """
    )
    con.execute(
        "create index race_history_trainer_grade_idx on race_history (source, chokyoshi_code, grade_code, race_date)"
    )


def stage_trainer_grade_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """trainer × grade_code 別 cumul stats (career, lookback)。"""
    con.execute(
        """
        create or replace temp table trainer_grade_daily as
        select source, chokyoshi_code, grade_code, race_date,
          count(*) as starts_on_day,
          sum(case when finish_position = 1 then 1 else 0 end) as wins_on_day,
          sum(case when finish_position <= 3 then 1 else 0 end) as top3_on_day
        from race_history
        where grade_code is not null
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table trainer_grade_cumul as
        select source, chokyoshi_code, grade_code, race_date,
          sum(starts_on_day) over trainer_grade_career as past_starts,
          sum(wins_on_day) over trainer_grade_career as past_wins,
          sum(top3_on_day) over trainer_grade_career as past_top3
        from trainer_grade_daily
        window trainer_grade_career as (
          partition by source, chokyoshi_code, grade_code
          order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        "create index trainer_grade_cumul_idx on trainer_grade_cumul (source, chokyoshi_code, grade_code, race_date)"
    )


def stage_trainer_target_race_cumul(con: duckdb.DuckDBPyConnection) -> None:
    """trainer × target_race_id 別 cumul stats。

    base parquet の target_race_id を joined して race_history × target_race の cross 集計。
    """
    # First derive target_race_id per race from input base (since base has target_race_id from lineage step)
    con.execute(
        """
        create or replace temp table target_classification as
        select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, target_race_id
        from base_input
        where target_race_id is not null
        """
    )
    con.execute(
        "create index target_classification_idx on target_classification (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)"
    )
    con.execute(
        """
        create or replace temp table trainer_target_daily as
        select rh.source, rh.chokyoshi_code, tc.target_race_id, rh.race_date,
          count(*) as starts_on_day,
          sum(case when rh.finish_position = 1 then 1 else 0 end) as wins_on_day,
          sum(case when rh.finish_position <= 3 then 1 else 0 end) as top3_on_day
        from race_history rh
        join target_classification tc
          on tc.source = rh.source
          and tc.kaisai_nen = rh.kaisai_nen
          and tc.kaisai_tsukihi = rh.kaisai_tsukihi
          and tc.keibajo_code = rh.keibajo_code
          and tc.race_bango = rh.race_bango
        group by all
        """
    )
    con.execute(
        """
        create or replace temp table trainer_target_cumul as
        select source, chokyoshi_code, target_race_id, race_date,
          sum(starts_on_day) over trainer_target_career as past_starts,
          sum(wins_on_day) over trainer_target_career as past_wins,
          sum(top3_on_day) over trainer_target_career as past_top3
        from trainer_target_daily
        window trainer_target_career as (
          partition by source, chokyoshi_code, target_race_id
          order by race_date
          rows between unbounded preceding and current row
        )
        """
    )
    con.execute(
        "create index trainer_target_cumul_idx on trainer_target_cumul (source, chokyoshi_code, target_race_id, race_date)"
    )


def stage_base_input(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """parquet を一度 staging に読み込み (target_race_id を参照するため)。"""
    con.execute(
        f"""
        create or replace temp table base_input as
        select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
               ketto_toroku_bango, race_date, race_year, target_race_id
        from read_parquet('{input_glob}', hive_partitioning=true)
        """
    )
    con.execute(f"create index base_input_idx on base_input ({RACE_PARTITION})")


def append_features_sql(input_glob: str, category: str = "jra") -> str:
    se_table = "pg.jvd_se" if category == "jra" else "pg.nvd_se"
    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    base_with_trainer as (
      select b.*, se.chokyoshi_code
      from base b
      left join {se_table} se
        on se.kaisai_nen = b.kaisai_nen
        and se.kaisai_tsukihi = b.kaisai_tsukihi
        and se.keibajo_code = b.keibajo_code
        and se.race_bango = b.race_bango
        and se.ketto_toroku_bango = b.ketto_toroku_bango
    ),
    joined as (
      select
        bwt.* exclude (chokyoshi_code),
        tg.past_starts as trainer_grade_career_starts,
        case when tg.past_starts > 0
             then tg.past_wins::double / tg.past_starts
             else null end as trainer_grade_win_rate,
        case when tg.past_starts > 0
             then tg.past_top3::double / tg.past_starts
             else null end as trainer_grade_top3_rate,
        tt.past_starts as trainer_target_race_career_count,
        coalesce(tt.past_wins, 0) as trainer_target_race_win_count,
        coalesce(tt.past_top3, 0) as trainer_target_race_top3_count,
        case when coalesce(tt.past_starts, 0) > 0 then 1 else 0 end as trainer_target_race_has_history
      from base_with_trainer bwt
      asof left join trainer_grade_cumul tg
        on tg.source = bwt.source
        and tg.chokyoshi_code = bwt.chokyoshi_code
        and tg.grade_code = bwt.grade_code
        and tg.race_date < bwt.race_date
      asof left join trainer_target_cumul tt
        on tt.source = bwt.source
        and tt.chokyoshi_code = bwt.chokyoshi_code
        and tt.target_race_id = bwt.target_race_id
        and tt.race_date < bwt.race_date
    )
    select * from joined
    """


def write_partitioned(
    con: duckdb.DuckDBPyConnection, sql: str, output_dir: Path
) -> None:
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
    stage_base_input(con, input_glob)
    stage_target_trainers(con, args.category)
    stage_race_history_with_trainer(con, args.from_date, args.category)
    stage_trainer_grade_cumul(con)
    stage_trainer_target_race_cumul(con)
    write_partitioned(
        con, append_features_sql(input_glob, args.category), args.output_dir
    )
    con.close()


if __name__ == "__main__":
    main()
