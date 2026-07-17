#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Append pace x style features (v8 iter9 layer).

Motivation:
  Iter 9 (v8) introduced ten pace-x-style signals that combine the horse's past
  running-style usage / sire-style tendency with the race-level pace pressure
  field already present in v7-lineage's ``add-race-internal-features.py``
  (field_*_pressure columns), plus the running-style probability distribution
  predicted by the ``race_running_style_model_predictions`` PG table for 2024+
  races. Tested in tmp/v8/iter9 with +0.10pp NDCG@10 lift on NAR and ~+0.04pp
  on JRA per docs/finish-position-accuracy/iter9 — promoted into the production
  per-category layer chain so the runtime container can rebuild the same vector
  for upcoming races.

Features added (per horse x race) — exactly 13 columns:
  - past_style_x_field_pace_match   AVAILABLE all years (uses field pressure)
  - sire_x_field_pace_score         AVAILABLE all years (uses field pressure)
  - rs_p_nige / rs_p_senkou / rs_p_sashi / rs_p_oikomi  (running-style v3 model
    probability, 2024+ only via PG ``race_running_style_model_predictions``)
  - rs_predicted_class              (model integer class, 2024+)
  - rs_predicted_corner_front_score (expected front score from rs probabilities)
  - rs_predicted_corner_rank        (race-level rank by the expected front score)
  - rs_predicted_corner_rank_pct    (rank normalized by shusso_tosu when available)
  - rs_confidence_entropy           (-sum p log p, 2024+)
  - rs_p_nige_x_field_pace          (cross term, 2024+)
  - rs_sire_style_match             (sum over k of p_k * sire_k_rate, 2024+)

For races whose running-style probabilities are not yet in PG (pre-2024 history
or a year where no rs model has scored yet), the rs_* columns are
emitted as NULL — CatBoost/XGBoost treat absent numeric inputs as 0.

Data leakage 防止: rs_* features only join when the rs_predictions row exists
for the SAME race_id (no cross-race aggregation), so no future-information
leakage. The pressure-derived columns are pure SELECTs from existing race-
internal columns already present in the input parquet (no PG read needed for
those two).
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

import duckdb

from _catalog_attach import attach_source_catalog

from _resource_defaults import add_resource_args, apply_to_connection

DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"
R2_BUCKET_DEFAULT = "pc-keiba-features-archive"
RUNNING_STYLE_CATALOG_GENERATION = "raw-iceberg-v1"
R2_PREDICTIONS_PREFIX = (
    f"running-style/predictions/by-day/{RUNNING_STYLE_CATALOG_GENERATION}"
)


# Best available running-style model_version per year, per category. Used to
# select rs_* rows from PG when multiple model_versions exist for the same
# (source, year). Mirrors tmp/v8/iter9_build_pacestyle_features.py
# RS_VERSION_PREF — the 2026 production model wins for 2026 rows, the late-2024
# ensemble wins for 2024/2025.
#
# This table is a starting preference, not the sole source of truth: the catalog
# staging path resolves each entry through resolve_version_pref(), selecting the
# model_version with the most recent race date when the preferred string matches
# zero rows. This resolution exists because this table went stale
# 2026-05-24 (production moved prod-v1.5 -> prod-v3 for 2026 without this
# table being updated) and silently zeroed every rs_p_* feature for the rest
# of 2026 until discovered 2026-07-11.
RS_VERSION_PREF: dict[str, dict[int, str]] = {
    "jra": {
        2024: "jra-running-style-ens-lgbm-trans-v1.3",
        2025: "jra-running-style-ens-lgbm-trans-v1.3",
        2026: "jra-running-style-lgbm-prod-v3",
    },
    "nar": {
        2024: "nar-running-style-trans-v1.4",
        2025: "nar-running-style-trans-v1.4",
        2026: "nar-running-style-lgbm-prod-v3",
    },
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="add_pacestyle_features")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--category",
        choices=("jra", "nar"),
        required=True,
        help="jra → pulls rs preds with source='jra'; nar → source='nar'",
    )
    parser.add_argument(
        "--pg-url",
        type=str,
        default=os.environ.get("LOCAL_PG_URL", DEFAULT_PG_URL),
    )
    parser.add_argument("--from-date", type=str, default="20100101")
    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        help="YYYYMMDD shard for raw-catalog-generated running-style predictions",
    )
    parser.add_argument(
        "--target-race",
        type=str,
        default=None,
        help=(
            "Focused production mode keibajo_code:race_bango. The input parquet "
            "is already race-scoped; this switches catalog staging to those race IDs."
        ),
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    attach_source_catalog(con, pg_url)


def build_version_filter_sql_from_pairs(pairs: dict[int, str]) -> str:
    """SQL fragment: ``(kaisai_nen='YYYY' and model_version='V') or ...``
    built from an explicit year -> model_version mapping.

    Returns ``'false'`` when ``pairs`` is empty (defensive — at present both
    categories have entries for 2024/2025/2026 so this branch is only hit by
    a category typo).
    """
    if not pairs:
        return "false"
    clauses = [
        f"(kaisai_nen = '{year}' and model_version = '{version}')"
        for year, version in pairs.items()
    ]
    return " or ".join(clauses)


def build_version_filter_sql(category: str) -> str:
    """Return a static preference-only SQL fragment without catalog lookups."""
    return build_version_filter_sql_from_pairs(RS_VERSION_PREF.get(category, {}))


def _preferred_version_has_rows(
    con: duckdb.DuckDBPyConnection, category: str, year: int, version: str
) -> bool:
    """Return whether the attached catalog has this model version."""
    row = con.execute(
        f"""
        select 1
        from pg.race_running_style_model_predictions
        where source = '{category}' and kaisai_nen = '{year}' and model_version = '{version}'
        limit 1
        """
    ).fetchone()
    return row is not None


def _latest_available_model_version(
    con: duckdb.DuckDBPyConnection, category: str, year: int
) -> str | None:
    """Model_version with the most recent kaisai_tsukihi for (category, year).

    ``None`` when the catalog has no rows for that (category, year) — e.g. a
    future year the champion has not scored yet. Ties (two model_versions
    sharing the same latest race date, such as a same-day champion flip
    plus rollback) break on model_version string descending, so the result
    is deterministic across repeated calls.
    """
    row = con.execute(
        f"""
        select model_version
        from pg.race_running_style_model_predictions
        where source = '{category}' and kaisai_nen = '{year}'
        group by model_version
        order by max(kaisai_tsukihi) desc, model_version desc
        limit 1
        """
    ).fetchone()
    return str(row[0]) if row is not None else None


def resolve_version_pref(
    con: duckdb.DuckDBPyConnection, category: str
) -> dict[int, str]:
    """Resolve ``RS_VERSION_PREF`` against the attached catalog, per year.

    ``RS_VERSION_PREF`` is a hardcoded per-year preference that goes stale
    whenever the champion RS model_version changes without this dict being
    updated in the same commit — exactly what happened 2026-05-24, when
    production moved from ``{jra,nar}-running-style-lgbm-prod-v1.5`` to
    ``prod-v3`` and this dict kept pinning v1.5, silently zeroing every
    rs_p_* column for the whole 2026 catalog path until this fix
    (discovered 2026-07-11).

    A preferred version that still has >=1 row is kept as-is (one cheap
    existence check, no behavior change from before). A preferred version
    with zero rows selects whichever model_version has the most recent race
    date for that (category, year) — self-healing across
    future champion changes instead of needing another hardcoded-string
    fix. Years with no PG rows at all for the category keep the original
    preference (harmless no-op: the filter clause matches nothing either
    way, same as pre-fix behavior).
    """
    preferred = RS_VERSION_PREF.get(category, {})
    resolved: dict[int, str] = {}
    for year, preferred_version in preferred.items():
        if _preferred_version_has_rows(con, category, year, preferred_version):
            resolved[year] = preferred_version
            continue
        available_version = _latest_available_model_version(con, category, year)
        if available_version is None:
            resolved[year] = preferred_version
            continue
        resolved[year] = available_version
        print(
            f"[add-pacestyle-features] RS_VERSION_PREF stale for {category} {year}: "
            f"preferred {preferred_version!r} has 0 rows, using most-recent "
            f"{available_version!r}",
            file=sys.stderr,
        )
    return resolved


def stage_target_race_ids(
    con: duckdb.DuckDBPyConnection, input_glob: str, category: str
) -> None:
    """Stage race IDs present in the input parquet for focused target-race mode."""
    con.execute(
        f"""
        create or replace temp table target_race_ids as
        select distinct
          '{category}:' || kaisai_nen || ':' || kaisai_tsukihi
            || ':' || keibajo_code || ':' || race_bango as race_id
        from read_parquet('{input_glob}', hive_partitioning=true)
        """
    )
    con.execute("create index target_race_ids_idx on target_race_ids (race_id)")


def target_race_ids_filter_sql(focused_target: bool) -> str:
    if focused_target:
        return (
            "and ('{category}:' || kaisai_nen || ':' || kaisai_tsukihi"
            " || ':' || keibajo_code || ':' || race_bango)"
            " in (select race_id from target_race_ids)"
        )
    return ""


def stage_rs_predictions_from_catalog(
    con: duckdb.DuckDBPyConnection, category: str, focused_target: bool = False
) -> None:
    """Build a ``rs_preds`` temp keyed by (race_id, ketto_toroku_bango).

    race_id format mirrors the rest of the pipeline:
      ``{category}:{kaisai_nen}:{kaisai_tsukihi}:{keibajo_code}:{race_bango}``

    Only the best model_version per (category, year) is loaded; rows from other
    model_versions are filtered out so each (race, horse) maps to exactly one
    probability vector. The best version is resolved against catalog data
    (``resolve_version_pref``) rather than read verbatim off the hardcoded
    ``RS_VERSION_PREF`` table, so a stale preference for the current year
    selects the latest available catalog model_version instead of matching zero rows.
    """
    version_filter = build_version_filter_sql_from_pairs(
        resolve_version_pref(con, category)
    )
    target_filter = target_race_ids_filter_sql(focused_target).format(category=category)
    con.execute(
        f"""
        create or replace temp table rs_preds as
        with rs_source as (
          select
            '{category}:' || kaisai_nen || ':' || kaisai_tsukihi
              || ':' || keibajo_code || ':' || race_bango as race_id,
            ketto_toroku_bango,
            cast(p_nige as double) as rs_p_nige,
            cast(p_senkou as double) as rs_p_senkou,
            cast(p_sashi as double) as rs_p_sashi,
            cast(p_oikomi as double) as rs_p_oikomi,
            cast(predicted_class as integer) as rs_predicted_class,
            cast(cell_model_key as varchar) as rs_cell_model_key,
            cast(cell_variant_id as varchar) as rs_cell_variant_id
          from pg.race_running_style_model_predictions
          where source = '{category}'
            and ({version_filter})
            {target_filter}
        ),
        rs_scored as (
          select
            *,
            rs_p_senkou + 2 * rs_p_sashi + 3 * rs_p_oikomi
              as rs_predicted_corner_front_score
          from rs_source
        )
        select
          *,
          row_number() over (
            partition by race_id
            order by rs_predicted_corner_front_score asc, rs_p_nige desc, ketto_toroku_bango asc
          ) as rs_predicted_corner_rank
        from rs_scored
        """
    )
    con.execute("create index rs_preds_idx on rs_preds (race_id, ketto_toroku_bango)")


def setup_r2_duckdb_secret(con: duckdb.DuckDBPyConnection) -> None:
    account_id = os.environ["R2_ACCOUNT_ID"]
    key_id = os.environ["R2_ACCESS_KEY_ID"]
    secret = os.environ["R2_SECRET_ACCESS_KEY"]
    con.execute("install httpfs; load httpfs;")
    con.execute(
        f"""
        create or replace secret r2_secret (
          type s3,
          key_id '{key_id}',
          secret '{secret}',
          endpoint '{account_id}.r2.cloudflarestorage.com',
          region 'auto',
          url_style 'path'
        )
        """
    )


def create_empty_rs_predictions(con: duckdb.DuckDBPyConnection) -> None:
    """Create the typed empty relation used when no same-day R2 shard exists."""
    con.execute(
        """
        create or replace temp table rs_preds as
        select
          cast(null as varchar) as race_id,
          cast(null as varchar) as ketto_toroku_bango,
          cast(null as double) as rs_p_nige,
          cast(null as double) as rs_p_senkou,
          cast(null as double) as rs_p_sashi,
          cast(null as double) as rs_p_oikomi,
          cast(null as integer) as rs_predicted_class,
          cast(null as varchar) as rs_cell_model_key,
          cast(null as varchar) as rs_cell_variant_id,
          cast(null as double) as rs_predicted_corner_front_score,
          cast(null as bigint) as rs_predicted_corner_rank
        where false
        """
    )
    con.execute("create index rs_preds_idx on rs_preds (race_id, ketto_toroku_bango)")


def stage_rs_predictions_from_r2(
    con: duckdb.DuckDBPyConnection,
    category: str,
    run_date_ymd: str,
    bucket: str,
    focused_target: bool = False,
) -> None:
    yyyy = run_date_ymd[:4]
    mm = run_date_ymd[4:6]
    dd = run_date_ymd[6:8]
    glob = (
        f"s3://{bucket}/{R2_PREDICTIONS_PREFIX}/{yyyy}/{mm}/{dd}/{category}/*.parquet"
    )
    target_filter = target_race_ids_filter_sql(focused_target).format(category=category)
    try:
        con.execute(
            f"""
            create or replace temp table rs_preds as
            with rs_source as (
              select
                '{category}:' || kaisai_nen || ':' || kaisai_tsukihi
                  || ':' || keibajo_code || ':' || race_bango as race_id,
                ketto_toroku_bango,
                cast(p_nige as double) as rs_p_nige,
                cast(p_senkou as double) as rs_p_senkou,
                cast(p_sashi as double) as rs_p_sashi,
                cast(p_oikomi as double) as rs_p_oikomi,
                cast(predicted_class as integer) as rs_predicted_class,
                cast(cell_model_key as varchar) as rs_cell_model_key,
                cast(cell_variant_id as varchar) as rs_cell_variant_id
              from read_parquet('{glob}')
              where true {target_filter}
            ),
            rs_scored as (
              select *,
                rs_p_senkou + 2 * rs_p_sashi + 3 * rs_p_oikomi
                  as rs_predicted_corner_front_score
              from rs_source
            )
            select *,
              row_number() over (
                partition by race_id
                order by rs_predicted_corner_front_score asc,
                  rs_p_nige desc, ketto_toroku_bango asc
              ) as rs_predicted_corner_rank
            from rs_scored
            """
        )
    except duckdb.IOException as error:
        if "No files found that match" not in str(error):
            raise
        print(
            f"[finish-position-features] rs_shard_status category={category} "
            f"race_date={run_date_ymd} shard_found=False",
            file=sys.stderr,
        )
        create_empty_rs_predictions(con)
        return
    print(
        f"[finish-position-features] rs_shard_status category={category} "
        f"race_date={run_date_ymd} shard_found=True",
        file=sys.stderr,
    )
    con.execute("create index rs_preds_idx on rs_preds (race_id, ketto_toroku_bango)")


def append_features_sql(input_glob: str, category: str) -> str:
    """SELECT that joins base parquet to rs_preds + computes pace-x-style cols.

    The two pressure-derived columns (``past_style_x_field_pace_match`` /
    ``sire_x_field_pace_score``) reduce to ``NULL`` when their inputs are not
    present (defensive; the v6 race-internal layer always emits ``field_*_pressure``
    so in production both will be populated).
    """
    base_extra = (
        "CASE WHEN b.past_nige_rate_self IS NOT NULL THEN"
        " b.past_nige_rate_self * coalesce(b.field_nige_pressure, 0)"
        " + b.past_senkou_rate_self * coalesce(b.field_senkou_pressure, 0)"
        " + b.past_sashi_rate_self * coalesce(b.field_sashi_pressure, 0)"
        " + b.past_oikomi_rate_self * coalesce(b.field_oikomi_pressure, 0)"
        " END as past_style_x_field_pace_match,\n"
        " CASE WHEN b.sire_nige_rate IS NOT NULL THEN"
        " b.sire_nige_rate * coalesce(b.field_nige_pressure, 0)"
        " + b.sire_senkou_rate * coalesce(b.field_senkou_pressure, 0)"
        " + b.sire_sashi_rate * coalesce(b.field_sashi_pressure, 0)"
        " + b.sire_oikomi_rate * coalesce(b.field_oikomi_pressure, 0)"
        " END as sire_x_field_pace_score"
    )

    rs_extra = (
        "rs.rs_p_nige, rs.rs_p_senkou, rs.rs_p_sashi, rs.rs_p_oikomi, "
        "rs.rs_predicted_class, "
        "rs.rs_predicted_corner_front_score, "
        "rs.rs_predicted_corner_rank, "
        "CASE WHEN rs.rs_predicted_corner_rank IS NOT NULL"
        " AND b.shusso_tosu IS NOT NULL AND b.shusso_tosu > 1 THEN"
        " (rs.rs_predicted_corner_rank - 1) / cast((b.shusso_tosu - 1) as double)"
        " END as rs_predicted_corner_rank_pct, "
        "CASE WHEN rs.rs_p_nige IS NOT NULL THEN"
        " -(rs.rs_p_nige * ln(rs.rs_p_nige + 1e-9)"
        " + rs.rs_p_senkou * ln(rs.rs_p_senkou + 1e-9)"
        " + rs.rs_p_sashi * ln(rs.rs_p_sashi + 1e-9)"
        " + rs.rs_p_oikomi * ln(rs.rs_p_oikomi + 1e-9))"
        " END as rs_confidence_entropy, "
        "CASE WHEN rs.rs_p_nige IS NOT NULL THEN"
        " rs.rs_p_nige * coalesce(b.field_pace_index, 0)"
        " END as rs_p_nige_x_field_pace, "
        "CASE WHEN rs.rs_p_nige IS NOT NULL THEN"
        " rs.rs_p_nige * coalesce(b.sire_nige_rate, 0)"
        " + rs.rs_p_senkou * coalesce(b.sire_senkou_rate, 0)"
        " + rs.rs_p_sashi * coalesce(b.sire_sashi_rate, 0)"
        " + rs.rs_p_oikomi * coalesce(b.sire_oikomi_rate, 0)"
        " END as rs_sire_style_match"
    )

    race_id_expr = (
        f"'{category}:' || b.kaisai_nen || ':' || b.kaisai_tsukihi"
        " || ':' || b.keibajo_code || ':' || b.race_bango"
    )

    return f"""
    with base as (
      select * from read_parquet('{input_glob}', hive_partitioning=true)
    ),
    joined as (
      select
        b.*,
        {base_extra},
        {rs_extra}
      from base b
      left join rs_preds rs
        on rs.race_id = {race_id_expr}
        and rs.ketto_toroku_bango = b.ketto_toroku_bango
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


def stage_rs_predictions(
    con: duckdb.DuckDBPyConnection,
    args: argparse.Namespace,
) -> None:
    """Load only the requested raw-Catalog generation in production."""
    focused_target = getattr(args, "target_race", None) is not None
    run_date = getattr(args, "run_date", None)
    if run_date is not None:
        setup_r2_duckdb_secret(con)
        stage_rs_predictions_from_r2(
            con,
            args.category,
            run_date,
            os.environ.get("R2_BUCKET", R2_BUCKET_DEFAULT),
            focused_target,
        )
        return
    if args.pg_url.startswith("r2-catalog://"):
        raise ValueError("--run-date YYYYMMDD is required for Catalog production")
    install_and_attach_pg(con, args.pg_url)
    stage_rs_predictions_from_catalog(con, args.category, focused_target)


def main() -> None:
    args = parse_args()
    input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    if args.target_race is not None:
        stage_target_race_ids(con, input_glob, args.category)
    stage_rs_predictions(con, args)
    write_partitioned(
        con, append_features_sql(input_glob, args.category), args.output_dir
    )
    con.close()


if __name__ == "__main__":
    main()
