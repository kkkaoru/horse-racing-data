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

Features added (per horse x race) — exactly 10 columns:
  - past_style_x_field_pace_match   AVAILABLE all years (uses field pressure)
  - sire_x_field_pace_score         AVAILABLE all years (uses field pressure)
  - rs_p_nige / rs_p_senkou / rs_p_sashi / rs_p_oikomi  (running-style v3 model
    probability, 2024+ only via PG ``race_running_style_model_predictions``)
  - rs_predicted_class              (model integer class, 2024+)
  - rs_confidence_entropy           (-sum p log p, 2024+)
  - rs_p_nige_x_field_pace          (cross term, 2024+)
  - rs_sire_style_match             (sum over k of p_k * sire_k_rate, 2024+)

For races whose running-style probabilities are not yet in PG (pre-2024 history
or a year where no rs model has scored yet), the seven rs_* columns are
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
from pathlib import Path

import duckdb

from _resource_defaults import add_resource_args, apply_to_connection

DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing"

# --rs-source selects where running-style probability rows come from. ``r2``
# reads the per-day Parquet that the sync-realtime-data Worker writes to R2 (key
# = ``running-style/predictions/by-day/{YYYY}/{MM}/{DD}/{source}/{model_version}.parquet``)
# under ``pc-keiba-features-archive``. ``pg`` keeps the legacy Neon ATTACH path.
# ``auto`` tries R2 first, then falls back to PG when the R2 setup raises (e.g.
# missing token, missing run-date, empty prefix) so the daily container can
# still finish even before the R2 token is provisioned.
RS_SOURCE_CHOICES = ("r2", "pg", "auto")
R2_BUCKET_DEFAULT = "pc-keiba-features-archive"
R2_PREDICTIONS_PREFIX = "running-style/predictions/by-day"

# Best available running-style model_version per year, per category. Used to
# select rs_* rows from PG when multiple model_versions exist for the same
# (source, year). Mirrors tmp/v8/iter9_build_pacestyle_features.py
# RS_VERSION_PREF — the 2026 production model wins for 2026 rows, the late-2024
# ensemble wins for 2024/2025.
RS_VERSION_PREF: dict[str, dict[int, str]] = {
    "jra": {
        2024: "jra-running-style-ens-lgbm-trans-v1.3",
        2025: "jra-running-style-ens-lgbm-trans-v1.3",
        2026: "jra-running-style-lgbm-prod-v1.5",
    },
    "nar": {
        2024: "nar-running-style-trans-v1.4",
        2025: "nar-running-style-trans-v1.4",
        2026: "nar-running-style-lgbm-prod-v1.5",
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
        "--rs-source",
        choices=RS_SOURCE_CHOICES,
        default="auto",
        help=(
            "Where to read race_running_style probabilities from. "
            "r2 = pc-keiba-features-archive Parquet; pg = Neon ATTACH (legacy); "
            "auto = R2 first, fall back to PG on any R2 error."
        ),
    )
    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        help=(
            "Target run date (YYYYMMDD) for rs-source=r2/auto. Selects which "
            "per-day Parquet shard to glob. Required when rs-source=r2."
        ),
    )
    add_resource_args(parser)
    return parser.parse_args(argv)


def install_and_attach_pg(con: duckdb.DuckDBPyConnection, pg_url: str) -> None:
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{pg_url}' as pg (type postgres, read_only)")


def build_version_filter_sql(category: str) -> str:
    """SQL fragment: ``(kaisai_nen='YYYY' and model_version='V') or ...``.

    Returns ``'false'`` when no year-version mapping is configured for the
    category (defensive — at present both categories have entries for
    2024/2025/2026 so this branch is only hit by a category typo).
    """
    pairs = RS_VERSION_PREF.get(category, {})
    if not pairs:
        return "false"
    clauses = [
        f"(kaisai_nen = '{year}' and model_version = '{version}')"
        for year, version in pairs.items()
    ]
    return " or ".join(clauses)


def stage_rs_predictions_from_pg(
    con: duckdb.DuckDBPyConnection, category: str
) -> None:
    """Build a ``rs_preds`` temp keyed by (race_id, ketto_toroku_bango).

    race_id format mirrors the rest of the pipeline:
      ``{category}:{kaisai_nen}:{kaisai_tsukihi}:{keibajo_code}:{race_bango}``

    Only the best model_version per (category, year) is loaded; rows from other
    model_versions are filtered out so each (race, horse) maps to exactly one
    probability vector.
    """
    version_filter = build_version_filter_sql(category)
    con.execute(
        f"""
        create or replace temp table rs_preds as
        select
          '{category}:' || kaisai_nen || ':' || kaisai_tsukihi
            || ':' || keibajo_code || ':' || race_bango as race_id,
          ketto_toroku_bango,
          cast(p_nige as double) as rs_p_nige,
          cast(p_senkou as double) as rs_p_senkou,
          cast(p_sashi as double) as rs_p_sashi,
          cast(p_oikomi as double) as rs_p_oikomi,
          cast(predicted_class as integer) as rs_predicted_class
        from pg.race_running_style_model_predictions
        where source = '{category}'
          and ({version_filter})
        """
    )
    con.execute(
        "create index rs_preds_idx on rs_preds (race_id, ketto_toroku_bango)"
    )


def setup_r2_duckdb_secret(con: duckdb.DuckDBPyConnection) -> None:
    """Install httpfs and register an R2-backed S3 secret on ``con``.

    Reads ``R2_ACCOUNT_ID`` / ``R2_ACCESS_KEY_ID`` / ``R2_SECRET_ACCESS_KEY``
    from the environment — KeyError propagates so ``--rs-source=auto`` can fall
    back to PG when the token is not provisioned yet.
    """
    account_id = os.environ["R2_ACCOUNT_ID"]
    key_id = os.environ["R2_ACCESS_KEY_ID"]
    secret = os.environ["R2_SECRET_ACCESS_KEY"]
    con.execute("install httpfs; load httpfs;")
    con.execute(
        f"""
        create or replace secret r2_secret (
          TYPE S3,
          KEY_ID '{key_id}',
          SECRET '{secret}',
          ENDPOINT '{account_id}.r2.cloudflarestorage.com',
          REGION 'auto',
          URL_STYLE 'path'
        )
        """
    )


def stage_rs_predictions_from_r2(
    con: duckdb.DuckDBPyConnection,
    category: str,
    run_date_ymd: str,
    bucket: str,
) -> None:
    """Build the ``rs_preds`` temp from the per-day R2 Parquet shard.

    Glob layout matches the Worker output:
      ``s3://{bucket}/running-style/predictions/by-day/{YYYY}/{MM}/{DD}/{category}/*.parquet``

    The wildcard accepts whichever ``model_version.parquet`` the Worker wrote;
    the v3 production model collapses to one file per (date, category).
    """
    yyyy = run_date_ymd[:4]
    mm = run_date_ymd[4:6]
    dd = run_date_ymd[6:8]
    glob = (
        f"s3://{bucket}/{R2_PREDICTIONS_PREFIX}/"
        f"{yyyy}/{mm}/{dd}/{category}/*.parquet"
    )
    con.execute(
        f"""
        create or replace temp table rs_preds as
        select
          '{category}:' || kaisai_nen || ':' || kaisai_tsukihi
            || ':' || keibajo_code || ':' || race_bango as race_id,
          ketto_toroku_bango,
          cast(p_nige as double) as rs_p_nige,
          cast(p_senkou as double) as rs_p_senkou,
          cast(p_sashi as double) as rs_p_sashi,
          cast(p_oikomi as double) as rs_p_oikomi,
          cast(predicted_class as integer) as rs_predicted_class
        from read_parquet('{glob}')
        """
    )
    con.execute(
        "create index rs_preds_idx on rs_preds (race_id, ketto_toroku_bango)"
    )


def append_features_sql(input_glob: str, category: str) -> str:
    """SELECT that joins base parquet to rs_preds + computes pace-x-style cols.

    The two pressure-derived columns (``past_style_x_field_pace_match`` /
    ``sire_x_field_pace_score``) reduce to ``NULL`` when their inputs are not
    present (defensive; the v6 race-internal layer always emits ``field_*_pressure``
    so in production both will be populated).
    """
    base_extra = (
        "(coalesce(b.past_nige_rate_self, 0) * coalesce(b.field_nige_pressure, 0)"
        " + coalesce(b.past_senkou_rate_self, 0) * coalesce(b.field_senkou_pressure, 0)"
        " + coalesce(b.past_sashi_rate_self, 0) * coalesce(b.field_sashi_pressure, 0)"
        " + coalesce(b.past_oikomi_rate_self, 0) * coalesce(b.field_oikomi_pressure, 0))"
        " as past_style_x_field_pace_match,\n"
        " (coalesce(b.sire_nige_rate, 0) * coalesce(b.field_nige_pressure, 0)"
        " + coalesce(b.sire_senkou_rate, 0) * coalesce(b.field_senkou_pressure, 0)"
        " + coalesce(b.sire_sashi_rate, 0) * coalesce(b.field_sashi_pressure, 0)"
        " + coalesce(b.sire_oikomi_rate, 0) * coalesce(b.field_oikomi_pressure, 0))"
        " as sire_x_field_pace_score"
    )

    rs_extra = (
        "rs.rs_p_nige, rs.rs_p_senkou, rs.rs_p_sashi, rs.rs_p_oikomi, "
        "rs.rs_predicted_class, "
        "-("
        " coalesce(rs.rs_p_nige, 0) * ln(coalesce(rs.rs_p_nige, 0) + 1e-9)"
        " + coalesce(rs.rs_p_senkou, 0) * ln(coalesce(rs.rs_p_senkou, 0) + 1e-9)"
        " + coalesce(rs.rs_p_sashi, 0) * ln(coalesce(rs.rs_p_sashi, 0) + 1e-9)"
        " + coalesce(rs.rs_p_oikomi, 0) * ln(coalesce(rs.rs_p_oikomi, 0) + 1e-9)"
        ") as rs_confidence_entropy, "
        "(coalesce(rs.rs_p_nige, 0) * coalesce(b.field_pace_index, 0)) "
        " as rs_p_nige_x_field_pace, "
        "(coalesce(rs.rs_p_nige, 0) * coalesce(b.sire_nige_rate, 0)"
        " + coalesce(rs.rs_p_senkou, 0) * coalesce(b.sire_senkou_rate, 0)"
        " + coalesce(rs.rs_p_sashi, 0) * coalesce(b.sire_sashi_rate, 0)"
        " + coalesce(rs.rs_p_oikomi, 0) * coalesce(b.sire_oikomi_rate, 0))"
        " as rs_sire_style_match"
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


def write_partitioned(con: duckdb.DuckDBPyConnection, sql: str, output_dir: Path) -> None:
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
    """Dispatch to the R2 or PG rs_preds loader based on ``args.rs_source``.

    For ``r2`` any failure propagates. For ``auto`` R2 is attempted first and any
    Exception falls back to the legacy PG path so an unprovisioned R2 token does
    not block today's predictions. For ``pg`` the R2 path is never touched.
    """
    bucket = os.environ.get("R2_BUCKET", R2_BUCKET_DEFAULT)
    if args.rs_source in ("r2", "auto"):
        try:
            if not args.run_date:
                raise ValueError(
                    "--run-date YYYYMMDD is required when --rs-source=r2 or auto"
                )
            setup_r2_duckdb_secret(con)
            stage_rs_predictions_from_r2(con, args.category, args.run_date, bucket)
            return
        except Exception:
            if args.rs_source == "r2":
                raise
    install_and_attach_pg(con, args.pg_url)
    stage_rs_predictions_from_pg(con, args.category)


def main() -> None:
    args = parse_args()
    input_glob = f"{args.input_dir.as_posix()}/race_year=*/*.parquet"
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_object_cache=true")
    apply_to_connection(con, args.threads, args.memory_limit)
    con.execute("SET preserve_insertion_order=false")
    stage_rs_predictions(con, args)
    write_partitioned(con, append_features_sql(input_glob, args.category), args.output_dir)
    con.close()


if __name__ == "__main__":
    main()
