#!/usr/bin/env python3
"""Leak-free RS-feature imputation keyed by (keibajo_code, kyori_band).

Background (D2a diagnosis, commit eb249b9):
  NAR venues 43 (Funabashi) and 44 (Kawasaki) have 25.9% NULL on all
  past_nige/senkou/sashi/oikomi_rate_self features in the 2023-2025 holdout,
  vs 9.3% at other NAR venues.  The NULL explosion is caused by locally-
  anchored horses (≥70 % of career races at 43/44 only) that have no
  corner-position history and therefore score NULL across all 59 RS/pacestyle
  features.  The 59-feature group has LOFO importance of ~15 pp.

Fix (H-RS-KEIBAJO-IMPUTE probe):
  When a horse's RS features (past_nige_rate_self etc.) are NULL, fill them
  with the empirical mean observed for the same (keibajo_code, kyori_band)
  cell in ALL PAST RACES whose race_date is STRICTLY BEFORE the target race.
  If the cell contains fewer than MIN_CELL_COUNT non-NULL samples, fall back
  to the NAR-global prior for that kyori_band.  A flag column ``rs_imputed``
  is set to 1 whenever any imputation fires.

Leak-free guarantee:
  - ``build_prior_table(df, cutoff_date)`` only uses rows where
    ``race_date < cutoff_date``.  The caller must pass the EARLIEST date in
    the split being scored (training fold or holdout start).
  - ``impute_rs_features(df, prior_df, global_prior_df)`` is stateless and
    deterministic; it never reads the target label.

Train/serve parity:
  The SAME function (``impute_rs_features``) is called in both the offline
  feature-build pipeline (add-pacestyle-features.py or a new post-step) and
  the online serving path.  The prior table is computed once per WF fold from
  data strictly before the fold start and stored alongside the fold model.

Columns imputed (all go NULL together when RS is missing):
  - past_nige_rate_self, past_senkou_rate_self,
    past_sashi_rate_self, past_oikomi_rate_self
  - past_corner_1_norm_avg_5, past_corner_1_norm_avg_3,
    past_corner_1_norm_avg_10, past_corner_1_norm_std_5,
    past_corner_1_norm_best_5, past_corner_1_norm_worst_5,
    past_corner_1_norm_iqr_5, past_corner_progression_avg_5
  - horse_keibajo_corner_1_norm_avg
  - last_race_corner_1_norm (only if NULL)
  - past_nige_win_rate_self, past_senkou_win_rate_self,
    past_sashi_win_rate_self, past_oikomi_win_rate_self

Plus new column:
  - rs_imputed: 0 (no imputation) or 1 (imputed from prior)

Representation note (polars migration):
  polars has no index, so ``cell_prior``/``global_prior`` are plain DataFrames
  with the group columns as REGULAR columns:
    - cell_prior columns: keibajo_code, kyori_band, <impute cols present>, n
    - global_prior columns: kyori_band, <impute cols present>
"""
from __future__ import annotations

import math
from typing import Final

import polars as pl

# Sentinel value: cells with fewer than this many non-NULL observations fall
# back to the NAR-global prior for that kyori_band.
MIN_CELL_COUNT: Final[int] = 20

# Primary trigger: if ALL four rate columns are NULL, the row is an imputation
# candidate.  (They are always NULL together; see horse_running_style_history_cte.)
RS_TRIGGER_COLS: Final[tuple[str, ...]] = (
    "past_nige_rate_self",
    "past_senkou_rate_self",
    "past_sashi_rate_self",
    "past_oikomi_rate_self",
)

# Full set of RS/corner columns that are imputed when the trigger fires.
RS_IMPUTE_COLS: Final[tuple[str, ...]] = (
    "past_nige_rate_self",
    "past_senkou_rate_self",
    "past_sashi_rate_self",
    "past_oikomi_rate_self",
    "past_corner_1_norm_avg_5",
    "past_corner_1_norm_avg_3",
    "past_corner_1_norm_avg_10",
    "past_corner_1_norm_std_5",
    "past_corner_1_norm_best_5",
    "past_corner_1_norm_worst_5",
    "past_corner_1_norm_iqr_5",
    "past_corner_progression_avg_5",
    "horse_keibajo_corner_1_norm_avg",
    "last_race_corner_1_norm",
    "past_nige_win_rate_self",
    "past_senkou_win_rate_self",
    "past_sashi_win_rate_self",
    "past_oikomi_win_rate_self",
)

# Keys for prior lookup.
PRIOR_GROUP_COLS: Final[tuple[str, str]] = ("keibajo_code", "kyori_band")

RS_IMPUTED_COL: Final[str] = "rs_imputed"


def rs_null_mask(df: pl.DataFrame) -> pl.Series:
    """Boolean mask: True where ALL RS trigger columns are NULL."""
    trigger_cols_present = [c for c in RS_TRIGGER_COLS if c in df.columns]
    if not trigger_cols_present:
        return pl.Series([False] * df.height, dtype=pl.Boolean)
    mask_expr = pl.col(trigger_cols_present[0]).is_null()
    for col in trigger_cols_present[1:]:
        mask_expr = mask_expr & pl.col(col).is_null()
    return df.select(mask_expr.alias("_mask")).get_column("_mask")


def build_prior_table(
    df: pl.DataFrame,
    cutoff_date: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Compute (keibajo, kyori_band) and (kyori_band) prior tables from past races.

    Only rows where ``race_date < cutoff_date`` are used — this guarantees zero
    future-data leakage when the caller passes the first date of the split being
    predicted.

    Parameters
    ----------
    df:
        Full feature DataFrame.  Must contain ``race_date``, ``keibajo_code``,
        ``kyori_band``, and all RS columns.
    cutoff_date:
        ISO-8601 or YYYYMMDD string.  Strictly less-than comparison on
        ``race_date`` (string comparison is safe because both use YYYYMMDD).

    Returns
    -------
    cell_prior:
        DataFrame with regular columns ``keibajo_code``, ``kyori_band``, one
        column per imputed feature (mean over non-NULL rows), and an ``n``
        column (non-NULL count for the trigger column ``past_nige_rate_self``).
    global_prior:
        DataFrame with regular column ``kyori_band`` and the same impute-column
        means over all NAR races before cutoff — the fallback when a cell is
        sparse.
    """
    past = df.filter(pl.col("race_date") < cutoff_date)

    impute_cols_present = [c for c in RS_IMPUTE_COLS if c in past.columns]

    cell_prior = past.group_by(list(PRIOR_GROUP_COLS)).agg(
        [pl.col(c).mean().alias(c) for c in impute_cols_present]
    )
    cell_counts = (
        past.filter(pl.col("past_nige_rate_self").is_not_null())
        .group_by(list(PRIOR_GROUP_COLS))
        .agg(pl.len().alias("n"))
    )
    cell_prior = cell_prior.join(
        cell_counts, on=list(PRIOR_GROUP_COLS), how="left"
    ).with_columns(pl.col("n").fill_null(0).cast(pl.Int64))

    global_prior = past.group_by("kyori_band").agg(
        [pl.col(c).mean().alias(c) for c in impute_cols_present]
    )

    return cell_prior, global_prior


def _build_cell_lookup(
    cell_prior: pl.DataFrame,
    impute_cols_present: list[str],
) -> dict[tuple[str, int], dict[str, float]]:
    """Build {(keibajo, kyori_band): {"n": float, col: float, ...}} from cell prior.

    NaN/None column values are skipped so absent columns fall through to global.
    """
    cell_cols_present = [c for c in impute_cols_present if c in cell_prior.columns]
    has_n = "n" in cell_prior.columns
    lookup: dict[tuple[str, int], dict[str, float]] = {}
    for row in cell_prior.iter_rows(named=True):
        key = (str(row["keibajo_code"]), int(row["kyori_band"]))
        n_val = float(row["n"]) if has_n and row["n"] is not None else 0.0
        row_dict: dict[str, float] = {"n": n_val}
        for col in cell_cols_present:
            v = row[col]
            if v is not None and not math.isnan(float(v)):
                row_dict[col] = float(v)
        lookup[key] = row_dict
    return lookup


def _build_global_lookup(
    global_prior: pl.DataFrame,
    impute_cols_present: list[str],
) -> dict[int, dict[str, float]]:
    """Build {kyori_band: {col: float, ...}} from global prior, skipping None/NaN."""
    global_cols_present = [c for c in impute_cols_present if c in global_prior.columns]
    lookup: dict[int, dict[str, float]] = {}
    for row in global_prior.iter_rows(named=True):
        kb = int(row["kyori_band"])
        gd: dict[str, float] = {}
        for col in global_cols_present:
            v = row[col]
            if v is not None and not math.isnan(float(v)):
                gd[col] = float(v)
        lookup[kb] = gd
    return lookup


def impute_rs_features(
    df: pl.DataFrame,
    prior_df: pl.DataFrame,
    global_prior_df: pl.DataFrame,
) -> pl.DataFrame:
    """Apply leak-free RS imputation (returns a new DataFrame).

    For each row where ALL RS trigger columns are NULL:
      1. Look up the (keibajo_code, kyori_band) cell in ``prior_df``.
      2. If n >= MIN_CELL_COUNT, fill each NULL impute column with the cell mean.
      3. Otherwise fall back to ``global_prior_df`` keyed by kyori_band.
      4. Set ``rs_imputed = 1`` for that row.

    Non-NULL rows are never modified.  ``rs_imputed = 0`` for all originally
    non-NULL rows (column is created/reset unconditionally).

    Parameters
    ----------
    df:
        Feature DataFrame for one split (train or holdout).  Must contain
        ``keibajo_code``, ``kyori_band``, and the RS columns.
    prior_df:
        Output of ``build_prior_table`` — columns keibajo_code, kyori_band, ..., n.
    global_prior_df:
        Output of ``build_prior_table`` — column kyori_band, plus impute means.

    Returns
    -------
    DataFrame with RS columns filled and ``rs_imputed`` column added.
    """
    out = df.with_columns(pl.lit(0).alias(RS_IMPUTED_COL))

    null_mask = rs_null_mask(out)
    if not bool(null_mask.any()):
        return out

    impute_cols_present = [c for c in RS_IMPUTE_COLS if c in out.columns]
    cell_lookup = _build_cell_lookup(prior_df, impute_cols_present)
    global_lookup = _build_global_lookup(global_prior_df, impute_cols_present)

    null_positions = [i for i, v in enumerate(null_mask.to_list()) if v]
    rows = out.to_dicts()

    for idx in null_positions:
        row = rows[idx]
        keibajo = str(row["keibajo_code"])
        kyori_band_val = int(row["kyori_band"])
        key = (keibajo, kyori_band_val)

        cell_data = cell_lookup.get(key)
        if cell_data is not None and cell_data.get("n", 0.0) >= MIN_CELL_COUNT:
            fill_values = {
                col: cell_data[col]
                for col in impute_cols_present
                if col in cell_data
            }
        else:
            global_data = global_lookup.get(kyori_band_val, {})
            fill_values = {
                col: global_data[col]
                for col in impute_cols_present
                if col in global_data
            }

        for col, val in fill_values.items():
            if row[col] is None:
                row[col] = val

        row[RS_IMPUTED_COL] = 1

    return pl.DataFrame(rows, schema=out.schema)


def impute_rs_features_batch(
    df: pl.DataFrame,
    prior_df: pl.DataFrame,
    global_prior_df: pl.DataFrame,
) -> pl.DataFrame:
    """Vectorised batch version of ``impute_rs_features``.

    Produces identical output for the same inputs.  Preferred for large
    DataFrames (avoids per-row Python overhead).

    Algorithm:
      For imputation candidates (all trigger columns NULL):
        - Join cell prior on (keibajo_code, kyori_band); use cell when n>=20.
        - For rows where cell is absent or sparse, join global prior on kyori_band.
        - Fill NULL impute columns from the resolved prior values.
      rs_imputed = 1 for all candidates.
    """
    out = df.with_columns(pl.lit(0).alias(RS_IMPUTED_COL))

    null_mask = rs_null_mask(out)
    if not bool(null_mask.any()):
        return out

    impute_cols_present = [c for c in RS_IMPUTE_COLS if c in out.columns]

    # Tag the original positions so order survives the split/concat round-trip.
    out = out.with_columns(pl.int_range(0, pl.len()).alias("_orig_idx"))
    candidates = out.filter(null_mask)
    non_candidates = out.filter(~null_mask)

    # ── cell-level fill ──────────────────────────────────────────────────────
    cell_cols = [c for c in impute_cols_present if c in prior_df.columns]
    has_n = "n" in prior_df.columns
    cell_select = list(PRIOR_GROUP_COLS) + cell_cols
    cell_renamed = (
        prior_df.select(cell_select + (["n"] if has_n else []))
        .rename({c: f"_cell_{c}" for c in cell_cols})
        .rename({"n": "_cell_n"} if has_n else {})
    )
    if not has_n:
        cell_renamed = cell_renamed.with_columns(pl.lit(0).alias("_cell_n"))
    candidates = candidates.join(
        cell_renamed, on=list(PRIOR_GROUP_COLS), how="left"
    )
    has_cell = pl.col("_cell_n").fill_null(0) >= MIN_CELL_COUNT

    for col in cell_cols:
        cell_col = f"_cell_{col}"
        candidates = candidates.with_columns(
            pl.when(
                has_cell
                & pl.col(col).is_null()
                & pl.col(cell_col).is_not_null()
            )
            .then(pl.col(cell_col))
            .otherwise(pl.col(col))
            .alias(col)
        )

    # ── global fallback for rows still NULL after cell fill ──────────────────
    global_cols = [c for c in impute_cols_present if c in global_prior_df.columns]
    global_renamed = global_prior_df.select(["kyori_band"] + global_cols).rename(
        {c: f"_global_{c}" for c in global_cols}
    )
    candidates = candidates.join(global_renamed, on="kyori_band", how="left")

    for col in global_cols:
        g_col = f"_global_{col}"
        candidates = candidates.with_columns(
            pl.when(pl.col(col).is_null() & pl.col(g_col).is_not_null())
            .then(pl.col(g_col))
            .otherwise(pl.col(col))
            .alias(col)
        )

    # Drop helper columns + set flag.
    drop_cols = [
        c
        for c in candidates.columns
        if c.startswith("_cell_") or c.startswith("_global_")
    ]
    candidates = candidates.drop(drop_cols).with_columns(
        pl.lit(1).alias(RS_IMPUTED_COL)
    )

    rebuilt = (
        pl.concat([candidates, non_candidates], how="vertical_relaxed")
        .sort("_orig_idx")
        .drop("_orig_idx")
    )
    return rebuilt.select(out.drop("_orig_idx").columns)
