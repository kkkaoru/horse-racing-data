#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportArgumentType=false, reportCallIssue=false, reportIndexIssue=false, reportOperatorIssue=false, reportAttributeAccessIssue=false, reportGeneralTypeIssues=false
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
"""
from __future__ import annotations

from typing import Final

import numpy as np
import pandas as pd

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


def rs_null_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask: True where ALL RS trigger columns are NULL."""
    trigger_cols_present = [c for c in RS_TRIGGER_COLS if c in df.columns]
    if not trigger_cols_present:
        return pd.Series(False, index=df.index)
    mask = pd.Series(True, index=df.index)
    for col in trigger_cols_present:
        mask = mask & df[col].isna()
    return mask


def build_prior_table(
    df: pd.DataFrame,
    cutoff_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
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
    prior_df:
        DataFrame with index (keibajo_code, kyori_band) and one column per
        imputed feature, containing the mean over non-NULL rows + a ``n``
        column (non-NULL count for the trigger column ``past_nige_rate_self``).
    global_prior_df:
        DataFrame with index (kyori_band,) and the same impute-column means
        over all NAR races before cutoff — the fallback when a cell is sparse.
    """
    past = df[df["race_date"] < cutoff_date].copy()

    impute_cols_present = [c for c in RS_IMPUTE_COLS if c in past.columns]

    cell_prior = (
        past.groupby(list(PRIOR_GROUP_COLS))[impute_cols_present]
        .agg("mean")
        .reset_index()
    )
    cell_counts = (
        past[past["past_nige_rate_self"].notna()]
        .groupby(list(PRIOR_GROUP_COLS))
        .size()
        .reset_index(name="n")
    )
    cell_prior = cell_prior.merge(cell_counts, on=list(PRIOR_GROUP_COLS), how="left")
    cell_prior["n"] = cell_prior["n"].fillna(0).astype(int)
    cell_prior = cell_prior.set_index(list(PRIOR_GROUP_COLS))

    global_prior = (
        past.groupby("kyori_band")[impute_cols_present]
        .agg("mean")
        .reset_index()
        .set_index("kyori_band")
    )

    return cell_prior, global_prior


def impute_rs_features(
    df: pd.DataFrame,
    prior_df: pd.DataFrame,
    global_prior_df: pd.DataFrame,
) -> pd.DataFrame:
    """Apply leak-free RS imputation in-place (returns a copy).

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
        Output of ``build_prior_table`` — index (keibajo_code, kyori_band).
    global_prior_df:
        Output of ``build_prior_table`` — index (kyori_band,).

    Returns
    -------
    DataFrame with RS columns filled and ``rs_imputed`` column added.
    """
    out = df.copy()
    out[RS_IMPUTED_COL] = 0

    null_mask = rs_null_mask(out)
    if not null_mask.any():
        return out

    impute_cols_present = [c for c in RS_IMPUTE_COLS if c in out.columns]
    null_idx = out.index[null_mask]

    # Convert priors to Python dicts once to avoid pandas .at[] wide-union issues.
    # Use pd.to_numeric to guarantee float scalars from any pandas Scalar type.
    # cell_lookup: (keibajo_code, kyori_band) → {"n": float, col: float, ...}
    cell_lookup: dict[tuple[str, int], dict[str, float]] = {}
    cell_cols_present = [c for c in impute_cols_present if c in prior_df.columns]
    # Build numpy arrays for each column in the cell prior (avoids per-row .at[])
    cell_index_list = list(prior_df.index)
    cell_n_arr = pd.to_numeric(
        prior_df["n"] if "n" in prior_df.columns else pd.Series(0, index=prior_df.index),
        errors="coerce",
    ).to_numpy(dtype=float, na_value=0.0)
    cell_col_arrs: dict[str, np.ndarray] = {
        col: pd.to_numeric(prior_df[col], errors="coerce").to_numpy(dtype=float, na_value=float("nan"))
        for col in cell_cols_present
    }
    for i, cell_key in enumerate(cell_index_list):
        k = (str(cell_key[0]), int(cell_key[1]))
        row_dict: dict[str, float] = {"n": float(cell_n_arr[i])}
        for col in cell_cols_present:
            v = cell_col_arrs[col][i]
            if not np.isnan(v):
                row_dict[col] = float(v)
        cell_lookup[k] = row_dict

    # global_lookup: kyori_band → {col: float, ...}
    global_lookup: dict[int, dict[str, float]] = {}
    global_cols_present = [c for c in impute_cols_present if c in global_prior_df.columns]
    global_index_list = list(global_prior_df.index)
    global_col_arrs: dict[str, np.ndarray] = {
        col: pd.to_numeric(global_prior_df[col], errors="coerce").to_numpy(
            dtype=float, na_value=float("nan")
        )
        for col in global_cols_present
    }
    for i, kb in enumerate(global_index_list):
        kb_int = int(kb)
        gd: dict[str, float] = {}
        for col in global_cols_present:
            v = global_col_arrs[col][i]
            if not np.isnan(v):
                gd[col] = float(v)
        global_lookup[kb_int] = gd

    for idx in null_idx:
        row = out.loc[idx]
        keibajo = str(row["keibajo_code"])
        kyori_band_val = int(row["kyori_band"])
        key = (keibajo, kyori_band_val)

        fill_values: dict[str, float] = {}
        cell_data = cell_lookup.get(key)
        if cell_data is not None and cell_data.get("n", 0.0) >= MIN_CELL_COUNT:
            for col in impute_cols_present:
                if col in cell_data:
                    fill_values[col] = cell_data[col]
        else:
            global_data = global_lookup.get(kyori_band_val)
            if global_data is not None:
                for col in impute_cols_present:
                    if col in global_data:
                        fill_values[col] = global_data[col]

        for col, val in fill_values.items():
            if pd.isna(out.at[idx, col]):
                out.at[idx, col] = val

        out.at[idx, RS_IMPUTED_COL] = 1

    return out


def impute_rs_features_batch(
    df: pd.DataFrame,
    prior_df: pd.DataFrame,
    global_prior_df: pd.DataFrame,
) -> pd.DataFrame:
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
    out = df.copy()
    out[RS_IMPUTED_COL] = 0

    null_mask = rs_null_mask(out)
    if not null_mask.any():
        return out

    impute_cols_present = [c for c in RS_IMPUTE_COLS if c in out.columns]
    candidates = out[null_mask].copy()

    # ── cell-level fill ──────────────────────────────────────────────────────
    cell_cols = [c for c in impute_cols_present if c in prior_df.columns]
    cell_prior_reset = (
        prior_df[cell_cols + ["n"]].reset_index()
        if "n" in prior_df.columns
        else prior_df[cell_cols].assign(n=0).reset_index()
    )
    candidates = candidates.merge(
        cell_prior_reset.rename(columns={c: f"_cell_{c}" for c in cell_cols})
        .assign(_cell_n=cell_prior_reset["n"]),
        on=["keibajo_code", "kyori_band"],
        how="left",
    )
    has_cell = candidates["_cell_n"].fillna(0) >= MIN_CELL_COUNT

    for col in cell_cols:
        cell_col = f"_cell_{col}"
        if cell_col in candidates.columns:
            candidates[col] = np.where(
                has_cell & candidates[col].isna() & candidates[cell_col].notna(),
                candidates[cell_col],
                candidates[col],
            )

    # ── global fallback for rows still NULL after cell fill ──────────────────
    global_cols = [c for c in impute_cols_present if c in global_prior_df.columns]
    global_reset = global_prior_df[global_cols].reset_index()
    candidates = candidates.merge(
        global_reset.rename(columns={c: f"_global_{c}" for c in global_cols}),
        on="kyori_band",
        how="left",
    )

    for col in global_cols:
        g_col = f"_global_{col}"
        if g_col in candidates.columns:
            candidates[col] = np.where(
                candidates[col].isna() & candidates[g_col].notna(),
                candidates[g_col],
                candidates[col],
            )

    # Drop helper columns
    drop_cols = [c for c in candidates.columns if c.startswith("_cell_") or c.startswith("_global_")]
    candidates = candidates.drop(columns=drop_cols)

    # Write imputed values back + set flag
    out.loc[null_mask, impute_cols_present] = candidates[impute_cols_present].values
    out.loc[null_mask, RS_IMPUTED_COL] = 1

    return out
