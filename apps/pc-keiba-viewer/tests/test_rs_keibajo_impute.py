# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportArgumentType=false, reportCallIssue=false, reportIndexIssue=false, reportOperatorIssue=false, reportAttributeAccessIssue=false, reportGeneralTypeIssues=false
"""Tests for rs_keibajo_impute — NOT DRY, every case self-contained.

Coverage targets: 100% statements, 100% branches.
All tests use fixed literals, no string concatenation in expect values.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from rs_keibajo_impute import (
    MIN_CELL_COUNT,
    PRIOR_GROUP_COLS,
    RS_IMPUTE_COLS,
    RS_IMPUTED_COL,
    RS_TRIGGER_COLS,
    build_prior_table,
    impute_rs_features,
    impute_rs_features_batch,
    rs_null_mask,
)


# ─────────────────────────────── helpers ────────────────────────────────────

def _fv(df: pd.DataFrame, key: object, col: str) -> float:
    """Extract a scalar float from a DataFrame index lookup via pd.to_numeric.

    Avoids the pandas .at[] wide-union type that ty cannot narrow to float.
    """
    return float(pd.to_numeric(df.at[key, col], errors="coerce"))


def _iv(df: pd.DataFrame, key: object, col: str) -> int:
    """Extract a scalar int from a DataFrame index lookup via pd.to_numeric."""
    return int(pd.to_numeric(df.at[key, col], errors="coerce"))


# ─────────────────────────────── fixtures ───────────────────────────────────

def _make_row(
    race_date: str = "20220101",
    keibajo_code: str = "44",
    kyori_band: int = 0,
    past_nige: float | None = None,
    past_senkou: float | None = None,
    past_sashi: float | None = None,
    past_oikomi: float | None = None,
    past_c1n5: float | None = None,
    past_c1n3: float | None = None,
    past_c1n10: float | None = None,
    past_c1n_std5: float | None = None,
    past_c1n_best5: float | None = None,
    past_c1n_worst5: float | None = None,
    past_c1n_iqr5: float | None = None,
    past_c_prog5: float | None = None,
    hkc1n: float | None = None,
    last_c1n: float | None = None,
    past_nige_win: float | None = None,
    past_senkou_win: float | None = None,
    past_sashi_win: float | None = None,
    past_oikomi_win: float | None = None,
) -> dict[str, object]:
    return {
        "race_date": race_date,
        "keibajo_code": keibajo_code,
        "kyori_band": kyori_band,
        "past_nige_rate_self": past_nige,
        "past_senkou_rate_self": past_senkou,
        "past_sashi_rate_self": past_sashi,
        "past_oikomi_rate_self": past_oikomi,
        "past_corner_1_norm_avg_5": past_c1n5,
        "past_corner_1_norm_avg_3": past_c1n3,
        "past_corner_1_norm_avg_10": past_c1n10,
        "past_corner_1_norm_std_5": past_c1n_std5,
        "past_corner_1_norm_best_5": past_c1n_best5,
        "past_corner_1_norm_worst_5": past_c1n_worst5,
        "past_corner_1_norm_iqr_5": past_c1n_iqr5,
        "past_corner_progression_avg_5": past_c_prog5,
        "horse_keibajo_corner_1_norm_avg": hkc1n,
        "last_race_corner_1_norm": last_c1n,
        "past_nige_win_rate_self": past_nige_win,
        "past_senkou_win_rate_self": past_senkou_win,
        "past_sashi_win_rate_self": past_sashi_win,
        "past_oikomi_win_rate_self": past_oikomi_win,
        "finish_position": 1,
    }


def _make_populated_row(
    race_date: str = "20220101",
    keibajo_code: str = "50",
    kyori_band: int = 1,
) -> dict[str, object]:
    """Row with all RS features populated (non-NULL)."""
    return _make_row(
        race_date=race_date,
        keibajo_code=keibajo_code,
        kyori_band=kyori_band,
        past_nige=0.15,
        past_senkou=0.30,
        past_sashi=0.35,
        past_oikomi=0.20,
        past_c1n5=0.48,
        past_c1n3=0.49,
        past_c1n10=0.47,
        past_c1n_std5=0.19,
        past_c1n_best5=0.28,
        past_c1n_worst5=0.68,
        past_c1n_iqr5=0.22,
        past_c_prog5=0.05,
        hkc1n=0.46,
        last_c1n=0.50,
        past_nige_win=0.08,
        past_senkou_win=0.12,
        past_sashi_win=0.11,
        past_oikomi_win=0.09,
    )


def _build_prior_rows(
    n: int = MIN_CELL_COUNT + 5,
    keibajo_code: str = "44",
    kyori_band: int = 0,
    nige: float = 0.16,
    senkou: float = 0.32,
    sashi: float = 0.32,
    oikomi: float = 0.20,
) -> list[dict[str, object]]:
    """Build ``n`` populated rows for prior table construction."""
    return [
        _make_populated_row(
            race_date="20210601",
            keibajo_code=keibajo_code,
            kyori_band=kyori_band,
        )
        | {
            "past_nige_rate_self": nige,
            "past_senkou_rate_self": senkou,
            "past_sashi_rate_self": sashi,
            "past_oikomi_rate_self": oikomi,
        }
        for _ in range(n)
    ]


# ──────────────────────────── constant tests ────────────────────────────────


def test_min_cell_count_is_20() -> None:
    assert MIN_CELL_COUNT == 20


def test_rs_trigger_cols_contains_four_rate_cols() -> None:
    assert "past_nige_rate_self" in RS_TRIGGER_COLS
    assert "past_senkou_rate_self" in RS_TRIGGER_COLS
    assert "past_sashi_rate_self" in RS_TRIGGER_COLS
    assert "past_oikomi_rate_self" in RS_TRIGGER_COLS
    assert len(RS_TRIGGER_COLS) == 4


def test_rs_impute_cols_includes_all_trigger_cols() -> None:
    for col in RS_TRIGGER_COLS:
        assert col in RS_IMPUTE_COLS


def test_rs_impute_cols_includes_corner_averages() -> None:
    assert "past_corner_1_norm_avg_5" in RS_IMPUTE_COLS
    assert "past_corner_1_norm_avg_3" in RS_IMPUTE_COLS
    assert "past_corner_1_norm_avg_10" in RS_IMPUTE_COLS


def test_rs_imputed_col_name() -> None:
    assert RS_IMPUTED_COL == "rs_imputed"


def test_prior_group_cols_are_keibajo_and_kyori_band() -> None:
    assert PRIOR_GROUP_COLS == ("keibajo_code", "kyori_band")


# ──────────────────────────── rs_null_mask ─────────────────────────────────


def testrs_null_mask_true_when_all_four_null() -> None:
    df = pd.DataFrame([_make_row(past_nige=None, past_senkou=None, past_sashi=None, past_oikomi=None)])
    mask = rs_null_mask(df)
    assert bool(mask.iloc[0]) is True
    assert mask.sum() == 1


def testrs_null_mask_false_when_one_col_populated() -> None:
    df = pd.DataFrame([_make_row(past_nige=0.1, past_senkou=None, past_sashi=None, past_oikomi=None)])
    mask = rs_null_mask(df)
    assert bool(mask.iloc[0]) is False


def testrs_null_mask_false_when_all_populated() -> None:
    df = pd.DataFrame([_make_populated_row()])
    mask = rs_null_mask(df)
    assert mask.sum() == 0


def testrs_null_mask_returns_false_when_no_trigger_cols_present() -> None:
    df = pd.DataFrame([{"race_date": "20220101", "keibajo_code": "44", "kyori_band": 0}])
    mask = rs_null_mask(df)
    assert mask.sum() == 0


def testrs_null_mask_handles_empty_dataframe() -> None:
    df = pd.DataFrame(columns=["past_nige_rate_self", "past_senkou_rate_self",
                                "past_sashi_rate_self", "past_oikomi_rate_self"])
    mask = rs_null_mask(df)
    assert len(mask) == 0


# ───────────────────────── build_prior_table ────────────────────────────────


def test_build_prior_table_returns_two_dataframes() -> None:
    rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    df = pd.DataFrame(rows)
    cell_prior, global_prior = build_prior_table(df, "20230101")
    assert isinstance(cell_prior, pd.DataFrame)
    assert isinstance(global_prior, pd.DataFrame)


def test_build_prior_table_excludes_cutoff_date_itself() -> None:
    rows_before = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    # Row on exactly the cutoff date must be excluded
    on_cutoff = _make_populated_row(race_date="20230101", keibajo_code="44", kyori_band=0)
    on_cutoff["past_nige_rate_self"] = 0.99  # distinctive value
    all_rows = rows_before + [on_cutoff]
    df = pd.DataFrame(all_rows)
    cell_prior, _ = build_prior_table(df, "20230101")
    # The mean should NOT be polluted by 0.99 because that row is excluded
    mean_nige = _fv(cell_prior, ("44", 0), "past_nige_rate_self")
    assert abs(mean_nige - 0.16) < 1e-6


def test_build_prior_table_excludes_all_data_after_cutoff() -> None:
    future_rows = [
        _make_populated_row(race_date="20240601", keibajo_code="44", kyori_band=0)
        for _ in range(30)
    ]
    df = pd.DataFrame(future_rows)
    cell_prior, global_prior = build_prior_table(df, "20230101")
    assert len(cell_prior) == 0
    assert len(global_prior) == 0


def test_build_prior_table_cell_has_n_column() -> None:
    rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    df = pd.DataFrame(rows)
    cell_prior, _ = build_prior_table(df, "20230101")
    assert "n" in cell_prior.columns


def test_build_prior_table_cell_n_equals_non_null_count() -> None:
    rows_populated = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    # Add 3 rows with NULL nige
    null_rows = [
        _make_row(
            race_date="20210601",
            keibajo_code="44",
            kyori_band=0,
        )
        for _ in range(3)
    ]
    df = pd.DataFrame(rows_populated + null_rows)
    cell_prior, _ = build_prior_table(df, "20230101")
    n = _iv(cell_prior, ("44", 0), "n")
    assert n == 25


def test_build_prior_table_cell_n_zero_when_all_null() -> None:
    null_rows = [
        _make_row(
            race_date="20210601",
            keibajo_code="44",
            kyori_band=0,
        )
        for _ in range(10)
    ]
    df = pd.DataFrame(null_rows)
    cell_prior, _ = build_prior_table(df, "20230101")
    if ("44", 0) in cell_prior.index:
        n = _iv(cell_prior, ("44", 0), "n")
        assert n == 0


def test_build_prior_table_global_keyed_by_kyori_band() -> None:
    rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    df = pd.DataFrame(rows)
    _, global_prior = build_prior_table(df, "20230101")
    assert global_prior.index.name == "kyori_band"


def test_build_prior_table_global_nige_mean_matches_input() -> None:
    rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.16)
    df = pd.DataFrame(rows)
    _, global_prior = build_prior_table(df, "20230101")
    mean_nige = _fv(global_prior, 0, "past_nige_rate_self")
    assert abs(mean_nige - 0.16) < 1e-6


def test_build_prior_table_cell_mean_matches_input() -> None:
    rows = _build_prior_rows(n=25, keibajo_code="43", kyori_band=1, nige=0.09, senkou=0.26)
    df = pd.DataFrame(rows)
    cell_prior, _ = build_prior_table(df, "20230101")
    assert ("43", 1) in cell_prior.index
    mean_nige = _fv(cell_prior, ("43", 1), "past_nige_rate_self")
    mean_senkou = _fv(cell_prior, ("43", 1), "past_senkou_rate_self")
    assert abs(mean_nige - 0.09) < 1e-6
    assert abs(mean_senkou - 0.26) < 1e-6


def test_build_prior_table_multiple_venues_separate_cells() -> None:
    rows_43 = _build_prior_rows(n=25, keibajo_code="43", kyori_band=0, nige=0.10)
    rows_44 = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.20)
    df = pd.DataFrame(rows_43 + rows_44)
    cell_prior, _ = build_prior_table(df, "20230101")
    assert ("43", 0) in cell_prior.index
    assert ("44", 0) in cell_prior.index
    assert abs(_fv(cell_prior, ("43", 0), "past_nige_rate_self") - 0.10) < 1e-6
    assert abs(_fv(cell_prior, ("44", 0), "past_nige_rate_self") - 0.20) < 1e-6


# ─────────────────────── impute_rs_features (row-by-row) ─────────────────────


def test_impute_rs_features_non_null_row_unchanged() -> None:
    populated = _make_populated_row(keibajo_code="50", kyori_band=1)
    df = pd.DataFrame([populated])
    prior_rows = _build_prior_rows(n=25, keibajo_code="50", kyori_band=1)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    # Original values must be unchanged
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.15) < 1e-6
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 0


def test_impute_rs_features_null_row_imputed_from_cell_prior() -> None:
    target_row = _make_row(
        race_date="20230601",
        keibajo_code="44",
        kyori_band=0,
    )
    df = pd.DataFrame([target_row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.155)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.155) < 1e-6
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_impute_rs_features_null_row_flag_set_to_1() -> None:
    target_row = _make_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([target_row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_impute_rs_features_sparse_cell_falls_back_to_global() -> None:
    # Cell (44,1) has only 5 non-NULL rows — below MIN_CELL_COUNT=20.
    # Global prior for kyori_band=1 blends both venues.
    # Expected global mean = (5 * 0.30 + 50 * 0.14) / 55 ≈ 0.15454...
    # This is distinctly different from 0.30 (sparse cell value).
    target_row = _make_row(keibajo_code="44", kyori_band=1)
    df = pd.DataFrame([target_row])
    sparse_rows = _build_prior_rows(n=5, keibajo_code="44", kyori_band=1, nige=0.30)
    global_rows = _build_prior_rows(n=50, keibajo_code="50", kyori_band=1, nige=0.14)
    prior_df_raw = pd.DataFrame(sparse_rows + global_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    # Must NOT use sparse cell value 0.30 directly
    nige_val = float(result.iloc[0]["past_nige_rate_self"])
    expected_global = (5 * 0.30 + 50 * 0.14) / 55.0
    assert abs(nige_val - expected_global) < 1e-5
    assert abs(nige_val - 0.30) > 0.01  # not the sparse cell value
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_impute_rs_features_missing_cell_falls_back_to_global() -> None:
    # No prior data for venue 99 at all → cell absent → fallback to global
    target_row = _make_row(keibajo_code="99", kyori_band=0)
    df = pd.DataFrame([target_row])
    global_rows = _build_prior_rows(n=50, keibajo_code="50", kyori_band=0, nige=0.12)
    prior_df_raw = pd.DataFrame(global_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    nige_val = float(result.iloc[0]["past_nige_rate_self"])
    assert abs(nige_val - 0.12) < 1e-5
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_impute_rs_features_no_null_rows_returns_unchanged() -> None:
    rows = [_make_populated_row(keibajo_code="50", kyori_band=1) for _ in range(3)]
    df = pd.DataFrame(rows)
    prior_df_raw = pd.DataFrame(_build_prior_rows(n=25))
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    assert (result[RS_IMPUTED_COL] == 0).all()
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.15) < 1e-6


def test_impute_rs_features_rs_imputed_col_zero_for_non_null_rows() -> None:
    populated = _make_populated_row()
    df = pd.DataFrame([populated])
    prior_df_raw = pd.DataFrame(_build_prior_rows(n=25))
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 0


def test_impute_rs_features_does_not_overwrite_non_null_impute_cols() -> None:
    # Row where trigger cols are NULL but some other impute cols are NOT NULL
    row = _make_row(
        keibajo_code="44",
        kyori_band=0,
        past_nige=None,
        past_senkou=None,
        past_sashi=None,
        past_oikomi=None,
        last_c1n=0.77,  # this is non-null already
    )
    df = pd.DataFrame([row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    # Set a different last_c1n in prior
    for r in prior_rows:
        r["last_race_corner_1_norm"] = 0.50
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    # last_race_corner_1_norm was non-null → must not be overwritten
    assert abs(float(result.iloc[0]["last_race_corner_1_norm"]) - 0.77) < 1e-6


def test_impute_rs_features_empty_dataframe_returns_empty() -> None:
    empty_df = pd.DataFrame(
        columns=["race_date", "keibajo_code", "kyori_band",
                 "past_nige_rate_self", "past_senkou_rate_self",
                 "past_sashi_rate_self", "past_oikomi_rate_self"]
    )
    prior_df_raw = pd.DataFrame(_build_prior_rows(n=25))
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(empty_df, cell_prior, global_prior)
    assert len(result) == 0


def test_impute_rs_features_mixed_null_non_null_rows() -> None:
    null_row = _make_row(keibajo_code="44", kyori_band=0)
    non_null_row = _make_populated_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([null_row, non_null_row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.155)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    # Row 0 imputed
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.155) < 1e-5
    # Row 1 unchanged
    assert int(result.iloc[1][RS_IMPUTED_COL]) == 0
    assert abs(float(result.iloc[1]["past_nige_rate_self"]) - 0.15) < 1e-6


def test_impute_rs_features_no_global_prior_for_kyori_band() -> None:
    # kyori_band=3 but no prior data at all for kyori_band 3 → all remain NULL
    target_row = _make_row(keibajo_code="99", kyori_band=3)
    df = pd.DataFrame([target_row])
    # Build priors only for kyori_band=0
    global_rows = _build_prior_rows(n=50, keibajo_code="50", kyori_band=0, nige=0.12)
    prior_df_raw = pd.DataFrame(global_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    # kyori_band=3 not in global_prior → impute fires but fills nothing (NULL stays)
    # rs_imputed is still 1 (imputation was attempted)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1
    assert pd.isna(result.iloc[0]["past_nige_rate_self"])


# ─────────────────── impute_rs_features_batch (vectorised) ──────────────────


def test_batch_non_null_rows_unchanged() -> None:
    rows = [_make_populated_row(keibajo_code="50", kyori_band=1) for _ in range(5)]
    df = pd.DataFrame(rows)
    prior_df_raw = pd.DataFrame(_build_prior_rows(n=25))
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    assert (result[RS_IMPUTED_COL] == 0).all()
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.15) < 1e-6


def test_batch_null_row_imputed_from_cell_prior() -> None:
    target_row = _make_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([target_row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.155)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.155) < 1e-5
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_batch_sparse_cell_falls_back_to_global() -> None:
    target_row = _make_row(keibajo_code="44", kyori_band=1)
    df = pd.DataFrame([target_row])
    sparse_rows = _build_prior_rows(n=5, keibajo_code="44", kyori_band=1, nige=0.30)
    global_rows = _build_prior_rows(n=50, keibajo_code="50", kyori_band=1, nige=0.14)
    prior_df_raw = pd.DataFrame(sparse_rows + global_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    nige_val = float(result.iloc[0]["past_nige_rate_self"])
    expected_global = (5 * 0.30 + 50 * 0.14) / 55.0
    assert abs(nige_val - expected_global) < 1e-5
    assert abs(nige_val - 0.30) > 0.01
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_batch_missing_cell_falls_back_to_global() -> None:
    target_row = _make_row(keibajo_code="99", kyori_band=0)
    df = pd.DataFrame([target_row])
    global_rows = _build_prior_rows(n=50, keibajo_code="50", kyori_band=0, nige=0.12)
    prior_df_raw = pd.DataFrame(global_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    nige_val = float(result.iloc[0]["past_nige_rate_self"])
    assert abs(nige_val - 0.12) < 1e-5
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_batch_empty_dataframe() -> None:
    empty_df = pd.DataFrame(
        columns=["race_date", "keibajo_code", "kyori_band",
                 "past_nige_rate_self", "past_senkou_rate_self",
                 "past_sashi_rate_self", "past_oikomi_rate_self"]
    )
    prior_df_raw = pd.DataFrame(_build_prior_rows(n=25))
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features_batch(empty_df, cell_prior, global_prior)
    assert len(result) == 0


def test_batch_mixed_null_non_null_rows() -> None:
    null_row = _make_row(keibajo_code="44", kyori_band=0)
    non_null_row = _make_populated_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([null_row, non_null_row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.155)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.155) < 1e-5
    assert int(result.iloc[1][RS_IMPUTED_COL]) == 0
    assert abs(float(result.iloc[1]["past_nige_rate_self"]) - 0.15) < 1e-6


def test_batch_multiple_null_rows_all_imputed() -> None:
    null_rows = [_make_row(keibajo_code="44", kyori_band=0) for _ in range(10)]
    df = pd.DataFrame(null_rows)
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.16)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    assert (result[RS_IMPUTED_COL] == 1).all()
    assert (result["past_nige_rate_self"].notna()).all()


def test_batch_and_rowwise_produce_identical_results() -> None:
    """Verify batch and row-by-row produce exactly the same output."""
    null_rows = [_make_row(keibajo_code="44", kyori_band=0) for _ in range(5)]
    populated_rows = [_make_populated_row(keibajo_code="50", kyori_band=1) for _ in range(5)]
    df = pd.DataFrame(null_rows + populated_rows)
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.155)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    row_result = impute_rs_features(df, cell_prior, global_prior)
    batch_result = impute_rs_features_batch(df, cell_prior, global_prior)
    common_cols = [c for c in row_result.columns if c in batch_result.columns]
    for col in common_cols:
        if pd.api.types.is_numeric_dtype(row_result[col]):
            rvals = row_result[col].to_numpy(dtype=float, na_value=float("nan"))
            bvals = batch_result[col].to_numpy(dtype=float, na_value=float("nan"))
            null_r = np.isnan(rvals)
            null_b = np.isnan(bvals)
            assert np.array_equal(null_r, null_b), f"NULL mismatch in {col}"
            assert np.allclose(
                rvals[~null_r],
                bvals[~null_b],
                atol=1e-10,
            ), f"Value mismatch in {col}"


# ──────────────────────── leak-free proof tests ──────────────────────────────


def test_build_prior_table_is_strictly_less_than_cutoff() -> None:
    """Future data strictly after cutoff must NOT influence the prior."""
    rows_past = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.16)
    # A row exactly on 20230101 must be excluded
    row_on_cutoff = _make_populated_row(race_date="20230101", keibajo_code="44", kyori_band=0)
    row_on_cutoff["past_nige_rate_self"] = 0.90  # type: ignore[assignment]
    df = pd.DataFrame(rows_past + [row_on_cutoff])
    cell_prior, _ = build_prior_table(df, "20230101")
    nige_mean = _fv(cell_prior, ("44", 0), "past_nige_rate_self")
    assert abs(nige_mean - 0.16) < 1e-6


def test_build_prior_table_future_row_excluded() -> None:
    """A row one day after cutoff must be excluded."""
    rows_past = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.16)
    row_future = _make_populated_row(race_date="20230102", keibajo_code="44", kyori_band=0)
    row_future["past_nige_rate_self"] = 0.90  # type: ignore[assignment]
    df = pd.DataFrame(rows_past + [row_future])
    cell_prior, _ = build_prior_table(df, "20230101")
    nige_mean = _fv(cell_prior, ("44", 0), "past_nige_rate_self")
    assert abs(nige_mean - 0.16) < 1e-6


def test_impute_does_not_use_target_race_label() -> None:
    """Impute must work when finish_position is NOT present in the DF."""
    row = {
        "race_date": "20230601",
        "keibajo_code": "44",
        "kyori_band": 0,
        "past_nige_rate_self": None,
        "past_senkou_rate_self": None,
        "past_sashi_rate_self": None,
        "past_oikomi_rate_self": None,
    }
    df = pd.DataFrame([row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    # Should not raise even without finish_position column
    result = impute_rs_features(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_batch_impute_does_not_use_target_race_label() -> None:
    row = {
        "race_date": "20230601",
        "keibajo_code": "44",
        "kyori_band": 0,
        "past_nige_rate_self": None,
        "past_senkou_rate_self": None,
        "past_sashi_rate_self": None,
        "past_oikomi_rate_self": None,
    }
    df = pd.DataFrame([row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


# ───────────────────── edge cases / robustness ───────────────────────────────


def test_impute_rs_features_rs_imputed_col_already_present() -> None:
    """If rs_imputed already exists in df, it should be overwritten."""
    row = _make_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([row])
    df[RS_IMPUTED_COL] = 99  # pre-existing junk value
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_batch_rs_imputed_col_already_present() -> None:
    row = _make_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([row])
    df[RS_IMPUTED_COL] = 99
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_impute_does_not_modify_input_dataframe() -> None:
    """Impute must return a copy, not modify the input in-place."""
    row = _make_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([row])
    original_nige = df.iloc[0]["past_nige_rate_self"]
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    _ = impute_rs_features(df, cell_prior, global_prior)
    # Input must not be modified
    assert pd.isna(df.iloc[0]["past_nige_rate_self"]) == pd.isna(original_nige)


def test_batch_does_not_modify_input_dataframe() -> None:
    row = _make_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    _ = impute_rs_features_batch(df, cell_prior, global_prior)
    assert pd.isna(df.iloc[0]["past_nige_rate_self"])


def test_impute_with_dataframe_missing_some_impute_cols() -> None:
    """If some RS_IMPUTE_COLS are absent from df, impute should not raise."""
    row = {
        "race_date": "20230601",
        "keibajo_code": "44",
        "kyori_band": 0,
        "past_nige_rate_self": None,
        "past_senkou_rate_self": None,
        "past_sashi_rate_self": None,
        "past_oikomi_rate_self": None,
        # past_corner_* columns intentionally omitted
    }
    df = pd.DataFrame([row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.16) < 1e-5


def test_batch_with_dataframe_missing_some_impute_cols() -> None:
    row = {
        "race_date": "20230601",
        "keibajo_code": "44",
        "kyori_band": 0,
        "past_nige_rate_self": None,
        "past_senkou_rate_self": None,
        "past_sashi_rate_self": None,
        "past_oikomi_rate_self": None,
    }
    df = pd.DataFrame([row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_build_prior_table_handles_wholly_null_rs_col_in_past_data() -> None:
    """Prior table must not raise if past data has NULL RS cols for some rows."""
    mixed = _build_prior_rows(n=20, keibajo_code="44", kyori_band=0)
    # Add rows with all NULL RS (locally-anchored horses, which is the whole point)
    null_rows = [_make_row(race_date="20210601", keibajo_code="44", kyori_band=0)
                 for _ in range(5)]
    df = pd.DataFrame(mixed + null_rows)
    cell_prior, _global_prior = build_prior_table(df, "20230101")
    assert ("44", 0) in cell_prior.index
    # n must count only non-NULL rows (20, not 25)
    assert _iv(cell_prior, ("44", 0), "n") == 20


def test_impute_exactly_min_cell_count_uses_cell_prior() -> None:
    """A cell with exactly MIN_CELL_COUNT non-NULL rows uses the cell prior."""
    target_row = _make_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([target_row])
    prior_rows = _build_prior_rows(n=MIN_CELL_COUNT, keibajo_code="44", kyori_band=0, nige=0.15)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    # n == MIN_CELL_COUNT → use cell prior
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.15) < 1e-5


def test_impute_skips_cell_col_when_prior_value_is_nan() -> None:
    """When a cell prior column is NaN (e.g. all rows were NULL for that col),
    the col stays NULL in the output rather than being set to NaN explicitly."""
    target_row = _make_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([target_row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.16)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    # Force a NaN in the cell prior for horse_keibajo_corner_1_norm_avg
    cell_prior.loc[("44", 0), "horse_keibajo_corner_1_norm_avg"] = float("nan")
    # Ensure global prior also NaN for that col
    if "horse_keibajo_corner_1_norm_avg" in global_prior.columns:
        global_prior.loc[:, "horse_keibajo_corner_1_norm_avg"] = float("nan")
    result = impute_rs_features(df, cell_prior, global_prior)
    # Should not raise; nige_rate still imputed
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.16) < 1e-5
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_impute_skips_global_col_when_global_prior_value_is_nan() -> None:
    """Fallback should not set a col to NaN; skip it when the global prior is NaN."""
    target_row = _make_row(keibajo_code="99", kyori_band=0)
    df = pd.DataFrame([target_row])
    global_rows = _build_prior_rows(n=50, keibajo_code="50", kyori_band=0, nige=0.13)
    prior_df_raw = pd.DataFrame(global_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    # Force NaN for horse_keibajo_corner_1_norm_avg in global prior
    if "horse_keibajo_corner_1_norm_avg" in global_prior.columns:
        global_prior.loc[:, "horse_keibajo_corner_1_norm_avg"] = float("nan")
    result = impute_rs_features(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1


def test_batch_cell_col_absent_from_candidates_skipped_gracefully() -> None:
    """Batch impute: when a cell_col is not present in candidates, skip gracefully."""
    target_row = _make_row(keibajo_code="44", kyori_band=0)
    df = pd.DataFrame([target_row])
    prior_rows = _build_prior_rows(n=25, keibajo_code="44", kyori_band=0, nige=0.155)
    prior_df_raw = pd.DataFrame(prior_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    # Remove a column from cell_prior so _cell_{col} won't appear after rename
    if "past_corner_1_norm_avg_5" in cell_prior.columns:
        cell_prior = cell_prior.drop(columns=["past_corner_1_norm_avg_5"])
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.155) < 1e-5


def test_batch_global_col_absent_from_candidates_skipped_gracefully() -> None:
    """Batch impute: when a g_col is not present in candidates, skip gracefully."""
    target_row = _make_row(keibajo_code="99", kyori_band=0)
    df = pd.DataFrame([target_row])
    global_rows = _build_prior_rows(n=50, keibajo_code="50", kyori_band=0, nige=0.12)
    prior_df_raw = pd.DataFrame(global_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    if "past_corner_1_norm_avg_5" in global_prior.columns:
        global_prior = global_prior.drop(columns=["past_corner_1_norm_avg_5"])
    result = impute_rs_features_batch(df, cell_prior, global_prior)
    assert int(result.iloc[0][RS_IMPUTED_COL]) == 1
    assert abs(float(result.iloc[0]["past_nige_rate_self"]) - 0.12) < 1e-5


def test_impute_exactly_min_cell_count_minus_one_uses_global() -> None:
    """A cell with MIN_CELL_COUNT - 1 rows must fall back to global.

    Expected global mean for kyori_band=2:
      ((MIN_CELL_COUNT-1) * 0.30 + 50 * 0.13) / (MIN_CELL_COUNT-1 + 50)
    The key assertion: nige_val must NOT be 0.30 (the sparse cell value).
    """
    target_row = _make_row(keibajo_code="44", kyori_band=2)
    df = pd.DataFrame([target_row])
    n_sparse = MIN_CELL_COUNT - 1
    sparse_rows = _build_prior_rows(n=n_sparse, keibajo_code="44", kyori_band=2, nige=0.30)
    global_rows = _build_prior_rows(n=50, keibajo_code="50", kyori_band=2, nige=0.13)
    prior_df_raw = pd.DataFrame(sparse_rows + global_rows)
    cell_prior, global_prior = build_prior_table(prior_df_raw, "20230101")
    result = impute_rs_features(df, cell_prior, global_prior)
    nige_val = float(result.iloc[0]["past_nige_rate_self"])
    expected_global = (n_sparse * 0.30 + 50 * 0.13) / float(n_sparse + 50)
    assert abs(nige_val - expected_global) < 1e-5
    # Must NOT be 0.30 (the sparse cell value)
    assert abs(nige_val - 0.30) > 0.01
