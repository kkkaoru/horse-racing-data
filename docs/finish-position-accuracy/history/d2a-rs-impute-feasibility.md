# D2a — H-RS-KEIBAJO-IMPUTE Feasibility Probe

**Date**: 2026-06-11
**Probe**: H-RS-KEIBAJO-IMPUTE (recommended in d2a-nar-venue-gap-diagnosis.md, commit eb249b9)
**Model**: NAR iter12 recipe (XGBoost rank:pairwise, same HPO params)
**Holdout**: 2023–2025 (3 WF folds, 40,710 races)
**Target venues**: 43 (Funabashi) + 44 (Kawasaki) = 5,612 holdout races (13.8% of NAR)
**Feature parquet**: `apps/pc-keiba-viewer/tmp/feat-nar-v7-final`
**Output JSON**: `tmp/rootcause/d2a_rs_impute.json`
**Impute module**: `apps/pc-keiba-viewer/src/scripts/finish-position-features/rs_keibajo_impute.py`
**Tests**: `apps/pc-keiba-viewer/tests/test_rs_keibajo_impute.py` (57 tests, cov≥95%)
**Verdict**: **ABORT**

---

## Impute Design

### Trigger

A horse is a candidate for imputation when ALL four RS trigger columns are simultaneously NULL:

- `past_nige_rate_self`
- `past_senkou_rate_self`
- `past_sashi_rate_self`
- `past_oikomi_rate_self`

These four columns go NULL together as a block when the running-style model v3 cannot score a horse (no corner-position history). This affects ~26% of 43/44 holdout rows vs ~9% at other NAR venues.

### Prior Table

Key: `(keibajo_code, kyori_band)` where `kyori_band` ∈ {0, 1, 2, 3} (distance band: short/mid/mid-long/long).

Built strictly from `race_date < cutoff_date` (the WF fold boundary). At cutoff 2023-01-01 there are **53 distinct cells** with mean non-NULL count per cell of ~1,600 rows. All 53 cells exceed the `MIN_CELL_COUNT=20` threshold; 1 cell (low-volume distance bucket at a sparse venue) sits near the boundary and triggers the global fallback.

Fallback: when cell n < 20, use the NAR-global prior for that `kyori_band` only.

### Columns Imputed (18 total)

All 4 RS rate columns + 8 corner norm statistics (`avg5/avg3/avg10/std5/best5/worst5/iqr5/prog5`) + `horse_keibajo_corner_1_norm_avg` + `last_race_corner_1_norm` + 4 RS win-rate columns.

### Leak-Free Guarantee

`build_prior_table(df, cutoff_date)` uses only rows where `race_date < cutoff_date` (string comparison on YYYYMMDD, strict less-than). For each WF fold year Y, the cutoff is `{Y}0101`. This is tested by `test_build_prior_table_excludes_cutoff_date_itself` and `test_build_prior_table_future_row_excluded`.

### Imputation Coverage (per fold)

| Fold | Prior cells | Train imputed rows | Holdout imputed rows |
| ---- | ----------- | ------------------ | -------------------- |
| 2023 | 53          | 154,003            | 16,202               |
| 2024 | 53          | 170,205            | 16,209               |
| 2025 | 53          | 186,414            | 16,476               |

Imputation fires on ~11.4% of holdout rows at 43/44 (not all NULL rows: some are at other venues with lower NULL rates).

---

## WITH vs WITHOUT Comparison (WF holdout 2023–2025)

### Per-fold results — Venues 43+44

| Fold | WITHOUT top1 | WITH top1 | Δ top1  | WITHOUT fukusho_2p | WITH fukusho_2p |
| ---- | ------------ | --------- | ------- | ------------------ | --------------- |
| 2023 | 0.4815       | 0.4777    | −0.38pp | 0.7625             | 0.7614          |
| 2024 | 0.4745       | 0.4745    | 0.00pp  | 0.7782             | 0.7761          |
| 2025 | 0.5254       | 0.5243    | −0.11pp | 0.7894             | 0.7878          |

### Aggregated (weighted by n_races) — Venues 43+44

| Metric     | WITHOUT | WITH    | Δ (pp) | Gate (pp) | Pass? |
| ---------- | ------- | ------- | ------ | --------- | ----- |
| top1       | 0.49376 | 0.49216 | −0.160 | ≥ +3.0    | FAIL  |
| top1 LB95  | 0.48069 | 0.47909 | —      | —         | —     |
| place2     | 0.28190 | 0.28207 | +0.017 | ≥ −0.05   | pass  |
| place3     | 0.20068 | 0.20692 | +0.624 | ≥ −0.05   | pass  |
| fukusho_2p | 0.77673 | 0.77512 | −0.161 | ≥ 0.0     | FAIL  |

### Aggregated — Global NAR

| Metric     | WITHOUT | WITH    | Δ (pp) | Gate (pp) | Pass? |
| ---------- | ------- | ------- | ------ | --------- | ----- |
| top1       | 0.58720 | 0.58696 | −0.024 | ≥ +0.3    | FAIL  |
| top1 LB95  | 0.58241 | 0.58217 | —      | —         | —     |
| place2     | 0.35630 | 0.35431 | −0.199 | —         | —     |
| place3     | 0.27564 | 0.27426 | −0.138 | —         | —     |
| fukusho_2p | 0.88195 | 0.88234 | +0.039 | —         | —     |

---

## Probe Gate

| Condition                               | Required  | Actual   | Result |
| --------------------------------------- | --------- | -------- | ------ |
| 43/44 top1 ≥ +3.0pp vs baseline         | ≥ +3.0pp  | −0.160pp | FAIL   |
| Global NAR top1 ≥ +0.3pp vs baseline    | ≥ +0.3pp  | −0.024pp | FAIL   |
| place2 no regression (≥ −0.05pp, 43/44) | ≥ −0.05pp | +0.017pp | pass   |
| place3 no regression (≥ −0.05pp, 43/44) | ≥ −0.05pp | +0.624pp | pass   |
| fukusho_2p no regression (≥ 0.0pp)      | ≥ 0.0pp   | −0.161pp | FAIL   |

**VERDICT: ABORT** — 3 of 5 gate conditions failed.

---

## Root Cause of ABORT

### XGBoost native NULL handling is competitive with mean imputation

XGBoost's `hist` booster learns a default split direction for each feature at each tree node when a value is missing. For the RS/corner features, the model has learned that NULL rows at 43/44 are likely "locally-anchored horses with unknown style" — and routes them to a specific internal branch that implicitly encodes the empirical distribution of outcomes for that sub-population.

Replacing NULL with the venue-class-distance mean collapses this learned NULL-routing signal to a single point estimate per (keibajo_code, kyori_band) cell. The mean imputation is informationally equivalent to what the model already does implicitly — but adds noise from the specific cell's sample variance and can shift some horses to wrong score positions relative to each other.

### Mean imputation cannot differentiate within the NULL sub-population

The ~16k imputed holdout rows at 43/44 are all locally-anchored horses. Within this group, imputing the same (keibajo, kyori_band) mean for all of them erases any individual variation. Two horses that both get NULL RS features may have very different underlying abilities; the model's NULL-routing branch can capture their relative ordering better via other non-NULL features (popularity_score, weight, jockey, field size, etc.) than by giving both the identical RS mean value.

### Place3 improves (+0.624pp) while top1 regresses (−0.160pp)

The asymmetric movement suggests imputation shifts the score ordering inside races — some horses that were ranked 1st without imputation fall to 3rd with imputation. This is consistent with the NULL-routing hypothesis: the model's native NULL branch assigned a moderate-to-high score to some locally-anchored frontrunners; mean imputation pulls their style features to "average", reducing their relative score and bumping them down one or two positions.

---

## Alternative Approaches Considered (not yet tested)

The probe gate requires ≥+3pp at 43/44 top1. With XGBoost's native NULL handling already capturing most of the available signal from non-RS features, reaching +3pp will require a different category of signal, not an imputation strategy.

| Approach                                   | Status                 | Notes                                                                   |
| ------------------------------------------ | ---------------------- | ----------------------------------------------------------------------- |
| (keibajo, kyori_band) mean imputation      | **ABORT** ← this probe | XGBoost native NULL already competitive                                 |
| RS model re-training on corner-less horses | NOT TESTED             | Would extend RS model to produce non-NULL estimates for local horses    |
| Additional venue-specific features         | NOT TESTED             | Feature engineering around local-horse concentration, cross-venue ratio |
| Per-horse locality score as new feature    | NOT TESTED             | `pct_career_at_this_venue` as explicit feature; not tied to imputation  |

The most promising next direction is to add an explicit **horse locality feature** (`pct_career_at_keibajo` or `n_career_races_keibajo`) as a new feature column, allowing the model to directly condition on how locally-anchored a horse is without destroying the NULL-routing signal.

---

## Implementation Notes

The impute module (`rs_keibajo_impute.py`) and its tests (`test_rs_keibajo_impute.py`) are committed as a reusable data-quality layer. Even though the probe ABORTs the specific fix path, the module is correct and may be useful for:

- Serving-side monitoring: detecting when a horse is locally-anchored (the `rs_imputed` flag)
- Feature augmentation: `rs_imputed` as a binary feature input to future models
- Diagnostic reporting: auditing NULL coverage over time per venue

The module passes `uv run pytest` with cov≥95% (98.31% on `rs_keibajo_impute.py`) and passes all ruff/ty/basedpyright checks.
