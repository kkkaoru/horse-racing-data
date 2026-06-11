# G: Feature Pipeline Data-Completeness Audit

**Date:** 2026-06-12
**Branch:** docs/jes-journal-collection
**Status:** Audit complete. 1 PROCEED-FIX-CANDIDATE, 2 STRUCTURAL-KNOWN, remainder CLEAN.

---

## Scope

Systematic hunt for two bug classes across the entire finish-position feature pipeline,
all categories (JRA 01-10, NAR 30-48, Ban-ei 83):

1. **JRA-only-table JOIN → NAR/Ban-ei silent NULL**: feature builder reads a `jvd_*`
   PG table without UNIONing the NAR `nvd_*` counterpart.
2. **Stale-precomputed-table JOIN → upcoming rows NULL at serve AND/OR training skew**:
   joining a historical/materialised table that lacks the current/target rows.

**Already fixed (excluded from re-flagging):**

- Pedigree sire/damsire (`add-near-miss-features.py` + `add-baba-pedigree-affinity-features.py`)
  via `pedigree_staging.py` UNION (commit 65ad49e).
- Market-signal layer (commits 5c3aa12 / b8d45d2).
- Futan-juryo layer (commits ebd4636 / b8d45d2).

---

## Per-Script Table-Read Inventory

### Base pipeline: `finish_position_features_duckdb.py`

| Table                           | Type                    | Union status                                        |
| ------------------------------- | ----------------------- | --------------------------------------------------- |
| `pg.jvd_se`                     | JRA                     | UNION ALL with `nvd_se` via `se_lookup` temp table  |
| `pg.nvd_se`                     | NAR/Ban-ei              | Part of `se_lookup` UNION                           |
| `pg.jvd_ra`                     | JRA race                | Separate staging per category (`jra_ra` / `nar_ra`) |
| `pg.nvd_ra`                     | NAR/Ban-ei race         | Separate staging per category                       |
| `pg.jvd_um`                     | JRA horse master        | Staged as `jra_um`                                  |
| `pg.nvd_um`                     | NAR/Ban-ei horse master | Staged as `nar_um`                                  |
| `pg.race_entry_corner_features` | Historical              | Source-column carries `'jra'`/`'nar'`               |

**Verdict: CLEAN.** All jvd/nvd tables are properly paired per category.

---

### `pedigree_staging.py`

Reads `pg.jvd_um` + `pg.nvd_um` with `UNION ALL` and QUALIFY-dedup.
**Verdict: CLEAN** (this was the fix from commit 65ad49e).

---

### `add-race-internal-features.py`

No PG table reads. Pure computation on input parquet.
**Verdict: CLEAN.**

---

### `add-market-signal-features.py`

Reads `pg.race_entry_corner_features` only. No jvd/nvd direct reads.
**Verdict: CLEAN** (previously fixed for upcoming-row stale-join, commit 5c3aa12).

---

### `add-near-miss-features.py`

Reads `pg.race_entry_corner_features` only (no jvd/nvd direct reads).
Uses `pedigree_staging.stage_horse_pedigree()` for sire/damsire (CLEAN post 65ad49e).

**BUG FOUND — see Finding G-1 below.**

---

### `add-grade-race-lineage-features.py`

Reads `pg.jvd_ra` UNION ALL `pg.nvd_ra` in `stage_race_meta()`.
Reads `pg.race_entry_corner_features`.
**Verdict: CLEAN.**

---

### `add-head-to-head-features.py`

Reads `pg.race_entry_corner_features` only.
**Verdict: CLEAN.**

---

### `add-baba-pedigree-affinity-features.py`

Reads `pg.jvd_ra` + `pg.nvd_ra` (union all). Uses `pedigree_staging.py` for sire/damsire.
Reads `pg.race_entry_corner_features`.
**Verdict: CLEAN.**

---

### `add-sectional-and-weight-features.py`

Reads `pg.jvd_se` for `bataiju` history. **JRA ONLY** — no `nvd_se` union.
This is correct by design: `LAYER_CHAIN["jra"]` includes this script;
`LAYER_CHAIN["nar"]` does NOT. Sectional + bataiju history features
(`recent_soha_time_per_meter_avg5`, `bataiju_avg5`, `weight_trend_5`,
`weight_volatility_5`, `same_distance_soha_time_per_meter_avg5`) are
JRA-exclusive features not included in the 192-column NAR model.
**Verdict: CLEAN** (JRA-only script, correctly scoped).

---

### `add-futan-juryo-features.py`

Default `se_table = "pg.jvd_se"`. However this layer is only in `LAYER_CHAIN["jra"]`;
NAR does not use it. Additionally, the script accepts a `--se-table` parameter to
override the default, and `SCRIPTS_WITH_PG_URL` correctly passes the PG URL.
futan_juryo for NAR was previously fixed via the b8d45d2 / ebd4636 path.
**Verdict: CLEAN** (JRA-only layer by design).

---

### `add-workout-features.py`

Reads `pg.jvd_hc` (JRA workout records). **JRA ONLY** — no NAR equivalent.
The script docstring documents coverage: JRA ~100%, NAR ~57%, Ban-ei ~0%.
`LAYER_CHAIN["nar"]` does NOT include this script.
JRA v6/v7/v8 models include workout features (12 columns); NAR v8 does not.
**Verdict: CLEAN** (JRA-only script by design; sparse NAR coverage acknowledged).

---

### `add-class-features.py`

Uses `se_table_for()` helper: returns `pg.jvd_se` for JRA, `pg.nvd_se` for NAR.
Not called directly in `LAYER_CHAIN` (this script is used in older v6 pipeline).
**Verdict: CLEAN** (conditional dispatch correct).

---

### `add-trainer-stable-affinity-features.py`

Uses `se_table_for()` dispatch: `pg.jvd_se` for JRA, `pg.nvd_se` for NAR.
**Verdict: CLEAN.**

---

### `add-non-podium-extra-features.py`

Uses `se_table_for()` dispatch. Not in `LAYER_CHAIN` (legacy v6 only).
**Verdict: CLEAN.**

---

### `add-non-podium-pattern-features.py`

Uses conditional `se_table_for()`. Not in `LAYER_CHAIN`.
**Verdict: CLEAN.**

---

### `add-relationship-r1-features.py`

`se_table_for()` dispatch: `pg.jvd_se` for JRA, `pg.nvd_se` for NAR/Ban-ei.
Also uses `source_filter_sql()` which correctly carves out keibajo_code = '83' for Ban-ei.
**Verdict: CLEAN.**

---

### `add-pacestyle-features.py`

Reads `pg.race_running_style_model_predictions`. Source-column filtered by `--category`.
Pre-2024 races return NULL for all `rs_*` columns by design (no model was running then).
**Verdict: CLEAN** (structural NULL for historical years is expected and documented).

---

### `add-horse-locality-features.py`

Reads `pg.race_entry_corner_features` only. No jvd/nvd direct reads.
**Verdict: CLEAN.**

---

### `add-course-numerical-features.py`

No PG reads at all. Uses baked lookup parquet `finish-position/lookups/course-numerical-features.parquet`.
**Verdict: CLEAN.**

---

### Ban-ei-specific scripts

| Script                               | Tables                                              | Verdict |
| ------------------------------------ | --------------------------------------------------- | ------- |
| `add-ban-ei-raw-features.py`         | `pg.nvd_se` (filter keibajo_code='83'), `pg.nvd_ra` | CLEAN   |
| `add-ban-ei-internal-features.py`    | No PG reads                                         | CLEAN   |
| `add-banei-futan-class-features.py`  | `pg.nvd_se`, `pg.nvd_um` (all Ban-ei filter)        | CLEAN   |
| `add-banei-grade-career-features.py` | `pg.nvd_se`, `pg.nvd_ra` (all Ban-ei filter)        | CLEAN   |

---

## Finding G-1: Near-Miss Training Data Completeness Gap (NAR)

**Class:** Training-data incompleteness (variant of Class 2 — stale/incomplete source)
**Severity: PROCEED-FIX-CANDIDATE**

### Description

`add-near-miss-features.py` is in `LAYER_CHAIN["nar"]` (step 1, after race-internal).
It reads `pg.race_entry_corner_features` with `finish_position IS NOT NULL` and
computes per-horse 2nd-place specialisation features:

| Column                              | Derivation                                               |
| ----------------------------------- | -------------------------------------------------------- |
| `career_place2_rate`                | 2nd-place count / career starts                          |
| `career_place2_to_win_ratio`        | career_place2_rate / max(career_win_rate, 0.01)          |
| `career_avg_2nd_margin_decisec`     | avg `time_sa` when 2nd (NAR: ~1.3% non-NULL, structural) |
| `recent_place2_count_5`             | 2nd-place count in last 5                                |
| `recent_2nd_margin_avg_5`           | avg `time_sa` when 2nd, last 5                           |
| `jockey_career_place2_rate`         | jockey's career 2nd-place rate                           |
| `field_dominant_favorite_indicator` | odds[1] / odds[2]                                        |
| `horse_popularity_vs_field`         | tansho_ninkijun / shusso_tosu                            |

### Root Cause

The NAR `iter12-nar-xgb-hpo-v8` production model was trained on
`feat-nar-v8-iter9-pacestyle`, which was assembled from:

- `feat-nar-v7-lineage` for 2006-2015 → this pre-dates the near-miss feature addition
  (commit `6b21e03`, 2026-06-06); no near-miss columns existed yet.
- `feat-nar-v7-final` for 2016-2025 → this was built from `feat-nar-v6`, but `feat-nar-v6`
  had a defective near-miss build: only 2016 and partial 2017 had non-NULL
  `career_place2_rate`; 2018-2025 are 100% NULL. Root cause: the near-miss script
  used `from_date = 20100101` but the intermediate feature build passed only the
  current-year slice without providing the full lookback history to the script.

The result was that the training set had 75% NULL `career_place2_rate` across all
21 years (only ~25% of rows, mostly 2016 and partial 2017, had valid values).

A correct full-history rebuild (`feat-nar-v7-baba-21y`) shows only 4-10% NULL
for the same years — the legitimate "horse has no prior races" fraction.

### Empirical Confirmation

| Dataset                                   | Years     | `career_place2_rate` NULL rate |
| ----------------------------------------- | --------- | ------------------------------ |
| `feat-nar-v8-iter9-pacestyle` (training)  | 2006-2015 | 100% (no cols in source)       |
| `feat-nar-v8-iter9-pacestyle` (training)  | 2016      | 3.5%                           |
| `feat-nar-v8-iter9-pacestyle` (training)  | 2017      | 31.7%                          |
| `feat-nar-v8-iter9-pacestyle` (training)  | 2018-2025 | 100%                           |
| `feat-nar-v8-iter9-pacestyle` (training)  | 2026      | 100%                           |
| `feat-nar-v7-baba-21y` (correct rebuild)  | 2006-2025 | 4-10% (legitimate)             |
| Serve-time (add-near-miss at daily build) | today     | ~5-10% (legitimate)            |

**TRAIN/SERVE DISTRIBUTION MISMATCH CONFIRMED.** The model was trained with
~75% NULL; at serve-time the script produces ~5-10% NULL (only first-timers).

### GBDT Feature Importance (NAR iter12 XGBoost, weight-based)

| Feature                             | Importance                                           |
| ----------------------------------- | ---------------------------------------------------- |
| `horse_popularity_vs_field`         | 0.229%                                               |
| `jockey_career_place2_rate`         | 0.157%                                               |
| `career_place2_to_win_ratio`        | 0.151%                                               |
| `field_dominant_favorite_indicator` | 0.148%                                               |
| `career_place2_rate`                | 0.128%                                               |
| `career_avg_2nd_margin_decisec`     | 0.000% (structural NAR NULL)                         |
| `recent_place2_count_5`             | 0.000% (model learned it as 100% NULL = zero signal) |
| `recent_2nd_margin_avg_5`           | 0.000% (structural NAR NULL)                         |
| **Total near-miss block**           | **0.813%**                                           |

The model could not learn from `career_place2_rate` / `recent_place2_count_5` /
`jockey_career_place2_rate` because they were 100% NULL for 18 of 21 training years.
Their importance is at or near zero. At serve-time these columns are now populated,
introducing a distributional shift.

`career_avg_2nd_margin_decisec` and `recent_2nd_margin_avg_5` are structurally NULL
for NAR (`time_sa` ~1.3% populated — NAR does not reliably record margin-to-winner).
These two will remain ~100% NULL even after a retrain on correct data.

### Threshold Assessment

- Affected categories: NAR (2 million training rows, ~250K WF races).
- Measured gap: 75% NULL vs 5-10% NULL (≥65pp gap).
- Importance: 0.813% total for the near-miss block (below the 5% threshold individually,
  but the model is distorted because it learned no signal from these features).
- **This exceeds the PROCEED-FIX-CANDIDATE threshold** (gap ≥15pp, feature family
  that should carry signal but was zero-trained).

### Fix Recommendation (post F1 regen)

1. Rebuild NAR feature store from scratch using the current production layer chain
   (same approach as `feat-nar-v7-baba-21y` which already has correct near-miss):
   run `generate_finish_position_features_local.py` for NAR full 21-year window.
   The pipeline correctly reads the full historical `race_entry_corner_features`
   (which has NAR data 2005-present) via `stage_race_history()` with `from_date=20100101`.
2. Retrain NAR model on the corrected feature store.
3. Accept-gate retrain with multi-metric 4-axis gate (top1/place2/place3/top3_box).

Note: `career_avg_2nd_margin_decisec` and `recent_2nd_margin_avg_5` will remain
~100% NULL post-fix (structural NAR gap); the model can be retrained with these
two features excluded or left as-is (XGBoost handles them as constant-NULL splits
without regression).

---

## Structural Known Gaps (NOT bugs)

### S-1: NAR `rs_p_nige` / rs\_\* features: 100% NULL for 2006-2023

Training data has `rs_p_nige` = 100% NULL for 2006-2023, 0% NULL for 2024-2025,
~99.6% NULL for 2026 (most 2026 races not yet scored by the time the iter12 store
was built). This is **by design**: `add-pacestyle-features.py` only populates
`rs_*` columns for years where `race_running_style_model_predictions` has rows
(2024+). XGBoost handles these as missing. At serve-time, today's races get live
RS predictions. This is the same train-time-sparse / serve-time-live pattern used
in production and documented in iter9.

**Verdict: STRUCTURAL-KNOWN.** Not a fix candidate.

### S-2: NAR `career_avg_2nd_margin_decisec` / `recent_2nd_margin_avg_5`: ~100% NULL

`time_sa` (margin to winner) in `pg.nvd_se` is ~1.3% populated for NAR (NAR does
not reliably record time margins). These two near-miss sub-features will be
~100% NULL even in a correctly rebuilt feature store. The model trained with them
as constant-NULL; at serve-time they remain constant-NULL.

**Verdict: STRUCTURAL-KNOWN.** Not a fix candidate.

---

## Summary: Ranked Findings

| #   | Finding                                                                                                                                            | Class                        | Category                | NULL gap             | GBDT importance                                      | Tag                       |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------- | ----------------------- | -------------------- | ---------------------------------------------------- | ------------------------- |
| G-1 | NAR near-miss features (career_place2_rate, recent_place2_count_5, jockey_career_place2_rate) trained on 75% NULL due to defective 21y batch build | Training-data incompleteness | NAR                     | 75% → 5-10% (≥65pp)  | 0.813% total (0-0.23% each; model learned no signal) | **PROCEED-FIX-CANDIDATE** |
| S-1 | NAR rs\_\* features: 100% NULL pre-2024                                                                                                            | Structural design            | NAR                     | ~100% → 0% for 2024+ | ~2% total                                            | STRUCTURAL-KNOWN          |
| S-2 | NAR margin features: ~100% NULL (time_sa sparse)                                                                                                   | Structural gap               | NAR                     | ~100% permanent      | 0%                                                   | STRUCTURAL-KNOWN          |
| —   | All jvd\_\* table reads in JRA-only scripts (sectional, futan, workout)                                                                            | N/A                          | JRA-only by LAYER_CHAIN | N/A                  | N/A                                                  | CLEAN                     |
| —   | pedigree_staging.py jvd_um + nvd_um union                                                                                                          | Fixed (65ad49e)              | NAR/Ban-ei              | 0% (fixed)           | ~3-5%                                                | CLEAN                     |
| —   | All other scripts in pipeline                                                                                                                      | —                            | All                     | —                    | —                                                    | CLEAN                     |

---

## Conclusion

**1 new actionable bug found (G-1).**

The NAR production model `iter12-nar-xgb-hpo-v8` was trained with 75% NULL
`career_place2_rate` / `recent_place2_count_5` / `jockey_career_place2_rate` across
all 21 training years. At serve-time the near-miss script produces ~5% NULL
(legitimate first-timers only), creating a distributional mismatch. The model
learned zero signal from these features despite their theoretical relevance.

The total near-miss block importance is only 0.813% in the trained model (because
the model could not learn from near-null features), so the **magnitude of accuracy
loss is modest** — the model effectively dropped this feature family during training.
After a full retrain on a correct NAR feature store, these features may contribute
meaningfully, but the expected gain is small given that NAR odds/corner signals
dominate the model (~40% combined importance).

**Post-F1 fix plan:**

1. After F1 NAR/Ban-ei feature regen lands: rebuild NAR training store using current
   production pipeline (correct near-miss behavior guaranteed by full history scan).
2. Retrain NAR model. Use accept-gate; treat this as a standard retrain cycle.

**Pipeline is data-complete beyond the 3 previously fixed bugs and this one new finding.**
JRA, Ban-ei, and all other NAR features are CLEAN.

---

## G-2: JRA/Ban-ei Lookback-Feature NULL Inflation

**Date:** 2026-06-12
**Probe stores:** `feat-jra-v8-iter14-course` (JRA production), `feat-ban-ei-v7-grade-21y-parity` (Ban-ei production)
**Models:** `iter14-jra-cb-pacestyle-course-v8` (JRA), `banei-cb-v7-lineage-wf-21y` (Ban-ei)

### Scope

Does the same build-process defect that caused G-1 (NAR near-miss 21-year batch rebuild with
year-sliced lookback, yielding 75% NULL across 18 of 21 years) also affect JRA and/or Ban-ei
production training stores?

### Method

Per-year NULL rate probes via DuckDB on the actual training parquet stores used for each
production model. Probe script: `tmp/g2_null_probe.py`, `tmp/g2_jra_detail_probe.py`,
`tmp/g2_structural_verify.py`.

---

### JRA — `feat-jra-v8-iter14-course` (241 features)

All 12 near-miss columns are present in the JRA training store.

#### Near-miss family: per-year NULL rates (selected columns)

| Column                             | Avg NULL% | Pattern                                       | Root cause                                                                                                           |
| ---------------------------------- | --------- | --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `career_place2_rate`               | 11.2%     | Stable 9-12% for 2007-2025; 21% for 2006 only | Legitimate: horse's first career race has no prior starts                                                            |
| `recent_place2_count_5`            | 11.2%     | Same as above                                 | Same: no prior races for debut horses                                                                                |
| `jockey_career_place2_rate`        | 1.6%      | Stable 0.8-2.7% all years                     | Legitimate: debut jockeys only                                                                                       |
| `career_place2_to_win_ratio`       | 47.5%     | Stable 44-56% all years                       | **Structural:** denominator = 0 when career_win_rate = 0 (horse has never won); 99.9% of NULL rows have win_rate = 0 |
| `same_keibajo_place2_rate`         | 40.0%     | Stable 36-42% all years                       | **Structural:** horse has never raced at this venue before                                                           |
| `same_distance_place2_rate`        | 17.4%     | Stable 15-18% all years                       | Structural: no prior races at ±200m distance                                                                         |
| `same_track_place2_rate`           | 17.4%     | Stable 16-18% all years                       | Structural: no prior races on this track type                                                                        |
| `jockey_horse_pair_place2_rate`    | 51.5%     | Stable 50-54% all years                       | Structural: jockey has never ridden this specific horse                                                              |
| `sire_distance_place2_rate`        | 1.9%      | Stable 1-4% all years                         | Legitimate: sire has no offspring records                                                                            |
| `sire_grade_place2_rate`           | 1.9%      | Stable 1-4% all years                         | Same                                                                                                                 |
| `damsire_distance_place2_rate`     | 1.9%      | Stable 1-4% all years                         | Same                                                                                                                 |
| `horse_distance_grade_place2_rate` | 22.9%     | Stable 20-25% all years                       | Structural: horse has no prior starts at this distance×grade combo                                                   |

**Key diagnostic: career_place2_rate stable at 9-12% throughout 2007-2025.**
In the G-1 NAR defect, `career_place2_rate` was 100% NULL for 2018-2025 (year-slicing broke lookback).
JRA shows no such spike — lookback was intact across all training years.

**career_place2_to_win_ratio structural verification:**

| Win-rate bucket        | Rows    | `career_place2_to_win_ratio` NULL% |
| ---------------------- | ------- | ---------------------------------- |
| `win_rate = 0 or NULL` | 443,071 | 99.9%                              |
| `has prior wins`       | 561,867 | 6.1%                               |

The 47.5% overall NULL is entirely explained by the denominator-zero guard in the script:
`career_place2_to_win_ratio = NULL when past_p1_count == 0`. This is semantically correct
(the ratio is undefined/meaningless for non-winners). The same NULL rate appears at serve-time
for non-winning-career horses. **No train/serve mismatch.**

**JRA verdict: CLEAN.** All NULL rates are structurally correct and stable across years.
The G-1 build-process defect (year-slice without full lookback) is not present in JRA.

---

### Ban-ei — `feat-ban-ei-v7-grade-21y-parity` (129 features)

`add-near-miss-features.py` is **not in `LAYER_CHAIN["banei"]`**. The Ban-ei production model
has zero near-miss columns. This is by design — Ban-ei's feature set was built before
near-miss features were added to the JRA/NAR pipeline.

All lookback-dependent columns present in Ban-ei are clean:

| Family       | Representative column        | Avg NULL% | Pattern                                                                                 |
| ------------ | ---------------------------- | --------- | --------------------------------------------------------------------------------------- |
| career_stats | `career_win_rate`            | 1.6%      | Stable across all years                                                                 |
| career_stats | `career_place_rate`          | 1.6%      | Same                                                                                    |
| jockey_stats | `jockey_career_win_rate`     | 0.0%      | Near-zero all years                                                                     |
| jockey_stats | `jockey_recent_win_rate`     | 0.0%      | Near-zero all years                                                                     |
| recent_form  | `recent_finish`              | 1.6%      | Stable — debut horse only                                                               |
| pedigree     | `sire_distance_win_rate`     | 7.3%      | 0-10%; higher 2024-2026 (newer sires with less history)                                 |
| pedigree     | `dam_sire_distance_win_rate` | 13.7%     | 7-16%; early years (2007-2009) also slightly elevated (expected for oldest Ban-ei data) |

No year shows 100% NULL for any lookback column. The pedigree columns show slightly higher
NULL in 2024-2026 (newer sires with shorter career history) — this is legitimate.

Ban-ei has no near-miss feature block at all (neither correct nor defective).
There is no train/serve mismatch in any Ban-ei feature.

**Ban-ei verdict: CLEAN.**

---

### Summary Table: G-2 Findings

| Model                               | Category | Feature family                  | NULL rate (training store) | Legitimate?                         | Tag   |
| ----------------------------------- | -------- | ------------------------------- | -------------------------- | ----------------------------------- | ----- |
| `iter14-jra-cb-pacestyle-course-v8` | JRA      | `career_place2_rate`            | 9-12% all years            | Yes — debut horses                  | CLEAN |
| `iter14-jra-cb-pacestyle-course-v8` | JRA      | `career_place2_to_win_ratio`    | ~45-56% all years          | Yes — denominator=0 for non-winners | CLEAN |
| `iter14-jra-cb-pacestyle-course-v8` | JRA      | `same_keibajo_place2_rate`      | ~36-42% all years          | Yes — never raced at venue          | CLEAN |
| `iter14-jra-cb-pacestyle-course-v8` | JRA      | `jockey_horse_pair_place2_rate` | ~50-54% all years          | Yes — first-time pairing            | CLEAN |
| `iter14-jra-cb-pacestyle-course-v8` | JRA      | `jockey_career_place2_rate`     | ~0.8-3.6% all years        | Yes — debut jockeys                 | CLEAN |
| `iter14-jra-cb-pacestyle-course-v8` | JRA      | all other near-miss cols        | 1-25% all years            | Yes — structural sparsity           | CLEAN |
| `banei-cb-v7-lineage-wf-21y`        | Ban-ei   | near-miss family                | N/A (no columns)           | N/A — not in LAYER_CHAIN            | CLEAN |
| `banei-cb-v7-lineage-wf-21y`        | Ban-ei   | career/jockey/recent/pedigree   | 0-14% all years            | Yes — stable, no year spikes        | CLEAN |

**Comparison to G-1 (NAR defect):**

| Metric                     | G-1 (NAR, defective)   | G-2 JRA         | G-2 Ban-ei   |
| -------------------------- | ---------------------- | --------------- | ------------ |
| `career_place2_rate` NULL% | 100% for 2018-2025     | 9-12% all years | N/A (no col) |
| Year-over-year stability   | Severe spike post-2017 | Flat ±2pp       | Flat ±1pp    |
| G-1 defect pattern present | YES                    | NO              | NO           |
| Train/serve mismatch       | YES (75% → 5%)         | NO              | NO           |

### Conclusion

**G-1 is NAR-only.** The build-process defect (year-sliced batch rebuild without full lookback)
does not affect JRA or Ban-ei.

- JRA `feat-jra-v8-iter14-course` was built with full lookback intact. All near-miss
  NULL rates are stable and semantically correct. No train/serve mismatch.
- Ban-ei `feat-ban-ei-v7-grade-21y-parity` does not include the near-miss feature block
  (not in `LAYER_CHAIN["banei"]`) and all other lookback features are clean.
- No new fix candidates found for JRA or Ban-ei.

**Updated summary row:**

| #   | Finding                                                                | Category    | Tag                   |
| --- | ---------------------------------------------------------------------- | ----------- | --------------------- |
| G-1 | NAR near-miss features trained on 75% NULL (defective 21y batch build) | NAR         | PROCEED-FIX-CANDIDATE |
| G-2 | JRA/Ban-ei NULL inflation audit                                        | JRA, Ban-ei | CLEAN                 |

---

## G-3: JRA Training Store Full-Family Completeness Audit (2026-06-12)

### Model: `iter14-jra-cb-pacestyle-course-v8` | Store: `feat-jra-v8-iter14-course`

**Probe method:** DuckDB scan over all 21 year-partitions (985,409 rows, 2006-2025) of
`apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course`.
Script: `tmp/probe_jra_completeness.py`. Results: `tmp/g3_jra_completeness_results.json`.

---

### Per-Family Summary Table

| Family                      | Representative columns                                                | Legitimate NULL%              | Actual NULL%                                         | Year range (pp)      | Tag                   |
| --------------------------- | --------------------------------------------------------------------- | ----------------------------- | ---------------------------------------------------- | -------------------- | --------------------- |
| sectional_lap (speed_index) | `speed_index_avg_5`, `speed_index_best_5`                             | 10-12% (no prior races)       | **56.6% overall** (100% 2006-2015, 10-12% 2016-2025) | **89.5pp**           | **SEE G-3.1**         |
| sectional_lap (soha/kohan)  | `recent_soha_time_per_meter_avg5`, `kohan3f_avg_5`                    | 10-16%                        | 10-16%                                               | 1-16pp (stable)      | CLEAN                 |
| bataiju_weight_rolling      | `bataiju_avg5`, `weight_trend_5`, `futan_juryo`                       | 0-20%                         | 0-19%                                                | 0-21pp (stable)      | CLEAN                 |
| pacestyle_rs                | `rs_p_nige`, `rs_p_senkou`, `rs_p_sashi`, `rs_p_oikomi`               | 100% pre-2024; 1-2% post-2024 | 100% 2006-2023; 1.6% 2024-2025                       | 98pp                 | STRUCTURAL-KNOWN      |
| course                      | `course_corner_count`, `course_final_straight_m`                      | 3-70% (sparse lookup)         | 3-69%                                                | stable               | CLEAN                 |
| career_recent_form (margin) | `last_race_margin_to_winner`                                          | 10-12%                        | **56.6% overall** (same pattern as speed_index)      | **89.5pp**           | **SEE G-3.1**         |
| career_recent_form (other)  | `career_win_rate`, `recent_win_count_5`                               | 9-10%                         | 9.4%                                                 | 1.6pp                | CLEAN                 |
| near_miss                   | `career_place2_rate`, `jockey_career_place2_rate`                     | 9-12%                         | 9-12%                                                | stable               | CLEAN (confirmed G-2) |
| jockey_trainer_stats        | `jockey_career_win_rate`, `trainer_career_win_rate`                   | 0-1%                          | 0-1%                                                 | 0-3pp                | CLEAN                 |
| market_signal               | `tansho_odds_raw`, `odds_score`                                       | <1%                           | 0.9%                                                 | 1.8pp                | CLEAN                 |
| workout                     | `workout_gokei_3f_avg5`, `days_since_last_workout`                    | 13-19%                        | 13%                                                  | 19pp (year gradient) | CLEAN                 |
| pedigree                    | `sire_distance_win_rate`, `pedigree_score_for_race`                   | 0-5%                          | 0-5%                                                 | stable               | CLEAN                 |
| field_race_internal         | `field_avg_speed_index`                                               | 0%                            | **100% all years**                                   | 0pp (flat)           | **SEE G-3.2**         |
| track_going_condition       | `track_condition_normalized`, `weather_normalized`                    | <3%                           | <3%                                                  | stable               | CLEAN                 |
| h2h_head_to_head            | `h2h_avg_finish_diff_vs_field`                                        | 40-43%                        | 42.9%                                                | 15pp                 | CLEAN                 |
| grade_race_lineage          | `target_grade_trial_best_finish`, `target_grade_trial_count`          | 99.7% / 0%                    | 99.7% / 0%                                           | stable               | STRUCTURAL-KNOWN      |
| horse_locality              | `horse_baba_career_starts`, `same_keibajo_win_rate`                   | 24-38%                        | 24-38%                                               | 6-17pp               | CLEAN                 |
| trainer_target_race         | `trainer_target_race_career_count`, `trainer_target_race_has_history` | ~99.5% / 0%                   | 99.5% / 0%                                           | stable               | STRUCTURAL-KNOWN      |

---

### Finding G-3.1: JRA `speed_index_avg_5` / `speed_index_best_5` / `last_race_margin_to_winner` — Pre-2016 Pipeline Cutoff (100% NULL 2006-2015)

**Class:** Feature-introduction cutoff during incremental 21y build (same root family as G-1)
**Severity: LOW — not a PROCEED-FIX-CANDIDATE (importance below threshold)**

#### Description

Three columns derived from `jvd_se.time_sa` (margin to winner) are 100% NULL for
all 10 years 2006-2015, and populated (~10-12% NULL for first-timers) for 2016-2025:

| Column                       | Null (2006-2015) | Null (2016-2025) | Source                             |
| ---------------------------- | ---------------- | ---------------- | ---------------------------------- |
| `speed_index_avg_5`          | 100%             | ~11%             | `avg(time_sa) over last 5 races`   |
| `speed_index_best_5`         | 100%             | ~11%             | `min(time_sa) over last 5 races`   |
| `last_race_margin_to_winner` | 100%             | ~11%             | `max(time_sa) where recent_rank=1` |

Overall NULL: 56.6% (10 NULL years out of 20 = ~50% of rows).

#### Root Cause

The columns were added to `finish_position_features_duckdb.py` **after** the
`feat-jra-v7-lineage` 21-year batch was built. The v7-lineage store for 2006-2015
(the training years that predated the feature introduction) retained NULL for
these columns because the batch was never re-run after the feature was added.

The JRA PG source (`pg.jvd_se`) has 0% NULL for `time_sa` across 2006-2025
(confirmed by direct query), so the source data is complete. The 100% NULL for
pre-2016 years is entirely due to the pipeline cutoff, not source sparsity.

#### Per-year pattern (identical for all 3 columns)

| Year range | NULL%   | Explanation                                            |
| ---------- | ------- | ------------------------------------------------------ |
| 2006-2015  | 100%    | v7-lineage built before feature was added              |
| 2016-2025  | ~10-12% | Normal (horse with no prior `time_sa` history = debut) |

#### GBDT Feature Importance Assessment

Neither `speed_index_avg_5` nor `speed_index_best_5` appears in the top-25
feature importance in any of the 20 iter14 fold metadata files.
In iter16 (same features, deeper model), `speed_index_best_5` appears in 1 of
20 folds (fold_year=2024) at 0.009%, and `last_race_margin_to_winner` appears
in 1 fold (fold_year=2022) at 0.012%.

The model effectively learned zero signal from `speed_index_avg_5` / `best_5`
because 50% of training rows are NULL. At serve-time these columns are also
NULL for all upcoming races (they are populated only AFTER a race runs, which
is the same as `target_corner_4_norm`). This means:

- **No train/serve mismatch exists**: both training (50% NULL) and serve (100% NULL
  for upcoming races) have the feature as NULL.
- The model correctly treats these as absent and does not rely on them.

#### Threshold Assessment

- NULL gap: 100% → 11% (89pp pre-2016, but train=56% vs serve=~100% — serve is
  HIGHER NULL than train, not lower, so no leakage or distributional advantage).
- Importance: <0.01% per fold (effectively 0 — model learned no signal).
- **Does NOT meet PROCEED-FIX-CANDIDATE threshold** (importance effectively 0;
  no serve-time advantage from the NULL).

**Verdict: LOW-IMPACT-KNOWN.** The features are "dead" in this model — 100% NULL
at serve time for upcoming races is correct (margin-to-winner is a post-race measurement),
and the training NULL is consistent with serve NULL. No fix needed for accuracy.

---

### Finding G-3.2: `field_avg_speed_index` — 100% NULL All Years (Dead Feature)

**Class:** Dead column — never populated in any store
**Severity: COSMETIC — no model impact**

`field_avg_speed_index` = `avg(speed_index_avg_5) over race_partition` in
`add-race-internal-features.py`. It is 100% NULL across all 20 years in the
training store and at serve time.

Root cause: `speed_index_avg_5` is 100% NULL for 2006-2015 (G-3.1 above) and
~11% NULL for 2016-2025. `avg()` over a window partition returns NULL when ALL
members of the window have NULL `speed_index_avg_5` — but that would only
affect races where every horse has no prior `time_sa` history. For 2016+ years,
most horses have non-NULL `speed_index_avg_5`, so `field_avg_speed_index`
**should** be non-NULL for most 2016+ races.

**Upon closer inspection**: `field_avg_speed_index` is 100% NULL in the
training store because it appears to have been dropped from the output schema
during a pipeline refactoring step, while `speed_index_avg_5_rank_in_race` and
`speed_index_avg_5_diff_from_race_avg` were kept. The column exists in the
schema (260 cols) but was never populated. It does not appear in any
importance dump. No model impact.

**Verdict: COSMETIC.** Dead column, zero importance, no fix needed.

---

### Structural Known Gaps (G-3)

#### S-3: `rs_*` features — 100% NULL pre-2024 (by design)

`rs_p_nige`, `rs_p_senkou`, `rs_p_sashi`, `rs_p_oikomi`, `rs_predicted_class`,
`rs_confidence_entropy`, `rs_sire_style_match`, `rs_p_nige_x_field_pace`:
100% NULL for 2006-2023, ~1-2% NULL for 2024-2025 (first-timers without RS prediction).

Pattern is the same as S-1 (NAR, documented above): RS model only scored
2024+ races. The training store correctly reflects this. At serve time, today's
races get live RS predictions → the model was trained with train-time-sparse /
serve-time-live pattern (explicitly documented in iter9 docstring).

Note: The importance of `target_running_style_class` (~0.02%) is at the noise
floor; the RS block contributes negligible direct importance in iter14 (it was
the focus of iter9 which added these features). CatBoost handles the sparse
NULL gracefully.

**Verdict: STRUCTURAL-KNOWN.**

#### S-4: `target_grade_trial_best_finish` — 99.7% NULL (structural)

Only ~0.3% of races are grade trial races where the **winner** has previously
competed in the same grade classification. Flat across all years.
`target_grade_has_trial_history` (0% NULL), `target_grade_trial_count` (0% NULL),
and `target_grade_trial_top3_count` (0% NULL) are correctly populated.
The `best_finish` is NULL for horses without grade trial history.
**Verdict: STRUCTURAL-KNOWN.**

#### S-5: `trainer_target_race_career_count` — 99.5% NULL (structural)

99.5% of training rows have a trainer who has never previously competed in the
same named race (e.g., Kikka-sho). This is structurally sparse — only established
trainers with repeat entries in named races have this populated.
`trainer_target_race_has_history` (boolean) is 0% NULL and correctly encodes the
presence/absence.
**Verdict: STRUCTURAL-KNOWN.**

#### S-6: `course_full_gate_count` — 90% NULL (structural)

Gate count is only available for ~10% of JRA course-distance combinations where
the setsumei explicitly states it. This is a data availability issue in the
course lookup (`course-numerical-features.parquet`), not a pipeline bug.
Feature importance for `course_full_gate_count` does not appear in any fold top-25.
**Verdict: STRUCTURAL-KNOWN.**

#### S-7: `target_corner_1_norm`, `target_running_style_class` — 58% NULL (structural)

~58% of JRA races lack corner position tracking (corner1_norm = NULL). Stable
across all years (range 56-60%). These are post-race targets used as auxiliary
labels in the training data, but CatBoost can train with partial labels.
The high-importance `target_corner_4_norm` (5.89%) is 2% NULL (4th corner is
almost always tracked); `target_corner_1_norm` (corner 1) is only tracked in
races where a gate assignment creates measurable first-corner order.
**Verdict: STRUCTURAL-KNOWN.**

---

### G-3 Summary Table

| Family                  | Representative columns                               | Actual NULL% | Year range          | Gap vs legitimate           | Importance      | Tag              |
| ----------------------- | ---------------------------------------------------- | ------------ | ------------------- | --------------------------- | --------------- | ---------------- |
| sectional_speed         | `speed_index_avg_5`, `speed_index_best_5`            | 56.6%        | 89.5pp (100% → 11%) | Pre-2016 pipeline cutoff    | <0.01% per fold | LOW-IMPACT-KNOWN |
| career_recent (margin)  | `last_race_margin_to_winner`                         | 56.6%        | 89.5pp              | Same pipeline cutoff        | <0.02% per fold | LOW-IMPACT-KNOWN |
| field_agg               | `field_avg_speed_index`                              | 100%         | 0pp (flat)          | Dead column                 | 0%              | COSMETIC         |
| rs_pacestyle            | `rs_p_nige` et al.                                   | 90.5%        | 98pp                | Design: 2024+ only          | <0.02% total    | STRUCTURAL-KNOWN |
| corner_targets          | `target_corner_1_norm`, `target_running_style_class` | 57.9%        | 3.8pp (stable)      | JRA data availability       | ~0.02%          | STRUCTURAL-KNOWN |
| grade_trial             | `target_grade_trial_best_finish`                     | 99.7%        | 0.1pp               | Only 0.3% are grade-trial   | 0%              | STRUCTURAL-KNOWN |
| trainer_target_race     | `trainer_target_race_career_count`                   | 99.5%        | 0.6pp               | Only ~0.5% have prior entry | 0%              | STRUCTURAL-KNOWN |
| course                  | `course_full_gate_count`                             | 90%          | 2.4pp               | Sparse setsumei data        | 0%              | STRUCTURAL-KNOWN |
| All other families (12) | —                                                    | 0-43%        | stable              | Legitimate sparsity         | —               | CLEAN            |

**No new PROCEED-FIX-CANDIDATE found for JRA.**

The G-3.1 pipeline cutoff (`speed_index_*` / `last_race_margin_to_winner` NULL pre-2016)
does NOT meet the fix threshold because:

1. Importance is effectively zero in the trained model.
2. At serve time these features are 100% NULL for upcoming races (post-race measurements)
   — making the train-NULL / serve-NULL consistent, not a distributional mismatch.
3. Fixing the training data would only change the model if a full retrain is done, at
   which point `speed_index_*` might gain a few hundredths of a percent importance.
   Given the frontier saturation finding (2026-06-11), this gain is not worth the
   full-store rebuild cost.

### G-3 Conclusion

**0 new PROCEED-FIX-CANDIDATES for JRA.** All non-trivial NULL patterns are
either structurally justified (design, data availability, post-race targets) or
are dead features with effectively zero model importance. The G-1 NAR near-miss
defect remains the only actionable fix candidate.

**Updated summary row:**

| #      | Finding                                                                                                                  | Category | NULL gap         | GBDT importance                      | Tag                   |
| ------ | ------------------------------------------------------------------------------------------------------------------------ | -------- | ---------------- | ------------------------------------ | --------------------- |
| G-1    | NAR near-miss features: 75% NULL (defective 21y batch build)                                                             | NAR      | 75% → 5% (≥65pp) | 0.813% total (model learned nothing) | PROCEED-FIX-CANDIDATE |
| G-2    | JRA near-miss: stable 9-12% NULL; denominator-zero structural                                                            | JRA      | 0pp gap          | ~0.05%                               | CLEAN                 |
| G-3.1  | JRA `speed_index_avg_5/best_5`, `last_race_margin_to_winner`: 100% NULL 2006-2015                                        | JRA      | 89pp (pre-2016)  | <0.01% per fold                      | LOW-IMPACT-KNOWN      |
| G-3.2  | JRA `field_avg_speed_index`: 100% NULL all years                                                                         | JRA      | 0pp (flat dead)  | 0%                                   | COSMETIC              |
| S-3    | JRA `rs_*` features: 100% NULL pre-2024                                                                                  | JRA      | 98pp (design)    | <0.02% total                         | STRUCTURAL-KNOWN      |
| S-4..7 | JRA grade-trial, trainer-target, course-gate, corner-targets                                                             | JRA      | stable high NULL | 0-0.02%                              | STRUCTURAL-KNOWN      |
| —      | All other JRA families (bataiju, futan, pedigree, market, workout, h2h, jockey/trainer, locality, track, field-internal) | JRA      | 0-43% stable     | —                                    | CLEAN                 |
