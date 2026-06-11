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
