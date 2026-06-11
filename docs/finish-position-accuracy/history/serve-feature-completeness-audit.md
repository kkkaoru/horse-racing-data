# Serve-Time Feature Completeness Audit

**Date:** 2026-06-11  
**Scope:** All layer scripts in the JRA/NAR finish-position pipeline. Market-signal layer (`add-market-signal-features.py`) is **excluded** — handled by a separate fix already in flight.  
**Method:** Static source-code trace of each layer's SQL + CatBoost `PredictionValuesChange` (JRA) and XGBoost `gain` (NAR) importance extraction from production models.

## Production Models

| Category      | Model                                               | Algorithm | Features |
| ------------- | --------------------------------------------------- | --------- | -------- |
| JRA global    | `iter14-jra-cb-pacestyle-course-v8`                 | CatBoost  | 241      |
| JRA per-class | `iter26-jra-cb-relationships-005-v8` (and siblings) | CatBoost  | 254      |
| NAR global    | `iter12-nar-xgb-hpo-v8`                             | XGBoost   | 192      |
| NAR per-class | `iter30-nar-cb-residual-A-v8` (and siblings)        | CatBoost  | 174      |

## Known / Excluded Gaps

| Gap                                 | Status                                       |
| ----------------------------------- | -------------------------------------------- |
| Odds/popularity OOD-median fallback | FIXED — JRA 09:30 cron injects realtime odds |
| Market-signal layer all-NULL        | EXCLUDED — separate agent fix in progress    |

## Confirmed Serve-Time Bugs

### BUG-1 — futan_juryo current-race value (MEDIUM)

**Layer:** `add-futan-juryo-features.py`  
**Root cause:** `stage_futan_juryo()` reads `race_entry_corner_features` and LEFT JOINs on current-race identity to get today's `futan_juryo`. The join returns NULL when `race_entry_corner_features` has not yet been rebuilt for today's races (the table is populated by `build-corner-feature-table.ts` which runs nightly but not before the 03:00 predict pipeline).

**Affected features and JRA iter14 importance:**

| Feature                          | JRA iter14 PVC% | has_nans / nan_treatment     | Serve behaviour                 |
| -------------------------------- | --------------- | ---------------------------- | ------------------------------- |
| `futan_juryo`                    | 0.20%           | `False / AsIs` (OOD if NULL) | NULL when corner_features stale |
| `futan_juryo_rank_in_race`       | 0.02%           | `False / AsIs`               | NULL (derived from futan_juryo) |
| `futan_juryo_diff_from_race_avg` | 0.005%          | `False / AsIs`               | NULL (derived)                  |

**NAR:** `futan_juryo` features are not in the NAR model (NAR uses `futan_per_barei` from the relationship layer, which has a correct `jvd_se` fallback).

**Fix direction:**  
Add `COALESCE(rec.futan_juryo, cast(se.futan_juryo as double) / 10.0)` in the `stage_futan_juryo()` SELECT so that when the `race_entry_corner_features` row is absent, the raw `jvd_se.futan_juryo` value (published pre-race in shutsuba-to, stored in units of 0.1 kg) is used as a fallback. This is a one-line SQL change — no schema changes needed.

---

### BUG-2 — futan_juryo history features (HIGH for established horses)

**Layer:** `add-futan-juryo-features.py`  
**Root cause:** History features use `h.race_date < t.race_date` to aggregate from `race_entry_corner_features`. These rows ARE available for established horses because the nightly rebuild covers all historical races. However, the history JOIN works correctly only when `race_entry_corner_features` contains the horse's past entries. The bug manifests when:

1. The horse is a debutant (no history — NULL is legitimate/expected).
2. `race_entry_corner_features` was never rebuilt at all (operational failure).

**Affected features and JRA iter14 importance:**

| Feature                 | JRA iter14 PVC% | has_nans / nan_treatment         | Serve behaviour                   |
| ----------------------- | --------------- | -------------------------------- | --------------------------------- |
| `past_futan_juryo_diff` | **2.68%**       | `False / AsIs` (OOD if NULL)     | NULL for debutants or stale table |
| `past_futan_juryo_avg5` | **2.33%**       | `False / AsIs`                   | NULL for debutants or stale table |
| `past_high_futan_share` | 0.00%           | `False / AsIs`, borders=0 (dead) | NULL but zero splits              |

**NAR:** Not in model.

**Combined JRA history importance: 5.02%** — this is the largest single recoverable importance block outside the known gaps.

**Fix direction:**  
The history JOIN itself is correct. The prerequisite is that `race_entry_corner_features` is rebuilt nightly with futan_juryo populated. BUG-1's COALESCE fix ensures future-race futan values are present in `race_entry_corner_features` after the next rebuild, closing the loop. For debutants, NULL is expected and accepted (CatBoost `has_nans=False` / `AsIs` with non-empty borders is an OOD issue only for non-debutant established horses whose table entry is missing).

---

### BUG-3 — shusso_tosu_1 (near-miss layer) (LOW)

**Layer:** `add-near-miss-features.py`  
**Root cause:** `stage_race_history()` reads `race_entry_corner_features WHERE finish_position IS NOT NULL`. The near-miss layer derives `shusso_tosu_1` from this history view — it represents the field size of the horse's most recent completed race. For established horses, this JOIN on horse identity should return the last completed race's value correctly, so this may be a false alarm. For first-race debutants, NULL is expected.

**Affected features:**

| Feature         | JRA PVC% | NAR gain% | Serve behaviour                                                |
| --------------- | -------- | --------- | -------------------------------------------------------------- |
| `shusso_tosu_1` | 0.03%    | 0.16%     | Populated for established horses (history), NULL for debutants |

**Note:** `shusso_tosu_1` differs from `shusso_tosu` (the current race's field size from the base builder, which is always populated). `shusso_tosu_1` is the historical field size from the horse's last race.

**Fix direction:** Verify SQL — if the LEFT JOIN on horse identity already correctly returns last-race field size from `race_history` for non-debutants, this is not a serve bug. Debutant NULL is legitimate.

---

### BUG-4 — field_dominant_favorite_indicator (near-miss layer) (NEGLIGIBLE/LOW)

**Layer:** `add-near-miss-features.py`  
**Root cause:** `race_favorite_dominance` CTE aggregates tansho_ninkijun from `race_history` (finish_position IS NOT NULL). For the current race partition, there are no finished rows yet, so this CTE produces NULL.

| Feature                             | JRA PVC% | NAR gain% | JRA nan treatment                | Serve behaviour             |
| ----------------------------------- | -------- | --------- | -------------------------------- | --------------------------- |
| `field_dominant_favorite_indicator` | 0.00%    | 0.15%     | `False / AsIs`, borders=0 (dead) | NULL for all upcoming races |

**Fix direction:** This conceptually measures pre-race market dominance. Could be computed from realtime odds (already available via the hot worker) rather than from historical finished rows. Lowest priority given near-zero JRA importance and 0.15% NAR importance.

---

### BUG-5 — current_baba_condition timing (LOW/MEDIUM for NAR)

**Layer:** `add-baba-pedigree-affinity-features.py`  
**Root cause:** `stage_race_baba()` reads `babajotai_code` from `jvd_ra`/`nvd_ra` for the current race. This field is published at race opening (~08:00 JST). The prediction pipeline runs at 03:00 JST — before publication.

| Feature                  | JRA PVC%            | NAR gain% | Serve behaviour            |
| ------------------------ | ------------------- | --------- | -------------------------- |
| `current_baba_condition` | 0.00% (zero splits) | **0.48%** | NULL at 03:00 (timing gap) |

**Fix direction:**

- **Option A (accept):** NULL at serve is uniform across all horses in a race (all see the same track that day) — no relative distortion within a race, only information loss.
- **Option B (proxy):** Use the most recent `babajotai_code` for the same `keibajo_code` from the prior day as a 03:00-safe proxy.
- **Option C (defer):** If the predict pipeline can be deferred to 09:00 JST (post odds cron), babajotai_code will be available.

---

### BUG-6 — track_condition_normalized + weather_normalized timing (NEGLIGIBLE for JRA, LOW for NAR)

**Layer:** `finish_position_features_duckdb.py` (base builder, `materialize_weather_lookup` + `base_features_select_sql`)  
**Root cause:** Both features derive from `ra` table columns (`babajotai_code_shiba/dirt` and `tenko_code`) which are published at race opening (~08:00 JST). Predict runs at 03:00 JST.

| Feature                      | JRA PVC% | JRA nan treatment                                   | NAR gain% | Serve behaviour |
| ---------------------------- | -------- | --------------------------------------------------- | --------- | --------------- |
| `track_condition_normalized` | 0.00%    | `has_nans=True / AsFalse`, borders=0 (NULL→0, dead) | **0.49%** | NULL→0 at serve |
| `weather_normalized`         | 0.00%    | `has_nans=False / AsIs`, borders=0 (dead)           | **0.19%** | NULL at serve   |

**JRA safety:** Both features have zero splits in JRA iter14 — model never branches on them regardless of value. `track_condition_normalized` has `nan_treatment=AsFalse` so NULL is mapped to 0 (safe). `weather_normalized` has `nan_treatment=AsIs` and borders=0 so it is also effectively ignored.

**Fix direction:** Same as BUG-5 Option A/B/C. Since JRA model ignores these entirely, the fix priority is NAR-only.

---

## Five Flagged Suspects — Final Verdict

| Feature                      | Verdict                     | JRA importance      | NAR importance | Reason                                                                                                                                                                                   |
| ---------------------------- | --------------------------- | ------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `shusso_tosu`                | **CLEAR**                   | —                   | 0.36%          | Base builder: `COALESCE(nullif(ra.shusso_tosu,0), count(*) OVER race)`. Always populated.                                                                                                |
| `umaban_norm`                | **CLEAR**                   | —                   | 0.45%          | `t.umaban / (t.shusso_tosu - 1)`. umaban from jvd_se pre-race; shusso_tosu always populated.                                                                                             |
| `track_condition_normalized` | **TIMING BUG — NEGLIGIBLE** | 0.00% (zero splits) | 0.49%          | ra.babajotai_code published ~08:00 JST; predict at 03:00 JST. JRA dead feature, NAR minor.                                                                                               |
| `weather_normalized`         | **TIMING BUG — NEGLIGIBLE** | 0.00% (zero splits) | 0.19%          | ra.tenko_code same timing issue. JRA dead feature.                                                                                                                                       |
| `weight_diff_from_avg`       | **MOSTLY CLEAR**            | 0.008%              | 0.28%          | `current_bataiju` from jvd_se (declared pre-race) + realtime bataiju via COALESCE. Official bataiju published ~07:30; at 03:00 falls back to jvd_se.bataiju=0 → diff=0. Very low impact. |

## Summary Table — All Confirmed Bugs

| Bug   | Feature(s)                                                            | Layer                                  | JRA PVC%  | NAR gain% | Severity                    | Fix effort                                      |
| ----- | --------------------------------------------------------------------- | -------------------------------------- | --------- | --------- | --------------------------- | ----------------------------------------------- |
| BUG-1 | futan_juryo, futan_juryo_rank_in_race, futan_juryo_diff_from_race_avg | add-futan-juryo-features.py            | 0.22%     | N/A       | MEDIUM                      | LOW — 1-line COALESCE                           |
| BUG-2 | past_futan_juryo_diff, past_futan_juryo_avg5                          | add-futan-juryo-features.py            | **5.02%** | N/A       | HIGH                        | LOW — prerequisite: BUG-1 fix + nightly rebuild |
| BUG-3 | shusso_tosu_1                                                         | add-near-miss-features.py              | 0.03%     | 0.16%     | LOW                         | VERIFY first — may not be a bug                 |
| BUG-4 | field_dominant_favorite_indicator                                     | add-near-miss-features.py              | 0.00%     | 0.15%     | NEGLIGIBLE/LOW              | MEDIUM — needs realtime odds source             |
| BUG-5 | current_baba_condition                                                | add-baba-pedigree-affinity-features.py | 0.00%     | 0.48%     | LOW-MEDIUM (NAR)            | ACCEPT or proxy                                 |
| BUG-6 | track_condition_normalized, weather_normalized                        | finish_position_features_duckdb.py     | 0.00%     | 0.68%     | NEGLIGIBLE (JRA), LOW (NAR) | ACCEPT or defer to 09:00                        |

## Total Recoverable Importance

| Model      | Confirmed bug importance | Note                                                  |
| ---------- | ------------------------ | ----------------------------------------------------- |
| JRA iter14 | **5.24%**                | Dominated by futan history (5.02%)                    |
| NAR iter12 | **1.47%**                | Spread across timing bugs (1.16%) + near-miss (0.31%) |

The JRA futan group (BUG-2) is the single highest-priority fix: 5.02% importance with a simple COALESCE prerequisite fix in BUG-1.

## Layer-by-Layer Serve-Safety Summary

| Layer script                                | Reads upcoming rows correctly? | Notes                                                                                                                                   |
| ------------------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| `finish_position_features_duckdb.py` (base) | YES (with caveats)             | `_rec_priority` QUALIFY trick handles upcoming via direct se×ra join. babajotai/tenko timing bugs exist but are dead features in JRA.   |
| `add-race-internal-features.py`             | YES                            | No external table dependency; all within-race computations from base parquet.                                                           |
| `add-market-signal-features.py`             | **EXCLUDED**                   | Separate fix in flight.                                                                                                                 |
| `add-sectional-and-weight-features.py`      | YES                            | Only reads history (h.race_date < t.race_date); no current-race dependency on stale sources.                                            |
| `add-futan-juryo-features.py`               | **PARTIAL BUG**                | Current-race futan_juryo NULL when corner_features stale (BUG-1/2). History correct for established horses.                             |
| `add-near-miss-features.py`                 | LIKELY OK                      | History JOIN on horse identity should return last completed race data. Verify shusso_tosu_1 / field_dominant_favorite_indicator.        |
| `add-lineage-features.py`                   | YES                            | Reads pedigree tables (static); no upcoming-race dependency.                                                                            |
| `add-h2h-features.py`                       | YES                            | History only (h.race_date < t.race_date filter).                                                                                        |
| `add-baba-pedigree-affinity-features.py`    | TIMING BUG (minor)             | current_baba_condition NULL at 03:00 (BUG-5). Historical affinity unaffected.                                                           |
| `add-trainer-features.py`                   | YES                            | Trainer win-rate history; no current-race row dependency.                                                                               |
| `add-pacestyle-features.py`                 | YES (with prerequisite)        | Reads R2 running-style predictions for today; prewarm cron at 12:00 JST prior day materialises predictions. Gap risk if prewarm failed. |
| `add-course-numerical-features.py`          | YES                            | Static course lookup; no upcoming-race dependency.                                                                                      |
| `add-relationship-r1-features.py`           | YES                            | `COALESCE(rec.futan_juryo, se.futan_juryo/10)` + `COALESCE(rec.barei, se.barei)` correctly handles upcoming rows via se fallback.       |

## Recommended Fix Priority

1. **BUG-1 (LOW effort, enables BUG-2 fix):** Add `COALESCE(rec.futan_juryo, cast(se.futan_juryo as double) / 10.0)` in `add-futan-juryo-features.py → stage_futan_juryo()`. This ensures `race_entry_corner_features` gets populated with today's futan_juryo on next nightly rebuild, which in turn fixes BUG-2 history features on the following day.
2. **Operational prerequisite for BUG-2:** Ensure `build-corner-feature-table.ts` runs as part of the daily data pipeline before predict time (or at minimum nightly), covering current-day races. This is the structural root cause: `race_entry_corner_features` is the canonical serve source but is not rebuilt in `pipeline_runner.py` before the layer chain runs.
3. **BUG-5/6 (timing, accept or proxy):** For NAR, `current_baba_condition`, `track_condition_normalized`, and `weather_normalized` are collectively 1.16% importance. These require either deferring predict to post-08:00 or accepting the uniform-NULL-for-all-horses information loss.
4. **BUG-3/4 (verify + low importance):** Verify `shusso_tosu_1` is not already correctly populated from historical JOIN before treating as a bug. `field_dominant_favorite_indicator` is lowest priority.
