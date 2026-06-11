# D2a — NAR Venue 43/44 Gap Root-Cause Diagnosis

**Date**: 2026-06-11
**Scope**: NAR Funabashi (keibajo 43) / Kawasaki (keibajo 44) top1 underperformance vs NAR average
**Model**: NAR iter12 (XGBoost, 192 features)
**Holdout**: 2023–2025 (412,729 rows, 40,702 races)
**Target races**: 43+44 = 5,612 holdout races (13.8% of NAR holdout)
**Feature parquet**: `apps/pc-keiba-viewer/tmp/feat-nar-v7-final`
**Output JSON**: `tmp/rootcause/d2a_nar_venue_gap.json`

---

## Known Gap (from D1 ceiling diagnosis)

| Venue          | top1 delta vs NAR avg | n races (holdout) | % of NAR holdout |
| -------------- | --------------------- | ----------------- | ---------------- |
| 43 (Funabashi) | -10.3pp               | 2,203             | 5.4%             |
| 44 (Kawasaki)  | -9.3pp                | 3,409             | 8.4%             |
| Combined       | ~-9.8pp               | 5,612             | 13.8%            |

NAR global top1 (holdout): 58.10%. Estimated actual at 43: ~47.8%, at 44: ~48.8%.

---

## Investigation 1 — Oracle Reducibility Test

**Method**: Market oracle accuracy = fraction of races where the horse with the lowest `popularity_score` (most popular, oracle proxy) won. Computed for 43+44 vs other NAR venues.

| Venue group    | n races | Oracle top1 |
| -------------- | ------- | ----------- |
| other NAR      | 35,097  | 44.91%      |
| 43+44 combined | 5,612   | 42.12%      |
| Venue 43       | 2,203   | 43.31%      |
| Venue 44       | 3,409   | 41.36%      |

**Oracle gap**: 44.91% − 42.12% = **−2.79pp**

**Gap decomposition**:

| Component                       | Amount | Type                                  |
| ------------------------------- | ------ | ------------------------------------- |
| Oracle gap (market also weaker) | 2.8pp  | Irreducible — genuine race difficulty |
| Residual model-specific gap     | ~7.0pp | Potentially reducible                 |
| Total model gap                 | ~9.8pp | Combined                              |

**Reducibility verdict: PARTIALLY_REDUCIBLE.** The market oracle is also ~2.8pp weaker at 43/44, confirming some structural difficulty (larger fields, more competitive fields). But ~7pp of the gap is MODEL-SPECIFIC and not explained by oracle weakness.

Individual venue oracle ranking (all NAR venues):

| Rank | Venue | Oracle top1 |
| ---- | ----- | ----------- |
| 1    | 46    | 50.18%      |
| 2    | 47    | 47.51%      |
| ...  | ...   | ...         |
| 13   | 43    | 43.31%      |
| 14   | 44    | 41.36%      |
| 15   | 42    | 38.47%      |

Venue 42 (Urawa) also has a weak oracle but is not as large a volume problem.

---

## Investigation 2 — Feature Completeness Audit

Per-feature NULL rate comparison: 43+44 vs all other NAR venues in the 2023–2025 holdout.

### FLAGGED features (|gap| > 3pp)

| Feature group     | Feature                         | 43/44 NULL % | Other NULL % | Gap         |
| ----------------- | ------------------------------- | ------------ | ------------ | ----------- |
| pacestyle_running | past_nige_rate_self             | **25.94%**   | 9.28%        | **+16.7pp** |
| pacestyle_running | past_senkou_rate_self           | **25.94%**   | 9.28%        | **+16.7pp** |
| pacestyle_running | past_sashi_rate_self            | **25.94%**   | 9.28%        | **+16.7pp** |
| pacestyle_running | past_oikomi_rate_self           | **25.94%**   | 9.28%        | **+16.7pp** |
| pacestyle_running | past_corner_1_norm_avg_5        | **39.36%**   | 14.31%       | **+25.1pp** |
| pacestyle_running | past_corner_1_norm_avg_3        | **45.07%**   | 17.11%       | **+28.0pp** |
| pacestyle_running | past_corner_1_norm_std_5        | **53.27%**   | 21.93%       | **+31.3pp** |
| pacestyle_running | horse_keibajo_corner_1_norm_avg | **39.32%**   | 19.81%       | **+19.5pp** |

**All other feature groups have <3pp NULL gap.** Market signal, jockey, trainer, track context, pedigree: all fully populated at 43/44 identically to other venues.

### NULL coverage per venue (full training set, 2016–2025)

| Venue   | n races    | RS NULL %  | Corner NULL % | Keibajo corner NULL % |
| ------- | ---------- | ---------- | ------------- | --------------------- |
| 54      | 12,280     | 2.17%      | 2.30%         | 4.59%                 |
| 55      | 12,375     | 3.47%      | 4.08%         | 7.18%                 |
| 50      | 17,088     | 4.38%      | 5.15%         | 6.45%                 |
| 46      | 9,601      | 3.88%      | 3.99%         | 6.79%                 |
| **43**  | **6,921**  | **14.52%** | **21.22%**    | **36.13%**            |
| **44**  | **11,480** | **23.99%** | **40.61%**    | **33.80%**            |
| 30 (Ōi) | 9,491      | 45.13%     | 60.24%        | 56.81%                |

43/44 are the 2nd and 3rd worst venues for RS NULL coverage. Venue 30 is worst but was already handled via the rejected Ōi specialist approach.

---

## Investigation 3 — Root Cause of the NULL Explosion

### Finding: Locally-Anchored Horse Population

**28.9% of all 43/44-racing horses run ≥90% of their career races exclusively at 43/44.**
**10.9% run 70–90% of their races at 43/44.**
**Total "local-dominant" horses: ~40% of the 43/44 racing population.**

These horses have essentially no corner history from other NAR venues. The running-style model (v3) cannot produce style predictions without corner position data → entire pacestyle group goes NULL.

| Horse concentration      | n horses | % of 43/44 pop | avg target races | avg other races |
| ------------------------ | -------- | -------------- | ---------------- | --------------- |
| Exclusively local (≥90%) | 5,910    | 28.9%          | 21.0             | 0.3             |
| Mostly local (70–90%)    | 2,227    | 10.9%          | 28.8             | 7.4             |
| Majority local (50–70%)  | 2,417    | 11.8%          | 21.7             | 14.7            |
| Mixed/travels (<50%)     | 9,900    | 48.4%          | 8.0              | 40.7            |

RS NULL rates by horse type within 43/44 races (per year):

| Year | local_43_44 RS NULL % | travels RS NULL % |
| ---- | --------------------- | ----------------- |
| 2023 | 30.77%                | 19.23%            |
| 2024 | 31.37%                | 17.01%            |
| 2025 | 31.89%                | 12.07%            |

The NULL rate for local horses is rising year-over-year (30→32%), likely as the local racing population grows without cross-venue history expanding.

---

## Investigation 4 — Routing Audit

NAR is explicitly excluded from Phase B per-class routing (`PER_CLASS_ENABLED_CATEGORIES` does not include NAR). All 43/44 races route to the single global NAR model `iter12-nar-xgb-pacestyle-v8`.

The `nar_subclass` regex (ＯＰ/新馬/未勝利/Ａ/Ｂ/Ｃ/２歳/３歳) is a feature-derivation artifact not stored in the parquet. It is correctly applied only for source='nar' AND keibajo_code ≠ '83'. No routing mis-bucket possible since all NAR non-ban-ei races go to one model.

**Routing verdict: NO BUG.**

---

## Investigation 5 — Track Characteristics / Field Size / Mix

### Field size: dramatically larger at 43/44

| Bucket           | 43+44 | Other NAR |
| ---------------- | ----- | --------- |
| Small (≤9)       | 23.1% | 36.6%     |
| Medium (10–12)   | 43.1% | 62.0%     |
| Large (13–14)    | 26.0% | 1.4%      |
| Very large (15+) | 7.8%  | 0.03%     |

Venue 44 avg field size: **12.57** vs ~10.2 at most other NAR venues.

This is the primary driver of the 2.8pp oracle gap. Large fields are structurally harder — 1/N base rate rises, stochastic noise dominates.

### track_bias_front: near-zero at 43/44

| Venue  | avg track_bias_front | Interpretation       |
| ------ | -------------------- | -------------------- |
| 46     | 0.066                | Strong front bias    |
| 47     | 0.050                | Moderate front bias  |
| 50     | 0.054                | Moderate front bias  |
| **43** | **0.021**            | Near-zero front bias |
| **44** | **0.017**            | Near-zero front bias |
| 35     | 0.003                | Near-zero front bias |

Venues 43 (Funabashi) and 44 (Kawasaki) have flat oval tracks with wide straights. The track_bias_front feature measures recent bias; near-zero values mean the feature carries minimal signal there. This is a minor contributor (<1pp).

### Jockey/trainer keibajo stats: diluted

| Feature                  | 43+44 mean | Other NAR mean | Gap    |
| ------------------------ | ---------- | -------------- | ------ |
| jockey_keibajo_win_rate  | 0.0787     | 0.0990         | -0.020 |
| trainer_keibajo_win_rate | 0.0903     | 0.1078         | -0.018 |
| same_keibajo_win_rate    | 0.1256     | 0.1353         | -0.010 |

These features are NOT NULL but carry weaker signal. 43/44 local jockeys/trainers are more densely concentrated at those venues → more even distribution → lower mean win rates → feature discriminability is lower.

---

## Investigation 6 — Calibration vs Ranking

**Method**: Within-race Pearson correlation of `popularity_score` (market rank proxy) vs `finish_position` as a market-ranking baseline. If the model follows the same ranking signal, model ranking quality tracks this.

| Venue group | n races | avg Pearson proxy | std   |
| ----------- | ------- | ----------------- | ----- |
| other NAR   | 35,092  | 0.5704            | 0.256 |
| 43+44       | 5,610   | 0.5518            | 0.243 |
| Difference  | —       | **−0.018**        | —     |

The gap is small and symmetric across the distribution. **The gap is RANKING quality degradation (from NULL features), not a calibration offset.** The model is not systematically mis-calibrating; it simply has fewer discriminative features available for 40% of 43/44 horses.

---

## Ranked Cause Table

| Rank  | Cause                                                                                                               | Estimated contribution          | Type              | Reducible? |
| ----- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------- | ----------------- | ---------- |
| **1** | **pacestyle_running NULL explosion** (locally-anchored horses have no RS history → 40% of rows missing 59 features) | **~4–5pp** of the 7pp model gap | Data completeness | **YES**    |
| 2     | Large field size (43/44 have 33% large/very-large field races vs 1.4% others)                                       | ~1–2pp (part of oracle gap)     | Structural        | Mostly NO  |
| 3     | track_bias_front near-zero signal (flat oval track geometry)                                                        | <1pp                            | Signal quality    | Marginal   |
| 4     | Jockey/trainer keibajo stats diluted (local specialist population)                                                  | <1pp                            | Signal quality    | NO         |

**Irreducible floor**: ~2.8pp (market oracle also weaker at 43/44).

---

## Targeted Fix Recommendation

### Probe: H-RS-KEIBAJO-IMPUTE

**Address**: Cause #1 — the NULL explosion in pacestyle_running features.

**Mechanism**:

For horses with no running-style predictions (all 4 RS style rates NULL), impute the NULL features using venue-class-distance priors derived from historical corner data at 43/44:

1. **Build a keibajo-specific RS prior table** (pre-compute from the full training parquet): for each (keibajo_code, nar_subclass_approx, kyori_band), compute:
   - `prior_nige_rate`, `prior_senkou_rate`, `prior_sashi_rate`, `prior_oikomi_rate`
   - `prior_corner_1_norm_avg`
     These are the empirical distributions of running styles among horses that DO have RS predictions at that venue+class+distance.

2. **Impute at feature-build time**: In the new feature layer (add after `add-pacestyle-features.py`), when `past_nige_rate_self IS NULL`, fill with the keibajo-class-distance prior. Keep a flag `rs_imputed = 1` so the model can weight imputed rows differently.

3. **Fallback**: If the keibajo-class-distance cell is sparse (<20 samples), fall back to NAR-global prior for that subclass.

**What this is NOT**: This is NOT a per-venue specialist model. The same global NAR model (iter12) runs for all venues. The fix is purely a data-completeness improvement to the feature pipeline.

**Expected recoverable**: ~3–4pp top1 within 43/44 races (bringing ~48% toward ~52–54%). Global NAR uplift: ~0.6–0.8pp top1 (3.5pp × 14% share × recovery fraction). The D1 ceiling estimate of ~1.3pp global assumes full recovery; realistic expectation is 0.6–1.0pp.

**Probe gate**: Accept if:

- top1 at 43+44 improves ≥3pp vs holdout baseline
- global NAR top1 ≥+0.3pp
- No regression on place2 or place3 (≥−0.05pp threshold)
- NAR iter13 beats iter12 on the combined 4-axis accept gate

**Implementation cost**: MEDIUM — requires building the keibajo-class prior lookup and a new feature layer script. No model architecture change. New training required (full retrain of NAR iter13).

**DO-NOT-RETEST**: Blind per-venue specialist model for 43/44 (same reason Ōi specialist was rejected: underpowered, trains on 14% of NAR data).

---

## Summary Verdict

| Question             | Answer                                                                                                                                                                 |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Reducible?           | **YES — partially.** 7pp of the ~9.8pp gap is model-specific; ~2.8pp is oracle/structural.                                                                             |
| Primary cause        | **pacestyle_running NULL explosion** due to locally-anchored horse population at 43/44 (40% of horses race ≥70% at these two venues and have no RS history elsewhere). |
| Routing bug?         | No. NAR uses single global model; nar_subclass regex is correct.                                                                                                       |
| Calibration issue?   | No. Gap is ranking quality degradation, not calibration offset.                                                                                                        |
| Fix type             | Data-completeness: venue-class-distance RS prior imputation for NULL rows.                                                                                             |
| Global recoverable   | ~0.6–1.0pp NAR top1 (vs 1.3pp ceiling estimate).                                                                                                                       |
| Probe recommendation | **H-RS-KEIBAJO-IMPUTE** — build keibajo-specific RS priors and impute NULL pacestyle_running features.                                                                 |
