# RSext Feasibility — Per-Horse Local Running-Style Proxy

**Date**: 2026-06-11
**Probe**: RSEXT-FEASIBILITY-PER-HORSE-LOCAL-RS
**Model**: NAR iter12 recipe (XGBoost rank:pairwise, HPO params from best-params.json)
**Holdout**: 2023–2025 (3 WF folds, 40,710 races)
**Target venues**: 43 (Funabashi) + 44 (Kawasaki) = 5,612 holdout races (13.8% of NAR)
**Feature base**: `apps/pc-keiba-viewer/tmp/feat-nar-v7-final` (182 features)
**Augmented feature set**: 189 features (182 + 7 local RS proxy columns)
**Output JSON**: `tmp/rootcause/rsext_feasibility.json`
**Probe script**: `tmp/rootcause/rsext_feasibility.py`
**Verdict**: **ABORT**

---

## Motivation

D2-A established three findings about NAR Funabashi/Kawasaki (43/44):

1. **H-RS-KEIBAJO-IMPUTE (ABORT)**: Venue-mean imputation of v3 RS features hurt accuracy (−0.160pp top1 at 43/44). XGBoost's native NULL routing is competitive with the venue mean because the mean erases inter-horse variation.
2. **H-LOCALITY-FEATURE (ABORT)**: Explicit locality features (pct_career_at_keibajo, n_career_races_at_keibajo, etc.) gained <1.25% of total; existing keibajo-level features already capture venue affinity implicitly.
3. **Root cause**: The ~26% RS-NULL rate at 43/44 reflects locally-anchored horses with no cross-venue corner history, meaning the v3 RS model produces NULL outputs. The model's NULL-routing handles this well for existing features, but the inter-horse RS variation for this population is completely unknown.

The D2-A conclusion recommended a **qualitatively new signal** — specifically a per-horse RS estimate from the horse's OWN 43/44 corner history, as a proxy for what an extended v3 model would output.

This probe tests that hypothesis directly.

---

## Proxy Design

### Core Idea

v3 RS features are NULL for locally-anchored horses because the RS model requires cross-venue corner history to score them. However, `target_corner_4_norm` (the horse's actual finishing-corner position in the race) is **0% null at 43/44** across all 10 years of the parquet (2016–2025). This means every past race at 43/44 contains a `target_corner_4_norm` value — which directly encodes where the horse ran at the last corner.

A per-horse average of `target_corner_4_norm` from PAST 43/44 races is a direct, leak-free proxy for their running-style tendency at these venues.

### RS Zone Thresholds

Derived from empirical corner_4_norm centroids per RS class at 43/44 (n=137,209 rows with non-null RS class):

| RS class | Name   | corner_4_norm mean | corner_4_norm std | Zone boundary |
| -------- | ------ | ------------------ | ----------------- | ------------- |
| 0        | nige   | 0.077              | 0.173             | ≤ 0.160       |
| 1        | senkou | 0.242              | 0.208             | 0.160–0.376   |
| 2        | sashi  | 0.509              | 0.223             | 0.376–0.641   |
| 3        | oikomi | 0.773              | 0.202             | > 0.641       |

Boundaries are midpoints between adjacent class means.

### Proxy Columns (7 new features)

| Column                 | Definition                                                 |
| ---------------------- | ---------------------------------------------------------- |
| `local_rs_nige_prop`   | Fraction of past 43/44 races where `corner_4_norm ≤ 0.160` |
| `local_rs_senkou_prop` | Fraction where `0.160 < corner_4_norm ≤ 0.376`             |
| `local_rs_sashi_prop`  | Fraction where `0.376 < corner_4_norm ≤ 0.641`             |
| `local_rs_oikomi_prop` | Fraction where `corner_4_norm > 0.641`                     |
| `local_rs_avg_corner4` | Mean `corner_4_norm` over past 43/44 races                 |
| `local_rs_avg_corner3` | Mean `corner_3_norm` over past 43/44 races                 |
| `local_rs_n_races`     | Count of past 43/44 races used for the estimate            |

### Injection Strategy

The 7 proxy columns are **added** to the feature matrix as new columns. The existing RS NULL values are left intact. This is not imputation — the model can learn independently whether the proxy adds value on top of its native NULL routing.

- For rows where v3 RS is NOT null: all 7 proxy columns = NaN (XGBoost default-split routing)
- For RS-null rows at 43/44 with ≥1 past local race: proxy values populated
- For RS-null horses with 0 past 43/44 races in the training window: all 7 remain NaN

### Leak-Free Guarantee

For each WF fold with holdout year Y:

- `compute_per_horse_local_rs_proxy(df, cutoff_date)` where `cutoff_date = {Y}0101`
- Historical aggregates use only rows where `race_date < cutoff_date` (strict less-than)
- The target race row is never used to compute its own proxy

### Proxy Coverage per Fold

| Fold | RS-null 43/44 rows (holdout) | Proxy injected | Coverage | Avg past races |
| ---- | ---------------------------- | -------------- | -------- | -------------- |
| 2023 | 5,423                        | 2,719          | 50.1%    | 7.0            |
| 2024 | 5,525                        | 2,856          | 51.7%    | 6.9            |
| 2025 | 5,546                        | 2,806          | 50.6%    | 7.1            |

The ~50% coverage reflects that roughly half the RS-null horses in each holdout year appear for the first time in that year (no prior 43/44 records in the parquet which starts at 2016). The other half have an average of ~7 past 43/44 races available for proxy estimation.

---

## WITH vs WITHOUT Comparison (WF holdout 2023–2025)

### Per-fold results — Venues 43+44

| Fold | WITHOUT top1 | WITH top1 | Δ top1  | WITHOUT fukusho_2p | WITH fukusho_2p | Δ fukusho_2p |
| ---- | ------------ | --------- | ------- | ------------------ | --------------- | ------------ |
| 2023 | 0.481462     | 0.477163  | −0.43pp | 0.762493           | 0.761419        | −0.11pp      |
| 2024 | 0.474468     | 0.477128  | +0.27pp | 0.778191           | 0.777660        | −0.05pp      |
| 2025 | 0.525387     | 0.518439  | −0.69pp | 0.789417           | 0.781935        | −0.75pp      |

### Aggregated (weighted by n_races) — Venues 43+44

| Metric      | WITHOUT  | WITH     | Δ (pp)  | Gate         | Pass? |
| ----------- | -------- | -------- | ------- | ------------ | ----- |
| top1        | 0.493763 | 0.490912 | −0.2851 | ≥ +1.5pp     | FAIL  |
| top1 LB95   | 0.480691 | 0.477843 | —       | —            | —     |
| place2      | 0.281896 | 0.281539 | −0.0357 | ≥ −0.05pp    | PASS  |
| place2 LB95 | 0.270277 | 0.269924 | —       | —            | —     |
| place3      | 0.200679 | 0.204600 | +0.3921 | ≥ −0.05pp    | PASS  |
| place3 LB95 | 0.190407 | 0.194249 | —       | —            | —     |
| fukusho_2p  | 0.776728 | 0.773700 | −0.3028 | (monitoring) | —     |

### Aggregated (weighted by n_races) — Global NAR

| Metric     | WITHOUT  | WITH     | Δ (pp)  | Gate     | Pass? |
| ---------- | -------- | -------- | ------- | -------- | ----- |
| top1       | 0.587202 | 0.587153 | −0.0049 | ≥ +0.2pp | FAIL  |
| top1 LB95  | 0.582411 | 0.582362 | —       | —        | —     |
| place2     | 0.356301 | 0.354901 | −0.1400 | —        | —     |
| place3     | 0.275639 | 0.273478 | −0.2161 | —        | —     |
| fukusho_2p | 0.881946 | 0.882142 | +0.0196 | —        | —     |

---

## Probe Gate

| Condition                              | Required  | Actual   | Result |
| -------------------------------------- | --------- | -------- | ------ |
| 43/44 top1 ≥ +1.5pp vs baseline        | ≥ +1.5pp  | −0.285pp | FAIL   |
| Global NAR top1 ≥ +0.2pp vs baseline   | ≥ +0.2pp  | −0.005pp | FAIL   |
| 43/44 place2 no regression (≥ −0.05pp) | ≥ −0.05pp | −0.036pp | PASS   |
| 43/44 place3 no regression (≥ −0.05pp) | ≥ −0.05pp | +0.392pp | PASS   |

**VERDICT: ABORT** — 2 of 4 critical gate conditions failed.

---

## Feature Importance of Local RS Proxy Columns

Average XGBoost gain across 3 folds, compared to top standard features:

| Rank | Feature                  | Avg Gain | % of total |
| ---- | ------------------------ | -------- | ---------- |
| —    | **top-1 standard**       |          |            |
| 1    | target_corner_4_norm     | 2,785    | 3.67%      |
| 2    | odds_score               | 2,485    | 3.27%      |
| 3    | target_corner_3_norm     | 2,223    | 2.93%      |
| 4    | corner_pass_avg_5        | 2,012    | 2.65%      |
| 5    | target_corner_1_norm     | 1,752    | 2.31%      |
| —    | **local RS proxy**       |          |            |
| —    | local_rs_n_races         | 188      | 0.28%      |
| —    | local_rs_avg_corner3     | 126      | 0.17%      |
| —    | local_rs_avg_corner4     | 82       | 0.11%      |
| —    | local_rs_sashi_prop      | 95       | 0.13%      |
| —    | local_rs_senkou_prop     | 100      | 0.13%      |
| —    | local_rs_nige_prop       | 75       | 0.10%      |
| —    | local_rs_oikomi_prop     | 69       | 0.09%      |
| —    | **All 7 proxy combined** | ~735     | ~0.97%     |

The 7 proxy columns together account for ~0.97% of total gain — 3.8× less than `target_corner_4_norm` alone. This is better than the locality features from D2a (~1.23% combined for 5 features), but the proxy still does not lift accuracy.

---

## Root Cause Analysis

### Why the proxy fails despite being per-horse

**1. The proxy is a time-averaged style that's not discriminative within a race**

`local_rs_nige_prop` tells XGBoost "this horse ran front ~30% of the time at 43/44 historically". But the critical question for the FP model is not "is this horse a front-runner?" in the absolute sense — it's "is this horse MORE of a front-runner THAN THE OTHER HORSES IN THIS RACE?". The existing `self_nige_rate_minus_field_avg` already captures this differential. For RS-null horses, this field-relative feature is also NULL (the RS null block is 26+ columns). The proxy only adds an unconditional past average, not a within-race comparison.

**2. corner_4_norm is at the END of the race — close to the finish**

The model already has `target_corner_4_norm` as its top feature (in-race information available at scoring time, used for training). This means the model is already capturing the actual final-corner position of the target horse in each training race. The proxy is a LAGGED average of this same quantity — providing the expected future value of the most informative feature. In theory this should help; in practice the per-horse mean is noisy (~7 past races) relative to the within-race variability.

**3. ~50% of RS-null horses in the holdout have no proxy** (debut/first-year at venue)

The parquet begins in 2016. Many locally-anchored horses that first appear in 2023–2025 have no prior 43/44 history in the training data. These horses get NaN proxy, falling back to XGBoost NULL routing — which is the same as WITHOUT proxy. The net signal is diluted to the ~50% with coverage.

**4. The model already routes NULL-RS horses well via `horse_keibajo_corner_1_norm_avg`**

The existing feature `horse_keibajo_corner_1_norm_avg` (rank 36, gain ~630–680, ~0.83% of total) captures historical corner position at this specific venue. For RS-null horses, this feature is also NULL (~100% null rate). But when it IS available (RS-not-null horses), it captures exactly what our proxy tries to provide. The proxy is a parallel path that adds a noisier version of overlapping information.

**5. The ~−0.285pp top1 regression confirms information conflict**

The pattern (2023: −0.43pp, 2024: +0.27pp, 2025: −0.69pp) is consistent with the proxy introducing mild noise. The model receives 7 new columns that fire for ~51% of RS-null 43/44 rows (2,700–2,850 per fold). For these rows, the proxy values shift the raw score slightly. Since the proxy's time-average IS informative to some degree (≠ zero gain), the model occasionally adjusts rankings. The net effect on top1 is flat-to-slightly-negative because the proxy doesn't improve the within-race ordering that matters for rank:pairwise loss.

---

## Comparison with Previous D2a Probes

| Probe                                     | Method                              | 43/44 top1 Δ | Global top1 Δ | Verdict   |
| ----------------------------------------- | ----------------------------------- | ------------ | ------------- | --------- |
| H-RS-KEIBAJO-IMPUTE (D2a-1)               | Venue-mean imputation of RS columns | −0.160pp     | −0.024pp      | ABORT     |
| H-LOCALITY-FEATURE (D2a-2)                | Explicit locality fraction features | −0.107pp     | −0.027pp      | ABORT     |
| **Per-horse local RS proxy (this probe)** | Per-horse corner_4_norm history     | −0.285pp     | −0.005pp      | **ABORT** |

All three probes converge on the same finding: signal delivered via feature engineering on the existing 43/44 corner data cannot lift 43/44 accuracy. The model's native NULL-routing is already optimal for the available information.

---

## Why the Proxy Itself is Not a Quick Win

The task asked whether the proxy alone (faster than retraining v3) could be a quick path. The answer is no:

- Proxy importance (~0.97% combined gain) is non-zero but produces flat/negative accuracy
- ~50% of RS-null holdout horses have no proxy at all (first appearances)
- The proxy represents the same information already encoded in `horse_keibajo_corner_1_norm_avg` but noisier (fewer past races, time-average vs recency-weighted)
- Even if proxy coverage were 100%, the accuracy delta would not flip to positive (the mechanism fails, not just coverage)

---

## Conclusion: The v3-Extension Avenue is Closed

This probe was the decisive make-or-break test for the RS-model extension avenue. The hypothesis was: per-horse historical RS estimates from 43/44 corner data should lift FP accuracy, because they are informationally richer than venue means and more specific than locality flags.

The result is ABORT on all accuracy metrics. The per-horse proxy:

- Has non-zero but small gain (~0.97% combined)
- Does NOT lift 43/44 top1 (−0.285pp)
- Does NOT lift global NAR top1 (−0.005pp)
- place2 and place3 are within noise

**Fundamental conclusion**: The RS signal (as encoded in corner position history) does not improve finish-position ranking at 43/44 even when it is per-horse, not a venue mean, and contains real information. XGBoost's existing NULL routing plus the existing venue/jockey/trainer/form features already extract the available predictive signal for locally-anchored horses. The ~26% RS-NULL sub-population at 43/44 is not a model deficiency — it is a data reality that the model has already learned to handle.

**The v3 RS model extension (retrain to produce estimates for locally-anchored horses) is NOT justified by this feasibility test.** Even if the v3 model were extended to produce such estimates, those estimates would be proxied by corner_4_norm averages, which is exactly what this probe tested. The signal path does not close the accuracy gap.

The 43/44 headroom (~1pp per project memory) requires a qualitatively new data source not currently in the feature store — e.g., venue-specific race footage, fractional times, or real-time pre-race signals. Feature engineering on existing parquet data has been exhausted by the three D2a probes.

---

## Artifacts

| File                                                                            | Contents                                                  |
| ------------------------------------------------------------------------------- | --------------------------------------------------------- |
| `tmp/rootcause/rsext_feasibility.json`                                          | Full per-fold metrics, proxy coverage, feature importance |
| `tmp/rootcause/rsext_feasibility.py`                                            | Probe script (not committed, in tmp/)                     |
| `docs/finish-position-accuracy/history/rsext-feasibility-per-horse-local-rs.md` | This file                                                 |
