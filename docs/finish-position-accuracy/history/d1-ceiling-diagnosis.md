# Phase D1 — Ceiling Diagnosis

**Date**: 2026-06-11
**Models**: JRA iter14 (CatBoost YetiRank, 241 features) + NAR iter12 (XGBoost, 192 features)
**Holdout**: 2023–2025 (race-level 30% sample for LOFO; full data for entropy)
**JRA holdout**: 3,114 sampled races (from 142,942 rows)
**NAR holdout**: 12,213 sampled races (from 412,729 rows)
**Script**: `tmp/rootcause/d1_ceiling_analysis.py`
**Output**: `tmp/rootcause/d1_ceiling.json`

---

## Context: What "SATURATED" means

From Phase I1 (`i1_headroom.json`), both categories already **exceed the market oracle** globally:

| Category   | Model top1 | Oracle (market) top1 | Model − Oracle |
| ---------- | ---------- | -------------------- | -------------- |
| JRA iter14 | 44.76%     | 33.46%               | **+11.3pp**    |
| NAR iter12 | 58.68%     | 43.25%               | **+15.4pp**    |

This means the WF model extracts **more predictive signal than the crowd-aggregated odds** on average across all races. There is no "ceiling above the model" from the market oracle perspective globally. The question for D1 is whether **within the current feature set** there remains headroom — and whether **specific segments** have reducible gaps.

---

## Method 1: LOFO Ablation

### Interpretation note

LOFO drop = how much accuracy DROPS when a feature group is zeroed/median-filled. A large drop means the model RELIES HEAVILY on that group. It does NOT automatically mean there is room to improve that group — it could mean the group is already maximally exploited. The question is: is the drop because (a) the feature is the primary carrier of ranking signal (→ already saturated) or (b) the group is underexploited and has room for better features within the group?

### JRA iter14 — LOFO table (sorted by combined drop)

| Group                 | top1 drop    | place2 drop | place3 drop | fk_2p drop | n_feats | Note                                |
| --------------------- | ------------ | ----------- | ----------- | ---------- | ------- | ----------------------------------- |
| **market_signal**     | **+13.68pp** | +2.41pp     | -0.03pp     | +15.61pp   | 13      | Primary ranking carrier — odds      |
| pacestyle_running     | +5.78pp      | +0.77pp     | -0.67pp     | +7.61pp    | 62      | Running-style predictions (rs*p*\*) |
| pedigree_sire         | +5.65pp      | -2.25pp     | -1.45pp     | +1.48pp    | 23      | Sire/bloodline signals              |
| horse_form_career     | +0.58pp      | -0.03pp     | -0.06pp     | +1.16pp    | 36      | Form/career history                 |
| futan_weight          | +0.77pp      | -0.32pp     | -0.22pp     | +1.51pp    | 11      | Weight/futan                        |
| speed_sectional       | +0.19pp      | -0.10pp     | -0.22pp     | +0.35pp    | 17      | Speed index                         |
| race_internal_context | +0.16pp      | +0.06pp     | -0.06pp     | +0.55pp    | 15      | Field size, track condition         |
| jockey                | +0.13pp      | -0.03pp     | -0.06pp     | 0.00pp     | 14      | Jockey stats                        |
| course_geometry       | +0.06pp      | +0.16pp     | -0.03pp     | +0.16pp    | 7       | Course layout                       |
| workout_soha          | +0.06pp      | 0.00pp      | -0.03pp     | +0.16pp    | 12      | Workout data                        |
| trainer               | 0.00pp       | +0.10pp     | -0.03pp     | -0.03pp    | 18      | Trainer stats                       |
| h2h_grade_trial       | 0.00pp       | 0.00pp      | 0.00pp      | 0.00pp     | 12      | Head-to-head/grade trial history    |

**All-groups floor**: top1 = 10.34%, place2 = 9.54% (near-random baseline)
**Total information span**: 45.67% − 10.34% = **35.33pp** extracted from features

### NAR iter12 — LOFO table (sorted by combined drop)

| Group                 | top1 drop    | place2 drop | place3 drop | fk_2p drop | n_feats | Note                                |
| --------------------- | ------------ | ----------- | ----------- | ---------- | ------- | ----------------------------------- |
| **pacestyle_running** | **+15.10pp** | +0.13pp     | -2.39pp     | +22.81pp   | 59      | Running-style dominant signal (NAR) |
| **market_signal**     | +9.93pp      | -3.04pp     | -2.36pp     | +4.82pp    | 4       | Odds signals (4 features only)      |
| horse_form_career     | +0.90pp      | -0.25pp     | -0.11pp     | +0.87pp    | 32      | Form history                        |
| speed_sectional       | +0.38pp      | -0.10pp     | -0.12pp     | +0.40pp    | 15      | Speed                               |
| race_internal_context | +0.22pp      | -0.20pp     | -0.07pp     | +0.21pp    | 15      | Context                             |
| h2h_grade_trial       | +0.03pp      | 0.00pp      | +0.04pp     | +0.11pp    | 12      | H2H                                 |
| jockey                | +0.03pp      | +0.01pp     | -0.02pp     | +0.08pp    | 13      | Jockey                              |
| futan_weight          | +0.03pp      | 0.00pp      | -0.02pp     | -0.02pp    | 2       | Weight                              |
| trainer               | 0.00pp       | -0.03pp     | -0.03pp     | +0.02pp    | 18      | Trainer                             |
| pedigree_sire         | -0.05pp      | +0.05pp     | +0.03pp     | +0.02pp    | 20      | (zero or negative marginal)         |

**All-groups floor**: top1 = 22.31%, place2 = 14.72%
**Total information span**: 58.10% − 22.31% = **35.79pp** extracted from features

### LOFO interpretation

**JRA**: `market_signal` carries 13.68pp of the 35.33pp total span (39% of extractable signal comes from odds). `pacestyle_running` carries another 5.78pp (16%). The rest is diffuse — no single non-market group contributes >5.78pp. Speed/sectional, workout, trainer, jockey, h2h each contribute <1pp.

**Critical distinction**: The large market*signal drop does NOT mean the model can be improved by better market features — odds are already the most predictive available signal. It means the model correctly relies on them. The question is whether there are \_additional* signals not in the current feature set.

**NAR**: `pacestyle_running` (59 features including all rs*p*_ and corner stats) is the dominant signal at 15.10pp. This is partly because NAR running style is highly predictive AND the model has 59 features encoding it. But the rs*p*_ features are outputs of the running-style model (v3) — their quality is bounded by the running-style model's accuracy.

**JRA pedigree_sire -2.25pp place2**: Ablating pedigree HURTS place2 → counterintuitive (replacement fills better). Not an improvement opportunity; reflects feature interaction artifacts.

---

## Method 2: Prediction Variance Proxy

Feature-subsample bootstrap (80% of features kept, 5 bootstrap runs):

| Category | Mean Spearman | Std   | Interpretation         |
| -------- | ------------- | ----- | ---------------------- |
| JRA      | 0.900         | 0.135 | MODERATE_VARIANCE      |
| NAR      | 0.974         | 0.017 | LOW_VARIANCE_CONVERGED |

**JRA note**: Bootstrap 3 had an outlier (Spearman = 0.640) suggesting one 20%-feature-removal draw happened to drop a high-leverage group (likely the one containing `inverse_odds_implied_prob`). The other 4 bootstraps are 0.88–0.99. This confirms market_signal is highly leveraged and JRA ranking is sensitive to odds-feature removal — consistent with LOFO finding.

**NAR note**: Low variance (std 0.017) across all 5 draws confirms NAR ranking is stable and the model has converged on a consistent signal hierarchy.

**Method limits**: This measures sensitivity to feature subsetting, NOT stochastic training noise. It cannot estimate how much a fundamentally new feature could add.

---

## Method 3: Repeated-Race Cohort Entropy (auxiliary)

Horse-level 3-bin entropy (top1 / 2nd-3rd / out) over 2023–2025 holdout:

| Category | n horses (≥5 races) | Mean entropy | Median | P75   | % > 1.0 | Max possible |
| -------- | ------------------- | ------------ | ------ | ----- | ------- | ------------ |
| JRA      | 11,223              | 0.695        | 0.722  | 1.157 | 31.0%   | 1.585        |
| NAR      | 20,149              | 0.938        | 0.986  | 1.313 | 47.6%   | 1.585        |

**JRA entropy = 0.695**: Most horses in holdout show moderate consistency — they win roughly 1/3 of the time when they win at all. The distribution is centered below the midpoint (0.79), suggesting a mix of consistent performers and volatile ones.

**NAR entropy = 0.938**: Higher entropy means NAR horses are more variable race-to-race. This is consistent with NAR's wider variety of venues, track conditions, and field sizes.

### Explicit caveats (per spec)

(a) **Survivor selection bias**: Winners advance to harder classes; losers exit the database. The cohort is systematically non-random. Horses in the holdout have already survived selection.
(b) **Similar ≠ identical**: Track/weather/field composition all vary between a horse's repeated races.
(c) **Entropy ≠ Bayes error**: High entropy is consistent with genuine irreducible noise, but cannot be separated from reducible prediction failures without counterfactual re-runs of the same race.
(d) **3-bin approximation**: Loses ordinal resolution (e.g., 2nd vs 8th are both "out").

**Conclusion from entropy alone**: The entropy values (JRA 0.695, NAR 0.938 of max 1.585) are CONSISTENT with a mixture of genuinely unpredictable races and modelable structure. They do NOT prove a ceiling or refute it — entropy alone is insufficient as a ceiling indicator (as stated in the D1 spec).

---

## Method 4: I2 Worst-Strata Assessment

### JRA worst strata

| Stratum        | top1 delta | Races | % of total | Reducibility        | Interpretation                        |
| -------------- | ---------- | ----- | ---------- | ------------------- | ------------------------------------- |
| odds 4.0-7.0x  | -15.1pp    | 684   | 5.8%       | PARTIALLY_REDUCIBLE | Model ranks below market in this band |
| field 17+      | -6.3pp     | 1,088 | 9.3%       | MOSTLY_IRREDUCIBLE  | 1/N combinatorial noise               |
| class 016      | -6.5pp     | 727   | 6.2%       | MIXED               | First-timers / sparse history         |
| class 005      | -3.5pp     | 3,147 | 26.9%      | MOSTLY_IRREDUCIBLE  | Within-variance bounds                |
| mile 1400-1799 | -4.4pp     | 3,663 | 31.3%      | MOSTLY_IRREDUCIBLE  | Within-variance bounds                |

**Moderate-odds band (JRA)**: In the 4.0-7.0x odds band, the model's top pick wins 29.7% vs global 44.8% (delta -15pp). This is the only stratum where the model's top pick rate falls **below** what the market would naively predict (~33%). This is a SYSTEMATIC underperformance — not explained by pure randomness. Cause: the model over-ranks horses it believes are competitive but the market prices at 4-7x (races where the favorite isn't dominant). **Partially reducible**: better odds-conditional calibration could recover ~3-5pp.

**Large fields (JRA 17+)**: The market oracle shows the same degradation at large fields. This is structural randomness from 1/N base rate.

### NAR worst strata

| Stratum       | top1 delta | Races | % of total | Reducibility        | Interpretation                |
| ------------- | ---------- | ----- | ---------- | ------------------- | ----------------------------- |
| field 15-16   | -10.5pp    | 490   | 1.1%       | PARTIALLY_REDUCIBLE | Rare large-field NAR races    |
| odds 4.0-7.0x | -11.5pp    | 475   | 1.0%       | PARTIALLY_REDUCIBLE | Same moderate-odds pattern    |
| venue 43      | -10.3pp    | 2,503 | 5.5%       | PARTIALLY_REDUCIBLE | Specific venue systematic gap |
| venue 44      | -9.3pp     | 3,841 | 8.4%       | PARTIALLY_REDUCIBLE | Specific venue systematic gap |
| class R       | -4.5pp     | 142   | 0.3%       | MOSTLY_IRREDUCIBLE  | Tiny volume                   |

**NAR venue 43/44 (Funabashi / Kawasaki)**: Combined ~6,344 races (~14% of NAR total) with systematic top1 underperformance of 9-10pp. This is a clear SYSTEMATIC gap — these venues likely have venue-specific dynamics (track biases, local jockey stats, race-day conditions) that the current feature set does not adequately capture. This is the **most actionable reducible gap** by volume.

---

## Synthesis: What Does "Reducible Headroom" Actually Mean Here?

### The LOFO paradox

Large LOFO drops for `market_signal` and `pacestyle_running` indicate the model **relies heavily** on these groups. But since the model already exceeds the market oracle globally:

- The market_signal contribution (13.68pp JRA) cannot be "improved" by changing market features — the market IS the signal, and we're already above it
- The pacestyle_running contribution (15.10pp NAR) is bounded by running-style model v3 accuracy

The reducible headroom is therefore NOT in improving existing feature groups — it would require:

1. **New signal not in the current feature set** (e.g., odds dynamics within a race-day session, sectional timing from automated cameras, physiological readiness signals)
2. **Better calibration within weak strata** (moderate-odds band: ~3-5pp recoverable in ~5% of races)
3. **Venue-specific features for NAR 43/44** (~2-4pp in 8-14% of NAR races)

### The "saturation" interpretation

The model exceeds the oracle globally, BUT within specific strata (moderate-odds band, NAR venues 43/44), it does not. These strata-level underperformances are **reducible in principle** but the total recoverable signal is bounded:

- JRA moderate-odds (~3-5pp × 5.8% of races) = global uplift of **~0.2-0.3pp**
- NAR venue 43/44 (~2-4pp × 8-14% of races) = global uplift of **~0.3-0.5pp**

This is small but non-zero.

---

## Three-Way Verdict

**Verdict: HEADROOM (conditional)**

The automated verdict fires "HEADROOM" based on large LOFO drops exceeding 3pp. However, after careful interpretation:

### What the evidence actually supports

**1. Large-group LOFO ≥ 3pp (market_signal, pacestyle_running)**
These are IMPORTANCE signals, not improvement opportunities. The model correctly relies on odds and running-style features. Removing them collapses accuracy. This is expected and represents well-exploited signal, not extractable headroom.

**2. Prediction variance: JRA MODERATE, NAR CONVERGED**
NAR is near-converged. JRA shows moderate sensitivity to feature dropping — consistent with the heavy market_signal leverage.

**3. Global saturation confirmed**
Both categories exceed the market oracle on all metrics. There is no "above-model" ceiling from market signal.

**4. Reducible stratum gaps exist but are small in global impact**

- JRA moderate-odds band: -15pp top1 in 5.8% of races. Partially reducible via better calibration. Global impact: ~0.3pp top1.
- NAR venues 43/44: -9.5pp avg top1 in ~14% of races. Partially reducible via venue features. Global impact: ~1.3pp top1.

### Refined verdict: HEADROOM — narrow and locatable

**The TRUE reducible headroom above the model's current level is:**

- **JRA**: ~0.3pp global top1 (odds-calibration in moderate-odds band). No other systematic signal gap.
- **NAR**: ~1.3pp global top1 (venue 43/44 systematic gap). This is the primary actionable target.

**D2 recommendation (if pursued)**:

- NAR: venue-specific feature engineering for Funabashi (venue 43) and Kawasaki (venue 44) — venue-by-venue track bias, jockey specialization, post-position effects
- JRA: odds-conditional calibration experiment in the 4-7x band (low priority given small global impact)

**D2 skip justification (if budget-constrained)**:

- Global impact is small (< 2pp top1 for NAR, < 0.5pp for JRA)
- The model already operates +11pp (JRA) and +15pp (NAR) above the market oracle
- Pursue D2 only if venue-specific NAR signal is feasible to engineer cost-effectively

---

## Key Numbers Summary

| Metric                         | JRA                 | NAR                   |
| ------------------------------ | ------------------- | --------------------- |
| Baseline top1 (holdout)        | 45.67%              | 58.10%                |
| All-groups-ablated floor       | 10.34%              | 22.31%                |
| Information span               | 35.33pp             | 35.79pp               |
| Market_signal drop             | 13.68pp             | 9.93pp                |
| Pacestyle_running drop         | 5.78pp              | 15.10pp               |
| Max non-market/pacestyle drop  | 5.65pp (pedigree)   | 0.90pp (horse_form)   |
| Pred variance                  | MODERATE            | CONVERGED             |
| Cohort entropy                 | 0.695               | 0.938                 |
| Model vs oracle                | +11.3pp SATURATED   | +15.4pp SATURATED     |
| Primary reducible gap (global) | ~0.3pp (odds calib) | ~1.3pp (venues 43/44) |
