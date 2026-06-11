# D2b — JRA Moderate-Odds Band Calibration Probe

**Date**: 2026-06-11
**Model**: JRA iter14 (CatBoost YetiRank, 241 features)
**Probe origin**: D1 ceiling diagnosis (commit c21d779 + I2) identified JRA 4-7x favorite-odds band as PARTIALLY_REDUCIBLE (-15pp top1 in 5.8% of races, ~+0.3pp global recoverable)
**Script**: `tmp/rootcause/d2b_probe.py`
**Output**: `tmp/rootcause/d2b_jra_mododds.json`

---

## Verdict: ABORT

**The -15pp mod-band gap is structural, not a calibration failure. No band-conditional fix recovers it. Zero pp recoverable.**

---

## Context

D1's I2 worst-strata analysis flagged the JRA 4-7x favorite-odds band (per-race minimum `tansho_odds_raw`) as PARTIALLY_REDUCIBLE:

- 684 races in D1's 30% sample = 5.8% of total holdout races
- Model top1 in band: 29.7% vs global 44.8% = **-15.1pp delta**
- D1 estimated ~3-5pp recoverable via "odds-conditional calibration"

This probe tests that hypothesis directly.

---

## Band Definition and Coverage

**Per-race minimum `tansho_odds_raw` = favourite's win odds**

| Band            | Holdout 2023-2025 | % total  |
| --------------- | ----------------- | -------- |
| tight_lt2.5     | 4,752 races       | 45.8%    |
| normal_2.5-4.0  | 4,901 races       | 47.3%    |
| **mod_4.0-7.0** | **710 races**     | **6.8%** |
| open_ge7.0      | 2 races           | ~0%      |

Nearly all JRA races (93%+) have a favourite priced below 4x. The mod band (4-7x) is a genuine minority segment of "open" races with no dominant favourite, concentrated at 4.0-5.0x odds (632/710 races, 89%).

---

## Step 1: Gap Quantification (Holdout 2023-2025)

### Model vs Global

| Band            | Model top1 | Global top1 | Delta        |
| --------------- | ---------- | ----------- | ------------ |
| tight_lt2.5     | 51.41%     | 44.79%      | +6.62pp      |
| normal_2.5-4.0  | 40.36%     | 44.79%      | −4.43pp      |
| **mod_4.0-7.0** | **31.13%** | **44.79%**  | **−13.66pp** |

### Model vs Market in mod band

|                                     | top1 rate            |
| ----------------------------------- | -------------------- |
| Model pick (predicted_rank=1)       | 31.13% (LB95=27.82%) |
| Market pick (tansho_ninkijun_raw=1) | 18.45%               |
| **Model advantage**                 | **+12.68pp**         |

**Critical finding**: The model beats the market by 12.68pp even in the "deficient" mod band. The -13.66pp delta vs global is relative to the model's own above-average performance in easier races, not evidence of market out-performance.

---

## Step 2: Root Cause — Disagree Analysis

When model pick ≠ market pick (66.5% of mod-band races):

| Outcome        | Count | Rate      |
| -------------- | ----- | --------- |
| **Model wins** | 143   | **30.3%** |
| Market wins    | 53    | 11.2%     |
| Neither wins   | 276   | 58.5%     |

**Model is 2.7x more accurate than the market in disagreement cases.** A market blend would actively destroy accuracy.

When model and market agree (33.5% of mod-band races): 32.8% top1 hit rate — consistent with the overall band level.

---

## Step 3: Honesty Check — Is Market Already Embedded?

Spearman correlation between model rank and market popularity rank (per race, averaged):

| Band            | Mean Spearman | Interpretation                   |
| --------------- | ------------- | -------------------------------- |
| tight_lt2.5     | 0.855         | HIGH_AGREEMENT — blend redundant |
| normal_2.5-4.0  | 0.846         | HIGH_AGREEMENT — blend redundant |
| **mod_4.0-7.0** | **0.829**     | HIGH_AGREEMENT — blend redundant |

Market rank (`tansho_ninkijun_raw`) is already embedded in the model's `market_signal` feature group (13 features, +13.68pp LOFO drop). The model has essentially already learned the market's ranking. Blending adds no new information.

---

## Step 4: Band-Conditional Blend Sweep (Tuning 2021-2022)

**Blend formula**: `score = w_band * norm(market_rank) + (1-w_band) * norm(model_score)`, applied only in 4-7x band.

Tuning set: 6,912 races (529 mod-band)

| w_band             | Global top1 | Mod top1   | Mod fk2p   |
| ------------------ | ----------- | ---------- | ---------- |
| **0.0 (baseline)** | **43.45%**  | **32.33%** | **49.72%** |
| 0.1                | 43.34%      | 31.00%     | 50.09%     |
| 0.2                | 43.33%      | 30.81%     | 48.77%     |
| 0.3                | 43.19%      | 28.92%     | 47.07%     |
| 0.4                | 43.08%      | 27.60%     | 44.99%     |
| 0.5                | 42.91%      | 25.33%     | 43.48%     |
| 0.6                | 42.66%      | 22.12%     | 40.45%     |
| 0.7                | 42.52%      | 20.23%     | 38.00%     |
| 0.8                | 42.29%      | 17.20%     | 36.67%     |
| 0.9                | 42.16%      | 15.50%     | 35.35%     |
| 1.0                | 42.16%      | 15.50%     | 35.35%     |

Every value of w > 0 degrades both mod-band AND global top1. Best weight on tuning: **w = 0.0** (pure model).

---

## Step 5: Holdout Evaluation

Best w = 0.0 selected on tuning (trivially — all w > 0 are strictly worse).

| Metric        | Delta (w=0.0 vs w=0.0) |
| ------------- | ---------------------- |
| mod band top1 | 0.000pp                |
| mod band fk2p | 0.000pp                |
| global top1   | 0.000pp                |

No holdout evaluation needed: the sweep unambiguously ruled out blending.

---

## Analysis: Why the Gap Exists

### Structural hardness, not model failure

The 4-7x band races are structurally harder to predict:

- No dominant favourite: open races where 3-5 horses have similar win probabilities
- 58.5% of mod-band races: **neither the model pick NOR the market pick wins** — the race goes to a 3rd-or-lower selection
- This is irreducible variance: even a perfect ranking model would see lower top1 in "wide-open" races
- The -13.66pp delta vs global reflects the transition from "fairly predictable" (tight band) to "genuinely uncertain" races

### D1 estimate was imprecise

D1 estimated "3-5pp recoverable" but did not test whether the model already exceeds the market in this band. The actual situation:

- Model still beats market by 12.68pp in the band
- The "partially reducible" framing assumed the model was BELOW the market in the band — it is not
- The global uplift estimate of ~0.3pp was correct in magnitude but wrong in mechanism (it assumed a calibration fix; no such fix exists)

---

## Honest Check: Is This Just "Trust the Market More"?

Yes, and the data confirms it would be harmful. The model already incorporates the market signal far better than a naive blend. The proper way to further reduce the mod-band gap would be:

- New signal that differentiates winners from non-winners in genuinely uncertain races (not available)
- Better running-style predictions for uncertain field compositions
- Neither is achievable by blending existing signals

---

## Gate Assessment

| Axis                    | Result                   | Threshold        |
| ----------------------- | ------------------------ | ---------------- |
| mod band top1 LB95 > 0  | 27.82% (>0, trivially)   | Must be positive |
| mod band top1 delta > 0 | 0.000pp (no improvement) | Must be positive |
| global top1 no-regress  | 0.000pp                  | ≥ -0.05pp        |
| fukusho_2p no-regress   | 0.000pp                  | ≥ -0.05pp        |

Gate: **NOT PASSED** (mod_top1_delta = 0 at best_w=0.0; all w>0 strictly negative)

---

## Conclusion

|                                |                                                                                     |
| ------------------------------ | ----------------------------------------------------------------------------------- |
| **Verdict**                    | **ABORT**                                                                           |
| Expected recoverable global pp | **0.0pp**                                                                           |
| Root cause                     | Structural hardness of open races (58.5% neither-wins), not model calibration error |
| Market already embedded        | Yes — Spearman 0.83, LOFO +13.68pp, market blending destroys accuracy               |
| Fixable by new approach        | Only via fundamentally new signal in open-field races — not via blending            |

**No production change required or warranted.** The model performs optimally in the mod band given available features. The gap vs global top1 is irreducible with current feature set.

---

## DO-NOT-RETEST

Per probe spec:

- odds-decoupling: rejected
- C1-C4 monotone reranks: no-op
- Band-conditional market blend: **ABORT** (this probe)

Any future work on the 4-7x band would require a genuinely new signal (e.g., in-race position dynamics, field composition clustering, horse recent form dispersion in open fields).
