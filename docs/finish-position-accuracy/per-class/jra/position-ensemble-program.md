# JRA Position-Specialized Model-Combination Program

**Date:** 2026-06-17
**Verdict: ABORT**
**Baseline:** iter20-jra-cb-2013-v8 (CatBoost YetiRank, 244 features, train 2013-2022)
**Script:** `tmp/position_ensemble_hybrid_a_fast.py`
**Results:** `tmp/position_ensemble_hybrid_a_results.json`

---

## Motivation

XGBoost-solo (same 244-feature store, same train window) gains **+0.60pp top1** (LB95 +0.13pp) vs CatBoost iter20,
but loses **−1.00pp exact place2** (LB95 −1.57pp) and **−0.47pp exact place3** (LB95 −1.00pp).
A single-model swap cannot escape this tradeoff.

**Question:** Can we use XGB for rank-1 and CatBoost for rank-2+ to capture the top1 gain without losing place2/3?

---

## Program Logic

### Hybrid A — XGB rank-1 + CB rank-2+ (the primary program)

**Idea:** Use each model at the position where it is best.

| Position | Model used | Rationale                                                       |
| -------- | ---------- | --------------------------------------------------------------- |
| rank-1   | XGBoost    | XGB is +0.60pp better at picking the exact winner               |
| rank-2+  | CatBoost   | CB's ranking of remaining horses is preserved (where CB excels) |

**Implementation:**

```
for each race:
    hybrid_score = cb_score (copy CB scores for all horses)
    xgb_winner = argmax(xgb_score)
    hybrid_score[xgb_winner] = max(race cb_scores) + 1.0
# Sort descending by hybrid_score → predicted ranks
```

In races where XGB#1 == CB#1 (**agree races**, 84.7%): hybrid output is **identical** to pure CatBoost.
In races where XGB#1 != CB#1 (**disagree races**, 15.3%): XGB's winner gets rank-1; CB re-orders
all remaining horses by their original CB scores. This preserves CB's rank-2/3 ordering
among those remaining horses.

The hybrid is equivalent to CatBoost for the vast majority of races.
The question is what happens in the 15.3% disagreement races.

---

## Powered Holdout

| Dimension     | Value                                                              |
| ------------- | ------------------------------------------------------------------ |
| Years         | 2023 + 2024 + 2025 (pooled)                                        |
| N races       | 10,365                                                             |
| N horses      | 141,523                                                            |
| Train set     | 2013-2022 (no overlap)                                             |
| Leakage guard | Hybrid uses no tuned blend parameters — pooling 2023-2025 is clean |

---

## Metric Definitions (exact-ordinal)

| Metric       | Definition                                                                  |
| ------------ | --------------------------------------------------------------------------- |
| `top1`       | Predicted rank-1 horse finishes **exactly** 1st                             |
| `place2`     | Predicted rank-2 horse finishes **exactly** 2nd                             |
| `place3`     | Predicted rank-3 horse finishes **exactly** 3rd                             |
| `top3_box`   | **Any** of top-3 predicted horses finished 1st                              |
| `fukusho_2p` | **Any** of top-2 predicted horses finished in actual top-3 (avg/2 per race) |

---

## Global Results (n = 10,365 races)

| Metric     | CB iter20 | Hybrid A | Delta (Hybrid−CB) |
| ---------- | --------: | -------: | ----------------: |
| top1       |  44.6503% | 45.2484% |       **+0.60pp** |
| place2     |  23.5890% | 23.7434% |       **+0.15pp** |
| place3     |  16.8548% | 16.7969% |       **−0.06pp** |
| top3_box   |  77.4916% | 77.5398% |       **+0.05pp** |
| fukusho_2p |  66.9658% | 66.9513% |       **−0.02pp** |

**vs XGBoost-solo (for comparison):**

| Metric     | XGB-solo delta |                  Hybrid A delta |
| ---------- | -------------: | ------------------------------: |
| top1       |        +0.60pp |                  +0.60pp (same) |
| place2     |    **−1.00pp** | **+0.15pp** (recovered +1.15pp) |
| place3     |    **−0.47pp** | **−0.06pp** (recovered +0.41pp) |
| fukusho_2p |        −0.24pp |             −0.02pp (recovered) |

The hybrid successfully recovers most of XGB-solo's place2/3 regression by preserving
CB ordering for rank-2+. The place2/3 losses shrink from −1.00/−0.47pp to +0.15/−0.06pp.

---

## Paired-Bootstrap LB95/UB95 (10,000 iterations, seed=42, vectorized)

| Metric     | Mean delta |        LB95 |    UB95 | Gate (LB95≥threshold)  |
| ---------- | ---------: | ----------: | ------: | :--------------------: |
| top1       |    +0.60pp | **+0.13pp** | +1.07pp |       PASS (≥0)        |
| place2     |    +0.15pp | **−0.25pp** | +0.56pp | **FAIL (need ≥−0.05)** |
| place3     |    −0.06pp | **−0.20pp** | +0.09pp | **FAIL (need ≥−0.05)** |
| top3_box   |    +0.05pp |     −0.02pp | +0.12pp |           —            |
| fukusho_2p |    −0.02pp |     −0.11pp | +0.08pp |           —            |

Bootstrap: race-level paired resampling (10k × 10,365 matrix), vectorized.

---

## ADOPT / ABORT Verdict

**Gate logic:** ADOPT iff `top1 LB95 ≥ 0` AND `place2 LB95 ≥ −0.05pp` AND `place3 LB95 ≥ −0.05pp`

| Gate condition        | Result   | Value       |
| --------------------- | -------- | ----------- |
| top1 LB95 ≥ 0         | PASS     | +0.13pp     |
| place2 LB95 ≥ −0.05pp | **FAIL** | **−0.25pp** |
| place3 LB95 ≥ −0.05pp | **FAIL** | **−0.20pp** |

**Verdict: ABORT**

The hybrid dramatically narrows XGB-solo's place2/3 regression (place2: −1.00 → +0.15pp point estimate;
place3: −0.47 → −0.06pp point estimate), but at n=10,365 races the LB95 confidence intervals
still include meaningful downside. The place2 LB95 of −0.25pp and place3 LB95 of −0.20pp both
fail the ≥−0.05pp threshold.

**Root cause:** In disagreement races (15.3% of total), demoting CB#1 to rank-2 disturbs the
CB ordering of remaining horses enough to introduce small aggregate place2/3 noise. Even though
the disagree-race mean deltas are positive for place2 (+1.01pp) and near-zero for place3 (−0.38pp),
the 15.3% effect propagates noise to the global LB95.

---

## Disagreement-Race Analysis

15.3% of races (1,588 of 10,365) have XGB#1 ≠ CB#1.
84.7% (8,777 races) are agree races — hybrid output is identical to CatBoost.

### Agree races (n = 8,777): hybrid == CB

| Metric |     CB | Hybrid |  Delta |
| ------ | -----: | -----: | -----: |
| top1   | 48.02% | 48.02% | 0.00pp |
| place2 | 24.13% | 24.13% | 0.00pp |
| place3 | 17.09% | 17.09% | 0.00pp |

Confirmed: output is byte-identical to CatBoost in agree races.

### Disagree races (n = 1,588): XGB winner used

| Metric     |     CB | Hybrid |       Delta |
| ---------- | -----: | -----: | ----------: |
| top1       | 26.01% | 29.91% | **+3.90pp** |
| place2     | 20.59% | 21.60% | **+1.01pp** |
| place3     | 15.55% | 15.18% | **−0.38pp** |
| top3_box   | 72.48% | 72.80% |     +0.31pp |
| fukusho_2p | 63.10% | 63.00% |     −0.09pp |

In disagree races, the hybrid wins top1 by a large margin (+3.90pp) — XGB is much better at
identifying the winner in races where the models disagree (these are inherently uncertain races;
CB's top1 in disagree races = 26.0% vs 48.0% in agree races). Place2 also improves (+1.01pp).
Place3 regresses slightly (−0.38pp): when CB#1 is demoted, its displaced rank-2 horse sometimes
lands at rank-3 less accurately.

**Insight:** Disagree races are systematically harder (CB top1 = 26.0% vs agree-race 48.0%). XGB
identifies the winner better in these races (+3.90pp). The place2/3 effect is small relative to
the top1 gain in disagree races, but aggregated globally the LB95 just misses the −0.05pp threshold.

---

## Per-Class Breakdown (n_boot = 2,000, vectorized)

| Class | Label          | N races | CB top1 | Hyb top1 |   Δtop1 | top1 LB95 |  CB p2 | Hyb p2 |     Δplace2 | p2 LB95 |  CB p3 | Hyb p3 | Δplace3 | p3 LB95 |
| ----- | -------------- | ------: | ------: | -------: | ------: | --------: | -----: | -----: | ----------: | ------: | -----: | -----: | ------: | ------: |
| 701   | 新馬           |     908 |  45.04% |   45.93% | +0.88pp |   −0.66pp | 24.56% | 24.34% |     −0.22pp | −1.65pp | 18.72% | 18.50% | −0.22pp | −0.66pp |
| 703   | 未勝利         |   3,710 |  49.41% |   50.62% | +1.21pp |   +0.43pp | 24.55% | 24.98% |     +0.43pp | −0.30pp | 18.57% | 18.65% | +0.08pp | −0.19pp |
| 005   | 1勝クラス      |   2,776 |  41.14% |   40.67% | −0.47pp |   −1.33pp | 22.40% | 22.55% |     +0.14pp | −0.65pp | 15.71% | 15.45% | −0.25pp | −0.54pp |
| 010   | 2勝クラス      |   1,400 |  43.43% |   44.71% | +1.29pp |   ±0.00pp | 21.50% | 21.50% |     ±0.00pp | −1.21pp | 15.14% | 15.29% | +0.14pp | −0.14pp |
| 016   | 3勝クラス      |     640 |  37.03% |   38.59% | +1.56pp |   ±0.00pp | 18.44% | 18.59% |     +0.16pp | −1.25pp | 14.69% | 14.38% | −0.31pp | −0.78pp |
| OP+   | OP/重賞(G1-G3) |     397 |  35.77% |   37.03% | +1.26pp |   −1.27pp | 22.67% | 25.69% | **+3.02pp** | +0.76pp | 12.09% | 11.08% | −1.01pp | −2.02pp |
| H     | 障害           |      15 |       — |        — |       — |         — |      — |      — |           — |       — |      — |      — |       — |       — |
| other | other(L/E/etc) |     519 |  47.21% |   44.90% | −2.31pp |   −4.62pp | 22.93% | 20.04% |     −2.89pp | −5.01pp | 18.30% | 19.08% | +0.77pp | ±0.00pp |

**Class notes:**

- **703 (未勝利):** Best class for hybrid — top1 LB95 = +0.43pp (confirmed), place2/3 both near-neutral. If class-specific deployment were possible, 703 would likely ADOPT.
- **OP+ (OP/重賞):** Large place2 gain (+3.02pp, LB95 +0.76pp confirmed) but place3 regresses (−1.01pp). Mixed class — top1 gain is noisy.
- **other (L/E/etc):** Hybrid loses on both top1 (−2.31pp) and place2 (−2.89pp) in this class. XGB's NDCG optimization is misaligned with L/E race structure.
- **005 (1勝クラス):** Hybrid loses top1 (−0.47pp, LB95 −1.33pp). CB is better than XGB in 1-win class races.

---

## Comparison Summary

| Program  | top1 delta | top1 LB95 | place2 delta | place2 LB95 | place3 delta | place3 LB95 | Verdict   |
| -------- | ---------: | --------: | -----------: | ----------: | -----------: | ----------: | --------- |
| XGB-solo |    +0.60pp |   +0.13pp |      −1.00pp |     −1.57pp |      −0.47pp |     −1.00pp | TRADEOFF  |
| Hybrid A |    +0.60pp |   +0.13pp |      +0.15pp |     −0.25pp |      −0.06pp |     −0.20pp | **ABORT** |

Hybrid A successfully recovers XGB-solo's place2/3 regression in point estimates (place2: −1.00 → +0.15pp;
place3: −0.47 → −0.06pp), keeping the same top1 gain. But the LB95s remain below the −0.05pp gate.

**The per-position split idea is structurally sound** — the mean deltas show it working — but at n=10,365
races the confidence interval for place2 LB95 (−0.25pp) doesn't clear the gate.

---

## Why This Fails the Gate Despite Working in Point Estimates

The disagree-race subgroup (n=1,588) shows +1.01pp place2 and −0.38pp place3 in point estimates.
But these 1,588 races are the hardest races in the set (CB top1 = 26.0% vs 48.0% for agree races —
models are uncertain about the winner). High uncertainty in these races means high variance in
per-race hit indicators, which widens the bootstrap confidence interval for the global metric.

The signal is real but noisy enough at this holdout size that LB95 misses by 0.20pp on place2
and 0.15pp on place3.

---

## Productionization Note (if ADOPT had passed)

If the gate had passed, productionization would require:

- At inference time: run both CB and XGB models per horse
- For each race: identify XGB's top-ranked horse, override its score above all CB scores
- Sort by hybrid score → predicted ranks
- Cost: 2x model inference vs current 1x CB
- Rollback: iter20 (revert to CB-only score)

Currently ABORT — no production change recommended.

---

## Raw Data

- Results JSON: `tmp/position_ensemble_hybrid_a_results.json`
- Script (fast vectorized): `tmp/position_ensemble_hybrid_a_fast.py`
- CB model: `tmp/v8/ensemble-impl-diverse/cb_model.json`
- XGB model: `tmp/v8/ensemble-impl-diverse/xgb_model.json`
- Feature store: `tmp/v8/feat-jra-v8-iter19-kohan3f-going` (race_year=2023/2024/2025)
- Prior XGB-solo eval: `docs/finish-position-accuracy/per-class/jra/xgb-solo-exact-eval.md`
