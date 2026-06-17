# JRA Finer-Cutoff + Ban-ei Window-Ablation Sweep

**Date:** 2026-06-17
**Scope:** JRA (cutoff ablation: 2014/2015/2016/2017 vs deployed 2013) + Ban-ei (window: 2007/2013/2016)
**Goal:** Determine whether a tighter JRA train-start than 2013 yields further gains; separately probe Ban-ei's optimal window.

---

## Context

The deployed JRA model (iter20-jra-cb-2013-v8) uses train-start=2013 (244 features, CatBoost YetiRank). The prior ablation (task #7) confirmed 2013+ beat 2006+ by +0.44pp top1 (LB95 +0.11pp). This sweep tests whether an even tighter cutoff (2014-2017) yields further gains.

For Ban-ei: the production model (banei-cb-v7-lineage-wf-21y) uses all available history (2007+). The feature store covers 2007-2026 (20 years, ~311k rows). This is a clean base-only ablation to check whether a 2013 or 2016 cutoff beats full history.

---

## Method

**Single OOT holdout:** train ≤ 2022, holdout 2023-2025. Paired-bootstrap LB95 (10k iterations, seed=42).

**JRA config:** CatBoost YetiRank, depth=8, lr=0.05, l2=3.0, iter=1000, od_wait=30, seed=2068, thread_count=6, time-decay weights (dropped by CatBoost YetiRank automatically). Features: 244 cols from `tmp/v8/feat-jra-v8-iter19-kohan3f-going`.

**Ban-ei config:** CatBoost YetiRank, depth=8, lr=0.05, l2=3.0, iter=300, od_wait=30, seed=2068, thread_count=6. Features: 129 total cols, ~119 numeric feature cols from `tmp/banei/features/` (10 layers: HORSE_CAREER, JOCKEY_TRAINER, PEDIGREE, RACE_CONTEXT, RECENT_FORM, MARKET_SIGNALS, BABA_AFFINITY, FUTAN_CLASS, GRADE_CAREER, HEAD_TO_HEAD).

**Gate (relaxed per USER):** top1 LB95 >= 0 AND no place regression beyond -0.05pp (place2 delta >= -0.05pp AND place3 delta >= -0.05pp).

---

## TEST A — JRA Finer Cutoff (2014-2017 vs 2013 baseline)

**Holdout:** 10,365 races, 141,523 rows (2023-2025).

### Raw metrics

| Window                        | train rows | top1 (%)    | place2 (%)  | place3 (%)  | top3_box (%) | fukusho_2p (%) | best_iter |
| ----------------------------- | ---------- | ----------- | ----------- | ----------- | ------------ | -------------- | --------- |
| **2013+ (baseline/deployed)** | 485,275    | **45.1712** | **23.3671** | **16.8066** | 15.6681      | **91.1240**    | 446       |
| 2014+                         | 452,350    | 44.9590     | 23.2610     | 16.4978     | 15.8032      | 91.0371        | 428       |
| 2015+                         | 417,879    | 45.1905     | 23.4250     | 16.3917     | 15.3883      | 90.9793        | 421       |
| 2016+                         | 381,453    | 45.3642     | 22.9908     | 16.9127     | 15.4752      | 91.0468        | 490       |
| 2017+                         | 342,765    | 44.9204     | 22.9812     | 16.4978     | 15.2629      | 91.0564        | 307       |

### Deltas vs 2013 baseline + LB95

| Challenger | top1 delta (pp) | top1 LB95 (pp) | place2 delta (pp) | place3 delta (pp) | Verdict            |
| ---------- | --------------- | -------------- | ----------------- | ----------------- | ------------------ |
| 2014+      | −0.2123         | −0.5210        | −0.1061           | −0.3087           | **NO_IMPROVEMENT** |
| 2015+      | +0.0193         | −0.3087        | +0.0579           | −0.4149           | **NO_IMPROVEMENT** |
| 2016+      | +0.1930         | −0.1447        | −0.3763           | +0.1061           | **NO_IMPROVEMENT** |
| 2017+      | −0.2508         | −0.5986        | −0.3859           | −0.3087           | **NO_IMPROVEMENT** |

All challengers fail the gate (top1 LB95 < 0 in all cases). 2016+ shows the highest raw top1 delta (+0.19pp) but its LB95 = −0.14pp and place2 regresses −0.38pp — it is not adoptable.

**VERDICT: NO_IMPROVEMENT. The deployed 2013 cutoff is optimal; no further tightening justified.**

Key pattern: as the window tightens beyond 2013, training data drops from 485k to 343k rows, best_iteration decreases, and LB95 intervals widen. The model is data-limited below 2013.

---

## TEST B — Ban-ei Window Ablation (2007 baseline vs 2013+ vs 2016+)

**Holdout:** 5,706 races (2023-2025). Feature count: 119 numeric features.
Note: Ban-ei data spans 2007-2026 (20 years). Earliest is 2007 (no pre-2007 data present).

### Raw metrics

| Window                          | train rows | top1 (%) | place2 (%) | place3 (%) | top3_box (%) | fukusho_2p (%) | best_iter |
| ------------------------------- | ---------- | -------- | ---------- | ---------- | ------------ | -------------- | --------- |
| **2007+ (baseline/production)** | 255,274    | 34.3014  | 20.6150    | 15.0721    | 12.0919      | 88.6484        | 48        |
| 2013+                           | 151,992    | 34.6811  | 20.6340    | 15.1860    | 12.0729      | 88.9332        | 79        |
| 2016+                           | 107,747    | 34.7380  | 20.8618    | 15.0152    | 12.0729      | 89.0281        | 53        |

### Deltas vs 2007 baseline + LB95

| Challenger | top1 delta (pp) | top1 LB95 (pp) | place2 delta (pp) | place3 delta (pp) | Verdict   |
| ---------- | --------------- | -------------- | ----------------- | ----------------- | --------- |
| 2013+      | +0.3797         | −0.0190        | +0.0190           | +0.1139           | **ABORT** |
| 2016+      | +0.4366         | +0.0190        | +0.2468           | −0.0569           | **ABORT** |

**2013+:** top1 LB95 = −0.019pp (just below zero). Gate fails on the LB95 threshold.

**2016+:** top1 LB95 = +0.019pp (passes LB95), place2 delta = +0.247pp (positive). But place3 delta = −0.0569pp which just barely fails the −0.05pp threshold. Borderline — the signal is real (top1 and place2 genuinely improved) but place3 regression exceeds the limit.

**VERDICT: ABORT. The full 2007+ history remains the optimal Ban-ei training window under strict gate criteria.**

Note on 2016+ borderline: If the orchestrator wishes to apply a relaxed gate (top1 LB95 > 0 AND max place regression ≤ −0.10pp), the 2016+ window would pass and is directly deployable (base-only, no ensemble weld). Place3 delta = −0.057pp is near-noise level given Ban-ei's small race count (~5,706 holdout races). This is left to the orchestrator's judgment.

---

## Summary

| Test                         | Verdict             | Action                                              |
| ---------------------------- | ------------------- | --------------------------------------------------- |
| TEST A: JRA cutoff 2014-2017 | NO_IMPROVEMENT      | Keep deployed 2013 cutoff; window is optimal        |
| TEST B: Ban-ei 2013+/2016+   | ABORT (strict gate) | Keep 2007+ history; 2016+ borderline (+0.44pp top1) |

**Best JRA cutoff vs 2013:** No challenger beats 2013. 2016+ is closest raw (+0.19pp top1) but LB95 = −0.14pp — noise.

**Ban-ei verdict:** ABORT under strict gate. 2016+ is the closest candidate (+0.44pp top1, LB95 +0.019pp, place2 +0.25pp) but place3 regresses −0.057pp (threshold = −0.05pp). Recommend orchestrator decides whether to apply relaxed gate given small holdout size.

No deploy actions taken. No production module edits.
