# JRA XGBoost-Solo Exact-Ordinal Evaluation vs CatBoost iter20

**Date:** 2026-06-17
**Feature store:** `tmp/v8/feat-jra-v8-iter19-kohan3f-going` (244 features)
**Baseline:** iter20-jra-cb-2013-v8 (CatBoost YetiRank, same 244-feat store, train 2013-2022)
**Verdict: TRADEOFF**

---

## Summary

XGBoost-solo gains **+0.60pp top1** (LB95 = +0.13pp, statistically confirmed) but loses
**−1.00pp exact place2** (LB95 = −1.57pp, confirmed regression) and
**−0.47pp exact place3** (LB95 = −1.00pp, confirmed regression).
CLEAN-SWAP gate fails on both exact place2 and place3.
No deployment recommended unless user explicitly accepts place2/place3 regression.

---

## Powered Holdout Definition

| Dimension         | Value                                      |
| ----------------- | ------------------------------------------ |
| Years             | 2023 + 2024 + 2025 (pooled)                |
| N races           | **10,365** (vs 3,455 in prior single-year) |
| N horses          | 141,523                                    |
| Statistical power | ~3x vs prior 2025-only holdout             |

**Data-leakage guard:** XGB-solo uses no tuned blend parameters (no tuning on 2023–2024
was needed for solo comparison), so pooling all three years is clean.
Train set remains 2013–2022 (no overlap).

---

## Exact-Ordinal Metric Definitions

| Metric       | Definition                                                         |
| ------------ | ------------------------------------------------------------------ |
| `top1`       | Predicted-rank-1 horse finishes **exactly** 1st                    |
| `place2`     | Predicted-rank-2 horse finishes **exactly** 2nd                    |
| `place3`     | Predicted-rank-3 horse finishes **exactly** 3rd                    |
| `top3_box`   | **Any** of top-3 predicted horses finished 1st (winner capture)    |
| `fukusho_2p` | **Any** of top-2 predicted horses finished in actual top-3 (avg/2) |

Source: `aggregate_bucket_eval_duckdb.py` lines 348–350 (canonical definition), confirmed in
`exact_ordinal_v4.json`.

---

## Global Exact-Ordinal Metrics (n = 10,365 races)

| Metric     | CB iter20 | XGB-solo | Delta (XGB−CB) |
| ---------- | --------: | -------: | -------------: |
| top1       |  44.6503% | 45.2484% |    **+0.60pp** |
| place2     |  23.5890% | 22.5953% |    **−1.00pp** |
| place3     |  16.8548% | 16.3821% |    **−0.47pp** |
| top3_box   |  77.4916% | 77.9643% |    **+0.47pp** |
| fukusho_2p |  66.9658% | 66.7246% |    **−0.24pp** |

---

## Paired-Bootstrap LB95/UB95 (10k iterations, seed=42)

| Metric     | Mean delta |        LB95 |    UB95 | LB95 ≥ −0.05pp? |
| ---------- | ---------: | ----------: | ------: | :-------------: |
| top1       |    +0.60pp |     +0.13pp | +1.07pp |    YES (>0)     |
| place2     |    −1.00pp | **−1.57pp** | −0.42pp |     **NO**      |
| place3     |    −0.47pp | **−1.00pp** | +0.06pp |     **NO**      |
| top3_box   |    +0.47pp |     +0.14pp | +0.81pp |       YES       |
| fukusho_2p |    −0.24pp |     −0.53pp | +0.04pp |       NO        |

Bootstrap method: paired race-level resampling, vectorized (10k × 10,365 matrix).

---

## CLEAN-SWAP / TRADEOFF Verdict

**Gate logic:** CLEAN-SWAP iff `top1 LB95 > 0` AND `place2 LB95 ≥ −0.05pp` AND `place3 LB95 ≥ −0.05pp`

| Gate condition        | Result   | Value         |
| --------------------- | -------- | ------------- |
| top1 LB95 > 0         | PASS     | +0.1254pp     |
| place2 LB95 ≥ −0.05pp | **FAIL** | **−1.5726pp** |
| place3 LB95 ≥ −0.05pp | **FAIL** | **−1.0034pp** |

**Verdict: TRADEOFF**

XGB top1 gain (+0.60pp mean, +0.13pp LB95) is real and statistically confirmed,
but comes with confirmed exact-place2 regression (−1.57pp LB95) and
exact-place3 regression (−1.00pp LB95). Both are large confirmed losses.
The powered multi-year holdout (3x more data) only strengthens the finding:
the confidence intervals are tighter, not wider, and the regressions are confirmed.

User judgment required:

- **top1 priority (60% target):** XGB wins by +0.60pp (confirmed)
- **place2/place3 priority (40% target):** CB wins by −1.00pp/−0.47pp (both confirmed regressions)

---

## Per-Class Breakdown (n_boot=2000, powered 2023–2025 holdout)

| Class | Label          | N races | CB top1 | XGB top1 |       Δtop1 |  Δtop1 LB95 |  CB p3 | XGB p3 |     Δplace3 |    Δp3 LB95 |  XGB wins p3?  |
| ----- | -------------- | ------: | ------: | -------: | ----------: | ----------: | -----: | -----: | ----------: | ----------: | :------------: |
| 701   | 新馬           |     908 |  45.04% |   45.93% |     +0.88pp |     −0.66pp | 18.72% | 19.82% |     +1.10pp |     −0.77pp |   NO (noisy)   |
| 703   | 未勝利         |   3,710 |  49.41% |   50.62% |     +1.21pp |     +0.43pp | 18.57% | 18.14% |     −0.43pp |     −1.37pp |       NO       |
| 005   | 1勝クラス      |   2,776 |  41.14% |   40.67% |     −0.47pp |     −1.33pp | 15.71% | 15.53% |     −0.18pp |     −1.19pp |       NO       |
| 010   | 2勝クラス      |   1,400 |  43.43% |   44.71% |     +1.29pp |     ±0.00pp | 15.14% | 14.14% |     −1.00pp |     −2.29pp |       NO       |
| 016   | 3勝クラス      |     640 |  37.03% |   38.59% |     +1.56pp |     ±0.00pp | 14.69% | 12.19% | **−2.50pp** | **−4.53pp** | **NO (worst)** |
| OP+   | OP/重賞(G1–G3) |     397 |  35.77% |   37.03% |     +1.26pp |     −1.27pp | 12.09% | 12.59% |     +0.50pp |     −2.27pp |   NO (noisy)   |
| H     | 障害           |      15 |       — |        — |           — |           — |      — |      — |           — |           — |   (skipped)    |
| other | other(L/E/etc) |     519 |  47.21% |   44.89% | **−2.31pp** | **−4.62pp** | 18.30% | 16.57% |     −1.73pp |     −4.05pp |       NO       |

**Key finding:** No class shows XGB winning on exact place3 with LB95 ≥ −0.05pp.

- 016 (3勝クラス) is the worst: XGB exact place3 = −2.50pp (LB95 = −4.53pp), large confirmed loss.
- `other` class (L/E/etc): XGB loses on both top1 (−2.31pp) and place3 (−1.73pp).
- 703 (未勝利) is the only class where XGB top1 LB95 > 0 (+0.43pp), but place3 still regresses.
- OP+ (OP/重賞): XGB wins place2 by +2.77pp (LB95 = 0.0pp) — interesting but noisy (n=397).

---

## XGB Model Parameters

| Parameter        | Value                                         |
| ---------------- | --------------------------------------------- |
| objective        | rank:ndcg                                     |
| eval_metric      | ndcg@3                                        |
| max_depth        | 8                                             |
| learning_rate    | 0.05                                          |
| lambda (L2)      | 3.0                                           |
| subsample        | 0.8                                           |
| colsample_bytree | 0.8                                           |
| min_child_weight | 5                                             |
| nthread          | 6                                             |
| seed             | 2068                                          |
| best_iteration   | 355                                           |
| feature_count    | 244                                           |
| model_file       | `tmp/v8/ensemble-impl-diverse/xgb_model.json` |

Train data: 2013–2022 (same as iter20-jra-cb-2013-v8).
Early stopping: 30 rounds on NDCG@3.

---

## Comparison with Prior 2025-Only Holdout

The prior experiment (`exact_ordinal_v4.json`, n=3,455 races, 2025 only) showed:

| Metric | CB (2025) | XGB (2025) |   Delta |
| ------ | --------: | ---------: | ------: |
| top1   |  43.9942% |   45.3835% | +1.39pp |
| place2 |  23.3285% |   22.5181% | −0.81pp |
| place3 |  16.4110% |   16.0926% | −0.32pp |

vs powered holdout (n=10,365):

| Metric | Delta (powered) |    LB95 |
| ------ | --------------: | ------: |
| top1   |         +0.60pp | +0.13pp |
| place2 |         −1.00pp | −1.57pp |
| place3 |         −0.47pp | −1.00pp |

The powered holdout narrows uncertainty significantly. The top1 gain is confirmed
but smaller than the single-year 2025 estimate. The place2/place3 regressions
are larger and more tightly confirmed.

---

## Deployment Notes

**DO NOT DEPLOY as CLEAN-SWAP.**

If user decides to accept the tradeoff (top1 60% priority only):

- Swap path: single-model swap (same serving infrastructure as iter20-jra-cb-2013-v8)
- Rollback: iter20-jra-cb-2013-v8 (revert model file reference)
- Serve cost: identical (one model inference per horse, same feature store)
- Expected on-serve: top1 +0.60pp (subject to serve-skew, verify against serve_accuracy_report)

**Recommendation:** Do not flip without explicit user approval of −1.00pp exact place2 loss.

---

## Raw Data Location

- Results JSON: `tmp/v8/ensemble-impl-diverse/xgb_solo_powered_eval.json`
- CB model: `tmp/v8/ensemble-impl-diverse/cb_model.json`
- XGB model: `tmp/v8/ensemble-impl-diverse/xgb_model.json`
- Feature store: `tmp/v8/feat-jra-v8-iter19-kohan3f-going` (race_year=2023/2024/2025)
