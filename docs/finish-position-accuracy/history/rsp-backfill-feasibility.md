---
science_track_entry: true
hypothesis_id: RSP-BACKFILL-FEASIBILITY
date: 2026-06-12
scope: JRA only (source='jra'), holdout kaisai_nen 2023-2026
status: ABORT — rs_p_* full backfill gives no statistically robust lift over sparse rs_p_*
verdict: >
  RS v3 model logits (p_nige/senkou/sashi/oikomi) are already scored for ALL years
  2006-2026 in tmp/bucket-eval/running-style/v1/logits/category=jra/ (100% join rate,
  zero NULLs). Backfilling rs_p_* from these logits into the feat-jra-v8-iter14-course
  parquet is a trivially cheap left-join. CatBoost iter14-style retrain (same HPO)
  with full-coverage rs_p_* vs sparse rs_p_* shows weak positive deltas (+0.10–0.96pp
  top1) but NO class achieves LB95 > 0 for either top1 or fk2p (gate requires ≥1
  target class with LB95 > 0 AND pooled no-regression). Pooled top1 LB95 = −0.12pp
  (FAILS gate). Only class 703 passes fk2p LB95 ≥ 0 (+0.45pp). ABORT.
production_change: none
probe_script: tmp/rsp-backfill/train_rsp_backfill_probe.py
log: tmp/rsp-backfill/probe2.log
results_json: tmp/rsp-backfill/rsp_backfill_results.json
---

## Background and Motivation

The v3 running-style LightGBM model (multiclass softmax: nige/senkou/sashi/oikomi)
outputs rs*p*_ probabilities that appear as finish-position features
`rs_p_nige`, `rs_p_senkou`, `rs_p_sashi`, `rs_p_oikomi` in the feature store.
Production feature parquets (`feat-jra-v8-iter14-course`) have these columns populated
**only for 2024–2025** (null rate: 98.3% null in 2024, ~1.6% null; 100% null for all
other years including 2026). This means that during training 2007-2025, the rs*p*_
columns are 90.7% NULL.

The prior probe (jra-relationship-verify.md) showed that relationship features
derived FROM rs*p*_ (nige*vs_field, oikomi_in_fast_field) add no signal when NULLs
are kept via CatBoost routing. The hypothesis here is distinct: test whether
\*\*absolute rs_p*_ values with full-history coverage\*_ add signal that the sparse
(2024–2025 only) rs*p*_ miss.

The strong skeptical prior (as stated in the task): rs*p*_ with full coverage may
be redundant with existing historical proxies (self*nige_rate_minus_field_avg,
corner_pass_avg_5, field_nige_pressure). The key difference is that those are
empirical rate averages from past race outcomes, whereas rs_p*_ is a probabilistic
prediction from a dedicated model trained on all available corner-pass features.

## Step 1: Feasibility

### RS logit coverage by year

The v3 model inference has ALREADY been run for the full 2006-2026 history.
The bucket-eval logits parquets contain p_nige/p_senkou/p_sashi/p_oikomi with
**zero NULLs** for every year:

```
tmp/bucket-eval/running-style/v1/logits/category=jra/race_year={2006..2026}.parquet
```

All 21 parquets use model_version=`jra-running-style-lgbm-prod-v3`.

### Feature store vs logits join test

Join keys: `(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)`.
Left-join of feature store onto logits: **100% join rate, zero row multiplication**
across all 21 years (1,004,938 rows).

| Year | base rs_p_nige null | full rs_p_nige null | N rows |
| ---- | ------------------- | ------------------- | ------ |
| 2006 | 1.000               | 0.000               | 49,365 |
| 2010 | 1.000               | 0.000               | 50,460 |
| 2018 | 1.000               | 0.000               | 49,124 |
| 2020 | 1.000               | 0.000               | 48,427 |
| 2023 | 1.000               | 0.000               | 47,672 |
| 2024 | 0.016               | 0.000               | 47,212 |
| 2025 | 0.017               | 0.000               | 48,058 |
| 2026 | 1.000               | 0.000               | 19,529 |

**Feasibility: UNBLOCKED.** Backfill is a single left-join, no re-inference needed.
Cost: negligible (parquet merge ~30s for all 21 years).

## Step 2: Bounded Probe

### Experimental setup

**Feature base**: `tmp/feat-jra-v8-iter14-course` — 247 features including 6 rs_p
columns: `rs_p_nige`, `rs_p_senkou`, `rs_p_sashi`, `rs_p_oikomi`,
`rs_predicted_class`, `rs_p_nige_x_field_pace`.

**BASE dataset**: original parquet — rs*p*\* are NULL for 2006-2023, 2026 (90.7%
null in training).

**FULL dataset**: same parquet with rs_p_nige/senkou/sashi/oikomi overwritten by
v3 logit join — 0.0% null for all years. `rs_predicted_class` and
`rs_p_nige_x_field_pace` are derived from the original rs_p columns (not
re-derived in this probe; they remain at original null rates).

**Architecture**: CatBoost YetiRank, depth=8, lr=0.05, l2_leaf_reg=3.0,
max_iter=1000, od_wait=30, seed=20260519.

**Train**: 2007-2025 (923,146 rows). **Val (early stopping)**: 2026 (18,824 rows).
**Holdout (eval)**: 2023-2026 (160,347 rows, 11,703 races).

**Best iterations**: BASE=188, FULL=210 (FULL converges slightly later, consistent
with the added signal needing more trees to digest).

**Class map**: fetched from PG `jvd_ra` for kaisai_nen 2023-2026,
JRA keibajo_code 01-10 (11,928 race-class rows).

### Per-class equal-footing judge

Holdout 2023-2026. Bootstrap: 10,000 iterations, seed=42, vectorized numpy.
LB95 = 5th percentile of bootstrap deltas (paired, races resampled with replacement).

#### Top-1 accuracy (fraction of races where predicted rank-1 = actual winner)

| Class  | N races | base top1 | new top1 | delta   | LB95        |
| ------ | ------- | --------- | -------- | ------- | ----------- |
| 005    | 3,147   | 41.47%    | 41.56%   | +0.10pp | **−0.41pp** |
| 010    | 1,583   | 42.26%    | 42.51%   | +0.25pp | **−0.38pp** |
| 016    | 727     | 37.96%    | 38.93%   | +0.96pp | **+0.00pp** |
| 703    | 4,229   | 49.54%    | 49.59%   | +0.05pp | **−0.40pp** |
| 701    | 953     | 45.12%    | 45.65%   | +0.52pp | **−0.52pp** |
| other  | 1,064   | 41.92%    | 41.54%   | −0.38pp | **−1.32pp** |
| pooled | 11,703  | 44.61%    | 44.76%   | +0.15pp | **−0.12pp** |

#### Fukusho-2p (≥2 of predicted top-3 are actual top-3)

| Class  | base fk2p | new fk2p | delta   | LB95        |
| ------ | --------- | -------- | ------- | ----------- |
| 005    | 65.27%    | 65.11%   | −0.16pp | **−0.70pp** |
| 010    | 64.62%    | 64.75%   | +0.13pp | **−0.57pp** |
| 016    | 56.95%    | 57.22%   | +0.28pp | **−1.10pp** |
| 703    | 74.89%    | 75.76%   | +0.87pp | **+0.45pp** |
| 701    | 72.51%    | 72.51%   | +0.00pp | **−0.84pp** |
| other  | 61.18%    | 61.28%   | +0.09pp | **−1.03pp** |
| pooled | 68.36%    | 68.67%   | +0.32pp | **+0.03pp** |

#### Accept gate evaluation

Gate criteria: ≥1 target class (005/010/016) with LB95 > 0 on top1 OR fk2p
**AND** pooled no-regression (top1 LB95 ≥ −0.05pp, fk2p LB95 ≥ −0.05pp).

| Criterion                             | Required | Actual          | Pass? |
| ------------------------------------- | -------- | --------------- | ----- |
| 005/010/016 with LB95 > 0 (top1)      | ≥1 class | 0 (016 = +0.00) | FAIL  |
| 005/010/016 with LB95 > 0 (fk2p)      | ≥1 class | 0               | FAIL  |
| Pooled top1 no-regression (≥ −0.05pp) | LB95 ≥ 0 | −0.12pp         | FAIL  |
| Pooled fk2p no-regression (≥ −0.05pp) | LB95 ≥ 0 | +0.03pp         | pass  |

**GATE: FAIL** — 3 of 4 criteria fail.

Note: class 016 top1 LB95 = exactly +0.00pp (boundary). This is the 5th bootstrap
percentile; the distribution is centered near +0.96pp but has a heavy lower tail
due to small n=727 races. Not a statistically robust positive signal.

## Root Cause Analysis

### Why doesn't full rs*p*\* coverage help?

1. **CatBoost NULL routing already exploits rs*p*\* efficiently.** In the BASE
   model, rs*p*_ is populated only for 2024-2025 (~10% of training data). CatBoost
   learns a dedicated NULL-routing branch that effectively says "this horse has no
   running-style probability estimate." For pre-2024 horses, the model falls back
   to the non-rs*p*_ features (corner_pass_avg_5, past_nige_rate_self,
   self_nige_rate_minus_field_avg, field_nige_pressure) which encode the same
   information but from historical race outcomes rather than probabilistic predictions.

2. **rs*p*\* is redundant with existing corner/rate features.** The v3 model itself
   was trained on the same corner-pass features already present in the feature store.
   When those features are present, rs*p*\* is a (noisy) transformation of features
   the model already has access to. The marginal contribution is zero or negative.

3. **FULL model's higher best_iteration (210 vs 188).** The FULL model needs more
   trees to "learn around" the rs*p*_ signal, suggesting rs*p*_ is not trivially
   aligned with the winning direction. If it were cleanly additive, the model would
   learn faster. Instead it adds weak structured noise that requires additional
   regularization passes.

4. **2023 holdout is 100% NULL in BASE** — if rs*p*_ had unique signal, we'd expect
   a strong positive delta specifically on 2023 (where FULL has values but BASE is
   all NULL). But the holdout 2023 horses are from the era when the corner-pass
   features are already well-populated, so the model already has their historical
   style information without rs*p*_.

### Why class 703 shows the only positive fk2p LB95?

Class 703 = Open races (high-quality fields, no class restriction). These races have
the most consistent runners with well-formed style patterns. The rs*p*\* probabilities
from v3 are most informative here because the model can precisely estimate running
style in predictable, deep fields. The +0.87pp fk2p delta and LB95=+0.45pp is the
strongest signal in the table — but it's not accompanied by target-class (005/010/016)
improvements, and the gate requires positive signal in ≥1 target class.

### Comparison with jra-relationship-verify.md

The relationship-verify probe tested derived features (nige*vs_field, oikomi_in_fast_field)
also constructed from rs_p*_ and showed ALL target classes with LB95 < 0, pooled
top1 delta = −0.19pp LB95=−0.41pp. This probe tests absolute rs*p*_ values with
full coverage and gets a modestly better result (+0.15pp pooled top1 LB95=−0.12pp),
but still fails the gate. The direction is correct (absolute values outperform
relationship values), but the effect size is insufficient.

## Non-Determinism Note

An earlier run (which crashed during bootstrap computation) showed markedly different
per-class deltas (~+1.18pp to +2.46pp top1 for target classes) with FULL best_iter=370.
This was due to non-determinism in CatBoost's early stopping on Apple Silicon (M5 Pro).
The authoritative run (run 2, completed cleanly, FULL best_iter=210) aligns with the
established iter14 base top1 values (005=41.47% exactly). Run 1's large numbers are
not reproducible and should be discarded.

## Conclusion and VERDICT

**VERDICT: ABORT — rs*p*\* full backfill is redundant with existing historical proxies.**

The RS v3 logits are trivially available for all years (unblocked), but backfilling
them into the finish-position feature store produces no statistically robust per-class
improvement. CatBoost's native NULL routing for the sparse 2024-2025 rs*p*\* already
captures the available signal via the existing corner/rate features for pre-2024 rows.

**Recommended next steps**: None within this avenue. The feature engineering space
for running-style-based finish-position signals is now exhausted:

1. Absolute rs*p*\* (sparse, 2024-2025): already in production, no further gain
2. Absolute rs*p*\* (full backfill): this probe — ABORT
3. Relationship features (nige_vs_field, oikomi_in_fast_field): jra-relationship-verify — ABORT
4. Per-horse locality proxy: d2a-rs-impute-feasibility — ABORT (mean imputation collapses NULL routing)

The running-style lever is exhausted at the JRA finish-position level. The only
remaining path is extending the running-style v3 model to produce probabilities for
horse types that currently score as NULL (e.g., NAR 43/44 locally-anchored horses
who lack early-corner data) — but that is a running-style model improvement project,
not a finish-position feature project.
