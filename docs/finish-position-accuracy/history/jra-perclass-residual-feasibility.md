# JRA Per-Class LightGBM LambdaRank Residual — Feasibility Probe

**Date:** 2026-06-12
**Verdict:** REJECT (all 6 classes)
**Deciding number:** fukusho_2p paired-bootstrap LB95 ≤ 0 on every class

## Hypothesis

The NAR iter36 LightGBM LambdaRank residual was adopted for NAR class C
(top1 +0.342pp, bootstrap LB95 = +0.0012). The JRA analogue has never been
tried. This probe tests whether a per-class LGB lambdarank residual on top
of the deployed JRA base (`iter14-jra-cb-pacestyle-course-v8`, CatBoost
YetiRank) beats production on any JRA class
(005 / 010 / 016 / 703 / 701 / other).

Architecture: identical to NAR H4 — iter14_score appended as 242nd feature,
WF folds train on chain-filtered data (e.g. class 016 trains on 016+010+005+703+701),
valid restricted to target class only, time-decay sample weights,
rank_blend of [iter14 baseline, lgb-residual] optimized by Optuna TPE.

## Phase A — Probe (Tuning Lift, Inner 2018-20 / Tuning 2021-22)

Probe gate: ≥2 of 5 metric axes (top1, place2, place3, top3_box, fukusho_2p)
positive on tuning split. Weights optimized on inner (2018-20, 100 Optuna trials).

| Class | Tuning Δ composite (pp) | Tuning Δ top1 (pp) | Positive axes | Lift detected |
| ----- | ----------------------: | -----------------: | ------------: | ------------- |
| 005   |                  +0.054 |             -0.269 |             2 | YES           |
| 010   |                  +0.424 |             +0.106 |             4 | YES           |
| 016   |                  +1.171 |             +0.937 |             4 | YES           |
| 703   |                  +1.049 |             +1.533 |             1 | NO            |
| 701   |                  +0.000 |             +0.000 |             0 | NO            |
| other |                  +3.300 |             +1.980 |             3 | YES           |

- **703**: strong top1 signal (+1.533pp) but only 1 positive axis (place2, place3,
  fukusho_2p all negative) → probe gate fails
- **701**: degenerate — residual_only top1 per-fold was 0.40-0.47 (below iter14),
  yielding zero net lift; Optuna collapses to baseline-only weights

Classes proceeding to Phase B: **005, 010, 016, other**.

## Phase B — Holdout Judge (2023-26)

Gate (strengthened vs NAR H4):

1. fukusho_2p paired-bootstrap LB95 > 0 (10k resamples, seed 42) — **sole place veto**
2. top1 OR fukusho_2p delta > 0
3. ≥2 of {top1, place2, place3, fukusho_2p, top3_box} positive
4. No veto axis (top1, fukusho_2p, top3_box) < −0.05pp
5. Holm correction across judged classes

LGB hyperparams (NAR H4 winner defaults applied to JRA, no per-class HPO):
`lr=0.137, num_leaves=15, max_depth=8, lambda_l2=0.1, feature_fraction=0.8,
bagging_fraction=0.8, n_estimators=300`.

| Class | n_hold races | Δ top1 (pp) | Δ place2 (pp) | Δ place3 (pp) | Δ fukusho_2p (pp) | Δ top3_box (pp) | Bootstrap LB95 | Verdict |
| ----- | -----------: | ----------: | ------------: | ------------: | ----------------: | --------------: | -------------: | ------- |
| 005   |        3,147 |      +0.763 |        +0.794 |        −0.254 |            +0.604 |          +0.095 |       −0.00318 | REJECT  |
| 010   |        1,734 |      −0.253 |        +0.063 |        +0.379 |            +0.253 |          +0.190 |       −0.00253 | REJECT  |
| 016   |        1,062 |      +2.201 |        +0.825 |        +1.651 |            +0.688 |          +1.100 |       −0.01376 | REJECT  |
| other |        1,487 |      +1.598 |        −0.376 |        −0.282 |            +0.094 |          +0.564 |       −0.01316 | REJECT  |

### Per-class binding reasons

- **005**: f2p bootstrap LB95 = −0.0032 ≤ 0 (sole deciding factor)
- **010**: top1 regresses −0.25pp (veto floor) + f2p bootstrap LB95 = −0.0025 ≤ 0
- **016**: most metrics positive (+2.20pp top1, 5/5 axes positive), but
  f2p bootstrap LB95 = −0.0138 ≤ 0 (high variance due to small class: 1,062 races)
- **other**: only 2/5 axes positive, f2p bootstrap LB95 = −0.0132 ≤ 0

Holm correction: p-values all ≥ 0.10 (min p = 0.1001 for class 005, Holm
adjusted 0.4004), no class survives α = 0.05.

## Diagnosis

### Why NAR class C adopted but JRA classes all reject

NAR C had 26,060 holdout races → narrow confidence interval → bootstrap LB95
turned positive (+0.0012) even with modest +0.342pp top1 gain.

JRA per-class holdout races are far smaller:

| Class | Holdout races | NAR C analogue |
| ----- | :-----------: | :------------: |
| 005   |     3,147     |     26,060     |
| 010   |     1,734     |       —        |
| 016   |     1,062     |       —        |
| other |     1,487     |       —        |

With 1,062–3,147 races the bootstrap variance is too wide. Class 016 shows
+2.20pp top1 and 5/5 axes positive, but the confidence interval straddles zero
(LB95 = −0.0138). This is a statistical power problem, not a model quality problem.

### What the raw deltas suggest

- **016**: +2.20pp top1, +1.65pp place3, +1.10pp top3_box — numerically substantial
  but statistically unreliable at 1,062 holdout races
- **005**: +0.76pp top1, +0.79pp place2, +0.60pp fukusho_2p — directionally positive
  on 3/5 axes, LB95 just barely negative (−0.0032)
- **703/701**: probe ABORT — no composite lift at all on tuning; no holdout touch

### Why the bootstrap fails where NAR C passes

The fukusho_2p metric operates per-race (binary: ≥2 predicted top-3 in actual
top-3). With 1,000–3,000 races, a 0.6–2.2pp nominal delta implies a per-race
hit-rate change of 0.006–0.022. With σ ≈ 0.48 for a Bernoulli with p ≈ 0.67,
the standard error of the mean is ≈ 0.48/√n. For n=1,062, SE ≈ 0.0147 — the
LB95 is mean_delta − 1.645 × SE ≈ 0.006 − 0.024 < 0. Doubling the sample
size to 2,000 would push LB95 to roughly −0.012, still negative. The 26k NAR C
races reduce SE to 0.003, making LB95 robustly positive.

### JRA class sizes are structurally constrained

JRA is a smaller total race pool (~3,500 races/year vs ~10,000 for NAR). Per-class
cuts (1,062–3,147 holdout races) are inherently narrower. The lambdarank residual
approach cannot be validated to the required statistical confidence on JRA
per-class buckets at current sample sizes — this is a data-volume structural
ceiling, not a model design flaw.

## Conclusion

**REJECT all JRA classes.** No deploy.

The JRA per-class LGB lambdarank residual shows credible numeric lift on 016 and
005 but cannot clear the fukusho_2p bootstrap LB95 > 0 gate due to insufficient
holdout race counts. The deciding factor in every case is bootstrap power, not
model quality.

**The deployed production models (iter14 + existing per-class ensembles iter25/26)
are unchanged.**

Scripts: `tmp/jra-perclass-residual/jra_residual_lib.py`,
`tmp/jra-perclass-residual/jra_lgb_residual_probe_judge.py`

Output: `tmp/jra-perclass-residual/probe_summary.json`,
`tmp/jra-perclass-residual/summary.json`, per-class `judge_<cls>.json`
