# JRA Pooled-System Residual Re-Judge

**Date:** 2026-06-12
**Verdict:** REJECT
**Deciding number:** fukusho_2p paired-bootstrap LB95 = −0.002734 ≤ 0 (full holdout, n=11,703 races)

## Motivation

The per-class feasibility probe (commit 9d91508, `jra-perclass-residual-feasibility.md`)
REJECTed all six JRA classes individually due to bootstrap statistical power failure on
small per-class holdout pools (1,062–3,147 races). However, the point estimates were
strongly positive (016 +2.20pp top1 5/5 axes; other +1.60pp; 005 +0.76pp).

**This re-judge evaluates the SYSTEM as a whole.** The correct deployment decision unit
is the full-holdout system, not each tiny class independently. The system is PRE-SPECIFIED
by tuning data (residuals applied only to tuning-pass classes 005/010/016/other; 703/701
keep the production base unchanged). The full holdout (11,703 JRA races 2023-26) is
touched exactly once.

## System Design

**BASELINE:** The deployed production JRA system:

- 005 → `iter26-jra-cb-ensemble-005-v8` (7-member blend, weights from manifest)
- 010 → `iter25-jra-cb-ensemble-010-v8` (5-member blend)
- 016 → `iter26-jra-cb-ensemble-016-v8` (6-member blend)
- 703 → `iter26-jra-cb-ensemble-703-v8` (7-member blend)
- other → `iter25-jra-cb-ensemble-other-v8` (5-member blend)
- 701 → `iter14-jra-cb-pacestyle-course-v8` (no per-class ensemble)

**SYSTEM:** For tuning-pass classes (005/010/016/other), the production ensemble
is blended with the new LGB lambdarank residual using the probe's inner-optimized
weights. For 703/701, system == baseline (no residual applied).

| Class | Prod ensemble weight | LGB residual weight |
| ----- | -------------------: | ------------------: |
| 005   |                  0.2 |                 0.8 |
| 010   |                  0.5 |                 0.5 |
| 016   |                  0.2 |                 0.8 |
| other |                  0.2 |                 0.8 |
| 703   |                  1.0 |                 0.0 |
| 701   |                  1.0 |                 0.0 |

The LGB residual blend uses the same weights as the per-class feasibility probe
(optimized on inner split 2018-20, 200 Optuna TPE trials).

## Full-Holdout Judge (2023-26, n=11,703 races)

| Metric         | Baseline (pp) | System (pp) |     Δ (pp) |
| -------------- | ------------: | ----------: | ---------: |
| top1           |        45.083 |      45.467 |     +0.385 |
| place2         |        23.438 |      23.567 |     +0.128 |
| place3         |        17.115 |      17.064 |     −0.051 |
| top3_box       |        15.885 |      15.928 |     +0.043 |
| **fukusho_2p** |    **68.957** |  **69.008** | **+0.051** |

### Bootstrap

- fukusho_2p paired-bootstrap (10k resamples, seed 42, n=11,703 races)
- LB95 = **−0.002734** (sole place veto gate)
- Bootstrap mean delta = +0.000517 (i.e., nominally positive but CI straddles zero)
- One-sided p-value = 0.385

### Gate evaluation

| Condition                                  | Result                  |
| ------------------------------------------ | ----------------------- |
| fukusho_2p bootstrap LB95 > 0              | FAIL (LB95 = −0.002734) |
| top1 OR fukusho_2p point-positive          | PASS (top1 +0.385pp)    |
| ≥2 of 5 axes positive                      | PASS (4/5 positive)     |
| No veto axis (top1/f2p/top3_box) < −0.05pp | PASS (all ≥ −0.05pp)    |

**VERDICT: REJECT — sole deciding factor is fukusho_2p bootstrap LB95 ≤ 0.**

## Why the Pooled Test Still Fails

The pooled n=11,703 races improves statistical power substantially vs individual classes,
but the nominal fukusho_2p delta (+0.051pp) is extremely small. Even with the full
holdout, SE ≈ 0.48/√11703 ≈ 0.0044, so LB95 ≈ 0.0005 − 1.645×0.0044 ≈ −0.0067.
The observed LB95 = −0.002734 is consistent with this theoretical bound.

The core issue: the LGB residual lift on the per-class predictions (vs iter14) does
NOT directly transfer at the same magnitude when the baseline is the deployed per-class
ensembles (iter25/26), which already incorporate iter14 residuals + low-cap + relationship
features. The per-class ensembles absorb much of the signal the LGB residual was trying
to capture, reducing the net delta from ~0.6pp per class (vs iter14 baseline) to ~0.05pp
at the pooled system level.

## Per-Class Safety Analysis

No class shows catastrophic regression (threshold: ≤ −0.30pp on top1 or fukusho_2p).

| Class | n races | Δ top1 (pp) | Δ place2 (pp) | Δ place3 (pp) | Δ fukusho_2p (pp) | Δ top3_box (pp) | Flag        |
| ----- | ------: | ----------: | ------------: | ------------: | ----------------: | --------------: | ----------- |
| 005   |   3,147 |      +0.191 |        +0.540 |        −0.794 |            −0.032 |          −0.318 | —           |
| 010   |   1,583 |      +0.569 |         0.000 |        +0.632 |            +0.253 |          +0.126 | —           |
| 016   |     727 |      +1.926 |        +0.413 |        +1.651 |            +0.275 |          +0.963 | —           |
| 703   |   4,229 |       0.000 |         0.000 |         0.000 |             0.000 |           0.000 | (unchanged) |
| 701   |     953 |       0.000 |         0.000 |         0.000 |             0.000 |           0.000 | (unchanged) |
| other |   1,064 |      +1.504 |        −0.470 |        −0.282 |            +0.094 |          +0.564 | —           |

Class 016 continues to show the strongest point-positive signal (+1.93pp top1,
+1.65pp place3, 4/5 axes positive), but statistically unreliable at 727 races.

## Diagnosis

The pattern across both the per-class and pooled judges is consistent:

- **Point estimates are positive** (system beats production baseline on 4/5 axes at
  the full-holdout level)
- **Bootstrap LB95 stays negative** because the nominal fukusho_2p delta is too
  small (+0.051pp pooled) given the per-race variance (σ ≈ 0.48 Bernoulli)

The pooled test addresses the "per-class statistical power" objection but reveals
that the effective signal, once the production ensembles absorb the iter14 residual,
is insufficient to clear the bootstrap gate even at n=11,703.

## Conclusion

**REJECT the JRA per-class LGB lambdarank residual system.** The pooled re-judge
with proper statistical power (n=11,703 vs 727–3,147 per class) confirms that the
signal is too weak to clear the fukusho_2p bootstrap LB95 > 0 gate.

The deployed production models (iter25/26 per-class ensembles) are unchanged.

**Script:** `tmp/jra-pooled-residual-judge/pooled_system_judge.py`
**Result:** `tmp/jra-pooled-residual-judge/pooled_system_result.json`
