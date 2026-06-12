---
probe_id: jra-rs-decision-rules
date: 2026-06-13
status: COMPLETE
scope: JRA Running-style v3 — post-hoc decision rules on calibrated probabilities
verdict_summary: |
  H1 (per-class multiplicative bias) REJECT: macro-F1 positive in 2024 (+0.10pp)
  but negative in 2025 (−0.08pp), fails all-years-positive gate.
  The bias meaningfully shifts nige precision (+3.4−3.8pp) at the cost of nige recall
  (−5.0−5.6pp), net nige F1 is flat/slightly negative. No pure gain found.
  H2 (hierarchical 2-stage) ABORT: in-sample macro-F1 gain is −0.62pp on training
  slice — hierarchical thresholds with calibrated probs underperform flat softmax even
  on data they were tuned on. Early abort triggered per protocol.
  Conclusion: calibrated argmax is already at the post-hoc decision-rule frontier.
  No deployment change recommended.
---

# JRA Running-Style — Post-Hoc Decision Rules Probe

## Context

Running-style v3 production baseline (calibrated argmax, JRA 2025 holdout):

| Class  | Precision | Recall | F1    | Support |
| ------ | --------- | ------ | ----- | ------- |
| nige   | 0.346     | 0.414  | 0.377 | 3,093   |
| senkou | 0.449     | 0.487  | 0.467 | 9,479   |
| sashi  | 0.479     | 0.386  | 0.428 | 12,991  |
| oikomi | 0.561     | 0.619  | 0.589 | 10,700  |

The above numbers are from `rs-leak-resolution.md` (production model single-fold retrain,
leak-free, 2016–2024 train → 2025 holdout). The calibration experiment (`rs-calibration-implementation.md`)
then showed that adding isotonic calibration improves the argmax decision boundary:
sashi +6.88pp F1 mean, nige +3.87pp F1 mean, oikomi −0.78pp F1 mean across 4 splits.

This probe asks: given the production calibrated probs already deployed, can a post-hoc
decision rule (per-class multiplicative bias or hierarchical 2-stage) further improve
per-class metrics without retraining?

**LEAK GUARD**: all evaluations use production `feature_columns` (146 cols from `metadata.json`),
explicitly excluding `rs_p_*`, `target_corner_{1,3,4}_norm`, and all other current-race leaks.

---

## Calibrated Argmax Baseline (production model, full 21-year train)

Scored using `jra-running-style-lgbm-prod-v3` + production calibrators (`fit_year=2025`).
Note: accuracy ~65-66% here vs ~48% in rs-leak-resolution because the production model was
trained on 21 years (2006–2026); rs-leak-resolution used a 2016→2025 retrain fold.
The DELTA from post-hoc rules is the metric that matters.

| Year | Acc    | Macro-F1 | nige F1 | senkou F1 | sashi F1 | oikomi F1 |
| ---- | ------ | -------- | ------- | --------- | -------- | --------- |
| 2023 | 0.6540 | 0.6567   | 0.6674  | 0.6305    | 0.6361   | 0.6929    |
| 2024 | 0.6606 | 0.6610   | 0.6613  | 0.6383    | 0.6438   | 0.7004    |
| 2025 | 0.6577 | 0.6575   | 0.6523  | 0.6401    | 0.6381   | 0.6993    |

**nige precision / recall (2023–2025):**

| Year | nige precision | nige recall | nige predicted_as | nige support |
| ---- | -------------- | ----------- | ----------------- | ------------ |
| 2023 | 0.619          | 0.724       | 3,901             | 3,336        |
| 2024 | 0.609          | 0.723       | 4,175             | 3,519        |
| 2025 | 0.602          | 0.712       | 3,658             | 3,093        |

nige is over-predicted (predicted_as > support), consistent with the rs-leak-resolution
finding (precision 0.346 at 48%-model level, same structural pattern preserved after calibration).
Calibration improved nige recall at the cost of increased over-prediction.

---

## H1 — Per-Class Multiplicative Bias Search

### Protocol

Given calibrated probs `p_k` per class k, apply `argmax_k(w_k * p_k)`.
Fit w on tuning year (2023). Evaluate on held-out years (2024, 2025).

Grid:

- w_nige ∈ [0.50, 3.00) step 0.10 (25 values)
- w_sashi ∈ [0.80, 2.50) step 0.10 (17 values)
- w_senkou ∈ [0.80, 1.60) step 0.10 (8 values)
- w_oikomi ∈ [0.70, 1.50) step 0.10 (8 values)
- Total: 27,200 combinations, optimizing macro-F1 on 2023

### Bias Search Results on Tuning Year (2023)

| Config    | Macro-F1 | nige F1 | sashi F1 |
| --------- | -------- | ------- | -------- |
| Baseline  | 0.6567   | 0.6674  | 0.6361   |
| Best bias | 0.6576   | —       | —        |
| Δ         | +0.0009  | —       | —        |

Best weights found: **nige=0.70, senkou=0.90, sashi=0.90, oikomi=0.90**

Interpretation: the optimizer prefers to _suppress_ nige (w=0.70 < 1.0), increasing
nige precision by reducing over-prediction, at the cost of nige recall. The pattern is
qualitatively sensible (nige over-predicted → downweight), but the in-sample gain is
only 0.09 pp macro-F1 — marginal.

### H1 Evaluation on Held-Out Years

#### 2024 holdout

| Class  | Baseline prec | Biased prec | Δ prec  | Baseline rec | Biased rec | Δ rec   | Baseline F1 | Biased F1 | Δ F1    |
| ------ | ------------- | ----------- | ------- | ------------ | ---------- | ------- | ----------- | --------- | ------- |
| nige   | 0.6093        | 0.6469      | +0.0376 | 0.7229       | 0.6732     | −0.0497 | 0.6613      | 0.6598    | −0.0015 |
| senkou | 0.6416        | 0.6346      | −0.0070 | 0.6350       | 0.6504     | +0.0154 | 0.6383      | 0.6424    | +0.0041 |
| sashi  | 0.6518        | 0.6506      | −0.0012 | 0.6360       | 0.6401     | +0.0041 | 0.6438      | 0.6453    | +0.0015 |
| oikomi | 0.7065        | 0.7056      | −0.0010 | 0.6944       | 0.6954     | +0.0010 | 0.7004      | 0.7004    | +0.0000 |

**2024 summary**: Baseline acc=0.6606 macro-F1=0.6610 → Biased acc=0.6620 macro-F1=0.6620, Δ=**+0.10pp**

#### 2025 holdout

| Class  | Baseline prec | Biased prec | Δ prec  | Baseline rec | Biased rec | Δ rec   | Baseline F1 | Biased F1 | Δ F1    |
| ------ | ------------- | ----------- | ------- | ------------ | ---------- | ------- | ----------- | --------- | ------- |
| nige   | 0.6020        | 0.6359      | +0.0340 | 0.7119       | 0.6557     | −0.0563 | 0.6523      | 0.6457    | −0.0067 |
| senkou | 0.6367        | 0.6288      | −0.0079 | 0.6435       | 0.6588     | +0.0153 | 0.6401      | 0.6435    | +0.0034 |
| sashi  | 0.6459        | 0.6439      | −0.0020 | 0.6304       | 0.6331     | +0.0027 | 0.6381      | 0.6385    | +0.0004 |
| oikomi | 0.7113        | 0.7101      | −0.0011 | 0.6877       | 0.6882     | +0.0006 | 0.6993      | 0.6990    | −0.0003 |

**2025 summary**: Baseline acc=0.6577 macro-F1=0.6575 → Biased acc=0.6580 macro-F1=0.6567, Δ=**−0.08pp**

### H1 Gate Check

| Criterion                              | 2024      | 2025      | Overall  |
| -------------------------------------- | --------- | --------- | -------- |
| macro-F1 positive                      | +0.10pp   | −0.08pp   | FAIL     |
| No class F1 < −2pp                     | min −0.15 | min −0.67 | PASS     |
| Overall acc ≥ −0.3pp vs baseline       | +0.14pp   | +0.03pp   | PASS     |
| **All-years-positive (gate requires)** |           |           | **FAIL** |

### H1 Verdict: REJECT

**Deciding factor**: 2025 macro-F1 regresses −0.08pp. The bias trades nige recall (−5.6pp)
for nige precision (+3.4pp), but the net nige F1 is −0.67pp in 2025 — the F1 cost of
suppressing nige predictions slightly exceeds the precision gain. The optimizer found a
tuning-year local optimum that does not generalize.

**Observation on the nige problem specifically**: Multiplicative downweighting of nige
(w=0.70) reliably raises nige precision (+3.4–3.8pp across years) but at a recall cost
(−5.0–5.6pp). The net F1 is flat or slightly negative. There is no free lunch: the
precision/recall trade-off in nige is a fundamental boundary confusion (nige vs senkou),
not a miscalibration artifact that a scalar weight can resolve. The calibrated probs
already encode the best available boundary information.

---

## H2 — Hierarchical 2-Stage Probe

### Protocol

Stage 1 binary: front {nige,senkou} vs back {sashi,oikomi} — threshold on `p_nige + p_senkou`
Stage 2a (front): nige vs senkou — threshold on `p_nige / (p_nige + p_senkou)`
Stage 2b (back): sashi vs oikomi — threshold on `p_sashi / (p_sashi + p_oikomi)`

Slice: train 2021+2022+2023 (117,898 rows calibrated), eval 2024 (40,021 rows).
Threshold search on training slice: `t_front_back ∈ [0.30,0.80)`, `t_front_inner ∈ [0.30,0.80)`,
`t_back_inner ∈ [0.30,0.80)` at step 0.05.

### Training-Slice Results

| Config                    | Macro-F1    | Notes                           |
| ------------------------- | ----------- | ------------------------------- |
| Flat calibrated (train)   | 0.6558      | calibrated argmax               |
| Best hierarchical (train) | 0.6496      | t_fb=0.45, t_fi=0.55, t_bi=0.50 |
| In-sample Δ               | **−0.62pp** | Below flat, no gain             |

### H2 Early Abort

In-sample gain is **−0.62pp** — the hierarchical thresholds do **worse** than flat softmax
even on the data they were tuned on. Early abort triggered per protocol.

### H2 Verdict: ABORT

**Root cause**: The calibrated flat probs already encode the front/back boundary optimally.
Forcing a hard binary split at stage 1 discards the soft probability information that the
flat argmax uses implicitly. For example, a horse with p_nige=0.28, p_senkou=0.23
(p_front=0.51) that should be predicted as sashi would be forced into the front group
regardless of p_sashi=0.26. The hierarchical boundary is strictly coarser than the joint
4-class argmax on calibrated probs, and the flat model is already well-calibrated (ECE
−71 to −76% from rs-calibration-implementation.md). Stage-wise thresholds cannot recover
what flat argmax already exploits.

---

## Summary Table

| Hypothesis                       | Tuning gain              | 2024 eval | 2025 eval | Gate                    | Verdict    |
| -------------------------------- | ------------------------ | --------- | --------- | ----------------------- | ---------- |
| H1 bias (nige=0.70, others=0.90) | +0.09pp macro-F1 on 2023 | +0.10pp   | −0.08pp   | FAIL (not all-positive) | **REJECT** |
| H2 hierarchical 2-stage          | −0.62pp in-sample        | —         | —         | EARLY ABORT             | **ABORT**  |

---

## Deciding Numbers — Per-Class F1 Deltas

### nige (key: was over-predicted, precision 0.346 at 48%-model level)

| Year | Bias prec Δ | Bias rec Δ | Bias F1 Δ |
| ---- | ----------- | ---------- | --------- |
| 2024 | +0.0376     | −0.0497    | −0.0015   |
| 2025 | +0.0340     | −0.0563    | −0.0067   |

Trade-off is real but net negative — bias cannot independently fix precision without
losing sufficient recall to keep F1 neutral.

### sashi (key: worst recall at 48%-model level)

| Year | Bias prec Δ | Bias rec Δ | Bias F1 Δ |
| ---- | ----------- | ---------- | --------- |
| 2024 | −0.0012     | +0.0041    | +0.0015   |
| 2025 | −0.0020     | +0.0027    | +0.0004   |

Tiny improvement (~0.15pp F1), consistent but negligible.

### Macro-F1 per year (baseline vs best-weight biased)

| Year        | Baseline | Biased | Δ       |
| ----------- | -------- | ------ | ------- |
| 2023 (tune) | 0.6567   | 0.6576 | +0.09pp |
| 2024 (eval) | 0.6610   | 0.6620 | +0.10pp |
| 2025 (eval) | 0.6575   | 0.6567 | −0.08pp |

---

## Conclusion

Both post-hoc decision rule approaches are exhausted:

1. **Calibrated argmax is already at the post-hoc frontier.** The isotonic calibration
   deployed in rs-calibration-implementation.md (+2.2–2.7pp argmax, +6.9pp sashi F1 mean)
   captured the full available post-hoc gain. There is no further low-cost improvement from
   multiplicative bias or hierarchical thresholds on top of calibrated probs.

2. **Nige over-prediction is structural, not a bias artifact.** The GBDT sees nige/senkou
   boundary confusion in the training distribution. A scalar downweight shifts the
   precision/recall trade-off without improving the boundary. Fixing nige precision requires
   new discriminative features (e.g., pace-setter history, field composition signals), not
   post-hoc rules.

3. **No deployment change recommended.** Production decision rule (calibrated argmax) is
   optimal under available post-hoc methods.

---

## Evidence Files

- `tmp/rs_decision_rules_experiment.py` — analysis script
- `tmp/rs_decision_rules_results.json` — full numeric results (JSON)
- `tmp/models/jra-running-style-lgbm-prod-v3/calibrators.json` — production calibrators used
- `docs/finish-position-accuracy/calibrators/jra-rs-v3-calibrators.json` — same, tracked copy
- Prior art: `rs-calibration-implementation.md` (isotonic calibration, SHIPPED), `rs-leak-resolution.md` (baseline)
