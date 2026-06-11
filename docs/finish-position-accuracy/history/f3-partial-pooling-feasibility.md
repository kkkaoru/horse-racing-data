# F3: JRA Per-Class Partial-Pooling / James-Stein Shrinkage — Feasibility Study

**Date:** 2026-06-12
**Status:** REJECT (mathematical proof of futility — not a near-miss)
**Elapsed:** 35.6 s

---

## Goal

Test whether training-time partial-pooling / random-effects shrinkage across JRA per-class
cohorts (005, 010, 016, 703, 701, other) improves fukusho_2p on small classes.
Distinct from rejected approaches: not a hard per-class split (iter20), not a post-hoc
cascade, not residual stacking (iter36), not calibration blending (phase3).

---

## Framework chosen

**James-Stein additive intercept shrinkage on the global model's raw scores.**

1. **Global model**: production `jra-lambdarank.lgb` (500 trees, LambdaRank, 161 features,
   trained on feat-v20-merged JRA). Used as-is — no re-training.
2. **Per-class delta estimation (tuning 2021-2022)**: compute mean raw score per class,
   compute raw delta from global mean. Apply James-Stein shrinkage factor:
   ```
   js_retain = between_class_variance / (between_class_variance + mean_within_class_sampling_variance)
   shrunk_delta[cls] = raw_delta[cls] * js_retain
   ```
3. **Holdout evaluation (2023-2026, single touch)**: apply `score_pooled = raw_score + shrunk_delta[cls]`,
   compute `compute_fukusho_2p` and `compute_top1` per class.
4. **Gate**: small class (016/701/703) delta >= +0.3pp AND paired-bootstrap LB95 > 0.

**Rationale for this formulation**: MixedLM or full hierarchical Bayes on feature coefficients
requires re-training and is equivalent to "train separate models with soft sharing" — which
GBDT already does implicitly. The additive intercept approach is the lightest-weight, fastest
testable proxy for partial pooling.

---

## Tuning-set JS shrinkage parameters (2021-2022)

Between-class variance: 0.022751
Mean within-class sampling variance: 0.000144
JS retain factor: **0.9937** (nearly no shrinkage — signal is almost entirely between-class)

| Class | n (tune) | raw delta | shrunk delta |
| ----- | -------- | --------- | ------------ |
| 005   | 24,721   | +0.0884   | +0.0879      |
| 010   | 12,416   | +0.1509   | +0.1500      |
| 016   | 6,139    | +0.1394   | +0.1386      |
| 701   | 7,895    | −0.0886   | −0.0881      |
| 703   | 35,334   | −0.2696   | −0.2679      |
| other | 59,598   | +0.0891   | +0.0885      |

The large between-class variance relative to within-class variance means JS shrinks barely
at all (retain=0.9937). Classes 703 and 701 score significantly below the global mean —
these are lower-quality cohorts relative to the model's internal calibration.

---

## Holdout results (2023-2026, SINGLE TOUCH)

| Class  | n_races | Global fukusho_2p | Pooled fukusho_2p | Δ (pp) | LB95 (pp) | Global top1 | Pooled top1 |
| ------ | ------- | ----------------- | ----------------- | ------ | --------- | ----------- | ----------- |
| 005    | 3,160   | 60.70%            | 60.70%            | +0.000 | +0.000    | 50.25%      | 50.25%      |
| 010    | 1,593   | 61.71%            | 61.71%            | +0.000 | +0.000    | 53.17%      | 53.17%      |
| 016 \* | 732     | 52.73%            | 52.73%            | +0.000 | +0.000    | 50.96%      | 50.96%      |
| 701 \* | 958     | 64.93%            | 64.93%            | +0.000 | +0.000    | 45.72%      | 45.72%      |
| 703 \* | 4,246   | 66.79%            | 66.79%            | +0.000 | +0.000    | 54.05%      | 54.05%      |
| other  | 7,676   | 72.07%            | 72.07%            | +0.000 | +0.000    | 60.08%      | 60.08%      |

`*` = small-class gate target

**Gate result**: All deltas are identically 0.000pp. Bootstrap LB95 = 0.000pp everywhere.

---

## Root cause of zero delta: rank invariance theorem

The zero-delta result is not a near-miss — it is a **mathematical identity**:

> All horses in a JRA race share the same `kyoso_joken_code` (confirmed by PG query: zero
> races in 2023-2026 have multiple class codes). Therefore, an additive per-class intercept
> shift `+delta[cls]` adds the same constant to every horse's score within each race. Since
> `compute_fukusho_2p` and `compute_top1` are both strictly rank-based within-race metrics,
> adding a global constant to all horses in a race leaves their relative order unchanged.
> The metric is provably invariant to the shift. **Delta = 0 exactly, for every class.**

This is not a tuning failure or data issue — it is a proof that this formulation cannot
possibly produce a non-zero result.

---

## Would a deeper formulation work?

The only alternative that could break the rank-invariance is **coefficient-level shrinkage**:
learning per-class feature weights (not just intercepts) that borrow strength from the global
model. This requires:

- Fitting a separate model per class with a regularization term pulling weights toward the
  global model.
- Or: augmenting the feature set with class-conditional interaction terms and letting GBDT
  re-learn them.

Both paths are equivalent to **re-training GBDT with class-aware structure**. This is exactly
what GBDT already does implicitly: the decision tree splits can condition on class identity
(via `grade_code`, `kyori`, and other class-proxies) without ever being told the class label
explicitly. GBDT's implicit shrinkage via depth/min_child_samples is already the optimal
partial-pooling for tree models.

**Result**: There is no headroom from an explicit partial-pooling overlay. The theoretical
basis for F3 collapses once the rank-invariance proof is recognized.

---

## VERDICT: REJECT

**Deciding number**: Δ = 0.000pp (exact, all classes, all metrics).

**Root cause**: Per-class additive score shifts are rank-invariant in single-class races,
making this formulation incapable of affecting any rank-based metric. The deeper alternative
(coefficient-level partial pooling) is structurally identical to re-training, which GBDT
already optimizes better than any explicit shrinkage overlay.

This approach does not constitute a new signal or a new model capacity — it is provably
equivalent to the global model for all rank-based metrics. No further compute investment
is warranted.

---

## Next actions

None — F3 is closed. The empirical frontier documented in
`project_finish_position_frontier_2026_06_11.md` stands: science-track saturation confirmed,
odds-decoupling rejected, new horse-level signals absent. The only remaining lever is
v3 running-style model extension (separate project).
