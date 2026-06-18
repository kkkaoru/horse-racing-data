---
probe: odds-independent-per-position-vector-correction
date: 2026-06-18
classes: 703 / 005 / 010 / 016 / 701 / other (all JRA)
method: V4 odds-free within-race-relative kNN → per-position affinity → post-hoc correction
status: ABORT — per-position affinity fails orthogonality gate; correction adds noise
---

# Odds-independent Per-Position Vector Correction — JRA

## USER Proposal

Build an odds-INDEPENDENT within-race-relative V4 vector per horse, retrieve K nearest
historical horse-races (leak-free), compute the DISTRIBUTION of their finishing positions —
a per-runner "position-affinity" vector [P(1st), P(2nd), P(3rd), P(4+)] — and apply
as a post-hoc CORRECTION to iter20's ranking.

The proposal rested on three prior findings:

1. V4 (odds-excluded, within-race z-score, NULL-aware, supervised-weighted) achieves
   partial-ρ = −0.11 with finish_norm on class 703, establishing orthogonal signal.
2. Odds-exclusion is mandatory (V1 with odds fails gate; V2 without odds recovers it).
3. Post-hoc correction proved to work better than feature injection (E-top2 precedent).

## Experiment Design

- **Baseline model**: iter20 per-class CatBoost (routed: 703-hpo, 005-hpo, 010, 016, 701, other)
- **Vector**: V4 odds-free within-race-relative, 18 curated features, supervised |ρ|-weights,
  median impute + NULL indicators. Excludes `odds_score`, `popularity_score`.
- **Train index**: 2013–2022 per class (703: 179,723 rows / 12,303 races)
- **kNN**: sklearn BallTree L2, k=50, fit once per class on train, query tune + blind
- **Affinity**: [P(1st), P(2nd), P(3rd), P(4+)] from neighbor finish-position distribution
- **Partial-ρ test**: each affinity[k] vs binary "finished exactly k-th", controlling for
  iter20_score + log(odds) via OLS on rank-transformed variables. Gate: |ρ| ≥ 0.08
- **Correction rule**: combined_score = iter20_score + λ₁·A₁ + λ₂·A₂ + λ₃·A₃,
  re-rank within race. Lambda tuned on 2023–2024 (1D sweep + refinement)
- **Blind gate**: 2025 only. ADOPT iff place2 OR place3 LB95 ≥ 0 with top1 delta ≥ −0.05pp

## Partial-ρ Orthogonality Results

Per-position affinity partial-ρ controlling for iter20_score + log(odds):

| Class | n (tune) | A[P1] ρ | A[P2] ρ | A[P3] ρ | A[P4+] ρ | Any clears gate? |
| ----- | -------- | ------- | ------- | ------- | -------- | ---------------- |
| 703   | 34,650   | +0.0497 | +0.0079 | +0.0103 | +0.0052  | **NO**           |
| 005   | 24,549   | +0.0336 | −0.0119 | −0.0045 | −0.0078  | **NO**           |
| 010   | 12,399   | +0.0301 | −0.0054 | +0.0086 | +0.0066  | **NO**           |
| 016   | 6,057    | +0.0460 | +0.0033 | +0.0024 | −0.0087  | **NO**           |
| 701   | 7,844    | +0.0472 | +0.0112 | −0.0043 | +0.0127  | **NO**           |
| other | 8,527    | +0.0542 | +0.0021 | −0.0012 | +0.0079  | **NO**           |

**All per-position affinity values fail the |ρ| ≥ 0.08 gate across all classes.**

Only P1 affinity approaches the gate marginally (max ρ ≈ +0.054 for "other"), but this
is consistent with the scalar V4 kNN score (1 − mean_finish_norm) which carried all the
signal concentrated into a single ranking signal. Decomposing into 4 position buckets
spreads the signal below detection threshold in each individual bucket.

## Correction Results — Blind 2025

### Per-class (lambda tuned on 2023–2024)

| Class | Blind races | top1 Δ | place2 Δ | place3 Δ | top3_box Δ | p2 LB95 | p3 LB95 |
| ----- | ----------- | ------ | -------- | -------- | ---------- | ------- | ------- |
| 703   | 1,252       | +0.559 | −0.160   | −0.399   | −0.240     | −1.038  | −1.038  |
| 005   | 909         | +0.770 | +0.330   | −0.770   | −0.330     | −0.660  | −1.760  |
| 010   | 464         | −0.431 | −0.216   | −0.216   | +0.432     | −2.371  | −2.371  |
| 016   | 216         | −1.852 | −0.926   | +0.926   | −0.926     | −3.704  | −0.926  |
| 701   | 304         | +0.000 | +0.329   | +0.000   | +0.329     | −0.658  | −0.987  |
| other | 310         | −1.290 | +0.000   | +0.968   | +0.968     | −1.935  | −1.290  |

All LB95 values are negative across all classes and all target metrics.

### Global (all classes combined, blind 2025)

| Metric     | Baseline | Corrected | Delta    | LB95     | Clears gate? |
| ---------- | -------- | --------- | -------- | -------- | ------------ |
| top1       | 41.476%  | 41.592%   | +0.116pp | −0.347pp | —            |
| place2     | 22.750%  | 22.721%   | −0.029pp | −0.608pp | **FAIL**     |
| place3     | 16.295%  | 16.064%   | −0.232pp | −0.753pp | **FAIL**     |
| top3_box   | 14.790%  | 14.530%   | −0.260pp | −0.579pp | FAIL         |
| fukusho_2p | 42.373%  | 42.084%   | −0.289pp | −0.753pp | FAIL         |

**VERDICT: ABORT** — no metric clears LB95 ≥ 0.

## Root Cause Analysis

### Why did V4 partial-ρ = −0.11 (from clean-vectorization-ablation.md) not transfer?

The prior result was for the **scalar kNN score** (1 − mean(finish_norm of 50 neighbors)):

- It aggregated all position information into a single ranking signal
- Partial-ρ of this scalar vs finish_norm = −0.11, gate cleared
- The scalar captures "how good these similar horses generally finished" as a continuous signal

The **per-position affinity** breaks this into 4 buckets [P1,P2,P3,P4+]:

- Each bucket captures a ~7% frequency signal (e.g., P1 ≈ 0.064–0.092 across classes)
- The GBDT already models "how likely a horse is to finish 1st/2nd/3rd" with 190+ features
- After controlling for iter20_score (which already encodes expected finishing position),
  the affinity buckets carry no additional discriminative power per position
- The orthogonal signal in the scalar kNN was capturing general horse quality relative to
  its race field — that signal, concentrated in the scalar, disperses below threshold when
  decomposed into 4 independent frequency estimates

### Why does the scalar clear the gate but not the bucketed version?

The scalar `1 − mean_finish_norm` uses continuous finish_norm (not binned positions), which
is a richer target. The partial-ρ of A[P1] vs "binary: did this horse finish 1st" has much
lower SNR than "scalar kNN score vs continuous finish_norm" because:

1. Binary targets have high variance (mostly 0, occasionally 1)
2. ~7% frequency → ~93% of target values are 0 → very weak correlation even if informative
3. Controls (iter20_score) already predict most of the variance in "finished 1st" binary

### Why did the correction fail despite the scalar passing the gate?

A previous experiment (`knn-feature-cheapfilter.md` or similar) established that even with
partial-ρ clearing the gate, injecting the kNN score as a GBDT feature failed (−0.72pp top1).
The same failure mode applies here: the scalar signal is already encoded by the GBDT's own
mechanisms, and post-hoc correction with the affinity (which is weaker than the scalar)
simply introduces misalignment noise.

The affinity correction's best found lambdas on tune often exploit training-set-specific
artifacts (e.g., class 010: λ = [0.0, −1.5, −0.5] found on tune, but fails on blind with
top1 −0.431pp; class 016: λ = [0.0, −0.5, −1.0] with tune p2+p3 +3.5pp but blind top1
−1.852pp). This is consistent with the signal being below detection threshold — any lambda
grid that produces positive tune metrics is overfitting to noise.

## Key Findings

1. **Per-position affinity fails the orthogonality gate** universally (all 6 classes,
   all 4 position buckets). Max ρ across all is +0.054 (for "other" class, P1), well below
   the |ρ| ≥ 0.08 bar.

2. **The V4 scalar signal (partial-ρ = −0.11) does not transfer to per-position buckets.**
   The information in V4 is about general horse quality relative to field (a ranking signal),
   not about a horse's preference for specific finishing positions.

3. **Post-hoc correction with sub-threshold affinity adds noise.** Lambda tuning on 2023–2024
   finds superficially positive values, but blind-2025 evaluation is consistently negative on
   place2 and place3 with LB95 well below 0 across all classes.

4. **The USER's "odds-independent" and "per-position" distinctions are both satisfied**, but
   the per-position bucketing is what kills the signal. The odds-exclusion + within-race
   transform (the V4 design) is confirmed necessary — but splitting into 4 affinity buckets
   diffuses the signal below actionable threshold.

## Verdict

**ABORT.** No per-position affinity corrects iter20 to achieve place2 LB95 ≥ 0 or
place3 LB95 ≥ 0 on blind 2025. The odds-free vector correction concept is sound but the
execution via per-position frequency distribution fails — the scalar kNN aggregation
(which cleared the gate) captures the useful component, and decomposing it into position
buckets loses signal below detection threshold.

The scalar V4 kNN score itself (1 − mean_finish_norm of 50 neighbors) remains the
cleanest expression of the orthogonal horse-quality signal. Previous experiments established
it fails feature injection into GBDT. Further exploration would require testing the scalar
as a post-hoc correction (not position-bucketed), but this is the territory of the already-
completed ensemble/correction experiments.

## Quality Gate

Probe-only result. No model code, no enforced-package files, no production features modified.
No `tmp/` files git-added. The only artifacts are:

- This documentation file
- `tmp/odds_independent_position_vector.py` (experiment script, not tracked)
- `tmp/oi_pos_vec_progress.log` (runtime log, not tracked)
- `tmp/odds_independent_position_vector_results.json` (results, not tracked)
