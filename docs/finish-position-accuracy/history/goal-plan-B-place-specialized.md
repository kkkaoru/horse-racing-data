# Plan B: Per-Position Multinomial Model + Hungarian Assignment

**Date:** 2026-06-17
**Scope:** JRA & NAR 2着/3着 exact-ordinal accuracy toward >40%, with top1 improving ~5%.
**User mandate:** train on 4th/5th/6th ordering; the prediction target AND assignment policy must
change — not just the relevance scheme.
**Distinct from:** Plan A (sub-4 graded-relevance full-system training — see goal-plan-A-graded-fullsystem.md).

---

## What Has Been Tried and Definitively Closed

The following approaches have been empirically rejected and must NOT be re-proposed.

| Approach                                                            | Verdict                                                                 | Key doc                                                         |
| ------------------------------------------------------------------- | ----------------------------------------------------------------------- | --------------------------------------------------------------- |
| LambdaRank place_weighted objective                                 | REJECT — top1 −11.77pp, place2 −7.72pp                                  | PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md §4.2                   |
| Binary place2/place3 specialist (standalone)                        | REJECT — place2 17.9% vs 27.4% baseline                                 | Same, §4.2                                                      |
| Hierarchical cascade (binary specialist)                            | REJECT — place2 −6.1–11.6pp across thresholds                           | Same, §4.3                                                      |
| Hierarchical cascade (transformer specialist)                       | REJECT — place2 −7.3pp                                                  | Same, §4.3                                                      |
| Conditional P(2nd\|≠1st) attention head (MLX)                       | REJECT — place2 −8.4pp; cascade −6.76pp                                 | Same, Phase B                                                   |
| Plackett-Luce re-rank (post-hoc)                                    | ZERO LIFT — mathematically identical to raw score ordering              | rootcause-i6-architecture.md §3.2                               |
| C4 joint placement re-rank (3P̂₁+2P̂₃, isotonic calibration)          | REJECT on real holdout — fuku LB95 negative JRA/NAR                     | phase3-calibration-c4-rerank.md                                 |
| Re-rank by P(place2) independently                                  | REJECT — catastrophic −10pp top1 (simulation + empirical)               | rootcause-i6-architecture.md §3.2                               |
| Asymmetric loss boosting place2 (r331/r341 variants)                | REJECT — top1 and place2 both degrade                                   | PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md §7-ter                 |
| Graded relevance sub-4 (scheme B/C/D) JRA — iter14 base             | REJECT WF (fold-inconsistent, f2p_lb95 −0.00357)                        | graded-relevance-experiments.md §4                              |
| Graded relevance sub-4 (scheme D) NAR — full-system judge           | REJECT — per-class residuals break on score shift                       | graded-relevance-experiments.md §7, nar-schemeD-deploy-judge.md |
| Exotic odds (umaren/wide/sanrenpuku) as features — JRA              | ABORT — partial ρ 0.02–0.03 on is_top3 (below gate 0.08)                | exotic-odds-place-signal.md; jra-fukusho-odds-probe.md          |
| Exotic odds — NAR                                                   | REJECT — 2024 ingest gap + fuku LB95 < 0                                | exotic-odds-place-verify.md                                     |
| Exotic odds — Ban-ei standalone (unified win model)                 | REJECT — top1 −0.234pp veto in unified model                            | exotic-odds-place-verify.md                                     |
| Fukusho odds (JRA) as place signal                                  | ABORT — ρ +0.024 on is_top3 < gate                                      | jra-fukusho-odds-probe.md                                       |
| JRA per-class LGB lambdarank residual (iter iters 20–36)            | REJECT all 6 classes                                                    | jra-perclass-residual-feasibility.md                            |
| Near-miss features (career_place2_rate, jockey_place2_rate etc.)    | +0.14pp place2 JRA only — sub-1pp lever cannot close +16pp gap          | PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md Phase A                |
| NAR sanrenpuku at serve time                                        | STOP — 100% NULL at 03:00 cron (serve/train mismatch)                   | sanrenpuku-serve-gate.md                                        |
| Residual ensembles + meta-learning (iters 15–18 for JRA; 30–36 NAR) | 4 consecutive REJECTs — v7-lineage saturation                           | project_v7_lineage_saturation_2026_06_04.md                     |
| NAR G-1+F1 combined retrain                                         | REJECT — serve-path regression −0.63pp after NULL routing was adapted   | project_nar_g1f1_combined_adopt_2026_06_12.md                   |
| Relationship / partial-ρ features (nige_vs_field, log_odds_z etc.)  | All ABORT/REJECT — GBDT already non-linearly captures via existing feat | project_relationship_perclass_investigation_2026_06_12.md       |

---

## Root-Cause Recap (I1–I6)

The key structural insight that motivates Plan B:

> The current pipeline outputs ONE score per horse, sorts descending, and reads off ordinal
> positions. This is optimal for NDCG / ranking / top1, but NOT for exact-ordinal hits at
> position k. The horse ranked #2 by overall quality is NOT necessarily the horse most likely
> to finish EXACTLY 2nd — yet we assign them rank 2.

Key empirical facts from I1/I6:

- P(finish=1) and P(finish=2) are both driven by horse quality → highly correlated.
- C4 re-rank confirmed: P̂₁ and P̂₃ from isotonic calibration of a single-score ranker are
  order-correlated monotone functions within each race → combining them with any weights
  produces the same rank ordering. (**Confirmed on real holdout, not just synthetic.**)
- "Exact-ordinal" place2/place3 is partially ill-posed: 47–62% of misses are adjacent-
  position swaps. Current baselines: JRA place2 ~22%, NAR place2 ~35%.
- Target >40% across JRA AND NAR is +18pp gap for JRA.

The C4 failure (phase3-calibration-c4-rerank.md, §Technical notes) gives the critical
constraint: re-ranking by calibrated single-score probs cannot lift exact-ordinal place2/3
because all P̂_k are monotone in the same underlying score.

**The only escape from this monotonicity trap is a model that outputs a DIFFERENT score
for each target position k — i.e., a genuine per-position probability matrix that is
NOT constrained to be rank-correlated across positions.** This is the Plan B formulation.

---

## Plan B Formulation: Per-Position Multinomial + Hungarian Assignment

### Why this is genuinely new (not a rehash)

This table distinguishes Plan B from every rejected approach:

| Rejected approach                           | Why it fails                                                              | Why Plan B differs                                                             |
| ------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Plackett-Luce re-rank (post-hoc)            | BT model is monotone in latent strength → same ordering as raw score      | Plan B trains a DIRECT P(k)-head, non-monotone across k per horse              |
| C4 joint placement re-rank (post-hoc)       | Calibrated probs are order-correlated (all from single score) — same rank | Plan B avoids calibrating a single score; it trains P(k) heads jointly         |
| Per-class HARD split (iter20)               | Single-score model split by sub-class → still monotone within class       | Plan B changes the PREDICTION TARGET, not the data partition                   |
| Post-hoc cascade (binary specialist)        | Cascade uses independent binary models that conflict with top1 ordering   | Plan B uses joint assignment (Hungarian) to maximize EXPECTED total exact hits |
| Residual ensembles / meta stacking          | Residual on a monotone score cannot break the rank correlation            | Plan B trains heads directly on ordinal labels, no upstream monotone score     |
| Per-place calibration/blend (phase3 REJECT) | Isotonic calibration of a single score → B==D (same ordering, confirmed)  | Plan B has multiple INDEPENDENT score dimensions (one per k); non-monotone     |

The decisive difference: **Plan B trains P(finish=k | horse, race) DIRECTLY for each
position k ∈ {1,...,7+}, producing a matrix P[horse, position] that is not constrained
to be rank-sorted along either axis.** The horse-position assignment is then solved as an
optimal assignment problem (Hungarian algorithm), which can produce non-monotone mappings
(e.g., horse ranked 3rd by P(win) could be assigned rank 2 if P(finish=2) for that horse
is unusually high).

---

## 1. Per-Position Probability Model

### 1.1 Prediction target

For each horse h in race r with N starters, predict:

```
P(finish_position = k | horse_h, race_r)   for k ∈ {1, 2, 3, 4, 5, 6, 7+}
```

This is a multinomial distribution over 7 position buckets. The 7+ bucket collapses all
positions ≥ 7 to avoid sparsity in large fields. For races with N ≤ 6, the 7+ bucket
has zero probability by construction (enforced via a field-size mask at inference time).

**Why 7 buckets, not N classes:**

- Horse count varies from 6 to 18 per race (JRA) or 6 to 16 (NAR).
- Using per-race variable N classes requires N-variable softmax — incompatible with fixed
  GBDT architecture.
- The key information for exact-ordinal is positions 1/2/3 (the ones we measure). Buckets
  4/5/6/7+ carry 4th/5th/6th ordering information (satisfying the user mandate) while
  keeping the output space tractable.
- An alternative is a fixed-width 16-class output and zeroing probabilities for positions
  > field_size at inference. This is valid but wastes modeling capacity. 7-bucket is the
  > recommended default; 4+/5+/6+/7+ can be merged if NAR small fields cause sparsity.

**This directly satisfies the user mandate:** the model explicitly trains on P(finish=4),
P(finish=5), P(finish=6), and P(finish=7+), learning the ordinal structure of positions
4–7 alongside positions 1–3. The 4th/5th/6th placement labels from historical data are
used as direct training targets, not just as epsilon-weight heuristics.

### 1.2 Label construction

For each (horse, race) row in the feature store:

```python
# finish_position is the actual finish ordinal (1-indexed, integer)
# Construct multinomial label: integer in {1..7} (7 = "finished 7th or later")
def position_bucket(finish_position: int) -> int:
    return min(finish_position, 7)
```

Leak safety: identical to the current pipeline. Feature store is built from races
completed BEFORE the prediction date. The finish_position label is the actual final
position of the horse in the TRAINING race — always available at training time (all rows
in the feature parquet represent completed races). No leak risk.

**Scratch handling:** horses that did not finish (DNF/scratch, finish_position = None or 0) are excluded from training rows — consistent with the current pipeline which also
excludes them from relevance labels.

### 1.3 Framework options

Three candidate frameworks, ordered by recommendation:

#### Option A (Recommended): LightGBM Multiclass

```python
# lgb.Dataset with label = position_bucket (0-indexed: 0..6 for positions 1..7+)
# Objective: multiclass (softmax)
# n_classes: 7
# num_leaves, max_depth: same as current NAR iter12 / JRA iter19 params as starting point
# Eval metric: multi_logloss (training monitor)
# Custom eval metric: top1_acc, place2_acc, place3_acc (from assignment — computed at race level)

import lightgbm as lgb
params = {
    "objective": "multiclass",
    "num_class": 7,
    "metric": "multi_logloss",
    "num_leaves": 127,  # iter12 NAR baseline
    "max_depth": -1,
    "learning_rate": 0.05,
    "n_estimators": 1000,
    "subsample": 0.618,
    "colsample_bytree": 0.8,
    "lambda_l2": 3.0,
    "random_state": 2068,
}
model = lgb.train(params, train_set, valid_sets=[val_set],
                  callbacks=[lgb.early_stopping(50)])
# Output: P[horse, 7] matrix per race via model.predict()
```

Advantages: reuses existing iter12/iter19 hyperparameter knowledge, multiclass is
natively supported, fast, well-calibrated softmax output.

#### Option B: CatBoost MultiClassification

```python
from catboost import CatBoostClassifier
model = CatBoostClassifier(
    loss_function="MultiClass",
    classes_count=7,
    depth=8,
    learning_rate=0.05,
    l2_leaf_reg=3.0,
    iterations=1000,
    eval_metric="Accuracy",
    random_seed=2068,
)
# Output: model.predict_proba() → P[horse, 7]
```

Advantage: aligns with JRA iter19 CatBoost base (same framework). But MultiClass is
a classifier, not a ranker — does not use within-race group structure during training.
This is a meaningful difference from Option A.

#### Option C: LightGBM Rank + Position-specific Output Heads (Ensemble)

Train 6 independent binary LGB models, one per position k ∈ {1,2,3,4,5,6}:

```python
# Model_k: binary lgb, label = (finish_position == k)
# P(finish=k) = model_k.predict() after per-race normalization: P_k /= sum(P_k)
```

This is a "one-vs-rest" decomposition. Advantage: each model optimizes directly for the
binary precision at position k using the existing binary ranking infrastructure.
Disadvantage: the 6 models are trained independently so they can conflict (P₁ and P₂
may not sum to ≤ 1 before normalization, requiring a secondary calibration step). This
increases implementation complexity and may introduce inconsistencies.

**Recommendation: Option A (LGB multiclass) for the initial experiment.** It is the
cleanest, most directly supported formulation. Option B as a fallback if JRA-specific
CatBoost infrastructure makes LGB inconvenient. Option C only if multiclass softmax
shows poor calibration on this task.

### 1.4 Field-size variability

The 7-bucket scheme handles fields of size 6–18 as follows:

- For a field of N horses, positions N+1 through 7+ have zero probability by construction
  (no horse can finish in a position that doesn't exist).
- At inference: after computing the P[horse, 7] matrix, zero out positions k > N for all
  horses, then renormalize each row: `P[h, k] = 0 for k > N; P[h] /= P[h].sum()`.
- During training: the label distribution naturally reflects N ≤ 7 (e.g. a 6-horse race
  has no 7th-place finisher). This is self-consistent because `position_bucket(k) = min(k, 7)`.
- For N > 7 (common in JRA): the 7+ bucket absorbs all positions 7–18. The model learns
  that the marginal probability of each 7+ bucket is ~ (N−6)/N for random horses, but the
  exact-ordinal metrics only measure positions 1–3, so the 7+ bucket is a "discard" class.

**Sparse positions consideration for NAR:** NAR fields are typically 6–12 horses.
Position 6 has non-trivial mass (10–16% of horses) but position 7 is sparse for
6-horse fields. Run a field-size distribution audit on the training data before finalizing
the bucket scheme. If races with N=6 are >10% of NAR data, consider a 6-bucket scheme
(positions 1–5 + 6+) to avoid label sparsity.

### 1.5 Feature store reuse

No new features are required. The per-position model trains on the SAME feature store
as the current production models:

- **JRA:** `feat-jra-v8-iter19-kohan3f-going` (244 features — iter19 deployed store)
- **NAR:** `feat-nar-v8-iter26-relationships` + `feat-nar-v8-iter12-exotic` overlay
  (173 base features — iter12 deployed store)

The feature set already includes market-signal features that encode WHERE in the field
each horse sits (odds_rank_in_race, popularity_rank_in_race, inverse_odds_market_share,
h2h_win_rate_vs_field). These field-relative features give the multinomial model
information about position-specific dynamics without requiring new data.

**No DuckDB rebuild needed.** The existing R2 parquet feature stores are used directly.
This saves ~40+ minutes of feature regeneration per category.

---

## 2. Optimal Assignment via Hungarian Algorithm

### 2.1 Why Hungarian, not monotone sort

Given the P[N×7] probability matrix for a race with N horses:

- **Monotone sort** assigns rank k to the horse with the highest P(finish=k). This is
  equivalent to `predicted_rank = argsort(-P[:, k])` per column — which cannot produce
  globally optimal exact hits because column-wise argmax doesn't enforce bijection.
- **Hungarian algorithm** solves `min sum_ij cost[i,j] * assignment[i,j]` subject to
  assignment being a bijection (one horse per position, one position per horse).

For exact-ordinal maximization, the cost matrix is:

```python
from scipy.optimize import linear_sum_assignment

def assign_positions(P: np.ndarray, field_size: int) -> np.ndarray:
    """
    P: shape (N, 7) — per-horse per-position probabilities, normalized, 0-indexed positions
    Returns: predicted_rank array (1-indexed) of length N
    """
    # Only assign positions 1..min(N, 7) (or N if N < 7)
    n_positions = min(field_size, 7)
    P_sub = P[:, :n_positions]   # N x n_positions (cols 0..n_positions-1 = positions 1..n_pos)

    # Hungarian minimizes cost; we want to maximize sum of P[i, assignment[i]]
    # cost = -P_sub
    row_ind, col_ind = linear_sum_assignment(-P_sub)

    predicted_rank = np.zeros(field_size, dtype=int)
    for horse_idx, position_idx in zip(row_ind, col_ind):
        predicted_rank[horse_idx] = position_idx + 1  # 1-indexed

    # Horses not assigned a position in 1..n_positions (if N > 7):
    # assign them ranks n_positions+1.. N by descending P(7+)
    unassigned = [i for i in range(field_size) if predicted_rank[i] == 0]
    p_7plus = P[unassigned, 6]  # column index 6 = position 7+
    order = np.argsort(-p_7plus)
    for rank_offset, horse_idx in enumerate(np.array(unassigned)[order]):
        predicted_rank[horse_idx] = n_positions + 1 + rank_offset

    return predicted_rank
```

**Crucial property:** the Hungarian assignment can produce a NON-MONOTONE mapping.
Example: horse A has P(1)=0.55, P(2)=0.10; horse B has P(1)=0.20, P(2)=0.60.
Monotone sort assigns rank 1 to A and rank 2 to B.
But what if horse C has P(1)=0.18, P(2)=0.25, P(3)=0.35?
Hungarian might assign: A→rank1, C→rank2, B→rank3 (if P(2)+P(3) for the combined
assignment is higher). This is the structural break from all prior approaches.

**This non-monotonicity is exactly why Hungarian can lift exact place2/3 beyond the
monotone-sort frontier — and why the C4 calibration failed (it was still monotone).**

### 2.2 Expected exact-hit improvement (theoretical)

Given a race where horse A has (P(1)=0.40, P(2)=0.15) and horse B has (P(1)=0.25, P(2)=0.50):

- Monotone sort → A is rank 1, B is rank 2.
  - E[exact_place2_hit] = P_B(2) = 0.50 (if B is assigned rank 2).
- Hungarian (2-horse version): assigns A→rank1 (P(1)=0.40), B→rank2 (P(2)=0.50).
  - Same as monotone here.
- For 3+ horses with crossing P(k) profiles, Hungarian can differ from monotone.

The asymptotic maximum achievable by Hungarian with perfect P[N×7] probabilities is the
oracle ceiling (being measured in Wave1 parallel agent — `goal-baseline-and-ceiling.md`).

### 2.3 Inference-time feasibility

Hungarian for N=16 is trivially fast (scipy.optimize.linear_sum_assignment is O(N³);
at N=16, this is ~4096 operations per race). For a 16-horse field: <1ms on CPU.
At serve time (Cloudflare Worker / Python predict script), the Hungarian step adds
negligible latency.

**Implementation note:** `scipy.optimize.linear_sum_assignment` is available in the
current Python environment (scipy is already a dependency). No new packages required.

---

## 3. Top1 Protection

### 3.1 The tension

Hungarian maximizes total expected exact hits across ALL positions. The natural Hungarian
formulation does NOT guarantee that the position-1 assignment goes to the horse with the
highest P(finish=1). It might assign rank 1 to a horse that has P(1)=0.30 if that
decision allows a globally better assignment for positions 2 and 3.

This would degrade top1 accuracy, violating the user requirement of top1 NOT degrading
and ideally +5%.

### 3.2 Resolution: Constrained Hungarian

Two policy options, both cleanly resolvable:

**Policy A (Recommended): Constrain rank-1 to argmax P(finish=1), then Hungarian
for positions 2..N.**

```python
def assign_positions_top1_protected(P: np.ndarray, field_size: int) -> np.ndarray:
    n_positions = min(field_size, 7)

    # Step 1: Fix rank-1 to the horse with highest P(finish=1)
    top1_horse = int(np.argmax(P[:, 0]))  # column 0 = position 1

    # Step 2: Build reduced problem: N-1 horses × positions 2..n_positions
    remaining_horses = [i for i in range(field_size) if i != top1_horse]
    if len(remaining_horses) == 0:
        return np.array([1])

    P_reduced = P[np.array(remaining_horses), 1:n_positions]  # (N-1) x (n_pos-1)

    # Step 3: Hungarian on reduced problem
    row_ind, col_ind = linear_sum_assignment(-P_reduced)

    predicted_rank = np.zeros(field_size, dtype=int)
    predicted_rank[top1_horse] = 1
    for r, c in zip(row_ind, col_ind):
        predicted_rank[remaining_horses[r]] = c + 2  # 1-indexed, starting from rank 2

    # Assign remaining (unassigned for 7+ bucket)
    unassigned = [i for i in range(field_size) if predicted_rank[i] == 0]
    p_7plus = P[unassigned, 6]
    order = np.argsort(-p_7plus)
    for rank_offset, horse_idx in enumerate(np.array(unassigned)[order]):
        predicted_rank[horse_idx] = n_positions + 1 + rank_offset

    return predicted_rank
```

This guarantees: top1 accuracy = accuracy of the underlying multinomial model's P(1)
argmax, which should be AT LEAST as good as the current ranker's top1 if the multinomial
model is well-trained (because top1 accuracy depends only on whether the model correctly
gives the highest P(finish=1) to the actual winner).

**Policy B:** Unconstrained Hungarian + top1 veto (if top1 degrades vs baseline, fall
back to Policy A). Use Policy A from the start to avoid evaluation complexity.

### 3.3 Will top1 improve +5%?

Top1 improvement in this formulation comes entirely from the quality of P(finish=1)
from the multinomial model — specifically whether the argmax of column 0 beats the
argmax of the current single-score ranker.

**The multinomial model has an advantage on top1 if and only if the multiclass
softmax objective produces a better-calibrated P(finish=1) than the pairwise-rank
objective.** The pairwise-rank objective (YetiRank, LambdaRank, rank:pairwise) is
directly optimized for ordering, while multiclass is optimized for per-class
log-likelihood. In theory, multiclass gives better probability estimates but potentially
worse pairwise ordering. In practice for 7-class problems with strong ordinal structure,
multiclass can match or exceed rankers on top-k accuracy.

**Empirical expectation:** Top1 improvement of +5% is optimistic as a guarantee.
The realistic range is −2pp to +3pp vs the current model depending on how well the
multiclass objective aligns with top1. The user's +5% target is better served by
Plan A (graded relevance sub-4) which directly optimizes the ranking objective, or
by HPO on the multinomial model specifically tuning P(1) precision.

**Gate policy:** Accept if top1 ≥ baseline − 0.05pp (no regression tolerance, same
veto floor as all experiments). The +5% top1 is a goal, not a hard requirement for
Plan B's ADOPT decision.

---

## 4. Distinctiveness from DO-NOT-RETEST List

### 4.1 vs Plackett-Luce re-rank (zero lift, I6 §3.2)

Plackett-Luce post-hoc re-ordering: takes the existing single-score ranker output,
constructs a BT strength from scores, computes expected rank. This is PROVABLY equivalent
to the original ordering because BT expected rank is a monotone function of the latent
score. **Plan B does not use a single score at all** — it trains an independent P(k)
for each position k, so the P(1) and P(2) columns of the output matrix need not be
rank-correlated. This is the structural difference: post-hoc PL cannot break
monotonicity; a trained multinomial CAN.

### 4.2 vs Per-class HARD split iter20

iter20 split the training data by race class (JRA class code) and trained a separate
ranker per class. It still trained a single-score ranker per class → still monotone
within each class → no improvement in exact-place assignment. **Plan B does not split
by class; it changes the PREDICTION TARGET from a scalar ranking score to a
per-position probability vector.**

### 4.3 vs Post-hoc cascade (hierarchical binary specialist)

The cascade (Phase B, 2026-05-20) trained a separate binary model `P(finish=2)` and used
it to re-rank after fixing rank 1. This model produced a single score (P(2)) that was then
used to sort. The problem: P(finish=2) is bell-shaped in quality (moderately good horses
have higher P(2) than the best or worst horses), causing catastrophic top1 regression when
re-ranked. **Plan B does NOT re-rank by P(k) for any single k** — it uses the full matrix
jointly via Hungarian assignment, which preserves top1 by construction (Policy A).

### 4.4 vs Residual ensembles (iter30–36)

Residual ensembles stacked a second model on top of the first model's score. Both the
base and residual output a SCALAR ranking score. The blend is still a weighted sum of
two scalars → still monotone. **Plan B changes the MODEL FAMILY, not the ensemble
architecture.** The multinomial model has NO scalar score — it has a vector output.

### 4.5 vs Per-place calibration/blend (phase3 REJECT)

Phase 3 calibration fit isotonic regressions on the output of the SAME single-score
ranker to produce P̂₁ and P̂₃. The critical finding (confirmed on real holdout): P̂₁ and
P̂₃ are monotone in the same raw score within each race → combining them with any weights
produces the same ordering (B==D on real data). **Plan B does not calibrate a single-score
ranker.** The multinomial model's P(1) and P(2) come from DIFFERENT output neurons trained
with DIFFERENT gradients — they need not be monotone in the same underlying representation.
This is the key structural distinction from Phase 3.

---

## 5. Evaluation and Gate

### 5.1 Metrics

Primary metrics, following the definition in `aggregate_bucket_eval_duckdb.py:341-350`:

| Metric       | Definition                                                             |
| ------------ | ---------------------------------------------------------------------- |
| `top1`       | `predicted_rank=1 AND finish_position=1` (per race, fraction of races) |
| `place2`     | `predicted_rank=2 AND finish_position=2` (per race, fraction of races) |
| `place3`     | `predicted_rank=3 AND finish_position=3` (per race, fraction of races) |
| `top3_box`   | all three {predicted_rank=1,2,3} match actual {1,2,3} in any order     |
| `fukusho_2p` | ≥2 of predicted top-3 finish in actual top-3                           |

Secondary: `top3_winner_capture_hit` (actual winner in predicted top-3).

### 5.2 Baseline

- **JRA:** `iter19-jra-cb-kohan3f-going-v8` (base-only, 244 features, current production)
- **NAR:** `iter12-nar-xgb-hpo-v8` + iter30/36 per-class ensemble blends (current production)

Current baselines (from deployed system, holdout 2023-2026):

- JRA: top1 ~44.5%, place2 ~22.0%, place3 ~17.0%, fukusho_2p ~68.4%
- NAR: top1 ~58.5%, place2 ~35.1%, place3 ~27.0%, fukusho_2p ~87.8%

### 5.3 Gate (4-axis multi-metric, identical to project standard)

```
fukusho_2p LB95 > 0.0          (paired bootstrap, 10k iters, seed=42)
AND  positive_axes >= 2         (axes = {top1, place2, place3, top3_box})
AND  positive_place_axes >= 1   (at least one of {place2, place3} positive)
AND  veto_floor: all axes >= -0.05pp
```

Accept as additional uplift if top1 improves; do NOT veto if top1 is flat (+/- 0.05pp).

### 5.4 Holdout and nested splits

- **Holdout:** 2023-2026 (same 11,703 JRA races / 45,573 NAR races as all prior experiments)
- **Walk-forward:** 3-fold (2023 / 2024 / 2025), then pool, then LB95 on the pool
- **Tuning split:** 2021-2022 (for HPO if needed — not initial experiment)

### 5.5 Oracle ceiling dependency

The achievable place2/3 accuracy from Plan B is **hard-capped by the oracle ceiling**
being measured in the Wave1 parallel task (`goal-baseline-and-ceiling.md`). The oracle
ceiling is the best achievable place2/3 if P[horse, position] were perfect (i.e., if the
model knew the true marginal probabilities exactly).

- If oracle ceiling < 40% for exact-ordinal place2: the >40% target is infeasible.
  Plan B should still be pursued to maximize toward the ceiling.
- If oracle ceiling ≥ 40%: the target is theoretically achievable, and Plan B's
  per-position model is the correct architectural path toward it.
- Recommendation: run Plan B experiment regardless of ceiling. If ceiling is low (e.g.
  30%), redefine success as "achieve oracle-ceiling −5pp" rather than ">40% absolute."

---

## 6. Realistic Expectation

### 6.1 Mechanism for place2/3 improvement

The multinomial model can improve place2 accuracy via two distinct mechanisms:

1. **Diagonal dominance in the P matrix for 2nd-place horses:** horses that consistently
   run 2nd (e.g., chronic bridesmaid horses, strong in a sprint but weak in a sprint
   final) should show high P(2) relative to P(1). A multiclass model trained on all
   ordinal labels simultaneously can learn this pattern from training data.

2. **Hungarian assignment of 2nd/3rd slots to horses with genuinely non-monotone
   P profiles.** In races where horse A's P(1) > P(2) > P(3) but horse B's P(2) > P(1)
   (because B is a place specialist), Hungarian assigns rank 2 to B even though A has
   a higher overall quality score.

### 6.2 Why place2/3 = 40% is uncertain

The adjacent-swap problem (47–62% of misses): a model assigning rank 2 to the horse
most likely to actually finish 2nd cannot exceed the oracle ceiling imposed by race
randomness. In typical horse races, ~30–40% of races have the top-2 quality horses
swap 1st/2nd place due to running conditions, traffic, pace dynamics — information that
is NOT in the feature store. No model architecture can exceed this ceiling.

**Most likely outcome range:**

- JRA place2: +0.5pp to +2.0pp above iter19 baseline (from ~22% to ~23%)
- NAR place2: +0.2pp to +1.5pp above iter12+ensemble baseline (from ~35% to ~37%)
- These are modest but directional — they represent the best achievable with the current
  feature set, given the architectural change.
- Reaching 40% place2 for JRA (+18pp) is NOT expected without new data sources.

The multinomial + Hungarian approach gives the MAXIMUM exact-ordinal accuracy achievable
for the current 244/173-feature envelope — it is the theoretically optimal assignment
policy given calibrated per-position probabilities. But the ceiling is determined by
signal quality, not the assignment mechanism.

### 6.3 Top1 improvement path to +5%

For top1 to improve +5% (from ~44.5% JRA to ~46.7%), the multinomial model's P(finish=1)
argmax must outperform the CatBoost YetiRank model's ranking by +5pp. This is achievable
only if:

(a) The multiclass objective produces a better representation of the win signal than
the pairwise-ranking objective, OR
(b) The multinomial model benefits from the sub-4 ordinal information (positions 4/5/6/7+)
as indirect supervision that sharpens the P(1) head.

Mechanism (b) is the main theoretical motivation: knowing the ordering of positions 4+
provides additional pairwise-comparison signal about horse quality that the 7-bucket
multiclass objective can exploit, while NDCG@3 (used by YetiRank) ignores this signal
entirely. This is analogous to why sub-4 graded relevance (Plan A) lifts top1 in some
experiments — the sub-4 ordering provides extra gradient signal that refines the model's
quality representation.

---

## 7. Memory and Compute Plan

All training subject to the HARD memory rule: ONE heavy train at a time, DuckDB 6GB/4
threads cap, memory_pressure < 30% free before starting any heavy step (M5 Pro 48GB,
Colima reserving 24GB → effective free budget ~24GB).

| Step                                       | Framework      | RAM peak | Duration  | Constraint            |
| ------------------------------------------ | -------------- | -------- | --------- | --------------------- |
| JRA cheap filter (LGB multiclass, 1M rows) | LightGBM       | ~3 GB    | ~4 min    | Safe                  |
| JRA walk-forward WF (3 folds)              | LightGBM       | ~3 GB    | ~15 min   | Safe (solo)           |
| NAR cheap filter (LGB multiclass, 3M rows) | LightGBM       | ~6 GB    | ~8 min    | Safe (solo)           |
| NAR walk-forward WF (3 folds)              | LightGBM       | ~8 GB    | ~35 min   | Solo only             |
| Hungarian assignment (inference, per race) | scipy (Python) | <100 MB  | <1ms/race | Negligible            |
| Feature parquet load (R2 read)             | DuckDB         | ~6 GB    | ~10 min   | 6GB/4threads enforced |

**Sequencing:** Run JRA first (cheaper, ~20 min total). Then NAR. Do NOT run in parallel —
LGB multiclass on 3M NAR rows peaks at ~8 GB, exceeding safe parallel budget.

**DuckDB note:** Feature parquet loading from R2 uses `SET memory_limit = '6GB'; SET threads = 4;`
per the hard rule. Training itself runs entirely in Python/NumPy — DuckDB is only used
for feature loading.

---

## 8. Implementation Checklist

### Phase 1: Data prep (no compute cost)

- [ ] Verify `finish_position` column availability in feature parquets for JRA and NAR.
      (Currently used as the relevance label — confirm it is an integer, not a rank float.)
- [ ] Compute position_bucket distribution: `SELECT min(fp), max(fp), percentile_cont(0.9) …`
      to check field-size distribution and validate 7-bucket scheme.
- [ ] Check for NULL / DNF rows (finish_position IS NULL): count them and confirm the
      current data pipeline already excludes them.

### Phase 2: JRA experiment

1. Write `tmp/plan_b/train_jra_multinomial.py`:
   - Load `feat-jra-v8-iter19-kohan3f-going` parquet (same 244 features as iter19)
   - Construct position_bucket label (7 classes, 0-indexed)
   - Cheap filter: train ≤ 2022, holdout 2023-2025
   - If cheap filter PASS (place2 positive AND not catastrophic top1 drop):
   - Walk-forward: 3 folds (2023 / 2024 / 2025)
   - Apply `assign_positions_top1_protected()` at each fold
   - Pool predictions, compute metrics, paired bootstrap LB95

2. Gate evaluation using `aggregate_bucket_eval_duckdb.py` logic (or inline equivalent).

3. Compare vs `iter19-jra-cb-kohan3f-going-v8` on holdout 2023-2026.

### Phase 3: NAR experiment (if JRA PASS)

1. Write `tmp/plan_b/train_nar_multinomial.py` — same structure, using NAR iter12 feature
   store (173 features).

2. NAR has per-class ensembles (iter30/36). For the Plan B judge:
   - Option 1: Run Plan B as a REPLACEMENT for the base model — judge against the full
     production pipeline (iter12 + iter30/36) as baseline.
   - Option 2: Run Plan B as an ADDITIONAL model and blend with the existing ensemble.
   - **Recommendation: Option 1 first.** If Plan B base model beats iter12 base + ensemble
     at the base level, consider co-training the per-class layer against Plan B base WF scores.
   - Same WELD risk as Plan A (Experiment 3): if the NAR Plan B base shifts score
     distribution, per-class residuals calibrated to iter12's distribution will regress.
   - **For the initial judge: measure Plan B base model alone (no per-class) vs production
     full system.** This is the correct minimum bar: Plan B must beat the full production
     system even WITHOUT per-class ensembles to be deployment-worthy.

---

## 9. Sequencing and Go/No-Go Criteria

```
JRA cheap filter (~4 min)
  └── FAIL (top1 drop >2pp OR place2 <0) → STOP; architecture may not be suitable;
              document and close Plan B for JRA
  └── PASS → JRA WF (3 folds, ~15 min)
       └── ADOPT (gate passes) → commit iter20-jra-plan-b-multinomial; swap production
       └── REJECT → investigate which fold drives failure; document; close JRA Plan B

NAR cheap filter (~8 min, independent of JRA result)
  └── FAIL → STOP
  └── PASS → NAR WF (3 folds, ~35 min)
       └── Base model vs production full system:
            ADOPT (gate passes) → co-train per-class layer → full-system judge → flip
            REJECT → document; close NAR Plan B

```

JRA and NAR cheap filters can run sequentially (NAR first if JRA is low-priority, or
JRA first to get a faster signal). Do NOT run simultaneously.

---

## 10. Connection to Existing History

- `rootcause-i6-architecture.md §3.2` — confirmed Plackett-Luce and C4 are bounded by
  monotonicity; Plan B explicitly breaks monotonicity with a direct per-position model.
- `phase3-calibration-c4-rerank.md` — proved P̂₁ and P̂₃ from a single-score model are
  order-correlated on real data; Plan B avoids this by having separate model heads trained
  on the full ordinal distribution.
- `PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md` — closed binary specialists, cascade, and
  LambdaRank place-weight; Plan B avoids all of these (no binary specialist, no cascade,
  no place-weighted ranking objective).
- `graded-relevance-experiments.md` — sub-4 epsilon labels (Plan A); Plan B uses sub-4
  labels as DIRECT TRAINING TARGETS for the 4th/5th/6th position buckets, not as epsilon
  gradients for a ranking objective.
- `project_science_track_saturation_2026_06_11.md` — identified that features are at
  frontier; Plan B does NOT require new features; it changes the model family and
  assignment policy on the existing feature store.

---

## 11. Standard Place-Specific Lever Ranking (secondary to Plan B)

The following levers complement Plan B but do not require Plan B as a prerequisite.
They are ranked by expected lift × implementation cost.

| Rank | Lever                                                            | Novelty                                    | Expected place2/3 lift                    | Cost   |
| ---- | ---------------------------------------------------------------- | ------------------------------------------ | ----------------------------------------- | ------ |
| 1    | **Plan B (multinomial + Hungarian, this doc)**                   | GENUINE — never tried                      | +0.5–2.0pp place2 JRA; +0.2–1.5pp NAR     | Medium |
| 2    | **Ban-ei dedicated place model** (exotic odds + place objective) | GENUINE — separate place model never tried | place3 +1.4pp / fukusho_2p +0.77pp Ban-ei | Medium |
| 3    | **NAR 10:00 JST re-predict pass** (after sanrenpuku available)   | GENUINE — requires infra + ingest fix      | place2 +0.10–0.20pp NAR (deferred path)   | High   |
| 4    | Conditional per-rank isotonic calibration                        | NOT NOVEL — variant of C4                  | ~0pp                                      | Low    |
| 5    | Rivalry/suppression features (horse-vs-dominant-field)           | MARGINAL — sub-species of near-miss        | place2 +0.05–0.12pp                       | Medium |
| 6    | Two-stage conditioned-on-rank place classifier                   | REHASH — MLX conditional head + I6 sim D   | Negative                                  | High   |
| 7    | Trained PL / sub-4 graded relevance (base model only)            | REHASH — scheme-D full-system REJECT       | 0pp net                                   | High   |
