---
probe: clean-vectorization-value-prep-ablation
date: 2026-06-17
class: 703
method: sklearn NearestNeighbors (L2), 5 value-prep variants
status: PARTIALLY-ARTIFACT — within-race transform is the decisive fix; odds-exclusion alone breaks the gate
---

# Clean-Vectorization Value-Prep Ablation — JRA 703

## Question

The prior probe (`pgvector-knn-703-005.md`) identified three value-prep flaws in the original
kNN vector:

1. **Odds contamination**: `odds_score` and `popularity_score` included in the horse-ability
   vector, so kNN re-clustered the market → the partial-ρ test controlled for log(odds)
   but the vector itself encoded odds → partial correlation may be inflated by the residual
   market structure inside the embedding.

2. **Absolute values instead of within-race relative**: standardized globally but finish is
   relative within a race. Two horses with identical absolute speed indices are similar peers
   even if one is a 1:1 favorite and the other is 50:1.

3. **Numeric track_code**: race-condition vector used `track_code` as a numeric label,
   making L2 distance between turf (10) and dirt (12) arbitrary.

The USER's hypothesis: the prior ABORT / marginal PROCEED was caused by these flaws, not
by vectorization being genuinely redundant.

## Experiment Design

Five value-prep variants on JRA 703 (未勝利), same kNN architecture as prior probe:
`sklearn.NearestNeighbors(ball_tree, L2)`, neighbors strictly from train years (2013–2022),
evaluated on holdout 2023–2025.

- **V0 (reproduce prior)**: 15 features including `odds_score` + `popularity_score`, 0-fill
  NaN, global `StandardScaler`. Baseline to confirm prior result.
- **V1 (odds-excluded)**: V0 without `odds_score` + `popularity_score` (13 features).
  Tests: does removing market from the vector change the orthogonality?
- **V2 (within-race relative)**: V1 features, each transformed to within-race z-score
  `(value − race_mean) / race_std` before global scaling. Tests: does encoding
  relative-to-field strength recover the signal lost by dropping odds?
- **V3 (NULL-aware)**: V2 + median impute (fit on train) + NULL indicator dimensions for
  features with >5% NULLs. Tests: does proper NULL handling further improve the signal?
- **V4 (curated + supervised weighting)**: V3 + expanded curated feature set (18 features
  from 263 available, including `recent_finish`, `avg_finish`, `career_top1_count`, etc.),
  each feature weighted by its univariate |Spearman ρ| with `finish_norm` on train.
  Tests: does feature selection and weighting push the signal clearly above the gate?

One additional fix tested separately:

- **V_race_condition_onehot**: race-condition embedding with `track_code` one-hot encoded
  (9 numeric features + one-hot `track_code`). Tests: does proper categorical encoding
  rescue the previously-zero race_condition signal?

**Partial-ρ method**: Partial Spearman rank correlation via OLS on rank-transformed
variables, controlling for `log(inverse_odds_implied_prob)` + `iter19_score` (iter19 WF
GBDT predictions, 100% coverage). Gate: |partial-ρ| ≥ 0.08.

**Data**: n_train = 179,723 rows / 12,303 races; n_holdout = 52,544 rows / 3,710 races.
All neighbors come from train years (no holdout-year leak).

## Results

| Variant                 | k   | kNN top1 | raw Spearman | partial-ρ | p-value  | clears gate    |
| ----------------------- | --- | -------- | ------------ | --------- | -------- | -------------- |
| V0 (prior reproduce)    | 20  | 36.20%   | −0.6033      | −0.0803   | 5.5e-76  | YES (marginal) |
| V0 (prior reproduce)    | 50  | 36.44%   | −0.6135      | −0.0887   | 3.3e-92  | YES            |
| V0 (prior reproduce)    | 100 | 36.55%   | −0.6171      | −0.0961   | 4.7e-108 | YES            |
| V1 (odds-excluded)      | 20  | 30.13%   | −0.5294      | −0.0624   | 1.6e-46  | **NO**         |
| V1 (odds-excluded)      | 50  | 31.54%   | −0.5441      | −0.0696   | 2.2e-57  | **NO**         |
| V1 (odds-excluded)      | 100 | 32.18%   | −0.5485      | −0.0724   | 5.5e-62  | **NO**         |
| V2 (within-race rel.)   | 20  | 31.51%   | −0.5531      | −0.0895   | 7.3e-94  | YES            |
| V2 (within-race rel.)   | 50  | 33.34%   | −0.5661      | −0.0948   | 3.1e-105 | YES            |
| V2 (within-race rel.)   | 100 | 32.64%   | −0.5686      | −0.0950   | 1.1e-105 | YES            |
| V3 (NULL-aware)         | 20  | 31.97%   | −0.5563      | −0.0945   | 1.3e-104 | YES            |
| V3 (NULL-aware)         | 50  | 32.43%   | −0.5690      | −0.1005   | 5.4e-118 | YES            |
| V3 (NULL-aware)         | 100 | 32.80%   | −0.5728      | −0.1035   | 4.0e-125 | YES            |
| V4 (curated)            | 20  | 32.35%   | −0.5640      | −0.1023   | 2.7e-122 | YES            |
| V4 (curated)            | 50  | 32.53%   | −0.5776      | −0.1130   | 8.9e-149 | YES            |
| V4 (curated)            | 100 | 32.35%   | −0.5804      | −0.1146   | 4.0e-153 | YES            |
| V_race_condition_onehot | 20  | 10.93%   | −0.0086      | −0.0047   | 0.279    | **NO**         |

**Baselines**: Market top1 = 35.74%, GBDT (iter19) top1 = **49.41%**

## Which prep fix mattered most?

### Finding 1: Odds-exclusion BREAKS the gate (V0 → V1 reversal)

When `odds_score` and `popularity_score` are removed from the vector (V1), the partial-ρ
drops from −0.0803 to −0.0624 at k=20 — falling below the 0.08 threshold. This reveals
that the prior probe's PROCEED result was **partially inflated by odds contamination in
the vector itself**. The vector encoded market information; even after controlling for raw
log(odds), the OLS residuals still contained market structure baked into the embedding.

### Finding 2: Within-race relative transform RECOVERS the signal without odds (V2)

Adding within-race z-score transformation (V2, no odds) recovers partial-ρ to −0.0895 at
k=20, clearing the gate comfortably. This is the **decisive fix**. The intuition: two horses
with identical absolute speed indices in different races are not comparable peers. A horse
whose speed_index is +1σ above its race mean encodes a different quality than one whose
speed_index happens to have the same absolute value but is average for its race. Within-race
relative encoding makes the L2 geometry meaningful.

### Finding 3: NULL handling and curation add incremental gains (V3, V4)

V3 (NULL-aware) pushes partial-ρ to −0.0945/−0.1035 at k=20/100. V4 (curated + supervised
weighting) reaches −0.1023/−0.1146 at k=20/100. These improvements are real but incremental
— the within-race transform (V2) does most of the work.

Top-5 features by univariate |ρ| with finish_norm (on train, identified in V4):

1. `recent_finish` (|ρ| = 0.493)
2. `avg_finish` (|ρ| = 0.487)
3. `last_race_finish_norm` (|ρ| = 0.486)
4. `recent_top3_count_5` (|ρ| = 0.396)
5. `career_place_rate` (|ρ| = 0.394)

Note: best V4 kNN top1 is 33.5% (k=50), still well below GBDT 49.4% — kNN cannot match
GBDT standalone, but the partial-ρ gate tests orthogonality, not standalone accuracy.

### Finding 4: Race-condition one-hot fix does NOT rescue the embedding

Even with proper one-hot encoding of `track_code`, the race-condition embedding has
partial-ρ = −0.0047 (p=0.28) — consistent null. The prior ABORT for race-condition was
NOT a numeric-label artifact; it is genuinely uninformative after controlling for GBDT
and odds. The GBDT already captures race environment information through its tree splits.
One-hot encoding of track_code does not change this: the race-condition embedding still
describes the same race context that GBDT already uses.

## Verdict

**PARTIALLY a value-prep artifact — but not in the direction the USER expected.**

The original prior probe's partial-ρ was **inflated by odds contamination in the vector**
(V0 odds-included clears the gate; V1 odds-excluded fails). Removing the market from the
vector reveals a weaker signal that simple global scaling cannot recover to gate threshold.

However, the **within-race relative transformation (V2) rescues the signal**: partial-ρ =
−0.0895 at k=20 without any odds in the vector, comfortably above the 0.08 gate. This
represents a **genuinely orthogonal component** that is not merely odds echo:

- V2 removes odds from the vector entirely
- Controls for log(odds) in the partial-ρ test
- Still clears 0.08 gate → the residual is real horse-quality structure orthogonal to
  both the market AND the GBDT's current approximation of it

The race-condition one-hot fix does NOT change the null result for race-condition vectors.

## Practical Implications

1. **The prior PROCEED for 703 (V0) was partially contaminated** by odds inclusion. The
   partial-ρ = −0.0814 was partly odds echo, not purely horse-quality aggregation.

2. **A clean kNN feature for 703 requires within-race-relative transformation** (V2 or
   better). Using V3 (NULL-aware, k=50) gives partial-ρ = −0.1005, clearly above the gate
   and not driven by any odds leakage.

3. **Recommended next step** (if the incremental GBDT retrain path is pursued):
   Use V3 or V4 encoding with k=50 to compute `knn_ability_score_clean_k50` as a training-
   time feature for the 703 per-class GBDT. The partial-ρ gate clears (necessary condition),
   but an incremental GBDT retrain under the standard multi-metric gate (≥2/4 axes positive,
   ≥1 place positive, LB95>0) is still required (partial-ρ is necessary, not sufficient).
   Given the prior 703 kNN ensemble member result was negative (iter32: top1 delta = −0.166pp),
   there is real risk the signal is too weak to survive the full gate.

4. **Race-condition vectorization is confirmed genuinely redundant** with GBDT regardless
   of encoding quality. Do not pursue further.

5. **Implication for the learned-embedding path (Ensemble C)**: a supervised embedding
   trained end-to-end (not hand-crafted L2 kNN) may capture the within-race relative
   structure automatically and produce a stronger orthogonal signal. This ablation does not
   test learned embeddings — that is the scope of the Ensemble C agent.

## Quality Gate (this doc only — no code change)

This is a probe-only result. No model code, no enforced-package files, no production
features were modified. No `tmp/` files were git-added. The only artifact is this
documentation file.
