---
probe: pgvector-knn-703-005
date: 2026-06-17
classes: [703, 005]
method: numpy kNN (L2 distance, pgvector-equivalent without pg_vector extension)
status: 703 PROCEED (horse_ability k≥20) / 005 ABORT / race_condition ABORT (both classes)
---

# pgvector kNN Vector-Correlation Probe — JRA 703 / 005

## Why this design differs from DO-NOT-RETEST

DO-NOT-RETEST entry #1 (ROADMAP §3): `iter32-jra-vec-knn-{class}-v8` used kNN as an
**ensemble member blended at inference time**. All 5 JRA classes were rejected under the
full nested-split + multi-metric + bootstrap gate. 703 top1 delta was −0.166pp. 005 was the
near-miss (4/4 axes positive but LB95 = −0.000953, correctly rejected).

**This probe tests a different question**: does the kNN-similarity score have **orthogonal
partial-ρ vs finish_norm after controlling for GBDT score + log(odds)**? This is a cheap-filter
(partial-ρ gate only), not a full model retrain + ensemble. A positive result would indicate
a kNN-derived value could serve as an **additive training-time feature** for the GBDT — a
structurally different path from the rejected ensemble-member design.

Two embedding designs are tested:

1. **horse_ability** — 15 numeric features: `speed_index_avg_5`, `speed_index_best_5`,
   `kohan3f_avg_5`, `career_win_rate`, `career_place_rate`, `same_distance_win_rate`,
   `same_keibajo_win_rate`, `jockey_recent_win_rate`, `trainer_career_win_rate`,
   `pedigree_score_for_race`, `popularity_score`, `odds_score`, `last_race_finish_norm`,
   `finish_trend_5`, `weight_avg_5`. These encode individual horse quality and form.
   NOTE: these features overlap with existing GBDT inputs — the probe tests whether the
   kNN aggregation adds anything orthogonal beyond what the GBDT already captures.

2. **race_condition** — 10 numeric features: `kyori`, `track_code` (numeric),
   `field_size_normalized`, `field_avg_speed_index`, `field_pace_index`,
   `field_nige_pressure`, `field_style_diversity`, `weather_normalized`,
   `track_condition_normalized`, `course_corner_count`. This encodes the competitive
   environment (ROADMAP §4, Candidate 4 design). The hypothesis: find historically similar
   race environments and use the per-horse-profile performance in those races as a feature.

## Method

- **Implementation**: `sklearn.NearestNeighbors` (ball_tree, euclidean L2) — equivalent to
  pgvector L2 similarity. pgvector indexing was not needed: the DuckDB memory limit (4GB,
  4 threads) was respected and numpy kNN on standardized float32 vectors was tractable.
- **Train / holdout split**: train years 2013–2022, holdout years 2023–2025. All neighbors
  are strictly from train years (leak check confirmed: no holdout-year neighbor in any sample).
- **kNN score**: `1 - mean(finish_norm of k nearest neighbors)`. Higher = neighbors
  historically finished better = this horse is likely to finish better.
- **Standardization**: `StandardScaler` fit on train, applied to holdout.
- **Partial-ρ**: partial Spearman rank correlation (OLS on rank-transformed variables).
  Controls: `log(inverse_odds_implied_prob)` + `iter19_score` (WF predictions from
  `tmp/v8/iter19-wf-predictions/`). GBDT score coverage: 100%.
- **Gate**: |partial-ρ| ≥ 0.08 to PROCEED.

## Results

### Class 703 (未勝利) — horse_ability embedding

n_train = 179,723 rows / 12,303 races  
n_holdout = 52,544 rows / 3,710 races

| k   | raw Spearman (kNN vs finish_norm) | partial-ρ (controls: log_odds + iter19) | p-value |
| --- | --------------------------------- | --------------------------------------- | ------- |
| 5   | −0.5507                           | −0.0509                                 | <0.0001 |
| 10  | −0.5855                           | −0.0690                                 | <0.0001 |
| 20  | −0.6037                           | −0.0814                                 | <0.0001 |
| 50  | −0.6135                           | −0.0892                                 | <0.0001 |
| 100 | −0.6170                           | −0.0965                                 | <0.0001 |

GBDT (iter19_score) raw Spearman vs finish_norm: −0.7158  
Odds (−log_inv_prob) raw Spearman vs finish_norm: +0.5997

kNN-only top1 (k=20, holdout): **36.44%** vs GBDT baseline 49.51% vs market 35.53%  
kNN-only top1 (k=100, holdout): consistent with k=20 (neighborhood size makes little difference to ranking)

**Interpretation**: The kNN-only ranking (36.4%) is slightly above market (35.5%) but far
below GBDT (49.5%). However, the partial-ρ after controlling for both GBDT score AND odds
clears |0.08| at k≥20 and is significant (p<0.0001 at all k). The signal grows with k
(−0.051 → −0.097), suggesting it is not noise but a genuine, if small, orthogonal component.

**What the orthogonal component likely is**: the horse_ability embedding includes several
features that the GBDT also sees directly. The kNN aggregation computes the mean finish_norm
of historically-similar horse-profiles. This is essentially a **nonlinear smoothing** over
the training data that the GBDT may not fully capture via tree splits, particularly for
maiden horses (703) with sparse race histories. The 15 features include `popularity_score`
and `odds_score` directly, so the controls only partial out the raw odds signal — the kNN
may be capturing a higher-order structure (e.g., "horses with this exact combination of form
indices typically finish where?") that the GBDT approximates but does not perfectly encode.

**Caution**: The prior kNN ensemble member for 703 (iter32-jra-vec-knn-703-v8) showed
top1 delta = −0.166pp when used as a blended member. The partial-ρ asks whether the signal
is orthogonal, not whether it is sufficient to improve the model when added as a feature.
The partial-ρ = −0.0814 at k=20 is only marginally above the 0.08 gate — the orthogonal
signal is real but small.

**VERDICT: PROCEED** — 703 horse_ability at k=20–100 clears |partial-ρ| ≥ 0.08.
Recommended next step: add `knn_ability_score_k50` as a training-time feature to the 703
per-class feature set and run a cheap incremental retrain (partial-ρ ≥ 0.08 is necessary
but not sufficient — the incremental GBDT retrain must clear the full multi-metric gate).

### Class 005 (1勝クラス) — horse_ability embedding

n_train = 140,077 rows / 10,143 races  
n_holdout = 36,801 rows / 2,776 races

| k   | partial-ρ (controls: log_odds + iter19) | p-value |
| --- | --------------------------------------- | ------- |
| 20  | −0.0687                                 | <0.0001 |

kNN-only top1 (k=20, holdout): **26.95%** vs GBDT 40.42% vs market 32.28%

**Interpretation**: partial-ρ = −0.0687 fails the |0.08| gate. The 1勝クラス horses have
richer race histories than maiden horses (703), so the GBDT already captures more of the
ability signal directly. The kNN aggregation adds less orthogonal information.

Also note: 005 was the near-miss in the prior ensemble-member test (iter32: all 4 axes
positive, top1 +0.064pp, LB95 = −0.000953). The partial-ρ result is consistent — there is
a real but sub-threshold orthogonal signal in 005 that is not sufficient to justify a retrain.

**VERDICT: ABORT** — 005 horse_ability partial-ρ = −0.0687 < 0.08 threshold.

### Race-condition embedding (both classes)

| Class | partial-ρ (k=20) | p-value | kNN-only top1 |
| ----- | ---------------- | ------- | ------------- |
| 703   | −0.0009          | 0.8418  | 8.89%         |
| 005   | −0.0017          | 0.7453  | 7.64%         |

**Interpretation**: The race-condition embedding carries essentially zero orthogonal signal
after controlling for GBDT score and odds. The 6-dimensional race environment vector
(`kyori`, `track_code`, `field_size_normalized`, `field_avg_speed_index`, `field_pace_index`,
`field_nige_pressure`, `field_style_diversity`, `weather_normalized`,
`track_condition_normalized`, `course_corner_count`) already exists as explicit features in
the GBDT. The kNN aggregation over this embedding finds "similar race environments" but the
mean finish_norm in those environments has no additional predictive power — the GBDT already
uses these conditions directly to split. The 8.9% / 7.6% kNN-only top1 (far below market
35.5%) confirms the race-condition vector alone is essentially random over horses.

This result invalidates ROADMAP §4 Candidate 4's hypothesis. The race-condition similarity
does NOT produce a feature orthogonal to the existing model — the GBDT already captures all
the race-environment information through its existing tree splits on `kyori`, `track_code`,
field statistics, etc.

**VERDICT: ABORT** — both classes, race_condition embedding (partial-ρ ≈ 0, p > 0.7).

## Summary

| Class | Embedding      | k   | partial-ρ | p-value | kNN-only top1 | GBDT top1 | Verdict |
| ----- | -------------- | --- | --------- | ------- | ------------- | --------- | ------- |
| 703   | horse_ability  | 20  | −0.0814   | <0.0001 | 36.44%        | 49.51%    | PROCEED |
| 703   | horse_ability  | 50  | −0.0892   | <0.0001 | ~36%          | 49.51%    | PROCEED |
| 005   | horse_ability  | 20  | −0.0687   | <0.0001 | 26.95%        | 40.42%    | ABORT   |
| 703   | race_condition | 20  | −0.0009   | 0.8418  | 8.89%         | 49.51%    | ABORT   |
| 005   | race_condition | 20  | −0.0017   | 0.7453  | 7.64%         | 40.42%    | ABORT   |

## Interpretation and Risk Assessment

The only PROCEED result (703, horse_ability, k≥20) is marginal and carries significant
caveats:

1. **Near-overlap with rejected design**: the horse_ability embedding uses features
   very similar to the GBDT input. The orthogonal partial-ρ (−0.08 to −0.10) may reflect
   a nonlinear aggregation effect, but it is small.

2. **Prior ensemble-member rejection**: when iter32 used a similar kNN as an ensemble
   MEMBER for 703, the result was top1 delta = −0.166pp (REJECT). The partial-ρ approach
   tests whether the signal is present, not whether it survives the full model gate.

3. **k sensitivity**: signal grows with k (−0.051 at k=5 → −0.097 at k=100), suggesting
   it is a global smoothing effect over many training examples. At k=50–100, approximately
   half to all of the training set is included, at which point this approaches a global
   mean — raising questions about whether the per-horse kNN signal is meaningfully local.

4. **Recommended next step for 703**: use k=50 to compute `knn_ability_score_k50` as an
   additional GBDT training feature, run a per-class retrain for 703 only, evaluate under
   the standard strengthened gate (≥2/4 axes positive, ≥1 place positive, LB95>0, Holm).
   Given the prior 703 kNN ensemble result was negative (−0.166pp), there is a real
   possibility the feature adds noise rather than signal in the full GBDT. This is the only
   honest path to resolve the ambiguity.

## Leak Verification

Confirmed: all k=20 neighbors for holdout horses (2023–2025) come from train years
2013–2022. Year distribution in sample (100 holdout horses, k=20 neighbors each):
2013:38, 2014:31, 2015:32, 2016:264, 2017:268, 2018:292, 2019:256, 2020:296, 2021:280,
2022:243. No holdout-year leak present.

## Quality Gate (this doc only — no code change)

This is a probe-only result. No model code was changed. No enforced-package files were
modified. The only artifact is this documentation file.
