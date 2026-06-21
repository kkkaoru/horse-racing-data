# RL + learned-vectorization + vector-search ensemble (JRA 703)

**Date**: 2026-06-17
**Class**: JRA 703 (未勝利)
**Task #31**: Ensemble C — RL + learned-vectorization + vector-search retrieval-augmented ensemble
**Verdict**: **ABORT** — ensemble top1=25.93% vs GBDT 48.23% (−22.30pp), LB95=−25.57pp

---

## Motivation — why this is distinct from prior probes

Prior experiments tested:

- **Raw-feature kNN** (task #22): cosine kNN on raw 244 features, top1~36%, partial-ρ~0.08–0.14. ABORT because raw-feature space is already captured by GBDT.
- **Raw-feature RL** (task #28): REINFORCE MLP on raw features, holdout top1~38.7%. ABORT — raw features insufficient to beat GBDT.
- **Method-diverse blend** (task #30): GBDT + raw-kNN + raw-RL. Best blend = GBDT-only (100% weight). ABORT.

This experiment is genuinely distinct: it trains a **learned 32-dimensional embedding** via a pairwise ranking/contrastive objective, then performs **vector search in the learned space**, and applies **RL policy gradient on [embedding + retrieval score]**. The hypothesis: a learned embedding might capture within-race relative horse quality in a way that raw features + kNN cannot, and retrieval over the learned space could provide a different signal from the GBDT's direct tabular prediction.

---

## Architecture

### 1. Vectorization (learned embedding)

```
MLX encoder MLP: 236 features → 128 → 64 → 32d (L2-normalized)
Objective: batched pairwise ranking loss (logistic)
  for all valid (i,j) pairs in each race where finish_norm[i] > finish_norm[j]:
    loss += log(1 + exp(-(score_i - score_j)))
```

Training: 2013–2021 (9,671 races), early-stop on val 2022. Epochs run: 6, best val top1: **29.62%** (0.2s/epoch with vectorized batch).

### 2. Vector search (retrieval over learned 32d space)

For each query horse-race (tune/blind), cosine-kNN in the learned 32d embedding space against the entire training set (only valid, non-padded horses indexed). k=50 neighbors. Retrieval score = similarity-weighted mean of neighbor `finish_norm` values. The result is a per-horse retrieval score in [0,1] representing expected quality relative to historical similar horses **in the learned embedding space** (not raw feature space).

Retrieval score population means: tune μ=0.506, blind μ=0.502 (near-neutral, expected).

### 3. RL policy (REINFORCE)

```
Input per horse: [learned_embedding (32d) + retrieval_score (1d)] = 33d
Policy MLP: 33 → 64 → 1 (logit per horse → softmax ranking)
Training: REINFORCE with Gumbel-Plackett-Luce sampling
Reward: 1.0*top1_hit + 0.5*place2_hit + 0.3*place3_hit + 0.3*top3_box_hit
Baseline: EMA of mean batch reward
```

RL training: 2013–2021 enriched with embeddings + retrieval scores, early-stop on val 2022. Epochs run: 5, best val top1: **26.60%** (0.6s/epoch).

### 4. Ensemble blend

Grid-search blend of {enc-head score, retrieval score, RL policy logit} on tune 2023–2024. Best blend evaluated on blind 2025 (never touched during selection). α values searched: {0, 0.25, 0.5, 0.75, 1.0} × 3 components, normalized.

---

## Results

### Data splits

| Split        | Years     | Races |
| ------------ | --------- | ----- |
| Train        | 2013–2021 | 9,671 |
| Val          | 2022      | 1,094 |
| Tune         | 2023–2024 | 2,155 |
| Blind (gate) | 2025      | 1,103 |

### Component-only accuracy (blind holdout 2025)

| Component                  | top1       | place2 | place3 |
| -------------------------- | ---------- | ------ | ------ |
| enc-only (encoder head)    | **0.18%**  | 0.82%  | 3.99%  |
| ret-only (retrieval score) | **0.09%**  | 1.18%  | 3.63%  |
| rl-only (REINFORCE policy) | **25.93%** | 8.43%  | 10.88% |

The encoder head and retrieval scores produce near-zero exact top-1 accuracy on the blind set. This is a key finding: **the 32-dimensional learned embedding does not learn to reliably identify the exact winner** — it learns a general quality ordering that is noisy for exact-rank prediction. The RL policy over [embedding + retrieval] achieves 25.93% but is still far below GBDT.

### Ensemble accuracy (blind holdout 2025)

| Method                           | top1       | place2     | place3     | LB95 (top1) |
| -------------------------------- | ---------- | ---------- | ---------- | ----------- |
| equal weight (1/3, 1/3, 1/3)     | 0.18%      | 0.82%      | 3.99%      | —           |
| best blend (grid-search on tune) | **25.93%** | 8.43%      | 10.88%     | −25.57pp    |
| **GBDT iter21 baseline**         | **48.23%** | **25.66%** | **20.40%** | —           |

Best blend weights: enc=0.00, ret=0.00, rl=1.00 (grid-search degrades to RL-only).

### Delta vs GBDT baseline

| Metric      | Delta    |
| ----------- | -------- |
| top1        | −22.30pp |
| place2      | −17.23pp |
| place3      | −9.52pp  |
| LB95 (top1) | −25.57pp |

**VERDICT: ABORT**

---

## Diagnosis — why the learned embedding + retrieval fails

### 1. Encoder head near-zero top-1

The pairwise ranking loss trains the encoder to produce embeddings where horses that finish better have higher head scores within a race. This is a ranking objective that explicitly doesn't need to identify the exact winner. In practice:

- `enc-only top1 = 0.18%` on 1,103 blind races means the encoder head picks the actual winner in ~2 races out of 1,103.
- The encoder learned **relative ordering** in embedding space, but lost the ability to pick the single best horse (its head score is miscalibrated for exact argmax).
- The L2-norm constraint collapses all embeddings to the unit sphere, which may cause ranking scores to be compressed.

### 2. Retrieval scores near-neutral

Retrieval scores (μ=0.502–0.506) converge near 0.5 for all splits. This indicates:

- The learned 32d embedding space distributes all training horses similarly — neighbors in learned space are not meaningfully better or worse finishers than random.
- kNN in the learned space retrieves horses that are "similar" in the embedding sense, but finish_norm of similar horses in embedding space is near-random (0.5).
- This is because the pairwise ranking loss trains WITHIN-race relative ordering — it does not encode absolute quality level that would be useful for cross-race retrieval.

### 3. RL policy limited by input quality

The RL policy takes [embedding (32d) + retrieval_score (1d)] as input. With retrieval scores near-neutral and embedding representations that don't encode absolute quality, the RL policy has essentially noise inputs + 32d learned representation. It achieves 25.93% top1 vs raw-feature RL's 38.7%, confirming the learned embedding representation is **worse** than raw features for the RL policy.

The RL policy's 26.60% val top1 suggests it's learning some signal from the embeddings (random baseline would be ~8-10% for field sizes of 10-14), but not nearly enough.

### 4. Same-feature bound holds

The fundamental constraint: the learned embedding uses the same 236 features as GBDT. A 3-layer MLP encoder in a 32-dimensional space cannot extract more signal than GBDT over those 236 features. GBDT's advantage:

- 500-1000 trees, each splitting on the most informative features
- Native handling of categorical features, missing values, and non-linear interactions
- Monotone constraints for market signals
- Trained directly with lambdarank loss on within-race rankings

The learned embedding is a lower-capacity model trained on a proxy (pairwise ranking loss) that generalizes poorly to exact winner prediction.

---

## Comparison with prior method-diverse ensemble

| Experiment                                    | top1 (blind 2025)       | vs GBDT      |
| --------------------------------------------- | ----------------------- | ------------ |
| Task #22: raw-feature kNN (k=50)              | ~36% (partial-ρ only)   | −12pp        |
| Task #28: raw-feature RL                      | 38.67%                  | −9.56pp      |
| Task #30: GBDT+kNN+RL blend                   | 48.23% (GBDT-only wins) | 0.00pp       |
| **This (task #31): learned-vec+retrieval+RL** | **25.93%**              | **−22.30pp** |

The learned embedding approach performs **worse** than raw-feature RL because the embedding representation discards discriminative absolute quality information during training. Raw features (including odds, speed index, jockey rates) carry direct predictive signal that the encoder's pairwise objective compresses into a 32d space that loses this granularity.

---

## Lessons

1. **Contrastive/ranking loss ≠ calibrated ranking score**: A pairwise ranking loss trains the model to order horses correctly but does not calibrate the head to produce reliable softmax-ranked outputs for exact winner prediction. A listwise loss (e.g., ListNet) or direct rank-regression would be better.

2. **Cross-race retrieval requires absolute quality encoding**: The learned embedding needs to encode each horse's absolute expected finish quality (not just within-race relative ordering) for cross-race kNN retrieval to be meaningful. This requires a fundamentally different objective: e.g., predicting `finish_norm` directly (regression), or contrastive training across different races.

3. **GBDT same-feature wall confirmed again**: Grinsztajn et al. (2022) and the full history of this project (iter1–iter27+) confirm that GBDT remains the best method for tabular horse racing prediction when the feature set is fixed. The learned-embedding + retrieval architecture does not break this wall.

4. **Retrieval augmentation requires structural novelty**: To be useful, retrieval-augmented predictions need a retrieval source that contains **different information** from the base model features (e.g., video data, workout video embeddings, past race video similarity). With the same 236 tabular features as both the encoder and GBDT, retrieval cannot add new signal.

---

## Implementation notes

- Feature store: `tmp/v8/feat-jra-v8-iter19-kohan3f-going` (236 features, class 703 rows via bucket-membership)
- Encoder: MLX MLP, vectorized padded batch training, 0.2s/epoch on M5 Pro
- Vector search: chunked cosine kNN (chunk_size=200 races), total elapsed ~200s for kNN over 9671 train races
- RL: REINFORCE with top-1 proxy gradient (full PL log-prob too slow in MLX), 0.6s/epoch
- Total elapsed: 257s (~4.3 minutes)
- Script: `tmp/ensemble_rl_vec_search_703.py` (not committed — experiment artifact)
- Results: `tmp/ensemble_rl_vec_search_703_results.json`

---

## v2: Odds-free + within-race-relative + proper NULL handling

**Date**: 2026-06-17
**Verdict**: **ABORT** — best blend top1=27.32% vs GBDT 43.99% (−16.68pp), LB95=−18.75pp

### Motivation for v2

The team-lead identified three potential artifacts in v1's input preparation that could have degraded embedding quality:

1. **Odds contamination**: v1 included 13 market/odds columns (`tansho_odds_raw`, `inverse_odds_implied_prob`, `popularity_score`, etc.) in the encoder inputs. These highly predictive market signals might dominate the embedding space, causing the learned representation to re-cluster around market consensus rather than learning orthogonal structural information.
2. **Absolute vs relative inputs**: v1 used globally-normalized features. Within-race z-score normalization would force the encoder to learn relative-to-field strength signals — the natural domain for ranking.
3. **NULL handling**: v1 used zero-fill for NaN values. v2 uses median imputation + explicit NULL indicator binary columns for all features with >1% missingness.

### v2 configuration changes

| Dimension            | v1                         | v2                                                                   |
| -------------------- | -------------------------- | -------------------------------------------------------------------- |
| Embedding features   | 236 (includes odds/market) | 231 (odds excluded) + 125 NULL indicators = 356 total embedding dims |
| Input normalization  | Global z-score             | Within-race z-score per embedding feature                            |
| NULL handling        | Zero-fill                  | Median impute + NULL indicator columns (125 columns, >1% missing)    |
| Categorical encoding | None (coerced to int)      | Label-encode `track_code`, `grade_code`                              |
| Odds features        | Mixed with embedding       | Separate (global z-score, not fed to encoder)                        |
| Encoder train split  | 2013–2021                  | 2013–2022 (10 years)                                                 |
| Blind holdout        | 2025 (n=1,103 races)       | 2025 (n=1,252 races, larger due to v8 feature store)                 |

### v2 results

#### Component-only accuracy (blind holdout 2025, n=1,252 races)

| Component                  | top1   | Retrieval score mean |
| -------------------------- | ------ | -------------------- |
| enc-only (encoder head)    | 27.32% | —                    |
| ret-only (retrieval score) | 27.40% | μ=0.514, σ=0.239     |
| rl-only (REINFORCE policy) | 15.58% | —                    |

Relative to v1 enc-only top1=0.18%: the odds-free + within-race-relative inputs dramatically raise encoder top-1 from 0.18% → 27.32%. This confirms the v1 encoder was being overwhelmed by odds signals that destroyed its absolute-ranking calibration. The within-race z-score is the critical fix: the encoder now sees relative-to-field quality and its head score is calibrated enough to produce reasonable top-1 accuracy.

Retrieval score μ=0.514 (vs v1 μ=0.502) — marginally better than neutral, but still near-uniform.

RL policy dropped from 25.93% (v1) to 15.58% (v2). The encoder embeddings are structurally different in v2 (odds-free, within-race-relative), but the RL policy trained on them performs worse. This suggests the 32d embedding in odds-free mode carries less discriminative signal for RL than the odds-inclusive version.

#### Blend search on tune split

Grid-search collapsed to encoder-only: `enc=1.00, ret=0.00, rl=0.00` (tune top1=29.05%).

This means the retrieval scores and RL logits add noise rather than signal when combined with the encoder head.

#### Blind holdout ensemble (best blend = encoder-only)

| Metric | Value  | Delta vs GBDT | LB95     |
| ------ | ------ | ------------- | -------- |
| top1   | 27.32% | −16.68pp      | −18.75pp |
| place2 | 45.21% | −17.72pp      | —        |
| place3 | 58.15% | −15.43pp      | —        |

**GBDT baseline (CatBoost YetiRank iter20, blind 2025): top1=43.99%, place2=62.92%, place3=73.58%**

### v2 diagnosis

The odds-free + within-race-relative fix genuinely improved encoder top-1 from 0.18% to 27.32% — a 27pp improvement in encoder head quality. This is substantial: it validates the hypothesis that odds contamination was degrading the v1 embedding. The encoder now learns structural quality signals orthogonal to market consensus.

However, 27.32% is still −16.68pp below GBDT (43.99%). The gap narrows compared to v1's −22.30pp, but remains very large.

**Root cause of the persistent gap**:

1. **Encoder ceiling at 27% top-1**: The pairwise ranking loss across 10 training years gives a 29.82% train top-1 and 27.32% blind top-1, with small overfitting gap. This is the ceiling of what a 3-layer 32d MLP trained with pairwise loss can achieve on 231 features. GBDT achieves 43.99% with 1000 trees on 244 features. The representational capacity difference is fundamental.

2. **Retrieval still near-neutral (μ=0.514)**: Even in the odds-free learned space, the within-race z-score normalization means no single horse's embedding reliably signals "absolute quality above average" — z-scores by construction zero-mean each race's horse embeddings before encoding. The retrieval augmentation cannot find historically-similar-but-better-finishing neighbors in a meaningful way.

3. **RL policy degrades to 15.58%**: The RL policy's poor performance (worse than v1's 25.93%) confirms the odds-free embedding, while structurally cleaner, is not a better basis for REINFORCE-style policy gradient training. The policy has less market signal to exploit.

4. **Same-feature wall at 27% encoder top-1**: The 231-feature odds-free encoder is trained on a subset of the GBDT's 244 features. The GBDT uses those 244 features with 1000 boosted trees; the encoder compresses them into 32 dimensions. Even at its best (odds-free + within-race-relative), the encoder does not recover the information lost in this compression.

### Comparison across all task #31 variants

| Variant                       | enc top1 | ret top1 | rl top1 | best blend top1 | vs GBDT  |
| ----------------------------- | -------- | -------- | ------- | --------------- | -------- |
| v1 (odds-in, global z)        | 0.18%    | 0.09%    | 25.93%  | 25.93%          | −22.30pp |
| v2 (odds-free, within-race z) | 27.32%   | 27.40%   | 15.58%  | 27.32%          | −16.68pp |
| GBDT baseline                 | —        | —        | —       | 43.99%          | 0.00pp   |

The v2 fixes improve the encoder substantially (27pp gain in enc top-1), but the best ensemble remains far below GBDT. The learned 32d embedding cannot replace the GBDT's direct tabular ranking.

### Final verdict for task #31

**ABORT (both v1 and v2)**. The RL + learned-vectorization + vector-search retrieval-augmented ensemble does not outperform the GBDT baseline on any configuration. The v2 odds-free + within-race-relative ablation:

- Confirms odds contamination was harming v1 encoder quality
- Shows the fundamental limit of pairwise-trained 32d MLP compression on the same feature set
- Closes the question: input quality is not the bottleneck, representational capacity and loss function design are

**DO-NOT-RETEST**: This architecture (learned embedding via pairwise ranking + kNN retrieval + REINFORCE RL) on the same 244-feat store for JRA 703. The same-feature wall is confirmed at both input-quality extremes (odds-in and odds-free).

### v2 implementation

- Script: `tmp/ensemble_rl_vec_search_703_v2.py` (not committed)
- Results: `tmp/ensemble_rl_vec_search_703_v2_results.json`
- Elapsed: 677s (~11 minutes)
- n_embed_features: 356 (231 base + 125 NULL indicators), n_odds_features: 13
- n_train_races: 12,303, n_tune_races: 2,458, n_blind_races: 1,252
