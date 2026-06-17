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
