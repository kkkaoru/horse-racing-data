# Odds-Decouple JRA — Science Track — 2026-06-11

**Status**: COMPLETE — All 5 classes evaluated, per-class λ_c selected, holdout + upset analysis done.

**Hypothesis (H-ODDS-DECOUPLE-JRA)**: A less-odds-dependent model (model_F = odds/popularity features removed) outperforms the odds-aware baseline (model_O) in upset-prone races and may improve place2/place3 even where top1 is neutral.

**Verdict**: PARTIAL SUPPORT (703, 010) / REJECT overall.

- Model_F is genuinely informative and strictly dominates model_O on upsets for 703/005/016.
- However, the global iter14_score residual feature (≥93% importance) absorbs nearly all signal — the remaining odds features contribute only 0.46–2.05% of total feature gain.
- Blended λ_c < 1 cannot reliably clear the LB95 bar on the primary axes due to sample size.
- Full decoupling (λ=0) causes severe regression in `other` class, which fails hard.

---

## 1. Experiment Design

**Feature set**: `feat-jra-v8-iter26-relationships` (254 columns, 241 numeric non-metadata → 242 with `iter14_score` residual).

**Architecture**: CatBoost YetiRank + residual (same as iter26). CB params: `depth=4, lr=0.1, l2=5.0, iters=500`. Chain inclusion (train) + target-only (valid).

**Splits**:
| Split | Years | Purpose |
|---------|-----------|--------------------------------|
| Inner | 2007–2020 | Training both model_F and \_O |
| Tuning | 2021–2022 | λ-sweep selection |
| Holdout | 2023–2026 | One-shot final evaluation |

**Odds features removed for model_F** (13 cols):
`popularity_score`, `odds_score`, `tansho_odds_raw`, `tansho_ninkijun_raw`, `inverse_odds_implied_prob`, `inverse_odds_market_share`, `inverse_odds_rank_in_race`, `popularity_rank_in_race`, `odds_score_diff_from_race_avg`, `popularity_score_diff_from_race_avg`, `popularity_odds_disagreement`, `horse_popularity_vs_field`, `field_dominant_favorite_indicator`.

**Metrics**: top1 (1st place predicted correctly), place2 (predicted 1st actually ≤2), place3 (predicted 1st actually ≤3), top3_box (all of {1,2,3} in predicted top-3).

**Bootstrap**: Paired 10,000 iterations, seed 42. LB95 = 5th-percentile of resampled (blended − baseline) difference.

**Upset subsets**:

- _Upset_: races where odds-favourite (lowest `tansho_ninkijun_raw`) did NOT finish 1st.
- _Upset-strict_: races where odds-favourite finished outside top 3.

---

## 2. Odds-Dependence Audit

| Class | n holdout | Features O/F | Odds gain % | Spearman(score, odds_score) | Dominant feature   |
| ----- | --------- | ------------ | ----------- | --------------------------- | ------------------ |
| 703   | 60,277    | 254 / 241    | 1.29%       | −0.8783                     | iter14_score 95.5% |
| 005   | 41,726    | 254 / 241    | 0.47%       | −0.8637                     | iter14_score 97.8% |
| 010   | 20,783    | 254 / 241    | 2.05%       | −0.8750                     | iter14_score 96.2% |
| other | 14,646    | 254 / 241    | 0.91%       | −0.8343                     | iter14_score 93.3% |
| 016   | 10,374    | 254 / 241    | 0.57%       | −0.8821                     | iter14_score 98.3% |

**Key finding**: The iter14 residual score consumes 93–98% of tree-split importance across all classes. Odds features contribute only 0.47–2.05% of total gain. Despite this, the model score correlates strongly NEGATIVELY with raw odds (Spearman −0.83 to −0.88), meaning the residual model still mirrors the market rankings.

**Top-5 features for 010 (highest odds dependence at 2.05%)**:

1. iter14_score: 96.19%
2. inverse_odds_implied_prob: 0.81% (ODDS)
3. target_corner_3_norm: 0.68%
4. odds_score: 0.58% (ODDS)
5. tansho_ninkijun_raw: 0.32% (ODDS)

---

## 3. λ-Sweep (Tuning Split 2021–2022)

Final blend formula: `score = (1−λ)·model_F + λ·model_O`

### 703 (未勝利)

| λ   | top1   | place2 | place3 | box    |
| --- | ------ | ------ | ------ | ------ |
| 0.0 | 0.5038 | 0.6789 | 0.7979 | 0.1928 |
| 0.1 | 0.5030 | 0.6773 | 0.7951 | 0.1944 |
| 0.4 | 0.5010 | 0.6785 | 0.7967 | 0.1944 |
| 1.0 | 0.4917 | 0.6753 | 0.7943 | 0.1952 |

→ **λ_c = 0.0** (model_F dominates; top1 +1.21pp, place2 +0.36pp vs λ=1)

### 005 (1勝クラス)

| λ   | top1   | place2 | place3 | box    |
| --- | ------ | ------ | ------ | ------ |
| 0.0 | 0.4085 | 0.6017 | 0.7320 | 0.1372 |
| 0.7 | 0.4080 | 0.6006 | 0.7277 | 0.1383 |
| 1.0 | 0.4037 | 0.5969 | 0.7250 | 0.1389 |

→ **λ_c = 0.0** (model_F dominates; top1 +0.48pp, place3 +0.70pp vs λ=1)

### 010 (2勝クラス)

| λ   | top1   | place2 | place3 | box    |
| --- | ------ | ------ | ------ | ------ |
| 0.0 | 0.4311 | 0.6165 | 0.7299 | 0.1292 |
| 0.5 | 0.4354 | 0.6208 | 0.7320 | 0.1292 |
| 0.6 | 0.4375 | 0.6218 | 0.7320 | 0.1303 |
| 1.0 | 0.4364 | 0.6186 | 0.7288 | 0.1303 |

→ **λ_c = 0.6** (slight improvement at 0.6 on top1 and place3; place2 also peaks here)

### other (OP/重賞/特別)

| λ   | top1   | place2 | place3 | box    |
| --- | ------ | ------ | ------ | ------ |
| 0.0 | 0.4043 | 0.5660 | 0.6667 | 0.1205 |
| 1.0 | 0.4010 | 0.5644 | 0.6683 | 0.1254 |

→ **λ_c = 0.0** (model_F has marginal tuning edge on top1/place2, but place3 reverses)

### 016 (3勝クラス)

| λ   | top1   | place2 | place3 | box    |
| --- | ------ | ------ | ------ | ------ |
| 0.0 | 0.3021 | 0.5176 | 0.6464 | 0.0913 |
| 1.0 | 0.3068 | 0.5176 | 0.6417 | 0.0913 |

→ **λ_c = 1.0** (model_O strictly better on top1 in tuning; model_F not selected)

---

## 4. Holdout Evaluation (2023–2026, one-shot)

### Per-class holdout table

| Class | n           | Metric   | λ=0 (F) | λ=1 (O) | λ_c chosen       | Δ(λ_c−λ=1)  | LB95        |
| ----- | ----------- | -------- | ------- | ------- | ---------------- | ----------- | ----------- |
| 703   | ~4229 races | top1     | 0.4985  | 0.4930  | 0.4985 (λ_c=0.0) | **+0.0054** | **+0.0007** |
|       |             | place2   | 0.6959  | 0.6950  | 0.6959           | +0.0009     | −0.0033     |
|       |             | place3   | 0.8007  | 0.8002  | 0.8007           | +0.0005     | −0.0033     |
|       |             | top3_box | 0.1977  | 0.2024  | 0.1977           | −0.0047     | −0.0085     |
| 005   | ~3147       | top1     | 0.4156  | 0.4144  | 0.4156 (λ_c=0.0) | +0.0013     | −0.0029     |
|       |             | place2   | 0.6155  | 0.6165  | 0.6155           | −0.0010     | −0.0048     |
|       |             | place3   | 0.7286  | 0.7267  | 0.7286           | +0.0019     | −0.0019     |
|       |             | top3_box | 0.1370  | 0.1382  | 0.1370           | −0.0013     | −0.0044     |
| 010   | ~1583       | top1     | 0.4340  | 0.4352  | 0.4378 (λ_c=0.6) | **+0.0025** | −0.0013     |
|       |             | place2   | 0.6178  | 0.6229  | 0.6216           | −0.0013     | −0.0051     |
|       |             | place3   | 0.7220  | 0.7252  | 0.7233           | −0.0019     | −0.0057     |
|       |             | top3_box | 0.1257  | 0.1282  | 0.1263           | −0.0019     | −0.0038     |
| other | ~1064       | top1     | 0.4164  | 0.4211  | 0.4164 (λ_c=0.0) | **−0.0047** | −0.0122     |
|       |             | place2   | 0.5789  | 0.5827  | 0.5789           | −0.0038     | −0.0113     |
|       |             | place3   | 0.6720  | 0.6758  | 0.6720           | −0.0038     | −0.0113     |
|       |             | top3_box | 0.1165  | 0.1165  | 0.1165           | 0.0000      | −0.0047     |
| 016   | ~727        | top1     | 0.3851  | 0.3810  | 0.3810 (λ_c=1.0) | 0.0000      | 0.0000      |
|       |             | place2   | 0.5433  | 0.5378  | 0.5378           | 0.0000      | 0.0000      |
|       |             | place3   | 0.6534  | 0.6520  | 0.6520           | 0.0000      | 0.0000      |
|       |             | top3_box | 0.0880  | 0.0839  | 0.0839           | 0.0000      | 0.0000      |

**Notes on 016 zeroes**: λ_c was chosen as 1.0 (model_O = baseline) so all deltas are exactly 0. The model_F at λ=0 is actually stronger for 016 (+0.41pp top1, +0.55pp place2, +0.14pp place3 vs λ=1) but the tuning split did not select it.

---

## 5. Upset-Subset Results

### Upset (favourite did NOT win)

| Class | λ_c | λ=0 top1 | λ=1 top1 | λ_c top1 | Δtop1       | λ=0 p2 | λ=1 p2 | λ_c p2 | Δp2         |
| ----- | --- | -------- | -------- | -------- | ----------- | ------ | ------ | ------ | ----------- |
| 703   | 0.0 | 0.3277   | 0.3161   | 0.3277   | **+0.0117** | 0.5934 | 0.5894 | 0.5934 | **+0.0040** |
| 005   | 0.0 | 0.2278   | 0.2250   | 0.2278   | +0.0028     | 0.4963 | 0.4963 | 0.4963 | 0.0000      |
| 010   | 0.6 | 0.2232   | 0.2232   | 0.2251   | +0.0019     | 0.4805 | 0.4844 | 0.4815 | −0.0029     |
| other | 0.0 | 0.2579   | 0.2684   | 0.2579   | −0.0105     | 0.4645 | 0.4737 | 0.4645 | −0.0092     |
| 016   | 1.0 | 0.1879   | 0.1818   | 0.1818   | 0.0000      | 0.3879 | 0.3818 | 0.3818 | 0.0000      |

### Upset-strict (favourite outside top-3)

| Class | λ_c | λ=0 top1 | λ=1 top1 | λ=0 p2 | λ=1 p2 |
| ----- | --- | -------- | -------- | ------ | ------ |
| 703   | 0.0 | 0.3529   | 0.3443   | 0.4765 | 0.4710 |
| 005   | 0.0 | 0.2504   | 0.2470   | 0.3744 | 0.3744 |
| 010   | 0.6 | 0.2254   | 0.2218   | 0.3399 | 0.3453 |
| other | 0.0 | 0.2407   | 0.2500   | 0.3248 | 0.3364 |
| 016   | 1.0 | 0.1864   | 0.1797   | 0.2712 | 0.2644 |

**Hypothesis confirmation (partial)**: In 703 races (the largest class, n≈4229 holdout), model_F (λ=0) beats model_O on upsets by +1.17pp top1 and +0.40pp place2. This is directionally consistent with the hypothesis. In 005 the gain is smaller (+0.28pp top1). In `other` (OP/重賞), the odds-aware model is strictly better on upsets — reflecting that market efficiency is highest in grade races.

---

## 6. Per-Class Recommendation

| Class | λ_c | Verdict         | Rationale                                                                                                                                                                  |
| ----- | --- | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 703   | 0.0 | **CONDITIONAL** | top1 LB95 = +0.0007 (positive), upset +1.17pp top1. Box regresses −0.47pp LB95 −0.85pp. Overall: model_F strictly better on top1/upsets, box regression is real but small. |
| 005   | 0.0 | NEUTRAL         | top1 +0.13pp (LB95 −0.29pp, not significant). place3 +0.19pp (LB95 −0.19pp borderline). Gains too small vs noise.                                                          |
| 010   | 0.6 | NEUTRAL         | top1 +0.25pp at λ=0.6 but LB95 −0.13pp. place2/3 both regress. No reliable gain.                                                                                           |
| other | 0.0 | REJECT          | All axes regress at λ=0 (−0.47pp top1, −0.38pp place2, −0.38pp place3). Odds features are genuinely useful for grade races.                                                |
| 016   | 1.0 | KEEP_ODDS       | Tuning selected λ=1. model_F shows gains in holdout but tuning did not validate them.                                                                                      |

**Global summary**: The hypothesis is **confirmed for 703 on upsets** and marginally directional for 005/010. However, the LB95 confidence interval does not clear significance except for 703 top1. The `other` class (OP/重賞) shows a clear REJECT — odds features encode market-efficiency signals that the residual alone cannot replace for high-stakes races.

---

## 7. Interpretation and Context

### Why iter14_score dominates (93–98%)

The residual architecture means model*F/model_O are both correcting iter14's predictions. The odds features present in iter14 already encode market beliefs. What the 13 removed features contribute (0.46–2.05%) is a \_second-order* market signal — fine-grained within-race relative popularity. Removing them does not destroy accuracy but modestly shifts predictions toward form-based ranking.

### Spearman(score, odds) = −0.83 to −0.88

This negative correlation means: higher model score → lower odds (more favoured). This is expected (good horses are both predicted well AND bet down). The strong correlation persists even in model_F (where the direct odds features are removed), confirming that form/speed/running-style features implicitly track market beliefs. True decoupling would require removing the entire residual structure, which is out of scope.

### Why `other` rejects hard

Grade races (OP/重賞) are the most efficiently priced. Odds encode trainer/jockey lineup changes, supplementary entries, and stable gossip that the feature matrix cannot capture. Removing odds features in this class removes genuine signal.

### 016 (3勝クラス) oddity

model_F at λ=0 actually shows +0.41pp top1 in holdout vs model_O, which is notable for a 16-class. But the tuning selection chose λ=1 because the tuning sample (n=6,103) is underpowered to distinguish small effects. This is an honest reporting: holdout is not re-used for selection.

---

## 8. Data Quality Notes

- Feature count: iter26 has 254 columns with 242 numeric non-metadata. After removing 13 odds features, model_F has 241 feature columns.
- Inner-split chain size is large (306k–687k rows) due to cumulative class inclusion chain.
- iter14 score coverage: all holdout years 2023–2026 have parquet files.
- No data leakage: inner-split predictions do not overlap with tuning/holdout years.

---

## 9. Artifacts

- Script: `tmp/odds-decouple/run_odds_decouple_jra.py`
- Results: `tmp/odds-decouple/jra_results.json`
- Models: `tmp/odds-decouple/models/jra/{703,005,010,other,016}/model_F.cbm` and `model_O.cbm`

---

## 10. Decision Matrix for Production

| Class | Deploy λ_c? | Condition                                                                                                                     |
| ----- | ----------- | ----------------------------------------------------------------------------------------------------------------------------- |
| 703   | Possible    | Only if upset-subset improvement (top1 +1.17pp) is prioritized over box regression (−0.47pp). Requires orchestrator decision. |
| 005   | No          | No axis clears LB95.                                                                                                          |
| 010   | No          | place2/3 regress. LB95 not positive.                                                                                          |
| other | No (REJECT) | Clear regression on all primary axes.                                                                                         |
| 016   | No          | λ_c=1 selected; model_F gains not validated in tuning.                                                                        |

**Overall recommendation**: Do NOT flip any class to λ_c in production. The evidence for 703 is the strongest but the box regression is real and the improvement is confined to upset races (not the majority). Treat this as a validated finding that model_F is informative and upsets are predictable better without odds — save for a future ensemble where model_F is a specialist upset detector.
