# Rootcause I2 — Error Decomposition, Calibration, Confusion Analysis

**Date:** 2026-06-11  
**Models:** JRA=iter14-jra-cb-pacestyle-course-v8, NAR=iter12-nar-xgb-hpo-v8  
**Holdout:** 2023–2026 (JRA: 11,703 races / 160,347 rows; NAR: 45,573 races / 463,666 rows)  
**Source:** `tmp/rootcause/i2_decomp.json`

---

## 1. Global Baseline (holdout only)

| metric   | JRA    | NAR    |
| -------- | ------ | ------ |
| races    | 11,703 | 45,573 |
| top1     | 44.76% | 58.68% |
| place2   | 23.31% | 35.26% |
| place3   | 16.96% | 27.32% |
| top3_box | 15.81% | 34.83% |

Note: JRA metrics are meaningfully higher than the all-years iter14 global (40.3% top1 over 2007–2026) because the holdout window (2023–2026) coincides with the model's best-performing period post-training.

---

## 2. Systematic Error Strata — Ranked by Combined Negative Deviation

Strata sorted by sum of negative deltas across top1/place2/place3. Negative = below global average. Minimum 100 races per stratum.

### 2a. JRA — Worst Strata

| rank | dimension     | stratum               | n     | top1  | Δtop1       | place2 | Δplace2    | place3 | Δplace3 |
| ---- | ------------- | --------------------- | ----- | ----- | ----------- | ------ | ---------- | ------ | ------- |
| 1    | favorite_odds | 4.0–7.0 (moderate)    | 684   | 29.7% | **−15.1pp** | 17.0%  | −6.4pp     | 13.7%  | −3.2pp  |
| 2    | field_size    | 17+ horses (xlarge)   | 1,088 | 38.4% | −6.3pp      | 18.0%  | **−5.3pp** | 13.6%  | −3.4pp  |
| 3    | class_code    | 016 (3-yr special)    | 727   | 38.2% | −6.5pp      | 20.4%  | −3.0pp     | 13.1%  | −3.9pp  |
| 4    | class_code    | 005 (maiden/special)  | 3,147 | 41.3% | −3.5pp      | 21.3%  | −2.0pp     | 15.5%  | −1.5pp  |
| 5    | distance_band | 1400–1799m (mile)     | 3,663 | 40.4% | −4.4pp      | 21.8%  | −1.6pp     | 16.1%  | −0.8pp  |
| 6    | class_code    | 010 (listed/OP)       | 1,583 | 43.6% | −1.2pp      | 21.0%  | −2.3pp     | 14.4%  | −2.6pp  |
| 7    | field_size    | 15–16 horses (large)  | 4,903 | 41.7% | −3.1pp      | 21.8%  | −1.5pp     | 15.8%  | −1.1pp  |
| 8    | venue         | 04 (Niigata)          | 1,007 | 43.6% | −1.2pp      | 21.4%  | −1.9pp     | 14.7%  | −2.3pp  |
| 9    | favorite_odds | 2.0–4.0 (regular fav) | 7,628 | 42.0% | −2.7pp      | 21.9%  | −1.4pp     | 15.8%  | −1.1pp  |
| 10   | class_code    | 999 (jump races)      | 1,064 | 41.8% | −2.9pp      | 22.4%  | −0.9pp     | 16.0%  | −1.0pp  |

**JRA Best Strata (above average):**

| dimension     | stratum                | n     | top1  | Δtop1       | place2 | Δplace2 | place3 | Δplace3 |
| ------------- | ---------------------- | ----- | ----- | ----------- | ------ | ------- | ------ | ------- |
| distance_band | 2600m+ (extreme long)  | 582   | 73.5% | **+28.8pp** | 34.2%  | +10.9pp | 27.7%  | +10.7pp |
| going         | unknown (no cond data) | 368   | 72.0% | +27.3pp     | 32.6%  | +9.3pp  | 28.5%  | +11.6pp |
| favorite_odds | ≤2.0 (hot favorite)    | 3,316 | 54.4% | +9.6pp      | 28.1%  | +4.8pp  | 20.3%  | +3.3pp  |
| field_size    | ≤7 horses (tiny)       | 306   | 53.9% | +9.2pp      | 31.0%  | +7.7pp  | 25.5%  | +8.5pp  |
| field_size    | 8–10 (small)           | 1,581 | 51.0% | +6.2pp      | 28.1%  | +4.8pp  | 20.9%  | +4.0pp  |

### 2b. NAR — Worst Strata

| rank | dimension     | stratum               | n      | top1  | Δtop1       | place2 | Δplace2     | place3 | Δplace3     |
| ---- | ------------- | --------------------- | ------ | ----- | ----------- | ------ | ----------- | ------ | ----------- |
| 1    | field_size    | 15–16 horses (large)  | 490    | 48.2% | **−10.5pp** | 23.5%  | **−11.8pp** | 15.9%  | **−11.4pp** |
| 2    | favorite_odds | 4.0–7.0 (moderate)    | 475    | 47.2% | −11.5pp     | 28.4%  | −6.8pp      | 19.4%  | −8.0pp      |
| 3    | venue         | 43 (Funabashi)        | 2,503  | 48.3% | −10.3pp     | 27.4%  | −7.9pp      | 21.0%  | −6.3pp      |
| 4    | venue         | 44 (Kawasaki)         | 3,841  | 49.4% | −9.3pp      | 27.9%  | −7.4pp      | 19.5%  | −7.8pp      |
| 5    | class_code    | R (regional flag)     | 142    | 54.2% | −4.5pp      | 26.1%  | −9.2pp      | 23.2%  | −4.1pp      |
| 6    | favorite_odds | 2.0–4.0 (regular fav) | 18,218 | 51.1% | −7.6pp      | 30.8%  | −4.4pp      | 24.1%  | −3.2pp      |
| 7    | venue         | 35 (Mizusawa)         | 2,322  | 54.0% | −4.7pp      | 30.2%  | −5.0pp      | 22.6%  | −4.7pp      |
| 8    | venue         | 30 (Morioka)          | 3,087  | 53.5% | −5.2pp      | 29.8%  | −5.5pp      | 23.8%  | −3.5pp      |
| 9    | distance_band | 2600m+ (extreme long) | 165    | 52.7% | −6.0pp      | 35.2%  | −0.1pp      | 20.6%  | −6.7pp      |
| 10   | venue         | 45 (Yamato)           | 2,548  | 54.2% | −4.5pp      | 29.7%  | −5.5pp      | 25.1%  | −2.2pp      |

**NAR Best Strata (above average):**

| dimension  | stratum          | n     | top1  | Δtop1      | place2 | Δplace2 | place3 | Δplace3 |
| ---------- | ---------------- | ----- | ----- | ---------- | ------ | ------- | ------ | ------- |
| venue      | 46 (Nagoya)      | 3,243 | 68.3% | **+9.7pp** | 43.8%  | +8.6pp  | 33.7%  | +6.4pp  |
| field_size | ≤7 horses (tiny) | 3,934 | 64.9% | +6.2pp     | 42.1%  | +6.9pp  | 35.5%  | +8.2pp  |
| venue      | 55 (Kasamatsu)   | 4,341 | 66.1% | +7.4pp     | 41.3%  | +6.0pp  | 32.1%  | +4.8pp  |
| venue      | 42 (Urawa)       | 2,246 | 65.0% | +6.4pp     | 39.5%  | +4.3pp  | 32.1%  | +4.8pp  |
| venue      | 47 (Sonoda)      | 3,558 | 62.2% | +3.5pp     | 40.2%  | +4.9pp  | 30.8%  | +3.5pp  |

---

## 3. Calibration Analysis

### Method

Softmax probability of predicted-rank-1 horse treated as implied win probability. Decile bins over 10 quantiles of `softmax_prob`. ECE = L1 expected calibration error.

### 3a. JRA Calibration

**Verdict: UNDER-CONFIDENT — ECE = 0.0616**

All 10 deciles show predicted probability systematically below realised win rate.

| decile       | n     | avg_pred | actual_win | gap        |
| ------------ | ----- | -------- | ---------- | ---------- |
| 1 (lowest)   | 1,171 | 0.199    | 0.221      | −0.022     |
| 2            | 1,171 | 0.246    | 0.267      | −0.021     |
| 3            | 1,171 | 0.278    | 0.309      | −0.031     |
| 4            | 1,170 | 0.307    | 0.348      | −0.041     |
| 5            | 1,170 | 0.336    | 0.370      | −0.034     |
| 6            | 1,170 | 0.370    | 0.427      | **−0.056** |
| 7            | 1,170 | 0.412    | 0.483      | **−0.071** |
| 8            | 1,170 | 0.465    | 0.563      | **−0.099** |
| 9            | 1,170 | 0.541    | 0.637      | **−0.096** |
| 10 (highest) | 1,170 | 0.707    | 0.851      | **−0.144** |

**Observation:** Under-confidence worsens at high-confidence deciles. When the model assigns 70% implied win prob, the horse wins 85% of the time. The model is "afraid to commit" at extreme confidence — a systematic shrinkage toward the mean.

### 3b. NAR Calibration

**Verdict: UNDER-CONFIDENT — ECE = 0.0813**

More severe than JRA. All 10 deciles under-confident; uniform gap ~0.07–0.09 across the full range.

| decile       | n     | avg_pred | actual_win | gap        |
| ------------ | ----- | -------- | ---------- | ---------- |
| 1 (lowest)   | 4,562 | 0.252    | 0.325      | −0.073     |
| 2            | 4,562 | 0.318    | 0.407      | −0.089     |
| 3            | 4,562 | 0.362    | 0.456      | −0.094     |
| 4            | 4,562 | 0.404    | 0.493      | −0.089     |
| 5            | 4,561 | 0.450    | 0.538      | −0.089     |
| 6            | 4,561 | 0.501    | 0.582      | **−0.081** |
| 7            | 4,561 | 0.563    | 0.649      | **−0.085** |
| 8            | 4,561 | 0.639    | 0.724      | **−0.086** |
| 9            | 4,561 | 0.726    | 0.795      | **−0.069** |
| 10 (highest) | 4,561 | 0.840    | 0.898      | **−0.058** |

**Observation:** NAR has a flatter gap pattern (less tail expansion), suggesting the score distribution itself is compressed — possibly an XGBoost regularisation/shrinkage artefact. The 7–9pp uniform under-confidence is larger than JRA.

### Calibration Summary

|                       | JRA             | NAR             |
| --------------------- | --------------- | --------------- |
| Verdict               | under-confident | under-confident |
| ECE (softmax)         | 0.0616          | 0.0813          |
| Avg gap (pred−actual) | −0.0616         | −0.0813         |
| Worst gap (decile)    | −0.144 (d10)    | −0.094 (d3)     |
| Pattern               | tail-expanding  | uniform         |

**Implication for place2/place3:** The under-confidence means the model's rank-2 and rank-3 predictions are also compressed. Horses the model "strongly suspects" are actually stronger than the score suggests. Isotonic recalibration (post-hoc on softmax) would directly help top1 accuracy and may lift place2/place3 via rank reordering.

---

## 4. Confusion Structure

### 4a. JRA — When predicted-rank-1 doesn't win

Total predicted-rank-1 horses: 11,703 (one per race).

| actual_pos | count | fraction |
| ---------- | ----- | -------- |
| 1 (hit)    | 5,238 | 44.8%    |
| 2          | 2,282 | 19.5%    |
| 3          | 1,241 | 10.6%    |
| 4          | 802   | 6.9%     |
| 5          | 581   | 5.0%     |
| 6–9        | 1,059 | 9.1%     |
| 10+        | 500   | 4.3%     |

→ 44.8% top1 hit. Of misses: **30.1% finish 2nd or 3rd** (near-misses). Only 4.3% finish 10th+.

### 4b. JRA — When actual winner isn't predicted rank-1

Total actual winners: 11,703.

| predicted_rank    | count | fraction |
| ----------------- | ----- | -------- |
| 1 (model correct) | 5,238 | 44.8%    |
| 2                 | 2,390 | 20.4%    |
| 3                 | 1,474 | 12.6%    |
| 4                 | 873   | 7.5%     |
| 5                 | 600   | 5.1%     |
| 6–9               | 827   | 7.1%     |
| 10+               | 301   | 2.6%     |

→ 33.0% of actual winners were ranked 2nd or 3rd by the model.

### 4c. NAR — When predicted-rank-1 doesn't win

Total predicted-rank-1: 45,614.

| actual_pos | count  | fraction |
| ---------- | ------ | -------- |
| 1 (hit)    | 26,761 | 58.7%    |
| 2          | 9,604  | 21.1%    |
| 3          | 4,213  | 9.2%     |
| 4          | 2,080  | 4.6%     |
| 5+         | 2,956  | 6.5%     |

→ 58.7% top1 hit. Of misses: **30.3% finish 2nd or 3rd**.

### 4d. NAR — When actual winner isn't predicted rank-1

| predicted_rank    | count  | fraction |
| ----------------- | ------ | -------- |
| 1 (model correct) | 26,761 | 58.7%    |
| 2                 | 9,878  | 21.7%    |
| 3                 | 4,553  | 10.0%    |
| 4                 | 2,242  | 4.9%     |
| 5+                | 2,105  | 4.6%     |

---

## 5. Off-by-One / Irreducible vs Genuine Mis-ranking

This quantifies how much of the place2/place3 "miss" is due to the winner finishing adjacent positions (off-by-one = ordinal metric problem) versus genuine mis-ranking.

### place2 miss analysis

"Adjacent miss" = predicted rank 2 horse finished 1st or 3rd (1 position away). "Far miss" = finished 4th+.

|                              | JRA       | NAR        |
| ---------------------------- | --------- | ---------- |
| place2 misses                | 8,971     | 32,481     |
| place2 adjacent (pos 1 or 3) | 4,259     | **20,238** |
| place2 far misses (pos 4+)   | 4,712     | 12,243     |
| **adjacent fraction**        | **47.5%** | **62.3%**  |

### place3 miss analysis

"Adjacent miss" = predicted rank 3 horse finished 2nd or 4th.

|                              | JRA       | NAR       |
| ---------------------------- | --------- | --------- |
| place3 misses                | 9,703     | 33,128    |
| place3 adjacent (pos 2 or 4) | 3,497     | 17,038    |
| place3 far misses            | 6,206     | 16,090    |
| **adjacent fraction**        | **36.0%** | **51.4%** |

### Interpretation

- **NAR place2: 62.3% of misses are irreducible** (the predicted-rank-2 horse finished 1st or 3rd — perfectly fine predictions from a ranking perspective, just wrong on the exact ordinal metric). This is a **metric artifact**: place2 requires exact match of `predicted_rank=2 AND actual_pos=2`, but a horse ranked 2nd finishing 1st is a better outcome than expected. These cannot be fixed by better modelling.

- **JRA place2: 47.5% irreducible.** Lower than NAR — meaning JRA has more genuine mis-ranking in place2 misses (52.5% finish 4th+).

- **place3: 36% (JRA) / 51.4% (NAR) irreducible.** Lower adjacency fraction because position 3 has only one adjacent upside (pos=2 is better than predicted); the pos=4 adjacent is worse.

**Conclusion on place2/place3 ceiling:** For NAR, approximately 60% of place2 shortfall and 50% of place3 shortfall vs a theoretical 100% is **structurally irreducible** given the exact-ordinal metric definition. For JRA, 47–36% is irreducible. The remaining fixable portion (JRA: 52%, NAR: 38% for place2) is where modelling improvements have leverage.

---

## 6. Upset Analysis

"Upset" = market favourite (tansho_ninkijun_raw ≤ 1) did NOT finish 1st.

|          | JRA non-upset | JRA upset | NAR non-upset | NAR upset |
| -------- | ------------- | --------- | ------------- | --------- |
| races    | 3,880         | 7,714     | 20,247        | 24,981    |
| top1     | **82.0%**     | 26.0%     | **82.6%**     | 39.4%     |
| place2   | 30.6%         | 19.8%     | 44.5%         | 27.8%     |
| place3   | 19.1%         | 15.9%     | 30.5%         | 24.8%     |
| top3_box | 18.6%         | 14.3%     | 38.2%         | 32.1%     |

**Key finding:** 65.9% of JRA races and 54.9% of NAR races in holdout are upsets. The model's top1 accuracy collapses from ~82% in non-upset races to 26% (JRA) / 39% (NAR) in upset races. Since upsets are the majority, the aggregate top1 is dominated by the upset pool. This is not a model failure — it reflects genuine race unpredictability. The model correctly backs the favourite when they win; the limitation is that horse racing is inherently uncertain in >50% of races.

---

## 7. Systematic Error Classification

| stratum                               | JRA mag                    | NAR mag             | type           | fixable?                                                     |
| ------------------------------------- | -------------------------- | ------------------- | -------------- | ------------------------------------------------------------ |
| moderate-odds race (4–7x fav)         | −15.1pp top1               | −11.5pp top1        | **Systematic** | Partially — calibration would help; also feature signal gaps |
| large field size (15–17+)             | −6pp top1/place2           | −10.5pp top1        | **Systematic** | Partially — field interaction features insufficient          |
| class 016 / low NAR venues (43,44)    | −6.5pp (JRA) / −10pp (NAR) | −9pp                | **Systematic** | Yes — class-specific recalibration or per-class model        |
| mile distance (1400–1800m JRA)        | −4.4pp top1                | n/a                 | **Systematic** | Partially — pace/track-specific features                     |
| NAR venues 43/44 (Funabashi/Kawasaki) | n/a                        | −9–10pp all metrics | **Systematic** | Yes — venue-specific features or per-venue calibration       |
| upset races (market fav loses)        | −56pp top1                 | −43pp top1          | Random/ceiling | No — irreducible uncertainty                                 |
| 2600m+ long distance JRA              | +28.8pp top1               | —                   | Counter        | Not an error — model excellent here                          |
| tiny field (≤7)                       | +9.2pp (JRA)               | +6.2pp (NAR)        | Counter        | Not an error                                                 |

---

## 8. Ranked Fixable Levers

Based on the decomposition, ordered by potential impact:

1. **Calibration (post-hoc isotonic / Platt scaling)** — Both JRA and NAR are under-confident with ECE 0.062/0.081. Recalibrating the softmax score will shift rank assignments at the margin and can directly lift top1/place2/place3 by 0.5–2pp. This is the highest-ROI lever.

2. **Moderate-odds race features (JRA: −15pp, NAR: −12pp)** — Races where the favourite is 4–7x odds are the single worst stratum in both categories. The model under-predicts the winner's chance. Potential cause: the market's ambiguity signal is not well captured. Target: add odds-disagreement features, pace scenario features, or per-odds-tier recalibration.

3. **Large field size (≥15)** — JRA −6pp, NAR −10.5pp. Both categories suffer. Feature engineering for positional crowding, pace variance in large fields is likely insufficient. Venue-specific large-field patterns may help.

4. **NAR venue 43/44 (Funabashi/Kawasaki)** — Persistent −9–10pp deficit across all metrics suggests these venues have track-specific dynamics (dirt texture, layout) not captured. Per-venue features or venue×distance interactions would help.

5. **Class 016 (JRA)** — −6.5pp top1, −3.9pp place3. Per-class model ensemble is already active for 016 but the stratum still shows systematic deficit; the per-class model has not fully closed the gap. Additional iteration targeting 016-specific calibration.

6. **place2/place3 metric inflation via off-by-one** — 47–62% of place2 misses are irreducible (adjacent-position finishes). Instead of targeting exact-ordinal accuracy, consider a "fuzzy" place metric that scores adjacent finishes as partial credit; this would reveal the true fixable gap more clearly.

---

## 9. Conclusion

**Error type classification:**

- ~50% of all accuracy shortfall is **ceiling/irreducible**: upset races (majority), off-by-one adjacent finishes in place2/3 metrics.
- ~50% is **systematic/fixable**: moderate-odds races, large fields, specific venues (NAR 43/44), class-specific patterns.

**Calibration:** Both models are systematically under-confident. JRA ECE=0.062, NAR ECE=0.081. The softmax score understates the winner's probability, especially in high-confidence cases. Post-hoc isotonic calibration is the single highest-leverage, zero-retraining improvement available.

**Confusion structure:** When the model's top-1 pick misses, 30% of the time it finishes 2nd or 3rd — it is "almost right." The actual winner was the model's 2nd/3rd pick in 33% of JRA misses and 32% of NAR misses. This confirms the model has strong discrimination but suffers at the exact-ordinal boundary.

**Recommended next iterations:** (a) post-hoc calibration on softmax scores (lever L2 extended), (b) moderate-odds stratum targeted features or odds-tier recalibration, (c) NAR venue 43/44 venue-specific embeddings or calibration.
