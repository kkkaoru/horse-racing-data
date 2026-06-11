---
investigation: I1
title: "Headroom analysis — Model vs Market vs Oracle ceiling"
date: 2026-06-11T00:00:00+09:00
status: complete
holdout: 2023-2026
categories: [jra, nar, ban-ei]
data_source:
  model_jra: iter14-jra-cb-pacestyle-course-v8 (production; v7-lineage baseline also included)
  model_nar: iter12-nar-xgb-hpo-v8 (production; v7-lineage also included)
  model_banei: banei-cb-v7-grade (sectional+grade features)
  market: tansho_ninkijun (人気順) from jvd_se / nvd_se (confirmed odds)
  oracle_method: Harville (1973) from tansho_odds — vectorized numpy; see "Oracle Method" section
---

## Purpose

Quantify HEADROOM per metric per class/category by comparing three baselines on the
same holdout race set (2023-2026):

- **MODEL**: production ML model predictions
- **MARKET**: rank horses by tansho_ninkijun (public odds rank, 人気順)
- **ORACLE**: Harville approximation from final odds → optimal deterministic rank assignment

**Metric definitions (exact-ordinal):**

- `top1`: predicted_rank=1 AND actual_finish_position=1
- `place2`: predicted_rank=2 AND actual_finish_position=2
- `place3`: predicted_rank=3 AND actual_finish_position=3
- `top3_box`: all 3 of predicted_rank≤3 have actual_finish_position≤3

**Set-membership metrics:**

- `place2_set`: top-2 predicted contains actual 2nd horse
- `place3_set`: top-3 predicted contains actual 3rd horse
- `rentai_hit`: top-2 predicted contains BOTH actual top-2 horses (連対)
- `fukusho_2p`: ≥2 of predicted top-3 actually finished top-3
- `fukusho_3p`: all 3 of predicted top-3 actually finished top-3

## Oracle Method

1. Win probabilities: `p[i] = (1/odds[i]) / sum_j(1/odds[j])` (normalize 1/odds, removing overround)
2. Harville (1973) vectorized via numpy outer-products:
   - `P(i=2nd) = p[i] * sum_{j≠i} p[j]/(1-p[j])` (subtract self-term)
   - `P(i=3rd)` derived from full (j,k,i) triple sum, vectorized as matrix outer product
3. Oracle rank assignment: argmax P(win) → rank1; argmax P(2nd) (excluding rank1) → rank2;
   argmax P(3rd) (excluding rank1,2) → rank3; remaining assigned 4+
4. Metric applied identically to exact-ordinal and set-membership definitions

**Key assumption**: the oracle assignment is deterministic given pre-race odds.
The model surpassing the oracle does NOT mean the model is near a theoretical limit —
it means the model has captured signal BEYOND what the market alone encodes.

## Status Classification

- **MODEL_EXCEEDS_ORACLE**: model > oracle (model uses information beyond odds)
- **SATURATED**: model ≈ oracle within ±1pp (model≈Harville odds ceiling)
- **ANTI-INFORMATIVE**: model < market (possible bug, skew, or coverage gap)
- **ROOM**: oracle > model > market (genuine headroom above model toward odds ceiling)

---

## Headroom Tables

### JRA — Global (v7-lineage baseline, n=11,703 races)

| Metric     | Model% | Market% | Oracle% | Mdl−Mkt | Orc−Mdl | Status               |
| ---------- | -----: | ------: | ------: | ------: | ------: | -------------------- |
| top1       |  44.51 |   33.35 |   33.46 |  +11.16 |  −11.05 | MODEL_EXCEEDS_ORACLE |
| place2     |  22.98 |   18.04 |   18.10 |   +4.94 |   −4.88 | MODEL_EXCEEDS_ORACLE |
| place3     |  16.97 |   13.75 |   13.88 |   +3.22 |   −3.09 | MODEL_EXCEEDS_ORACLE |
| top3_box   |  15.71 |    9.31 |    9.50 |   +6.41 |   −6.21 | MODEL_EXCEEDS_ORACLE |
| place2_set |  42.57 |   37.43 |   37.68 |   +5.14 |   −4.89 | MODEL_EXCEEDS_ORACLE |
| place3_set |  43.40 |   40.37 |   40.50 |   +3.03 |   −2.90 | MODEL_EXCEEDS_ORACLE |
| rentai_hit |  24.23 |   16.39 |   16.53 |   +7.84 |   −7.71 | MODEL_EXCEEDS_ORACLE |
| fukusho_2p |  68.35 |   56.43 |   56.64 |  +11.92 |  −11.71 | MODEL_EXCEEDS_ORACLE |
| fukusho_3p |  15.65 |    9.27 |    9.47 |   +6.37 |   −6.18 | MODEL_EXCEEDS_ORACLE |

### JRA — Global (iter14 production, n=11,703 races)

| Metric     | Model% | Market% | Oracle% | Mdl−Mkt | Orc−Mdl | Status               |
| ---------- | -----: | ------: | ------: | ------: | ------: | -------------------- |
| top1       |  44.76 |   33.35 |   33.46 |  +11.41 |  −11.30 | MODEL_EXCEEDS_ORACLE |
| place2     |  23.31 |   18.04 |   18.10 |   +5.27 |   −5.21 | MODEL_EXCEEDS_ORACLE |
| place3     |  16.96 |   13.75 |   13.88 |   +3.21 |   −3.08 | MODEL_EXCEEDS_ORACLE |
| top3_box   |  15.81 |    9.31 |    9.50 |   +6.50 |   −6.31 | MODEL_EXCEEDS_ORACLE |
| place2_set |  42.79 |   37.43 |   37.68 |   +5.36 |   −5.11 | MODEL_EXCEEDS_ORACLE |
| place3_set |  43.54 |   40.37 |   40.50 |   +3.18 |   −3.04 | MODEL_EXCEEDS_ORACLE |
| rentai_hit |  24.34 |   16.39 |   16.53 |   +7.96 |   −7.82 | MODEL_EXCEEDS_ORACLE |
| fukusho_2p |  68.63 |   56.43 |   56.64 |  +12.20 |  −12.00 | MODEL_EXCEEDS_ORACLE |
| fukusho_3p |  15.74 |    9.27 |    9.47 |   +6.47 |   −6.27 | MODEL_EXCEEDS_ORACLE |

### JRA — Per Class (iter14 v7-lineage baseline, n races in holdout)

| Class         | n races | top1 Model | top1 Market | top1 Oracle | Mdl−Mkt | Status               |
| ------------- | ------: | ---------: | ----------: | ----------: | ------: | -------------------- |
| 未勝利 703    |   4,229 |      49.40 |       35.09 |       35.23 |  +14.31 | MODEL_EXCEEDS_ORACLE |
| 1勝クラス 005 |   3,147 |      40.99 |       31.90 |       32.13 |   +9.09 | MODEL_EXCEEDS_ORACLE |
| 2勝クラス 010 |   1,583 |      43.02 |       35.19 |       35.38 |   +7.83 | MODEL_EXCEEDS_ORACLE |
| 3勝クラス 016 |     727 |      37.55 |       31.91 |       31.91 |   +5.64 | MODEL_EXCEEDS_ORACLE |
| 新馬 701      |     953 |      45.02 |       34.00 |       33.79 |  +11.02 | MODEL_EXCEEDS_ORACLE |
| 障害 999      |   1,064 |      42.01 |       28.38 |       28.29 |  +13.63 | MODEL_EXCEEDS_ORACLE |

_Note: "other" (OP/重賞) class had 0 matched races in the v7 parquet holdout (kyoso_joken_code not present in NAR parquet)._

JRA per-class place2 / place3 detail (top rows only):

| Class | place2 Mdl | place2 Mkt | Mdl−Mkt | place3 Mdl | place3 Mkt | Mdl−Mkt |
| ----- | ---------: | ---------: | ------: | ---------: | ---------: | ------: |
| 703   |      25.80 |      18.70 |   +7.09 |      19.06 |      14.92 |   +4.14 |
| 005   |      20.46 |      18.21 |   +2.26 |      15.44 |      13.31 |   +2.13 |
| 010   |      20.78 |      18.57 |   +2.21 |      14.66 |      13.52 |   +1.14 |
| 016   |      20.91 |      15.82 |   +5.09 |      12.79 |      10.87 |   +1.93 |
| 701   |      23.92 |      18.36 |   +5.56 |      20.46 |      13.96 |   +6.51 |
| 999   |      23.03 |      15.32 |   +7.71 |      16.35 |      12.50 |   +3.85 |

### NAR — Global (iter12 production, n=45,572 races)

| Metric     | Model% | Market% | Oracle% | Mdl−Mkt | Orc−Mdl | Status               |
| ---------- | -----: | ------: | ------: | ------: | ------: | -------------------- |
| top1       |  58.68 |   43.04 |   43.25 |  +15.64 |  −15.43 | MODEL_EXCEEDS_ORACLE |
| place2     |  35.26 |   23.05 |   22.85 |  +12.21 |  −12.41 | MODEL_EXCEEDS_ORACLE |
| place3     |  27.32 |   17.30 |   17.29 |  +10.02 |  −10.02 | MODEL_EXCEEDS_ORACLE |
| top3_box   |  34.85 |   15.18 |   15.33 |  +19.66 |  −19.52 | MODEL_EXCEEDS_ORACLE |
| place2_set |  56.27 |   43.26 |   43.69 |  +13.01 |  −12.59 | MODEL_EXCEEDS_ORACLE |
| place3_set |  55.26 |   44.57 |   45.03 |  +10.69 |  −10.23 | MODEL_EXCEEDS_ORACLE |
| rentai_hit |  42.93 |   23.73 |   24.92 |  +19.20 |  −18.01 | MODEL_EXCEEDS_ORACLE |
| fukusho_2p |  87.92 |   68.96 |   69.54 |  +18.97 |  −18.39 | MODEL_EXCEEDS_ORACLE |
| fukusho_3p |  34.73 |   15.15 |   15.27 |  +19.59 |  −19.46 | MODEL_EXCEEDS_ORACLE |

### NAR — By Grade (iter12 production, selected grades)

| Grade         | n races | top1 Mdl | top1 Mkt | Mdl−Mkt | place2 Mdl | place2 Mkt | Mdl−Mkt |
| ------------- | ------: | -------: | -------: | ------: | ---------: | ---------: | ------: |
| E (地方重賞E) |  10,285 |    55.53 |    41.70 |  +13.83 |      32.53 |      22.25 |  +10.29 |
| S (地方重賞S) |     482 |    62.66 |    46.47 |  +16.18 |      37.97 |      22.41 |  +15.56 |
| T (地方重賞T) |     408 |    61.27 |    44.36 |  +16.91 |      37.75 |      27.21 |  +10.54 |
| A (地方重賞A) |      39 |    56.41 |    43.59 |  +12.82 |      25.64 |      17.95 |   +7.69 |
| B (地方重賞B) |      40 |    55.00 |    52.50 |   +2.50 |      32.50 |      15.00 |  +17.50 |
| C (地方重賞C) |      69 |    52.17 |    37.68 |  +14.49 |      33.33 |      27.54 |   +5.80 |

_NAR non-graded (blank grade_code) races not included in this table as no class-level breakdown was available via grade_code; covered in global row above._

### Ban-ei — Global (banei-cb-v7-grade, n=5,928 races)

| Metric     | Model% | Market% | Oracle% |   Mdl−Mkt | Orc−Mdl | Status               |
| ---------- | -----: | ------: | ------: | --------: | ------: | -------------------- |
| top1       |  34.62 |   34.46 |   34.62 |     +0.15 |    0.00 | SATURATED            |
| place2     |  20.65 |   20.72 |   20.65 |     −0.07 |    0.00 | SATURATED            |
| place3     |  15.54 |   15.55 |   15.69 |     −0.02 |   +0.15 | SATURATED            |
| top3_box   |  11.37 |   11.02 |   11.18 |     +0.35 |   −0.19 | MODEL_EXCEEDS_ORACLE |
| place2_set |  40.62 |   40.64 |   40.76 |     −0.02 |   +0.13 | SATURATED            |
| place3_set |  44.01 |   43.89 |   44.16 |     +0.12 |   +0.15 | SATURATED            |
| rentai_hit |  15.13 |   17.81 |   18.18 | **−2.68** |   +3.05 | **ANTI-INFORMATIVE** |
| fukusho_2p |  62.65 |   61.66 |   62.25 |     +1.00 |   −0.40 | MODEL_EXCEEDS_ORACLE |
| fukusho_3p |  11.34 |   11.00 |   11.17 |     +0.34 |   −0.17 | MODEL_EXCEEDS_ORACLE |

---

## Answers to Three Key Questions

### (a) Does the model beat the market, and by how much, per class?

**JRA (iter14 production)**: beats market on ALL exact-ordinal metrics across all classes.

- top1 delta vs market: +11.41pp global; strongest in 703 (未勝利) +14.31pp and 999 (障害) +13.63pp
- place2 delta: +5.27pp global; 703 and 999 show +7pp
- place3 delta: +3.21pp global; weakest in 010 (+1.14pp)
- All classes: positive. No class is ANTI-INFORMATIVE.

**NAR (iter12 production)**: beats market by large margins.

- top1: +15.64pp global (ranging +2.5pp grade_B to +16.9pp grade_T)
- place2: +12.21pp global
- top3_box: +19.66pp global (largest absolute gap)
- Exception: grade_B place3 is ANTI-INFORMATIVE (model 20.0% < market 25.0%) — but n=40 races only.

**Ban-ei (banei-cb-v7-grade)**: essentially ties the market.

- top1 delta: +0.15pp (not significant)
- place2 delta: −0.07pp (slightly below market)
- rentai_hit: **−2.68pp vs market** (ANTI-INFORMATIVE) — the model's pair-prediction of
  the exact top-2 pair is systematically worse than the market
- Summary: Ban-ei model provides **zero meaningful lift over market ranking**

### (b) Which metric (exact-ordinal vs set-membership) has the largest model-reachable headroom?

**Framing**: since the model already exceeds the Harville oracle for JRA and NAR, the
Harville oracle does NOT define the practical ceiling — the model has already surpassed it.
The question becomes: what metric shows the most upward leverage from new signals?

**Exact-ordinal gaps are narrow and hard to improve**:

- JRA top1 oracle is 33.46% and model is 44.76% — model already far exceeds the odds-only ceiling.
- The oracle captures only ~33% top1 because the market is noisy; the model achieves 44.76%.
- The oracle cannot be exceeded by definition of "what the market encodes" — the model beats it
  by using training history beyond odds.

**Set-membership metrics show structurally larger absolute values and wider gaps**:

- fukusho_2p (JRA): model 68.63%, market 56.43% — delta +12.2pp
- rentai_hit (JRA): model 24.34%, market 16.39% — delta +7.96pp
- place3_set (JRA): model 43.54%, market 40.37% — delta +3.18pp (smallest gap)

**Recommendation**: `fukusho_2p` (top-3 predicted contains ≥2 actual top-3 horses) is the
metric with the most headroom for new signal investigation, because:

1. It reaches 88% for NAR already (meaning current model is strong for set prediction)
2. JRA is at 68.63% — still 12pp above market, and the theoretical max is ~100%
3. Exact-ordinal metrics (top1/place2/place3) have structural noise floors from the
   inherent unpredictability of exact placement positions

**For the iterative optimization loop**: `top1` remains the primary metric as defined in the
accept gate, but `fukusho_2p` and `rentai_hit` should be monitored as leading indicators
because they better reflect distributional ranking quality.

### (c) Is exact-ordinal place2/place3 near its oracle ceiling (= ill-posed/noise)?

**Yes, with important qualification.**

The exact-ordinal `place2` and `place3` metrics have:

- JRA market: 18.04% / 13.75%
- JRA oracle: 18.10% / 13.88% (Harville from odds)
- JRA model: 23.31% / 16.96%

The Harville oracle for place2/place3 is very close to the market (18.04 vs 18.10, 13.75 vs 13.88).
This means the exact-ordinal 2nd/3rd placement is near-random even given perfect knowledge of
win probabilities. The model exceeds both by ~5pp and ~3pp respectively.

**Interpretation**: exact-ordinal place2/place3 IS ill-posed in the sense that the signal
ceiling for these metrics (from odds alone) is only slightly above the market baseline.
However, the model has already found ~3-5pp additional signal. Any further improvement
requires finding information orthogonal to the odds signal (e.g., specific pace/position
dependencies that influence exact 2nd/3rd placement beyond general "who is good").

**For Ban-ei**: place2 and place3 exact-ordinal are effectively random given the current model —
model ≈ market ≈ oracle within noise. This is the clearest saturation case in the dataset.

---

## Recommended Primary Metric

**For optimization loop**: **top1** (exact-ordinal) remains the primary metric because:

1. It has the largest absolute improvement over market (+11pp for JRA, +16pp for NAR)
2. It is well-calibrated and easy to interpret
3. The accept gate already uses it correctly

**For diagnosing per-class regression**: **fukusho_2p** as a leading indicator — it
aggregates 3 placement positions and is less noisy than individual place2/place3

**For Ban-ei investigation**: **top1** only (place2/place3 are noise-dominated at current
model capability level); the primary lever is adding information orthogonal to market odds

---

## Key Findings Summary

| Finding                             | Detail                                                                               |
| ----------------------------------- | ------------------------------------------------------------------------------------ |
| JRA/NAR model vs market             | Large positive lift: JRA +11pp top1, NAR +16pp top1                                  |
| JRA/NAR model vs Harville oracle    | Model significantly EXCEEDS oracle — extra-odds signal is real                       |
| Ban-ei model vs market              | Essentially zero lift (±0.15pp top1, ANTI-INFORMATIVE on rentai)                     |
| Exact-ordinal place2/place3 ceiling | Low (market ~18%/14% for JRA); model has found ~5/3pp above — near-informative limit |
| Best headroom metric                | fukusho_2p (top-3 contains ≥2 finishers) — wide gap, interpretable                   |
| Saturation status (JRA/NAR)         | No saturation: model already beats oracle, but oracle is NOT the absolute ceiling    |
| Saturation status (Ban-ei)          | True saturation — model equals market on all metrics; new features needed            |

---

## Data Provenance

- JRA odds source: `jvd_se` (kaisai_nen 2023-2026, keibajo_code 01-10, tansho_odds valid)
- NAR odds source: `nvd_se` (kaisai_nen 2023-2026, excluding keibajo_code=83)
- Ban-ei odds source: `nvd_se` (keibajo_code='83')
- Race matching: inner join on (race_year:kaisai_tsukihi:keibajo_code:race_bango, umaban)
- JRA model predictions: enriched-predictions/v7-lineage-wf-21y (baseline) + bucket-eval/iter14 (production)
- NAR model predictions: enriched-predictions/v7-lineage-wf-21y (baseline) + bucket-eval/iter12 (production)
- Ban-ei model predictions: re-scored from `tmp/banei/models/banei-cb-sectional-ri.cbm` on
  `tmp/banei/sectional_features` holdout (race_date 2023-2026)
- Full JSON: `tmp/rootcause/i1_headroom.json`
- Script: `tmp/rootcause/i1_compute_headroom.py` + `tmp/rootcause/i1_final_merge.py`
