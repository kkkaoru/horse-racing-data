---
iteration: 8
date: 2026-06-04T07:50:00+09:00
based_on_iteration: 0
lever: L11-NDCG@5-variant
status: reject
quality_gate: passed
loop_status: stopped_s1_8_consecutive_reject
model_version_jra: not-applicable
model_version_nar: nar-xgb-v8-iter8-ndcg5-wf-21y
metrics:
  wf_21y:
    nar:
      standalone:
        baseline:
          { races: 244811, top1: 0.58193, place2: 0.36107, place3: 0.28564, top3_box: 0.37146 }
        iter8: { races: 244811, top1: 0.58370, place2: 0.36153, place3: 0.28487, top3_box: 0.36866 }
        delta_pp: { top1: 0.176, place2: 0.047, place3: -0.077, top3_box: -0.280 }
      ensemble_w_0_5:
        delta_pp: { top1: 0.114, place2: -0.014, place3: -0.025, top3_box: -0.072 }
training_time:
  nar: ~3min (20 folds, ~9s/fold)
artifacts:
  predictions_parquet_nar: tmp/bucket-eval/finish-position/iter8-nar-xgb-ndcg5-v8/predictions/category=nar/race_year=*/
  ensemble_predictions: tmp/bucket-eval/finish-position/iter8-nar-xgb-ndcg5-ens-v8/predictions/
  ensemble_weights: tmp/v8/iter8-ensemble-weights.json
---

## What was tried

L11 NDCG@5 variant — iter 7 NDCG@3 で place3/top3_box が trunc-3 範囲外で犠牲となったため、 lambdarank_num_pair_per_sample=5 で top-5 ranking 直接最適化、 place3 を truncation 内に含めることで rescue 試行。 NAR XGB WF 21y 20 folds (~9s/fold)。

## Results

### NDCG@5 standalone (244811 races)

| Metric   | baseline | iter 8  | Δpp          |
| -------- | -------- | ------- | ------------ |
| top1     | 58.193%  | 58.370% | **+0.176** ✓ |
| place2   | 36.107%  | 36.153% | **+0.047** ✓ |
| place3   | 28.564%  | 28.487% | **-0.077** ✗ |
| top3_box | 37.146%  | 36.866% | **-0.280** ✗ |

Gate (a) violation on place3 (-0.077) と top3_box (-0.280)。 但 iter 7 NDCG@3 比 place3 -0.234 → -0.077 (大幅改善)、 top3_box -0.464 → -0.280 (改善)。 NDCG@5 truncation 拡張は方向性正しいが gate 通過まで届かず。

### NDCG@5 ensemble with v7-lineage (w_iter8 sweep)

| w_iter8 | top1   | place2 | place3 | top3_box | composite |
| ------- | ------ | ------ | ------ | -------- | --------- |
| 0.5     | 58.17% | 36.13% | 28.60% | 37.09%   | 0.4000    |
| 0.7     | 58.24% | 36.12% | 28.53% | 36.91%   | 0.3995    |
| 1.0     | 58.24% | 36.12% | 28.52% | 36.90%   | 0.3995    |

Best w_iter8=0.5 vs baseline (244811 races clean):

- top1 +0.114 / place2 -0.014 / place3 -0.025 / top3_box -0.072
- Gate (a): top3_box -0.072 FAIL
- Gate (b): only top1 positive (place2 negative) → 1 軸 < 2 FAIL
- Gate (c): place2 / place3 not in positives FAIL

Ensemble dilutes iter 8 standalone gains、 gate 通過しない。

## Decision

**Reject (NAR)**: standalone fails gate (a)、 ensemble fails gates (a)(b)(c)。

JRA は本 iter scope 外、 baseline 維持。

State: `last_iter_id=8, accept_count=0, reject_count=8, consecutive_reject_count=8`。 **Stop S1 (8 consecutive reject) triggered**。 Loop autonomous 終了。

## Loop termination summary

8 連続 reject (iter 1-8) で v8 iterative loop が autonomous stop 条件 S1 で終了。 production baseline は v7-lineage 維持 (`jra-cb-v7-lineage-wf-21y` / `nar-xgb-v7-lineage-wf-21y`)、 production flip は user 復帰後の明示指示待ち (本 plan の autonomous scope 外)。

### 試行 lever 結果

| Iter | Lever                                      | Family                          | Result | Insight                                                          |
| ---- | ------------------------------------------ | ------------------------------- | ------ | ---------------------------------------------------------------- |
| 1    | L2 isotonic calibration                    | post-hoc                        | reject | per-race-bucket = identity transform、 ranking 不変              |
| 2    | L8 Ridge stacking + running-style          | post-hoc linear                 | reject | linear blend rank-preserving with race-constant features         |
| 3    | L8 LGBM stacking + horse-level features    | post-hoc non-linear             | reject | existing horse-level history features saturated in base          |
| 4    | L1B CB+LGBM rank ensemble (v6 features)    | multi-arch ensemble             | reject | LGBM-on-v6 weaker than CB-on-v7                                  |
| 5    | L1B CB+LGBM rank ensemble (v7 features)    | multi-arch ensemble             | reject | GBDT family saturation confirmed、 composite identical to iter 4 |
| 6    | L4 bucket-aware sample-weight base retrain | base retrain                    | reject | JRA quick window saturation、 NAR data failure                   |
| 7    | L11 NAR XGB rank:ndcg objective            | base retrain different gradient | reject | top1/place2 lift but place3/top3_box big regression              |
| 8    | L11 NDCG@5 variant                         | trunc level extension           | reject | mitigates iter 7 regression but still gate (a) violation         |

### Key findings

- **v7-lineage CB-JRA + XGB-NAR は existing PG features 空間で local optimum**: post-hoc layers (calibration / stacking / ensemble)、 同 features 同 family の retrain (L4 base / L11 NDCG@k variants) すべて gain 失敗
- **NDCG@k objective family の限界**: truncation level 拡張 (3→5) で deeper-rank regression mitigate するが top-k と deeper-rank の trade-off は本質的、 gate (a) ≥-0.05pp 条件で同時 pass 不可
- **Saturation の本質**: v7-lineage CB が既に internalize 済の features 空間で、 GBDT family の決定境界は near-equivalent、 ensemble 効果 zero
- **唯一未試行で可能性ある lever**: L5A pace × style horse-level signal addition (~17h with feature engineering)、 fundamentally 新 information を v7-lineage に投入する only path

### Recommendations for user (13:00 JST 復帰時)

1. **Production flip 不実施** — 全 iter が baseline 改善せず、 v7-lineage 維持が現状最良
2. **本 loop の knowledge artifact** — iter 1-8 history MDs + decision JSONs で saturation 解析記録、 将来の改善試行時に参照
3. **次世代探索方向**:
   - L5A pace × style horse-level signal addition (feature engineering + retrain ~17h、 本格挑戦)
   - L19 FT-Transformer MLX research-only (deep learning 試行、 prior MLX -8pp 警告だが TabM 系は別アーキで未試行)
   - L5B odds-momentum (~6h、 2025+ history のみだが新 signal、 future race で蓄積後に効く可能性)
4. **Memory 更新済**: `project_v7_lineage_saturation_2026_06_04.md` で knowledge 保存、 将来 session で参照可能

## Quality Gate Results

iter 8 commit 直前に check + python:check verify。

- tsc: 0 errors
- lint: 0 warnings
- format:check: exit 0
- test:coverage: unchanged from ed145ad (99.08/96.50/98.89/99.10)
- python:check: 97.15% cov (1514 tests, unchanged)
