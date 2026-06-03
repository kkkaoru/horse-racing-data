---
iteration: 7
date: 2026-06-04T07:30:00+09:00
based_on_iteration: 0
lever: L11-NAR-XGB-rank-ndcg
status: reject
quality_gate: passed
model_version_jra: not-applicable
model_version_nar: nar-xgb-v8-iter7-ndcg-wf-21y
metrics:
  wf_21y:
    nar:
      standalone:
        baseline:
          { races: 244811, top1: 0.58193, place2: 0.36107, place3: 0.28564, top3_box: 0.37146 }
        iter7: { races: 244811, top1: 0.58364, place2: 0.36146, place3: 0.28330, top3_box: 0.36682 }
        delta_pp: { top1: 0.170, place2: 0.040, place3: -0.234, top3_box: -0.464 }
      ensemble_best_w_iter7: 0.5
      ensemble_metrics_w_0_5:
        { races: 258966, top1: 0.5811, place2: 0.3606, place3: 0.2852, top3_box: 0.3700 }
training_time:
  nar: ~3.25min (20 folds total, ~9.5s per fold)
artifacts:
  predictions_parquet_nar: tmp/bucket-eval/finish-position/iter7-nar-xgb-ndcg-v8/predictions/category=nar/race_year=*/
  ensemble_predictions: tmp/bucket-eval/finish-position/iter7-nar-xgb-ndcg-ens-v8/predictions/
  ensemble_weights: tmp/v8/iter7-ensemble-weights.json
---

## What was tried

L11 (NAR XGBoost rank:ndcg objective retrain) — NDCG@3 直接最適化、 lambdarank_pair_method=topk、 lambdarank_num_pair_per_sample=3。 期待: place2/place3 lift via top-3 ranking emphasis。 NAR XGB WF 21y (2007-2026、 20 folds、 v7-lineage features 120-183 cols)。

## Implementation summary

- Helper scripts (Stage 0C 拡張 train_finish_position_xgboost_walk_forward.py + 新 tmp/v8/iter7_train_predict.py)
- 全 20 folds training 完了 (~9.5s/fold avg、 best_iteration 218-353)
- Standalone evaluation + ensemble with v7-lineage (w_iter7 ∈ {0.5, 0.7, 1.0})
- 2,572,237 ensemble rows for w_iter7=0.5 winner

## Results

### NAR L11 NDCG@3 standalone (244811 races)

| Metric   | baseline | iter 7  | Δpp          |
| -------- | -------- | ------- | ------------ |
| top1     | 58.193%  | 58.364% | **+0.170** ✓ |
| place2   | 36.107%  | 36.146% | **+0.040** ✓ |
| place3   | 28.564%  | 28.330% | **-0.234** ✗ |
| top3_box | 37.146%  | 36.682% | **-0.464** ✗ |

Gate (b) ≥2 軸 positive > +0.03pp: top1 + place2 = 2 軸 PASS。 Gate (c) positive set includes place2: PASS。
**Gate (a) ≥-0.05pp on all: place3 -0.234 FAIL、 top3_box -0.464 FAIL → REJECT**。

### NAR L11 ensemble with v7-lineage (rank-avg, w_iter7 sweep)

| w_iter7 | top1   | place2 | place3 | top3_box | composite |
| ------- | ------ | ------ | ------ | -------- | --------- |
| 0.5     | 58.11% | 36.06% | 28.52% | 37.00%   | 0.3992    |
| 0.7     | 58.23% | 36.11% | 28.38% | 36.73%   | 0.3986    |
| 1.0     | 58.23% | 36.11% | 28.37% | 36.71%   | 0.3985    |

Best w_iter7=0.5 vs baseline (258966 races different join window):

- top1 -0.083pp、 place2 -0.047pp、 place3 -0.044pp、 top3_box -0.146pp
- All 4 metrics regress、 ensemble does not rescue。 Gate (a) violation persists。

## Decision

**Reject (NAR)**: Gate (a) violation on place3 (-0.234pp standalone) と top3_box (-0.464pp standalone)、 ensemble rescue 不可。

**Insight**: NDCG@3 objective truncation level 3 が top-3 ordering を直接最適化する一方、 truncation 外の rank 4-N の ordering を犠牲。 top1 + place2 (上位 ranking) は実 lift するが、 place3 + top3_box (deeper / box patterns) は regression。 **NDCG@k トレードオフは k=3 では NAR で逆効果**。

JRA は本 iter scope 外 (NAR-only lever)、 baseline 維持。

State: `last_iter_id=7, accept_count=0, reject_count=7, consecutive_reject_count=7`。 Stop S1 (8 consecutive reject) まで 1 iter 余裕。

## Next iteration recommendation

**Iter 8 候補**:

1. **L11 NDCG@5 variant** — `lambdarank_num_pair_per_sample=5` で top-5 ranking 最適化、 place3 と top3_box を直接 truncation 内に含める。 estimated ~5-6h NAR。 期待: place3/top3_box rescue + top1/place2 lift 維持。
2. **L5A pace × style horse-level signal addition** — ~17h、 budget 厳しい。
3. **Stop loop** — 7 連続 reject で saturation 確定、 user 復帰時に決定。

優先: **L11 NDCG@5** (truncation 拡張で deeper rank 救済期待、 同 lever variation で fast test)。 iter 8 reject なら stop S1 自動 trigger。

## Quality Gate Results

quick window 段階で 0 source-code edits commit、 quality gate 自動 passed (Stage 0C 拡張済 train script を再 invoke のみ)。

- tsc: 0 errors
- lint: 0 warnings
- format:check: exit 0
- test:coverage: unchanged from 3a5456e (99.08/96.50/98.89/99.10)
- python:check: 97.15% cov (1514 tests, unchanged)
