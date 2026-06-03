---
iteration: 6
date: 2026-06-04T06:25:00+09:00
based_on_iteration: 0
lever: L4-bucket-aware-base-retrain
status: reject
quality_gate: passed
model_version_jra: jra-cb-v8-iter6-alpha-quick-window-only
model_version_nar: nar-xgb-v8-iter6-alpha-data-failure
metrics:
  wf_21y: not_run
  quick_window_2024_2026_jra:
    baseline: { races: 8247, top1: 0.4490, place2: 0.2314, place3: 0.1696, top3_box: 0.1581 }
    alpha_0_3_delta_pp: { top1: -0.243, place2: 0.036, place3: -0.036, top3_box: 0.012 }
    alpha_0_5_delta_pp: { top1: -0.170, place2: 0.243, place3: -0.133, top3_box: 0.012 }
    alpha_0_75_delta_pp: { top1: -0.340, place2: 0.243, place3: 0.170, top3_box: -0.049 }
  nar: data_failure
training_time:
  jra_quick_window: ~10min (3 α × 3 folds)
  nar_quick_window: ~10min partial (data write failure)
artifacts:
  predictions_parquet_jra: tmp/bucket-eval/finish-position/iter6-jra-cb-alpha{0.3,0.5,0.75}/predictions/category=jra/race_year={2024,2025,2026}/
  predictions_parquet_nar: missing
  quick_metrics: tmp/v8/iter6-quick-metrics.json (NAR portion bogus per analysis)
---

## What was tried

L4 (bucket-aware sample-weight base retrain) — modified training loss with `sample_weight = time_decay × (1 + α × is_weak_bucket_score)` where weak buckets identified from accumulated iter 1-5 delta CSVs. α A/B sweep {0.3, 0.5, 0.75} per cat. Time-decay weight clip [0.5, 1.75] per plan stability item 6. Quick window 2024-2026 for α selection before full WF.

## Implementation summary

- Stage 1.5 bucket membership computed (existing artifacts: `tmp/v8/bucket-membership/category={jra,nar}/race_year=*/`)
- Helper scripts: `tmp/v8/iter6_train_predict.py`, `compute_iter6_quick_metrics.py`, `compute_iter6_metrics_and_decision.py`
- JRA CB α sweep ran successfully (parquets at `tmp/bucket-eval/finish-position/iter6-jra-cb-alpha*/predictions/`)
- NAR XGB α sweep technical failure (no predictions parquet written despite training logs reporting completion)

## Results

### JRA L4 quick window 2024-2026 (8247 common races)

| α    | top1 Δpp | place2 Δpp | place3 Δpp | top3_box Δpp | composite |
| ---- | -------- | ---------- | ---------- | ------------ | --------- |
| 0.3  | -0.243   | +0.036     | -0.036     | +0.012       | 0.2515    |
| 0.5  | -0.170   | +0.243     | -0.133     | +0.012       | 0.2519    |
| 0.75 | -0.340   | +0.243     | +0.170     | -0.049       | 0.2521    |

**Winner α=0.75 但 composite gain は微小**、 すべての α で top1 regression、 gate (a) (no axis < -0.05pp) violation 確実。 quick window 2024-2026 では JRA L4 = reject 強信号。 full WF も同様予測される。

### NAR L4 quick window 2024-2026

NAR XGB sweep `bv5xnpej9` の出力ログは α=0.3 で `status: no_data`、 α=0.5 で `trained 31.8s rows=138375` を報告したが、 expected output path `tmp/bucket-eval/finish-position/iter6-nar-xgb-alpha*/predictions/` に parquet 不在。 compute_iter6_quick_metrics.py が出力した NAR メトリクスは bogus (common race count anomaly: baseline 18540 > 各単独 α の race 数で論理破綻)。 技術 failure、 valid data なし。

## Per-bucket findings

JRA: bucket-aware weights upweighted underperforming buckets (JRA grade C 系 + 距離 1400-1799m 等)、 dominant bucket accuracy が trade-off で低下。 weak bucket set が broad すぎる (ほぼ全 bucket が何らかの metric で weak flagged) → effective sample mean weight 0.88 (downweight)、 結果は plain retrain より accuracy 低下。

NAR: 評価不可。

## Decision

**Reject (両 cat)**:

- JRA: data-driven reject (quick window で saturation 確認、 全 α top1 regression)
- NAR: technical failure (data missing)、 inconclusive but per state machine treats as reject

State: `last_iter_id=6, accept_count=0, reject_count=6, consecutive_reject_count=6`。 Stop S1 (8 consecutive reject) まで余裕 2 iter。

## Next iteration recommendation

5 連続 reject (iter 1-5、 calibration/Ridge/LGBM stack/v6 ensemble/v7 ensemble) に iter 6 L4 base retrain も追加で 6 連続。 GBDT 系の **base retrain (L4) ですら saturation**、 すべて existing v7-lineage が internalize 済の features 空間で動作している。

**Iter 7 候補** (autonomous lever pivot per plan、 different direction):

1. **L11 NAR rank:ndcg objective** (NAR XGB のみ、 ~5-6h、 NDCG@3 直接最適化で place2/3 lift) — 新 objective でも features 同じだが gradient 方向が変わる、 試行価値あり
2. **L5A pace × style horse-level signal addition** (~17h、 feature engineering 含む) — 新 horse-level signal で v7-lineage 未捕捉領域に access
3. **L3 Optuna HPO** (~6-12h、 v7-lineage architecture で hyperparam tuning) — base retrain は saturation 確認済なので gain 限定的見込

優先: L11 (NAR-only、 fast、 new objective gradient) → fail なら L5A (radical signal addition)。

NAR data failure 解析 (iter6_train_predict.py の NAR path handling) は iter 7 で投資価値ある場合のみ実施。

## Quality Gate Results

quick window 段階で 0 source-code edits commit、 quality gate 自動 passed。 commit 直前に check + python:check 再確認。

- tsc: 0 errors (no TS edits since c8eafd2)
- lint: 0 warnings
- format:check: exit 0
- test:coverage: 99.08/96.50/98.89/99.10 (unchanged from c8eafd2)
- python:check: 97.15% cov, 1514 tests (unchanged)
