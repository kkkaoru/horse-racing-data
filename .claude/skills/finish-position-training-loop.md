# finish-position-training-loop — 着順予測の反復学習ループ

> **自動学習ループ**: Production の自動化学習ループスクリプトは `.claude/skills/continuous-learner.md` を参照。
> このファイルは手動 iterative loop と DO-NOT-RETEST レジストリを管理する。

着順予測モデルの反復学習ループ (train → predict → evaluate → accept/reject) を実行するスキル。
JRA / NAR / Ban-ei の 3 カテゴリを対象とする。

---

## §1. 本番モデル状態 (2026-06-23 時点)

**model_meta.json**: `apps/finish-position-predict-container/src/predict_lib/model_meta.json`

| カテゴリ | model_version                | features | アーキテクチャ                  | 備考                              |
| -------- | ---------------------------- | -------- | ------------------------------- | --------------------------------- |
| JRA      | `iter20-jra-cb-2013-v8`      | 244      | CatBoost YetiRank (2013+)       | E-top2 overlay (iter22-jra-etop2) |
| NAR      | `iter12-nar-xgb-hpo-v8`      | 192      | XGBoost (HPO, Optuna 50 trials) | per-class routing 有              |
| Ban-ei   | `banei-cb-v7-lineage-wf-21y` | 111      | CatBoost YetiRank               | 市場レベルで飽和                  |

**E-top2 (JRA)**: XGB rank-1 == CB rank-2 の race で rank-1 を override。blind 2025: top1 +1.36pp。
**Per-class routing (NAR)**: `per_class.py` に登録。A/B/C/NEW/MUKATSU/OP/other。

**state.json**: `tmp/v8/state.json` — last_iter_id=17, accept_count=3, reject_count=17

---

## §2. ディレクトリ構造

```
tmp/v8/                               ← 学習スクリプト・中間成果物
  iter{N}_train_predict*.py           ← 各イテレーションの学習スクリプト
  compute_iter{N}_metrics_and_decision.py ← メトリクス計算・判定
  compute_metrics_duckdb.py           ← 汎用 DuckDB メトリクス計算
  identify_weak_buckets_fallback.py   ← 弱バケット特定 (Stage 0D)
  state.json                          ← ループ状態管理
  best-iteration.json                 ← 最良イテレーション追跡
  enriched-predictions/               ← 予測 + 実績結合済み parquet
  calibrated-predictions/             ← キャリブレーション済み parquet
  run_per_class_loop.sh               ← JRA per-class 学習ループ
  run_iter21_chain_loop.sh            ← chain 学習ループ
  run_iter22_residual_loop.sh         ← residual 学習ループ
tmp/nar-perclass/                     ← NAR per-class 専用
  run_iter30_all_classes.sh           ← NAR 全クラス学習ループ
  iter30_train_predict_residual_nar.py
apps/finish-position-predict-container/
  models/finish-position/{jra,nar,ban-ei}/ ← モデルバイナリ
  src/predict_lib/
    model_meta.json                   ← 本番モデル定義
    per_class.py                      ← per-class routing
    ensemble_routing.py               ← アンサンブルルーティング
    subgroup.py                       ← サブグループ分類関数
apps/pc-keiba-viewer/src/scripts/
  aggregate_bucket_eval_duckdb.py     ← DuckDB バケット評価 (本体)
  continuous_learner.py               ← 連続学習ループ自動化
  finish-position-features/
    evaluate-bucket-21y-v8.ts         ← WF バケット評価 (TS版)
    identify-weak-buckets.ts          ← 弱バケット特定 (TS版)
docs/finish-position-accuracy/
  per-class/ROADMAP.md                ← マスターロードマップ
  per-class/{jra,nar,ban-ei}/         ← カテゴリ別クラスドキュメント
  history/                            ← イテレーション履歴
```

---

## §3. 学習パイプラインの実行手順

### Stage 0: サブグループ弱点分析

```bash
# DuckDB で弱バケット特定 (PG 不要、parquet 直読み)
cd /Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data
uv run python tmp/v8/identify_weak_buckets_fallback.py
# → tmp/v8/weak-buckets-iter0.json

# または TS 版 (PG 必要)
cd apps/pc-keiba-viewer
bun run src/scripts/finish-position-features/identify-weak-buckets.ts \
  --model-version-jra iter20-jra-cb-2013-v8 \
  --model-version-nar iter12-nar-xgb-hpo-v8 \
  --output ../../tmp/v8/weak-buckets.json
```

サブグループ次元 (6 軸):

- `distance_band`: sprint (≤1400m) / mile (≤1800m) / intermediate (≤2200m) / long (≤2800m) / extended
- `field_size_band`: small (≤8) / medium (≤14) / large
- `season_band`: spring (3-5月) / summer (6-8月) / autumn (9-11月) / winter (12-2月)
- `surface`: turf (track_code 10-22) / dirt (23-29) / obstacle (51-59)
- `class_code`: 競走条件コード
- `venue`: 競馬場コード

### Stage 1: 特徴量エンジニアリング (lever に依存)

新特徴量を追加する場合、`tmp/v8/iter{N}_build_*.py` で構築。
既存特徴量セットで学習する場合はスキップ。

### Stage 2: Walk-Forward 学習 + 予測

```bash
# JRA (CatBoost YetiRank)
uv run python tmp/v8/iter{N}_train_predict.py \
  --category jra \
  --model-version iter{N}-jra-cb-{desc}-v8 \
  --output-root tmp/bucket-eval/finish-position/iter{N}-jra-cb-{desc}-v8 \
  --model-out-root apps/finish-position-predict-container/models/finish-position/jra/iter{N}-jra-cb-{desc}-v8 \
  --pg-url "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing" \
  --summary-out tmp/v8/iter{N}-train-summary.json

# NAR (XGBoost)
uv run python tmp/v8/iter{N}_train_predict.py \
  --category nar \
  --model-version iter{N}-nar-xgb-{desc}-v8 \
  --output-root tmp/bucket-eval/finish-position/iter{N}-nar-xgb-{desc}-v8 \
  --model-out-root apps/finish-position-predict-container/models/finish-position/nar/iter{N}-nar-xgb-{desc}-v8 \
  --pg-url "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing" \
  --summary-out tmp/v8/iter{N}-nar-train-summary.json

# Ban-ei (CatBoost YetiRank)
uv run python tmp/v8/iter{N}_train_predict.py \
  --category ban-ei \
  --model-version iter{N}-banei-cb-{desc}-v8 \
  --output-root tmp/bucket-eval/finish-position/iter{N}-banei-cb-{desc}-v8 \
  --model-out-root apps/finish-position-predict-container/models/finish-position/ban-ei/iter{N}-banei-cb-{desc}-v8 \
  --pg-url "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing" \
  --summary-out tmp/v8/iter{N}-banei-train-summary.json
```

### Stage 3: メトリクス計算 + 判定

```bash
uv run python tmp/v8/compute_iter{N}_metrics_and_decision.py
# → tmp/v8/iter{N}-decision.json
# → tmp/v8/iter{N}-metrics-global.json
# → tmp/v8/iter{N}-data.json
```

汎用 DuckDB メトリクス計算:

```python
from tmp.v8.compute_metrics_duckdb import fetch_global_metrics, fetch_per_bucket_metrics, evaluate_accept
```

### Stage 4: サブグループ評価

```bash
cd apps/pc-keiba-viewer
uv run python src/scripts/aggregate_bucket_eval_duckdb.py \
  --predictions-glob 'tmp/bucket-eval/finish-position/{model_version}/predictions/**/*.parquet' \
  --local-pg-url "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing" \
  --neon-url "$NEON_DIRECT_DATABASE_URL" \
  --running-style-feature-version v3 \
  --finish-position-version v1
```

### Stage 5: 連続学習ループ (自動化)

```bash
cd apps/pc-keiba-viewer
uv run python src/scripts/continuous_learner.py \
  --features-parquet <path> \
  --category <jra|nar|ban-ei> \
  --repo-root /Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data \
  --n-trials 20 \
  --deploy-threshold 0.005
```

---

## §4. Accept/Reject Gate (判定ロジック)

4 指標: `top1`, `place2`, `place3`, `top3_box` (全て exact-ordinal)

**Gate A**: 全指標で regression ≤ 0.05pp
**Gate B**: ≥2 指標で > +0.03pp の改善
**Gate C**: 改善指標に place2 または place3 を含む
**Gate D**: per-bucket worst regression ≤ +2.0pp
**Gate E**: top1 paired-bootstrap LB95 > 0
**Gate F**: Holm correction across classes

**緩和ルール (2026-06-17)**:

- incremental gain でも採用 = primary {top1, place2, place3} のどれか改善 + regression なしで ADOPT
- 一部 up 一部 down は goal 優先で判断 or ロジック見直し
- per-class 評価 + class-conditional adoption 可能

**参照**: `tmp/v8/compute_metrics_duckdb.py:evaluate_accept()`

---

## §5. メモリ予算ルール (HARD)

**48GB Mac, Colima 24GB 常時確保** → 実質 24GB

1. **heavy 学習は同時 1 本のみ** (kernel panic 2026-06-12 教訓)
2. DuckDB は `memory_limit 6GB / threads 4` 必須
3. heavy 学習前に `memory_pressure` 確認 (free < 30% で待機)
4. compute agent ≤ 2

```bash
# メモリチェック
memory_pressure
# → "The system has 60% free" 等。30% 以上なら OK
```

---

## §6. DO-NOT-RETEST レジストリ (再試行禁止)

詳細: `docs/finish-position-accuracy/per-class/ROADMAP.md §3`

### JRA 枯渇済み:

- per-class kNN/pgvector (iter32): 全 5 クラス REJECT
- Relationship features: 全 probe ABORT
- HPO (iter13-14): CV → WF 移転失敗
- 016 class-transition features: ρ ≤ 0.026 ABORT
- 999 jump features: ρ < 0.02 ABORT
- Window ablation: 2013+ が最適、さらに狭めると regression
- Blinker A-H ablation: feature importance=0, 再現不能
- Draw/dist-speed/momentum: CatBoost 既捕捉
- Score-additive correction: top1 負け
- Season×sex×weight 全 5 手法: REJECT
- Venue-specific correction: 構造的不可行
- Ensemble 5-for-5 ABORT (E-top2 のみ成功)
- Per-class routing (iter20): REJECT
- HPO selection bias (iter21): holdout で却下
- **Field-difficulty 特徴量 (odds_entropy/eff_contenders/iop_gap_to_fav/fav_margin)**: 3-blind-year で REJECT。2023/2025 は +0.49pp だが 2024 は −0.96pp (regime-dependent)。GBDT が既存 field*\*/inverse_odds*\* で既捕捉。probe partial-ρ は strong (-0.23) だが GBDT 内部で非線形に既捕捉。**odds-derived field aggregation は DO-NOT-RETEST** (2026-06-23)

### NAR 枯渇済み:

- B-class dedicated model (iter30): ABORT
- 3YO/2YO age features: ρ < 0.02 ABORT
- kNN similarity (iter32): 全 7 クラス REJECT
- Field-relative/recency features (H1-H3): REJECT
- Per-class HPO (H4): holdout gate 失敗
- C place-preserving objective (W3/W4): 8+6 variants 全 REJECT
- G1+F1 retrain: 修正すると −0.63pp (NULL routing が最適)
- Signal4 sire×keibajo: serve coverage 22% でブロック中 (#254)
- **Window ablation (2013+/2015+/2017+)**: 全 REJECT。NAR は full 2006+ history 必要 (JRA と真逆) (2026-06-23)
- **Venue-specialist routing**: global に全 metric 敗北 (fragmentation) (2026-06-23)

### Ban-ei 枯渇済み:

- Window ablation: **2011+ が最適** (2026-06-23 ADOPT, +0.475pp top1, 5-seed robust)。2007+ は pre-2011 非定常で希釈。2013+/2016+ は切りすぎ
- Sectional/race-internal features: no gain
- Exotic odds (fukusho): top1 trade → REJECT
- Odds decoupling: −7.95pp REJECT
- Relationship features (futan): n=25 で infeasible
- E-top2 XGB override: override 逆 signal ABORT

---

## §7. 残存候補 (試行可能)

### HIGH PRIORITY:

1. **JRA 703 pgvector race-condition 類似度** — 新設計 (race-condition vector, 過去の horse-history vector とは別)。probe gate: partial-ρ ≥ 0.08。
2. ~~**NAR window ablation**~~ — 2026-06-23 全 REJECT。NAR は full 2006+ 必須。

### MEDIUM PRIORITY:

3. **JRA/NAR 新交互作用特徴量** — 弱バケットから特定された次元の交互作用
4. **RL policy-gradient ranker** — GBDT が通常優位だが NAR C or JRA 703 で probe 可能

### LOW PRIORITY (飽和リスク高):

5. **Ban-ei E-top2 XGB override** — 2026-06-19 ABORT 済み。再試行は新アプローチ必要

### BLOCKED:

- **Signal4 (NAR sire×keibajo)**: nvd_um #254 解決待ち (serve coverage ≥80% 必要)

---

## §8. 学習ループの Agent 実行パターン

各カテゴリを background agent で実行。**メモリ制約により同時に 1 つの heavy training のみ**。

```
Agent 実行順序:
1. JRA agent (background) — subgroup 分析 → lever 選定 → train → evaluate
2. NAR agent (background) — JRA 完了後に heavy training 開始
3. Ban-ei agent (background) — NAR 完了後に heavy training 開始
メイン: 3 agent の進捗・メモリ・エラーを定期監視
```

各 agent は以下のサイクルを繰り返す:

1. `memory_pressure` チェック (free ≥ 30%)
2. 弱バケット分析で lever 候補を特定
3. DO-NOT-RETEST と照合して候補を絞る
4. 学習スクリプトを作成・実行
5. メトリクス計算 + accept/reject 判定
6. state.json 更新
7. ADOPT なら model_meta.json + per_class.py 更新 (commit OK, push は user 指示待ち)

---

## §9. メトリクス定義 (canonical)

```
top1     := per-race max(predicted_rank=1  AND finish_position=1)
place2   := per-race max(predicted_rank=2  AND finish_position=2)
place3   := per-race max(predicted_rank=3  AND finish_position=3)
top3_box := per-race cast(all of {rank≤3} have {finish≤3} = 3)
```

全て **exact-ordinal** (place2=50% = 2 位予測馬が実際に 2 位で完走した割合)。

**exact place2/place3 の天井**: JRA place2 ~18-23% / NAR place2 ~37% (情報理論的限界)。
**累積指標** (fukusho_2p 75-86% / top3_box 80-87%) は既に高水準。

---

## §10. Per-Class Routing 設定

**JRA**: E-top2 overlay のみ (per-class ensemble なし = global model)
**NAR** (`per_class.py`):

```python
PER_CLASS_MODEL_VERSIONS = {
    ("nar", "NEW"): "iter30-nar-cb-ensemble-NEW-v8",
    ("nar", "MUKATSU"): "iter30-nar-cb-ensemble-MUKATSU-v8",
    ("nar", "C"): "iter36-nar-lgb-ensemble-C-v8",
    ("nar", "A"): "iter30-nar-cb-ensemble-A-v8",
    ("nar", "OP"): "iter30-nar-cb-ensemble-OP-v8",
    ("nar", "other"): "iter30-nar-cb-ensemble-other-v8",
}
# B → iter12 fallback (ensemble なし)
```

**Ban-ei**: per-class routing 無効

---

## §11. DB 接続情報

```bash
# Local PostgreSQL (Colima Docker)
PG_URL="postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"

# Neon (本番)
NEON_URL="$NEON_DIRECT_DATABASE_URL"  # 環境変数から

# テーブル
# - finish_position_active_models (category, model_version, activated_at, subclass)
# - model_prediction_bucket_evaluations (per-bucket metrics)
# - model_prediction_subgroup_evaluations (per-dimension rollups)
# - model_prediction_evaluations (global rollup)
# - race_finish_position_model_predictions (per-race predictions)
```

---

## §12. Inverse-Signal 記録フォーマット

学習ループで精度が大幅に低下した lever は、その逆手法 (inverse) を自動的に試行し、結果を構造化して保存する。

### 記録先ファイル

- **per-iteration**: `tmp/v8/iterative-loop-results/{category}/iter-{N}.json`
- **累積 state**: `tmp/v8/iterative-loop-state.json`
- **特徴量試行記録**: `tmp/v8/feature-trial-log.json` (特徴量ごとの正/逆の両方の試行結果)

### per-iteration JSON の構造

```json
{
  "iteration": 1,
  "category": "nar",
  "lever": "window-ablation-2017+",
  "delta_pp": { "top1": -0.52, "place2": -0.08, "place3": -0.307, "top3_box": -0.06 },
  "decision": "REJECT",
  "inverse_trials": [
    {
      "original_lever": "window-ablation-2017+",
      "original_delta_pp": { "top1": -0.52, "place3": -0.307 },
      "inverse_approach": "weight_inversion",
      "inverse_description": "Upweight pre-2017 samples 1.3x",
      "inverse_delta_pp": { "top1": 0.12, "place3": 0.18 },
      "inverse_decision": "ADOPT",
      "inverse_mechanism": "Old data contains place-predictive patterns"
    }
  ],
  "strongly_negative_log": [
    {
      "lever": "window-ablation-2017+",
      "metric": "place3",
      "delta_pp": -0.307,
      "inverse_attempted": true,
      "inverse_result": "ADOPT"
    }
  ]
}
```

### 特徴量試行記録 (feature-trial-log.json)

```json
{
  "features": {
    "cand_odds_entropy": {
      "original_trial": {
        "category": "jra",
        "date": "2026-06-23",
        "delta_pp": { "top1": 0.376 },
        "decision": "PENDING_MULTIYEAR"
      },
      "inverse_trial": null,
      "strongly_negative": false
    },
    "venue_specialist_44": {
      "original_trial": {
        "category": "nar",
        "date": "2026-06-23",
        "delta_pp": { "top1": -0.436, "place2": -1.007 },
        "decision": "REJECT"
      },
      "inverse_trial": {
        "approach": "anti_correlation",
        "description": "specialist disagreement as confidence signal",
        "delta_pp": { "top1": 0.05 },
        "decision": "REJECT"
      },
      "strongly_negative": true,
      "negative_metrics": ["place2"]
    }
  },
  "negative_signal_bank": [
    {
      "feature_or_lever": "venue_specialist_44",
      "category": "nar",
      "worst_metric": "place2",
      "worst_delta_pp": -1.556,
      "inverse_approaches_tried": ["anti_correlation"],
      "inverse_approaches_remaining": ["feature_negate", "weight_invert"],
      "first_observed": "2026-06-23"
    }
  ]
}
```

### 閾値

- `STRONG_NEGATIVE_THRESHOLD_PP = -1.0` — この閾値未満で inverse 自動試行
- inverse approach 種別: `feature_negate` / `weight_invert` / `window_invert` / `anti_correlation`

### 自動化スクリプト

`tmp/v8/iterative_training_loop.py` — inverse-signal 記録付き反復学習ループ

---

## §13. 学習ループ実行時の注意事項

1. **commit OK / push 禁止**: 学習成果物の commit は autonomous で可。push は user の明示的指示まで待つ。
2. **テスト同時更新**: コード変更時は同ディレクトリのテストも同ターンで更新。
3. **SubAgent 経由でコード変更**: orchestrator は plan file 以外を Edit/Write しない。
4. **git stash/checkout 禁止**: 並列 agent で未コミット作業が消える。
5. **特徴量の列を減らさない**: schema 拡張のみ OK。
6. **カバレッジ 95% 維持**: vitest (TS) / pytest (Python) のしきい値を下げない。
7. **oxlint/oxfmt 0 warnings**: lint/format 違反を disable コメントで隠さない。
