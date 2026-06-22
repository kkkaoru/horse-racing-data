# continuous-learner — 自動学習ループ全スクリプトのリファレンス

着順予測の **自動化された自己改善パイプライン** を構成する 4 スクリプト
(`apps/pc-keiba-viewer/src/scripts/learning/`) の決定版リファレンス。
手動 iterative loop は `.claude/skills/finish-position-training-loop.md` を参照。

```
apps/pc-keiba-viewer/src/scripts/learning/
├── __init__.py
├── continuous_learner.py
├── feature_explorer.py
├── feature_registry.py
└── subgroup_diagnostics.py
```

---

## §1. Overview

4 スクリプトが連携して自己改善型の予測パイプラインを形成する。1 ラウンドの流れ:

1. **特徴量組み合わせを探索** — Optuna + Walk-Forward 評価 (`feature_explorer.py`)
2. **全試行を DuckDB registry に記録** — dedup 付き (`feature_registry.py`)
3. **大幅に負の結果を検出 → inverse 手法を自動試行** — `feature_trials` から
   `delta_pp ≤ -1.0pp` の trial を抽出し、未試行の inverse approach を実行
4. **閾値超えの改善を自動デプロイ** — NDCG@3 が `--deploy-threshold` を超えたら
   model 再学習 → staging → `model_meta.json` 更新 → Docker rebuild
5. **inverse を含む全試行を `inverse_trials` テーブルへ累積記録** — UNIQUE 制約で重複排除

最適化指標は **NDCG@3** (relevance: 1着=3.0 / 2着=2.0 / 3着=1.0)。
手動ループの 4 exact-ordinal 指標 (top1/place2/place3/top3_box) とは別軸。

---

## §2. Script Reference

### `continuous_learner.py` (539 行)

**目的**: Optuna 特徴量探索 → NDCG@3 改善検出 → inverse 自動試行 → 自動デプロイの統合ループ。

- **主要クラス**:
  - `ContinuousLearner` — ループ本体。`run(max_rounds)` / `request_stop()` / `_explore_round()` / `_maybe_deploy()` / `_check_and_try_inverses()` / `_deploy()`
  - `AdaptiveLoadController` — CPU/mem 負荷に応じて `n_trials` を動的調整 (`round_params() → (n_trials, sleep_secs)`)。psutil 必須、未インストール時は調整なし
- **主要関数**: `write_filtered_parquet()` / `main(argv)`
- **定数**: `DEFAULT_DOCKER_TAG="finish-position-predict-local:split2"` / `DEFAULT_DEPLOY_THRESHOLD=0.005` / `DEFAULT_N_TRIALS=20` / `STRONG_NEGATIVE_THRESHOLD_PP=-1.0`
- **CLI 引数**:

  | フラグ               | 必須 | デフォルト                             | 説明                                       |
  | -------------------- | ---- | -------------------------------------- | ------------------------------------------ |
  | `--features-parquet` | ✅   | —                                      | 学習用 parquet (file or dir)               |
  | `--category`         | ✅   | —                                      | `jra` / `nar` / `ban-ei`                   |
  | `--repo-root`        | ✅   | —                                      | `model_meta.json` 特定用のリポジトリルート |
  | `--registry-path`    | —    | `feature_registry.duckdb`              | DuckDB registry パス                       |
  | `--docker-tag`       | —    | `finish-position-predict-local:split2` | rebuild する image tag                     |
  | `--n-trials`         | —    | `20`                                   | 1 ラウンドの基本 Optuna 試行数             |
  | `--min-trials`       | —    | `5`                                    | 高負荷時の下限 (AdaptiveLoadController)    |
  | `--max-trials`       | —    | `50`                                   | 低負荷時の上限 (AdaptiveLoadController)    |
  | `--deploy-threshold` | —    | `0.005`                                | NDCG@3 改善幅がこの値超でデプロイ          |
  | `--max-rounds`       | —    | なし (無限)                            | N ラウンドで自動停止                       |

  カテゴリ別 training script (`_TRAINING_SCRIPT`): jra/ban-ei → `train_finish_position_catboost_walk_forward.py`、nar → `train_finish_position_xgboost_walk_forward.py`。

### `feature_explorer.py` (273 行)

**目的**: Optuna で特徴量サブセットを探索し、Walk-Forward fold ごとに NDCG@3 で評価する探索エンジン。

- **主要関数**: `run_exploration()` (entry) / `build_objective()` / `evaluate_feature_set()` / `run_fold_with_backend()` / `select_features()`
- **backends**: `DEFAULT_BACKENDS = ("lightgbm", "xgboost", "catboost")` — 各 trial を全 backend で評価し平均
- **定数**: `DEFAULT_TRAIN_START="20160101"` / `DEFAULT_VALIDATION_YEARS=[2023, 2024]` / `MIN_FEATURES=5` (選択特徴 5 未満の trial は NDCG=0 で棄却)
- objective は `trial.suggest_categorical(f"use_{col}", [True, False])` で各列の採否を探索し、`registry.maybe_promote()` で active 昇格を判定

### `feature_registry.py` (304 行)

**目的**: 試行・デプロイ・inverse 試行を保持する DuckDB-backed registry。コンテキストマネージャ。

- **クラス**: `FeatureRegistry(db_path)` — `with FeatureRegistry(path) as reg:`
- **3 テーブル**:
  - `feature_trials` (id, trial_id, ndcg_at_3, is_active, feature_names, definition_json, created_at)
  - `deployments` (id, ndcg_at_3, feature_count, deployed_at)
  - `inverse_trials` (id, original_trial_id, inverse_name, approach_type, delta_pp_json, decision, created_at) — **`UNIQUE(original_trial_id, inverse_name)`** で dedup
- **主要メソッド**: `maybe_promote()` / `record_trial()` / `activate()` / `get_active_entry()` / `get_best_ndcg()` / `get_deployed_ndcg()` / `record_deployment()` / `list_trials()` / `record_inverse_trial()` / `has_inverse_been_tried()` / `list_strongly_negative_trials(threshold_pp=-1.0)` / `list_untried_inverses()`
- **定数**: `INVERSE_APPROACH_TYPES = ("feature_negate", "weight_invert", "window_invert", "anti_correlation")` / `NDCG_IMPROVEMENT_THRESHOLD=0.005`
- sequence は pre-migration DB との id 衝突を避けるため `MAX(id)+1` に同期して再生成

### `subgroup_diagnostics.py` (186 行)

**目的**: 予測品質を source × surface × distance_band で分解し、弱いサブグループを特定する (学習データは分割せず診断専用)。

- **主要関数**: `compute_subgroup_diagnostics(predictions, ground_truth)` (entry) / `evaluate_subgroup()` / `assign_subgroup_keys()` / `compute_race_ndcg()` / `compute_race_top1()` / `compute_race_top3_box()`
- **次元**:
  - source: `jra` / `nar` / `banei` (keibajo_code=83)
  - surface: `turf` (track 10-22) / `dirt` (23-29) / `other` ※ jra のみ、それ以外は dirt 固定
  - distance_band: sprint (<1200) / mile (<1600) / intermediate (<2000) / long (<2400) / extended
- 返値: `list[SubgroupMetrics]` (subgroup, race_count, ndcg_at_3, top1_accuracy, top3_box_accuracy)、subgroup key 昇順

### テスト (全て 95%+ カバレッジ強制)

`tests/learning/test_continuous_learner.py` / `tests/learning/test_feature_explorer.py` / `tests/learning/test_feature_registry.py` / `tests/learning/test_subgroup_diagnostics.py`

---

## §3. Inverse-Signal Exploitation

大幅に負の結果を出した lever は、その「逆」を自動試行する機構。

- **トリガー**: ある trial が primary metric のいずれかで `delta_pp < -1.0pp`
  (`STRONG_NEGATIVE_THRESHOLD_PP`) → INVERSE_CANDIDATE。
  `feature_trials.definition_json` の `delta_pp` を `list_strongly_negative_trials()` が走査
- **4 approach 種別** (`INVERSE_APPROACH_TYPES`):
  - `feature_negate` — 特徴量の符号反転
  - `weight_invert` — サンプル重みの反転 (例: 棄却した期間を upweight)
  - `window_invert` — 学習窓の反転 (例: 切った古いデータを採用)
  - `anti_correlation` — 逆相関信号として再利用 (例: specialist 不一致を confidence に)
- **Dedup**: DuckDB `inverse_trials` の `UNIQUE(original_trial_id, inverse_name)`。
  ループは `has_inverse_been_tried(trial_id, inverse_name)` で実行前に skip 判定。
  `inverse_name = f"{trial_id}__{approach}"`
- **実行**: 各 inverse は `run_exploration()` を半分の試行数 (`max(n_trials//2, 3)`) で回し、
  `delta = best_ndcg - active_ndcg` の正負で `ADOPT`/`REJECT` を決定 →
  `record_inverse_trial()` で永続化

---

## §4. Feature Trial Log Format

**注意**: 2 つの記録系がある。混同しないこと。

1. **DuckDB registry** (`feature_registry.duckdb`) — `continuous_learner.py` が使う
   **自動ループの永続ストア** (§2 `feature_registry.py` 参照)。
2. **`tmp/v8/feature-trial-log.json`** — **手動 iterative loop** が読み書きする
   蓄積 JSON。read-append。特徴量ごとに正/逆の試行と negative_signal_bank を保持。

`feature-trial-log.json` のスキーマ:

```json
{
  "features": {
    "<name>": {
      "original_trial": { "category", "date", "delta_pp": {...}, "decision" },
      "inverse_trial": { "approach", "description", "delta_pp": {...}, "decision" } | null,
      "strongly_negative": false,
      "negative_metrics": ["place2", ...]
    }
  },
  "negative_signal_bank": [
    {
      "feature_or_lever": "<name>",
      "category": "nar",
      "worst_metric": "place2",
      "worst_delta_pp": -1.556,
      "inverse_approaches_tried": ["anti_correlation"],
      "inverse_approaches_remaining": ["feature_negate", "weight_invert"],
      "first_observed": "2026-06-23"
    }
  ],
  "summary": {
    "total_features_tested": 0,
    "adopted": 0,
    "rejected": 0
  }
}
```

手動ループ側の per-iteration / 累積 state の詳細は
`.claude/skills/finish-position-training-loop.md §12` を参照。

---

## §5. Running the Loop

```bash
cd <repo-root>/apps/pc-keiba-viewer
uv run python src/scripts/learning/continuous_learner.py \
  --features-parquet <path> \
  --category <jra|nar|ban-ei> \
  --repo-root <repo-root> \
  [--registry-path <path>] \
  [--docker-tag <tag>] \
  [--n-trials N] \
  [--min-trials N] \
  [--max-trials N] \
  [--deploy-threshold F] \
  [--max-rounds N]
```

### 実行例 (JRA、無制限ループ)

```bash
cd /Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data/apps/pc-keiba-viewer
uv run python src/scripts/learning/continuous_learner.py \
  --features-parquet /data/features/jra/features.parquet \
  --category jra \
  --repo-root /Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data \
  --n-trials 20 \
  --deploy-threshold 0.005
```

### 実行例 (テスト用 3 ラウンドのみ)

```bash
uv run python src/scripts/learning/continuous_learner.py \
  --features-parquet /data/features/jra/features.parquet \
  --category jra \
  --repo-root /Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data \
  --max-rounds 3
```

### 前提条件

```bash
uv --version                                    # uv 利用可
docker info --format '{{.ServerVersion}}'       # Docker/Colima 起動済
ls <features-parquet>                            # parquet 存在
ls <repo-root>/apps/finish-position-predict-container/src/predict_lib/model_meta.json
memory_pressure                                  # free ≥ 30% (heavy 学習前、§5 of training-loop)
```

### 停止

- **Ctrl+C** (SIGINT) / `kill <PID>` (SIGTERM) で現在ラウンド終了後に安全停止。
- Docker rebuild 中に強制終了すると staging 済み状態になり、次回起動で再デプロイされる
  (失敗時は `_rollback_deploy()` が staged dir 削除 + `model_meta.json` 復元)。

### トラブルシューティング

| エラー                                   | 対処                                                   |
| ---------------------------------------- | ------------------------------------------------------ |
| `model_meta.json not found`              | `--repo-root` を確認                                   |
| `model.json not found in fold directory` | `--resume-from-checkpoint` なしで fold を再学習        |
| `docker: command not found`              | `colima start`                                         |
| `No parquet files found`                 | `--features-parquet` のパス / parquet 生成を確認       |
| `ModuleNotFoundError`                    | `uv sync`                                              |
| 全ラウンド改善しない                     | `--deploy-threshold` を下げる or `--n-trials` を増やす |

---

## §6. Cross-references

- **手動 iterative loop + DO-NOT-RETEST レジストリ**: `.claude/skills/finish-position-training-loop.md`
  (特に §6 = 再試行禁止リスト、§12 = inverse-signal 記録フォーマット)
- **本番モデル状態**: `apps/finish-position-predict-container/src/predict_lib/model_meta.json`
- **DuckDB バケット評価 (本体)**: `apps/pc-keiba-viewer/src/scripts/aggregate_bucket_eval_duckdb.py`
- **DB 接続 / メモリ予算ルール**: `.claude/skills/finish-position-training-loop.md §5, §11`
