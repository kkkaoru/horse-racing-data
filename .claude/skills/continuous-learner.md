# continuous-learner — 連続学習ループ起動スキル

連続 Walk-Forward 特徴量探索 + 自動デプロイループ (`continuous_learner.py`) を安全に起動・管理するスキルです。

## このスキルが行うこと

1. 起動に必要な引数をユーザーへ確認する
2. 前提条件 (Docker / uv / parquet ファイル) を検証する
3. `uv run python src/scripts/continuous_learner.py ...` コマンドを構築して実行する
4. 実行中の注意点と停止方法を案内する

---

## 引数確認チェックリスト

以下の情報をユーザーに確認し、不足があれば質問する。

| 引数 | CLI フラグ | 必須 | デフォルト | 説明 |
|------|-----------|------|-----------|------|
| 特徴量 parquet パス | `--features-parquet` | ✅ | — | 学習に使う parquet ファイルまたはディレクトリ |
| カテゴリ | `--category` | ✅ | — | `jra` / `nar` / `ban-ei` のいずれか |
| リポジトリルート | `--repo-root` | ✅ | — | リポジトリのルートディレクトリ (`model_meta.json` の場所を特定するため) |
| レジストリ DB パス | `--registry-path` | — | `feature_registry.duckdb` | DuckDB ファイルのパス |
| Docker タグ | `--docker-tag` | — | `finish-position-predict-local:split2` | ビルドする Docker イメージのタグ |
| 基本試行数 | `--n-trials` | — | `20` | 1 ラウンドあたりの Optuna 試行数 |
| 最小試行数 | `--min-trials` | — | `5` | 高負荷時の下限 |
| 最大試行数 | `--max-trials` | — | `50` | 低負荷時の上限 |
| デプロイ閾値 | `--deploy-threshold` | — | `0.005` | NDCG@3 の改善幅がこの値を超えたらデプロイ |
| 最大ラウンド数 | `--max-rounds` | — | なし (無限) | 指定すると N ラウンドで自動停止 |
| PostgreSQL DSN | `--pg-dsn` | — | なし | Colima PG の接続文字列 (負荷制御に使用) |

---

## 前提条件の確認手順

以下を Bash で確認し、不足があればユーザーに知らせる。

```bash
# 1. uv が利用可能か
uv --version

# 2. Docker が利用可能か
docker info --format '{{.ServerVersion}}'

# 3. parquet ファイルが存在するか
ls <features-parquet-path>

# 4. リポジトリルートに model_meta.json が存在するか
ls <repo-root>/apps/finish-position-predict-container/src/predict_lib/model_meta.json

# 5. feature_registry.duckdb を置くディレクトリが書き込み可能か
ls <registry-path の親ディレクトリ>
```

---

## コマンド構築と実行

`apps/pc-keiba-viewer` ディレクトリで実行する。

```bash
cd <repo-root>/apps/pc-keiba-viewer

uv run python src/scripts/continuous_learner.py \
  --features-parquet <path> \
  --category <jra|nar|ban-ei> \
  --repo-root <repo-root> \
  [--registry-path <path>] \
  [--docker-tag <tag>] \
  [--n-trials <int>] \
  [--min-trials <int>] \
  [--max-trials <int>] \
  [--deploy-threshold <float>] \
  [--max-rounds <int>] \
  [--pg-dsn <dsn>]
```

### 実行例 (JRA、無制限ループ)

```bash
cd /path/to/horse-racing-data/apps/pc-keiba-viewer

uv run python src/scripts/continuous_learner.py \
  --features-parquet /data/features/jra/features.parquet \
  --category jra \
  --repo-root /path/to/horse-racing-data \
  --n-trials 20 \
  --deploy-threshold 0.005
```

### 実行例 (テスト用 3 ラウンドのみ)

```bash
uv run python src/scripts/continuous_learner.py \
  --features-parquet /data/features/jra/features.parquet \
  --category jra \
  --repo-root /path/to/horse-racing-data \
  --max-rounds 3
```

---

## ログの読み方

標準出力に以下の形式でログが出力されます。

```
2026-06-20 12:34:56  INFO     continuous_learner  ━━━ 連続学習ループ 開始 ━━━  カテゴリ: jra | 最大 10 ラウンド | 基本試行数: 20
2026-06-20 12:34:56  INFO     continuous_learner  ─── ラウンド 1/10 開始 (試行数: 20) ───
2026-06-20 12:45:12  INFO     continuous_learner  デプロイ閾値未満のためスキップします  現在: 0.6123 | デプロイ済み: 0.6100 | 差分: +0.0023 | 必要改善幅: 0.0027
2026-06-20 12:45:12  INFO     continuous_learner  ─── ラウンド 1/10 完了 (所要時間: 616.3 秒) ───
...
2026-06-20 13:10:00  INFO     continuous_learner  NDCG@3 が +0.0063 改善しました (0.6100 → 0.6163): デプロイを開始します
2026-06-20 13:10:00  INFO     continuous_learner  ┌── デプロイ開始 ──────────────...
2026-06-20 13:10:00  INFO     continuous_learner  │  バージョン : auto-jra-20260620131000
2026-06-20 13:10:00  INFO     continuous_learner  │  NDCG@3    : 0.6163
2026-06-20 13:10:00  INFO     continuous_learner  │  特徴量数  : 87 列
```

---

## 停止方法

- **Ctrl+C** (SIGINT) または `kill <PID>` (SIGTERM) で送信するとループが現在のラウンド終了後に安全停止します。
- Docker ビルド中に強制終了するとモデルファイルが staging 済みの状態になります。その場合は次回起動時に再デプロイされます。

---

## トラブルシューティング

| エラー | 原因と対処 |
|--------|-----------|
| `FileNotFoundError: model_meta.json not found` | `--repo-root` が正しいか確認する |
| `docker: command not found` | Docker / Colima が起動しているか確認する (`colima start`) |
| `No parquet files found` | `--features-parquet` のパスが正しいか、parquet が生成済みか確認する |
| `ModuleNotFoundError` | `uv sync` でパッケージをインストールする |
| NDCG@3 が全ラウンド改善しない | `--deploy-threshold` を下げるか、`--n-trials` を増やす |
