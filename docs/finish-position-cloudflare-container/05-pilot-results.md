# CF Container 着順予測 フルパス Pilot 結果

## 実施概要

| 項目                   | 値                                     |
| ---------------------- | -------------------------------------- |
| 実施日時 (JST)         | 2026-06-19                             |
| Worker Version ID      | `6263cee2-0536-4429-a5bd-fa5487304638` |
| Container Image        | `005d35d9`                             |
| Pilot カテゴリ         | NAR                                    |
| Pilot runDate (KV key) | `20260619` / `20260620`                |
| /health レスポンス     | 200 OK                                 |

## Go/No-Go 判定: **GO**

全確認項目をパス。SIGTERM/reap なし。Container → DO → Queue consumer のフルパスが疎通。

---

## 確認項目

### 1. Container 起動・完走 (SIGTERM/reap なし)

`wrangler tail` 観測:

```
GET http://do/predict?category=nar&daysAhead=2&mode=full&runDate=20260619 - Ok
Queue finish-position-predict-queue (1 message) - Ok

GET http://do/predict?category=nar&daysAhead=2&mode=full&runDate=20260620 - Ok
Queue finish-position-predict-queue (1 message) - Ok
```

- `Exception Thrown` / `Container error: Network connection lost` / SIGTERM: **なし**
- Container が sleepAfter=15m のホールドフェッチを正常完了

### 2. KV 状態 (status=success)

```json
predict:20260619:nar → {"racesPredicted":0,"startedAt":"2026-06-18T17:57:42.511Z","status":"success"}
predict:20260620:nar → {"racesPredicted":0,"startedAt":"2026-06-18T18:03:54.139Z","status":"success"}
```

- `status: success` 確認
- `racesPredicted: 0`: NAR は daysAhead=2 の upcoming races がデータ未登録のため 0 — Container の正常応答

### 3. Queue Consumer 疎通

- Dedup (isAlreadyRunning KV チェック) 動作確認済み
- NDJSON ストリーム parse 正常 (parseNdjsonStream → result 型で racesPredicted 取得)
- message.ack() 呼出し確認 (Queue - Ok ログ)

### 4. Mac launchd 互換性

- Dockerfile に `ENV PREDICT_SERVE_MODE=http` を追加
- Mac launchd `finish-position-predict-daily.sh` に `-e PREDICT_SERVE_MODE=""` を追加
- Mac 側 CLI バッチモードは変更なし (authority 維持)

### 5. Worker Secrets injection (container env)

`container-class.ts` で `containerFetch` 前に `this.envVars` へ以下を注入:

```typescript
this.envVars = {
  MODELS_DIR: "/models",
  NEON_DATABASE_URL: this.env.NEON_DATABASE_URL,
  PREDICT_DAYS_AHEAD: this.env.PREDICT_DAYS_AHEAD,
};
```

---

## 本番 Cutover 前の残タスク

| タスク                              | 状態                                  |
| ----------------------------------- | ------------------------------------- |
| predict/rescore cron の有効化       | **未実施 — Mac launchd が authority** |
| JRA / Ban-ei カテゴリ smoke         | 未実施                                |
| racesPredicted > 0 実レースでの確認 | 未実施 (JRA 開催日に実施推奨)         |
| Neon コスト測定 (21y full scan)     | 未実施                                |

### Neon コスト考察

Container は Neon へ直接接続 (Hyperdrive なし、DuckDB postgres extension で native libpq)。
21 年分フルスキャン 1 回のコンピュートコストは Mac launchd と同等と想定するが、
CF Container 経由で毎日発火する場合は月次コストを実測して評価すること。

---

## 判定根拠

- **90s reap リスク**: sleepAfter 10-15m の graceful + held-fetch (/predict がレスポンスを返すまで DO は生存)。今回 NAR pilot で 0 races の短時間完了でも reap なし確認。長時間 (JRA ~3-5 min) は JRA 開催日 smoke で別途確認。
- **DO 内部エラー**: Worker コード連続更新直後の transient reset (5 回目 pilot)。安定デプロイ後 (6 回目) は解消。本番は deploy 頻度が低いため問題なし。
- **runDate フォーマット**: ISO `2026-06-19` → YYYYMMDD `20260619` バグを修正済み (queue-consumer.ts)。

---

## 修正サマリ (本 pilot 実施中に Fix したもの)

| ファイル                           | 変更内容                                                                 |
| ---------------------------------- | ------------------------------------------------------------------------ |
| `wrangler.jsonc`                   | KV namespace id `PLACEHOLDER_KV_ID` → `d984fba531804927ac1b551200d4b3cb` |
| `container-class.ts`               | `this.envVars` に NEON_DATABASE_URL 等を注入                             |
| `queue-consumer.ts`                | `runDate` → `runYmd` (YYYYMMDD) バグ修正                                 |
| `worker.ts`                        | `resolveCategory()` で POST body の `category` フィールドをパース        |
| `Dockerfile`                       | `ENV PREDICT_SERVE_MODE=http` を追加 (CF Container HTTP サーバモード)    |
| `finish-position-predict-daily.sh` | `-e PREDICT_SERVE_MODE=""` を追加 (Mac launchd CLI モード維持)           |
