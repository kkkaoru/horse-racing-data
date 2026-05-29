# sync-realtime-data-features — AI 向け開発ルール

## 役割

旧 `sync-realtime-data` から分離した **特徴量 (daily features) + 脚質予測 + 着順予測 専用** Worker。 旧 D1 (`sync-realtime-data`) に対する CPU pressure を切り離し、 features 本体は per-race R2 Parquet (`pc-keiba-features-archive`) に保存する。

- **管理対象**:
  - R2 Parquet: `features/by-race/{YYYY}/{MM}/{DD}/{source}/{keibajoCode}/{raceBango}.parquet` (新 R2 bucket `pc-keiba-features-archive`)
  - 新 D1 (`sync-realtime-data-features-db`): `race_running_styles`, `race_finish_position_predictions`, `running_style_inference_state`, `finish_position_inference_state`
- **管理外**: `realtime_race_sources` / `daily_race_entries` / odds 系 — それぞれ旧 worker / hot worker 側

## 重要制約

1. **旧 D1 `daily_race_entries` への SELECT は禁止** (Phase 0 方針 3) — 計算は Hyperdrive (Postgres) 直 read のみで完結する
2. **`DailyRaceEntryRow` (47 列相当) を縮小しない** — Parquet schema は旧 `daily_race_entries` の全列を保持
3. **`v8 ignore` / `oxlint disable` / `eslint disable` の新規追加禁止** (CLAUDE.md root 方針)
4. **しきい値 95% を下げない**

## 不要 D1 アクセス抑制 (Gate 1-7)

| Gate | 場所                                     | 役割                                                        |
| ---- | ---------------------------------------- | ----------------------------------------------------------- |
| 1    | `src/gates/polling-window-gate.ts`       | JST 06-21 外は cron tick 完全 no-op                         |
| 2    | `src/gates/race-list-kv-cache.ts`        | 当日 race 一覧 KV cache                                     |
| 3    | `src/gates/enqueue-lock-kv.ts`           | per-race / per-job-type の 60s queue dedupe                 |
| 4    | `src/gates/edge-cache.ts`                | Cache API for /api/features/race-trend                      |
| 5    | `src/gates/latest-features-kv-mirror.ts` | per-race 最新 features を KV mirror して R2 GET を抑制      |
| 6    | `src/gates/r2-list-cache.ts`             | R2 list 結果を KV cache (10 min TTL)                        |
| 7    | `src/gates/parquet-bytes-cache.ts`       | R2 GET Parquet bytes を Cache API に 5 min colo-local cache |

加えて `src/gates/build-state-kv.ts` で旧 worker の `probeDailyRaceEntriesFreshness` を KV ベースに置換 (Phase 0 方針 3 のため).

## カバレッジ要件

`vitest.config.ts` の 4 指標 (lines / statements / functions / branches) すべてに 95% を設定。
`bun run --filter sync-realtime-data-features test:coverage` で 1 つでも下回ると CI / pre-commit がブロック。

### 計測対象外 (`coverage.exclude`)

- `src/types.ts` / `src/index.ts` (型定義 / barrel)
- `src/features/parquet.ts` (`@dsnp/parquetjs` + `hyparquet` の薄ラッパで、 Node stream に依存しており Workers env では真の実行パスをテスト不可)
- `src/features/build.ts` / `src/features/postgres-pool.ts` (Hyperdrive (pg Pool) 起動が必要で worker-only)
- `src/running-style/inference.ts` / `src/finish-position/inference.ts` (skeleton — 次 Phase で実装される LightGBM 推論 logic を入れる時に coverage を上げる)

### コードを追加・変更したときに必ずやること

1. **同じディレクトリに `*.test.ts` を作成・更新** — 新規関数・分岐ごとに少なくとも 1 ケース
2. **`bun run --filter sync-realtime-data-features test:coverage`** で 4 指標すべてを確認
3. 既存テストが落ちた場合は期待値かコードかを切り分ける (回帰の隠蔽禁止)

### 推奨コマンド

```sh
bun run --filter sync-realtime-data-features test                # 高速テスト実行
bun run --filter sync-realtime-data-features test:coverage       # しきい値含むフルチェック
bun run --filter sync-realtime-data-features tsc                 # 型チェック
bun run --filter sync-realtime-data-features lint                # oxlint
```

## テストコードのスタイル

- `describe` 最小化、 ネスト避ける
- `toContain` / `expect(...).includes(...).toBe(true)` 禁止 → `toStrictEqual` / `toBe`
- `toBe` / `toStrictEqual` 引数は固定リテラルのみ (文字列展開 / 変数結合禁止)
- NOT DRY (各テスト自己完結)
- for ループ / ネスト避ける
- ファイル / ネットワーク I/O は `vi.mock` / `vi.stubGlobal("fetch", ...)`
- `from "bun:test"` 禁止 — vitest を使う

## データ削除禁止

新 D1 のすべてのテーブル (`race_running_styles` / `race_finish_position_predictions` / `running_style_inference_state` / `finish_position_inference_state`) の **DELETE / TRUNCATE / DROP / retention sweep** は禁止 (`feedback_no_data_delete`)。
