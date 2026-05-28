# sync-realtime-data-hot — AI 向け開発ルール

## 役割

旧 `sync-realtime-data` から分離した **高頻度オッズ取得専用** Worker。`* * * * *` cron による odds polling と odds 保存・読み込みを担当し、旧 D1 (`sync-realtime-data`) に対する CPU pressure を切り離す。

- **管理対象**: `odds_snapshots`, `odds_fetch_state`, `fetch_logs` (新 D1 `sync-realtime-data-hot`)
- **管理外**: `realtime_race_sources`, `daily_race_entries`, `running_styles`, `premium_*`, `win5_*`, `track_condition_*` — 旧 Worker 側

## 不要 D1 アクセス抑制 (Gate 1-7)

新 D1 への書き込み圧力も将来 saturate させないため、以下の 7 層 Gate で **「触る前に弾く」** 設計を維持:

| Gate | 場所                                        | 役割                                                         |
| ---- | ------------------------------------------- | ------------------------------------------------------------ |
| 1    | `src/gates/polling-window-gate.ts`          | JST 06-21 外は cron tick 完全 no-op                          |
| 2    | `src/gates/race-list-kv-cache.ts`           | 当日 race 一覧 KV cache (race_key + race_start + last_fetch) |
| 3    | `src/gates/enqueue-lock-kv.ts`              | 動的 TTL (発走 ±5min は 0、-30min~ は 20s、それ以外 60s)     |
| 4    | `src/gates/edge-cache.ts`                   | Cache API for GET /api/odds/:raceKey, insert 同期 purge      |
| 5    | `src/gates/latest-odds-kv-mirror.ts`        | 最新オッズ KV mirror、fetched_at 鮮度チェック                |
| 6    | `src/gates/r2-archive.ts`                   | 7 日超の odds_snapshots を R2 にミラー (削除しない)          |
| 7    | `src/gates/edge-cache.ts` (D1 query 結果版) | Cache API で D1 read 結果を colo-local cache                 |

**`?fresh=1` / `X-Odds-Force-Fresh: 1`** で全 cache layer を bypass して D1 直行。発走 ±5 分窓では viewer 側が常に force-fresh を使う。

## カバレッジを必ず維持

| Metric         | しきい値   | 補足                                         |
| -------------- | ---------- | -------------------------------------------- |
| **Lines**      | **>= 95%** |                                              |
| **Statements** | **>= 95%** |                                              |
| **Functions**  | **>= 95%** |                                              |
| **Branches**   | **>= 95%** | dead defensive arm (`?? null` 等) は残さない |

```ts
// vitest.config.ts
const COVERAGE_THRESHOLD = 95;
```

`bun run test:coverage` で 4 指標が **どれか一つでも** 上記未満なら CI / pre-commit がブロック。**しきい値を下げるのは禁止** — ユーザーが明示的に承認した場合のみ可。

### コードを追加・変更したときに必ずやること

1. **同じディレクトリに `*.test.ts` を作成・更新** — 新規関数・分岐ごとに少なくとも 1 ケース。
2. **`bun run --filter sync-realtime-data-hot test:coverage`** で 4 指標すべてがしきい値を満たすことを確認。
3. 既存テストが落ちた場合、テスト側の期待値が古いのか、コードの不具合かを切り分ける。

### しきい値を満たすためのチェックリスト

- 新しい `if` / 三項演算子 / `??` / `||` / `&&` を追加したら両分岐をテスト。
- `?? null` / `?? defaultValue` は **到達可能なら必ずテスト**、不可能なら **fallback 自体を削除** し `!` で済ませる。
- 早期 return (guard clause) は両側テスト。
- Cloudflare Worker bindings (D1 / R2 / Queue / KV / DO / Service / Cache API) はすべて `vi.mock` でモック。

### include / exclude 範囲

`vitest.config.ts` の `coverage.include` / `coverage.exclude` を変更してしきい値をすり抜けるのは禁止。

### 推奨コマンド

```sh
bun run --filter sync-realtime-data-hot test                # 高速テスト実行
bun run --filter sync-realtime-data-hot test:coverage       # しきい値含むフルチェック
bun run --filter sync-realtime-data-hot tsc                 # 型チェック
bun run --filter sync-realtime-data-hot lint                # oxlint
```

### テストコードのスタイル (`.claude/rules/typescript.md` 主要点)

- `describe` 最小化、ネスト避ける
- `toContain` / `expect(...).includes(...).toBe(true)` 禁止 → `toStrictEqual` / `toBe`
- `toBe` / `toStrictEqual` 引数は固定リテラルのみ (文字列展開 / 変数結合禁止)
- NOT DRY (各テスト自己完結)
- for ループ / ネスト避ける
- ファイル / ネットワーク I/O は `vi.mock` / `vi.stubGlobal("fetch", ...)`
- `from "bun:test"` 禁止 — vitest を使う

## データ削除禁止

`odds_snapshots` / `odds_fetch_state` / `fetch_logs` の **DELETE / TRUNCATE / DROP / retention sweep** は禁止。容量都合での削減は R2 archive (`Gate 6`) を使う。
