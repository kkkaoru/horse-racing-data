# sync-realtime-data — AI 向け開発ルール

## カバレッジを必ず維持

このパッケージは Cloudflare Workers (D1 / R2 / Queue / Durable Objects) を多用するため、Branches は v8 v8-counter が `??` / `||` / `&&` / 三項演算子を 1 分岐ずつ計測する特性上、他指標より到達しにくい。それを踏まえて以下のしきい値を **`vitest.config.ts`** で固定しています。

| Metric         | しきい値   | 補足                                                    |
| -------------- | ---------- | ------------------------------------------------------- |
| **Lines**      | **>= 95%** | 意味的網羅                                              |
| **Statements** | **>= 95%** | 意味的網羅                                              |
| **Functions**  | **>= 95%** | 公開 API の網羅                                         |
| **Branches**   | **>= 90%** | parser/規制値の `??` / ternary が多数を占めるため別管理 |

```ts
// vitest.config.ts
const COVERAGE_THRESHOLD = 95;
const BRANCHES_THRESHOLD = 90;
thresholds: {
  lines: COVERAGE_THRESHOLD,
  branches: BRANCHES_THRESHOLD,
  functions: COVERAGE_THRESHOLD,
  statements: COVERAGE_THRESHOLD,
}
```

`bun run test:coverage` で 4 指標が **どれか一つでも** 上記未満なら CI / pre-commit がブロックします。**しきい値を下げるのは禁止** — ユーザーが明示的に承認した場合のみ可。

### コードを追加・変更したときに必ずやること

1. **同じディレクトリに `*.test.ts` を作成・更新** — 新規関数・分岐ごとに少なくとも 1 ケース。
2. **`bun run --filter sync-realtime-data test:coverage`** をローカル実行し、4 指標すべてがしきい値を満たすことを確認。
3. 既存テストが落ちた場合、テスト側の期待値が古いのか、コードの不具合かを切り分ける(回帰の隠蔽を禁止)。

### しきい値を満たすためのチェックリスト

- 新しい `if` / 三項演算子 / `??` / `||` / `&&` を追加したら、両方の分岐を 1 つ以上のテストで通すこと。
- null / undefined のフォールバック (`?? null`, `?? ""`, `?? 0`) は、**フォールバックが「到達可能」なら必ずテストする**。到達不可能 (regex matchAll 後の capture group など) なら **そのフォールバック自体を削除** し `match[1]!` で済ませる (防御的死コードは coverage と保守性の両方を悪化させる)。
- 早期 return (guard clause) は両側 (true / false) をテストで通す。
- Cloudflare Worker のバインディング (D1 / R2 / Queue / Durable Object) はすべて `vi.mock` でモックする — 実バインディング前提のコードは書かない。
- Playwright (`@cloudflare/playwright`) は `vi.mock` で `launch` を差し替え、page/locator は階層 mock を組む。`url()` / `innerHTML()` は呼び出しごとに違う値を返すスタブにし、`clickAndWaitForOdds` の URL/HTML 変化ループに依存する箇所が無限ループしないようにする。

### include / exclude 範囲

`vitest.config.ts` の `coverage.include` / `coverage.exclude` を変更してしきい値をすり抜けるのは禁止。

- 新規ファイルを `exclude` に追加する場合は、そのファイルがテスト不可能 (one-shot CLI、pg pool 起動、wrangler 直接呼び出しなど) であることを確認し、コミットメッセージで理由を明記する。
- `include` を狭めて分母を減らすのは禁止。

### Branches を上げる時の正攻法

`Branches` しきい値を 90% から上げたい場合の優先順位:

1. **dead defensive arms を削除する** — `match[1] ?? ""` のように regex matchAll 後の capture group は常に存在するので `match[1]!` に。1 つ削るだけで Branches 分母 -1 / カバー率 +0.03〜0.06pp。`refactor(realtime): drop dead defensive ?? ...` のような単一目的コミットにする。
2. **`?? null` / `?? defaultValue` の else arm をテストで通す** — 入力に null を含めるバリエーションを追加。
3. **多腕 ternary (`a ? x : b ? y : z`)** はそれぞれの腕を 1 ケース以上テストする。
4. 上記で十分なテストを書いた上でしきい値を **90 → 91 → 92…** と段階的に上げる。下げる方向は禁止。

### 推奨コマンド

```sh
bun run --filter sync-realtime-data test                # 高速テスト実行
bun run --filter sync-realtime-data test:coverage       # しきい値含むフルチェック
bun run --filter sync-realtime-data tsc                 # 型チェック
bun run --filter sync-realtime-data lint                # oxlint
```

### テストコードのスタイル (`.claude/rules/typescript.md` の主要点)

- `describe` の使用は最小化、ネストを避ける
- `toContain` / `expect(...).includes(...).toBe(true)` は使用禁止 → `toStrictEqual` / `toBe` を使う
- `toBe` / `toStrictEqual` の引数に文字列展開や変数結合を入れない (固定リテラルのみ)
- テストコードでは DRY ではなく **NOT DRY** に書く (各テストを自己完結させる)
- テスト内 for ループ・ネストを避ける
- ファイル / ネットワーク I/O は必ず mock する (`vi.mock`, `vi.stubGlobal("fetch", ...)`)
- `import { from "bun:test" }` 禁止 — vitest を使う
