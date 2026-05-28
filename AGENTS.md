Use bun/bunx, not use npm/npx

## Per-package rules — coverage thresholds are enforced

All listed packages enforce minimum coverage via their own config + `lefthook.yml` pre-commit. The thresholds CANNOT be lowered without explicit user approval, and the coverage `include` / `source` must not be shrunk to hide regressions.

- `apps/pc-keiba-viewer/` — **TypeScript + Python**. See `apps/pc-keiba-viewer/CLAUDE.md`. TS enforced by `vitest.config.ts` (all 4 metrics >= 95). Python enforced by `pyproject.toml --cov-fail-under=95` over `corner_lightgbm`, `finish_position_lightgbm`, `finish_position_features_duckdb`, `finish_position_transformer`.
- `apps/local-postgresql/` — **TypeScript**. See `apps/local-postgresql/CLAUDE.md`. Enforced by `vitest.config.ts` (all 4 metrics >= 95).
- `apps/sync-realtime-data/` — **TypeScript (Cloudflare Workers)**. See `apps/sync-realtime-data/CLAUDE.md`. Enforced by `vitest.config.ts` (all 4 metrics >= 95). Branches was raised from 90 to 95 by removing dead `??` arms and testing reachable fallback arms — same playbook applies if it ever regresses.
- `scripts/` (repo root Python) — **Python**. See `scripts/CLAUDE.md`. Enforced by `pyproject.toml --cov-fail-under=95` over `pc_keiba_auto_update`.

Other apps (`apps/horse-racing-duckdb/`, `packages/*`) currently have no enforced threshold — when AI edits them, do not regress whatever level exists, but the hard rules above apply to the listed packages.

## Coverage-protecting rules (apply to every enforced package above)

When AI edits files inside any enforced package, ALL of the following must hold. Per-package `CLAUDE.md` / `AGENTS.md` may add tactical detail on top, but cannot weaken these.

1. **しきい値を下げる変更は禁止** — `vitest.config.ts` の `thresholds`、 `pyproject.toml` の `--cov-fail-under` を下げる変更はユーザーが明示的に承認した場合のみ可。
2. **計測対象を狭めるのは禁止** — `coverage.include` を縮小したり、 既存ファイルを `coverage.exclude` に逃がしたりして分母を減らさない。 新規ファイルを `exclude` に追加するのは、 そのファイルが構造上テスト不可能 (one-shot CLI, Worker binding 直接呼び出し, pg pool 起動など) と確認した上で、 コミットメッセージに理由を明記した場合のみ。
3. **`/* v8 ignore */` / `/* c8 ignore */` の新規追加は禁止** — 例外として `apps/sync-realtime-data/` の `if (import.meta.main)` CLI wrapper のみ事前承認済み。 それ以外の箇所で使いたい場合は適用前にユーザーに承認を取る。
4. **`// oxlint-disable*` / `// eslint-disable*` / `// @ts-ignore` / `// @ts-expect-error` の新規追加は禁止** — lint 警告は本物のコード問題を示しているケースが多い。 まず `vi.spyOn`, 型ガード, dead arm 削除など実装側で解消する。 やむを得ない場合は適用前にユーザーに承認を取る。
5. **lefthook を `--no-verify` で迂回するのは禁止** — pre-commit (oxfmt / oxlint / tsc / test:coverage) が止めた場合は原因を直してから commit する。
6. **コード追加・変更時は同じディレクトリの `*.test.ts` / `tests/test_*.py` も同時に更新** — 新規関数・分岐ごとに最低 1 ケース。 既存テストが落ちた場合は期待値かコードかを切り分け、 回帰の隠蔽は禁止。
7. **commit 前に `bun run --filter <pkg> test:coverage` (TS) / `uv run pytest` (Python) を local 実行**し、 4 指標すべてがしきい値を満たすこと、 lint 0 warnings、 tsc 0 errors を確認する。
