# pc-keiba-viewer — AI 向け開発ルール

## カバレッジ 95% 以上を必ず維持 (TypeScript + Python)

このパッケージは TypeScript と Python の両方を含み、**いずれも 95% 以上**を維持します。

### TypeScript

`vitest.config.ts` で 4 指標すべてに 95% のしきい値を設定し、`lefthook.yml` の pre-commit で `bun run --filter pc-keiba-viewer test:coverage` が実行されます。**95% 未満になるコミットはブロックされます**。

```ts
thresholds: { branches: 95, functions: 95, lines: 95, statements: 95 }
```

### Python

`pyproject.toml` の `[tool.pytest.ini_options]` に `--cov-fail-under=95` を設定し、`lefthook.yml` の pre-commit で `bun run python:check` (内部で `uv run pytest`) が実行されます。**95% 未満になるコミットはブロックされます**。

```toml
[tool.pytest.ini_options]
addopts = "--cov=corner_lightgbm --cov=finish_position_lightgbm --cov=finish_position_features_duckdb --cov=finish_position_transformer --cov-report=term-missing --cov-fail-under=95"
[tool.coverage.run]
branch = true
```

Python 側で新規スクリプトを追加した場合は、`--cov=<module>` を `addopts` に追加して計測対象に入れること (計測から外して見かけ上のカバレッジを上げる回避は禁止)。

### コードを追加・変更したときに必ずやること

1. **TypeScript**: 同じディレクトリに `*.test.ts` / `*.test.tsx` を作成・更新する — 新規関数・分岐ごとに少なくとも 1 ケース。
2. **Python**: `tests/test_<module>.py` を作成・更新する。`src/scripts/foo.py` を追加したら `tests/test_foo.py` を必須。pytest-cov の `--cov=<module>` 指定も `pyproject.toml` に追加する。
3. **ローカルで実行**: `bun run --filter pc-keiba-viewer test:coverage` (TS) と `uv run pytest` (Python) の両方を実行し、しきい値を満たすことを確認する。
4. 既存テストが落ちた場合、テスト側の期待値が古いのか、コードの不具合かを切り分ける(回帰の隠蔽を禁止)。

### しきい値を満たすためのチェックリスト

- 新しい if / 三項演算子 / `??` / `||` / `&&` を追加したら、両方の分岐を 1 つ以上のテストで通すこと。
- null / undefined のフォールバック (`?? null`, `?? ""`, `?? 0`) は、フォールバックが「到達可能」なら必ずテストする。到達不可能なら **そのフォールバック自体を削除する** (Coverage は防御的な死コードを許さない)。
- 早期 return (guard clause) は両側 (true / false) をテストで通す。
- `Array.prototype.toSorted` などの比較関数の各タイブレーカーは、実際にそのタイブレーカーが効くデータでテストする。

### include / exclude 範囲

`vitest.config.ts` の `coverage.include` / `coverage.exclude` を変更してしきい値をすり抜けるのは禁止。

- **新規ファイルを exclude に追加する場合は、そのファイルがテスト不可能 (Server Component, Cloudflare Worker bindings, barrel re-export) であることを確認し、PR / コミットメッセージで理由を明記すること。**
- include 範囲を狭めて分母を減らすのは禁止。

### しきい値を下げるのは禁止

`vitest.config.ts` の `thresholds` 値を下げる変更は、**ユーザーが明示的に承認した場合のみ** 許可されます。AI 側の判断で下げてはいけません。

### 推奨コマンド

```sh
# TypeScript
bun run --filter pc-keiba-viewer test                   # 高速テスト実行
bun run --filter pc-keiba-viewer test:coverage          # しきい値含むフルチェック

# Python
uv run pytest                                            # しきい値含むテスト (--cov-fail-under=95)

# コミット前 (lefthook と同じ)
bun run --filter pc-keiba-viewer check                  # format + lint + tsc + test:coverage
bun run --filter pc-keiba-viewer python:check           # ruff + ty + basedpyright + pytest (cov)
```

### テストコードのスタイル (`.claude/rules/typescript.md` の主要点)

- `describe` の使用は最小化、ネストを避ける
- `toContain` / `expect(...).includes(...).toBe(true)` は使用禁止 → `toStrictEqual` を使う
- `toBe` / `toStrictEqual` の引数に文字列展開や変数結合を入れない (固定リテラルのみ)
- テストコードでは DRY ではなく **NOT DRY** に書く (各テストを自己完結させる)
- テスト内 for ループ・ネストを避ける
- ファイル/ネットワーク I/O は必ず mock する
