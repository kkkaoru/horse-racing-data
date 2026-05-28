# scripts/ — AI 向け開発ルール

## カバレッジ 95% 以上を必ず維持 (Python)

このディレクトリは `pyproject.toml` の `[tool.pytest.ini_options]` に `--cov-fail-under=95` を設定し、`lefthook.yml` の pre-commit で `scripts-pytest` (`cd scripts && uv run pytest`) が実行されます。**95% 未満になるコミットはブロックされます**。

```toml
[tool.pytest.ini_options]
addopts = "--cov=pc_keiba_auto_update --cov-report=term-missing --cov-fail-under=95"
[tool.coverage.run]
branch = true
source = ["pc_keiba_auto_update"]
```

### コードを追加・変更したときに必ずやること

1. 同じ `tests/` 配下に `test_<module>.py` を作成・更新する。新規関数・分岐ごとに少なくとも 1 ケース。
2. `cd scripts && uv run pytest` をローカルで実行し、しきい値を満たすことを確認する。
3. 既存テストが落ちた場合、テスト側の期待値が古いのか、コードの不具合かを切り分ける(回帰の隠蔽を禁止)。
4. 新規モジュールを追加した場合は `pyproject.toml` の `[tool.coverage.run] source = [...]` および `addopts = "--cov=<module>"` を必ず追加する。

### しきい値を満たすためのチェックリスト

- 新しい `if` / `elif` / `else` / `try`/`except` / `match` を追加したら、各分岐を 1 つ以上のテストで通すこと。
- `Optional[T]` の None 経路は両側 (None / 非 None) をテストで通す。
- 早期 return (guard clause) は両側 (true / false) をテストで通す。
- `pragma: no cover` の濫用禁止。本当に到達不可能なコード (raise SystemExit, `...` Protocol 本体など) は `[tool.coverage.report] exclude_lines` ですでに除外済み。

### しきい値を下げるのは禁止

`pyproject.toml` の `--cov-fail-under=95` を下げる変更は、**ユーザーが明示的に承認した場合のみ** 許可されます。AI 側の判断で下げてはいけません。
`source = [...]` / `addopts = "--cov=<module>"` から計測対象を外して見かけ上のカバレッジを上げる回避も禁止。

### 推奨コマンド

```sh
cd scripts && uv run pytest                  # しきい値含むテスト
cd scripts && uv run ruff check              # lint
cd scripts && uv run ty check                # type check (ty)
cd scripts && uv run basedpyright            # type check (basedpyright)
```

### テストコードのスタイル

- `from unittest.mock import MagicMock` / `pytest-mock` で I/O は必ず mock 化
- 1 つのテストは 1 つの振る舞いのみを検証する (NOT DRY 推奨)
- `pytest.raises` で例外パスもテストする
- ファイル / ネットワーク / Windows API への直接アクセスは禁止 (`pywinauto` 周りは mock)
