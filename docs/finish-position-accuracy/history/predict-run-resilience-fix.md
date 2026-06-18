# predict-run resilience fix — connect retry + per-category isolation + reconnect

日付: 2026-06-19
対象: `apps/finish-position-predict-container`

---

## 背景

2026-06-18 NAR の serve top1 が 16.7%（通常比 −20pp 超）に急落した。
根本原因は `predict_upcoming.py` の run が transient DB エラーで `exit 1` し、
全カテゴリ（JRA/NAR/Ban-ei）の predictions が書き込まれないまま終了したことである。

odds 再生成 cron はその直前に完走済みのため再走されず、serve 側は前回の
pre-odds median fallback のまま配信された。

### 3 つの失敗モード

| #   | エラー種別                                               | 発生経路                                                                                                                                  |
| --- | -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `psycopg.errors.AdminShutdown`                           | Neon compute が長時間フィーチャビルド中に autosuspend → write connection が dead socket を掴む。`_flush_predictions` の `_execute` で発生 |
| 2   | `psycopg.OperationalError` / "Name or service not known" | container 起動直後の Docker bridge resolver blip による DNS 失敗。bootstrap probe で発生し retry なしで `exit 1`                          |
| 3   | "connection is lost" / "connection is closed"            | Neon が connect と初回 use の間に TCP 接続を close （race condition）                                                                     |

いずれも transient であり、再接続で解消する。また 6/17 夜 run でも同種の stall が
確認されており、chronic な問題であった（単発 incident ではない）。

### 症状の連鎖

```
predict_upcoming.py run
  → bootstrap probe or first category write で AdminShutdown / DNS blip
    → exception → (failure mode 2: exit 1 immediately from bootstrap)
                   (failure mode 1/3: category failed → races_predicted==0 → exit 1)
      → 全カテゴリ predictions 未書込
        → odds 再生成 cron は既に完了 → 再走なし
          → serve: pre-odds fallback (median) を配信
            → top1 急落（16.7%）
```

---

## 修正内容

### 1. connect retry-with-backoff (`connect_postgres_with_retry`)

`src/db_driver.py` に追加。`_connect()` が `connect_postgres_with_retry` を呼ぶため、
bootstrap probe・per-category write 接続・audit 接続の全接続パスが自動的にカバーされる。

- transient エラー（上記 3 種）のみ最大 `CONNECT_MAX_RETRIES=4` 回リトライ
- バックオフ: `1s → 2s → 4s → 8s → ...`（最大 16s cap）
- 非 transient（認証失敗・DB 名不正など）は即 re-raise（リトライ無駄打ち防止）
- `lazy-connect 47598be` の上に補強する形で実装

```python
CONNECT_MAX_RETRIES: int = 4
CONNECT_BACKOFF_BASE_SECONDS: float = 1.0
```

### 2. per-category isolation（main ループの独立 try/except）

`src/predict_upcoming.py` の `main()` category ループは既に各 category 独立の
`try/except` で囲まれている（47598be commit から）。今回の修正で以下を確認・維持:

- 1 category が失敗しても他の category は完走する（partial commit）
- 全 category 失敗時のみ `sys.exit(1)`（launchd が翌日再試行）
- 部分成功時は成功 category 分の predictions が serve に反映される

### 3. reconnect-on-write（`_execute` の mid-write transient 対応）

`_execute` が mid-write transient エラーを検知した場合に 1 回だけ reconnect+retry する。

- 古い接続は `rollback()` → `contextlib.suppress` で `close()`（どちらも失敗しても無視）
- `_connect(db_url)` で新接続を取得して同じ SQL を再実行
- `_flush_predictions` が `(written, connection)` を返すよう変更 → caller が reconnect 後の正しい接続を `close()` する
- retry も失敗した場合はそのエラーを呼び元に伝播

### 4. `_is_transient_error` helper (`db_driver.py`)

AdminShutdown（クラス名マッチ）と OperationalError 系（メッセージ部分マッチ）を
一元判定するヘルパー。`_execute` と `connect_postgres_with_retry` の両方が参照する。

---

## テスト / カバレッジ

| ファイル                                | 追加テスト内容                                                                                                                                                      |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/test_db_driver.py` (新規)        | `_is_transient_error` 正/負判定 11 ケース、`connect_postgres_with_retry` retry contract・バックオフスケジュール・非 transient 即 raise・デフォルト定数整合 9 ケース |
| `tests/test_predict_upcoming.py` (追加) | `_execute` reconnect 系 5 ケース、`_flush_predictions` reconnect propagation 3 ケース                                                                               |

- predict_lib coverage: **100%**（678 statements / 204 branches、全 miss=0）
- 全テスト: **529 passed**
- ruff check: **All checks passed** / ruff format: **exit 0**
- basedpyright: **0 errors**

---

## デプロイ計画

本修正は `apps/finish-position-predict-container` のソースのみ変更する。
本番への反映には split2 Docker image の再ビルド + R2/DO デプロイが必要。

**orchestrator 認可待ち・未実施。**

---

## 残課題

- **realtime-odds JSONDecodeError**（2026-06-18 観測）は `sync-realtime-data-hot`
  worker 側の別件。本修正の対象外。
