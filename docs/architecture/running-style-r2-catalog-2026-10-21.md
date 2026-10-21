# 脚質予測の R2 Catalog 化と Neon 依存の棚卸し (2026-10-21)

## 現在の構成

| 経路             | 内容                                                                                                                                                                                            |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 本番の生成       | `sync-realtime-data` が D1 に書き、日ごとの Parquet (`pc-keiba-features-archive/running-style/predictions/by-day/raw-iceberg-v1/...`) と Neon に書く                                            |
| 本番の配信       | コンテナの `add-pacestyle-features.py` はレース単位で by-day Parquet を読む (変更なし)                                                                                                          |
| Catalog への追記 | `daily-keiba-sync` の cron (JST 05:30) が前日までの 3 日分の Parquet のうち etag が変わったものを丸ごと `pc_keiba.race_running_style_model_predictions` に追記する (`running-style-catalog.ts`) |
| 学習             | `attach_source_catalog` の view がキーごとに最新の `prediction_generated_at` を返す。`--pg-url r2-catalog://` の一括ビルドで rs\_\* が埋まる                                                    |
| 再構築・圧縮     | `apps/pc-keiba-r2-catalog/scripts/backfill_running_style_catalog.py` (Parquet > Neon > ローカル PG の優先順で統合し全置換)。障害復旧用で、定期実行は不要                                        |
| 監視             | 成功のたびに `SYNC_CACHE` の `running-style-catalog:last-success` を更新。`pipeline-health-monitor` が 26 時間更新なし・対象 0 件を critical として Discord に通知する                          |

- バックフィル: 1,787,862 行。2024〜2025 年は view 経由でローカル PG と完全一致 (1,722,618 行、差分 0、重複キー 0)。
- Neon の写しは 2026-07/08 の 641 行が欠けていた (2026-08-16 の同期障害)。Parquet (D1 由来) のほうが完全なので、Parquet を最優先にした。
- ローカル PG の 2026 年分 (65,244 行) は Catalog の view から入れ替えた。ローカル PG 経由の学習でも rs\_\* が埋まる。
- 照合 (2026-10-21 23:30 JST): 2026-10-17〜20 の各日で Parquet と Catalog の予測キー数が一致 (808 / 1,003 / 578 / 505)、Parquet にあって Catalog に無いキーは 0。
- 追記は冪等: 実バインディングでの 2 回目の実行は 0 行。commit とウォーターマーク書き込みの間で落ちた場合の再追記は view の重複除去で吸収する。2026-07-15〜10-21 の Parquet 129 個の最終更新は、全て対象日の 3 日後 (D+3) 05:30 より前 (翌日 05:30 以降の更新は 4 個)。書き直しは 3 日の追記窓に収まり取りこぼさない。重複は書き直し分だけで上限があり、読み取りの重複除去で吸収するので圧縮は不要 (2026-10-22 時点の重複 4,683 行は手動テスト 2 回分)。

## まだ残す Neon 依存 (順番の提案)

1. `sync-realtime-data/src/running-style-queue.ts` の Neon 完了ゲート。現状のまま運用でき、対応は不要。外す場合は「D1 書き込み + Parquet 件数」に置き換え、Neon 書き込みを best-effort にする。
2. 学習スクリプトのうち `attach_source_catalog` を通らないもの (`generate_running_style_features_local.py`、`continuous_learner`、`aggregate_bucket_eval_duckdb.py`) はローカル PG を読む (Neon は読まない)。Neon に触るのは `aggregate_bucket_eval_duckdb.py` の評価結果の upsert (`--neon-url`、ビューア表示用) だけで、これは 4 の本番系に属する。Catalog 経由にするのはローカル PG 依存を減らす段階で行う。
3. Catalog に無いテーブル (オッズ系、`jvd_wf`) を Catalog に追加する。それまではローカル PG が必要。ローカル PG の追い付きには `bun run --filter local-postgresql training:pull-neon` (Neon をバックアップとして使う break-glass 経路) を使う。
4. 本番の読み取り (viewer の Hyperdrive 読み取り、WIN5、評価、finish-position の予測 upsert / 監査、cron の readiness / rescore) は Neon が主系のまま。配信影響が大きいので個別に移行計画を立てる。
