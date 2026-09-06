# 2026-09-06 JRA 障害の恒久対策

## 確認できた原因と対策

### 日次取得の dedupe キー

未来日付の管理API実行が当日の定時取得キーを先に消費していた。`daily-keiba-sync` は JST 当日を超える runDate を拒否する。回帰テストあり。管理トークンは Worker / GitHub Actions / Git-ignored root .env で同期済み。値はこの文書へ記録しない。

### Catalog の物理ファイル置換と D1 位置索引

本番 Iceberg 履歴で以下を確認した（時刻 JST）。

| テーブル | 日時           | 親 snapshot      | 置換 snapshot       |
| -------- | -------------- | ---------------- | ------------------- |
| jvd_ra   | 09-05 04:29:15 | 6915615671667222 | 252960237411825118  |
| jvd_se   | 09-05 04:29:27 | 4483724952280408 | 7660463430763251038 |

両方とも `operation=replace`, `deleted-data-files=5`, `added-data-files=1`。日次 Worker の commit 記録にはない物理ファイル置換により、D1 の `file_path + row_position` が古くなった。これはコンパクションと整合するが、取得した snapshot summary は実行主体までは記録していない。

Cloudflare は自動コンパクションによるファイル書き換えを仕様として提供する:
https://developers.cloudflare.com/r2/data-catalog/table-maintenance/

根本的な設計上の問題は、外部書き換えのある Iceberg に対して、D1 位置索引が常に最新であると仮定し、正常に起こり得るズレを恒久失敗としていたこと。

修正:

- 新規コミットの operation を記録する**前**に必要 partition の snapshot を検証する。
- 不一致では書き込まず、ready 索引を pending に戻して既存 index-plan / index-file Queue で再構築、完了後再開する。構築中索引は中断しない。
- 必要索引が未準備の場合も再構築ジョブを実際に送る。
- index_pending の recovery は、既に ready でも index-plan を再送し、通知欠落後の waiter を起こす。Neon に早送りしない。
- 自分の書き込み後は、table-wide snapshot のため**元 snapshot に一致する全 ready partition**を追従させる。更新対象外の年だけが古くなる不具合を防止し、元から古い索引を誤って ready 扱いにはしない。
- コミットを試行済みで snapshot が変化し、結果が不明な場合は安全停止を維持する。推測で operation を削除・再実行しない。
- ファイル置換→書き込み拒否→索引再構築→再試行成功、古い/構築中索引の非昇格、通知欠落の回帰テストを追加。

コンパクションを止めて症状を隠す対策は採用しない。継続的な外部書き換えで競合する場合にも不正位置で削除しないことを優先する。

### 特徴量・予測

- 日次 foundation に不足した11 raw特徴量を正式生成し、推論時派生列は raw Parquet projection から分離。
- `field_nige_pressure_rank` は実データから SQL と同じ順位規則で算出。
- `RUNNING_STYLE_REQUIRE_DAY_BASE_CACHE_HIT=1` を維持。ゼロ埋め、ローカル予測代替はしない。
- JRA/NAR の readiness を分離し、一方の未準備が他方の完成済み foundation を止めない。
- 着順予測の完了は現在の KV serving → Viewer API の horse identity/count/model/features で判定。Neon の空テーブルを未生成判定に使わない。

### ヒートマップ

- G1 のみに適用すべき grade 条件を他のタイトル一致レースへ適用していた SQL を修正。
- R2 SQL で未対応の regexp_matches を regexp_match IS NOT NULL に修正。
- 検証済み祖先名の欠損補完は registration と両親で identity を固定し、provider master を常に優先。公式マスター/予測特徴量を書き換えない。
- Catalog 例外を空配列へ変換せず section の retryable unavailable 経路へ伝播。HTTP 200/HIT だけで成功と判定しない。

### データ上位馬

取得側 realtime API と Viewer API 全36レースの各3頭、計108頭を比較し、馬名・騎手名・選出理由の存在/一致を確認した。

- Viewer の upstream HTTP/network failure を `[]` に変換する経路を廃止。
- 不正 payload を拒否し、15秒の source request timeout を設ける。
- 正常な未公開 `dataTopHorses: []` は障害と混同しない。
- 既存の空キャッシュ非保存・再取得、およびUI定期再取得を維持。
- 既存取得処理は data-top 自身の認証判定を使い、他ページのアップセル表示で有効データを消さない。未認証時は既存データを保持する。

重要: `fetchedAt` は最新取得時刻であり初回取得時刻ではない。10:04–10:34 の時刻だけでは初回取得が遅延したと断定できない。過去状態が上書きされており、ユーザーが見た時点の未表示原因をこれだけで単一要因に特定してはいけない。確認できた失敗隠蔽経路とキャッシュ欠落への対策を実装した。

## 検証・本番反映

- daily: 187 tests、lint/type/format、全 coverage 指標95%以上。runtime deploy `1df3094c-5c2f-4c92-91c7-164f671c772f`（後続追加は回帰テストのみ）。
- Viewer: 4953 tests、lint/type、coverage 98.68 / 96.34 / 99.09 / 98.80%。最終 Viewer deploy `f70a4b60-d4ac-4d0e-8439-9a9d01197783`（データ上位馬のエラー伝播を含む）。
- Python 復旧時: 5095 tests, 97.51%。既存 Ruff 12 findings は別件として残し、クリーンと報告しない。
- 直近本番: 着順36/36・491頭、ヒートマップ36/36 HIT、ヒートマップ値監査は騎手/調教師491頭・欠損血統なし・異常なし。データ上位馬36/36・108頭。最終Viewer反映後、通常sectionキャッシュを回避した全36レースで HTTP 200 / BYPASS、馬名・騎手名・選出理由と取得側の一致、異常0を11:05 JSTに確認。SSRも全36ページ・491頭の登録番号を確認した。
- task128 の Viewer suite は exit 0 だが既存 happy-dom localhost fetch の teardown エラーが出力される。lint/type の成功と、この出力がないことは区別する。

作業用の詳細証跡は `tmp/jra-recovery-20260906/` の snapshot-history、finish-serving-audit、heatmap-value-audit、data-top-audit、data-top-final-semantic-audit、ssr-audit を参照。tmp は永続運用のジョブではない。通常の自動再構築は Worker Queue/recovery が担う。

既存の別作業の変更は保持。コミットは行っていない。
