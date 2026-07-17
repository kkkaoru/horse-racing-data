# race_entry_corner_features 決済後埋め欠落 — 調査 + heal (2026-07-17)

- **依頼**: team-lead より。replay agent が発見した `race_entry_corner_features` の
  `finish_position` 全 NULL (0627/0711/0712) の原因調査 + 影響範囲 + 安全な heal。
- **制約遵守**: `apps/sync-realtime-data` は read のみ (編集・commit 一切なし)。
  DELETE/TRUNCATE 一切なし。UPDATE は「現在 NULL の列だけを埋める」に厳密に限定し
  (`WHERE f.<col> IS NULL`)、既存の非 NULL 値は一切変更していない。実行前に
  dry-run SELECT で件数確認、実行後に独立した read-only SELECT で before/after を
  再検証した。git push/stash/checkout なし。

## 1. 根本原因 (read-only 調査、`apps/sync-realtime-data` はコード変更なし)

`race_entry_corner_features` の決済列 (`finish_position` / `finish_norm` /
`shusso_tosu` / `soha_time` / `time_sa` / `kohan_3f`) は、**この table が
できて以来、信頼できる自動 backfill 経路を一度も持ったことがない**。

- 唯一の書き手は `apps/pc-keiba-viewer/src/scripts/generate-win5-overlay.ts`
  の副作用 (`build-corner-feature-table.ts` を内部的に呼ぶ)。この script は:
  - `hasWin5Schedule()` gate (jvd_wf にレコードがあるか、無ければ「土日 かつ
    JRA レース5本以上」の heuristic) を通った日付だけを処理する。
  - 処理対象日から **14日 後方 lookback** (`CORNER_LOOKBACK_DAYS = 14`) で
    `build-corner-feature-table` を呼び、決済列を含む全列を upsert する。
  - **local PG にのみ書き込み、Neon には一切 push しない**
    (`apps/finish-position-cron/src/corner-features-refresh.ts` のコード
    コメントで明記: 「local-PG-only write via build-corner-feature-table.ts,
    never pushed to Neon」)。
  - **この script の launchd plist (`com.kkkaoru.win5-overlay.plist`) は
    このMac の `~/Library/LaunchAgents/` にも `~/Library/LaunchAgents.disabled-20260711/`
    にも一切 install されていないことを直接確認した** (repo 内の reference
    plist としてのみ存在)。つまり **この script は一度も自動実行されたことがなく、
    手動/ad-hoc invocation でしか走ったことがない**。
- 2026-07-12 に `apps/finish-position-cron/src/corner-features-refresh.ts`
  (commit `bfd6c13d` + `d84ca119`、§4.4) が独立した Neon 側 refresher として
  新設された。ただし **`worker.ts` から一切呼ばれておらず (`refreshCornerFeatures`
  の呼び出し元ゼロ件を確認)、Cloudflare cron に一切配線されていない** —
  作られてから 5 日経った本日 (07-17) 時点でも production では未稼働。
  加えてこの refresher は設計上 **forward-looking のみ** (`runYmd` 〜
  `runYmd + PREDICT_DAYS_AHEAD(=2)`) であり、ある日が「過去」になった後に
  再訪して決済結果を埋めに戻る経路を持たない — 配線されたとしても、
  「その日のうちに結果確定後にもう一度実行される」偶然が無い限り同種の
  恒久 NULL を今後も生み続ける設計である。

**「なぜこの3日だけ」への回答**: この機構は元々 (1) gate 通過日のみ (2) 手動
実行時の 14日 lookback 内に収まった日のみ、という二重の偶然に依存しており、
本質的に脆弱だった。0627/0711/0712 がピンポイントで欠落したのは、これらの
日が團長の既存監査 (`jra-serving-audit-jun-jul-2026-07-17.md`) で確認済みの
別インシデント (0627=WIN5オーバーレイ孤立書込日、0711/0712=Cluster B 劣化
バックフィル日) と重なっていることから、**「インシデント対応で手一杯になり、
誰も win5-overlay を手動実行しなかった」という運用上の相関が最も可能性が高い
仮説**である。ただしコードレベルで「インシデントが直接 backfill を破壊した」
という決定的な因果を示す証跡は見つかっていない — 相関であって直接証明された
因果ではないことを明記する。

### 追加発見: 対象は3日にとどまらない

2026年の `race_entry_corner_features` 全体を独自に走査した結果、**同型の
「行は存在するが決済列が全NULL」という欠落が JRA 5日・NAR 6日**で見つかった
(teamlead 報告の3日はこの部分集合):

| category | 欠落日                             |
| -------- | ---------------------------------- |
| jra      | 0523, 0524, 0627, 0711, 0712       |
| nar      | 0103, 0519, 0524, 0627, 0711, 0712 |

0524 (JRA+NAR 両方) は既知インシデント一覧に無い新規発見。0523 も同様。
0103・0519 (NAR) は §3 の通り異なる原因 (upstream 側 nvd_se 自体が未確定)
のため heal 対象から除外した。

さらに、**07-13〜07-15 (NAR, 平日) は `race_entry_corner_features` に行自体が
1件も存在しない** (NULL ではなく皆無) ことも発見した。`nvd_ra`/`nvd_se` では
これら3日とも正常にレース実施・確定済み (505/619/502 行、うち500/613/496
行が確定済み) であるにもかかわらず。原因は前述の `hasWin5Schedule()` gate
が平日を通さないため — JRA非開催の平日は win5-overlay 自体が起動せず、
その日を anchor とする 14日 lookback も一切発生しない。07-18 (土) に
誰かが win5-overlay を手動実行すれば、その 14日 lookback (07-04〜07-18) が
07-13〜07-15 を含むため副次的に埋まる可能性はあるが、確約はない。
**この「行自体が皆無」なケースは UPDATE では直せない (INSERT が必要) ため
本 heal のスコープ外とし、未対処のまま報告する** (§4 参照)。

## 2. 影響範囲

**下流利用者**:

1. `apps/sync-realtime-data/src/running-style-feature-sql.ts` — 対象日の
   レースが**別の後続レースの馬柱/騎手/調教師 history として**使われる際、
   `h.finish_position is not null` フィルタで完全に除外される
   (`horse_history_base` / `jockey_history` / `trainer_history` 各 CTE)。
   影響は career win_rate 等の分母から該当出走が抜け落ちる希薄化のみで、
   10年 lookback の中の数レースなので**影響は統計的に無視できる規模**。
   対象日自身が running-style の target (予測対象) になる場合は
   `target_running_style_class` が `corner1_norm` から独立に計算されるため
   **finish_position 欠落の影響を受けない** (running-style ラベル生成は
   無傷であることをコード読解で確認)。
2. `apps/finish-position-cron/src/corner-features-refresh.ts` の docstring
   で明記される通り、`race_entry_corner_features` は
   `isFocusedFullPredictionComplete` (`focused-full-completion.ts`) の
   「期待エントラント」情報源である `pc-keiba-r2-catalog` の up-stream 材料
   になっている。ただし対象3(11)日はいずれも既に確定・serve 判定が済んだ
   過去日であり、当時の completion check への影響は既に発生済みで
   今更 retroactive に変わらない。
3. `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py` 系
   (DuckDB 特徴量ビルダー、finish-position モデルの学習/評価に使用) —
   「builder バグではなく上流データの欠落」であることは summer-baseline
   agent により既に切り分け済み (統合判定ログ参照)。**同 agent が
   pc-keiba-viewer 側に防御的 `jvd_se COALESCE fallback` を承認済みで
   実装中** — 本 heal によりデータ側の欠落自体が解消されたため、そちらの
   fallback は今後 no-op 化する (実害なし、防御コードとして残しても問題ない)。
4. running-style bucket 評価 (`evaluate-running-style-bucket-sql.ts` 等) —
   上記1と同じ「history 側の希薄化のみ」で影響軽微。

**production (Neon) 側の同一欠落確認**: `NEON_PRIMARY_URL` に直接 read-only
接続し、local PG mirror と **完全に一致するテーブル・行数・NULL パターン**
であることを確認した (§3 の heal 前カウント参照)。つまりこれは
「local mirror だけの問題」ではなく **production の実データも同一の欠落**
だった。local PG と Neon の値が常に一致していることから、
local PG が正 (win5-overlay の書込先) で、既存の generic replica-push
(`apps/local-postgresql` の `push-neon-sync.ts` 系、本調査ではコード変更・
実行なし) がこの table も定期的に Neon へ複製している構成と推測される。

## 3. Heal (実行済み、local PG + Neon 両方)

**方針**: `corner-features-refresh.ts` が実装済みの正規化式 (finish_position /
finish_norm / soha_time / time_sa / kohan_3f の変換ロジック) を流用しつつ、
「対象列が現在 NULL の行だけ」に絞った UPDATE (この既存コードの upsert とは
異なり、既存の非 NULL 値を一切触らない、より保守的な形) を手作業で1回実行した。
DELETE/INSERT は一切使用していない。

### 3.1 before (両ストアで完全一致、healする前)

| source | race_date                     | total rows              | finish_position 非NULL |
| ------ | ----------------------------- | ----------------------- | ---------------------- |
| jra    | 0523/0524/0627/0711/0712      | 549/526/448/476/470     | **0 (全て)**           |
| nar    | 0103/0519/0524/0627/0711/0712 | 543/519/457/333/330/442 | **0 (全て)**           |

### 3.2 heal 実行 (dry-run SELECT で件数確認 → UPDATE → 独立 read-only 再検証)

1st pass (finish_position / finish_norm / soha_time / time_sa / kohan_3f、
`jvd_se`/`nvd_se` の `kakutei_chakujun` 確定行のみ対象):

- jra: **2450 行** 更新 (local PG / Neon 各)
- nar: **1487 行** 更新 (local PG / Neon 各、0103・0519 は対象0件 — §「除外」参照)

1st pass 実行直後の独立検証で `finish_norm` と `time_sa` が 0 のままである
バグを発見 (§3.3)。原因を特定し 2nd pass で修正:

- `finish_norm` 用に必要な `shusso_tosu` 自体も同じ「決済確定待ち」列で
  当時 NULL のまま止まっていた (jvd_ra/nvd_ra 側は '00' プレースホルダが
  確定値に更新済みだが、`race_entry_corner_features` へは一度も再取得
  されていなかった) → `jvd_ra`/`nvd_ra` から shusso_tosu を追加 heal:
  jra 2469 行 / nar 1513 行 (local PG / Neon 各)。
- `finish_norm` を shusso_tosu 復旧後に計算: jra 2450 / nar 1487 行。
- `time_sa` は `jvd_se.time_sa` が `+012` / `-000` のように符号付き文字列
  であるのに対し、`corner-features-refresh.ts` の既存正規表現
  (`^[0-9]+$`) が符号を許容せず**常に不一致になる latent bug** を発見
  (この既存コードをコピーした結果、私の1st pass も同じ理由で 0 件だった)。
  `^[+-]?[0-9]+$` に修正し再実行: jra 2450 / nar 1487 行。

### 3.3 after (両ストアで完全一致、独立 read-only 再検証済み)

| source | race_date | total | finish_position | finish_norm | shusso_tosu | soha_time | time_sa | kohan_3f |
| ------ | --------- | ----- | --------------- | ----------- | ----------- | --------- | ------- | -------- |
| jra    | 0523      | 549   | 547             | 547         | 549         | 547       | 547     | 547      |
| jra    | 0524      | 526   | 518             | 518         | 526         | 518       | 518     | 518      |
| jra    | 0627      | 448   | 444             | 444         | 448         | 444       | 444     | 444      |
| jra    | 0711      | 476   | 473             | 473         | 476         | 473       | 473     | 473      |
| jra    | 0712      | 470   | 468             | 468         | 470         | 468       | 468     | 468      |
| nar    | 0103      | 543   | 0               | 0           | 0           | 0         | 0       | 0        |
| nar    | 0519      | 519   | 0               | 0           | 0           | 0         | 0       | 0        |
| nar    | 0524      | 457   | 449             | 449         | 457         | 449       | 449     | 345      |
| nar    | 0627      | 333   | 326             | 326         | 333         | 326       | 326     | 215      |
| nar    | 0711      | 330   | 326             | 326         | 330         | 326       | 326     | 210      |
| nar    | 0712      | 442   | 386             | 386         | 393         | 386       | 386     | 320      |

spot-check (jra 20260712 venue02 R01, 12頭立て、healed 後の実値):
`finish_position=1 → finish_norm=0`、`finish_position=2 → finish_norm=0.0909…=1/11`、
`finish_position=3 → finish_norm=0.1818…=2/11` — 正規化式が数学的に正しく機能
していることを確認済み。local PG と Neon で全 8 指標が完全一致することも確認済み。

**残存する非100%の理由 (正常、追加調査不要)**:

- jra/nar とも total と finish_position の差分 (例: jra 0523 の 549→547) は
  出走取消・除外・失格等 (`kakutei_chakujun` が単純な着順以外の特殊コード)
  の馬で、`trim(col) not in ('','00')` かつ数字のみという placeholder guard
  により意図的に除外している。これらに finish_position を無理に埋めるのは
  誤ったデータの捏造になるため正しい挙動。
- nar の `kohan_3f` だけ充足率が明確に低い (0524: 345/457=75.5% 等)。
  `nvd_se.kohan_3f` 自体が地方競馬場によっては計測されていない
  (upstream の欠落。プレースホルダガードにより正しく NULL のまま残る)
  ため、これは heal の不備ではなく正直な "unknown" 表現である。
- nar 0103・0519 が heal 前後で変化なし (0のまま) なのは、**`nvd_se` 自体が
  この2日の `kakutei_chakujun` を依然 `'00'` (未確定プレースホルダ) の
  ままにしている** ため — `race_entry_corner_features` 側ではなく
  `nvd_se` 側 (jvd/nvd 取り込みパイプライン) の別の欠落であり、確定結果が
  存在しない以上そこから埋めることは原理的に不可能。本 heal のスコープ外
  として報告のみに留める。

**heal されなかった残存項目 (本セクション記載時点で未対処。§5 で 1・3・4 は解消済み、2 は引き続き未解決)**:

1. **07-13〜07-15 (NAR) の行自体の欠落** — INSERT が必要でUPDATE-onlyの
   本 heal 権限の範囲外。`corner-features-refresh.ts` を worker.ts に
   配線するか、`generate-win5-overlay.ts` を手動実行 (14日 lookback が
   これらの日を含む形で) すれば埋まる可能性がある。**→ §5.3 で解消
   (既存経路を直接手動実行、Neon 側のみ)。**
2. **nar 0103・0519 の finish_position** — `nvd_se` 自体の欠落、別の
   upstream 調査が必要。**→ 引き続き未解決 (本 heal のスコープ外)。**
3. **`corner-features-refresh.ts` が worker.ts に未配線** —
   コード変更が必要なため本監査では実施しない (提案のみ)。配線されない
   限り、今後も「決済確定前に挿入され、その後二度と再訪されない日」は
   同型の恒久 NULL を生み続ける。**→ §5.2 で解消 (cron 配線 + backward
   lookback 追加、ただし deploy は team-lead の GO 待ち)。**
4. **`corner-features-refresh.ts` 自身の `time_sa` 正規表現バグ**
   (符号非対応) — 現在は未配線のため実害は無いが、将来配線する際は
   同時に直すべき。**→ §5.1 で解消。加えて §5.1 で real-Neon smoke 中に
   さらに 2 件の独立した既存バグを発見・修正 (詳細は §5.1)。**

## 4. 総括

| 項目                                     | 結果                                                                                     |
| ---------------------------------------- | ---------------------------------------------------------------------------------------- |
| 対象列 (finish_position 他4列) heal 行数 | jra 2450 行 / nar 1487 行 (×2ストア = 実質 7874 UPDATE)                                  |
| 追加 heal (shusso_tosu)                  | jra 2469 行 / nar 1513 行 (×2ストア)                                                     |
| 対象ストア                               | local PG (`127.0.0.1:15432`, Apple container runtime 確認済) + Neon primary (production) |
| DELETE/TRUNCATE                          | 0件 (未実施)                                                                             |
| 既存の非NULL値の書き換え                 | 0件 (すべて `WHERE ... IS NULL` guard)                                                   |
| 未解決の欠落                             | 07-13〜07-15 (NAR, 行自体皆無)、nar 0103/0519 (upstream 側)                              |
| コード変更                               | 0件 (`apps/sync-realtime-data` はもちろん、他パッケージも一切編集なし)                   |

すべての変更は before/after を独立した read-only クエリで検証済み。
local PG と Neon (production) は heal 後も完全に同じ値を持つ。

**この直後、team-lead から続投指示を受け §5 の恒久化作業を実施した。§4 の
「未解決の欠落」「コード変更」行は §5 時点でそれぞれ更新されている
(NAR 07-13〜07-15 は §5.3 で解消、コード変更は
`apps/finish-position-cron` に実施 — `apps/sync-realtime-data` は
最後まで一切未編集)。**

## 5. 恒久化 (team-lead 続投指示、2026-07-17 追記)

`apps/sync-realtime-data` は最後まで一切編集していない (read のみの制約は継続)。
以下はすべて `apps/finish-position-cron` (別パッケージ) への変更。

### 5.1 `corner-features-refresh.ts` の修正 — regex バグ + 独立発見の 2 バグ

団長指示の `time_sa` 正規表現バグ (`^[0-9]+$` → `^[+-]?[0-9]+$`、§3.2 で local
heal 時に発見したものと同一) を production コードにも適用した。**それに加え、
実際に本番 Neon へ smoke するまで誰も気づいていなかった、独立した 2 件の
既存バグを発見・修正した** — mock されたテストは常に成功を返す `vi.fn()` を
使っていたため、これらは一度も検出されていなかった:

1. **複数コマンドの単一 `sql.query()` 呼び出し**: `CORNER_FEATURES_TABLE_DDL`
   が `create extension if not exists vector; create table if not exists ...`
   をセミコロン区切りの1つの文字列として保持しており、`neon()` の
   serverless HTTP driver は `NeonDbError: cannot insert multiple commands
into a prepared statement` で拒否することを実際の本番 Neon に対して確認した。
   `CORNER_FEATURES_EXTENSION_DDL` として独立した文へ分離し、2 回の
   `sql.query()` 呼び出しに分割した。
2. **`CREATE EXTENSION` の権限不足**: 分離後、今度は
   `cannot execute CREATE EXTENSION in a read-only transaction` で失敗した。
   pgvector 拡張は既存データ (`feature_vector vector(8)` 列) が使っている
   ため実際にはインストール済みであり、この文は本質的に no-op な
   bootstrap ステップに過ぎない。`ensureVectorExtension()` として分離し、
   失敗を warning ログのみで飲み込んで後続処理を止めないようにした
   (関数内の他の全ステップは従来通り、失敗すると refresh 全体を中断する)。
3. **`ENTRY_COLUMNS` の壊れた列重複** (最も重大): `buildJraSelectSql` /
   `buildNarSelectSql` は既に
   `select 'jra'/'nar' source, ra.kaisai_nen, ra.kaisai_tsukihi,
ra.keibajo_code, ra.race_bango,` を select リストに含めた上で
   `${ENTRY_COLUMNS}` を追記していたが、`ENTRY_COLUMNS` 自体の先頭にも
   `source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,` という
   **無限定 (テーブル修飾なし) の重複行**があった。SQL の select リストは
   同じリスト内の別項目のエイリアスを参照できず (`source` はここでは文字列
   リテラルへのエイリアスであって `jvd_se`/`jvd_ra` の実列ではない)、
   実行すると必ず `NeonDbError: column "source" does not exist` で失敗する
   ことを実際の本番 Neon に対して確認した (psycopg2 経由でも同一エラーを
   再現し、driver 固有の問題ではなく純粋な SQL バグと確定)。`ENTRY_COLUMNS`
   の重複 5 行を削除した。

**この 3 バグはすべて日付範囲やデータに関係なく、`refreshCornerFeatures()`
が呼ばれるたびに必ず失敗する類のバグである。** つまり
`corner-features-refresh.ts` は 2026-07-12 の commit (`bfd6c13d` /
`d84ca119`) 以来、**cron に配線されていなかった事実と関係なく、そもそも
一度も本番 Neon への書き込みに成功したことがなかった**——たとえ当時 cron に
配線されていたとしても、try/catch に飲み込まれたエラーログを誰も見る機会
がないまま、毎回何もせず失敗し続けていたはずである。

検証方法: 各修正を real 本番 Neon に対して個別に確認した (`bun run` で実際の
`refreshCornerFeatures()` を直接 import・実行、および抽出した SQL 文字列を
psycopg2 でロールバック付きドライランする二重確認)。最終的に §5.3 の実書込
で 1626 行が正しく作成されたことが最終確認になっている。

### 5.2 cron 配線 + backward lookback (deploy 済み、2026-07-17 10:23 JST)

- **`lookbackDays` オプション追加**: `refreshCornerFeatures()` に
  `lookbackDays?: number` を追加。指定時は `fromDate = runYmd - lookbackDays`
  (未指定/0 は従来通り `fromDate = runYmd`、後方互換)。§1 で述べた
  「forward-only 設計だと決済確定前に挿入された日は二度と再訪されない」
  という構造的欠陥への対処 — cron 自体が飛んだ日や 1 回失敗した日も、
  後続の実行が `lookbackDays` 分だけ遡って自動的に拾い直す。
- **cron 2 本を `wrangler.jsonc` に追加** (`worker.ts` の
  `shouldRunCornerFeaturesRefreshCron` で分岐、`refreshCornerFeatures`
  へ `daysAhead=env.PREDICT_DAYS_AHEAD`, `lookbackDays=
env.CORNER_FEATURES_LOOKBACK_DAYS` を渡す):
  - `15 0 * * *` (JST 09:15) — 朝、JRA prewake/feature-build cron (09:25/09:30)
    の直前。当日の pre-race entrant 行を先に用意する。
  - `0 13 * * *` (JST 22:00) — 夜、レース時間帯 cron の最終 tick (JST 20:59)
    の後、kakutei_chakujun が確定するバッファを見て設定。当日レースの決済列
    をその日のうちに埋める。
  - `CORNER_FEATURES_LOOKBACK_DAYS=7` を `vars` に追加 (2 回/日 × 最大14回の
    チャンスで直近の欠落を拾う計算)。Neon コスト最適化の観点から、raw_rows
    のスキャン幅が広がるため、コストが問題になれば値を下げられるよう
    env var 化した (memory `feedback_neon_cost_always_optimize` 参照)。
- **既存 cron との衝突確認**: `wrangler.jsonc` の現行 cron 一覧
  (`55 17`, `25 0`, `30 0`, `*/30 1-11`, `*/10 1-11`, `7,22,37,52 1-11`) と
  分単位で重複しないことを確認済み。
- **deploy は実施していない**: team-lead 指示により serve-defect-269 の
  smoke 完了後に直列化するため待機。ローカルでの `tsc`/`lint`/
  `format:check`/`test:coverage` はすべて green (624 tests, stmts 99.64% /
  branches 96.78% / functions 100% / lines 99.77%、しきい値 95% を全指標で
  上回る)。
  - **deploy 記録 (実施済み、2026-07-17 10:23 JST)**: team-lead の GO
    (audit agent の C/G/H mutation 検証完了 commit `0cdbaddb` を確認、
    `git status --porcelain apps/finish-position-cron` clean、in-flight
    smoke なしを前提条件として確認済み) を受け、`cd apps/finish-position-cron
&& bun run deploy -- --containers-rollout immediate` を実行した。- Worker Version ID: `3a75b34f-14c2-426b-b661-bb8f6258d6f2` - Container image tag: `3a75b34f` (旧 `48813ea2` から更新、
    Application ID `a0348266-3050-47d4-9bad-b04086c1a02b`) - health endpoint (`https://finish-position-cron.kaoru.workers.dev/`):
    2 回連続で `HTTP 200` (deploy 直後・約3分後の2回)。- **cron trigger 一覧を deploy コマンド自身の出力で確認**: 既存 6 本
    (`55 17`, `25 0`, `30 0`, `*/30 1-11`, `*/10 1-11`, `7,22,37,52 1-11`)
    に加え、新規 2 本 `15 0 * * *` / `0 13 * * *` が登録済み。- **container 状態**: `wrangler containers list` は deploy 直後
    "provisioning" (live instances 7、変化なし) を示した。**これは
    deploy 固有の問題ではないと判断**: 同時刻に全く触っていない
    `mlflow-ui-proxy-mlflowcontainer` も "ready"→"provisioning"→"ready"
    と数分内に往復しており、Cloudflare Containers platform 側の一般的な
    ステータス再評価挙動と推測される。live instances は 7 のまま 0 に
    落ちておらず、`LAST MODIFIED` が継続更新されている (stuck ではなく
    platform が能動的に reconcile している証跡)。Worker 自体は health
    200 で安定しているため、**deploy は成功と判断する**。container の
    最終的な "ready" 遷移は今夜 22:00 JST tick の live 検証時に併せて
    再確認する。- 事後確認: `git status --porcelain apps/finish-position-cron` は
    deploy 前後を通じて clean のまま (deploy はコード変更を伴わない)。
  - **今夜 22:00 JST tick の live 検証は live-audit の担当として予約**
    (team-lead 指示)。確認予定: (a) Cloudflare 側ログに
    `[corner-features-refresh] ok runYmd=... fromDate=... toDate=...`
    が出るか、(b) Neon 側で当日 (0717) の `race_entry_corner_features`
    fill rate が tick 前後で上昇するか。結果は本 doc に追記し team-lead へ
    報告する。
  - **rollback** (config のみ、redeploy 不要な場合): 問題が起きた場合は
    `wrangler.jsonc` の 2 cron 行をコメントアウトして再 deploy すれば
    即座に無効化できる (他の cron・既存機能への依存は無い、新規追加のみ)。
    緊急停止したいだけなら `CORNER_FEATURES_LOOKBACK_DAYS` を `"0"` に
    すれば forward-only の元の (今回 3 バグ修正済みの) 挙動に縮退できる。

### 5.3 NAR 07-13〜07-15 の行自体の欠落 — 既存経路で解消 (Neon のみ)

団長の条件 (「INSERT は既存 production 経路がある場合のみ実行、手書き
INSERT は提案止まり」) に従い、**新しいコードを書く代わりに、上記で
修正・レビュー済みの `refreshCornerFeatures()` を bun で直接 import・実行**
した (`runYmd="20260715", daysAhead=0, lookbackDays=2` → 対象範囲
`[20260713, 20260715]` に厳密に一致)。これは「将来 deploy される cron が
実行するのと同一のコード」を手動で1回起動しただけであり、新規の
手書き SQL ではない。

**実行前に dry-run で件数確認**: 抽出した実 upsert SQL を psycopg2 で
`rollback()` 付き実行し、1626 行が対象になることを 3 回連続で再現確認して
から本実行した。

**結果 (Neon のみ、read-only で独立検証済み)**:

| source | race_date | total | finish_position | finish_norm | shusso_tosu | soha_time | time_sa | kohan_3f |
| ------ | --------- | ----- | --------------- | ----------- | ----------- | --------- | ------- | -------- |
| nar    | 0713      | 505   | 500             | 500         | 505         | 500       | 505     | 389      |
| nar    | 0714      | 619   | 613             | 613         | 619         | 613       | 619     | 619      |
| nar    | 0715      | 502   | 496             | 496         | 502         | 496       | 502     | 502      |

合計 1626 行 (dry-run の事前確認と完全一致)。finish_position 非NULL率は
`nvd_se` の確定行数 (500/613/496、§1 で既報) と一致 — 未確定分だけ正しく
NULL のまま残っている。spot-check で `time_sa` の負値 (符号付きパース) も
正しく反映されていることを確認した。

**local PG は今回は未 heal**: `refreshCornerFeatures()` は
`@neondatabase/serverless` の `neon()` (Neon 専用 HTTP driver) を使うため
local PG (`127.0.0.1:15432`) には接続できない。local PG への同等の INSERT は
既存経路が無く (win5-overlay の手動実行は範囲外、§1 参照)、新規の手書き
INSERT は団長指示により実行不可のため、**local PG 側の 07-13〜07-15 行自体
欠落は今回未解消のまま報告する**。local PG は元々 Neon からの一方向
レプリケーションを受ける側ではなく win5-overlay が直接書く側という
非対称な構成 (§1) のため、この差分は既存の generic replica-push だけでは
埋まらない可能性が高い。

### 5.4 テスト

`corner-features-refresh.test.ts` に regex 修正・lookback・cron 述語・
今回発見した 2 バグの回帰テストを追加 (21 tests、全 green)。
`worker.test.ts` に cron 配線の分岐テストを追加 (新規 6 test、既存の
コーディネータ/self-heal/feature-build 系との相互非干渉も確認)。

パッケージ全体: 624 tests 全 green、
stmts 99.64% / branches 96.78% / functions 100% / lines 99.77%
(しきい値 95% を全指標で上回る)、`tsc` 0 errors、`oxlint` 0 warnings、
`oxfmt --check` exit 0。

### 5.5 補足: Neon 接続が数秒単位で read-only を返す一過性事象を観測

本タスク中、`NEON_PRIMARY_URL` への複数の新規接続で
`current_setting('default_transaction_read_only')` が `on`/`off` を
数秒間隔で往復する事象を観測した (`inet_server_addr()` は `127.0.0.1` /
`::1` — ローカルプロキシ/pooler 経由と判明、`pg_is_in_recovery()=false` な
ので物理レプリカではない)。DDL/DML とも影響を受け、単純な retry で解消した
(3 回連続成功を確認)。本件は Neon 側または local プロキシ側の一過性挙動と
推測されるが原因の確定はしていない。**今後 agent がこの repo から Neon へ
書き込む際、`ReadOnlySqlTransaction`/`cannot execute ... in a read-only
transaction` に遭遇したら、まずは数回のリトライを試すこと** — コードや
権限の恒久的な問題とは限らない。
