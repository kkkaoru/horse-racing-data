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

**heal されなかった残存項目 (未対処、報告のみ)**:

1. **07-13〜07-15 (NAR) の行自体の欠落** — INSERT が必要でUPDATE-onlyの
   本 heal 権限の範囲外。`corner-features-refresh.ts` を worker.ts に
   配線するか、`generate-win5-overlay.ts` を手動実行 (14日 lookback が
   これらの日を含む形で) すれば埋まる可能性がある。
2. **nar 0103・0519 の finish_position** — `nvd_se` 自体の欠落、別の
   upstream 調査が必要。
3. **`corner-features-refresh.ts` が worker.ts に未配線** —
   コード変更が必要なため本監査では実施しない (提案のみ)。配線されない
   限り、今後も「決済確定前に挿入され、その後二度と再訪されない日」は
   同型の恒久 NULL を生み続ける。
4. **`corner-features-refresh.ts` 自身の `time_sa` 正規表現バグ**
   (符号非対応) — 現在は未配線のため実害は無いが、将来配線する際は
   同時に直すべき。

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
