# feature_guard × coverage-self-heal 相互作用監査 — 恒久劣化レースの再トリガループ有無 (2026-07-17)

- **担当**: serve-defect-269（feature_guard/completion-guard/self-heal の直系知見を持つ agent への team-lead 指名）
- **仮説 (team-lead)**: feature_guard が劣化入力を正当拒否 → completion guard は行数不足で未完了判定 →
  self-heal cron (15 分毎, JST 10:00-20:59) が gap として再トリガ → 元に戻る、で
  **同一レースを 1 日最大 ~44 回再計算し続けるループ**が起き得るのではないか
  （container 計算課金 + slot 占有リスク）。
- **結論（先出し）**: **仮説は部分的に誤り**。実際の container 再計算（課金の直接要因）は
  設計上 **1 レースにつき 1 日最大 2 回**に固定でキャップされており、無制限ループではない。
  一方、キャップ到達後は**エスカレーションイベント（D1 書込のみ、container 再計算なし）が
  当日の残り時間帯、15 分毎に際限なく記録され続ける**という、より小さい形の未キャップ挙動は
  実在する。**実データでは、記録開始以来この再トリガパターンが1件も発生したことがない**
  （後述）。→ team-lead 選択肢 (b)（理論上の穴・現在実害なし・characterization + doc化で終了）
  を採用し、コード修正は行わない。

## 1. コードレベル確定

### 1.1 再トリガの試行上限は存在する（`MAX_SELF_HEAL_ENQUEUES_PER_RACE = 2`）

`apps/finish-position-cron/src/coverage-self-heal.ts` の `healCandidate()`:

1. `isFocusedFullPredictionComplete()` で完了判定。完了なら何もしない。
2. 未完了なら `countPriorSelfHealEnqueues()` で当日・当該レースの **`enqueued=1` イベント数**
   を D1 (`finish_position_coverage_gap_events`) から数える。
3. **`priorEnqueueCount >= 2` なら `escalateCandidate()`** — `enqueued=false, escalated=true`
   のイベントを記録し `console.error` するのみ。**container への新規リクエストは一切発行しない**
   （`enqueuePredict` も `claimFocusedFullRace` も呼ばれない — 既存テスト
   `coverage-self-heal.test.ts:365` 「escalates instead of re-enqueueing once the per-race
   cap is reached」で `enqueuePredictMock`/`claimFocusedFullRaceMock` 共に
   `not.toHaveBeenCalled()` を assert 済み）。
4. `priorEnqueueCount < 2` の場合のみ、DO ベースの claim (`claimFocusedFullRace`,
   staleness 15 分) を経て `skipDedup:true` の focused-full メッセージを enqueue し、
   `enqueued=true` イベントを記録。

コード内コメント（意図の明文化、`coverage-self-heal.ts:40-45`）:
「2 re-triggers gives one retry beyond the first before treating repeated failure as a
poison-pill race that needs a human, not more blind re-triggering」— **この設計上限は
team-lead の懸念そのものを見越して既に実装されている**。

### 1.2 「際限なく再計算し続ける」という字義通りの無限ループではない、ただし副作用あり

`escalateCandidate()` が記録するイベントは `enqueued=false` であり、
`countPriorSelfHealEnqueues()` の SQL は `where ... and enqueued = 1` で **`enqueued=1`
の行のみ**を数える。つまり、一度 `priorEnqueueCount=2` に達すると、それ以降の毎ティック
（15分毎、JST 10:00-20:59、最大約 44 ティック/日）は必ず「`count>=2` → escalate」の分岐に
入り続け、**container への新規リクエストは二度と発行されない**が、**エスカレーション
イベントの D1 書込（安価、`console.error` ログ含む）は当日の残り時間、際限なく繰り返される**。

→ team-lead の「~44 回」という数字は、正しくは**「container の実計算」ではなく
「D1 への軽量なログ書込」の回数**に対応する。課金インパクトの主要因である container
実行回数は**厳密に 2 回でキャップ**されている。

### 1.3 feature_guard 拒否 → completion 側への「意図的スキップ」伝達経路は存在しない（確認済み）

`isFocusedFullPredictionComplete()`（`focused-full-completion.ts`）は Neon の
実行数（`count(distinct ketto_toroku_bango) where model_version=expected and
ketto_toroku_bango = any(catalog由来の現在の出走馬リスト)`）と、Catalog から取得した
現在の出走頭数を**単純比較**するのみ。feature_guard が「意図的に拒否した」のか
「何らかのエラーで書けなかった」のか「単に未着手」なのかを区別する経路は存在せず、
**行数不足という一つの signature に完全に縮退している**。これは team-lead の仮説の
前提部分として正しい。

### 1.4 late-scratch edge case は既に正しくハンドリングされている（副次確認）

調査中に検討した関連仮説「予測後に馬が取消/除外されると、Neon 側に古い予測行が残り、
Catalog 側の現在出走頭数より多くなって不整合が起きるのでは」は、コード確認の結果
**既に正しく設計されている**ことを確認した: 完了判定 SQL は
`ketto_toroku_bango = any($7::text[])`（`$7` は Catalog から取得した**現在の**
出走馬リスト）で Neon 側のカウントを絞り込んでいるため、取消された馬の古い予測行は
分母・分子の両方から自動的に除外される。逆方向（予測後の追加出走）は正しく
「未完了」判定され、再スコアリングの対象になる（意図通り）。**この経路にバグはない。**

## 2. 実データ検証

### 2.1 本日 (2026-07-17): self-heal 発火イベント 0 件

D1 `finish_position_coverage_gap_events` を `run_ymd='20260717'` で照会 → **0 件**
（時間帯を 00:50-01:20 UTC に絞っても、`run_ymd` 全体でも同じ）。矛盾するようだが
これは §9 で既報の「本日 NAR は全レース 2026-07-15 生成の T+2 事前予測のまま」という
発見と整合する: 完了判定は行数のみを見るため、**古いが行数は揃っている予測は「完了」
と判定され、self-heal は本日一度もギャップを検出していない**。この経路自体は
「バグではなく設計通り」だが、副次的に「stale だが complete」という状態を
self-heal は検出できないことの実例になっている（別件、§(f)⑦で既報告）。

### 2.2 全期間: 再トリガイベント総数 6 件、全て 1 回限り

D1 `finish_position_coverage_gap_events` の**記録開始以来の全行**を
`(run_ymd, category, keibajo_code, race_bango)` でグルーピングして集計:

| run_ymd  | category | keibajo | race | n_enqueued | n_escalated | n_total |
| -------- | -------- | ------- | ---- | ---------- | ----------- | ------- |
| 20260712 | jra      | 02      | 05   | 1          | 0           | 1       |
| 20260712 | jra      | 02      | 08   | 1          | 0           | 1       |
| 20260712 | jra      | 02      | 09   | 1          | 0           | 1       |
| 20260712 | jra      | 02      | 10   | 1          | 0           | 1       |
| 20260712 | jra      | 02      | 11   | 1          | 0           | 1       |
| 20260712 | jra      | 10      | 07   | 1          | 0           | 1       |

**6 件全てが `n_enqueued=1, n_escalated=0`** — 一度も 2 回目の再トリガに至った
レースは存在しない。テーブル全体の集計 (`count(*)=6, sum(enqueued)=6,
sum(escalated)=0`) でも一致。この 6 件は並行監査
(`jra-serving-audit-jun-jul-2026-07-17.md`) が既に特定した「venue==02 ルール欠落
時代の false-positive 再トリガ」（うち 5 件、函館レース、修理済み）と符合する
時期・venue（02 が 5/6）。

**結論: 「同一レースへの複数回再トリガ」自体が、記録開始以来 1 件も観測されていない。**
仮説の再トリガループは理論上の設計余地としては実在するが、**実際に発火した実績がゼロ**。

## 3. リスク定量化（理論上の worst case）

- **container 実計算の worst case**: 1 レースにつき最大 2 回 × 1 回あたり 15〜27.5 分
  (JRA worst-case、doc 記載値) = **1 レースあたり最大約 30〜55 分の「結果的に無駄になる」
  container 実行時間/日**。feature_guard は entries（既に構築済みの特徴行列）に対して
  チェックするため、**高コストな DuckDB feature build 自体は feature_guard 到達前に
  既に完了している** — つまり feature_guard は Neon への誤書込は防ぐが、container の
  計算コスト自体は削減しない。
- **複数レースが同時に劣化する場合**（例: 07-12 Cluster B 相当の障害が self-heal
  稼働中に発生した場合）: 影響レース数 × 30〜55 分/レースで線形にスケールする。
  Cluster B 規模 (72 レース) 相当が起きた場合、理論上 36〜66 時間相当の container
  実行時間が「2 巡目までの空撃ち」に費やされ得る（1巡目は元々必要な試行なので、
  「無駄」なのは 2 巡目のみ = 半分、実質 18〜33 時間相当）。ただし §2.2 の通り
  実績はゼロで、Cluster B 自体も self-heal 由来ではなかった（§7.3/§9 で既報）。
- **D1 書込コスト**: エスカレーション後の繰り返しログは 1 レースにつき最大約 40 回/日
  （キャップ到達ティックから 20:59 JST までの残りティック数）、1 回あたり 1 INSERT。
  D1 の書込単価は container 実行時間と比べて無視できるほど小さく、**課金インパクトの
  主要因ではない**（ログ量としてはノイズになり得るが、コストとしては軽微）。
- **正確な $ 換算**: Cloudflare Container の従量課金体系の正確な単価は本調査の範囲外
  （`reference_cloudflare_billing_api.md` 参照で別途算出可能）。時間換算のみ記録。

## 4. 判定と対応

**team-lead 選択肢 (b) を採用: 理論上のギャップであり、現在進行中の実害はない
(§2 実測ゼロ件) ため、新規コード修正は行わない。**

根拠:

- 設計上のキャップ (2回) が既に実装され、既存テスト
  (`coverage-self-heal.test.ts:365`) が「キャップ到達後は re-enqueue しない」ことを
  characterization test として既に検証済み — **新規テストの追加は不要**と判断した
  （既存テストが team-lead の懸念の核心 = 「無限に container を叩き続けるか」を
  正確にカバーしている）。
- 実データで 1 件も多重再トリガが発生していない。
- 唯一の未キャップな挙動（エスカレーションログの反復）は container 課金に直結せず、
  軽微。

**残す観察事項（コード修正なし、doc 記録のみ）**:

- エスカレーション後もログが 15 分毎に繰り返される点は、運用監視ダッシュボード等で
  ノイズになり得る（同一レースが 40 回近くログに現れる）。将来 D1 の
  `finish_position_coverage_gap_events` を監視/アラートに使う場合は、
  `escalated=1` の**初回のみ**を見る、あるいはレース単位で dedup するクエリを
  使う設計が望ましい（新規コード変更は不要、クエリ側の注意点として記録）。
- feature_guard の拒否と genuine gap を区別する信号経路が無い点（§1.3）は、
  「劣化データが恒久的なのか一時的なのか」を運用者が判別する手段が現状ログの
  目視のみである、という運用上の限界として記録する。

## 5. スコアリングパス edge case の軽量監査（深追いなし、列挙のみ）

- **出走取消馬 (`ijo_kubun_code` 系)**: `apps/finish-position-predict-container` /
  `finish_position_features_duckdb.py` を含む feature-build・scoring パイプライン
  全体を grep したところ、**`ijo_kubun_code`（JV-Data 異常区分コード）への参照が
  一切存在しない**。現在の設計は「取消された馬は源テーブルの行自体が現れなくなる」
  ことに暗黙に依存していると推測されるが、これは確認できていない（JV-Data の実際の
  格納挙動 — 取消確定後に行が消えるのか、`ijo_kubun_code` 付きで残るのか — の検証は
  本監査のスコープ外）。**もし源データが取消馬の行を `ijo_kubun_code` 付きで
  残す挙動なら、その馬が feature 構築・スコアリング対象に含まれ、理論上
  predicted_rank=1 を取消馬に割り当てる可能性がある**（実害は「絶対に勝てない馬が
  1 位予測になる」= ユーザー体験の劣化であり、精度指標の汚染ではない — 取消馬が
  実際に勝つことはあり得ないため）。**要 follow-up、本監査では深追いしない。**
- **極小頭数レース**: `rank.py::rank_within_race` は単純ソートでフィールドサイズに
  依存しないため field size に無関係に正しく動作する。`feature_guard.py` の
  欠損率チェックも entries 平均ベースで件数依存の特殊挙動はない。**問題なし。**
- **同着 (dead heat)**: 予測 (`predicted_rank`) はモデルスコアのソート順であり、
  実着順の同着とは無関係な軸 — 予測パイプライン自体に dead heat 固有のロジックは
  不要（サービング側の懸念ではない）。ただし**精度集計側**（`serve_accuracy_report.py`
  等）が `kakutei_chakujun` の同着表現（JV-Data は同着馬に同一着順番号を付与する
  想定）を正しく「predicted_rank=1 が同着 1 着馬のどちらかと一致すればヒット」と
  扱っているかは、本監査では未検証（精度集計スクリプト側の別監査対象として記録のみ）。

## Artifacts

- 本 doc のみ（コード変更なし）。
