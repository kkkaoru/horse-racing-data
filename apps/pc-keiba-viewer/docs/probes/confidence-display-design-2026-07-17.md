# 確信度表示層 — 設計提案 (2026-07-17)

**Status**: USER 決定⑥により (a)・(b) を実装、本番投入済み (2026-07-18)。(c) は
保留のまま。同日、緊急ユーザー保護として恒久機能 (d) 品質ゲート (非表示型)
を追加したが、同日中に USER 指示で (d') 透明性+ユーザー選択型に作り替え
(下記セクション参照、(d) は役目を終えて置き換え済み)。**Scope**:
`apps/pc-keiba-viewer` の finish-position 予測表示。**3 案とも
`predicted_rank`（推薦順位）を一切変更しない** — 既存予測結果の
「見せ方」のみを変える display-layer 改善であり、§7.2 accept gate や
WF/serve accuracy の gated metrics には一切触れない（§5 で根拠を明示）。

## (d) 品質ゲート — 緊急実装 (2026-07-18)

**契機**: 同日、running-style 生成障害 (502、修復中) により本日の JRA 全
36 レースで garbage 予測 (within-race predicted_score stddev 0.05-0.15、
ほぼランダム) が書き込まれ、viewer で自信ありげに表示される事態が発生
(`jra-269-serve-defect-2026-07-17.md` の Cluster B 実測と同じ signature)。
朝導入した確信度 badge は「低」表示するが、それだけではユーザーが誤って
信頼するリスクを十分に防げない — **順位テーブル自体を表示しない品質
ゲート**を追加。

**実装**: `queries.ts::getFinishPositionLambdarankPredictions` で既に
計算している predicted_score stddev (confidenceTier の算出元) を再利用し、
`FINISH_POSITION_QUALITY_GATE_MIN_STDDEV = 0.5` 未満なら
`isQualityGated: true` をレース単位で設定 (confidenceTier と同じ
propagation 経路: `FinishPositionModelPredictionFeature` →
`buildFinishPredictionRowsFromResults` → `FinishPredictionRow` →
`FinishPositionPredictionTable`)。0.5 は §2 表の「健全個体群はどれも
0.5-0.7 以上、劣化 Cluster B は 0.0439-0.1657」という実分布の谷を根拠に
選定。`isQualityGated` が true の場合、順位テーブルと補正トグル UI を
非表示にし、代わりに「予測を準備中です (品質基準未達のため一時的に非表示
にしています)。」というメッセージを表示する。確信度 badge / E-grade
注意 badge / 検証精度サマリはレース固有の順位情報ではないため表示を維持。

**即時ロールバック**: 環境変数
`PC_KEIBA_FINISH_POSITION_QUALITY_GATE_DISABLED=1` でゲート全体を無効化
可能 (コード変更不要、再 deploy のみで反映)。

**恒久機能としての位置づけ**: このゲートは今回の障害限定の対症療法では
なく、今後同種の garbage 予測 (デプロイ不良、モデル破損等) が再発しても
ユーザーに自信ありげな誤情報を見せない防波堤として恒久的に残す。502 復旧
後、健全な予測が UPSERT されれば stddev がフロアを超えて自動的に表示が
復帰する設計 (手動での再有効化操作は不要)。

**検証**: `bun run --filter pc-keiba-viewer tsc` / `lint` / `format:check`
/ `test:coverage` 全て成功 (176 test files / 3954 tests pass、coverage
statements 99.36% / branches 97.36% / functions 99.14% / lines 99.39%、
閾値 95% を全指標で超過)。

### 本番デプロイ後に発覚した 2 件の stale cache 層 (2026-07-18)

commit `457e933c` を deploy (`ea6afa8f`) した直後、本日 (2026-07-18) の
実レース (函館 02-01) を chrome-devtools で確認したところ、`isQualityGated`
が API レスポンスに一切現れず、順位テーブルがそのまま表示され続けていた
(confidenceTier は正しく "low" を返しており、コード自体は動いていた)。

原因は本ゲートと無関係な 2 層の cache が、ゲート導入前にそのレースへ
一度でもアクセスされていた場合、古い形状の JSON をデプロイ後も TTL 経過
まで返し続けていたこと (`race-cache-bust` の既存 per-race cache-bust
endpoint はどちらの層も対象外):

1. `queries.ts::withDbQueryCache` — content-hash キー、既定 TTL 1 時間。
   `CACHE_NAMESPACE` を `v3`→`v4` に bump して全エントリを即座に
   invalidate (commit `c7cf848f`)。
2. `finish-prediction-inputs-cache.server.ts` — `FinishPredictionBuildInputs`
   全体 (isQualityGated を含む `modelPredictionFeatures` ごと) を各レース
   の発走時刻から最大 6 時間キャッシュする、完全に別の namespace/KV
   key を持つ層。今回の本丸 (発覚が遅ければ最大 6 時間ユーザーに garbage
   順位を見せ続けるリスク) だった。`v2`→`v3` に bump (commit `7c5012f2`)。

2 件とも「新しいフィールドが増えただけ」の純粋な追加的変更で、リテラル
文字列に依存するテストは存在しないため安全に bump 可能と確認済み。両
commit とも `tsc`/`lint`/`format:check`/`test:coverage` 実行後に deploy
(最終 version `c938d065-9288-416a-ba84-1fdcc9c2b3d1`)。

### 実データでの最終検証 (2026-07-18、chrome-devtools MCP)

**Positive (garbage 抑止)**: 本日 2026-07-18 函館 1R
(`/races/2026/07/18/02/01`, 発走済み) — DB 直接算出 stddev は本ゲート
導入前の deploy で既に 0.05-0.16 台 (全 36 レース共通の running-style
生成障害由来) と確認済み。deploy 後、`isQualityGated:true` が
API レスポンスに含まれ、実画面で「予測の自信度: 低」バッジの直下に
「予測を準備中です (品質基準未達のため一時的に非表示にしています)。」
というメッセージが表示され、順位テーブルと補正トグル UI は非表示。
函館(02)/福島(03) の計 5 レースを API 直接確認し、全て `isQualityGated:
true` を確認。

**Negative (健全予測の誤抑止なし、対照確認)**: 2026-06-21 阪神1R
(`/races/2026/06/21/09/01`, ダート1400m, 3歳未勝利) — Cluster B / 本日の
障害と無関係な健全な過去レース。predicted_score stddev を直接算出する
と 1.46 (ゲート閾値 0.5 を大きく上回る)。API レスポンスは
`confidenceTier:"mid"` / `isQualityGated:false`。実画面でも品質ゲート
メッセージは表示されず、16 行の順位テーブル (スコア 1.00〜0.00、馬番・
騎手名・オッズ・複勝率まで) と補正トグル UI が通常どおり表示されること
を確認 — ゲートが健全な予測を誤って隠さないことを実データで確認済み。

## 実装メモ (2026-07-18)

(a)/(b) は worktree branch 上で先に実装され (`commit 99af695d`)、`main` への
cherry-pick 中に `queries.ts` で conflict が発生 (`git status` に `UU` として
07-18 未明から放置)。原因は同じ挿入点で 2 つの独立した module-scope 定数
ブロックが競合しただけ — `main` 側 (`commit 1b74e097`, tier-3 off-label
variant 防御的 hardening) と cherry-pick 側 (本 doc の確信度 tier 計算) は
互いのロジックに一切依存しない。両ブロックをそのまま両方保持する形で解消
(順序: 既存の off-label variant ブロックを先、確信度 tier ブロックを後ろに
配置)。他の 7 ファイル (globals.css / queries.test.ts /
finish-position-prediction-table.tsx(+test) /
finish-position-prediction.ts(+test) / race-types.ts) は cherry-pick 時点で
無 conflict のまま既に stage 済みだった。解消後 `bun run --filter
pc-keiba-viewer tsc` / `lint` / `format:check` / `test:coverage` を実行し、
全て成功 (coverage: statements 99.36% / branches 97.36% / functions 99.14%
/ lines 99.39%、閾値 95% を全指標で超過、176 test files / 3945 tests pass)
したことを確認して `git cherry-pick --continue` 相当のコミットで着地
(commit `7cdf6149`)。

## 本番デプロイ・検証 (2026-07-18)

`bun run --cwd apps/pc-keiba-viewer deploy:worker` でデプロイ。**Version ID
`b11c772c-c3c5-4bf5-82d0-844f672c994b`** (`wrangler deployments list` で
100% トラフィックの最新デプロイと確認済み)。URL:
`https://pc-keiba-viewer.kkk4oru.com`。デプロイ前に `git status --porcelain
apps/pc-keiba-viewer` が空であることを再確認 (`apps/sync-realtime-data` の
別件 WIP は pc-keiba-viewer のビルド依存に含まれないことも
`package.json` の workspace 依存を確認して検証済み)。

**検証レース**: 2026-07-12 函館9R 北海ハンデキャップ
(`/races/2026/07/12/02/09`)。grade_code='E' かつ venue='02' (Hakodate) で
案 (b) の対象、かつ `jra-269-serve-defect-2026-07-17.md` の Cluster B
劣化ウィンドウ (07-12 05:51-05:52 UTC 生成) に含まれる 40 レースの一つで
predicted_score stddev を直接算出すると **0.0795**（Neon 本番テーブルを
直接 SQL で読んで確認）— 低確信度側のしきい値 1.3 を明確に下回る、実デー
タでの境界動作確認に適したレース。

Cloudflare Access 越しの検証のため、`.env.local` の
`PC_KEIBA_ACCESS_CLIENT_ID`/`_SECRET` を注入するローカル一時プロキシ
(scratchpad 限定、リポジトリ非追跡) を経由して chrome-devtools MCP セッシ
ョンで実ページを操作。結果:

- ページロード直後に「アーモンドAI予想を利用しますか」の confirm()
  ダイアログが出る (既存機能、本検証と無関係) → dismiss して継続。
- `document.querySelector('.finish-prediction-confidence-badge')` →
  1 件、テキスト **「予測の自信度: 低」**、class
  `finish-prediction-confidence-badge-low`。stddev 0.0795 → low tier
  というコード上の期待値と一致。
- `document.querySelector('.finish-prediction-upset-warning-badge')` →
  1 件、テキスト **「⚠ 荒れやすい傾向のレース」**。
- 両バッジはレース単位の予測テーブル直上に並んで表示され、
  予想着順 (predicted_rank) は着順予測スコア降順のまま不変
  (1.00/0.92/0.85/0.77/0.69…) — 表示専用で順位に影響しないことを実データ
  で確認。
- コンソールに WebSocket handshake 501 (`trends/live` /
  `paddock/live`) と 504 / `ERR_INCOMPLETE_CHUNKED_ENCODING`
  (`horse-weights-stream`, `time-score`, `condition`) のエラーが出たが、
  **すべて検証用一時プロキシの制約が原因と切り分け済み**: プロキシは
  upgrade リクエストを明示的に 501 で拒否し (WebSocket 中継未実装)、
  レスポンスを `.text()` でバッファするため真のストリーミングを壊し、
  8 秒のハード abort タイムアウトを持つ。`time-score`
  (200, 472,928 bytes) と `condition` (200, 47,275 bytes) は本番へ直接
  (プロキシ経由せず、WAF 対策の `User-Agent: curl/8.7.1` 付きで) リクエス
  トして問題なく 200 で返ることを個別に確認 — 本番側の欠陥ではない。
  `paddock/live`/`trends/live`/`horse-weights-stream` はこの badge 機能
  と無関係な既存のリアルタイム機能で、今回の検証スコープ外。
- スクリーンショット: 両バッジとテーブル上部を含む viewport
  キャプチャあり (scratchpad 保存、リポジトリには含めず)。

以上により (a)/(b) は実データ・本番環境で意図どおり動作していることを確認。

## 0. なぜ今この角度か

順位変更系のレバー（confidence-shrinkage 07-11 REJECT、E-top2
place-preserving override 廃止、longshot detector v1/v2 REJECT 等）は
本キャンペーンで全て閉鎖済み。しかし閉鎖の理由は「予測をいじる（re-rank
する）用途では効果が確認できなかった」であり、その根拠になった実測信号
自体（E-grade レースの荒れやすさ、within-race score stddev）は本物で、
`jra-summer-upset-refresh-2026-07-17.md` §3 も「E-grade は display-layer
calibration idea としてなら使える、REJECT されたのは training feature /
re-ranking gate としてだけ」と明記している。表示層でユーザーに事実を伝える
という用途はこれまで誰も設計していない — これが本 doc の対象。

## 1. 現状のアーキテクチャ（3 案共通の土台）

- **表示コンポーネント**: `src/app/races/detail/finish-position-prediction-table.tsx`
  の `FinishPositionPredictionTable`（`"finish-prediction"` タブ、タブ見出し
  「着順予測」）。行コンポーネント `FinishPredictionTableRow`
  （L477-554）。マウント元は `lazy-detail-sections.tsx:779` →
  `race-detail-page.tsx` の `RaceDetailView`。
- **データ経路**: `detail-section-data.ts:1508-1613` が
  `getActiveFinishPositionPredictions(race, runners)` を呼び、
  `src/db/queries.ts` の `getFinishPositionLambdarankPredictions`
  （L2919-3082）が `race_finish_position_model_predictions` から
  `model_version, umaban, predicted_score, predicted_rank` を SELECT
  （L3035-3039）。**`predicted_score` は SQL 上は取得されているが、行
  mapper（L3061-3075）で捨てられている** — `predicted_rank` から
  `predictedFinishNorm` を作るだけで、生スコアはクライアントまで届かない。
  以降 `src/lib/finish-position-prediction.ts`（`buildFinishPredictionRowsFromInputs`）
  →`FinishPredictionRow`（`src/lib/race-types.ts:679-691`）にも
  `predictedScore` フィールドは存在しない。
- **既存バッジ/indicator パターン**（新規デザインの参考、共有 Badge
  コンポーネントは無く素の `<span>/<p>` + kebab-case グローバル CSS）:
  - `finish-position-bucket-section.tsx:229-241` の
    `finish-position-bucket-small-sample-badge`（少サンプル注意バッジ、
    同じ着順予測タブ内・集計レベル）— 最も近い前例。
  - `running-style-section.tsx:294` の `MetricsBadge`（modelVersion +
    macro-F1 表示）。
  - `frame-number-badge.tsx` の `FrameNumberBadge` 等、小さい再利用バッジ。
  - 既存の「odds-correction overlay」トグル（`finish-prediction-odds-toggle`,
    L556/749）が UI 上のトグル・注記パターンの前例。
- **race メタデータ**: `grade_code`（`gradeCode`）は `queries.ts` 全体で
  既に多用されており（L248 等）、レース単位で常に取得済み。venue
  （`keibajoCode`）も同様。**案 (b) は追加フェッチが不要。**
- **スタック**: Next.js 16.2（App Router/RSC）、React 19.2、TypeScript、
  プレーングローバル CSS（Tailwind/CSS Modules 無し）、Drizzle ORM
  （`sql` タグ付きテンプレート、`getDb()`）。

## 2. 案 (a) per-race 確信度インジケータ（3 段階バッジ）

**シグナル**: レース内（within-race）`predicted_score` の標準偏差
（stddev）。同じ指標が本日 `jra-269-serve-defect-2026-07-17.md` で
serving 品質監視に使われており、実測値が既にある:

| population                                | stddev 実測範囲    | 出典        |
| ----------------------------------------- | ------------------ | ----------- |
| 健全 fallback 経路                        | 0.7〜1.4           | 同 doc L22  |
| 健全 (Cluster B 外、07-07/07-08)          | 1.3173〜1.4359     | 同 doc L103 |
| 健全 preflight（36 races、全て "0.5 超"） | 0.89〜1.76         | 同 doc L503 |
| **劣化 (Cluster B、40 races)**            | **0.0439〜0.1657** | 同 doc L102 |
| 運用上のヘルス下限（目安）                | ≳0.5               | 同 doc L263 |

**重要な区別**: 上記の「0.5」は「パイプラインが壊れていないか」を判定する
運用監視用の下限フロアであり、案 (a) が使いたい信号はそれとは別 —
健全なレース群の**内部でのバラつき**（0.5〜1.8 超の範囲で実際に variance
がある）こそが「モデルがこのレースをどれだけ強く差別化できているか」＝
確信度の実質信号。3 段階バッジの閾値は健全 population 内の tertile 分割を
想定（暫定値、実装時に実分布から再算出すべき）: 目安として low < 0.8 /
mid 0.8-1.2 / high > 1.2（いずれも劣化フロア 0.5 を明確に超えている前提）。

**実装**:

1. `queries.ts::getFinishPositionLambdarankPredictions` 内で、行を個別に
   返す前に、そのレースの全馬 `predicted_score` から stddev を 1 回計算
   （race 単位、馬単位ではない）。個々の生スコアを外に出す必要はない —
   `predictionConfidenceTier: 'low'|'mid'|'high'`（または raw stddev
   の 1 値）だけをレース単位の payload に追加すれば足りる。
2. `detail-section-data.ts` の finish-prediction payload に新フィールド
   を素通しする。
3. `race-types.ts` の該当型（レース単位の親情報、`FinishPredictionRow`
   個々ではなく）に 1 フィールド追加。
4. `finish-position-prediction-table.tsx` のタブ見出し付近（行ごとでは
   なくレース単位）に 3 段階バッジを表示。

**工数**: 小〜中。`predicted_score` は既に SELECT 句にある（新規クエリ
不要）。SQL 集計 1 個 + 型 2 箇所 + UI バッジ 1 個。

**文言案**:

> 予測の自信度: 高 / 中 / 低
> （ツールチップ）馬同士の予測スコアの差が大きいほど「自信度」を高く表示
> しています。的中を保証するものではありません。

## 3. 案 (b) E-grade × 函館・札幌「荒れやすい」注意バッジ

**シグナル**: `grade_code == 'E'`（特別戦）かつ venue が 01 札幌／02 函館
（弱いが同方向の傾向は 03 福島／10 小倉にも）。指標は S2 = 勝ち馬自身の
`tansho_ninkijun >= 4`（人気薄馬が勝った率）。**2 つの独立した日付の
プルで二重確認済み**（`jra-summer-upset-refresh-2026-07-17.md` L67）:

| venue | 07-04 doc (S2 delta, E-grade − 非E-grade) | 07-17 doc (同、より広い母集団) | 2026 単独 (n 薄い)   |
| ----- | ----------------------------------------- | ------------------------------ | -------------------- |
| 函館  | +12.58pp                                  | +12.34pp                       | +7.30pp              |
| 札幌  | +6.70pp                                   | +5.88pp                        | — (2026 開催前、n=0) |
| 福島  | +6.35pp                                   | +7.33pp                        | +6.71pp              |
| 小倉  | +3.22pp                                   | +2.85pp                        | +8.57pp              |

2026 年に入っても傾向は途切れていない（測定できた 3 venue 全てで
E-grade の S2 delta が正）。**この事実自体は「settled」**（同 doc L138:
「E-grade-related REJECTs（training feature、confidence-shrinkage
gate）を settled として扱うことを間接的に支持」）— REJECT されたのは
「これを使って順位を変える」ことだけで、事実の表示は別の設計判断。

**既存レバーとの関係の明記**: `apps/pc-keiba-viewer/tmp/confidence-shrinkage/`
（07-11）は同じ E-grade×札幌/函館セグメントで**予測スコアを再ランキング
する**ことを試み、selection bias で REJECT された。案 (b) はスコアも
順位も一切変更しない、純粋な注意書き表示であり、このレバーの再挑戦では
ない。

**実装**: `grade_code`/venue は既にレースメタデータにある。追加クエリ
ゼロ、純粋なフロントエンドの条件分岐 + 静的バッジ。

**工数**: 最小。新規データ取得ゼロ、条件分岐 + バッジ 1 個のみ。

**文言案**:

> ⚠ 荒れやすい傾向のレース
> （ツールチップ）特別戦かつ函館・札幌開催のレースは、過去実績で人気薄の
> 馬が勝つ割合が高めです。予測の精度自体が下がるわけではありませんが、
> 波乱の可能性を念頭に置いてご覧ください。

## 4. 案 (c) rank2-5 cell×rank 実測精度の informational 表示

**シグナル**: 本日構築された cell×model×rank1-5 台帳
（`jra-cell-model-selection-ledger-2026-07-17.md`、326 cell 網羅、MLflow
`finish-position/cell-eval`）と `rank1_5_by_venue` 系の補助集計
（venue×rank1-5 個別精度、例: 函館 top1 20.8% / place2 29.2% / place3
37.5% / place4 50.0% / place5 54.2%、0711+0712 実測）。

**実装**: 台帳データは MLflow / 一時 parquet に閉じており、viewer から
直接読める形になっていない。レース context（venue/class/distance-band/
season/surface）に一致する cell の実測精度を、`export_production.py`
の `cell_routing.json` エクスポートに近い形の**新規 export パイプライン**
（MLflow → 静的 JSON → viewer 配信）で用意する必要がある。

**工数**: 中〜大。(a)/(b) と違い、新規のデータエクスポート/同期経路が
要る（かつ日次更新の運用も設計する必要がある）。

**文言案**:

> このカテゴリの過去実測的中率（参考値、保証ではありません）:
> 単勝 X% ／ 複勝2着内 Y% ／ 複勝3着内 Z% …

## 5. §7.2 gated metrics への非影響

`docs/finish-position-prediction-system.md` §7.2 の accept gate は
`top1/place2/place3`（+ finish-position cell routing では place4-6 /
top3_box）という、**`predicted_rank` の的中率**から計算される cell 単位
の delta 指標であり、WF walk-forward 評価または本番 serve 実測のいずれか
から算出される。案 (a)/(b)/(c) はいずれも:

- `predicted_rank` / `FinishPredictionRow` の並び順を**一切変更しない**
  （§7.2 が読む唯一の入力）。
- WF 評価パイプライン・serve 精度計測パイプライン（`serve_accuracy_report.py`
  等）のいずれにも触れない — viewer の表示専用コードだけを変更する。
- したがって §7.2 の gate 判定・noise floor・cell routing gate の**いずれ
  の数値も一切動かせない**。技術的に動かしようがない設計（表示層は精度
  計測パイプラインの下流にすら存在しない）。

## 6. 誤解を招かない表示原則

1. **確率として提示しない** — 「自信度: 高」であっても「95% 当たる」の
   ような数値化された的中確率と誤解されないよう、相対的な表現
   （高/中/低、注意バッジ）に留める。実際の的中率と表示上の確信度ラベル
   を安易に紐付ける文言（例:「高確信度は的中率◯%」）は避ける。
2. **保証ではないことを明示** — (b)/(c) は「過去実績」「参考値」という
   語を必ず添え、将来の的中を約束しない。
3. **一貫した粒度** — (c) は必ず cell×rank 個別数値（rank1-5 それぞれ）
   で表示し、要約精度（例: fukusho_2p のような合成指標）だけを見せない
   — 本キャンペーンの評価規律（cell 単位×着順個別精度）を表示層にも
   適用する。
4. **既存の odds-correction overlay トグルと同じ透明性方針** — デフォルト
   ON/OFF・出典・計算方法をユーザーが確認できる形にする（tooltip
   またはヘルプリンク）。

## 7. 実装優先度案

| 順位 | 案                        | 理由                                                                |
| ---- | ------------------------- | ------------------------------------------------------------------- |
| 1    | (b) E-grade 注意バッジ    | 新規データ取得ゼロ、条件分岐のみ、工数最小                          |
| 2    | (a) confidence バッジ     | 小さいクエリ変更 + 型追加のみ、predicted_score は既に SELECT 済み   |
| 3    | (c) rank2-5 informational | 新規 export パイプラインが要り、他 2 案よりコスト高。優先度低め推奨 |

## USER 判断事項

- 0〜3 案のうちどれを実装するか（全部 / 一部 / 保留）。
- (a) の 3 段階閾値は暫定値（0.8/1.2）— 実装時に健全 population の実分布
  から再算出すべきか、この暫定値で先に出して後日調整するか。
- (c) を実装する場合、export パイプラインの運用（更新頻度、所有者）を
  誰が設計するか。

## 運用注意: viewer deploy 時の 2 層 cache (2026-07-18 の教訓)

`getFinishPositionLambdarankPredictions` (queries.ts) や
`FinishPredictionBuildInputs`（`modelPredictionFeatures` を含む）を
返す値の**形状**を変えるような viewer の表示ロジック変更をデプロイする
ときは、以下 2 層の cache がどちらも既存の per-race cache-bust endpoint
(`/api/internal/race-cache-bust`) の対象外であることに必ず注意する
(2026-07-18 の品質ゲート初回デプロイで実際にこれを踏み、本番の
pre-loaded レースが最大 6 時間 stale な形状を返し続けた):

1. `src/db/query-cache.ts` の `withDbQueryCache` — content-hash キー、
   既定 TTL は `PC_KEIBA_DB_CACHE_TTL_SECONDS`（本番 3600 秒 = 1 時間）。
2. `src/lib/finish-prediction-inputs-cache.server.ts` — レース発走時刻 +
   `PC_KEIBA_DETAIL_SECTION_CACHE_AFTER_START_SECONDS`（本番 21600 秒 =
   6 時間）まで保持。**こちらの方が影響が大きい** (6 時間 vs 1 時間)。

どちらも先頭の `const CACHE_NAMESPACE = "...:vN"` という文字列がキーの
一部に折り込まれているだけなので、**表示に影響するデプロイのたびに両方の
`vN` を 1 つ上げてコミットに含める**(`c7cf848f` / `7c5012f2` が実例)。
リテラル文字列に依存するテストが無いことは両ファイルとも確認済みなので、
安全に bump できる。逆に、返す値の形状を変えない (文言修正やスタイルのみの)
デプロイでは bump 不要。

## (d') 透明性+ユーザー選択への作り替え (2026-07-18、USER 指示)

(d) の「品質未達なら順位テーブルを非表示にする」は同日中に USER の
明示指示で撤回・作り替え。新方針:

1. **非表示にしない** — stddev が低くても順位テーブルは**デフォルトで
   表示**する。
2. **標準偏差の実数値+説明を常に表示** — `predictedScoreStddev`
   (queries.ts で confidenceTier と同じ箇所で計算済みの生値、レース
   単位で `FinishPositionModelPredictionFeature` → `FinishPredictionRow`
   まで confidenceTier と同じ経路で伝播) を
   「予測スコアのばらつき (標準偏差): 0.08」の形式で表示し、直下に
   標準偏差の意味の説明文（高い=モデルが実力差を明確に区別、低い=馬が
   横並びで信頼性が低い、的中率を保証する数値ではない旨）を常設で表示。
   閾値による表示・非表示の分岐はここには無い — 値が高くても低くても
   同じ形式で常に出る。
3. **ユーザーが選べる非表示トグル** — `predictedScoreStddev` が
   `FINISH_PREDICTION_LOW_RELIABILITY_MAX_STDDEV`（0.5、旧 (d) の
   閾値をそのまま流用）未満のときだけ、警告文 +「この予測を非表示にする」チェックボックスを追加表示。**既定は
   unchecked（表示したまま）** — USER の「非表示にはしない」という
   指示どおり、隠すかどうかは完全にユーザー任せ。チェックを入れると
   順位テーブルと補正トグル UI が「選択により、この予測の順位表を
   非表示にしています。上のチェックボックスを外すと再表示できます。」
   に差し替わる。チェック状態はレース単位の React state
   (`isPredictionHiddenByUser`) で保持し、レースが変わったら (realtime
   request の year/month/day/keibajoCode/raceNumber が変わったら)
   自動的に unchecked へリセットする — 別レースに前のレースの非表示
   選択を持ち越さない。

**確信度 badge (高/中/低) は変更なし** — 同じ `confidenceTier` から
引き続き表示、標準偏差の生値表示とは独立した別要素として共存。

**旧 (d) からの削除**: `isQualityGated` フィールド (queries.ts /
race-types.ts / finish-position-prediction.ts / table component 全て)
と、対応する env kill switch
`PC_KEIBA_FINISH_POSITION_QUALITY_GATE_DISABLED` は完全に削除
(自動非表示という概念自体が無くなったため、無効化スイッチも不要に
なった)。デプロイ時は上記「運用注意」どおり、両 cache namespace を
再度 bump 済み。

**検証**: `bun run --filter pc-keiba-viewer tsc` / `lint` / `format:check`
/ `test:coverage` 全て成功 (176 test files / 3957 tests pass、coverage
statements 99.36% / branches 97.36% / functions 99.14% / lines 99.39%、
閾値 95% を全指標で超過)。
