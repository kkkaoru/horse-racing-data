# jockey-pedigree269 serve 精度 0% 疑惑 — 緊急診断 (2026-07-17)

- **担当**: serve-defect-269 (専任診断 agent)、team-lead 指示
- **第一報**: 並行 agent (summer-baseline) が `jra-cb-v9-sim-2013-clean-jockey-pedigree269`
  (以下 269) の 2026 serve top1 が 0/29 races (0%) と報告。同一レース群のプレーン
  champion (`jra-cb-v9-sim-2013-clean`) は venue02 で 41.67% (WF 期待並み)。
- **並行監査**: `jra-serving-audit-jun-jul-2026-07-17.md` (同日、別 agent) が独立に
  ほぼ同じ signature (score 分散崩壊+近乱択精度) を発見済み。本doc §7 で相互参照。
  本 doc は根本原因特定 (同監査のスコープ外) を主目的とする。

## TL;DR

**269 は 269 固有のバグではない。** 269 とプレーン champion は、2026-07-12
05:51:45〜05:52:32 UTC (47秒間) に書き込まれた単一の write burst
(以下 Cluster B) の下で **同じ倍率で劣化**しており (269 top1=10.00% vs
champion top1=3.33%、269 の方がむしろ僅かに良い)、Cluster B の外側
(2026-07-07/07-08 の 269 単発バックフィル、2026-07-11 の champion Mac batch
fallback) ではどちらも健全 (score stddev 0.7〜1.4、top1 33〜43%)。cell_routing.json
の JRA ルート撤去 (当初想定された fallback mitigation) は根本原因を解決しない
— champion も同じ burst で同程度壊れるため。

Cluster B を生成したスクリプト/リクエストは、read-only 権限による静的解析
(git log、wrangler.jsonc の feature flag 履歴、コード読解) では**特定できな
かった** (並行監査も同じ結論)。特定できた/除外できたことは:

- `COORDINATOR_ENABLED` (mode=rescore の per-race coordinator) は
  2026-07-11 17:37 JST に無効化されており、Cluster B の時刻 (07-12 14:51
  JST) まで無効のまま — mode=rescore 経由ではない。
- `DAY_BASE_SPLIT_ENABLED` は `wrangler.jsonc` のどこにも設定されておらず
  (空 allowlist)、`is_day_base_split_enabled("jra")` は False — 07-12
  03:20 JST に merge されたばかりの day-base split 高速パス
  (`build_upcoming_feature_rows_split`) は JRA に対して dormant。同パス自体も
  コードレビューの結果、ANY gap で `None` を返し呼び出し元が全 LAYER_CHAIIN
  にフォールバックする設計 (fail-closed) であることを確認済み。
- coverage self-heal cron (07-12 04:26 JST 配線) は当日 (`runYmd=今日`) の
  レースしか対象にしないため、Cluster B に混在する 2026-07-11 付レースの
  説明にはならない。
- cell_router.py / ensemble_routing.py に variant 固有の rank 反転コードパスは
  存在しない — `_score_one_race_direct` は champion/variant 共通の1関数。

以上より、Cluster B は本番の genuine per-race Cloudflare Container パイプライン
ではなく、退化した (ほぼ空の) feature を伴うバッチ的な書き込みだったと強く推定
される (36レース23秒という所要時間は genuine per-race DuckDB build の27.5分
worst-case と整合しない) が、**発生源スクリプトの断定はできなかった**。

## 実施した修理 (2件、根本原因の断定なしに実施可能な防御的修正)

1. **`predict_lib/feature_guard.py` (新規)** — 特徴量の欠損率がレース平均で
   50% 以上のレースをスコアリングせず、そのレースの予測行を一切書き込まない
   (self-heal が後で再試行する)。Cluster B の観測 signature (score stddev が
   健全時の1/10以下に崩壊 = ほぼ全列 0-fill) を機械的に検知して遮断する
   fail-closed guard。`_score_one_race_direct` (champion+全 cell-routing
   variant 共通) と `_score_one_race_nar_blend` (NAR transformer blend) の
   両方に配線。**発生源が何であれ、同じ症状の再発を防ぐ。**
2. **cell-routing variant loader の feature-order 検証強化** —
   `_feature_set_hash` (`predict_upcoming.py`) は意図的に **order-independent**
   (ハッシュ前に sort する) — 列の**集合**が baked artifact と一致するかしか
   検証せず、**列の順序**が実際に学習された順序と一致するかは一切検証しない。
   一方 `ensemble_routing.py` の per-class ensemble member には同じ検証の
   厳格版 (`catboost_model_feature_names` + `member_feature_order_matches`
   — booster 自身が保持する `feature_names_` を metadata.json の順序と直接
   突合) が既に実装されているが、**cell_routing.json 経由の variant ロードには
   一切配線されていなかった**。今回 `score_races()` の variant ロードループに
   同じチェックを追加 — 順序不一致の variant は pool に載せず、該当レースは
   category default に自動フォールバックする (variant 未検出時の既存の
   フォールバック経路をそのまま再利用)。**これは今回の Cluster B の直接原因
   ではないと判断している** (§2 参照) が、269 のような cell-routing variant
   経路に実在した検証ギャップであり、将来の再学習時のサイレント破損を防ぐ。

## 1. 独立検証: 269 の 2026 serve 精度 (Neon read-only)

`race_finish_position_model_predictions` を read-only session
(`SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY`) で直接クエリ。
placeholder 罠ガード (`trim(kakutei_chakujun) not in ('','00')`) 適用。

### 1.1 model_version × 書込クラスタ (分単位) — venue 02/03/10, 2026

| gen_at (UTC, 分丸め) | model_version                       | rows | races |
| -------------------- | ----------------------------------- | ---- | ----- |
| 07-07 07:21          | jockey-pedigree269                  | 16   | 1     |
| 07-07 23:33          | jockey-pedigree269                  | 11   | 1     |
| 07-11 01:47          | jra-cb-v9-sim-2013-clean (champion) | 288  | 21    |
| 07-12 05:51          | prior-corner274                     | 8    | 1     |
| 07-12 05:51          | champion                            | 73   | 6     |
| 07-12 05:51          | jockey-pedigree269                  | 200  | 15    |
| 07-12 05:52          | prior-corner274                     | 9    | 1     |
| 07-12 05:52          | champion                            | 300  | 24    |
| 07-12 05:52          | jockey-pedigree269                  | 356  | 25    |

07-12 05:51:45〜05:52:32 UTC (Cluster B) は champion / 269 / 274 の**全モデル
に跨って**同時多発している。venue02 の全レースは 269 に、venue03/10 の
非703-joken レースは champion に、cell_routing.json の実ルールに**忠実に**
振り分けられている (=ルーティング自体は壊れていない。routing を迂回した
書込ではない)。

### 1.2 within-race score stddev — 269 全 42 レース

Cluster B (07-12 05:51-05:52 UTC) の 40 レース全てで stddev 0.0439〜0.1657。
Cluster B 外 (07-07/07-08 の 3 レース) は stddev 1.3173〜1.4359。champion も
同一パターン (Cluster A [07-11 Mac batch] 0.69〜1.51 / Cluster B 0.04〜0.16)。
`odds_score` / `tansho_odds` / `futan_juryo` / `weight_diff_from_avg`
の4監査列は全行 NULL — ただしこれは 2026-07-12 16:58 JST の
`persist serve-time audit values` commit (Cluster B より後) 以前の書込である
ため、非母集団的 (これらの列がまだ書き込まれていなかった時代の行) であり
診断的中立。

### 1.3 champion vs 269, 同一手法・同一クラスタでの直接比較

venue 02/03/10、2026、champion/269 各々を Cluster A (pre-07-12) /
Cluster B (07-12 05:51-05:52 UTC) で分離し、`aggregate_fp_metrics` と
同一定義 (predicted_rank=1 の実際順位から top1/place2/place3) で算出。

| model_version | cluster      | races | top1       | place2  | place3   |
| ------------- | ------------ | ----- | ---------- | ------- | -------- |
| champion      | A: pre-07-12 | 21    | **42.86%** | 52.38%  | 66.67%   |
| champion      | B: Cluster B | 30    | 3.33%      | 13.33%  | 20.00%   |
| 269           | A: pre-07-12 | 2     | 0.00%\*    | 0.00%\* | 50.00%\* |
| 269           | B: Cluster B | 40    | **10.00%** | 22.50%  | 35.00%   |

\* n=2、統計的に無意味 (施策判断には使えない)。

**269 は Cluster B 内で champion より劣化していない** (top1/place2/place3
いずれも 269 の方が高い)。両モデルとも Cluster A では健全、Cluster B では
著しく劣化 — **モデル間の相対差ではなく、書込クラスタが精度を支配している**。

### 1.4 優勝馬 predicted_rank 分布 (並行監査 §7.2 の独立再現、全 269 行)

n=43 (venue 制限なし、全 JRA venue の 269 行)、avg field size 13.93、優勝馬
predicted_rank 平均 6.23 (一様乱択期待値 7.47 より明確に良い)、下位半分入り
34.9% (一様乱択期待の 50% を下回る)。**一様分布でも末尾偏り (順位反転) でも
ない — 弱いが方向として正しいシグナルは残存**。並行監査の解釈 (順位反転より
signal attenuation = 入力の減衰/圧縮 が疑わしい) と一致。

### 1.5 チーム内の数値差異の整理

団長第一報 (0/29, 0%)、並行監査 ASC-dedup (n=22, 0.00%)、本検証の Cluster B
限定集計 (n=40, 10.00%) はいずれも**同じ現象の異なる標本**であり、n=22-29
の小標本では真の比率 ~5-12% でも 0 ヒットが十分あり得る (二項確率
0.90^22 ≈ 9.8%)。**「厳密に 0%」ではなく「一様乱択と統計的に区別できない
水準まで劣化」が正確な記述**であり、この訂正は結論 (269 固有ではない、
緊急に対処すべき) を変えない。

## 2. コードレベル root-cause 追跡 (仮説の順次検証)

団長指示の4仮説を検証:

- **(i) rank 割当の反転 (variant 分岐のみ)**: **棄却**。`_score_one_race_direct`
  (`predict_upcoming.py`) は champion・全 cell-routing variant で完全に
  同一の関数呼び出し (booster/feature_names/architecture/model_version の
  みが差し替わる)。`rank_race_entries` → `rank_within_race`
  (`predict_lib/rank.py`) は純粋関数で variant 分岐を一切持たない。variant
  専用の反転コードパス自体が存在しない。
- **(ii) metadata.json feature_names とマトリクス列順の不一致**:
  **重大な検証ギャップを発見・修正したが、Cluster B の直接原因である証拠は
  ない**。`_feature_set_hash` (order-independent) の弱点は上記「実施した
  修理」#2 参照。269/274 双方とも cell_routing.json に明示的
  `feature_names` 配列を持たず (`feature_set_hash` のみ)、この弱いチェック
  にのみ依存していた。しかし champion (cell-routing 非対象、この検証ギャップ
  の対象外) も 269 と全く同じ倍率で Cluster B 内で劣化しているため (§1.3)、
  **この検証ギャップだけでは champion の劣化を説明できず**、Cluster B の
  root cause としては採用しない — 独立した防御的修正として実施。
- **(iii) 19 jockey/pedigree 列の per-race SQL が壊れた値を返す**:
  **棄却**(269 単独の問題としては)。champion は jockey/pedigree269 の
  19列を一切持たないにもかかわらず Cluster B で同程度劣化しているため、
  この19列固有の欠陥では説明がつかない。ただし §1.2 の score stddev
  崩壊パターンから、**このバッチ全体で (19列に限らず) 大半の特徴量列が
  何らかの理由で欠損/0-fill されていた可能性が高い**という、より広い
  形の同系統仮説は棄却できない (この広い形が「実施した修理」#1 の対象)。
- **(iv) 別 artifact 取り違え**: **棄却**。§1.1 の通り、cell_routing.json の
  ルール (venue02→269, 703-joken→269, dirt×f_le10×005→274, 他→champion)
  が Cluster B 内でも忠実に守られている — 取り違えなら venue/joken と
  model_version の対応が崩れるはずだが崩れていない。

### 2.1 Cluster B 発生源の追跡 (特定に至らず)

同日 (2026-07-12) の関連 commit を時系列で確認 (git log):

- 03:20 JST `feat: split feature pipeline into per-day day-base + per-race
RACE_CHAIN` — `DAY_BASE_SPLIT_ENABLED` は wrangler.jsonc 未設定
  (空 allowlist) につき JRA には dormant (§TL;DR参照)。
- 03:35 JST `feat: add is_final_race cell-routing dimension`
- 04:26 JST `feat: wire the §4.3 coverage self-healing cron` — 当日
  (`runYmd=今日`) のみ対象、07-11 付レースは対象外 (§TL;DR)。
- **05:51-05:52 UTC (14:51-14:52 JST) — Cluster B 書込**
- 15:14 JST `fix: day-base prewarm's own contract violation`
- 16:12 JST `fix: populate R2 feat-cache for focused-full requests`
  (Cluster B の**約80分後**。focused-full が自分の R2 feature cache を
  書き込めていなかった欠陥の修正だが、これは mode=rescore の高速パスが
  読む側のキャッシュ欠如問題であり、`COORDINATOR_ENABLED=0` の間
  mode=rescore 自体は呼ばれていない — Cluster B の直接原因としては
  timeline 上は成立しない)。

これ以上の特定 (どのスクリプト/リクエストが Cluster B を書いたか) は
read-only 権限による静的解析の限界に達した。並行監査も同じ結論 (「生成
スクリプト自体を直接特定する証跡は本監査の read-only 権限では確認できて
いない」)。

## 3. 修理の詳細

### 3.1 `src/predict_lib/feature_guard.py` (新規、17テスト、カバレッジ100%)

`missing_feature_fraction` (1エントリの欠損率) →
`race_missing_feature_fraction` (レース平均、1頭だけ疎な新馬が誤検知
しないよう平均を採用) → `is_degenerate_feature_matrix`
(閾値0.5、実データの健全/劣化の分離マージンから較正: 健全レース最小
stddev 0.69 は閾値の遥か上、劣化レース最大 stddev 0.1657 は遥か下)。
`predict_upcoming.py` の `_score_one_race_direct` /
`_score_one_race_nar_blend` に配線 — 閾値超過レースは空リストを返し
UPSERT 自体をスキップする (self-heal が後で再試行、Neon には「未生成」
として現れる = 「自信満々な誤答」より安全)。

**残課題**: `_score_one_race_etop2` / `score_one_race_nar_etop2`
(E-top2 override 経路) には未配線 — 現在 `JRA_ETOP2_ENABLED=False` /
`NAR_ETOP2_ENABLED=False` で dormant のため優先度を下げたが、将来
再有効化する際は同じ guard を配線すべき。

### 3.2 cell-routing variant の feature-order 検証 (`predict_upcoming.py`)

`_variant_booster_feature_order_matches()` を新設し、`score_races()` の
variant ロードループで `_validate_variant_feature_contract` の直後に
呼び出し。`predict_lib.ensemble_routing.catboost_model_feature_names` /
`member_feature_order_matches` (per-class ensemble member 用に既存)
を再利用 — 新規ロジックの追加ではなく既存の検証済みヘルパーの適用範囲
拡大。6テスト追加 (直接ユニットテスト3件 + `score_races` 統合テスト2件
[不一致→フォールバック、一致→正常動作] + 既存 `VariantModel` テスト)。

## 4. テスト結果

```
apps/finish-position-predict-container:
  uv run ruff check         → All checks passed!
  uv run basedpyright        → 0 errors, 0 warnings, 0 notes
  uv run pytest               → 1288 passed, 1 skipped (pre-existing, unrelated)
  Total coverage: 99.81% (>=95% required)
    predict_lib/feature_guard.py: 100%
  oxlint . / oxfmt --check package.json pyproject.toml DEPLOY.md → clean
```

## 5. deploy 手順案 + rollback (実行は orchestrator へ委譲)

本 agent は Neon/R2/D1 への書込・wrangler deploy を一切実行していない
(read-only 診断 + local git commit のみ)。

1. **deploy 前チェックリスト (§6.3 4-point 相当)**: 本変更は
   `apps/finish-position-predict-container` のみ (Python)。viewer 側
   (`apps/pc-keiba-viewer/src/lib/finish-position-cell-routing.ts`) や
   `apps/finish-position-cron` には触れていないため、TS 側 parity test
   の追従は不要 — cell_routing.json 自体は変更していない (ルール撤去は
   実施しなかった。§TL;DR の通りルート撤去は根本原因を解決しないため)。
2. **deploy 対象**: container image の再ビルド + デプロイ (通常の
   `finish-position-predict-container` デプロイ手順、`DEPLOY.md` 参照)。
   Worker 側 (`finish-position-cron`) の変更はゼロ — Worker 再デプロイ
   不要。
3. **推奨タイミング**: 明日 (2026-07-18 土、次回 JRA 開催日) の朝
   09:25-09:30 JST cron (Neon pre-wake + feature build) より前。
4. **deploy 後の確認**: 07-18 の最初の genuine per-race 予測 (venue02
   函館 R1 目安) で (a) `race_finish_position_model_predictions` に行が
   書かれること (feature_guard が誤検知で全レースを握り潰していないか)、
   (b) within-race score stddev が健全水準 (目安 stddev ≳ 0.5) に戻って
   いること、を Neon read-only で確認。これは並行監査 §4 が既に推奨
   していた「07-18 に genuine live serving を観測する」ステップと合致する
   — 本修理はそのステップの安全網として機能する (もし07-18も再び
   degenerate な書込が発生した場合、feature_guard がそれを検出し
   Neon に書かせない)。
5. **rollback**: `git revert` 1コマンド (2ファイルの追加的変更のみ、
   既存ロジックの削除・置換なし)。feature_guard は「行を書かない」方向
   にのみ動作するため、rollback しなくても実害は「self-heal が余分に
   再試行する」程度に留まる (安全側)。

## 6. 残リスク

- Cluster B の発生源が未特定のため、**同じ発生源がまだ生きている場合**
  同種の劣化行が明日以降も生成され得る。feature_guard は「書かせない」
  ことはできるが「genuine な予測を生成させる」わけではないため、
  発生源が生きている限り該当レースは「未生成」のまま self-heal ループに
  入り続ける可能性がある。deploy 後の初回観測 (§5-4) が最重要。
- `_score_one_race_etop2` / `score_one_race_nar_etop2` は guard 未配線
  (現状 dormant なので実害なし、再有効化時は追従が必要)。
- serve_accuracy_report.py の dedup バグ (ORDER BY prediction_generated_at
  DESC が model_version 混在時に backfill 行を拾い得る) は時間の都合で
  今回未着手 — 別 agent/次セッションへの申し送り。
- Cluster B が書いた劣化行そのもの (Neon 上の既存データ) はそのまま残存
  している。本修理は将来の再発を防ぐのみで、既存の劣化行を訂正しない。
  viewer priority-0 機構によりこれらが引き続き優先表示される問題
  (並行監査 Defect B) は本 doc の対象外。
