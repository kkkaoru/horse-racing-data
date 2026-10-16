# 単勝オッズ禁止・全本番モデル再学習キャンペーン (2026-10-16〜)

## ユーザー指示 (確定)

- 着順予測が人気順をなぞる問題を解決する。単勝オッズの値・人気順の利用は全モデルで禁止。
- 利用可能なオッズは 馬連・3連複 のみ、かつ「人気とのずれ」の形でだけ追加する
  (生の implied probability / 順位は禁止)。馬単・3連単・枠連・ワイド・複勝も使わない。
- 本番で利用する全てのモデルを、モデルごとに上記ルールで再学習し、本番の全モデルを更新する。
- 単勝オッズを使わない単一モデルを全レースへ画一的に適用することは禁止 (セル別構成を維持)。
- ローカル PG (127.0.0.1:15432) は必要に応じて自分で起動する。

## 根拠となった計測

- 本番 (2026, 6,469 races): 予測上位3頭=人気上位3頭 55.3%、1-2-3番人気順そのまま 31.9%。
- 券種別 implied ranking vs 単勝人気 (JRA 2023-2025, 9,996 races, 確定オッズ):
  馬単 ρ0.986 / 3連単 0.985 / 馬連 0.983 / 3連複 0.978 / 枠連 0.617。
  単勝に対する追加情報 (3着内 対数尤度/race): 馬連 0.048, 3連複 0.044, 馬単 0.026, 3連単 0.023。
  → 生の値は人気の再符号化。ずれ (curve residual 等) の形でのみ採用。

## 新特徴量 (単勝不使用)

学習: `apps/pc-keiba-viewer/src/scripts/finish-position-features/add_exotic_market_deviation_features.py`
配信: `apps/finish-position-predict-container/src/predict_lib/market_deviation.py`
(採点直前に hot worker の最新 `umaren` / `3renpuku` から計算。数式と基準表は共通)
基準表: `apps/pc-keiba-viewer/finish-position/lookups/exotic-market-deviation-baseline.json`

- `exotic_umaren_p2_rank_residual` / `exotic_sanrenpuku_p3_rank_residual`:
  log(share·N) − E[log(share·N) | N, 市場順位] (E は 2013-2024 確定オッズのカテゴリ別平均)。
- 2025 年での within-race Spearman (人気 / 着順): JRA 馬連 −0.08 / −0.16、3連複 −0.02 / −0.12、
  NAR −0.09 / −0.17、+0.01 / −0.12、ばんえい +0.01 / −0.11、+0.09 / −0.06。
- 不採用: in-race 曲線残差 (人気と −0.31)、log p3 − log p2 (人気と +0.86 / 順位条件付けでも +0.30)、
  順位差 (着順と無相関)。

## 対象モデル (本番 selector closure, 2026-10-16 監査)

JRA 55 / NAR 14 / ばんえい 2。禁止列ヒットの無いもの: JRA stage1-marketfree 系 3、
high-payout-specialist235、NAR stage1-marketfree-184、NAR consensus pedigree/physiology。
監査スクリプト結果: scratchpad `selected_audit.json` (再生成は ban 規則 + artifact_integrity
`derive_selected_artifact_keys`)。

## 特徴量ストア再構築

`.cache/tansho-ban-retrain/build_features.py` が本番と同じ
`pipeline_args.LAYER_CHAIN` / `build_layer_argv` を履歴期間で実行
(prophet entity trend は 2026 固定 lookup で本番着順モデル未使用のため除外)。
2025-06-01..15 の試運転で JRA 全モデルの特徴列が欠落 0。

## 進捗

- [x] 監査 / 試運転 / 新レイヤー + テスト
- [x] 学習ポリシー (viewer) + 配信ガード (container `assert_no_banned_market_features`) + 配信側特徴量付与
- [x] JRA 2013-2026 特徴量
- [x] JRA 再学習 (本番 selector closure 内の学習可能モデル全て、`-nowin1`)
- [ ] NAR / ばんえい 特徴量
- [ ] モデルごとの再学習 / 2026 blind 評価 (精度 + 人気一致率)
- [ ] viewer / cron / container 更新、隔離 worktree からデプロイ、デプロイ後検証

## 2026 blind 評価 (JRA, 2,610 races, `.cache/tansho-ban-retrain/evaluate.py`)

本番 `score_races` で旧 (単勝あり) / 新 (`-nowin1`) のセル構成を採点。両群とも Stage-1 fallback は無効
(有効だと offline ではほぼ全レースが stage1 単一モデルに落ち、セル別構成の比較にならないため)。

| 指標                               | 旧    | 新    |
| ---------------------------------- | ----- | ----- |
| 人気順との Spearman                | 0.986 | 0.856 |
| 予測1-3位 = 1-2-3番人気そのまま    | 67.1% | 13.1% |
| 予測上位3頭 = 人気上位3頭 (順不同) | 83.7% | 35.5% |
| 予測上位5頭 = 人気上位5頭 (順不同) | 78.3% | 27.8% |
| 予測1位 = 1番人気                  | 89.9% | 57.4% |
| 1着的中                            | 34.2% | 26.6% |
| 予測1位の3着内率                   | 66.8% | 58.2% |

旧の的中は確定単勝オッズ (発走前より情報量が多い) を入力とするため上振れを含む。

## 再学習時の判断

- 名前付きレースモデル (`jra-named-*`) の学習範囲は「対象レース出走馬の全出走 + 対象レース 3 倍重み」に統一。
  旧版の一部 (`ninki2` / `nonfav` 系) は単勝人気で学習行を選別・重み付けしていたため、その選別は再現しない。
- レース名照合は NFKC 正規化して行う (JV-Data の `kyosomei_hondai` は全角: `中京２歳ステークス`, `ＢＳＮ賞`)。
- メモリ制約: 特徴量ビルドは DuckDB 10GB 上限 × 最大 3 本 + 学習 1 本まで。

## 脚質モデル (running-style) の再学習

- 本番脚質モデル `{jra,nar}-running-style-lgbm-prod-v3` (R2 `running-style/models/<cat>/latest.flatbin`,
  146 特徴) は `popularity_score` / `odds_score` を入力に持つ。配信時はこの 2 列が NULL
  (脚質の配信ビルドに単勝の join が無い。`rs-jra-market-feature-serve-gap-2026-07-19.md`) のため
  配信中の `rs_p_*` に単勝情報は入っていないが、学習で単勝を使っているため規則に従い再学習する。
- 学習ポリシー: `feature_selection_policy.resolve_feature_columns_for_target` の禁止列除外を
  running_style にも適用。
- `.cache/tansho-ban-retrain/rs_retrain.py`: prod-v3 のヘッダ特徴から 2 列を除いた 144 特徴、
  〜2024 で学習 / 2025 で early stopping、2025 の flatbin 生確率で OvR isotonic 校正。
- 2026 blind (JRA 16,697 頭、配信と同じく popularity/odds/track_condition を NULL、
  本番 flatbin 評価器で両モデルを採点):

|                  | prod-v3 | prod-v3-nowin1 |
| ---------------- | ------- | -------------- |
| 生 argmax 正解率 | 41.3%   | 47.5%          |
| 校正後 正解率    | 45.6%   | 48.5%          |

- ダート小頭数 hybrid NN (`jra-dirt-small-005-hybrid-v1`) は学習コードがリポジトリに無いため、
  再学習ではなく `JRA_DIRT_HYBRID_ENABLED=0` で停止 (セルは再学習した prior-corner モデルで配信)。
- 単勝依存の dynamic-market シャドー評価は配信ガードにより読み込み停止 (配信順位には非影響)。

## 作業中に見つけて修正した既存バグ

1. **NAR Transformer 学習コードの推論時 dropout** (`finish_position_transformer/training.py`):
   MLX モジュールは学習モードのまま採点しており、予測が毎回揺れ、検証 NDCG / 早期終了もノイズ化。
   `predict_rank_scores` / `predict_all_logits` を推論モードで実行し、呼び出し元の学習フラグを復元。
   修正後、本番 numpy 採点器 (`transformer_scorer`) と float32 誤差 (≤0.0015) で一致。
2. **pytorch_mpx ONNX が出走頭数 8 固定** (`timesfm-finish-position/scripts/export_pytorch_mpx_onnx.py`):
   旧 tracer が注意機構の reshape に頭数 8 を焼き込み、8 頭以外の全レースで onnxruntime が失敗して
   補正が黙ってスキップされていた (検証も 8 頭のみ)。ONNX 初期値から重みを完全復元 (旧版と 1e-6 一致)、
   dynamo 書き出し + 単一ファイル化 + 1〜18 頭・部分マスクでの一致検証 (≤2e-6) に変更し再書き出し。
   サイドカー既定値が重み JSON を上書きして元重みを失う問題も解消 (`--source-json` 必須)。
3. **既存 exotic オッズデコーダの桁ずれ** (`add_exotic_odds_features.py`): 馬連・3連複のオッズを
   5 桁で読んでいた (JV-Data は 6 桁 + 人気 3 桁)。SQL / Python 両経路とテストフィクスチャを修正。
4. **trainer 層の一括入力時 OOM** (`add-trainer-stable-affinity-features.py`): 対象レースごとの OR 述語が
   年単位入力で DuckDB メモリを使い切る。200 レース超は日付範囲 (調教師集合の上位集合、出力不変) に切替。
5. **pandas index 混入**: 学習用 parquet 書き出しで `__index_level_0__` が特徴量化される問題をツール側で解消。

## NAR release (2026-10-17)

- 13 NAR finish-position models + the iter40 Set Transformer retrained as `-nowin1`
  (features 2006-2026, train through 2025). `MARKET_BAN_PENDING_CATEGORIES` is now empty
  and the NAR Stage-1 fallback is disabled, so every category serves per-cell models
  without win odds.
- Bug found while training: the feature store keeps `current_baba_condition` as a float
  (`1.0`), while the NAR routing rules match `"1"`. Every track-condition cell scope came out
  empty until `retrain.py` normalized it to an integer string.
- Blind 2026 comparison (10,037 races, same per-cell routing in both arms):

| arm             | top1   | top3 hit | top3 box | pred1 = favourite | Spearman vs popularity |
| --------------- | ------ | -------- | -------- | ----------------- | ---------------------- |
| old (win odds)  | 43.87% | 75.46%   | 14.88%   | 81.2%             | 0.925                  |
| new (`-nowin1`) | 40.94% | 72.72%   | 13.49%   | 68.9%             | 0.865                  |

- One-day stop-gap on 10/17: from the morning deploy until the NAR release,
  `stage1_routing.json` used `force_market_free` to send every NAR race to the market-free
  Stage-1 model. This was a deliberate exception to the per-cell rule, because every NAR cell
  model still read win odds at the time. The flag stays supported (with tests) but is unset.
- `finish_position_active_models` now points at the `-nowin1` defaults for jra / ban-ei / nar.
  The viewer allow-list keeps the superseded versions so past races' predictions still show.
