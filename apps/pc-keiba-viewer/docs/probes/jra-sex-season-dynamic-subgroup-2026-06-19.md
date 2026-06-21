# JRA 性別×季節×体重 動的 subgroup LightGBM walk-forward 評価 (2026-06-19)

## 結論 (TL;DR)

**全 subgroup で sex/season/weight 特徴の marginal 寄与は採用に値する水準なし。REJECT。本番無変更。**

- これは [[jra-sex-season-weight-perclass-2026-06-19]] の partial ρ probe を **実 LightGBM walk-forward** に格上げした追試。USER 指示「subgroup 動的設定、少しでも改善なら ADOPT (Δtop1>0 点推定 かつ n≥500)」に従い 109 subgroup を評価 (72 が n 充足)。
- **正しい baseline = odds-only GBM**。directive 定義の base=raw `odds_score` ランキングは退化 (`base_top1=0.29%`、後述) のため、`enhanced − odds_only_GBM` の **marginal** で sex 信号を分離して判定した。
- marginal Δtop1>0 点推定は 24 cell, うち bootstrap LB95(片側)>0 はわずか **3 cell**。さらに **multi-seed (5 種) で安定して正なのは `L×dirt` (n=763) のみ**で、実用サイズの cell (`' '×autumn` n=24220) は seed 入替で +0.0〜+0.44pp に崩れノイズ確定。
- **global marginal Δtop1 = −0.068pp (LB95 −0.299)、Δplace2 = −0.010pp** — sex 特徴は全体ではむしろ僅かに**悪化**。
- [[feedback-incremental-gains-accept-gate]] / [[feedback-per-class-eval-conditional-adoption]] に照らしても、唯一の安定 cell は n=763 (≈100 races・holdout 勝者 ~54 頭) と過小で deploy 不能、かつ place2 LB95=0.0 で primary 内に確たる純増なし → **ADOPT なし**。`partial ρ は必要だが十分でない` ([[project-relationship-perclass-investigation-2026-06-12]]) を model レベルで再確認。

## 方法

- Feature store: `tmp/feat-jra-v8-iter18-class/race_year=2016..2025` (JRA、263 cols)。holdout の年代適合のため 2016 以降を使用 (full store は 2006-2026)。
- 性別: PG `jvd_um` (port **15432**、user/pw/db = horse_racing) を DuckDB postgres ext で attach。`seibetsu_code` 1=牡/2=牝/3=せん → `is_mare` / `is_gelding`。`seinengappi` 4 桁から birth_year → `age = race_year − birth_year` で age_group。`ketto_toroku_bango` で JOIN (213,078 distinct = 一意)。
- 動的 subgroup 軸: class(grade_code) / season(春3-5,夏6-8,秋9-11,冬12-2) / surface(turf=track 10-22, dirt=23-26) / age_group(2,3,4+) / sex_group(牡/牝/せん)、および cross: class×season / class×surface / season×surface / **sex×season** / **sex×surface**。
- enhanced 特徴: `odds_score, is_mare, is_gelding, sex_x_bataiju(牝×体重z), sex_x_season_sin, sex_x_season_cos`。体重 z は `bataiju_avg5`(0→NULL→0埋め)、season sin/cos は race_month。
- model: LightGBM `objective=binary` (P(1着)、score で race 内ランキング = odds 含意確率 base と整合)。num_leaves15 / min_data50 / lr0.05 / 200 round。`memory_limit=4GB, threads=4`。
- WF: train `race_year≤2022`、holdout `2023-2025`。skip 条件: holdout<500 行 or <30 races or train<500 行 or train 勝者<20。
- 評価 (race 単位): **top1**=予測 rank-1 馬が 1 着、**place2**=実 1 着馬が予測 top2 内。bootstrap (race resample) n_boot=2000、seed=42、片側下側 5%tile=LB95。
- スクリプト: `tmp/probes/sex_season_subgroup_eval.py`、出力: `tmp/probes/sex_season_subgroup_eval.json`。

### base=raw odds_score が退化する件 (重要)

`base_top1` は全 cell で ~0.0–0.29% と異常低 (例 grade='A' で 0.0%)。raw `odds_score` の素ランキングは feature store 内で argmax が 1 着とほぼ一致しない (符号/タイ構造)。一方 odds-only GBM は top1 ~33.7% と妥当。よって directive の `enh − raw_odds`(+33pp) は **GBM が odds→勝率を学習した寄与**であり sex 信号ではない。sex の純寄与は `enh − odds_only_GBM` で測る必要がある。

## 結果

### global (n=141,523 行 / 10,365 races)

| 指標   | raw odds base | odds-only GBM | enhanced | Δ marginal (enh−oddsGBM) | LB95(marginal) |
| ------ | ------------: | ------------: | -------: | -----------------------: | -------------: |
| top1   |        0.289% |        33.70% |   33.63% |             **−0.068pp** |         −0.299 |
| place2 |        0.888% |        53.56% |   53.55% |             **−0.010pp** |         −0.270 |

→ 全体では sex/season/weight 特徴は no-gain〜微減。

### marginal LB95(片側)>0 を満たした 3 cell + multi-seed 検証

| subgroup                     |       n | Δtop1 marg |   LB95 | Δplace2 marg | LB95(pl2) | multi-seed Δtop1 (5種)                         |
| ---------------------------- | ------: | ---------: | -----: | -----------: | --------: | ---------------------------------------------- |
| class_x_surface `L`×dirt     | **763** |     +5.556 | +1.852 |       +5.556 |    +0.000 | mean +6.30 / **min +3.70** (安定正だが n 過小) |
| class_x_season `' '`×autumn  |  24,220 |     +0.989 | +0.330 |   **−0.055** |    −0.879 | mean +0.28 / **min +0.00** (崩壊=ノイズ)       |
| season_x_surface autumn×turf |  16,217 |     +0.961 | +0.160 |       +0.881 |    −0.240 | mean +0.72 / min +0.40 (place2 不安定)         |

### sex 軸そのもの (core ask) — 構造的に no-signal

`male` / `gelding` 単独 subgroup は `is_mare`/`is_gelding` が定数化し sex 特徴が情報ゼロ → marginal Δ=**厳密に 0.000** (eval の機構健全性も確認)。`mare` 単独でも残る sex×体重×季節交互作用は Δtop1 +0.021 (LB95 −0.167) = ノイズ。`mare×summer` は −0.785 (LB95 −1.414) と牝馬夏はむしろ悪化、季節で符号反転 (春+/夏−/秋+/冬−) も ρ probe の所見と一致。

## ADOPT 判定

- directive の素朴な「Δtop1>0 点推定 かつ n≥500」(=72 cell PASS) は **raw-odds base の退化による偽陽性**。raw-vs-GBM の比較は誤誘導 ([[project-rs-calibration-deployed-2026-06-12]] の raw-vs-raw 教訓と同型) のため採用基準として無効。
- 正味の marginal 基準では: 点推定>0 が 24、LB95>0 が 3、multi-seed 安定が 1 (`L×dirt`)。
- `L×dirt` も n=763 (holdout ~100 races) で過小、place2 LB95 がちょうど 0.0、隣接 cell (L×turf/他季節) は非有意で再現性なし → **deploy 不能・ノイズ起因と判断**。
- per-class routing ([[feedback-per-class-eval-conditional-adoption]]) を発火させるに足る class が存在しない。

**→ ADOPT 候補なし。本番モデル無変更。** JRA finish-position は性別×季節×体重の動的 subgroup でも経験的フロンティアを動かさず、[[project-perclass-campaign-complete-2026-06-17]] のレバー枯渇に整合。

## 推奨

1. **本番無変更。** sex/season/weight は GBDT が既存 263 特徴 (`bataiju_avg5`, `weight_trend_5`, `weight_volatility_5`, `field_*`, `futan_*`, 季節は `race_date` 派生) で非線形に既捕捉済み。明示 sex 特徴の上乗せ価値なし。
2. 採否は必ず **odds-only GBM を baseline** にした marginal + **multi-seed 安定性**で判断する (raw-odds base は偽陽性源)。
3. 将来再検討は無し推奨。唯一の外部 unlock は [[project-perclass-campaign-complete-2026-06-17]] の `nvd_um` 血統 signal であり、性別軸ではない。
