# JRA トラックバイアス交互作用 動的 subgroup 評価 (2026-06-19)

## 結論 (TL;DR)

**REJECT。draw × condition/distance 交互作用は、どの動的 subgroup でも frontier を動かさない。本番無変更を推奨。**

- グローバル: Δtop1 **−0.048pp**（CI −0.396..+0.318）、Δplace2 **−0.193pp**（CI −0.579..+0.193）。両指標とも CI が 0 を跨ぐ＝差なし。
- 全 subgroup（global 除く）で **bootstrap CI 下限 > 0 はゼロ件**。タスクの素の ADOPT 基準 `Δtop1>0 & n_rows≥500` は 23 件返すが、**1 件も統計的に robust でない**。
- n_rows≥500 の 57 subgroup で **正 23 / 負 29 / ゼロ 5** = ほぼcoin-flip。平均 Δtop1 **−0.157pp**、平均 Δplace2 **−0.440pp**（net 負）。
- enhanced ranker の gain 内訳: `odds_score` **96.8%**、交互作用4特徴 **合計 ~1.8%**（`inner_heavy_flag` は 0.0%）。GBDT は draw×条件の効果を既存特徴（`odds_score` + `track_bias_inside` + 既存 `umaban_*`/`field_*`/`course_*_track_*_rentai` 等）で**非線形に既に捕捉済**。
- [[project-relationship-perclass-investigation-2026-06-12]] の `partial ρ は必要だが十分でない`、[[feedback-incremental-gains-accept-gate]]（CI 下限 > 0 を伴わない点推定は reject floor）、[[project-perclass-campaign-complete-2026-06-17]] のフロンティア堅固に整合。

## 方法

- Feature store: `tmp/feat-jra-v8-iter18-class/race_year=2006..2025`（263 cols）。
- Walk-forward: **train ≤ 2022**（830,372 行 / 58,714 races）、**holdout 2023-2025**（141,523 行 / 10,365 races）。leakage なし。
- target: relevance = `clip(6 − finish_position, 0, 5)`（LightGBM lambdarank、group = race_id）。評価は per-race の予測 #1。
- **base**: `odds_score + track_bias_inside + umaban_norm`
- **enhanced**: base + 4 交互作用
  - `umaban_x_baba` = umaban_norm × current_baba_condition（1-4）
  - `umaban_x_dist` = umaban_norm × (kyori_band+1)（sprint..long）
  - `draw_advantage` = umaban_norm × track_bias_inside
  - `inner_heavy_flag` = (umaban ≤ 4 AND baba ≥ 3)
- surface 導出: track_code 10-22=turf / 23-29=dirt（JRA 平地、jump 50+ は other）。
- 指標: **top1** = 予測#1 が 1 着、**place2** = 予測#1 が 2 着以内。
- Bootstrap: race 単位リサンプル、n_boot=2000、seed=42、95% CI。
- DuckDB `memory_limit='4GB'; threads=4`、LightGBM `num_threads=4`。学習は base/enhanced を直列（heavy 同時1本ルール遵守、開始時メモリ free 65%）。
- スクリプト: `tmp/probes/trackbias_subgroup_eval.py` / 結果: `tmp/probes/trackbias_subgroup_eval.json`。

## 動的 subgroup（全 8 軸）

class（grade_code） / surface（turf,dirt） / baba（good=1, yielding=2, heavy=3-4） / venue（'01'..'10'） / surface×baba / class×surface / venue×surface / class×baba。

## 「ADOPT 候補」（素の基準 Δtop1>0 & n_rows≥500 — 上位、全 23 件中）

| scope/label               | n_rows | Δtop1 pp | top1 CI          | Δplace2 pp |
| ------------------------- | -----: | -------: | ---------------- | ---------: |
| venue_x_surface/02_turf   |  3,200 |   +1.176 | −1.176 .. +3.529 |     +0.000 |
| venue/02 (函館)           |  5,300 |   +1.157 | −0.463 .. +2.546 |     +0.000 |
| venue_x_surface/02_dirt   |  2,100 |   +1.130 | +0.000 .. +2.825 |     +0.000 |
| class_x_baba/E_heavy      |  3,024 |   +0.905 | −1.810 .. +3.620 |     +0.000 |
| class_x_surface/B_turf    |  1,564 |   +0.901 | −4.505 .. +6.306 |     +0.000 |
| venue_x_surface/10_turf   |  6,465 |   +0.891 | −0.445 .. +2.450 |     +0.445 |
| surface_x_baba/turf_heavy |  5,096 |   +0.800 | −1.867 .. +3.733 |     +0.267 |

注: 最大の n を持つ候補（surface/turf n=67k）でも Δtop1 +0.02pp（CI −0.518..+0.499）。venue 02（函館、小回り）クラスタが点推定上位だが、**CI 下限は全て ≤ 0**（02_dirt のみ +0.000 = 実質ゼロ）で、隣接 venue では正負バラバラ＝再現性なし＝ノイズ。

## ADOPT 判定

**ADOPT なし。** タスクの素の閾値（点推定 Δtop1>0 & n≥500）では 23 件該当するが、採否は probe 点推定ではなく incremental model 検証の **CI 下限 > 0** で確定すべき（[[feedback-incremental-gains-accept-gate]]）。本評価では:

1. グローバルが net 負（Δtop1 −0.048 / Δplace2 −0.193）。
2. robust 正（CI 下限 > 0）の subgroup = **0 件**。
3. 大 subgroup 全体で符号が coin-flip（正23/負29）、平均 net 負。
4. 交互作用の gain 寄与 ~1.8%（既存特徴と冗長）。

→ per-class/per-venue routing を正当化する信号量に達していない。

## 観察

- `track_bias_inside` の生レンジが 0..0.065 と極小で、それ自体の gain も 0.7% 止まり。PG raw の draw effect（外枠が芝で ~1.2 着分不利）は**既に odds に価格付けされ、かつ既存の course-level rentai 特徴に吸収済**。市場効率の壁（[[project-science-track-saturation-2026-06-11]]）と一致。
- `inner_heavy_flag`（内枠×重馬場）は gain 0.0%＝完全に死特徴。重馬場×内枠の優位は既存 `course_heavy_track_nige_rentai_rate_pct` 等で捕捉済。
- place2 は subgroup 平均で −0.44pp と top1 より悪化幅が大きく、交互作用は順位較正をむしろ僅かに乱す。

## 推奨

1. **本番モデル無変更。**
2. トラックバイアス系は per-class/per-venue でも frontier を動かさない（global partial ρ −0.011 の最弱が model 検証でも確認された）。
3. 将来再検討するなら、subgroup 集計ではなく **per-horse のコース実績 × 当日馬場の時系列**（倉庫外データ）か、当日の進行バイアス（[[project-science-track-saturation-2026-06-11]] のハロン時系列）が必要。本 store の静的 track_bias_inside の交互作用では不可能。
