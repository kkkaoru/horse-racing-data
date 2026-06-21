# JRA 脚質×競馬場×距離×芝ダ 動的 subgroup 評価 (2026-06-19)

**結論: REJECT（本番無変更）。** 42 subgroup・holdout 137,500 行・proper walk-forward LightGBM
検証で、**robust（bootstrap LB95>0）な Δtop1 改善 subgroup は 0/42**。脚質×競馬場 interaction
は iter26 store の既存特徴に対して **冗長**。

## 方法

- store: `tmp/feat-jra-v8-iter26-relationships`（JRA full 21y, 272 列）
- `predicted_rs` := argmax(`past_{nige,senkou,sashi,oikomi}_rate_self`)。**rs*p*\* は 2024-25 のみ
  backfill（2006-23 は 0% non-null）**ゆえ train≤2022 の WF では使用不可。historical self-rate が
  唯一 serve-valid な脚質 proxy（全年 ~60% non-null、NULL=初出走馬）。
- 新規 2 特徴（store 列のみ、**LOYO** で holdout 年の outcome 漏洩を排除）:
  - `rs_nige_x_venue` = `past_nige_rate_self` × venue別 nige-argmax 馬の advantage（mean(1−finish_norm)−global）
  - `rs_venue_affinity` = Σ*style `past*{style}\_rate_self` × cell(keibajo,surface,dist_band,style) advantage
- base=LGBM[`odds_score`] / enhanced=LGBM[`odds_score`,新2特徴]、WF train≤2022 / holdout 2023-25 pooled
- metric=exact top1（pred_rank==1 & finish==1）/ exact place2（`aggregate_bucket_eval_duckdb.py:341-350` と同義）
- bootstrap=**race-cluster** resample, n_boot=2000, seed=42

## 結果

**overall（n_races=9,991）: Δtop1 = −0.0018 [−0.0053,+0.0015]、Δplace2 = −0.0028 [−0.0074,+0.0018]**（両者 negative 点推定・CI が 0 跨ぎ）。

タスクの literal gate（Δtop1>0 点推定 ∧ n≥500）では 11 subgroup が候補化するが、**全て point-only**で
[[feedback-incremental-gains-accept-gate]]（LB95≥0 ∧ place 回帰なし）を満たさない:

| subgroup            |   n | Δtop1 [LB95,UB95]             | Δplace2 [LB95,UB95]       |
| ------------------- | --: | ----------------------------- | ------------------------- |
| predicted_rs=oikomi | 612 | +0.0098 [**+0.0000**,+0.0196] | +0.0082 [−0.0082,+0.0245] |
| senkou×mile         | 744 | +0.0067 [−0.0013,+0.0161]     | +0.0161 [+0.0000,+0.0323] |
| sashi×intermediate  | 944 | +0.0064 [−0.0021,+0.0159]     | −0.0064 [−0.0222,+0.0095] |
| keibajo=10(小倉)    | 714 | +0.0070 [−0.0056,+0.0182]     | −0.0098 [−0.0280,+0.0070] |

- **robust(LB95>0) top1 subgroup = 0/42**、robust place2 = 0/42。
- point-positive は **11/42 = 26%**（true-zero なら ~50% 期待 → むしろ下回る = 微 net-negative）。
- robust **negative** subgroup は存在: `keibajo=05(東京)×predicted_rs=空`（初出走）Δtop1 −0.0139 [−0.0264,**−0.0014**]。

## なぜ効かないか（核心）

iter26 store は既に venue×style を非線形に捕捉する特徴を保有: `rs_p_nige_x_field_pace`,
`same_keibajo_win_rate`/`same_keibajo_place2_rate`, `jockey_keibajo_win_rate`,
`course_good_track_nige_rentai_rate_pct`, `self_nige_rate_minus_field_avg`, `field_nige_pressure` 等。
GBDT がこれらの split で「小回り×逃げ有利」等の競馬知識を既に学習済 → 手製 interaction は増分ゼロ/冗長。
[[project-relationship-perclass-investigation-2026-06-12]] の核心教訓（partial ρ は必要十分でない、
GBDT は既存特徴で非線形に既捕捉）を、今回は **proper model + race-cluster bootstrap** で再確証。

## 残課題（このアプローチでは到達不能）

真の脚質 signal 強化は v3 脚質モデル自体の改良（別PJ、[[project-rs-calibration-deployed-2026-06-12]]）か、
store の `rs_p_*` full-history backfill（[[project-relationship-perclass-investigation-2026-06-12]] で
ABORT 済 = serve-valid だが pooled sub-gate）が必要。本 interaction lever は枯渇。

成果物: `tmp/probes/rs_venue_subgroup_eval.json`、`tmp/v8/rs_venue_subgroup_eval.py`
