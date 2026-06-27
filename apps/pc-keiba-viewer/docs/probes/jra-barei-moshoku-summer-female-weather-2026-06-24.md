# JRA 馬齢・毛色・夏×牝馬×天気 調査 (2026-06-24)

## 結論: REJECT(夏牝馬×天気交互作用) / 毛色=弱・REJECT候補 / 馬齢=要 store rebuild(本調査では未検証)

`tmp/eval_summer_female_weather.py` / `tmp/eval_summer_female_weather_result.json`

---

## 1. 毛色 (moshoku_code)

- PG `nvd_se` に存在。主要値: 03鹿毛(1.49M) / 01栗毛(0.78M) / 04黒鹿毛(0.54M) / 07芦毛(0.18M) / 05青鹿毛(0.17M) / 06青毛(0.10M) / 02栃栗毛(0.015M)。粕毛・駁毛・白毛は希少。
- **勝率 signal は弱い**: 全期間勝率は 9.97〜10.62% に密集(spread 0.65pp)。最高は栗毛 10.62% だが avg_popularity も 4.31 と人気上位 → **市場が既に織込み**。青毛は 10.38% で最も人気(avg_pop 3.90)。
- 毛色は per-horse 安定属性(121,369 頭中 5 頭のみ複数値=ノイズ)。
- **判定: 単独 signal 弱 + popularity と交絡。ADOPT 見込み薄。** store 追加は技術的には容易(下記)だが優先度低。

## 2. 馬齢 (barei)

- PG `nvd_se` / `race_entry_corner_features` に存在(varchar "02"-"19")。**フィーチャストア未追加。**
- **明確な年齢カーブ signal**: 勝率は 3-4歳 ~11.7% でピーク → 単調減少(7歳 8.1% → 11歳 5.6% → 13歳 3.9%)。avg_popularity も加齢で上昇 → 市場は部分的に織込み。
- store には `career_win_rate` / `career_top1_count` / `consecutive_race_count` / `experience_in_g1_race` / `recent_win_count_5` 等の career 系があり **加齢を部分 proxy** している。ただし「軽量出走の7歳 vs 多数出走の3歳」を分離する main effect としての barei は冗長ではない可能性。
- **判定: 唯一 store rebuild で再検証する価値あり**(本調査の v9 store には barei 列が無いため model 検証不能)。GBDT が career proxy で既に捕捉している恐れも大きく、[[project-season-sex-weight-probe-2026-06-20]] の教訓(raw main effect は既存特徴で冗長になりがち)を踏まえ過度な期待は禁物。

## 3. 夏 × 牝馬 × 天気 交互作用 — **REJECT**

### base-rate / residual は genuine

- 牝馬勝率: 春5.87% → **夏7.17%** → 秋6.36% → 冬5.66%(牡馬はほぼ flat 7.6-8.1%)。
- **人気band内の residual も positive**(市場は完全には織込んでいない): mare_minus_colt 勝率(pp)
  - fav(1-3): 夏 **+0.86** / 春 −1.35 / 秋 −2.09 / 冬 −3.47
  - mid(4-6): 夏 **+0.45** / 他季 全て負
  - long(7+): 夏 **+0.13** / 他季 全て負
  - → 同人気の牡馬を「夏だけ」上回る。signal は実在。

### model 検証は REJECT

- JRA v9 weather store / CatBoost YetiRank(production iter20 同設定 depth8/lr0.05/iter1000/od_wait30/seed20260519)。train 2013-2024、blind 2025、n_common=3,455 races。
- 3 arm: baseline(130feat, 天気なし) / v9_full(142, +天気12) / summer_female(146, +夏牝馬交互作用4)。
- 追加4列: `summer_x_female` / `summer_x_female_x_temperature` / `summer_x_female_x_precipitation` / `winter_x_male_x_cold`。

**Global (pooled) summer_female vs v9_full** — 全 rank で LB95<0:

| rank | Δ(sf−v9) pp | LB95 pp |
| ---- | ----------- | ------- |
| 1    | +0.029      | −0.492  |
| 2    | +0.463      | −0.289  |
| 3    | −0.521      | −1.216  |
| 4    | +0.318      | −0.376  |
| 5    | −0.492      | −1.158  |
| 6    | +0.145      | −0.521  |

- **primary {top1,place2,place3} で LB95>0 のものは無し**、place3 は点推定も −0.52pp 悪化。[[feedback-incremental-gains-accept-gate]] の ADOPT gate を満たさず。
- 天気自体(v9_full vs baseline)も top1 Δ0.000 / place2 +0.232(LB95 −0.492)で robust gain 無し。
- per-class×season cell: 夏 cell(COND:summer n=684 / E:summer n=216)は全 rank で LB95 が 0 を跨ぐ。唯一の adopt candidate は **COND:autumn rank6**(LB95 +0.168)=仮説と無関係・~120 cell×rank 検定中 1 件の多重比較 artifact。

### Why REJECT

[[project-season-sex-weight-probe-2026-06-20]] が raw GBDT ablation と serve-time swap で既に REJECT 済の夏牝馬 signal を、**天気条件付き交互作用**という未検証アングルで再試行したもの。base-rate/residual は genuine だが、GBDT は `seibetsu_code` + `season_band` + `venue_temperature` 等の raw 列から既に非線形に捕捉しており、precomputed 交互作用は冗長で place3 に noise を注入する。市場 under-pick は base-rate 現象であって serve 時の comparative ranking error ではない、という前回結論を model レベルで再確認。

**夏×牝馬×天気 は DO-NOT-RETEST。**

---

## DuckDB builder への barei/moshoku_code 追加方針

`src/scripts/finish_position_features_duckdb.py`、3 つの rec-select site:

- `_rec_select_from_corner_features` (JRA/NAR 履歴): `barei` 有 / **`moshoku_code` 無**
- `_rec_select_from_ban_ei` (ban-ei): 3 列とも `pg.nvd_se` から取得可
- `_rec_select_from_se_ra` (upcoming): 3 列とも `pg.nvd_se` から取得可

**barei**: 各 site で `try_cast(nullif(trim(barei), '') as int) as barei` を `seibetsu_code` の隣に追加 → `target` CTE → `base_features_select_sql` を素通しで `t.barei` 出力(既存 `seibetsu_code` と同パターン)。

**moshoku_code**: per-horse 安定属性なので `nvd_um`/`nvd_nu`(馬マスタ、両方に存在)へ `ketto_toroku_bango` join が最もクリーン(corner-features の欠落を回避、pedigree join と同様)。char コードなので `CATEGORICAL_FEATURE_NAMES` に追加して categorical 扱いが妥当。

いずれも本調査では **コード変更せず**(builder は触らない指示)。barei のみ store rebuild + model 検証する価値あり。毛色は signal 弱く優先度低。
