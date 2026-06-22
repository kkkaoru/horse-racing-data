# JRA Probe — 体重変動 (weight-dynamics) 動的 subgroup 評価

**Date**: 2026-06-19
**Category**: JRA finish-position
**Verdict**: **REJECT (全 subgroup)** — apparent な subgroup gain は LightGBM の学習非決定性ノイズ。
deterministic 再学習で headline の `layoff +1.50pp` は **−0.87 〜 −1.62pp** に反転、global は
0 近傍 (−0.31 〜 +0.03pp) で run 間スイングが全ての点推定を上回る。`jra-weight-change-interaction-2026-06-19`
の ρ-gate REJECT を model-validation で追認・強化。

## Hypothesis / 指示

事前 probe (`jra-weight-change-interaction-2026-06-19`) は **global partial ρ で REJECT**
(best `layoff_change` +0.047 < 0.08 gate)。ただし raw signal は real な U-shape
(big_loss/big_gain ほど着順悪化)。USER 指示は「subgroup を動的に設定し、少しでも改善が
あれば ADOPT」。よって **global ρ-gate を越えて per-subgroup の incremental model 検証**まで
踏み込み、特定 class/休養明け/体重帯/季節で top1 が伸びる cell が存在するかを判定した
(memory `feedback_per_class_eval_conditional_adoption` / `feedback_incremental_gains_accept_gate`)。

## Method

- **Feature store**: `tmp/feat-jra-v8-iter18-class` (263 cols, **2006–2026**, JRA `source='jra'`)。
  v8 (179 cols, 2016–2025) より年数・特徴量とも広いため採用。
- **Weight-dynamics source**: PG `jvd_se`(JRA-only, port **15432**)。
  - `bataiju_kg` = 当日馬体重 (`bataiju`, `000`/blank→NULL)
  - `abs_zogen` = |前走比| (`zogen_sa`, `999`/blank→NULL)
  - `zogen_sa_signed` = `zogen_fugo` 符号付き (blank fugo は必ず `000`=確認済)
  - `big_change_flag` = |zogen_sa| > 8kg
  - `layoff_change` = abs_zogen × `is_returning_from_layoff`
  - join key: `(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, ketto_toroku_bango)`。
    finish は feature store の `finish_position` を権威 (scratched/DQ 除外済)。
- **Coverage**: `abs_zogen` non-null **90.6%**, `bataiju` non-null **100%**。
- **base vs enhanced**: 同一 LightGBM lambdarank (num_boost_round=300, leaves=63)。
  base = 既存 237 数値特徴 (odds_score / weight_diff_from_avg / is_returning_from_layoff 等を含む)。
  enhanced = base + `abs_zogen` + `layoff_change` + `big_change_flag` (240)。
  relevance = field_size − finish_position。
- **Walk-forward**: train ≤2022 (830,372 rows / 58,714 races)、holdout 2023–2025 (141,523 rows / 10,365 races)。
- **動的 subgroup 軸**: class(grade_code) / layoff / weight_class(light<440・mid440-500・heavy>500) /
  season + class×layoff / class×weight / layoff×season / layoff×weight。
- **判定**: per-subgroup の paired Δtop1 (predicted-winner が実際 1 着か) を bootstrap (n=2000, seed=42)。
  Δ>0 & n≥500 → ADOPT candidate、LB95>0 → STRONG。
- DuckDB `memory_limit='4GB', threads=4`。

## Results

### Global (holdout 2023–2025)

base top1 = **0.4051**、enhanced = **0.4086**、**Δ=+0.347pp** (95% CI **[−0.145, +0.878]**)。
点推定は正だが CI が 0 を跨ぐ。

### 単一 run の subgroup (誤誘導された初期結果)

初回 run では 73 subgroup 中 22 が「Δ>0 & n≥500」で WEAK_ADOPT に分類され、特に：

| axis            | subgroup               |    n |  base |   enh |       Δpp |      LB95 |  UB95 |
| --------------- | ---------------------- | ---: | ----: | ----: | --------: | --------: | ----: |
| layoff          | **layoff**             | 1199 | 0.407 | 0.422 | **+1.50** | **+0.00** | +3.00 |
| class_x_weight  | E \| heavy(>500)       |  597 | 0.397 | 0.410 |     +1.34 |     −0.84 | +3.52 |
| layoff_x_weight | layoff \| mid(440-500) |  783 | 0.401 | 0.414 |     +1.28 |     −0.64 | +3.19 |
| class_x_layoff  | (open) \| layoff       |  723 | 0.422 | 0.433 |     +1.11 |     −0.55 | +2.90 |

STRONG_ADOPT (LB95>0) は **0 件**。`layoff` のみ LB95 が 0 にちょうど触れた。

### 再現性チェック (決定打)

`layoff` cell を multi-seed / deterministic 再学習で検証 → **再現しない**：

| model seed | global Δpp | layoff Δpp (n) |
| ---------: | ---------: | -------------: |
|          1 |     −0.309 |  −0.872 (1261) |
|         42 |     +0.000 |  −1.617 (1237) |
|        123 |     +0.029 |  −1.063 (1223) |

- layoff cell は **3 seed すべてで負** (−0.87 〜 −1.62pp)。初回の +1.50pp は
  `feature_fraction`/`bagging_fraction` の確率性が n≈1200 の薄い slice で当たった artifact。
- 同 cell の bootstrap LB95 を 10 seed で取ると **全て負** (−3.31 〜 −3.23pp)、`seeds with LB95>0 = 0/10`。
- McNemar (1 run): enh-only-wins=44 / base-only-wins=64、two-sided **p=0.067** — むしろ base 寄り。
- global の run 間スイング (±0.35pp) と layoff の run 間スイング (±1.5pp) が、全 subgroup の
  点推定 (最大 +1.5pp) を**上回る**。signal ではなく分散。

## Interpretation

事前 probe の結論「U-shape は real だが `odds_score` に完全に priced」が、subgroup model
検証でも保持される。GBDT は既に odds + 5-race weight 偏差 (`weight_diff_from_avg`) +
`is_returning_from_layoff` で当日馬体重情報を非線形に捕捉済みで、`abs_zogen`/`layoff_change`/
`big_change_flag` を足しても **どの subgroup でも安定した増分はない**。

`feedback_per_class_eval_conditional_adoption` が想定する「global REJECT でも特定 class で routing
すれば効く」ケースには該当しない — ここでは per-class でも gain が seed 分散に埋もれており、
routing 先が存在しない。標準的な市場効率フロンティア
(`project_science_track_saturation_2026_06_11`, `project_relationship_perclass_investigation_2026_06_12`)
と整合。

教訓: 薄い subgroup (n≈1000) の単一 LightGBM run での Δtop1 ±1.5pp は容易にノイズ。
ADOPT 判定前に **multi-seed / deterministic 再現性チェック必須**。partial ρ は必要だが十分でなく、
incremental model 検証もさらに seed 安定性まで確認しないと誤陽性を出す。

## ADOPT 判定

**全 subgroup REJECT**。本番 (JRA iter20-2013 / E-top2) は無変更。weight-dynamics 特徴の
finish-position 追加学習は不要。

## Reproduce

- 評価: `apps/pc-keiba-viewer/tmp/probes/weight_dynamics_subgroup_eval.py`
  (`uv run python3 tmp/probes/weight_dynamics_subgroup_eval.py`)
- 結果 JSON: `apps/pc-keiba-viewer/tmp/probes/weight_dynamics_subgroup_eval.json`
- 再現性チェックは同 script の `train_ranker` を seed 1/42/123 + `deterministic=True` で再実行。
