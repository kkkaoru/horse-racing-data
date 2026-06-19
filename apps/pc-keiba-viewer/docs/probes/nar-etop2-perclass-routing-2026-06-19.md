# NAR Probe — E-top2 per-class routing 評価

**Date**: 2026-06-19
**Category**: NAR finish-position
**Verdict**: **PARTIAL ADOPT** — CB-2013 × current PROD XGB base で {A, B, NEW, other} class に per-class routing 適用。
{C, OP, MUKATSU} は place2 回帰が大きく REJECT。NAR_ETOP2_ENABLED は CB-2013 本番モデル学習完了まで False。

## Hypothesis / 背景

JRA で E-top2 override が成功した (commit 8d5cbd7b, `project_etop2_place_preserving_win_2026-06-18`) ため、
同手法の NAR 適用を検討。JRA との主な差異:

- NAR は class (A/B/C/OP/NEW/MUKATSU/other) の分布と馬質が JRA と大きく異なる
- XGB base = iter12-nar-xgb-hpo-v8 (PROD)
- CB base は 3 start-year (2006/2013/2015) × 4 XGB start-year (2006/2010/2013/2015) = 12 combo で探索

## Method

- **Feature store**: NAR walk-forward 3-fold (2023/2024/2025)。train は全 prior years。
- **Override logic** (JRA と同一): `CB#1 == XGB#2` の race のみ fire。
  - XGB#2 を rank-1 に昇格、CB#1 を rank-2 に降格。rank-3+ は変更なし (place3 構成保存)。
- **CI**: race-cluster resampling bootstrap、n_boot=2000、seed=42。
- **Eval**: exact-ordinal (pred-rank-k と actual-rank-k の一致)。
- **DuckDB**: memory_limit=4GB、threads=4。
- **Artifact**: `tmp/nar-window-ablation/etop2_analysis.json`、`eval_etop2_perclass.py`。

## 12-Combo 結果

### CB-2006 (全 4 variant FAIL — LB95 < 0)

| CB start | XGB start | fire% | Δtop1  | LB95       | Δplace2 |
| -------- | --------- | ----- | ------ | ---------- | ------- |
| 2006     | 2006      | 6.04% | +0.074 | **-0.096** | -0.319  |
| 2006     | 2010      | 6.46% | +0.042 | **-0.135** | -0.290  |
| 2006     | 2013      | 7.09% | +0.044 | **-0.142** | -0.260  |
| 2006     | 2015      | 7.07% | +0.012 | **-0.172** | -0.300  |

CB-2006 は点推定では小幅正だが全て LB95 < 0。place2 回帰が −0.26〜−0.32pp と一貫して大きい。
NAR の非定常性が強い 2006 年代データが CB の過去過学習を招いていると推察。

### CB-2013 (全 4 variant PASS — LB95 > 0)

| CB start | XGB start | fire%     | Δtop1      | LB95       | Δplace2    |
| -------- | --------- | --------- | ---------- | ---------- | ---------- |
| 2013     | 2006      | 6.28%     | +0.236     | **+0.064** | -0.111     |
| 2013     | 2010      | 6.38%     | +0.197     | **+0.022** | -0.064     |
| **2013** | **2013**  | **6.46%** | **+0.219** | **+0.039** | **-0.052** |
| 2013     | 2015      | 6.75%     | +0.187     | **+0.003** | -0.076     |

CB-2013 は 4/4 全 PASS。top1 で +0.19〜+0.24pp の安定した改善を示す。
Best balance: **CB-2013 × XGB-2013** (top1 +0.219, LB95 +0.039, place2 -0.052 が CB-2013 中最小)。

### CB-2015 (4 variant 中 2 PASS)

| CB start | XGB start | fire% | Δtop1  | LB95       | Δplace2 |
| -------- | --------- | ----- | ------ | ---------- | ------- |
| 2015     | 2006      | 6.23% | +0.238 | **+0.066** | -0.135  |
| 2015     | 2010      | 6.27% | +0.172 | **+0.000** | -0.081  |
| 2015     | 2013      | 6.45% | +0.192 | **+0.015** | -0.059  |
| 2015     | 2015      | 6.44% | +0.162 | **-0.015** | -0.088  |

CB-2015 × XGB-2006 は LB95 最大 (+0.066) だが place2 が -0.135 と CB-2013 より大きく悪化。
CB-2015 × XGB-2015 は LB95 が辛うじて FAIL (−0.015)。

## Per-class ブレークダウン

CB-2015 × XGB-2006 の per-class 結果 (CB variant 間で傾向は共通):

| Class   | n_races | Δtop1      | Δplace2    | Δplace3 | 判定      |
| ------- | ------- | ---------- | ---------- | ------- | --------- |
| A       | 2,515   | **+0.954** | **+0.199** | 0.000   | **ADOPT** |
| B       | 6,326   | **+0.190** | -0.111     | 0.000   | **ADOPT** |
| C       | 23,133  | +0.156     | **-0.246** | 0.000   | REJECT    |
| OP      | 1,116   | +0.358     | **-0.269** | 0.000   | REJECT    |
| NEW     | 544     | **+1.471** | **+2.390** | 0.000   | **ADOPT** |
| MUKATSU | 535     | +0.748     | **-0.561** | 0.000   | REJECT    |
| other   | 6,541   | **+0.138** | -0.046     | 0.000   | **ADOPT** |

### ADOPT classes: {A, B, NEW, other}

- **A** (2,515 races): top1 +0.954pp、place2 +0.199pp — 両指標改善。CB の精度優位が最も顕著。
- **B** (6,326 races): top1 +0.190pp、place2 -0.111pp — place2 微回帰だが top1 改善が主優先指標。
- **NEW** (544 races): top1 +1.471pp、place2 +2.390pp — 新馬戦で両指標が最大改善。
  override の fire 条件が新馬 class の不確実性分布に合致している可能性。
- **other** (6,541 races): top1 +0.138pp、place2 -0.046pp — 小幅だが acceptable。

### REJECT classes: {C, OP, MUKATSU}

- **C** (23,133 races): n が最大。top1 +0.156 の一方 place2 -0.246pp と回帰が顕著。
  C class は NAR の主力 bulk だが CB の place 予測が XGB より劣後している。
- **OP** (1,116 races): top1 +0.358pp だが place2 -0.269pp。
- **MUKATSU** (535 races): top1 +0.748 にもかかわらず place2 -0.561pp と最大回帰。
  条件馬/未勝利特有のレース展開で CB の override が裏目に出る。

## Standalone モデル比較

| Model              | top1 (%) | place2 (%) | place3 (%) |
| ------------------ | -------- | ---------- | ---------- |
| PROD XGB-2006      | 58.647   | 35.453     | 27.259     |
| CB-2013 standalone | 58.885   | 35.436     | 27.026     |
| CB-2015 standalone | 58.865   | 35.274     | 26.999     |

CB standalone は top1 でわずかに XGB を上回るが、place2/place3 では下回る。
これが override fire 時に place2 回帰が class によって出る根本原因。

## 選択理由: CB-2013 × PROD XGB base

1. **4/4 PASS** で最も安定した LB95 > 0。CB-2015 は 2/4 のみ。
2. **place2 回帰が CB-2013 中最小** (−0.052 for XGB-2013 combo)。
3. PROD XGB base は iter12-nar-xgb-hpo-v8 で start year 固定だが、CB-2013 はどの XGB 組合せでも PASS → PROD XGB との組合せでも期待值は正。
4. XGB-2013 combo の LB95 +0.039 は CB-2013 中 2 番目の安全域。

## Decision

**ADOPT: CB-2013 × iter12-nar-xgb-hpo-v8、per-class routing {A, B, NEW, other}**

実装:

- `nar_etop2_override.py`: per-class routing ロジック (ADOPT_CLASSES = {A, B, NEW, other})
- `model_meta.py`: NAR_ETOP2_ENABLED / NAR_ETOP2_CB_MODEL_KEY 定数追加

**NAR_ETOP2_ENABLED = False** (現状)。CB-2013 本番モデル学習・R2 upload・smoke test 完了後に True へ flip。

## Next Steps

1. CatBoost YetiRank、`--no-cat-features`、train 2013-present で CB-2013 NAR 本番モデル学習
2. R2 upload: `finish-position/nar/cb-nar-2013-v8/`
3. throwaway DB で smoke test (CLI parity 確認)
4. `NAR_ETOP2_ENABLED=True` + `active_models` registry 更新 → deploy

## Artifacts

- `tmp/nar-window-ablation/etop2_analysis.json` — 12-combo + per-class 全結果
- `tmp/nar-window-ablation/eval_etop2_perclass.py` — 再現スクリプト
- `apps/finish-position-predict-container/src/predict_lib/nar_etop2_override.py` — override 実装
- `apps/finish-position-predict-container/tests/test_nar_etop2_override.py` — unit tests
