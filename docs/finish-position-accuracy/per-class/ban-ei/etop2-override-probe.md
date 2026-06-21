# Ban-ei E-top2 override probe

**日付**: 2026-06-19
**verdict**: ABORT

---

## 仮説

JRA で有効だった E-top2 place-preserving override（XGB rank-1 == CB rank-2 の race のみ CB rank-1↔rank-2 を swap）が Ban-ei でも精度改善をもたらすか。

JRA では blind 2025 holdout で top1 +1.36pp [LB95 +0.58pp] / place2 +0.69pp [LB95 +0.06pp] / place3 ±0.00pp で ADOPT。

---

## 手順

### 使用モデル

- **base model**: `banei-cb-v7-lineage-wf-21y`（CatBoost YetiRank、NO cat-features）
  - 予測: `tmp/bucket-eval/finish-position/v7-lineage-wf-21y/predictions/category=ban-ei/race_year=*/`
- **probe XGBoost**: rank:pairwise、seed 3本平均（seed=42/123/456）

### 特徴量

- `tmp/feat-ban-ei-v7-lineage-21y/race_year=*/`（2007-2026、125特徴量）
- META_COLUMNS / LABEL_COLUMNS 除外後の numeric 列を全て使用

### 訓練・holdout 分割

- **train**: 2016-01-01 〜 2022-12-31（107,747行）
- **holdout**: 2023-01-01 〜 2025-12-31（49,382行 / 5,268 races）

### XGBoost パラメータ

| パラメータ       | 値                         |
| ---------------- | -------------------------- |
| objective        | rank:pairwise              |
| eval_metric      | ndcg@3                     |
| max_depth        | 8                          |
| learning_rate    | 0.05                       |
| n_estimators     | 300（early_stopping=30）   |
| min_child_weight | 30                         |
| reg_lambda       | 3.0                        |
| tree_method      | hist                       |
| seeds            | 42, 123, 456（スコア平均） |

### newcomer 除外

Ban-ei には `kyoso_joken_code` が存在せず、`is_newcomer_race` は全レースで 1（JRA 701 相当なし）。除外クラスなし。

---

## 結果

### override_fraction

| 項目                  | 値                               |
| --------------------- | -------------------------------- |
| 総 races              | 5,268                            |
| override 候補 races   | 775                              |
| **override_fraction** | **14.7%**（JRA の 13.1% と近似） |

### full holdout 評価（2023-2025、n=5,268 races）

| 指標   | base (CB) | E-top2 | delta        | LB95         |
| ------ | --------- | ------ | ------------ | ------------ |
| top1   | 34.34%    | 33.67% | **-0.664pp** | **-1.291pp** |
| place2 | 20.52%    | 20.84% | +0.323pp     | -0.209pp     |
| place3 | 15.36%    | 15.36% | ±0.000pp     | +0.000pp     |

### override subset 評価（n=775 races）

| 指標   | base (CB) | E-top2 | delta    |
| ------ | --------- | ------ | -------- |
| top1   | 27.48%    | 22.97% | -4.516pp |
| place2 | 18.58%    | 20.77% | +2.194pp |
| place3 | 15.74%    | 15.74% | ±0.000pp |

---

## Gate 判定

| ゲート条件                | 値              | 合否     |
| ------------------------- | --------------- | -------- |
| top1 LB95 > 0             | -1.291pp        | **FAIL** |
| place3 LB95 >= -0.05pp    | +0.000pp        | PASS     |
| top1 or place2 点推定 > 0 | place2 +0.323pp | PASS     |

**3条件中2条件のみ通過（top1 LB95 FAIL）→ ABORT**

---

## Verdict: ABORT

### 判断理由

1. **top1 が大幅に悪化**: override subset で top1 -4.516pp（27.5% → 23.0%）。Ban-ei では XGB rank-1 が CB rank-2 と一致する場面で XGB が CB より弱く、swap が裏目に出る。
2. **place2 の改善は弱い**: override subset で +2.194pp の改善があるが、全 holdout では +0.323pp のみ（LB95 -0.209pp で統計的に有意でない）。
3. **place3 は変化なし**: swap 設計上 rank-3 は不変なので当然。
4. **LB95 top1 が -1.291pp**: 統計的に top1 悪化が確定的。

### Ban-ei 飽和との関係

Ban-ei は既にフロンティア飽和済み（`project_finish_position_frontier_2026_06_11.md`）。E-top2 は JRA ではXGBoost が rank-1 に強いという特性（WF top1 34.4% vs CB）を活かした手法だが、Ban-ei でも XGBoost top1 精度は CB（34.3%）とほぼ同等。しかし E-top2 override subset での top1 -4.5pp は、XGB が CB の rank-2 予測馬を rank-1 に置く場面で実際には CB rank-1 が正解であるケースが多いことを示している。

JRA で有効だった理由（JRA CB rank-2 は「 intra-race XGB が評価し直したい競走馬」であることが多い）がBan-eiでは成立しない。Ban-ei は少頭数・特殊体重レースで CB YetiRank の rank 序列が JRA より安定しており、XGB の上書きはノイズになる。

---

## 備考

- スクリプト: `tmp/banei_etop2_probe.py`（git 管理外）
- 結果 JSON: `tmp/banei_etop2_probe_result.json`（git 管理外）
- XGBoost ndcg@3 holdout: 0.56571（early stopping iter 46）
- 3 seed で同一スコア（Ban-ei 特徴量 + rank:pairwise + seed 無関係な収束）
