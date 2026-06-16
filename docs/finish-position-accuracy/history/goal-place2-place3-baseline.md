# 2着/3着精度 Goal Baseline — 全指標定義・現状・GAP分析

**Date**: 2026-06-17  
**Purpose**: "JRA+NAR 2着精度と3着精度を両方40%超にする" ゴールの出発点を厳密に定義し、現行デプロイモデルでの実測値を記録する。指標定義が複数あるため全て計測し、40%目標の実現可能性を判定する。  
**Status**: DEFINITIVE BASELINE — コミット前の最新 production serving 実測

---

## 1. 測定対象・方法

### 対象期間・データ

- **ホールドアウト**: 2023-2026 (4年間)
- **JRA**: `jvd_se` の `kakutei_chakujun`（確定着順、数値かつ >0 のみ）
- **NAR**: `nvd_se` の `kakutei_chakujun`（同、Ban-ei = keibajo_code='83' を含む）

### 予測取得ロジック（本番サーバーと同一）

```sql
DISTINCT ON (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
ORDER BY prediction_generated_at DESC
```

馬ごとに最新の予測1件のみを使用（複数モデルが同一馬・同一レースに予測した場合、
最新タイムスタンプのモデルが勝つ）。

### デプロイ済みモデル

| カテゴリ | 支配的モデル                        | 馬ベース比率           |
| -------- | ----------------------------------- | ---------------------- |
| JRA      | `iter14-jra-cb-pacestyle-course-v8` | 69.8%                  |
| JRA      | `win5-xgb-v7-lineage-v1`            | 28.8% (Win5対象レース) |
| NAR      | `iter12-nar-xgb-hpo-v8`             | 93.5%                  |
| NAR      | `ban-ei-trans-lgbm-ensemble-v1.0`   | 6.4% (Ban-ei)          |

**注意**: JRA は iter14 と win5 モデルが混在。iter14 は 2026年6月生成 > win5 は 2026年5月生成のため、
両モデルが共存するレースでは iter14 が DISTINCT ON で選ばれる。
win5-only レースは iter14 が未予測のレース（6,469/18,410 レース = 35%）。

### iter14-only JRA 純粋精度

win5 混在を除いた iter14 単独レース (N=11,706) での測定は後述の「詳細分析」に記載。

---

## 2. 指標定義（8種類 + 隣接分析）

| 記号                   | 定義                               | 既存スクリプト対応                                |
| ---------------------- | ---------------------------------- | ------------------------------------------------- |
| `exact_top1`           | P(予測1位馬 == 実際の1着馬)        | `serve_accuracy_report.py` top1 と同一            |
| `exact_place2`         | P(予測2位馬 == 実際の2着馬)        | **新定義**                                        |
| `exact_place3`         | P(予測3位馬 == 実際の3着馬)        | **新定義**                                        |
| `actual2_in_pred_top2` | P(実際の2着馬 が 予測top2内)       | 累積/寛容                                         |
| `actual3_in_pred_top3` | P(実際の3着馬 が 予測top3内)       | 累積/寛容                                         |
| `pred2_in_actual_top3` | P(予測2位馬 が 実際top3内)         | 寛容                                              |
| `pred3_in_actual_top3` | P(予測3位馬 が 実際top3内)         | 寛容                                              |
| `fukusho_2p`           | P(予測top3のうち2頭以上が実際top3) | `serve_accuracy_report.py` fukusho_2p と同一      |
| `top3_box`             | P(予測1位馬 が 実際top3内)         | `serve_accuracy_report.py` place3/top3_box と同一 |

**既存 `serve_accuracy_report.py` の `place2`/`place3`**:

- `place2` = P(予測1位馬 が 実際top2) = `pred1_in_actual_top2` 相当（本テーブルにはなし）
- `place3` = P(予測1位馬 が 実際top3) = `top3_box` と同一
- これらは "2着精度・3着精度" の目的変数ではなく、トップ馬が圏内かどうかの指標

---

## 3. 全指標実測値（2023-2026 ホールドアウト、95% CI）

### 3a. JRA — 全モデル混在 (N=18,410 レース)

| 指標                 | 値         | 95% CI               | vs 40%       |
| -------------------- | ---------- | -------------------- | ------------ |
| exact_top1           | 50.68%     | [49.99%, 51.42%]     | +10.68pp     |
| **exact_place2**     | **27.71%** | **[27.06%, 28.35%]** | **-12.29pp** |
| **exact_place3**     | **20.44%** | **[19.83%, 20.99%]** | **-19.56pp** |
| actual2_in_pred_top2 | 46.85%     | [46.17%, 47.62%]     | +6.85pp      |
| actual3_in_pred_top3 | 47.21%     | [46.46%, 47.94%]     | +7.21pp      |
| pred2_in_actual_top3 | 65.10%     | [64.41%, 65.79%]     | +25.10pp     |
| pred3_in_actual_top3 | 49.93%     | [49.21%, 50.62%]     | +9.93pp      |
| fukusho_2p           | 75.32%     | [74.71%, 75.92%]     | +35.32pp     |
| top3_box             | 79.93%     | [79.34%, 80.52%]     | +39.93pp     |

**隣接分析（exact-miss のうち ±1 着順）**:

- place2 miss: 13,308 レース、うち隣接 6,882 (51.71%)
- place3 miss: 14,647 レース、うち隣接 5,995 (40.93%)

### 3b. JRA — iter14 純粋 (N=11,706 レース, win5 除外)

| 指標                 | 値         |
| -------------------- | ---------- |
| exact_top1           | 44.75%     |
| **exact_place2**     | **23.30%** |
| **exact_place3**     | **16.97%** |
| actual2_in_pred_top2 | 42.73%     |
| actual3_in_pred_top3 | 43.42%     |
| pred2_in_actual_top3 | 59.73%     |
| pred3_in_actual_top3 | 46.40%     |
| fukusho_2p           | 68.67%     |
| top3_box             | 74.85%     |

**注**: serve-condition-baseline-population.md の JRA FULL (N=11,703) = 44.71% top1 と一致する。
win5 モデルの混在が全体 top1 を 50.68% まで引き上げているが、
win5 モデルは Win5 対象レースのみに使われ母集団が異なる（的中率が高い傾向）。
**正確な iter14 実力は N=11,706 の値を参照すること**。

### 3c. NAR — 全モデル混在 (N=49,099 レース, Ban-ei 含む)

| 指標                 | 値         | 95% CI               | vs 40%       |
| -------------------- | ---------- | -------------------- | ------------ |
| exact_top1           | 56.95%     | [56.52%, 57.41%]     | +16.95pp     |
| **exact_place2**     | **34.16%** | **[33.74%, 34.57%]** | **-5.84pp**  |
| **exact_place3**     | **26.45%** | **[26.07%, 26.85%]** | **-13.55pp** |
| actual2_in_pred_top2 | 54.98%     | [54.53%, 55.41%]     | +14.98pp     |
| actual3_in_pred_top3 | 54.36%     | [53.91%, 54.80%]     | +14.36pp     |
| pred2_in_actual_top3 | 74.12%     | [73.70%, 74.51%]     | +34.12pp     |
| pred3_in_actual_top3 | 56.74%     | [56.30%, 57.15%]     | +16.74pp     |
| fukusho_2p           | 86.05%     | [85.74%, 86.35%]     | +46.05pp     |
| top3_box             | 87.37%     | [87.09%, 87.66%]     | +47.37pp     |

**隣接分析**:

- place2 miss: 32,328 レース、うち隣接 19,619 (60.69%)
- place3 miss: 36,110 レース、うち隣接 18,044 (49.97%)

### 3d. NAR — iter12 純粋 (N=45,631 レース, Ban-ei 除外)

| 指標                 | 値         |
| -------------------- | ---------- |
| exact_top1           | 58.62%     |
| **exact_place2**     | **35.22%** |
| **exact_place3**     | **27.29%** |
| actual2_in_pred_top2 | 56.14%     |
| actual3_in_pred_top3 | 55.15%     |
| pred2_in_actual_top3 | 75.51%     |
| pred3_in_actual_top3 | 57.57%     |
| fukusho_2p           | 87.87%     |
| top3_box             | 88.88%     |

---

## 4. 指標別 "40% 超" 状態

| 指標                 | JRA (全混在) | NAR (全混在) | 両方 >= 40%? |
| -------------------- | ------------ | ------------ | ------------ |
| exact_top1           | 50.68% ✓     | 56.95% ✓     | YES          |
| **exact_place2**     | **27.71% ✗** | **34.16% ✗** | **NO**       |
| **exact_place3**     | **20.44% ✗** | **26.45% ✗** | **NO**       |
| actual2_in_pred_top2 | 46.85% ✓     | 54.98% ✓     | YES          |
| actual3_in_pred_top3 | 47.21% ✓     | 54.36% ✓     | YES          |
| pred2_in_actual_top3 | 65.10% ✓     | 74.12% ✓     | YES          |
| pred3_in_actual_top3 | 49.93% ✓     | 56.74% ✓     | YES          |
| fukusho_2p           | 75.32% ✓     | 86.05% ✓     | YES          |
| top3_box             | 79.93% ✓     | 87.37% ✓     | YES          |

**既存 `serve_accuracy_report` の `place2`/`place3`（別定義）:**

- JRA FULL: place2=24.506%, place3=15.475% (serve-condition-baseline-population.md)  
  ここでの place2 = "予測1位馬が実際top2内" = `pred1_in_actual_top2` 相当
- NAR FULL: place2=42.378% (**既に40%超**)、place3=34.771%

---

## 5. Oracle 上限

| カテゴリ | ランダム基準 exact_place2    | ランダム基準 exact_place3 | 理論上限 |
| -------- | ---------------------------- | ------------------------- | -------- |
| JRA      | 8.58% (平均出走頭数 ~11.6頭) | 8.58%                     | 100%     |
| NAR      | 10.32% (平均出走頭数 ~9.7頭) | 10.32%                    | 100%     |

- 現行モデルはランダム基準を大幅に上回る（JRA: 27.71% vs 8.58%、NAR: 34.16% vs 10.32%）
- 隣接着順ミスが 50-60% に達する → 予測2位/3位馬は「概ね正しい」が、
  正確な着順は多くの場合ほぼ同実力の馬同士の拮抗で決まる

---

## 6. "2着精度・3着精度 40%" の達成難易度

### exact_place2 / exact_place3 の場合（最も厳格な解釈）

| 指標         | JRA gap  | JRA難易度 | NAR gap  | NAR難易度    |
| ------------ | -------- | --------- | -------- | ------------ |
| exact_place2 | -12.29pp | HARD      | -5.84pp  | 困難だが可能 |
| exact_place3 | -19.56pp | VERY HARD | -13.55pp | HARD         |

- **NAR exact_place2** は -5.84pp で最も到達可能性が高い
- **JRA exact_place2** は -12.29pp、**両カテゴリ同時達成は非常に困難**
- **exact_place3** は両カテゴリとも非常に困難

### 隣接ミス分析が示す構造的限界

exact_place2 のミスのうち 51-61% が「±1 着順」（実際には1着か3着になった）。
これは予測モデルが馬の相対的強さをほぼ正確に捉えているが、
最終的な確定着順は競馬のランダム性（スタート・コース取り・騎手判断）に依存することを示す。

### cumulative / lenient 定義の場合

- `actual2_in_pred_top2` (JRA 46.85%, NAR 54.98%) → 両方すでに40%超
- `actual3_in_pred_top3` (JRA 47.21%, NAR 54.36%) → 両方すでに40%超
- `pred2_in_actual_top3` / `fukusho_2p` / `top3_box` → 全て既に40%超

---

## 7. 推奨: 目標指標の明確化

「2着精度」「3着精度」には以下の解釈があり、難易度が大きく異なる:

| 解釈      | 定義                                        | JRA    | NAR    | 難易度         |
| --------- | ------------------------------------------- | ------ | ------ | -------------- |
| A (厳格)  | exact_place2: 予測2位馬==実際2着            | 27.71% | 34.16% | HARD/VERY HARD |
| B (累積)  | actual2_in_pred_top2: 実際2着馬が予測top2内 | 46.85% | 54.98% | **既に達成**   |
| C (寛容)  | pred2_in_actual_top3: 予測2位馬が実際top3内 | 65.10% | 74.12% | **既に達成**   |
| A' (厳格) | exact_place3: 予測3位馬==実際3着            | 20.44% | 26.45% | VERY HARD      |
| B' (累積) | actual3_in_pred_top3: 実際3着馬が予測top3内 | 47.21% | 54.36% | **既に達成**   |
| C' (寛容) | pred3_in_actual_top3: 予測3位馬が実際top3内 | 49.93% | 56.74% | **既に達成**   |

**重要**: ゴールが解釈 B/B' 以上の寛容定義であれば**すでに達成済み**。
解釈 A/A' (exact) の場合は JRA で -12pp/-20pp の大幅ガップが存在する。

---

## 8. 隣接ミス詳細

予測2位/3位馬が「外れた」際に隣接着順に入った割合:

|     | place2 miss中 隣接率 | place3 miss中 隣接率 |
| --- | -------------------- | -------------------- |
| JRA | 51.71%               | 40.93%               |
| NAR | 60.69%               | 49.97%               |

この高い隣接率は "ill-posed" の定量的確認:

- 予測2位馬は実際2位になれなかった場合、約半数が1位か3位に収まる
- つまりモデルは着順の大体の正解は出しているが exact 着順は揺らぎに支配される

---

## 9. 本ファイルの参照関係

- `serve-condition-baseline-population.md` — JRA/NAR FULL vs DEGRADED 比較（定義が異なる place2/3）
- `serve_accuracy_report.py` — 日次サーブ精度確認（place2 = pred1 in actual top2）
- `measure_place_baseline_v3.py` (tmp/) — 本ファイルの測定スクリプト
- `goal-plan-A-graded-fullsystem.md` — サブ4 graded learning 計画
