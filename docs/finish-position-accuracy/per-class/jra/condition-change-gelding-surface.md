# JRA 条件変化特徴 probe: 去勢明け初戦 / 初ダ / 初芝

**作成日**: 2026-06-19
**対象期間**: 2020–2025（JRA・中央競馬 10 場）
**probe スクリプト**: `apps/pc-keiba-viewer/tmp/probe_condition_change_jra.py`
**結論**: 全 subgroup ABORT（gate-2 partial-ρ 0.08 未達）

---

## 仮説

馬の「初めての条件」は既存特徴で十分捕捉されていない可能性：

1. **去勢明け初戦（gelding_first）**: 騙馬になって初出走。`seibetsu_code` が非3→3 に変わった最初のレース。
2. **初芝（surface_debut_turf）**: ダート専用馬が初めて芝コースに出走。
3. **初ダート（surface_debut_dirt）**: 芝専用馬が初めてダートコースに出走。

---

## Gate-0: データ可用性 — PASS

| Subgroup           | 2020–2025 合計 n | 算出方法                                                                |
| ------------------ | ---------------- | ----------------------------------------------------------------------- |
| gelding_first      | 1,383            | `jvd_se.seibetsu_code` LAG で非3→3 の初レース                           |
| surface_debut_turf | 3,550            | `race_entry_corner_features.track_code` 過去走集計、今回芝 & 過去走全ダ |
| surface_debut_dirt | 12,719           | 今回ダ & 過去走全芝                                                     |

**判定: PASS**（各 n ≥ 200 threshold を大幅超過）

データ算出における leak-free 確認：

- 去勢フラグは `jvd_se.seibetsu_code` から算出（当日発走前に確定）
- 芝/ダート履歴は過去走の `track_code` のみ使用（対象レース結果は不使用）

---

## Gate-1: Subgroup 信号（人気 vs 着順乖離） — PASS

非 subgroup ベースラインに対する平均人気順（ninkijun_diff）と平均着順（fp_diff）の差：

| Subgroup           | n      | avg_ninkijun_sg   | avg_fp_sg         | ninkijun_diff | fp_diff |
| ------------------ | ------ | ----------------- | ----------------- | ------------- | ------- |
| gelding_first      | 1,369  | ベースライン+1.80 | ベースライン+1.78 | +1.804        | +1.777  |
| surface_debut_turf | 3,527  | ベースライン+3.49 | ベースライン+3.07 | +3.488        | +3.071  |
| surface_debut_dirt | 12,635 | ベースライン+0.22 | ベースライン+1.17 | +0.218        | +1.169  |

**判定: PASS**（|fp_diff| ≥ 0.3 を全 subgroup が超過）

### 解釈

- `surface_debut_turf` は人気差 +3.49 に対して着順差 +3.07 とほぼ比例 → 市場が既に織り込み済み
- `surface_debut_dirt` は人気差 +0.22 に対して着順差 +1.17 と差が大きい → 人気ほど悪くはないが着順は悪化する傾向
- `gelding_first` も人気差と着順差がほぼ一致 → 市場が概ね適切に評価

### Grade 別内訳（主要 grade のみ）

| grade_code     | gelding_first_n | gelding_fp_diff | turf_debut_n | turf_fp_diff | dirt_debut_n | dirt_fp_diff |
| -------------- | --------------- | --------------- | ------------ | ------------ | ------------ | ------------ |
| （無印/一般）  | 主体            | +1.5前後        | 主体         | +3.0前後     | 主体         | +1.1前後     |
| E（500万下等） | 小              | 類似            | 小           | 類似         | 小           | 類似         |
| L（L race）    | 少              | 変動大          | 少           | 変動大       | 少           | 変動大       |

---

## Gate-2: Partial-ρ（within-race demean、ninkijun 制御後） — **ABORT**

Within-race demean（レース内平均差引き）を施した上で `tansho_ninkijun` を partial out した条件変化フラグの residual と `finish_norm_dm` の相関：

| Subgroup           | n（全レース） | partial-ρ   | 判定（0.08 基準） |
| ------------------ | ------------- | ----------- | ----------------- |
| gelding_first      | 283,722       | **+0.0126** | ABORT（< 0.08）   |
| surface_debut_turf | 283,722       | **+0.0261** | ABORT（< 0.08）   |
| surface_debut_dirt | 283,722       | **+0.0514** | ABORT（< 0.08）   |

### Grade 別 partial-ρ

| grade_code | n       | rho_gelding | rho_turf | rho_dirt   |
| ---------- | ------- | ----------- | -------- | ---------- |
| （無印）   | 208,780 | 0.0139      | 0.0235   | 0.0559     |
| A          | 2,388   | 0.0066      | 0.0171   | 0.0034     |
| B          | 3,159   | 0.0261      | 0.0378   | 0.0020     |
| C          | 5,988   | -0.0066     | 0.0399   | 0.0354     |
| E          | 57,601  | 0.0102      | 0.0362   | 0.0443     |
| L          | 5,133   | 0.0001      | 0.0113   | **0.0724** |

最大値は grade_code=L の surface_debut_dirt で **0.0724** だが、0.08 基準に届かない。

**判定: ABORT**（max |partial-ρ| = 0.0724 < 0.08 threshold）

---

## Gate-3: Incremental 確認 — 未実施（gate-2 ABORT のため）

gate-2 で partial-ρ が 0.08 未達のため gate-3 には進まない。

教訓（per-class campaign `project_relationship_perclass_investigation_2026_06_12`）：

> partial-ρ は必要条件にすぎない。0.08 未満では GBDT が既存特徴（人気・過去走成績等）で非線形に既捕捉している可能性が高く、incremental model 検証に進む根拠として不十分。

---

## Per-class Verdict（JRA）

| Subgroup           | gate-0 | gate-1                | gate-2           | 最終判定  |
| ------------------ | ------ | --------------------- | ---------------- | --------- |
| gelding_first      | PASS   | PASS (fp_diff=+1.777) | ABORT (ρ=0.0126) | **ABORT** |
| surface_debut_turf | PASS   | PASS (fp_diff=+3.071) | ABORT (ρ=0.0261) | **ABORT** |
| surface_debut_dirt | PASS   | PASS (fp_diff=+1.169) | ABORT (ρ=0.0514) | **ABORT** |

**JRA 全体: 条件変化特徴は経験的フロンティア確認 — 採用なし**

---

## 考察

1. **初芝・初ダの着順悪化は市場が概ね織り込み済み**: gate-1 の人気差と着順差がほぼ比例しており、オッズに既に反映されている。
2. **初ダートの人気乖離（+0.22 vs fp_diff +1.17）**: 市場よりやや悪化するが、GBDT は `same_track_win_rate`・`speed_index_avg_5` など馬場別成績を既に特徴量として持ち、非線形に捕捉できている。
3. **去勢明け初戦**: サンプルが 1,383 と少なく、partial-ρ=0.0126 で最も弱い。気性改善効果は個体差が大きく平均では信号が薄れる。
4. **grade_code=L の surface_debut_dirt（ρ=0.0724）**: 最も有望だが 0.08 に届かず、L レースのサンプル数（n=5,133）を考慮すると統計的にも不安定。

---

## 結論

JRA における条件変化特徴（去勢明け初戦・初芝・初ダート）は、gate-0/1 を通過するが **gate-2 の partial-ρ 0.08 基準を全 subgroup で未達** → **全 ABORT**。

既存特徴（`same_track_win_rate`, `tansho_ninkijun`, `speed_index_avg_5` 等）が条件変化の情報を既に非線形に捕捉しており、新規フラグの追加による incremental な改善は見込めない。JRA の経験的フロンティアを改めて確認。
