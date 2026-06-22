# NAR 条件変化特徴 probe: 去勢明け初戦 / 初ダ / 初芝

**作成日**: 2026-06-19
**対象期間**: 2020–2025（NAR・地方競馬、Ban-ei 除外）
**probe スクリプト**: `apps/pc-keiba-viewer/tmp/probe_condition_change_nar.py`
**結論**: 全 subgroup ABORT（gate-0 サンプル不足 + gate-2 partial-ρ < 0.02）

---

## 仮説

馬の「初めての条件」が既存特徴で十分捕捉されていない可能性（JRA probe と同仮説）：

1. **去勢明け初戦（gelding_first）**
2. **初芝（surface_debut_turf）**
3. **初ダート（surface_debut_dirt）**

---

## Gate-0: データ可用性 — ABORT（サンプル不足）

| Subgroup           | 2020–2025 合計 n | threshold | 判定  |
| ------------------ | ---------------- | --------- | ----- |
| gelding_first      | 865              | 1,000     | ABORT |
| surface_debut_turf | 851              | 1,000     | ABORT |
| surface_debut_dirt | 270              | 1,000     | ABORT |

### NAR 全体データ確認

| 指標                                             | 値                         |
| ------------------------------------------------ | -------------------------- |
| nvd_se 総行数（2020-2025、Ban-ei除外、着順あり） | 809,402                    |
| seibetsu_code 被覆率                             | 100.0%                     |
| nvd_ra track_code 被覆率                         | 100.0%                     |
| nvd_um マッチ数 / ユニーク馬数                   | 26,436 / 40,561            |
| **nvd_um 被覆率**                                | **65.2%**（既知問題 #254） |

**判定: ABORT**（全 subgroup < 1,000）

### 不足の背景

NAR 地方競馬は芝コースを持つ競馬場が少ない（大井・川崎・船橋・浦和はダート専用）。

- `surface_debut_turf`（初芝）: 地方→中央転入の初芝が主体だが、nvd_se が JRA 出走を持たないため捕捉できていない
- `surface_debut_dirt`（初ダ）: NAR ではほぼ全馬がダートのため該当が極めて少ない
- `gelding_first`: JRA より絶対数が少なく 865 件にとどまる

---

## Gate-1: Subgroup 信号 — PASS（surface_debut_dirt のみ）

非 subgroup ベースライン：avg_pop=5.758、avg_fp=5.743（n=807,376）

| Subgroup               | n   | avg_pop   | avg_fp    | 着順差（vs baseline） | 信号         |
| ---------------------- | --- | --------- | --------- | --------------------- | ------------ |
| gelding_first          | 865 | 5.353     | 6.155     | +0.41                 | なし（悪化） |
| surface_debut_turf     | 851 | 6.639     | 6.770     | +1.03                 | なし（悪化） |
| **surface_debut_dirt** | 270 | **3.896** | **4.937** | **−0.81**             | あり（好走） |

**注**: `surface_debut_dirt` は avg_fp=4.94 とベースライン 5.74 より好走（人気順 3.9 と上位人気が多い）。しかし母数が 270 件と少ない。

---

## Gate-2: Partial-ρ — ABORT

Within-race demean 後の条件変化フラグ residual と `finish_norm_dm` の相関：

| Subgroup           | partial-ρ | 判定            |
| ------------------ | --------- | --------------- |
| gelding_first      | 0.006     | ABORT（< 0.02） |
| surface_debut_turf | 0.003     | ABORT（< 0.02） |
| surface_debut_dirt | 0.002     | ABORT（< 0.02） |

**判定: ABORT**（全 subgroup |partial-ρ| < 0.02。gate-1 で見えた surface_debut_dirt の優位は popularity 特徴で完全に説明される）

---

## Gate-3: Incremental 確認 — 未実施

gate-0/2 で ABORT のため進まない。

---

## Per-class Verdict（NAR subclass 別）

| Subgroup           | gate-0        | gate-1       | gate-2          | 最終判定  |
| ------------------ | ------------- | ------------ | --------------- | --------- |
| gelding_first      | ABORT (n=865) | なし         | ABORT (ρ=0.006) | **ABORT** |
| surface_debut_turf | ABORT (n=851) | なし         | ABORT (ρ=0.003) | **ABORT** |
| surface_debut_dirt | ABORT (n=270) | PASS (−0.81) | ABORT (ρ=0.002) | **ABORT** |

**NAR 全体: 条件変化特徴は採用なし**

---

## 考察

1. **NAR 芝/ダート切替サンプルが構造的に少ない**: NAR の主要競馬場はほぼダート専用のため、初芝・初ダートのサンプルが JRA の 1/10 以下。統計的検出力が著しく低い。
2. **nvd_um 被覆 65.2%（#254）**: 血統情報の欠損は本 probe 直接の問題ではないが、条件変化特徴が血統と交互作用を持つ場合、検出を阻害する可能性がある。
3. **surface_debut_dirt の gate-1 通過（−0.81）**: サンプル 270 件と少なく、且つ上位人気（avg 3.9）が多い選択バイアスが疑われる。市場が地方転入馬の適性を過小評価している可能性も否定できないが、partial-ρ=0.002 で odds を partial out すると信号が消える。
4. **gelding_first サンプル 865**: JRA の 1,383 と比較して少ない。地方競馬での去勢手術後転厩パターンが JRA と異なる可能性。

---

## 結論

NAR における条件変化特徴は、**構造的サンプル不足（gate-0 ABORT）かつ partial-ρ 0.02 未達（gate-2 ABORT）** → **全 ABORT**。

NAR の経験的フロンティア（`project_finish_position_frontier_2026_06_11` / `project_nar_g1f1_combined_adopt_2026_06_12`）を改めて確認。
唯一の外部 unlock は nvd_um #254（NAR 血統 Signal4）であり、条件変化フラグは独立した改善レバーとして機能しない。
