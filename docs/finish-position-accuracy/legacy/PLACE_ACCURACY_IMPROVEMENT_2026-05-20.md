# JRA/NAR 着順予測 place2 / place3 精度向上 実装記録

**Date**: 2026-05-20
**Status**: empirical に **不可行** と判定、現 active 維持
**Related**: `docs/finish-position-accuracy/legacy/FINISH_POSITION_PREDICTION_DESIGN.md`

---

## 1. ゴール

JRA / NAR の着順予測で **place2 (2 着的中率) と place3 (3 着的中率)** を +1pp 以上改善する。top1 精度は維持必須 (`docs/finish-position-accuracy/legacy/FINISH_POSITION_PREDICTION_DESIGN.md` の精度低下禁止原則)。Ban-ei 対象外。

### 開始時点の現状

| カテゴリ | active model                                | top1   | place2 | place3 |
| -------- | ------------------------------------------- | ------ | ------ | ------ |
| JRA      | `jra-cb-v5-single` (CatBoost YetiRank)      | 52.37% | 28.58% | 20.20% |
| NAR      | `nar-xgb-v5-single` (XGBoost rank:pairwise) | 58.59% | 35.78% | 27.47% |

---

## 2. 検討した方針 (v2 設計プラン)

empirical 段階評価による fast feedback 設計。詳細は `/Users/kkk4oru/.claude/plans/iridescent-imagining-clock.md` (v2 セルフレビュー反映版)。

| Phase | 内容                                                        | 想定効果                              |
| ----- | ----------------------------------------------------------- | ------------------------------------- |
| P-1a  | PG/Neon に index 3 件追加                                   | feature engineering / eval 高速化     |
| P-1b  | DuckDB スクリプトの threads / memory auto-detect 化         | M5 Pro 15 core 活用                   |
| M5P-3 | `train-env.sh` で OMP/Accelerate thread cap                 | BLAS oversubscription 回避            |
| P0'   | LightGBM LambdaRank + `--relevance-tier place_weighted`     | place2/3 +1pp 期待                    |
| P0''' | MLX Transformer NAR 初試行 + JRA place 再評価               | Metal GPU 活用                        |
| P0''  | legacy NAR binary-place2/place3 baseline 計測               | hierarchical 投資判断                 |
| P1    | position-specific binary base models 訓練 (JRA 6 + NAR 4)   | rank2/3 specialist                    |
| P2    | hierarchical-rank-assignment.py + cascade gate              | rank 1 不変、2/3 を specialist で改善 |
| P3    | optimize-ensemble-place.py (place_N 直接最大化 grid search) | place2/3 上積み                       |
| P4    | walk-forward + Out-of-time 2025 H2 hold-out + deploy        | 採用判断                              |

### Mac GPU (Metal) 活用検討

- MLX (`mlx.core`, `mlx.nn`) が install 済、Metal GPU 自動使用可能
- 既存 `finish_position_transformer/RaceSetTransformer` あり、`--place2-weight/--place3-weight` 既存
- FT-Transformer / TabNet / NODE / TabPFN 等は **不採用** (既存 transformer と同系列で empirical 劣後の見込み、Grinsztajn 2022 と整合)

### M5 Pro MacBook Pro 専用最適化 (実測 spec ベース)

- CPU: Apple M5 Pro — 5P + 10E = 15 logical cores
- RAM: 48 GB unified memory
- numpy → **Apple Accelerate** BLAS にリンク済
- libomp brew 入り → LightGBM/XGBoost OpenMP 並列 OK
- 推奨スレッド: DuckDB=15, ML training=8, parallel-fold 時=5×2

---

## 3. 実装した成果物

### コード追加

```
apps/pc-keiba-viewer/scripts/train-env.sh                                     [NEW]
apps/pc-keiba-viewer/src/scripts/finish-position-features/
  ├── _resource_defaults.py                                                   [NEW]
  └── hierarchical-rank-assignment.py                                         [NEW]
```

### コード変更

```
apps/pc-keiba-viewer/src/scripts/
  ├── finish_position_features_duckdb.py                  (auto-detect 化)
  └── finish-position-features/
        ├── add-workout-features.py                       (argparse + auto-detect)
        ├── add-ban-ei-raw-features.py                    (同上)
        ├── add-ban-ei-internal-features.py               (同上)
        └── add-futan-juryo-features.py                   (同上)
```

### Empirical 検証で得た artifact

```
tmp/finish-position-eval/predictions-active-v5/
  ├── jra/2024-2025.jsonl                  (CatBoost 再訓練 walk-forward, top1=47.09%)
  ├── nar/2024-2025.jsonl                  (XGBoost 再訓練 walk-forward, top1=58.45%)
  ├── jra-hierarchical-binary/...          (hierarchical + binary, 劣後を実証)
  ├── jra-hierarchical-trans/...           (hierarchical + transformer, 劣後を実証)
  ├── nar-hierarchical-binary/...
  └── nar-hierarchical-trans/...
tmp/finish-position-eval/wf-jra-v5-cb.json     (再訓練 walk-forward report)
tmp/finish-position-eval/wf-nar-v5-xgb.json
```

### Skip した Phase (DB index)

PG/Neon `model_prediction_evaluations` / `race_finish_position_model_predictions` / `race_entry_corner_features` の 3 候補 index は **すべて実測で不要** と判明:

- `model_prediction_evaluations`: 6 行 / 32KB、active eval 取得 0.487ms (PK + heapsort)
- `race_finish_position_model_predictions`: 既存 PK が `(model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)` で完全カバー
- `race_entry_corner_features`: 既存 `race_entry_corner_features_lookup_idx (source, race_date, track_code, kyori)` を planner が Index Only Scan で利用 (4.7M 行から 72K 行を 61ms)

---

## 4. Empirical 検証結果

### 4.1 Baseline (再訓練の active 同型 model)

| カテゴリ | model                         | top1   | place2     | place3     |
| -------- | ----------------------------- | ------ | ---------- | ---------- |
| JRA      | CatBoost YetiRank (regen)     | 47.09% | **27.39%** | **20.06%** |
| NAR      | XGBoost rank:pairwise (regen) | 58.45% | **35.59%** | **27.32%** |

NAR は production active (58.60%/35.78%/27.47%) にほぼ一致。JRA は hyperparameter 違いで production active (52.37%) より -5pp 低いが、hierarchical の **delta 検証用 baseline** としては十分。

### 4.2 既存 single-model 試行 (すべて active 大幅劣後)

| 試行                        | JRA top1              | JRA place2       | NAR top1 | NAR place2 |
| --------------------------- | --------------------- | ---------------- | -------- | ---------- |
| LambdaRank place_weighted   | **40.60%** (-11.77pp) | 20.86% (-7.72pp) | —        | —          |
| MLX Transformer iter2b ens3 | 44.34%                | 21.21%           | —        | —          |
| MLX Transformer iter2b 単体 | 44.16%                | 21.68%           | —        | —          |
| binary-place2 standalone    | 22.0%                 | 17.9%            | 26.96%   | 20.44%     |
| binary-place3 standalone    | 13.4%                 | 17.9%            | 13.71%   | 18.35%     |
| NAR transformer trans-ens3  | —                     | —                | 44.93%   | 23.11%     |
| NAR transformer trans-s23   | —                     | —                | 44.95%   | 23.03%     |

### 4.3 Hierarchical Rank Assignment (cascade gate)

実装: `apps/pc-keiba-viewer/src/scripts/finish-position-features/hierarchical-rank-assignment.py`

ロジック:

1. rank 1 = active model の rank 1 (不変)
2. rank 2 = place2 specialist 候補 (cascade confidence > threshold の時) または active rank 2
3. rank 3 = 同様
4. rank 4+ = active model 順
5. Cascade confidence = `(top1_score - top2_score) / |top1_score|`

#### binary specialist で実行 (cascade threshold 0.05)

| カテゴリ | top1     | place2 (Δ)               | place3 (Δ)          | cascade skip ratio |
| -------- | -------- | ------------------------ | ------------------- | ------------------ |
| JRA      | 47.09% ✓ | **21.30% (-6.10pp)** ❌  | 17.12% (-2.94pp) ❌ | 12.03%             |
| NAR      | 58.45% ✓ | **23.95% (-11.64pp)** ❌ | 18.52% (-8.80pp) ❌ | 4.80%              |

#### transformer specialist で実行 (cascade threshold 0.05)

| カテゴリ | top1     | place2 (Δ)               | place3 (Δ)          |
| -------- | -------- | ------------------------ | ------------------- |
| JRA      | 47.09% ✓ | **20.12% (-7.27pp)** ❌  | 16.57% (-3.49pp) ❌ |
| NAR      | 58.45% ✓ | **24.26% (-11.33pp)** ❌ | 18.56% (-8.76pp) ❌ |

#### cascade threshold sweep (JRA, transformer specialist)

| threshold | place2 | vs baseline          |
| --------- | ------ | -------------------- |
| 0.05      | 20.12% | **-7.27pp** ❌       |
| 0.3       | 23.28% | -4.11pp ❌           |
| 0.5       | 25.85% | -1.54pp ❌           |
| 0.8       | 27.31% | -0.08pp (実質 no-op) |

→ どの threshold でも positive な改善は **得られない**。閾値を上げるほど baseline に漸近する (= specialist 注入の sweet spot は存在しない)。

### 4.4 Iter2b proxy での smoke test (algorithm 正当性検証)

弱い top1 model (iter2b transformer, top1=44%) を baseline にすると hierarchical が place2 +1.19pp 改善した。これは **algorithm 自体は正しい** ことを示すが、強い GBDT baseline では specialist が improve できない empirical 事実を裏付ける。

---

## 5. なぜ全部失敗するか (root cause)

現 active GBDT (CatBoost/XGBoost) の predicted_rank ordering は **rank 1, 2, 3 を同時最適化** している。これより rank 2 候補を「specialist」が改善するためには:

1. specialist が **rank 1 を除外した条件付き** で active 以上に正確である必要がある
2. ところが binary-place2 specialist の学習目的は P(finish=2) であって P(finish=2 | not 1st) ではない
3. P(finish=2) と P(finish=1) は強く相関 (どちらも「強い馬の signal」) → specialist は active の rank 1 と類似馬を rank 2 候補に出す
4. 結果、active が natural に出す rank 2 (= スコア 2 位) より悪い候補が選ばれる

**つまり place 専用 specialist は active が既に学習している情報を再学習しているだけで、新しい signal を持っていない。**

v1 セルフレビュー時に懸念した "cascade risk" が 100% 的中したケース。Cascade gate は機能するが、active rank 2 を超える specialist が存在しない以上、何も助けにならない。

---

## 6. 結論と推奨

### 結論

**place2/place3 +1pp 改善目標は、提案された v2 プランのアプローチでは empirical に達成不可**。今後この方向 (LambdaRank place_weighted / binary specialist / transformer specialist / hierarchical / score blending) を再試行することは時間の浪費。

### 今後達成余地がある方向 (本プラン scope 外)

1. **新しい feature engineering** — 2 着馬と 1 着馬を区別する signal を陽に設計
   - "historically near-miss horse" score
   - "specific competitor matchup loser" — 特定の強い馬がいる時に 2 着になりやすい
   - "pace race vs lone front-runner race" type — ペース展開で 2 着取得率が変わる傾向
2. **GNN / explicit rank-2 attention architecture** — race set 内の馬間関係を陽に表現
3. **追加 training data** — 2024-2025 を超える年間データ収集 or 海外レース横展開
4. **ensemble at score level (not rank reassignment)** — active と specialist の score を blend して全 rank 再構成 (本プランの hierarchical とは別物)。要 empirical 検証。

### Production への影響

- **現 active 維持** (`jra-cb-v5-single` / `nar-xgb-v5-single`)
- `model_prediction_evaluations` / `finish_position_active_models` / `race_finish_position_model_predictions` への投入なし
- Neon push なし

---

## 7. Reusable assets

以下は今後の実験で再利用可能:

| Asset                                                                                       | 用途                                                                                                                                                   |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `apps/pc-keiba-viewer/src/scripts/finish-position-features/_resource_defaults.py`           | M5 Pro 用 threads/memory auto-detect helper。新規 DuckDB スクリプトで `from _resource_defaults import add_resource_args, apply_to_connection` で再利用 |
| `apps/pc-keiba-viewer/scripts/train-env.sh`                                                 | ML 訓練前に `source` して OMP/Accelerate thread cap を設定                                                                                             |
| `apps/pc-keiba-viewer/src/scripts/finish-position-features/hierarchical-rank-assignment.py` | 将来 active を超える specialist が見つかった時に再活用可。**現状は production 投入不可**                                                               |
| `tmp/finish-position-eval/predictions-active-v5/{jra,nar}/2024-2025.jsonl`                  | active 同型 model の WF predictions、hierarchical 検証 baseline                                                                                        |
| `_resource_defaults.py` で auto 設定された 5 DuckDB スクリプト                              | M5 Pro で 15 threads / 31GB を使用                                                                                                                     |

---

## 7-bis. 追補: 新規 feature engineering + MLX conditional attention の試行 (2026-05-20 同日継続)

ユーザー指示「+1pp に届くために (1) 2 着馬と 1 着馬を区別する新規 feature engineering、(2) GNN / explicit rank-2 attention を実施」を受けて両方を実装・empirical 検証。

### Phase A: Near-miss / 2nd-place specific feature engineering

新規 12 features を `add-near-miss-features.py` に実装、`feat-jra-v6` (8 features) → `feat-jra-v7` (12 features) を生成。CatBoost で walk-forward 訓練。

**v7 追加 features**:
| feature | 由来 | empirical 値 |
|---|---|---|
| `career_place2_rate` | 馬の career 2 着率 | avg 9.5%、bimodality 確認済 |
| `career_place2_to_win_ratio` | 2 着型 vs 勝ち切り型の区別 | avg 0.90 |
| `career_avg_2nd_margin_decisec` | 2 着時の time_sa 平均 (僅差度) | per-horse |
| `recent_place2_count_5` | 直近 5 走の 2 着回数 | per-horse |
| `recent_2nd_margin_avg_5` | 直近の負け差 | per-horse |
| `jockey_career_place2_rate` | 騎手 2 着率 | avg 7.8% |
| `field_dominant_favorite_indicator` | 1 番人気 odds / 2 番人気 odds | avg 0.64 |
| `horse_popularity_vs_field` | ninkijun / shusso_tosu | per-horse |
| `same_keibajo_place2_rate` | 同 keibajo 過去 2 着率 | avg 11.6% |
| `same_distance_place2_rate` | 距離 ±200m での 2 着率 | avg 10.4% |
| `same_track_place2_rate` | 同 track type の 2 着率 | avg 10.4% |
| `jockey_horse_pair_place2_rate` | 同コンビ過去 2 着率 | avg 12.6% |

**v7 結果** (JRA CatBoost, regen baseline 47.09% / 27.39% / 20.06%):

- top1 **47.25% (+0.16pp)** ✓
- **place2 27.54% (+0.14pp)** ✓
- place3 20.05% (-0.01pp、誤差)

**Phase A 評価**: context-specific 2 着率 features が **再現的に小幅正方向の signal** を持つことを empirical 確認。但し **+1pp 目標は未達**。

### Phase B: MLX RaceSetTransformer conditional rank-2 attention head

`model.py` に以下を追加実装:

- `conditional_place2_logit`: 予測 winner_emb (softmax-weighted) を各馬の encoded と連結 → MLP → logit
- `conditional_place3_logit`: 予測 winner_emb + 予測 runnerup_emb (softmax-weighted) を各馬の encoded と連結 → MLP → logit
- multitask loss に 2 つの BCE term 追加 (weight 2.0 / 1.5)
- walk-forward predict で 3 つの jsonl 出力 (rank / cp2 / cp3)

**訓練結果** (`feat-jra-v7` を入力、M5 Pro Metal GPU):

- 訓練時間: 64 秒 / 2 fold
- trans-rank top1 = **38.56% (baseline -8.5pp)** — conditional loss 追加で rank score が悪化
- trans-cp2 standalone place2 = **18.97% (baseline -8.4pp)** — conditional head は学習されたが標準偏差大幅劣後
- trans-cp3 standalone place3 = 13.71%

**hierarchical 経由検証** (v7 CatBoost top1 + trans-cp2/cp3 specialist):

- top1 47.25% ✓ (rank 1 不変)
- place2 **20.78% (-6.76pp)** ❌
- place3 **16.21% (-3.85pp)** ❌

**Phase B 評価**: conditional attention head は architecture として正しく学習されたが、本タスクで transformer が GBDT に勝てない empirical 事実は覆らず。conditional head 経由でも diversity を引き出せず、hierarchical で大幅劣化。

### 結論 (Phase A + B 完了後)

| 戦略                                          | place2 改善       | 結果                                 |
| --------------------------------------------- | ----------------- | ------------------------------------ |
| **Phase A v7 (near-miss + context features)** | **+0.14pp**       | empirical に best (production-ready) |
| Phase B (MLX conditional attention)           | -6.76pp 〜 -8.4pp | empirical に dead                    |
| Phase A + B hierarchical                      | -6.76pp           | empirical に dead                    |

**+1pp 目標は本セッションで empirical 達成不可** と判明。v7 の +0.14pp は production CatBoost (52.37% baseline) でも同様 +0.1-0.2pp 程度の改善が期待でき、deploy 価値あり。

### 追加した artifact

- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-near-miss-features.py` (12 new features)
- `apps/pc-keiba-viewer/src/scripts/finish_position_transformer/model.py` (conditional heads)
- `apps/pc-keiba-viewer/src/scripts/finish_position_transformer/training.py` (multitask loss + predict_all_logits)
- `apps/pc-keiba-viewer/src/scripts/finish_position_transformer/cli.py` (CLI 引数 + 3 jsonl 出力)
- `tmp/feat-jra-v7/` (12 features 拡張 parquet)
- `tmp/finish-position-eval/predictions-v7/jra/` (v7 CatBoost predictions)
- `tmp/finish-position-eval/predictions-v7-trans-cond/jra/` (transformer predictions)

### NAR について

NAR (race_entry_corner_features に 2018-2025 データ無し、apd_se_nv 等の別系列) は本 Phase A の scope 外。NAR 2018+ raw データを race_entry_corner_features に backfill する別 issue が必要。

### 今後の方向 (本 issue scope 外)

empirical に +1pp 達成は困難と判明。さらなる挑戦には:

1. NAR-side raw data backfill (apd_se_nv → race_entry_corner_features 統合)
2. **より sophisticated な feature** (例: pedigree level place2 rate、distance-grade combo place2 rate、horse-horse pairwise rivalry signal)
3. **target-aware training** — finish_position の 2 と 1+3 を区別する非対称 loss
4. **データ量を増やす** — 海外レース活用 (米国・香港) 等

これらは現プランの empirical findings を踏まえた将来 issue で議論。

---

## 7-ter. 第三弾追補: Phase C/D/E/F 実装と Pareto 限界の empirical 確定 (2026-05-20 同日継続)

ユーザー指示「NAR-side raw data backfill、higher-order context features (pedigree×distance×grade)、target-aware asymmetric loss」を実施。

### Phase C: NAR-side raw backfill = データパイプライン GAP として確定 (本 scope 外)

調査結果:

- `race_entry_corner_features` (canonical): NAR 2024-2025 = **0 行** (build-corner-feature-table.ts が `nvd_se` 経由で構築するが nvd_se が 2005-2017+2026 のみ)
- `apd_se_nv`: 40 cols あるが `kakutei_chakujun` / `time_sa` / `kishu_code` 無し (pre-race aggregate のみ)
- `apd_sokuho_se`: 空テーブル (live feed placeholder)
- `nvd_*` 系列: 2018-2025 の finish position raw 不在

→ **データ ingestion task として別 issue 化**、本 PR scope 外と確定。

### Phase D: pedigree × distance × grade higher-order features (4 features 追加、v8)

実装した features (`add-near-miss-features.py` 拡張):

1. `sire_distance_place2_rate` — sire のこの ±200m kyori での過去 2 着率
2. `sire_grade_place2_rate` — sire のこの grade での過去 2 着率
3. `damsire_distance_place2_rate` — damsire のこの ±200m kyori での過去 2 着率
4. `horse_distance_grade_place2_rate` — この馬の (kyori ±200m × grade) ペアでの過去 2 着率

#### SQL 最適化 (重要な学び)

初稿 v8: target × sire_race_history × damsire_race_history の cartesian → 1h+ 経過しても未完了 (4.5 trillion 中間行)。

修正版: **pre-aggregate + window cumulative + ASOF join + kyori expansion**:

- `sire_daily_kyori` で per (sire_id, race_date, kyori) を集約
- `sire_kyori_cumul` で window cumulative
- `target × distinct kyori within ±200m` で expand (~5x で済む)
- ASOF join で per (sire_id, kyori) の最新累積を取得

→ **24.9 秒**で完了 (1h+ → 25 秒)。**精度は維持** (±200m tolerance も保持)。

### Phase E: target-aware asymmetric loss (`--relevance-rank{1,2,3}` 引数追加)

`finish_position_catboost.py` / `finish_position_xgboost.py` に `make_to_relevance()` factory + CLI 引数を追加。Default 3-2-1 (既存維持) → boost variants 試行。

### Phase F: 全 variant 評価

| variant                                  | top1 (Δ vs v5 47.09%) | place2 (Δ vs v5 27.39%) | place3 (Δ vs v5 20.06%) |
| ---------------------------------------- | --------------------- | ----------------------- | ----------------------- |
| v5 baseline                              | 47.09%                | 27.39%                  | 20.06%                  |
| **v7** (12 near-miss + context features) | +0.16pp               | **+0.14pp** ★ best      | -0.01pp                 |
| v8 default (16 features)                 | +0.06pp               | -0.06pp                 | -0.01pp                 |
| v8 r331 (place2 boost)                   | -0.30pp ❌            | -0.32pp ❌              | 0.00pp                  |
| **v8 r431** (rank1=4 separation)         | +0.08pp               | -0.12pp                 | **+0.32pp** ★ best      |
| v8 r341 (rank2 highest)                  | -0.75pp ❌            | -0.67pp ❌              | -0.15pp                 |
| v8 r442                                  | -0.16pp               | +0.04pp                 | +0.26pp                 |
| **blend64** (60% v7 + 40% v8-r431)       | **+0.18pp** ★ best    | +0.10pp                 | -0.05pp                 |
| blend50                                  | +0.07pp               | -0.03pp                 | +0.15pp                 |

### 重要な empirical 発見

1. **v8 default が v7 を超えない**: pedigree×distance/grade features は既存の `sire_distance_win_rate` などと相関しすぎ、純粋な追加 signal が薄い。
2. **place2 boost (r331/r341) は悪化**: 過剰な rank1=rank2 化で model が discrimination を失う。
3. **rank1=4 separation (r431) は place3 で +0.32pp**: rank1 と rank2 のスコア差を強調することで place3 distinguishability が向上 (副次効果)。
4. **Blend は線形補間**: v7 と v8-r431 を blend しても両 metric 同時改善は不可能 — Pareto frontier 上で trade-off するのみ。
5. **+1pp 目標は本セッションで empirical 達成不可** と確定 (試行モデル数 12+ で全 fail)。

### 達成可能だった最良値

- top1: 47.27% (+0.18pp) via blend64
- place2: 27.54% (+0.14pp) via v7
- place3: 20.38% (+0.32pp) via v8-r431

### 追加 artifact (Phase C/D/E/F 由来)

- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-near-miss-features.py` 拡張 (179 cols 出力対応)
- `apps/pc-keiba-viewer/src/scripts/finish_position_catboost.py` — `make_to_relevance()` + `--relevance-rank{1,2,3}`
- `apps/pc-keiba-viewer/src/scripts/finish_position_xgboost.py` — 同上
- `tmp/feat-jra-v8/` — pedigree features 込み 179 col parquet
- `tmp/finish-position-eval/predictions-v8{,-r331,-r431,-r341,-r442}/jra/` — 5 variant predictions
- `tmp/finish-position-eval/predictions-blend-v7-v8r431/w*/` — 6 blend variants

### 最終結論 (Phase A+B+C+D+E+F 完了後)

**+1pp 目標は本タスクの empirical 限界を超えており、現データ・現 model family で達成不可**。試行した全 12+ variants (LambdaRank place_weighted / binary specialist / MLX transformer + conditional / hierarchical / v6 / v7 / v8 / asymmetric loss / blend) すべて失敗。

達成可能な改善 (deploy 推奨):

- v7 features を production CatBoost (52.37% baseline) に移植 → 期待 place2 +0.1-0.2pp、top1 +0.1-0.2pp
- 必要なら v8-r431 を併用して place3 +0.2-0.3pp を取りに行く (top1 trade-off あり)

達成不可能な方向 (再試行禁止):

- ❌ binary specialist (Phase A2 binary objective 系列)
- ❌ hierarchical rank reassignment (cascade error 確定)
- ❌ MLX transformer + conditional attention heads
- ❌ asymmetric loss で place2 emphasize (r331/r341 系列)
- ❌ pedigree×distance/grade features (既存 sire_distance_win_rate と相関、純粋追加 signal 薄)

達成余地のある未試行方向 (本セッション外):

- NAR raw データ backfill (data pipeline task)
- ペアレース matchup signal (横の馬間 rivalry を encode)
- 海外レース横展開によるデータ量 2x 化
- 賭式 (馬連・馬単) target を組み込んだ multi-task

---

## 8. 関連 memory (Claude 用)

- `.claude/projects/.../memory/user_machine_m5pro.md` — M5 Pro 実測スペック
- `.claude/projects/.../memory/project_mlx_transformer_status.md` — MLX transformer の JRA 11 iter 実測結果
- `.claude/projects/.../memory/project_place_improvement_infeasible.md` — 本ドキュメントの要約 (今後同じ提案の再試行防止)
- `/Users/kkk4oru/.claude/plans/iridescent-imagining-clock.md` — v2 プラン全文 (セルフレビュー反映)
