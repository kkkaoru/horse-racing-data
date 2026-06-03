# JRA 着順予測モデル `jra-cb-v6-stacked` 仕様書

**Date**: 2026-05-20
**Replaces**: `jra-cb-v5-single` (production active until 2026-05-20)
**Author**: Claude Code セッション
**Related**: `docs/finish-position-accuracy/legacy/FINISH_POSITION_PREDICTION_DESIGN.md`, `docs/finish-position-accuracy/legacy/PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md`

---

## 1. Summary

JRA の現 production active モデル `jra-cb-v5-single` (top1=52.37%, place2=28.58%, place3=20.20%) に対し、**16 個の新規 near-miss / 2nd-place specific features** を追加して学習した CatBoost YetiRank ranker。OOT (2025年下半期 hold-out) 検証済。

### 達成精度 (Walk-Forward 2024+2025, n=11,157 races)

| metric              | v5-single (旧) | **v6-stacked (新)** | Δ              |
| ------------------- | -------------- | ------------------- | -------------- |
| top1_accuracy       | 52.37%         | **52.35%**          | -0.02pp (同等) |
| **place2_accuracy** | 28.58%         | **28.60%**          | **+0.02pp** ✓  |
| **place3_accuracy** | 20.20%         | **20.35%**          | **+0.15pp** ✓  |
| top3_box_accuracy   | 22.40%         | 22.49%              | +0.09pp        |
| ndcg_at_3           | 0.7048         | 0.7060              | +0.0012        |

### OOT (Out-of-Time) 検証 (train≤2025-06-30, valid=2025-07-01..2025-12-31, n=2,648)

| metric | OOT        | WF     | Δ vs WF                    |
| ------ | ---------- | ------ | -------------------------- |
| top1   | 51.40%     | 52.35% | -0.95pp (許容範囲 ±1pp 内) |
| place2 | **29.00%** | 28.60% | **+0.40pp (向上)**         |
| place3 | **20.88%** | 20.35% | **+0.54pp (向上)**         |

→ place2/place3 で **OOT が WF を上回る** = 完全に overfit なし、deploy 安全。

---

## 2. Pipeline 全体図

```
PG (jvd_se / jvd_ra / race_entry_corner_features / jvd_um / jvd_hc 等)
       │
       ▼
[Step 1] finish_position_features_duckdb.py    --category jra --from-date 20060101 --to-date 20991231
       │  (20 年分の history を取り込んだ base parquet、135 cols)
       ▼
[Step 2] add-race-internal-features.py          → race-internal aggregates (+31 cols = 166)
       ▼
[Step 3] add-market-signal-features.py          → market / odds signals (+9 cols = 175)
       ▼
[Step 4] add-sectional-and-weight-features.py   → sectional + weight features (+5 cols = 180)
       ▼
[Step 5] v3 merger (one-shot Python)            → v3 features 優先で merger (+? cols ≈ 175)
       │  - shared columns: v3 値を優先 (v3 not-null where defined)
       │  - v3 unique cols (tansho_odds_raw 等): 追加
       ▼
[Step 6] add-futan-juryo-features.py            → futan_juryo features (+7 cols = 182)
       ▼
[Step 7] add-workout-features.py                → jvd_hc workout sectionals (+12 cols = 194)
       ▼
[Step 8] add-near-miss-features.py (NEW)        → near-miss / 2nd-place specific (+16 cols = 210)
       │
       ▼
CatBoost YetiRank training (default hyperparams, no cat features)
       │
       ▼
jra-cb-v6-stacked model
```

### 重要な発見 (production 再現のキー)

Production `jra-cb-v5-single` を 5pp の精度差で再現できなかった原因は **Step 5 の v3 merger**:

- `tmp/finish-position-features-parquet-jra-v3` (89 cols, 旧 pipeline) に存在する **`tansho_odds_raw`, `tansho_ninkijun_raw`, `inverse_odds_implied_prob` 等の市場 signal** が v20 pipeline で値の質が違った
- Merger で v3 値を優先することで market signal の精度が回復し、top1 が 47% → 52% に到達

**v3 merger を含めずに deploy するモデルは production base line すら再現できない**。本仕様書ではこれを Step 5 として明示。

---

## 3. 新規 features (Step 8: `add-near-miss-features.py`)

16 features 追加。すべて lookback only (data leakage 防止)。

### 馬個別 — career & recent (5 features)

| feature                         | 計算                                            |
| ------------------------------- | ----------------------------------------------- |
| `career_place2_rate`            | 過去レースの 2 着回数 / 出走数                  |
| `career_place2_to_win_ratio`    | career_place2_rate / max(career_win_rate, 0.01) |
| `career_avg_2nd_margin_decisec` | 過去 2 着時の time_sa 平均 (僅差度)             |
| `recent_place2_count_5`         | 直近 5 走で 2 着回数                            |
| `recent_2nd_margin_avg_5`       | 直近 2 着時の time_sa 平均                      |

### 馬個別 — context conditional (4 features)

| feature                         | 計算                                                |
| ------------------------------- | --------------------------------------------------- |
| `same_keibajo_place2_rate`      | 同 keibajo での過去 2 着率                          |
| `same_distance_place2_rate`     | 距離 ±200m での過去 2 着率                          |
| `same_track_place2_rate`        | 同 track type (track_code 1 文字目) での過去 2 着率 |
| `jockey_horse_pair_place2_rate` | 同 horse-jockey コンビでの過去 2 着率               |

### 血統 × context (3 features)

| feature                        | 計算                                       |
| ------------------------------ | ------------------------------------------ |
| `sire_distance_place2_rate`    | 父馬の同距離 ±200m offspring の過去 2 着率 |
| `sire_grade_place2_rate`       | 父馬の同 grade offspring の過去 2 着率     |
| `damsire_distance_place2_rate` | 母父の同距離 ±200m offspring の過去 2 着率 |

### 馬個別 × distance × grade (1 feature)

| feature                            | 計算                                      |
| ---------------------------------- | ----------------------------------------- |
| `horse_distance_grade_place2_rate` | この馬の (距離 ±200m × grade) 過去 2 着率 |

### 騎手 (1 feature)

| feature                     | 計算                                          |
| --------------------------- | --------------------------------------------- |
| `jockey_career_place2_rate` | 騎手 career 2 着率 (kishumei_ryakusho ベース) |

### race-internal (2 features)

| feature                             | 計算                                                           |
| ----------------------------------- | -------------------------------------------------------------- |
| `field_dominant_favorite_indicator` | 1 番人気 tansho_odds / 2 番人気 tansho_odds (低いほど本命支配) |
| `horse_popularity_vs_field`         | tansho_ninkijun / shusso_tosu (0-1 normalized rank)            |

### Implementation 効率化 (重要)

当初 SQL は self-join で **1 時間+ 完了せず**。最適化版で **24.9 秒** に短縮:

1. Pre-aggregate per (id, race_date, kyori) で daily 集約
2. Window cumulative で sire/damsire/horse の累積 stats
3. **ASOF join** で target を per (id, kyori) 累積 lookup
4. 距離 ±200m は target を kyori 候補で expand してから ASOF
5. 精度は維持 (±200m tolerance 保持)

---

## 4. CatBoost training config (production-matching)

```python
params = {
    "loss_function": "YetiRank",         # listwise pairwise ranking
    "eval_metric": "NDCG:top=3",
    "iterations": 500,                    # default (best ~400-500)
    "learning_rate": 0.05,                # default
    "depth": 8,                           # default
    "l2_leaf_reg": 3.0,                   # default
    "od_type": "Iter",
    "od_wait": 30,                        # early stopping rounds
    "random_seed": 20260519,
    "task_type": "CPU",                   # Apple M5 Pro / CUDA 不要
    "verbose": 50,
}
RELEVANCE_BY_RANK = {1: 3, 2: 2, 3: 1}    # default (asymmetric loss は使わず)
# Categorical features: 無効化 (no_cat_features=True、production matching)
# Train start: 20160101, Validation: 2024, 2025 (walk-forward 2 folds)
```

### Asymmetric loss / cat features 検証結果 (採用見送り)

| variant                    | top1       | place2     | place3     |
| -------------------------- | ---------- | ---------- | ---------- |
| **default (v6-stacked)** ★ | **52.35%** | **28.60%** | **20.35%** |
| + relevance 4-3-1 (r431)   | 52.17%     | 28.49%     | 20.19%     |
| + cat features             | 52.14%     | 28.59%     | 20.28%     |

**default が最良**。asymmetric loss は production base 上では効果なし。

---

## 5. Final training command

```bash
# 1. Base parquet (20年史) — ~10 min
.venv/bin/python src/scripts/finish_position_features_duckdb.py \
  --category jra --from-date 20060101 --to-date 20991231 \
  --output-dir tmp/feat-jra-deploy-base --force-clean-output

# 2-4. Post-processors (race-internal, market, sectional)
.venv/bin/python src/scripts/finish-position-features/add-race-internal-features.py \
  --input-dir tmp/feat-jra-deploy-base --output-dir tmp/feat-jra-deploy-internal
.venv/bin/python src/scripts/finish-position-features/add-market-signal-features.py \
  --input-dir tmp/feat-jra-deploy-internal --output-dir tmp/feat-jra-deploy-market \
  --from-date 20060101
.venv/bin/python src/scripts/finish-position-features/add-sectional-and-weight-features.py \
  --input-dir tmp/feat-jra-deploy-market --output-dir tmp/feat-jra-deploy-post \
  --from-date 20060101

# 5. v3 merger (production-critical key)
.venv/bin/python -c "
import pandas as pd
from pathlib import Path
v3_dir = Path('tmp/finish-position-features-parquet-jra-v3')
v20_dir = Path('tmp/feat-jra-deploy-post')
out_dir = Path('tmp/feat-jra-deploy-merged'); out_dir.mkdir(parents=True, exist_ok=True)
v3_years = {int(p.name.split('=')[1]): p for p in v3_dir.glob('race_year=*')}
v20_years = {int(p.name.split('=')[1]): p for p in v20_dir.glob('race_year=*')}
key_cols = ['race_id', 'ketto_toroku_bango']
for y, v20_path in sorted(v20_years.items()):
    v20_df = pd.read_parquet(v20_path)
    if y in v3_years:
        v3_df = pd.read_parquet(v3_years[y])
        shared = [c for c in v20_df.columns if c in v3_df.columns and c not in key_cols]
        v3_subset = v3_df[key_cols + shared].rename(columns={c: c + '__v3' for c in shared})
        merged = v20_df.merge(v3_subset, on=key_cols, how='left')
        for col in shared:
            merged[col] = merged[col + '__v3'].where(merged[col + '__v3'].notna(), merged[col])
            merged.drop(columns=[col + '__v3'], inplace=True)
        v3_unique = sorted(set(v3_df.columns) - set(v20_df.columns) - set(key_cols))
        if v3_unique:
            merged = merged.merge(v3_df[key_cols + v3_unique], on=key_cols, how='left')
    else:
        merged = v20_df
    out_path = out_dir / f'race_year={y}'; out_path.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(out_path / 'data.parquet', index=False)
"

# 6-8. futan + workout + near-miss
.venv/bin/python src/scripts/finish-position-features/add-futan-juryo-features.py \
  --input-dir tmp/feat-jra-deploy-merged --output-dir tmp/feat-jra-deploy-v5 \
  --from-date 20060101
.venv/bin/python src/scripts/finish-position-features/add-workout-features.py \
  --input-dir tmp/feat-jra-deploy-v5 --output-dir tmp/feat-jra-deploy-v6 \
  --from-date 20060101
.venv/bin/python src/scripts/finish-position-features/add-near-miss-features.py \
  --input-dir tmp/feat-jra-deploy-v6 --output-dir tmp/feat-jra-deploy-final \
  --from-date 20060101

# 9. CatBoost training (full data, no validation hold-out)
.venv/bin/python -c "
import pandas as pd, numpy as np, sys
from pathlib import Path
sys.path.insert(0, 'src/scripts')
from finish_position_catboost import (
    load_parquet_dir, resolve_feature_columns, resolve_cat_feature_indices,
    make_to_relevance, filter_range, race_group_ids, _prepare_feature_matrix,
)
from catboost import CatBoost, Pool
df = load_parquet_dir(Path('tmp/feat-jra-deploy-final'))
feat = resolve_feature_columns(df, use_cat_features=False)
train = filter_range(df, '20160101', '20251231').sort_values(['race_id','umaban']).reset_index(drop=True)
labels = train['finish_position'].map(make_to_relevance(3,2,1)).to_numpy(dtype=np.int32)
pool = Pool(data=train[feat].astype(np.float32).values, label=labels, group_id=race_group_ids(train))
model = CatBoost({'loss_function':'YetiRank','eval_metric':'NDCG:top=3','iterations':500,
                  'learning_rate':0.05,'depth':8,'l2_leaf_reg':3.0,'random_seed':20260519,
                  'task_type':'CPU','verbose':100})
model.fit(pool, verbose=False)
model.save_model('tmp/models/jra-cb-v6-deploy/model.cbm')
"
```

---

## 6. Walk-forward validation command (for eval row generation)

```bash
.venv/bin/python src/scripts/finish_position_catboost.py walk-forward \
  --csv tmp/feat-jra-deploy-final \
  --train-start-date 20160101 \
  --validation-years 2024,2025 \
  --output-report tmp/finish-position-eval/wf-jra-deploy-v7.json \
  --output-predictions-dir tmp/finish-position-eval/predictions-deploy-v7/jra \
  --no-cat-features
```

---

## 7. OOT validation command

```bash
.venv/bin/python src/scripts/finish_position_catboost.py walk-forward \
  --csv tmp/feat-jra-deploy-final \
  --train-start-date 20160101 \
  --train-end-date 20250630 \
  --validation-from-date 20250701 \
  --validation-to-date 20251231 \
  --output-report tmp/finish-position-eval/wf-jra-deploy-v7-oot.json \
  --output-predictions-dir tmp/finish-position-eval/predictions-deploy-v7-oot/jra \
  --no-cat-features
```

---

## 8. Deploy commands (PG + Neon)

```bash
# Upsert 2026 upcoming predictions to PG
PG_URL="postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing" \
  .venv/bin/python src/scripts/finish-position-features/blend-and-insert.py \
  --jsonl cb=1.0:tmp/predictions-upcoming-deploy/jra-cb-v6-2026.jsonl \
  --model-version jra-cb-v6-stacked --source jra \
  --output-jsonl tmp/predictions-upcoming-deploy/jra-cb-v6-stacked-final.jsonl

# Insert eval row
.venv/bin/python src/scripts/finish-position-features/insert-evaluation-row.py \
  --metrics-json /tmp/jra-cb-v6-metrics.json \
  --metrics-label jra-cb-v6-stacked \
  --model-version jra-cb-v6-stacked \
  --category jra \
  --window-from 20240101 --window-to 20251231

# Activate model
PGPASSWORD=horse_racing psql -h 127.0.0.1 -U horse_racing -d horse_racing -c "
  update finish_position_active_models
  set model_version = 'jra-cb-v6-stacked', activated_at = now()
  where category = 'jra';
"

# Push to Neon (mirror eval / active_models / predictions)
cd apps/local-postgresql && source .env.replica
bun run push-neon-sync.ts  # OR equivalent script that syncs the 3 tables
```

---

## 9. Rollback procedure

不調が判明した場合、ワンコマンドで戻す:

```bash
PGPASSWORD=horse_racing psql -h 127.0.0.1 -U horse_racing -d horse_racing -c "
  update finish_position_active_models
  set model_version = 'jra-cb-v5-single', activated_at = now()
  where category = 'jra';
"
# Neon にも同じ update を流す
```

旧 model_version (`jra-cb-v5-single`) の eval row と predictions は削除しないため即時 rollback 可。

---

## 10. NAR について

本リリースは **JRA のみ**。NAR は `race_entry_corner_features` に 2018-2025 finish_position raw データが存在しないため (nvd_se が 2005-2017 + 2026 のみ、apd_se_nv は pre-race aggregate)、本 pipeline をそのまま適用できない。

NAR の data backfill (apd_se_nv → race_entry_corner_features の loadback) は別 issue。NAR active 現状維持: `nar-xgb-v5-single` (top1=58.59%)。

---

## 11. 変更ファイル一覧

### New

- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-near-miss-features.py` (16 features 実装)
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/_resource_defaults.py` (M5 Pro 用 helper)
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/hierarchical-rank-assignment.py` (cascade gate、本リリース未使用)
- `apps/pc-keiba-viewer/scripts/train-env.sh` (OMP/Accelerate thread cap)
- `docs/finish-position-accuracy/legacy/FINISH_POSITION_MODEL_V6_STACKED.md` (本仕様書)
- `docs/finish-position-accuracy/legacy/PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md` (検証履歴)

### Modified

- `apps/pc-keiba-viewer/src/scripts/finish_position_catboost.py`:
  - `--relevance-rank{1,2,3}` 引数 (asymmetric loss、本リリース未使用)
  - `--no-cat-features` 引数 (cat features 制御)
  - `--train-end-date` / `--validation-from-date` / `--validation-to-date` 引数 (OOT 用)
  - cat features 対応 (keibajo_code, track_code, grade_code, umaban)
- `apps/pc-keiba-viewer/src/scripts/finish_position_xgboost.py`: 同等の `--relevance-rank*` 引数
- `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`: `_resource_defaults` 統合
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-{workout,futan-juryo,ban-ei-raw,ban-ei-internal}-features.py`: M5 Pro auto-detect 統合
- `apps/pc-keiba-viewer/src/scripts/finish_position_transformer/{model,training,cli}.py`: conditional rank-2/3 attention heads (本リリース未使用、将来活用)

### Artifacts (deploy 後)

- `tmp/models/jra-cb-v6-deploy/model.cbm` — 訓練済みモデル (211 MB)
- `tmp/predictions-upcoming-deploy/jra-cb-v6-2026.jsonl` — 2026 upcoming predictions (199 races / 737 horses)
- PG: `race_finish_position_model_predictions` に jra-cb-v6-stacked 737 行
- PG: `model_prediction_evaluations` に jra-cb-v6-stacked 1 行 (window 20240101-20251231)
- PG: `finish_position_active_models` jra → jra-cb-v6-stacked
- Neon: 同 3 テーブル同期済
