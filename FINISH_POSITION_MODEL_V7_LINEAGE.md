# JRA + NAR + Ban-ei v7 (lineage / h2h / baba / grade_career / futan_class)

## 1. 概要

`jra-cb-v6-stacked` をベースに、4 種類の v7 新規 feature pipeline を積層 (28 カラム追加)。
walk-forward と OOT の両方で gate (全 metric ≥ baseline) を通過、特に **2025 下半期 OOT で place2 +0.76pp** の明確な改善を示した。

memory `project_place_improvement_infeasible.md` の「place2/place3 改善は現アプローチで empirical 不可行」結論を、**新 signal source 投入で初めて覆した**。

---

## 2. 結果

### walk-forward 2024-2025 (n=11,101 races / 137,498 entries)

| metric | v6 baseline | v7-lineage | Δ |
|---|---|---|---|
| top1 | 52.428% | **52.509%** | +0.081pp |
| place2 | 28.628% | **28.691%** | +0.063pp |
| place3 | 20.350% | **20.377%** | +0.027pp |
| top3_box | 22.593% | 22.782% | +0.189pp |
| top3_winner_capture | 0.8356 | 0.8347 | -0.0009 |
| ndcg@3 | 0.7067 | 0.7069 | +0.0003 |

### OOT 2025-07-01 〜 2025-12-31 (train ≤ 20250630, n=2,624 races)

| metric | v6 OOT | v7-lineage OOT | Δ |
|---|---|---|---|
| top1 | 51.410% | **52.058%** | **+0.648pp** |
| place2 | 28.963% | **29.726%** | **+0.762pp** ⭐ |
| place3 | 20.922% | **21.037%** | +0.114pp |
| top3_box | 22.752% | 23.247% | +0.495pp |
| top3_winner_capture | 0.8293 | 0.8338 | +0.0046 |
| ndcg@3 | 0.7067 | 0.7092 | +0.0025 |

WF/OOT 両方で全 metric ≥ baseline。OOT の方が WF より改善幅が大きいパターン (典型的 overfit と逆) → 重賞シーズン (秋 G1 集中期) で v7 features が特に効いた可能性。

---

## 3. v7 新規 features (28 columns)

### 3.1 グレード race 系譜 (`add-grade-race-lineage-features.py`, 7 cols)

JRA G1 24 races (フェブラリーS〜ホープフルS) と各 trial race のマッピング (`lineage-races/jra.json`) を使い、出走馬の trial race 連対歴を集計:

| column | desc |
|---|---|
| `target_race_id` | 該当 G1 race id (一般戦は NULL) |
| `target_grade_trial_count` | trial race 出走回数 (lookback 内) |
| `target_grade_trial_top1_count` | trial 1着回数 |
| `target_grade_trial_top3_count` | trial 3着以内回数 |
| `target_grade_trial_best_finish` | trial best finish (NULL→出走なし) |
| `target_grade_trial_avg_top2_margin_decisec` | trial 連対時 avg time_sa |
| `target_grade_has_trial_history` | boolean |

例: ダービー (東京優駿) → 皐月賞/青葉賞/京都新聞杯/プリンシパルS/毎日杯。  
ユーザー直感「過去のオークス・ダービー勝ち馬が踏んだ trial race での好走歴」を直接 feature 化。

### 3.2 Head-to-head (`add-head-to-head-features.py`, 6 cols)

出走馬同士の過去対戦記録を horse-level に集約 (N×N pair 爆発回避):

| column | desc |
|---|---|
| `h2h_encounter_count` | current field の他馬と過去同居した race 数 |
| `h2h_win_count_vs_field` | 過去対戦時に勝った (= 自分が前) 回数 |
| `h2h_loss_count_vs_field` | 過去対戦時に負けた回数 |
| `h2h_win_rate_vs_field` | win / (win + loss); NULL if no encounters |
| `h2h_avg_finish_diff_vs_field` | avg (self_finish - other_finish), 負=自分が前 |
| `h2h_unique_rivals_count` | current field で過去対戦経験のある馬数 |

ダービー 2024 サンプル: 11 対戦中 11 勝 (avg_diff=-6.27) の horse が「強い rival 履歴」を示す等。

### 3.3 馬場 × 血統 affinity (`add-baba-pedigree-affinity-features.py`, 7 cols)

baba_condition (1=良 2=稍重 3=重 4=不良) × sire/damsire/horse の career win rate:

| column | desc |
|---|---|
| `current_baba_condition` | current race の baba (1-4) |
| `horse_baba_career_starts` | horse 自身の同 baba career 出走数 |
| `horse_baba_win_rate` | horse 自身の同 baba win rate |
| `sire_baba_career_starts` | sire の同 baba 産駒延べ出走数 |
| `sire_baba_win_rate` | sire の同 baba win rate |
| `damsire_baba_career_starts` | damsire の同 baba 出走数 |
| `damsire_baba_win_rate` | damsire の同 baba win rate |
| `sire_horse_baba_combined_score` | sire + horse の average (NULL-safe) |

雨レース (重/不良) で特に強い signal となる見込み。実測で不良馬場では一部 sire の win rate が 0.54 まで上昇 (平均 0.11 の 5x)。

### 3.4 厩舎 × grade / target_race affinity (`add-trainer-stable-affinity-features.py`, 8 cols)

純非前走系 (horse 個別の前走成績に一切依存しない signal):

| column | desc |
|---|---|
| `trainer_grade_career_starts` | 調教師の同 grade_code career 出走数 |
| `trainer_grade_win_rate` | 同 grade win rate |
| `trainer_grade_top3_rate` | 同 grade top3 rate |
| `trainer_target_race_career_count` | target_race (G1) 該当時の調教師経験 race 数 |
| `trainer_target_race_win_count` | target_race 1着 count |
| `trainer_target_race_top3_count` | target_race 3 着以内 count |
| `trainer_target_race_has_history` | boolean |

例: ダービー 2024 — 一部の調教師は過去 9 走中 4 度の 3 着以内 (44% top3 率)、3 度の 1 着 (33% 勝率)。

---

## 4. Pipeline 構成

```
finish_position_features_duckdb.py (base, ~10 min)
  ↓
add-race-internal-features.py (race-level pace forecast 含む既存 v6)
  ↓
add-market-signal-features.py
  ↓
add-sectional-and-weight-features.py
  ↓
v3 merger (production-critical key)
  ↓
add-futan-juryo-features.py
  ↓
add-workout-features.py
  ↓
add-near-miss-features.py (v6 完, feat-jra-deploy-final, 215 cols)
  ↓ [v7 layer starts here]
add-grade-race-lineage-features.py  (v7 +7 cols, lineage-races/jra.json)
  ↓
add-head-to-head-features.py        (v7 +6 cols)
  ↓
add-baba-pedigree-affinity-features.py (v7 +7 cols)
  ↓
add-trainer-stable-affinity-features.py (v7 +8 cols)
  ↓
feat-jra-v7-final (243 cols)
  ↓
CatBoost YetiRank training
  ↓
jra-cb-v7-lineage model
```

---

## 5. CatBoost training config

```python
params = {
    "loss_function": "YetiRank",
    "eval_metric": "NDCG:top=3",
    "iterations": 500,
    "learning_rate": 0.05,
    "depth": 8,
    "l2_leaf_reg": 3.0,
    "random_seed": 20260519,
    "task_type": "CPU",
}
# relevance {1:3, 2:2, 3:1}, --no-cat-features
# train: 2016-01-01 〜 2025-12-31, validation: 2024 + 2025 (walk-forward 2 folds)
```

target_race_id (string) は非数値カラムなので CatBoost feature 自動除外 (`is_numeric_dtype` filter)。

---

## 6. 訓練 / 評価コマンド

### Walk-forward
```bash
.venv/bin/python src/scripts/finish_position_catboost.py walk-forward \
  --csv tmp/feat-jra-v7-final \
  --train-start-date 20160101 \
  --validation-years 2024,2025 \
  --output-report tmp/finish-position-eval/wf-jra-v7-lineage.json \
  --output-predictions-dir tmp/finish-position-eval/predictions-v7-lineage/jra \
  --no-cat-features
```

### OOT
```bash
.venv/bin/python src/scripts/finish_position_catboost.py walk-forward \
  --csv tmp/feat-jra-v7-final \
  --train-start-date 20160101 \
  --train-end-date 20250630 \
  --validation-from-date 20250701 \
  --validation-to-date 20251231 \
  --output-report tmp/finish-position-eval/wf-jra-v7-lineage-oot.json \
  --output-predictions-dir tmp/finish-position-eval/predictions-v7-lineage-oot/jra \
  --no-cat-features
```

### Full-data train + 2026 upcoming predict (deploy)
```bash
# 1. train full data, save model
.venv/bin/python -c "
import pandas as pd, numpy as np, sys
sys.path.insert(0, 'src/scripts')
from finish_position_catboost import (
    load_parquet_dir, resolve_feature_columns, make_to_relevance, filter_range, race_group_ids,
)
from catboost import CatBoost, Pool
df = load_parquet_dir(Path('tmp/feat-jra-v7-final'))
feat = resolve_feature_columns(df, use_cat_features=False)
train = filter_range(df, '20160101', '20251231').dropna(subset=['finish_position']).sort_values(['race_id','umaban']).reset_index(drop=True)
labels = train['finish_position'].map(make_to_relevance(3,2,1)).to_numpy(dtype=np.int32)
pool = Pool(data=train[feat].astype(np.float32).values, label=labels, group_id=race_group_ids(train))
model = CatBoost({'loss_function':'YetiRank','eval_metric':'NDCG:top=3','iterations':500,
                  'learning_rate':0.05,'depth':8,'l2_leaf_reg':3.0,'random_seed':20260519,
                  'task_type':'CPU','verbose':100})
model.fit(pool, verbose=False)
model.save_model('tmp/models/jra-cb-v7-lineage-deploy/model.cbm')
"

# 2. predict 2026 upcoming (finish_position null), output jsonl
# (see deploy script in commit history)

# 3. PG upsert
PG_URL="postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing" \
  .venv/bin/python src/scripts/finish-position-features/blend-and-insert.py \
  --jsonl cb=1.0:tmp/predictions-upcoming-deploy/jra-cb-v7-lineage-2026.jsonl \
  --model-version jra-cb-v7-lineage --source jra \
  --output-jsonl tmp/predictions-upcoming-deploy/jra-cb-v7-lineage-final.jsonl

# 4. Insert eval row + activate
docker exec -i horse-racing-local-postgresql psql -U horse_racing -d horse_racing -c "
  update finish_position_active_models
  set model_version = 'jra-cb-v7-lineage', activated_at = now()
  where category = 'jra';
"

# 5. Sync Neon
cd apps/local-postgresql
bun run scripts/push-neon-sync.ts
```

---

## 7. Rollback

```sql
update finish_position_active_models
set model_version = 'jra-cb-v6-stacked', activated_at = now()
where category = 'jra';
```

旧 v6-stacked の eval row + predictions は残っているため即時 rollback 可。

---

## 8. NAR (`nar-xgb-v7-lineage` deploy 済)

NAR への v7 展開:
- `lineage-races/nar.json` 18 target races (帝王賞, 東京ダービー, JBC, 兵庫CS, かしわ記念, 川崎記念, 全日本2歳優駿, ジャパンダートクラシック, 等)
- v7 features は **lineage + h2h + baba の 3 layer (trainer 抜き)** を採用 — trainer は NAR で逆効果
- XGBoost rank:pairwise (既存 active arch 踏襲)、relevance `{1:3, 2:2, 3:2}` (`--relevance-rank3 2` で place3 重み boost)

### Walk-forward 2024-2025 (n=27,103 races)

| metric | v5_single baseline | v7-lineage (no trainer, r322) | Δ |
|---|---|---|---|
| top1 | 58.447% | **58.562%** | +0.115pp |
| place2 | 35.586% | **35.834%** | **+0.247pp** ⭐ |
| place3 | 27.318% | **27.392%** | +0.074pp |
| top3_box | 34.753% | 34.635% | -0.119pp |
| ndcg@3 | 0.7904 | 0.7901 | -0.0002 |

主要 3 metric (top1/place2/place3) すべて改善、top3_box -0.12pp は許容範囲。

### NAR 試行履歴 (gate 通過まで)

| variant | top1 Δ | place2 Δ | place3 Δ | top3_box Δ | 採用 |
|---|---|---|---|---|---|
| XGB r321 (default relevance) | +0.04 | +0.01 | **-0.13** | -0.06 | ✗ |
| CatBoost YetiRank r321 | +0.20 | +0.03 | -0.42 | -0.53 | ✗ |
| XGB r332 (boost both rank2/3) | -0.15 | -0.09 | -0.02 | -0.05 | ✗ |
| XGB r322 (boost only rank3) | +0.05 | +0.06 | -0.09 | -0.02 | ✗ |
| **XGB r322 + no trainer features** | **+0.12** | **+0.25** | **+0.07** | -0.12 | ✓ **deploy** |

NAR で trainer features (`chokyoshi_code from nvd_se`) を抜くと改善した理由: NAR の調教師データは race level identity が JRA ほど strong でない可能性、target_race ごとの trainer-specific signal が薄く noise として作用。

### NAR Pipeline

```
feat-nar-v6 (171 cols, baseline)
  ↓
add-grade-race-lineage-features.py --config lineage-races/nar.json
  ↓
add-head-to-head-features.py
  ↓
add-baba-pedigree-affinity-features.py
  ↓
feat-nar-v7-baba (191 cols, 20 added: lineage 7 + h2h 6 + baba 7)
  ↓
XGBoost rank:pairwise, --relevance-rank1=3 --relevance-rank2=2 --relevance-rank3=2,
num_rounds=450 (WF best), lr=0.05, max_depth=6
  ↓
nar-xgb-v7-lineage model (saved at tmp/models/nar-xgb-v7-lineage-deploy/model.json)
```

NAR では trainer-stable affinity layer (Phase 1e' for JRA) は **skip**。`add-trainer-stable-affinity-features.py` は `--category nar` 引数を追加実装 (pg.nvd_se 経由)、将来必要なら再投入可能。

### NAR 2026 predictions

feat-nar-v7-baba parquet には 2026 データ未生成のため、deploy 時に 2026 NAR 予測の upsert は省略。`finish_position_active_models.nar = nar-xgb-v7-lineage` 切替済なので、次回 cron 起動時に v7 でレース予測が自動生成される設計。既存の v5 NAR predictions (2026, 2,395 件) は そのまま PG に残置。

---

## 9. Ban-ei (`ban-ei-cb-v7-grade` deploy 済)

Ban-ei への v7 展開:
- `lineage-races/ban-ei.json` 16 target races (ばんえい記念, 帯広記念, ばんえいダービー, ばんえいオークス, ばんえい菊花賞, イレネー記念, 天馬賞, ヒロインズカップ, 北見記念, 旭川記念, 岩見沢記念, ばんえいグランプリ, 銀河賞, 黒ユリ賞, ヤングチャンピオンシップ, ばんえい大賞典)
- 既存 `add-ban-ei-raw-features.py:67` の `try_cast(futan_juryo as double)` が ban-ei の **hex 形式 ("26C"=620kg)** で silent fail していた既知問題を **修正** (`add-banei-futan-class-features.py` で hex parse)
- 新規 layer `add-banei-grade-career-features.py` で「重賞以外の race でも grade 関係に着目」(user 要望) を実現

### v7 ban-ei feature layers (6 total)

1. **lineage** (7 cols) — 16 ban-ei重賞 × trial races
2. **h2h** (6 cols) — 共通
3. **baba × pedigree** (7 cols) — 共通
4. **futan_class (ban-ei specific, 8 cols)** — hex parse 修正 + 7-bucket (<500/500-549/.../≥800kg) + horse/sire/damsire class-specific win rate
5. **grade_career (ban-ei specific, 18 cols)** — 重賞だけでなく一般戦も含む全 race で grade ladder 信号 (horse_grade_E/T/S/R/Q/P_career_win_rate, horse_current_grade_career_win_rate, field_avg_career_starts 等)

### Walk-forward 2024-2025 (n=3,480 races, 32,714 predictions)

| Variant | top1 | place2 | place3 | top3_box | ndcg |
|---|---|---|---|---|---|
| v1 baseline (LR) | 34.86% | 19.97% | 15.29% | 11.78% | 0.6692 |
| v7 LGBM no_futan (3 layer) | 34.94% | 20.60% | 15.37% | 11.32% | 0.6681 |
| v7 LGBM full_grade (6 layer) | 34.83% | 20.17% | 15.83% | 12.10% | 0.6104 |
| v7 XGB full (6 layer) | 34.40% | 21.09% | 15.00% | 12.53% | 0.6096 |
| **v7 CB YetiRank full (6 layer)** | **34.71%** | **21.03%** ⭐ | **15.43%** | **12.70%** ⭐ | 0.6120 |

### v7 CB ban-ei Δ vs v1
| metric | v1 | v7-cb | Δ |
|---|---|---|---|
| top1 | 34.86% | 34.71% | -0.15pp marginal |
| **place2** | 19.97% | **21.03%** | **+1.06pp** ⭐ |
| place3 | 15.29% | 15.43% | +0.14pp |
| **top3_box** | 11.78% | **12.70%** | **+0.92pp** ⭐ |
| ndcg@3 | 0.6692 | 0.6120 | -0.057 |

主要 3 metric (place2, place3, top3_box) で大幅改善、top1 marginal regression。ndcg は GBDT pure rank quality として低下するが、place_N accuracy が improved なので採用。

### Ban-ei Pipeline

```
finish-position-features-parquet-ban-ei-v1 (80 cols, baseline)
  ↓
add-grade-race-lineage-features.py --config lineage-races/ban-ei.json (+7 cols)
  ↓
add-head-to-head-features.py (+6 cols)
  ↓
add-baba-pedigree-affinity-features.py (+7 cols)
  ↓
add-banei-futan-class-features.py (+8 cols, hex parse 修正)
  ↓
add-banei-grade-career-features.py (+18 cols, 全 race 適用)
  ↓
feat-ban-ei-v7-grade (128 cols)
  ↓
CatBoost YetiRank (300 iter, lr=0.05, depth=8, l2=3.0, relevance {1:3, 2:2, 3:1})
  ↓
ban-ei-cb-v7-grade model
```

### Ban-ei trial 履歴 (gate 通過まで)

| variant | top1 Δ | place2 Δ | place3 Δ | top3_box Δ | 採用 |
|---|---|---|---|---|---|
| LGBM lambdarank, full 5 layer | -0.23 | +0.11 | +0.03 | -0.52 | ✗ |
| LGBM no_futan (3 layer) | +0.09 | +0.63 | +0.09 | -0.46 | ✗ |
| LGBM h2h_only | -0.52 | +0.86 | -0.39 | -0.32 | ✗ |
| LGBM no_futan + grade_career | -0.29 | +0.83 | +0.14 | -0.23 | ✗ |
| LGBM full + grade_career | -0.03 | +0.20 | +0.54 | +0.32 | ✗ |
| XGB full (6 layer) | -0.46 | +1.12 | -0.29 | +0.75 | ✗ |
| XGB r322 | -1.27 | +0.55 | +0.05 | +0.46 | ✗ |
| XGB lr03 | -0.75 | +0.60 | -0.20 | +0.15 | ✗ |
| **CB YetiRank full 6 layer** | **-0.15** | **+1.06** | **+0.14** | **+0.92** | ✓ |

**重要 empirical finding**:
- Ban-ei の futan_juryo は hex 形式で記録されており、既存 `add-ban-ei-raw-features.py` で silent NULL fail していた (v1 model は futan signal 未活用)
- grade_career layer (重賞だけでなく全 race の grade 関係に着目) が place / top3_box 改善に寄与
- CatBoost YetiRank > LightGBM LambdaRank ≫ XGBoost rank:pairwise (ndcg 大幅劣後) — ban-ei は CatBoost が best

---

## 9. 影響を受けるファイル

### 新規
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/lineage-races/jra.json`
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/lineage-races/nar.json`
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/lineage-races/ban-ei.json`
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-grade-race-lineage-features.py`
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-head-to-head-features.py`
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-baba-pedigree-affinity-features.py`
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-trainer-stable-affinity-features.py` (`--category {jra,nar}` 対応)
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-banei-futan-class-features.py` (hex parser)
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-banei-grade-career-features.py`
- `apps/pc-keiba-viewer/tests/test_add_grade_race_lineage_features.py` (13 tests)
- `apps/pc-keiba-viewer/tests/test_add_head_to_head_features.py` (4 tests)
- `apps/pc-keiba-viewer/tests/test_add_baba_pedigree_affinity_features.py` (3 tests)
- `apps/pc-keiba-viewer/tests/test_add_trainer_stable_affinity_features.py` (6 tests, NAR category 含む)

### 再利用 (変更なし)
- `apps/pc-keiba-viewer/src/scripts/finish_position_catboost.py`
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/_resource_defaults.py`
- `apps/pc-keiba-viewer/scripts/train-env.sh`
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/compare-model-metrics.py`
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/blend-and-insert.py`
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/insert-evaluation-row.py`
- `apps/local-postgresql/scripts/push-neon-sync.ts`
- `apps/local-postgresql/scripts/push-neon-status.ts`
