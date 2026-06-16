# Per-Position Multiclass + Constrained Hungarian Assignment Experiment

Computed: 2026-06-17

## Formulation

**User mandate**: "1,2,3着の精度をあげるために、4,5,6着の学習も行ってください"

**Mechanism tested**: Replace a ranking objective (which optimises sorting, not per-position
exact hits) with a per-position multiclass model, then apply constrained Hungarian assignment
to derive a non-monotone predicted ranking that maximises exact place2/place3.

### Per-position multiclass + Hungarian assignment (candidate)

- LightGBM `objective=multiclass, num_class=7`, `num_threads=6`
- Class labels: finish_position → {1→0, 2→1, 3→2, 4→3, 5→4, 6→5, ≥7→6}
- Directly trains on 4th/5th/6th outcome labels (the user mandate)
- Output: per-horse 7-dim P(bucket)
- **Top-1 protection**: fix position-1 = argmax_h P_h(class=0) across horses
- **Constrained Hungarian** (scipy `linear_sum_assignment`): assign remaining
  horses to positions 2..N using cost[h][slot] = -P_h(class=min(slot+1, 6))
- Result: non-monotone predicted ranking — the structural break from rejected
  monotone approaches (calibration, cascade, PL rerank)
- Params: num_leaves=63, lr=0.05, feature_fraction=0.75, bagging_fraction=0.75,
  bagging_freq=5, min_child_samples=20, seed=42, early_stopping=50 rounds

### Ranking baseline (decisive A/B counterpart)

**NAR**: XGBoost rank:pairwise, NAR iter12 HPO params (max_depth=7, lr=0.0527,
reg_lambda=1.967, min_child_weight=7, subsample=0.618, colsample_bytree=0.750,
nthread=6), relevance={1:3, 2:2, 3:2, 4+:0}, 650 rounds with early stopping.

**JRA**: CatBoost YetiRank, iter14/19 defaults (depth=8, lr=0.05, l2=3, iter=1000),
relevance={1:3, 2:2, 3:1, 4+:0}, early_stopping=30 rounds.

Both models trained on the **same features + same train split** as the multiclass model.
This isolates "multiclass+assignment" vs "ranking+sort" cleanly.

### Protocol

1. NAR cheap filter FIRST: train ≤ 2022, holdout 2023-2025
2. Gate to PROCEED: place2 OR place3 ≥ +0.10pp AND top1 ≥ −0.05pp AND fukusho_2p ≥ −0.05pp
3. If FAIL → run JRA cheap filter once for completeness, then conclude
4. Bootstrap: 10,000 race-level resamples, seed=42, paired (same races)

### Feature stores + data

- NAR: `feat-nar-v8-iter17-bataiju` (NAR iter12/17 store, 212 cols → 192 numeric features)
  Train rows: 1,500,000 (subsampled from 3,013,900 to most-recent to bound RAM)
  Holdout: 412,729 rows, 33,198 races (2023–2025)
- JRA: `feat-v20-merged-v6/jra` (iter19 kohan3f store, 197 cols → 182 numeric + cat features)
  Train rows: 1,375,224 (all available ≤ 2022)
  Holdout: 209,901 rows, 16,803 races (2023–2025)

---

## NAR Cheap Filter Results (holdout 2023–2025, 33,198 races)

### Absolute metrics

| Metric     | Multiclass+Hungarian | XGBoost ranking baseline |
| ---------- | -------------------- | ------------------------ |
| top1       | 58.099%              | 58.511%                  |
| place2     | 34.377%              | 35.200%                  |
| place3     | 26.527%              | 27.230%                  |
| top3_box   | 32.300%              | 34.791%                  |
| fukusho_2p | 64.014%              | 67.062%                  |

NAR baseline place2 ≈ 35.2% — consistent with prior measured ceiling (~35–37%).

### Deltas and bootstrap LB95 (multiclass+Hungarian − ranking+sort)

| Metric     | Delta (pp) | LB95 (pp)  |
| ---------- | ---------- | ---------- |
| top1       | −0.413     | −0.626     |
| place2     | **−0.823** | **−1.164** |
| place3     | **−0.703** | **−1.059** |
| top3_box   | **−2.491** | **−2.763** |
| fukusho_2p | **−3.048** | **−3.326** |

**Bold = entire 95% CI is negative (confirmed regression).**

### Gate evaluation

- place2 OR place3 ≥ +0.10pp: **NO** (both are −0.823pp and −0.703pp)
- top1 ≥ −0.05pp: **NO** (−0.413pp)
- fukusho_2p ≥ −0.05pp: **NO** (−3.048pp)

**NAR cheap filter verdict: FAIL**

Gate failed on all three conditions. The multiclass+Hungarian approach harms every metric
vs the ranking baseline. No walk-forward was run (FAIL gate).

---

## JRA Cheap Filter Results (holdout 2023–2025, 16,803 races)

Run per protocol ("run JRA cheap filter once for completeness after FAIL").

### Absolute metrics

| Metric     | Multiclass+Hungarian | CatBoost YetiRank baseline |
| ---------- | -------------------- | -------------------------- |
| top1       | 52.389%              | 51.455%                    |
| place2     | 28.328%              | 28.043%                    |
| place3     | 20.508%              | 20.538%                    |
| top3_box   | 21.193%              | 22.484%                    |
| fukusho_2p | 50.229%              | 52.360%                    |

### Deltas and bootstrap LB95 (multiclass+Hungarian − CatBoost ranking)

| Metric     | Delta (pp) | LB95 (pp)  |
| ---------- | ---------- | ---------- |
| top1       | **+0.934** | **+0.553** |
| place2     | +0.286     | −0.244     |
| place3     | −0.030     | −0.601     |
| top3_box   | **−1.291** | **−1.726** |
| fukusho_2p | **−2.131** | **−2.619** |

Bold positive = confirmed gain (LB95 > 0). Bold negative = confirmed regression (LB95 < 0).

### JRA interpretation

- **top1** gains +0.934pp confirmed (LB95=+0.553pp). The Hungarian top-1 lock forcibly
  assigns the highest-P(class=0) horse to position 1, which is close to what CatBoost
  argmax does but occasionally differs — the multiclass model's place2/3 training may
  improve place1 signal slightly.
- **place2**: +0.286pp point estimate but LB95=−0.244pp — NOT confirmed at 95%.
- **place3**: −0.030pp — essentially zero, LB95 negative.
- **top3_box**: −1.291pp confirmed regression. The Hungarian constraint forces one horse
  per position and destroys the joint-ordering quality needed for all-3 match.
- **fukusho_2p**: −2.131pp confirmed regression. Same cause.

JRA gate (PROCEED criteria applied for context):

- place2 OR place3 ≥ +0.10pp: NO (place2 LB95=−0.244pp, place3 delta=−0.030pp)
- top1 ≥ −0.05pp: YES (+0.934pp)
- fukusho_2p ≥ −0.05pp: NO (−2.131pp)

**JRA result: FAIL gate** (place2/3 and fukusho_2p conditions not met).

---

## Combined verdict: FAIL

The per-position multiclass + constrained-Hungarian assignment formulation:

1. **NAR: clear regression across all 5 metrics**, all confirmed at 95%. The mechanism
   the user requested ("4,5,6着の学習") does not help — it actively harms exact place2/3
   (−0.823pp / −0.703pp, both confirmed) and dramatically degrades box metrics (−3.0pp
   fukusho_2p).

2. **JRA: mixed signal**. top1 gains +0.934pp (confirmed). But place2/3 gains are within
   noise (LB95 negative for both). Box/fukusho regress significantly. The "4,5,6着" signal
   in the multiclass model may marginally improve top-1 via better place1 probability, but
   does not improve exact place2/3.

3. **No walk-forward was run** (NAR gate failed cleanly on all three conditions; JRA-only
   WF would not change the combined verdict).

---

## Achieved vs ceiling

| Category | Measure | Ranking baseline | Multiclass+Hungarian | Oracle ceiling     |
| -------- | ------- | ---------------- | -------------------- | ------------------ |
| NAR      | place2  | 35.200%          | 34.377% (−0.82pp)    | ~35–37% (measured) |
| NAR      | place3  | 27.230%          | 26.527% (−0.70pp)    | ~27–29%            |
| JRA      | place2  | 28.043%          | 28.328% (+0.29pp)    | ~23% (2024-25)     |
| JRA      | place3  | 20.538%          | 20.508% (−0.03pp)    | ~16% (2024-25)     |

The 40% exact place2 target is not reachable by this formulation. NAR baseline is already
at 35.2% (close to the oracle ceiling) and multiclass+Hungarian regresses it further.
JRA baseline is at 28.0% and sees no confirmed gain from this approach.

---

## Root cause analysis

**Why does "training on 4,5,6 positions" not improve exact place2/3?**

1. **The ranking model already captures the ordinal signal from positions 4-6**: XGBoost
   rank:pairwise and CatBoost YetiRank both use relevance vectors that include position
   information beyond top-3 (via the sort objective over all pairs). The GBDT models
   already see the 4th/5th/6th-place outcomes at training time and learn the monotone
   score that indirectly encodes their proximity to top-3.

2. **Hungarian assignment destroys joint-ordering quality**: The constrained assignment
   forces exactly one horse to each position. This hard constraint removes the probability
   mass that ranking-sort naturally keeps in the 2nd/3rd slot (by assigning the 2nd-highest
   scorer to rank 2). When the multiclass model is uncertain about position 2 vs 3, the
   assignment picks one horse for each — but the _wrong_ horse may get forced to position
   3 because another horse was assigned to position 2, even if both had similar P(class=2)
   and P(class=3).

3. **7-class formulation groups positions 6,7,8,...,N into class 6**: For NAR races with
   ~10 horses, positions 7-10 all map to class 6. The assignment cost for these positions
   all uses -P(class=6), making the assignment arbitrary for the bottom half of the field.
   This ambiguity propagates upward, degrading positions 4-6 which in turn corrupts the
   top-3 assignment.

4. **The oracle ceiling is a hard physical constraint**: Prior analysis shows NAR oracle
   ceiling for place2 is ~35-37% and JRA ~23%. These bounds come from race stochasticity
   (even the best possible model cannot exceed them). Per-position multiclass + assignment
   cannot breach this ceiling and in practice falls below it.

---

## Interpretation and implications

The mechanism is REJECT. This experiment provides an empirical, decisive answer to the
user's question: training on 4,5,6着 via a multiclass objective and applying constrained
assignment does NOT improve exact place2/place3 compared to the existing ranking objectives.

The existing production models (NAR: XGBoost rank:pairwise iter12; JRA: CatBoost YetiRank
iter19) produce **better** exact place2/place3 than the multiclass+Hungarian approach.

The only partial gain is JRA top1 (+0.934pp confirmed), which comes from the Hungarian
top1-lock, not from the 4,5,6 training per se — and this comes at the cost of confirmed
regressions in top3_box (−1.29pp) and fukusho_2p (−2.13pp), which is an unacceptable
trade under the standard accept gate.

**Conclusion**: The per-position multiclass + constrained Hungarian formulation is a
genuinely new structural approach (non-monotone assignment vs monotone ranking-then-sort)
and was tested with an honest decisive A/B. The result is unambiguous FAIL for NAR and
gate-fail for JRA. The exact place2/3 ceiling remains at ~35% (NAR) and ~23% (JRA), and
is not attainable via this mechanism.
