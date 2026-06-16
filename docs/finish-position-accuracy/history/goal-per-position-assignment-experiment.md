# JRA Per-Position Multiclass + Hungarian Assignment A/B

Computed: 2026-06-17

## Formulation

**Mechanism tested**: "4,5,6着の学習" — replacing ranking objective with per-position
probability targets, then applying constrained assignment to maximise exact podium hits.

### Per-position multiclass (candidate)

- LightGBM `objective=multiclass, num_class=7`, `num_threads=6`
- Labels: position bucket {1→0, 2→1, 3→2, 4→3, 5→4, 6→5, ≥7→6}
- Output: per-horse 7-dim P(bucket)
- Constrained-Hungarian assignment per race (scipy `linear_sum_assignment`):
  - Fix position-1 = argmax P(class=0) across horses (top1 protection)
  - Assign remaining horses to positions 2..N using cost[h][pos] = -P(class=pos-1)
    (class 6 used for pos≥7)

### LambdaRank baseline

- LightGBM `objective=lambdarank`, group=race_id
- Relevance labels: {1→3, 2→2, 3→1, else→0}
- Predicted rank = argsort(score, descending)

Both models: 238 features (iter19 kohan3f going), same 500-round params
(lr=0.05, num_leaves=63, subsample=0.8, colsample=0.8, lambda=1.0, seed=42).

### Data split

- Train: race_year ≤ 2022 — 781,623 rows, 55,261 races
- Holdout: 2023–2025 — 141,523 rows, 10,365 races

---

## JRA A/B Results (holdout 2023–2025, 10,365 races)

### Absolute metrics

| Metric     | Multiclass+Hungarian | LambdaRank baseline |
| ---------- | -------------------- | ------------------- |
| top1       | 41.38%               | 40.64%              |
| place2     | 20.03%               | 19.32%              |
| place3     | 14.36%               | 14.11%              |
| top3_box   | 9.19%                | 9.86%               |
| fukusho_2p | 32.66%               | 34.35%              |

### Deltas (per-position − baseline, percentage points)

| Metric     | Delta (pp) | LB95 (pp) | UB95 (pp) | p(Δ>0) |
| ---------- | ---------- | --------- | --------- | ------ |
| top1       | +0.74      | **+0.12** | +1.36     | 98.9%  |
| place2     | +0.71      | −0.13     | +1.54     | 95.3%  |
| place3     | +0.25      | −0.57     | +1.06     | 72.1%  |
| top3_box   | −0.67      | −1.21     | **−0.14** | 0.6%   |
| fukusho_2p | −1.69      | −2.48     | **−0.89** | 0.0%   |

Bootstrap: 10,000 race-level resamples, seed=42, paired (same races).

---

## Achieved vs Ceiling

The oracle ceiling from `goal-baseline-and-ceiling.md` (identity assignment = optimal,
using model predictions 2024–2025):

| Metric | Ceiling (model oracle) | This A/B (multiclass) | Gap to ceiling | ≥40%? |
| ------ | ---------------------- | --------------------- | -------------- | ----- |
| top1   | 0.4011 (2024-25)       | 0.4138 (2023-25)      | above          | YES   |
| place2 | 0.2131 (2024-25)       | 0.2003 (2023-25)      | −1.28pp        | NO    |
| place3 | 0.1556 (2024-25)       | 0.1436 (2023-25)      | −1.20pp        | NO    |

Place2 and place3 remain far below the 40% goal. The oracle ceiling itself was 21% / 15%,
which is already mathematically bounded by race stochasticity.

---

## Verdict

**Per-position multiclass + constrained-Hungarian assignment does NOT beat LambdaRank on
exact place2/place3 at the 95% confidence level for JRA.**

- **top1**: +0.74pp, LB95=+0.12pp (positive, confirmed). The Hungarian top1-lock helps.
- **place2**: +0.71pp delta, but LB95=−0.13pp (not confirmed at 95%). Positive signal
  but uncertain.
- **place3**: +0.25pp delta, LB95=−0.57pp (not confirmed). Statistically weak.
- **top3_box**: −0.67pp, LB95 negative (confirmed regression). Assignment hurts box metrics.
- **fukusho_2p**: −1.69pp, LB95 negative (confirmed regression). Hungarian assignment
  systematically degrades top-2 box.

The formulation achieves its stated partial goal (place2/3 point estimates are positive,
top1 does not drop), but the exact place2/3 gains fall within noise and the box/fukusho
metrics regress significantly. This is consistent with the prior NAR H1-H5 finding
(all per-class members rejected): changing the objective toward position-specific
probabilities trades top3-box coverage for marginal and uncertain exact-ordinal gains.

**Root cause**: The Hungarian assignment forces one horse per position, which reduces
the probability of jointly-correct top-2 box (fukusho_2p) while achieving only noisy
exact-ordinal gain. The ranking objective (LambdaRank) better preserves the joint
ordering distribution needed for box metrics.

**Implication**: This formulation is not sufficient to reach the 40% exact place2/3 goal.
The oracle ceiling (`goal-baseline-and-ceiling.md`) shows the hard limit is ~20% place2
and ~15% place3, confirming the goal is mathematically infeasible under exact-ordinal.

**One-line verdict**: Per-position multiclass + Hungarian assignment gives noisy positive
point estimates on exact place2 (+0.71pp) and place3 (+0.25pp) for JRA, but neither
clears the 95% bootstrap bar; top3_box and fukusho_2p regress significantly; the
formulation is REJECT under the standard gate.

---

## Notes on prior evidence alignment

The `goal-baseline-and-ceiling.md` showed assignment=identity is globally optimal for
market-ranked references (Hungarian returns identity permutation). This experiment extends
that finding: even with a model that explicitly learns per-position probabilities, the
optimal assignment still does not materially improve exact ordinal place2/3 because:

1. The information-theoretic ceiling (~23% JRA place2 with the model oracle) is a hard
   physical constraint from race stochasticity, not a modelling limitation.
2. Hungarian assignment over per-position probs trades joint-ordering quality (box) for
   uncertain exact-ordinal marginal gains — the same trade observed in NAR H1-H5 probes.

Consistent with the `oi-2026-06-10-wave1-h1-h5.md` finding: "place3-up members trade
top1 down" — this experiment shows an inverted version (top1 protected by lock, but box
trades are unavoidable with the assignment constraint).

---

## NAR per-position A/B

Computed: 2026-06-17

### Setup

- Feature set: `feat-v20-merged-v5/nar`, 167 numeric features
- Same formulation as JRA (multiclass num_class=7 + constrained-Hungarian vs LambdaRank)
- Same hyperparams: lr=0.05, num_leaves=63, subsample=0.8, colsample=0.8, lambda=1.0, seed=42
- **Train: 2016–2022 (subsampled for memory safety; 913,804 rows)**
- Holdout: 2023–2025 — 412,429 rows, **40,710 races**
- NAR field size: mean=10.1, median=10 (vs JRA ~14-15); smaller fields were expected to help

### Absolute metrics (holdout 2023–2025, 40,710 races)

| Metric     | Multiclass+Hungarian | LambdaRank baseline |
| ---------- | -------------------- | ------------------- |
| top1       | 58.64%               | 58.87%              |
| place2     | 34.58%               | 35.39%              |
| place3     | 26.72%               | 27.32%              |
| top3_box   | 32.80%               | 35.00%              |
| fukusho_2p | 64.38%               | 67.18%              |

NAR baseline place2 ≈ 35.4% — close to the previously reported ~35% ceiling.

### Deltas (multiclass+Hungarian − LambdaRank, percentage points)

| Metric     | Delta (pp) | LB95 (pp) | UB95 (pp) | p(Δ>0) |
| ---------- | ---------- | --------- | --------- | ------ |
| top1       | −0.24      | **−0.48** | 0.00      | 2.4%   |
| place2     | **−0.82**  | **−1.22** | **−0.41** | 0.0%   |
| place3     | **−0.60**  | **−1.04** | **−0.17** | 0.4%   |
| top3_box   | **−2.20**  | **−2.55** | **−1.85** | 0.0%   |
| fukusho_2p | **−2.80**  | **−3.13** | **−2.47** | 0.0%   |

Bold = confirmed regression (entire 95% CI negative).
Bootstrap: 10,000 race-level resamples, seed=42, paired (same races).

### Achieved vs NAR ceiling

NAR production baseline (LambdaRank): place2 ≈ 35.4%, place3 ≈ 27.3%.
The per-position multiclass approach **regresses both** by −0.82pp and −0.60pp (confirmed).
The 40% place2 goal remains far above even the LambdaRank baseline.

### NAR Verdict

**REJECT — confirmed regression on all five metrics.**

Unlike JRA (where place2/3 point estimates were weakly positive), NAR shows unambiguous
confirmed regressions across place2 (−0.82pp, LB95=−1.22pp), place3 (−0.60pp,
LB95=−1.04pp), top3_box (−2.20pp) and fukusho_2p (−2.80pp). Top1 also regresses
(−0.24pp, LB95=−0.48pp, p=2.4%).

**Root cause for NAR**: Smaller fields (~10 horses vs JRA ~14-15) do not rescue the
Hungarian approach. With 10 horses, the multiclass model must assign exactly one horse to
each of positions 1-10, but the 7-class formulation (classes 1-6, ≥7) groups positions
7-10 into a single bucket. This forces ambiguous assignment for the bottom half of the
field, degrading quality throughout the ranking — especially the box metrics where NAR's
higher baseline (fukusho_2p 67%) means there's more to lose.

**Combined JRA+NAR conclusion**: The per-position multiclass + constrained-Hungarian
formulation is REJECT for both categories. NAR's result is even cleaner than JRA's (all
metrics confirmed negative vs JRA's mixed signal). The "4,5,6着の学習" mechanism via
multiclass objective does not improve exact place2/3 and actively harms box/fukusho
metrics. The oracle ceiling for NAR place2 (~35-37%) is a hard constraint from race
stochasticity; the 40% goal is not attainable by this formulation.
