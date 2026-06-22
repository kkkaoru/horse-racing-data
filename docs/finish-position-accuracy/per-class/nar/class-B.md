---
class: B
category: nar
n_races_holdout_2023_26: 7124
baseline_top1: 58.10
baseline_place2: 34.40
baseline_place3: 26.68
active_ensemble: iter12-nar-xgb-hpo-v8 (global fallback)
last_updated: 2026-06-17
---

# NAR B — class file

## Status

Active ensemble: `iter12-nar-xgb-hpo-v8 (global fallback)`.

B is the only NAR sub-class with no dedicated per-class ensemble. All other NAR
sub-classes (NEW, MUKATSU, C, A, OP, other) route through per-class ensembles
registered in `PER_CLASS_MODEL_VERSIONS`. B has no registered entry and
falls back to the category-global iter12 XGBoost model.

---

## Baseline (holdout 2023-2026, measured 2026-06-17)

n = 7,124 races (well-powered, MDE ~1.5pp at 80% power).

Note: the baseline numbers in the ROADMAP.md headroom table (51.41% / 29.00% /
21.09%) reflect an earlier measurement on a different window or methodology.
The canonical holdout 2023-2026 baseline measured here is:

| Metric     | iter12 fallback |
| ---------- | --------------- |
| top1       | 58.10%          |
| place2     | 34.40%          |
| place3     | 26.68%          |
| fukusho_2p | 86.97%          |
| top3_box   | 32.55%          |

---

## Experiment 1: Dedicated CatBoost YetiRank Residual (iter30 recipe)

**Date**: 2026-06-17
**Verdict**: ABORT

### Recipe

Follows the canonical iter30 NAR per-class residual recipe, identical to the
accepted members for NEW / MUKATSU / C / A / OP / other:

- **Base**: `iter12-nar-xgb-hpo-v8` predicted score used as an anchor feature
  (`iter12_score` column) — leak-free, from per-fold predictions.
- **Residual model**: CatBoost YetiRank
  - Depth: 4, LR: 0.1, L2: 5.0, iterations: 500, early-stop: 30
  - Train rows: B-class inclusion chain (B + C + MUKATSU + NEW)
  - Validation rows: B-class ONLY (for each fold)
  - Feature count: 174 (173 NAR iter26-relationships numeric cols + 1 iter12_score)
  - Time decay sample weights (min 0.5, linear ramp to 1.0 over years)
- **Artifacts**: `tmp/bucket-eval/finish-position/iter30-nar-cb-residual-B-v8/`
  and `apps/finish-position-predict-container/models/finish-position/nar/per-class/B/iter30-nar-cb-residual-B-v8/`
- **Full WF**: 20 folds (2007-2026), all completed.

### Blend optimization

Inner tuning (validation 2018-2022): grid search over alpha ∈ [0.0, 1.0] in 0.05
steps, maximizing top1 on B-class rows.

```
best_alpha = 0.50  (w_base = 0.50, w_residual = 0.50)
val_top1   = 56.31%  (alpha=1.0 pure-baseline: 56.25%)
```

The alpha grid shows essentially flat validation top1 across all blend ratios
(range: 56.20% to 56.31%). This is an early diagnostic of no signal in the
residual — the optimizer assigns alpha=0.5 but the gain is < 0.07pp over the
pure baseline.

### Holdout evaluation (one-shot, 2023-2026, B-class only, n=7,124 races)

| Metric     | iter12 fallback | WITH residual | Delta   | LB95    | UB95    |
| ---------- | --------------- | ------------- | ------- | ------- | ------- |
| top1       | 58.10%          | 58.18%        | +0.08pp | -0.14pp | +0.31pp |
| place2     | 34.40%          | 34.62%        | +0.21pp | -0.10pp | +0.52pp |
| place3     | 26.68%          | 26.52%        | -0.17pp | -0.49pp | +0.15pp |
| fukusho_2p | 86.97%          | 87.10%        | +0.13pp | -0.06pp | +0.31pp |
| top3_box   | 32.55%          | 32.37%        | -0.18pp | -0.44pp | +0.07pp |

Paired bootstrap: 10,000 iterations, seed=42.

### Accept gate

Gate (relaxed per-class rule from ROADMAP §4 / per task spec):

> ADOPT iff top1 or any place metric improves (LB95 ≥ 0 robust) AND
> no primary {top1, place2, place3} regression beyond −0.05pp.

| Gate criterion                       | Result                                        |
| ------------------------------------ | --------------------------------------------- |
| LB95 ≥ 0 on top1 OR place2 OR place3 | FAIL (top1=-0.14, place2=-0.10, place3=-0.49) |
| No primary regression > −0.05pp      | FAIL (place3 = −0.17pp, top3_box = −0.18pp)   |
| Positive axes                        | 3/5 (top1, place2, fukusho_2p positive)       |

**Verdict: ABORT**

All three primary metrics fail the LB95 ≥ 0 robustness check. Point estimates
are marginal (top1 +0.08pp, place2 +0.21pp) but not reliable — lower bounds
are negative, and place3 and top3_box are directionally negative. The residual
adds no robust signal beyond what the iter12 global model already captures for
B-class races.

### Root-cause analysis

The iter30 residual recipe works for NEW/MUKATSU/C/A/OP/other because those
classes have more distributional specificity relative to the global model's
training distribution. B is the largest class in the inclusion chain (after C),
which means:

1. The global iter12 model already has substantial B-class representation during
   training. The residual's "chain" training (B + C + MUKATSU + NEW rows) does
   not substantially differ from iter12's effective training distribution for B.
2. The alpha grid scan (val top1 effectively flat 56.20-56.31% across all blend
   ratios) confirms the residual predictions are nearly collinear with the
   baseline — there is no complementary variance to capture.

This is the same saturation pattern observed for NAR in the D-phase and
confirmed again here: GBDT already captures the B-class structure from the
global training set. The inclusion-chain residual cannot learn anything new
when the class is already well-represented in the global training data.

---

## Evaluation Log

| Date       | Hypothesis                                 | Method                | Verdict | Ref                                         |
| ---------- | ------------------------------------------ | --------------------- | ------- | ------------------------------------------- |
| 2026-06-17 | CatBoost YetiRank residual (iter30 recipe) | ML per-class residual | ABORT   | `tmp/nar_b_class_residual_eval_result.json` |

---

## Next steps for B-class

The only untried NAR per-class method that could differ from the above:

1. **Different feature set**: If B-class horses have specific signals not in the
   173-column NAR iter26-relationships feature store (e.g., B-class-specific
   recent-race patterns), a new feature targeting B-class dynamics could open
   headroom. Requires partial-ρ probe first.
2. **Full class-specific retrain**: Rather than a residual on top of iter12,
   train a completely separate model only on B-class rows (no inclusion chain).
   Risk: less training data; but removes the iter12 baseline signal as a
   confound in the residual's learning objective.
3. **Saturation confirmed**: If no new B-class-specific signal is available,
   the conclusion is that iter12 already handles B-class optimally and the
   class is at the empirical frontier.
