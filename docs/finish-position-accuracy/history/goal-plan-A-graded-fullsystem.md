# Plan A: Sub-4 Graded Full-System Training to Raise 2着/3着 Accuracy

| Field   | Value                                         |
| ------- | --------------------------------------------- |
| Date    | 2026-06-17                                    |
| Goal    | JRA & NAR place2/place3 >40% while top1 holds |
| Mandate | train with 4th/5th/6th+ ordering information  |
| Status  | PLAN — no training or deploy performed        |

---

## Executive Summary

The user mandate is: train using sub-4 (4th/5th/6th+) ordinal placement signals, not the
current relevance scheme that discards all ordering information past rank 3.

**JRA is the cleanest near-term target.** iter19-jra-cb-kohan3f-going-v8 (244 features,
CatBoost YetiRank) is deployed base-only with NO per-class ensembles — the "WELD" failure
mode that killed the NAR scheme-D full-system judge does not apply. Any relevance scheme
change to JRA feeds directly and cleanly through to production with no downstream blend to
fight.

**NAR requires co-design.** NAR has active per-class residual ensembles (iter30 CB + iter36
LGB) whose blend weights are calibrated to iter12-nar-xgb-hpo-v8's score distribution. The
NAR scheme-D full-system judge (2026-06-13) proved that base swap alone causes consistent
regression across all 4 axes (top1 −0.40pp, place3 −0.43pp, f2p LB95 = −0.00481).
Sub-4 relevance for NAR requires one of three co-designs (ranked by risk in §5).

---

## 1. Production State Reference

| Category | Active model                      | Architecture                         | place2 baseline                                                    | place3 baseline           |
| -------- | --------------------------------- | ------------------------------------ | ------------------------------------------------------------------ | ------------------------- |
| JRA      | iter19-jra-cb-kohan3f-going-v8    | CatBoost YetiRank, 244f              | ~0.2173 (iter14 WF proxy; iter19 holdout: ~0.22)                   | ~0.1624 (iter14 WF proxy) |
| NAR      | iter12-nar-xgb-hpo-v8 + iter30/36 | XGB rank:pairwise + CB/LGB residuals | 0.3529 (holdout 2023-26)                                           | 0.2718 (holdout 2023-26)  |
| Ban-ei   | banei-cb-v7-lineage-wf-21y        | CatBoost YetiRank                    | — (skip, graded schemes all fail cheap filter per §3-experiment-5) | —                         |

**Current relevance schemes:**

- JRA (CB YetiRank): `{1:3, 2:2, 3:1, 4+:0}` — float labels supported, int32 cast in code
- NAR (XGB rank:pairwise): `{1:3, 2:2, 3:2, 4+:0}` — walk-forward trainer hardcodes rank3=2
- Ban-ei (CB YetiRank): `{1:3, 2:2, 3:1, 4+:0}`

---

## 2. Lessons from Prior Art (must not repeat)

| Lesson                                                       | Source                                                              | Implication                                                                                                                                                                                                                                     |
| ------------------------------------------------------------ | ------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| NAR scheme-D base WF ADOPT → full-system REJECT              | `nar-schemeD-deploy-judge.md`                                       | WELD: per-class residuals calibrated to iter12 score distribution. Base swap shifts distribution → ensemble regresses. Any NAR retrain must co-train or replace the per-class layer.                                                            |
| JRA weld is GONE                                             | `iter19-deploy.md`                                                  | iter19 is base-only; no per-class layer exists. Graded relevance retrain has a clean path to production.                                                                                                                                        |
| CatBoost YetiRank rejects large integer label inflation      | `graded-relevance-experiments.md §4, §5`                            | JRA scheme B (7/6/5/4/3/2/1) FAILS cheap filter: top1 −2.23pp. JRA/Banei sensitive to label scale. Only scheme D (epsilon tail 0.1/0.08/0.05/0.02) passes JRA cheap filter — though WF was still REJECT for old iter14 base.                    |
| iter19 is a NEW base vs the failed iter14 graded experiments | `jra-kohan3f-verify.md`                                             | Scheme D was tested against iter14 (241 feats). iter19 has 244 features and significantly higher base accuracy (+0.68pp pooled top1 vs iter14). The new feature mix may change how well sub-4 ordering signal propagates. Re-test is warranted. |
| Adoption gate must be full-system, not base-model alone      | `nar-schemeD-deploy-judge.md`, `g1f1-combined-nar-retrain-judge.md` | For JRA (base-only): WF judge = full-system judge. For NAR: WF judge on new base is INSUFFICIENT — full pipeline must be judged.                                                                                                                |
| NAR scheme D base alone: place3 +0.40pp, f2p LB95 +0.00174   | `graded-relevance-experiments.md §3-C`                              | The signal is real at the base level. The problem is the WELD, not the label scheme. If NAR per-class is co-designed, scheme D has a plausible path.                                                                                            |

---

## 3. Experiments — Ranked by Confidence × Impact

### Experiment 1 (HIGHEST PRIORITY): JRA iter19 + Scheme D (epsilon tail)

**Rationale:** JRA is clean (base-only), and scheme D was the only scheme to pass the JRA
cheap filter in prior art (Δtop1 −0.0028 < −0.05pp veto, Δplace2 +0.0020, Δplace3
+0.0014). The prior WF REJECT was on iter14 (241 feats); iter19 is a different base with 3
new going-conditional features that improved every sub-class. The 244-feature feature set
may produce different scheme-D dynamics. Additionally, JRA now has ZERO per-class WELD
risk — a WF ADOPT is a production ADOPT.

**Relevance scheme (unchanged from prior art):**

```
pos=1 → 3        (unchanged, top-3 preserved)
pos=2 → 2        (unchanged)
pos=3 → 1        (unchanged)
pos=4 → 0.1
pos=5 → 0.08
pos in [6,8] → 0.05
pos in [9,12] → 0.02
pos >= 13 → 0
```

**Framework:** CatBoost YetiRank. Remove `np.int32` cast in `make_to_relevance`; pass
float. Eval metric stays `NDCG:top=3`.

**Training recipe:**

1. Cheap filter: train ≤ 2022, holdout 2023-2025, n≈1.0M JRA rows.
   If cheap filter FAIL (< 2 of 4 axes positive, or place3 < 0), stop.
2. Walk-forward: 3 folds (2023 / 2024 / 2025), paired bootstrap 10k seed=42.
3. Feature store: `feat-jra-v8-iter19-kohan3f-going` (244 features — same store as deployed iter19).
4. Hyperparams: depth=8, lr=0.05, l2_leaf_reg=3.0, iterations=1000, seed=2068 (iter19 defaults).
5. Memory: single train ~5 min on M5 Pro, <4 GB peak. DuckDB not involved in training.

**Accept gate:**

- fukusho_2p LB95 (paired bootstrap, 10k, seed=42) > 0
- ≥ 2 of {top1, place2, place3, top3_box} positive
- ≥ 1 of {place2, place3} positive
- Veto floor: no axis below −0.05pp

**Expected effect on place2/3:** Scheme D showed Δplace2 +0.0020 / Δplace3 +0.0014 in the
JRA cheap filter (iter14 base). With iter19's higher base quality, the signal may be
stronger or weaker — the cheap filter resolves this cheaply before WF.

**Production path if ADOPT:** Single model swap (iter20-jra-cb-scheme-d-v8 or similar),
update `model_meta.py` + feature count. No ensemble rebuild needed.

**Cost:** cheap filter ~6 min, WF if needed ~20 min total. Memory <4 GB.

---

### Experiment 2: JRA iter19 + Scheme D-2 (finer top-6 gradation, place-emphasis)

**Rationale:** The goal is specifically to raise exact_place2 and exact_place3 beyond what
scheme D provides. A fine-grained top-6 scheme rewards the model for correctly ordering 4th
vs 5th vs 6th, not just providing epsilon signal. This is more aggressive than scheme D and
explicitly targets 2nd/3rd ordinal precision via stronger pairwise gradients from 4th-place
horses relative to 5th/6th.

**Relevance scheme (new — not tested before):**

```
pos=1 → 6        (compressed top — reduce distance to pos2)
pos=2 → 5
pos=3 → 4
pos=4 → 3
pos=5 → 2
pos=6 → 1
pos >= 7 → 0
```

This is a field-truncated version of scheme B — the prior scheme B failure (top1 −2.2pp)
used grades {7,6,5,4,3,2,1}. Removing grades 7 and 13+ and capping at 6 may reduce the
gradient shift on the top1 axis while retaining the sub-4 signal. The compression of the
top-3 gap (5→6→4 instead of 7→6→5) is intentional: it nudges the model to care slightly
more about the 2nd/3rd ordinal.

**Framework:** CatBoost YetiRank, same float label path as Experiment 1.

**Training recipe:** Same cheap filter → WF protocol as Experiment 1.

**Cost:** Same as Experiment 1.

**Risk:** This scheme squeezes the top-3 relative gap (each step = 1 unit) which may hurt
top1 discrimination. The cheap filter exits immediately if top1 drops more than −0.05pp
with no compensating place gain. Run AFTER Experiment 1 (not in parallel) to save compute.

---

### Experiment 3: NAR — Joint base + ensemble co-train with scheme D

**Rationale:** The WELD lesson is clear: any NAR base swap that shifts the score distribution
causes residual ensembles to regress. The fix is not a sequential "base swap then ensemble
retrain" (same weld risk at retrain time with stale WF predictions) but rather a **joint
pipeline** that trains the new scheme-D base, generates fresh WF base scores for all years,
then retrains all 6 per-class residuals against those fresh scheme-D base WF scores.

This is Option (a) in the team-lead framing: JOINT base + ensemble co-train.

**Why not Option (b) replace per-class with single graded base?**
The NAR deploy judge for scheme D on base-only is unknown (scheme D was judged as
base-only WF ADOPT; full-system with retrained ensembles was REJECT; base-only without
any ensemble was never separately judged for NAR). At 0.3529 place2 / 0.2718 place3, the
per-class ensembles contribute measurable lift (MUKATSU +1.08pp top1 in scheme-D judge
despite system-level regression). Dropping the ensemble layer risks losing that lift on
small-data sub-classes.

**Why not Option (c) graded base + freshly-refit ensembles?**
This IS the co-train option — same as (a). The distinction from the failed scheme-D
judge is: in the judge, WF base predictions were generated from scheme-D base, then
residuals were re-trained. This IS the right pipeline, and the judge DID run it. The
judge result (REJECT) came from regression in the FULL system. The question is whether
scheme D with iter12 HPO params is optimal for NAR scheme D, or whether the iter12 HPO
optimized for scheme {3,2,2} and those params are suboptimal for scheme D.

**Proposed refinement:** run a mini-HPO (coarse, 8–12 configs) on scheme-D XGBoost
specifically for NAR before the full co-train. If HPO finds different optimal params
(e.g. deeper tree, larger n_estimators), use those for both the base and the full
co-train pipeline. The scheme-D judge used iter12's HPO params verbatim.

**Training recipe:**

1. NAR scheme-D mini-HPO: 8–12 configs, cheap filter (≤2022 / holdout 2023-25).
   Select best config or confirm iter12 params are still optimal.
2. NAR scheme-D full base WF: train 20-fold WF (2006-2025), all years.
3. For each of 6 sub-classes (NEW / MUKATSU / A / OP / other / C):
   - Generate per-fold scheme-D base WF scores (2007-2026).
   - Retrain CB YetiRank residual (NEW/MUKATSU/A/OP/other) or LGB lambdarank (C)
     on scheme-D base WF scores as the residual feature.
4. Full-system judge (base + retrained ensembles vs production iter12+iter30/36),
   holdout 2023-2026, paired bootstrap 10k seed=42.

**Memory budget:** NAR base train ~230s per fold × 20 = ~77 min. Per-class retrain ~130s
total. Full pipeline: ~90 min. Must run as sole heavy train (memory rule: 1 heavy train
at a time, DuckDB 6GB/4threads if used).

**Accept gate (same 4-axis gate, full-system judge against TRUE production baseline):**

- fukusho_2p LB95 > 0
- ≥ 2 of {top1, place2, place3, top3_box} positive
- ≥ 1 of {place2, place3} positive
- No axis below −0.05pp

**Expected effect:** If HPO confirms params are correct and the scheme-D signal is real
(base WF showed place3 +0.40pp), the co-trained ensemble should align with the new score
distribution. Risk of REJECT remains if the residuals simply cannot improve on a scheme-D
base. Estimated probability of ADOPT: moderate (50%) given base-level positive + weld fix,
but full-system prior was REJECT even with retrained ensembles.

---

## 4. Banei — Skip

All schemes (B, C, D) fail the cheap filter on Banei (157k rows, unusual weight/distance
structure, all-negative deltas for scheme D). No experiments planned for Banei.
Current production: banei-cb-v7-lineage-wf-21y unchanged.

---

## 5. NAR Architecture Options (ranked by risk)

| Option                                       | Description                                                                                | Cost                   | Risk                                                                                                | Recommended        |
| -------------------------------------------- | ------------------------------------------------------------------------------------------ | ---------------------- | --------------------------------------------------------------------------------------------------- | ------------------ |
| A: Joint co-train                            | New base WF → retrain all 6 residuals against fresh WF scores                              | ~90 min                | Medium — ensemble still may not align if HPO params suboptimal                                      | YES (Experiment 3) |
| B: Replace per-class with single graded base | Drop ensembles entirely, use scheme-D base-only                                            | ~20 min                | High — small-class regression (MUKATSU, NEW) likely; these classes benefit from per-class treatment | NO                 |
| C: Per-class weight HPO on scheme-D system   | After co-train, re-optimize blend weights (currently production values) on scheme-D system | +20 min after co-train | Low marginal risk; recommended as a sub-step after co-train if initial judge fails                  | After A            |

---

## 6. Sequencing

```
Experiment 1: JRA iter19 + scheme D
  └─ cheap filter (~6 min)
       └─ PASS → WF judge (~20 min)
            └─ ADOPT → production flip (base-only swap)
            └─ REJECT → Experiment 2

Experiment 2: JRA iter19 + scheme D-2 (finer top-6)
  └─ cheap filter (~6 min)
       └─ PASS → WF judge (~20 min)
       └─ FAIL → JRA graded relevance exhausted at base level;
                  consider finer hyperparameter search on scheme D params

Experiment 3: NAR co-train (independent of JRA experiments)
  └─ mini-HPO NAR scheme-D (~30 min)
       └─ full base WF (~77 min)
            └─ per-class retrain (~15 min)
                 └─ full-system judge
                      └─ ADOPT → NAR production flip
                      └─ REJECT → blend-weight HPO step, then re-judge
```

Experiments 1 and 3 are independent (different categories) and can run in parallel if
memory budget permits. However, NAR full WF (~77 min) + JRA WF (~20 min) simultaneous
risks exceeding the 1-heavy-train-at-a-time memory rule on 48GB. **Run sequentially:
Experiment 1 first (shorter), then Experiment 3.**

---

## 7. Accept Gate Specification (canonical for all experiments)

Identical to the project standard:

```
fukusho_2p_lb95 > 0.0
AND positive_axes >= 2 (where axes = {top1, place2, place3, top3_box})
AND positive_place_axes >= 1 (at least one of place2, place3 must be positive)
AND veto_floor: all axes >= -0.05pp (absolute)
```

For NAR Experiment 3, the baseline for the judge is the TRUE production system:
iter12-nar-xgb-hpo-v8 + iter30/36 per-class ensembles (same methodology as the
nar-schemeD-deploy-judge.md). Holdout: 2023-2026.

For JRA Experiment 1/2, the baseline is iter19-jra-cb-kohan3f-going-v8 (base-only,
current production). Holdout: 2023-2026 (same 11703 races as kohan3f-verify.md).

---

## 8. Why JRA Base-Only is the Cleanest Near-Term Win

Three properties make JRA Experiment 1 the highest-confidence starting point:

1. **No WELD risk.** iter19 is deployed base-only. A WF ADOPT on JRA is immediately a
   production ADOPT with a single `model_meta.py` + manifest change. No ensemble retraining
   required, no blend-weight calibration needed.

2. **New base, new data.** The prior scheme-D REJECT on JRA was against iter14 (241 feats).
   iter19 adds 3 going-conditional kohan3f features that improved EVERY sub-class and lifted
   pooled place2 by ~0.47pp. The feature interactions with sub-4 epsilon labels are
   genuinely untested on this new base.

3. **Cheapest experiment.** A CatBoost cheap filter + WF for JRA costs ~26 min total,
   requires <4 GB RAM, and either produces a concrete production improvement or provides
   a crisp NO (REJECT) with minimal wasted compute before moving to NAR.

---

## 9. Technical Implementation Notes

### JRA scheme D code change (minimal)

In `apps/pc-keiba-viewer/src/scripts/finish_position_catboost.py` (or equivalent trainer):

```python
# Current (production)
def make_to_relevance(rank1: int = 3, rank2: int = 2, rank3: int = 1) -> dict[int, int]:
    return {1: rank1, 2: rank2, 3: rank3}  # returns 0 for all others

# Change to support float sub-4 tail (scheme D)
SCHEME_D_TAIL: dict[int, float] = {4: 0.1, 5: 0.08, 6: 0.05, 7: 0.05, 8: 0.05,
                                    9: 0.02, 10: 0.02, 11: 0.02, 12: 0.02}

def make_relevance_scheme_d(pos: int, rank1: float = 3.0, rank2: float = 2.0,
                              rank3: float = 1.0) -> float:
    top3 = {1: rank1, 2: rank2, 3: rank3}
    if pos in top3:
        return top3[pos]
    return SCHEME_D_TAIL.get(pos, 0.0)
```

Remove `np.int32` cast — CatBoost YetiRank accepts float labels in Pool natively.
Coverage rule applies: update `finish_position_catboost.test.py` for the new function.

### NAR HPO params to try (coarse, Experiment 3 step 1)

Based on scheme D's behavior (faster convergence at large label scale — best_iteration=48
vs iter12's=147): test `{max_depth: [6, 7, 8], n_estimators: [400, 650, 900], subsample: [0.618, 0.75]}`.
Keep lr=0.0527 (iter12 HPO) and other params stable. 8 configs × cheap filter ~25 min.

---

## 10. Memory Budget

| Step                                   | RAM          | Duration | Constraint  |
| -------------------------------------- | ------------ | -------- | ----------- |
| JRA cheap filter (CB, 1M rows)         | ~3 GB        | ~6 min   | Safe        |
| JRA WF (3 folds, CB)                   | ~3 GB × fold | ~20 min  | Safe        |
| NAR mini-HPO (8 configs, cheap filter) | ~6 GB peak   | ~25 min  | Safe (solo) |
| NAR full WF base (20 folds, XGB)       | ~8 GB        | ~77 min  | Solo only   |
| NAR per-class retrain (6 models)       | ~2 GB ea     | ~15 min  | Safe        |

DuckDB 6GB/4threads cap applies if feature queries are run during training.
All steps must run as the sole heavy process (no concurrent training agents).
Check `memory_pressure` before each heavy step; defer if free < 30%.
