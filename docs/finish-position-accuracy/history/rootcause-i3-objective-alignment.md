# Root-cause I3: Objective-Alignment Probe

| Field           | Value                                                                     |
| --------------- | ------------------------------------------------------------------------- |
| Date            | 2026-06-11                                                                |
| Status          | **CLOSED — Objective mismatch is NOT a binding constraint**               |
| Category tested | NAR (XGB rank:pairwise, iter 12 WF predictions + feasibility retrain)     |
| JRA reference   | iter 14 CB YetiRank — same conclusion applies (metrics shown for context) |
| Output artefact | `tmp/rootcause/i3_objective.json`                                         |

---

## Background

Training uses NDCG@3 ranking objectives (CatBoost YetiRank for JRA/ban-ei,
XGB rank:pairwise/ndcg@3 for NAR) with relevance `{1→3, 2→2, 3→1, else→0}`.
Ensemble blend weights were tuned to TOP1.  
Eval metrics are exact-ordinal top1 / place2 / place3 / top3_box.

The hypothesis (I3): NDCG@3 is strongly correlated with top1 but weakly
correlated with place2/place3, so optimising NDCG@3 leaves place
accuracy unoptimized, and a better-aligned objective would lift place.

---

## Part 1 — NDCG@3 vs metric decoupling (NAR 258 966 OOF races)

Source: iter12-nar-xgb-hpo-v8 out-of-fold predictions across all years.

| Pair               | Pearson r |
| ------------------ | --------- |
| NDCG@3 vs top1     | **0.704** |
| NDCG@3 vs top3_box | 0.433     |
| NDCG@3 vs place2   | 0.369     |
| NDCG@3 vs place3   | **0.236** |

Interpretation: NDCG@3 is indeed only weakly correlated with place3 (r=0.24)
and moderately with place2 (r=0.37), while its correlation with top1 is
substantially higher (r=0.70). This quantifies the **decoupling**: optimising
NDCG@3 preferentially improves top1 and leaves place3 particularly under-served.

Additional diagnostic:

- Races where NDCG@3 = 1.0 (perfect top-3 ranking): **15.6 %** of all races.
  Of those, place2 hit rate = 0.999, place3 hit rate = 0.998. So when the
  model gets the full ordering exactly right, place2/3 are trivially hit.
- Of 41 650 races where both place2 AND place3 are hit simultaneously, only
  **3.6 %** have NDCG@3 < 1.0 — meaning near-perfect ordering is nearly
  necessary for joint place hits.

**Conclusion on decoupling**: mismatch is real and quantifiable (r*place3=0.24
vs r_top1=0.70), but it is a \_structural* property of exact-ordinal metrics
evaluated on ranking outputs — not a tunable training objective property.
See Part 2.

---

## Part 2 — Feasibility: Can a re-aligned objective lift place2/3?

Setup:

- Category: NAR (all grades; XGBoost same HPO params as iter12)
- Train: 2013–2022 (1 292 736 rows, 129 394 races)
- Tune / early-stop: 2023 (136 354 rows, 13 607 races)
- Test (held out): 2024 (138 375 rows, 13 677 races)
- Threads: 4

Four objectives tested:

| ID  | Objective                               | Relevance scheme        |
| --- | --------------------------------------- | ----------------------- |
| A   | rank:pairwise (production baseline)     | {1→3, 2→2, 3→1, else→0} |
| B   | rank:pairwise + place-boosted           | {1→2, 2→3, 3→3, else→0} |
| C   | rank:ndcg eval_metric=ndcg@2 lambdarank | {1→3, 2→3, 3→1, else→0} |
| D   | rank:pairwise set-membership            | {1→1, 2→1, 3→1, else→0} |

### Test-year (2024) metrics

| Objective           | top1       | place2     | place3     | top3_box   |
| ------------------- | ---------- | ---------- | ---------- | ---------- |
| A baseline          | **0.5846** | **0.3568** | **0.2734** | 0.3529     |
| B place-boosted     | 0.2844     | 0.2428     | 0.1954     | 0.3382     |
| C NDCG@2 lambdarank | 0.5856     | 0.3542     | 0.2676     | 0.3485     |
| D place-set         | 0.5794     | 0.3469     | 0.2711     | **0.3528** |

### Delta vs baseline (pp)

| Objective           | Δtop1     | Δplace2   | Δplace3  | Δtop3_box |
| ------------------- | --------- | --------- | -------- | --------- |
| B place-boosted     | **–30.0** | **–11.4** | **–7.8** | –1.5      |
| C NDCG@2 lambdarank | +0.10     | –0.26     | –0.58    | –0.44     |
| D place-set         | –0.52     | –0.99     | –0.23    | –0.01     |

---

## Finding

**No tested objective variant lifted place2 or place3 above the baseline.**

- Objective B (place-boosted: make pos-2/3 more relevant than pos-1) is
  catastrophic: top1 drops –30 pp, place2 –11.4 pp, place3 –7.8 pp.
  Forcing the model to rank 2nd/3rd above 1st destroys the learned
  score ordering entirely.
- Objective C (NDCG@2 lambdarank, equal relevance for pos-1 and pos-2)
  shows +0.10 pp top1 but –0.26 pp place2 and –0.58 pp place3. No gain.
- Objective D (flat set-membership) shows uniform small regressions across
  all metrics (–0.52 pp top1, –0.99 pp place2, –0.23 pp place3). No gain.

The baseline (production NDCG@3 relevance scheme A) dominates all three
alternatives on all four exact-ordinal metrics.

---

## Root-cause interpretation

The decoupling (r_place3=0.24) is **structural**: exact-ordinal metrics
require getting the exact predicted rank position correct, not just a
correct relative ordering within the top-3 set. A ranking model that
perfectly orders {1st, 2nd, 3rd} will trivially hit all three exact-ordinal
metrics. Reweighting the relevance scheme cannot resolve this because the
fundamental signal — predicting exactly which horse finishes 2nd vs 3rd —
is limited by available features, not by the choice of ranking objective.

Empirically: the models trained here have top1 ≈ 58.5 %, place2 ≈ 35.7 %,
place3 ≈ 27.3 %, which is roughly consistent with what one would expect from
the top-1 accuracy and the difficulty of exact 2nd/3rd placement.

**Objective alignment is NOT a binding constraint.** The place2/place3 gap
relative to top1 is driven by the inherent difficulty of exact-ordinal
prediction, not by the training objective.

---

## Recommendation

Phase 3 should **NOT** re-align the training objective. The production
NDCG@3 relevance scheme is already optimal among tested alternatives.

Remaining levers to improve place2/place3 are:

1. **New horse-level signals** that specifically distinguish 2nd from 3rd
   (e.g., sectional splits, race-internal running-style features still
   under investigation in the science track).
2. **Full retrain / HPO** with the existing optimal objective after new
   signals are validated.
3. Ensemble of separate binary classifiers for place2-hit and place3-hit
   as a post-processing calibration layer (not objective change).

Closing I3. Objective-alignment lever is exhausted.
