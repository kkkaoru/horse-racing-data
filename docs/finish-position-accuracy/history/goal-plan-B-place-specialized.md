# Plan B: Place-Specialized Approaches for 2着/3着 >40%

**Date:** 2026-06-17  
**Scope:** JRA & NAR 2着 / 3着 accuracy beyond current frontier, keeping top1 intact.  
**Distinct from:** Plan A (sub-4 graded-relevance — covered by sibling agent).

---

## What Has Been Tried and Definitively Closed

Before proposing new angles, all rejected paths are listed to prevent re-proposal.

| Approach                                                         | Verdict                                                    | Key doc                                                         |
| ---------------------------------------------------------------- | ---------------------------------------------------------- | --------------------------------------------------------------- | ------------- |
| LambdaRank place_weighted objective                              | REJECT — top1 −11.77pp, place2 −7.72pp                     | PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md §4.2                   |
| Binary place2/place3 specialist (standalone)                     | REJECT — place2 17.9% vs 27.4% baseline                    | Same, §4.2                                                      |
| Hierarchical cascade (binary specialist)                         | REJECT — place2 −6.1–11.6pp across thresholds              | Same, §4.3                                                      |
| Hierarchical cascade (transformer specialist)                    | REJECT — place2 −7.3pp                                     | Same, §4.3                                                      |
| Conditional P(2nd                                                | ≠1st) attention head (MLX)                                 | REJECT — place2 −8.4pp; cascade −6.76pp                         | Same, Phase B |
| Plackett-Luce re-rank                                            | REJECT — zero lift (mathematically identical to raw score) | rootcause-i6-architecture.md §3.2                               |
| C4 joint placement score (3P̂₁+2P̂₃, isotonic calibration)         | REJECT on real data — fuku LB95 negative for JRA/NAR       | phase3-calibration-c4-rerank.md                                 |
| Re-rank by P(place2) independently                               | REJECT — catastrophic −10pp top1 (simulation + empirical)  | rootcause-i6-architecture.md §3.2                               |
| Asymmetric loss boosting place2 (r331/r341 variants)             | REJECT — top1 and place2 both degrade                      | PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md §7-ter                 |
| Pedigree×distance×grade higher-order features                    | REJECT — redundant with existing sire_distance_win_rate    | Same, Phase D                                                   |
| Graded relevance sub-4 (scheme B/C/D) JRA                        | REJECT WF                                                  | graded-relevance-experiments.md §4                              |
| Graded relevance sub-4 (scheme D) NAR full-system                | REJECT — per-class residuals break on score shift          | graded-relevance-experiments.md §7, nar-schemeD-deploy-judge.md |
| Exotic odds (umaren/wide/sanrenpuku) as features — JRA           | ABORT — partial ρ 0.02–0.03 on is_top3 (below gate 0.08)   | exotic-odds-place-signal.md; jra-fukusho-odds-probe.md          |
| Exotic odds — NAR                                                | REJECT — o3/o2 2024 ingest gap + fuku LB95 < 0             | exotic-odds-place-verify.md (re-run)                            |
| Exotic odds — Ban-ei standalone                                  | REJECT — top1 −0.234pp veto                                | exotic-odds-place-verify.md                                     |
| Fukusho odds (JRA) as place signal                               | ABORT — ρ +0.024 on is_top3 < gate                         | jra-fukusho-odds-probe.md                                       |
| JRA per-class LGB lambdarank residual                            | REJECT all 6 classes (bootstrap power, not model quality)  | jra-perclass-residual-feasibility.md                            |
| Near-miss features (career_place2_rate, jockey_place2_rate etc.) | +0.14pp place2 JRA only — too small for >40% target        | PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md Phase A                |
| NAR sanrenpuku at serve time                                     | STOP — 100% NULL at 03:00 cron (serve/train mismatch)      | sanrenpuku-serve-gate.md                                        |

---

## Root-Cause Recap (from I1–I7 analysis)

The fundamental constraint documented across I1/I6:

- P(finish=2) and P(finish=1) are both driven by horse quality → they are near-identical signals.
- JRA top1 ~52% and place2/3 are near the Pareto frontier of the current 241-feature set.
- "Exact-ordinal" place2/place3 (which horse finishes exactly 2nd/3rd) is near-ill-posed: 47–62% of misses are adjacent-position swaps that no model can reliably separate.
- Market odds efficiency wall: odds already encode 56% of predictive information (Ban-ei clean test: odds-only gain 56%, removing odds costs 7.95pp top1). JRA/NAR similarly odds-dependent.
- C4 re-rank failed on REAL data (only worked in synthetic) because P̂₁ and P̂₃ are order-correlated monotone functions of the same raw score in single-score GBDT.

The conclusion from I6: **architecture is not the binding constraint; signal saturation is.** New approaches must either (a) introduce genuinely new orthogonal signals or (b) exploit structural properties not yet encoded.

---

## Five Candidate Angles — Assessed

### Angle 1: Trained Plackett-Luce / Listwise Objective with Sub-4 Labels (place-aware)

**What it is:**  
The "Plackett-Luce / listwise trained PL objective" here is distinct from post-hoc PL re-ordering of raw scores (which was proven zero-lift in I6). The question is whether **training** with a full permutation likelihood that down-weights ranks 4+ specifically relative to 1/2/3 adds information. This is what graded-relevance scheme-D attempted for NAR (adding epsilon labels 0.1/0.08 for ranks 4–8).

**Novelty vs prior work:**  
Scheme-D NAR passed the base-model WF gate (+4.0pp place3 at base level) but FAILED the full-system judge: the per-class residual ensembles (iter30/36) are calibrated to iter12's score distribution; scheme-D shifts that distribution and the blended system regresses (f2p LB95 = −0.00481, top1 −0.40pp, place3 −0.43pp). The same failure mode is expected for scheme-B.

**For JRA specifically:** scheme-D REJECT at WF (fold-level inconsistent, f2p_lb95 −0.00357). Scheme B/C fail cheap filter (top1 −2.2–3.1pp).

**Verdict: REJECTED REHASH** — this is exactly what graded-relevance experiments tested. The sub-4 epsilon tail idea is not a new lever; it was fully evaluated and rejected at the full-system level for the only category (NAR) where the base model showed signal. A clean-room retrain without per-class ensembles would be required to isolate the base-model gain, but per-class ensembles are integral to production.

**Expected place2/3 lift:** 0pp net at full-system level (confirmed empirically).

---

### Angle 2: Two-Stage Conditioned-on-Rank Place Classifier

**What it is:**  
Stage-1 ranker produces a rank-1 prediction. Stage-2 classifies is-2nd / is-3rd conditioned on stage-1 score + field context (remaining horses after removing the predicted winner). The conditioned variant is claimed to differ from prior binary specialists because it explicitly knows who is predicted to win.

**Prior work relevance:**  
The "conditional_place2_logit" head in the MLX transformer (Phase B of PLACE_ACCURACY_IMPROVEMENT_2026-05-20.md) is exactly this: it concatenates the softmax-weighted "winner embedding" with each horse's embedding → MLP → P(2nd). Result: −6.76pp place2 vs baseline via hierarchical cascade, and the transformer's rank head was also −8.5pp vs GBDT on top1.

The I6 simulation additionally tested strategy D: "predict rank1 by P(1), then re-rank rest by P(2|≠rank1)". Result: −1.167pp place2. The root cause is consistent: P(2nd | ≠rank1) is dominated by the same quality signal — the conditional doesn't get "new information about who specifically runs 2nd."

**What genuinely differs from prior work:**  
The transformer's conditional head used learned embeddings on the same feature set. A GBDT-based two-stage where stage-2 features include _field-relative signals computed after removing the predicted winner_ (e.g., who is now the relative favorite, new within-race odds ranks) might differ — but those derived features are computable from existing features already in the model (odds_rank_in_race, popularity_rank_in_race already encode field position). GBDT already learns this partition implicitly.

**Feasibility:**  
Would require a new inference-time pipeline: run stage-1 prediction → compute stage-2 field-relative features → run stage-2 for each remaining horse → combine. High engineering cost, 0 empirical precedent showing gain over GBDT in this domain.

**Expected place2/3 lift:** Negative to zero. Confirmed by MLX conditional head (−6.76pp) and simulation (−1.167pp). Conditioned-on-rank does not escape the quality-correlation trap.

**Verdict: REJECTED REHASH** — the conditional structure has been tested both architecturally (MLX) and in simulation (I6 strategy D). Both show negative results for the same root cause.

---

### Angle 3: Conditional Score Calibration per Position Rank (Per-rank Isotonic)

**What it is:**  
Calibrate separately for each predicted rank slot (rank-1 horses, rank-2 horses, etc.). Fit isotonic regressions that map raw score → P(actual=2nd | predicted_rank=2) vs. the current approach of calibrating across all horses. If rank-2 predicted horses have a systematically different calibration curve, correcting this could improve place2 accuracy.

**Novel vs prior work:**  
The C4 calibration experiment (phase3-calibration-c4-rerank.md) used isotonic calibration on the full score distribution to produce P̂₁ and P̂₃, then re-ranked by 3P̂₁+2P̂₃. It did NOT fit per-rank-slot calibrations. This is a narrow distinction but genuine.

**Expected lift assessment:**  
The C4 experiment showed B==D (combining calibrated P̂₁ and P̂₃ with different weights produces the same ordering because P̂₁ and P̂₃ are monotone in the same raw score). Per-rank calibration would not change this ordering: if P(actual=2nd | predicted_rank=2) is calibrated more accurately, but the raw score ranking is the same, the re-ranking by calibrated probabilities still produces the same output as ranking by raw score (the calibration is monotone-preserving by construction of isotonic regression). The only way per-rank calibration produces different orderings is if the calibration has flat regions (ties), and those ties happen to favor the wrong horse. This was tested implicitly in phase3: "The small number of rank changes that do occur (~13% of horse positions) arise from ties created by the isotonic calibration's flat regions, and these changes do not systematically improve or hurt aggregate metrics."

**Expected place2/3 lift:** ~0pp. Monotone calibration preserves ordinal ranks; non-monotone calibration introduces noise that is not directional.

**Verdict: NOT GENUINELY NOVEL** — this is a minor variant of the C4 calibration experiment. The root cause (P̂₁ and P̂₃ are monotone in the same score) applies equally. Low effort but near-zero expected gain.

---

### Angle 4: Exotic Odds for Place in a LATE-PASS Display-Only or Separate Place Model

**What it is:**  
Since exotic odds (wide/sanrenpuku/umaren) are blocked at 03:00 JST serve time for NAR/Ban-ei, and are redundant with tansho for JRA, there are two sub-angles:

**4a. JRA: dedicated late-pass place model (10:00 JST re-predict)**  
JRA advance odds ARE available at 03:00 JST (−19 h pre-race-day). Fukusho implied probability was ABORT (partial ρ = 0.024 on is_top3). Sanrenpuku/wide/umaren were ABORT for JRA (highest ρ = 0.08 for sanrenpuku, below gate, per exotic-odds-place-signal.md). JRA exotic odds do not add place signal beyond tansho because the JRA pari-mutuel market efficiently prices all bet types off the same pool information.

**4b. Ban-ei: dedicated place model using exotic odds, ignoring top1**  
The Ban-ei exotic verify showed top3_box +1.774pp (LB95 +0.853pp) and fukusho_2p +0.770pp (LB95 +0.167pp) with exotic odds added. These are blocked only by top1 −0.234pp veto. A dedicated Ban-ei PLACE model (separate from the win model, optimizing fukusho_2p or place3 directly) could exploit exotic odds without top1 regression.

**4c. NAR: 10:00 JST re-predict pass with sanrenpuku**  
The NAR sanrenpuku serve-gate doc explicitly identifies this as a path: implement a second prediction run at 10:00 JST when sanrenpuku becomes available. The 2024 o3/o2 ingest gap also needs fixing. The base verify showed +0.16pp place2 / +0.0031pp f2p pooled (2023–2025 first run), though the 2024 regression drove the overall REJECT. Once the ingest gap is fixed and a later cron is available, this becomes a viable probe.

**Novelty:**  
4b (Ban-ei dedicated place model) is genuinely untried — all prior experiments used a unified win+place model. 4c (NAR 10:00 JST pass) is a known-deferred path, not yet attempted. 4a (JRA) is exhausted.

**Expected lift:**

- 4b Ban-ei: place3 +1.4pp, fukusho_2p +0.77pp (from existing verify data); top1 no constraint in a place-only model. Feasibility: medium — requires a separate prediction slot that does not overwrite the win-model output, plus a display path for "place prediction."
- 4c NAR: +0.16pp place2 expected IF the 10:00 JST pass is implemented and the ingest gap is fixed. Small gain.
- 4a JRA: 0pp (exhausted).

**Cost/gate:**  
4b requires: (a) Ban-ei place model training (separate objective, e.g. fukusho_2p-weighted loss or direct place3 target), (b) a serve-time slot that runs at a time when exotic odds are available (~12:00 JST for Ban-ei), (c) viewer-side display for a "place prediction" that is separate from the win prediction. Gate: the place model must NOT regress an existing metric; it runs in parallel and outputs a different prediction column.

---

### Angle 5: Field-Composition Signal — Rivalry / Suppression Features

**What it is:**  
Encode WHY specific horses consistently finish 2nd: head-to-head rivalry patterns against specific dominant horses, pace-composition effects on place3 specifically, and field-size-adjusted probability of a specific finish slot.

**Sub-features:**

- `career_place2_rate_when_odds_favorite_present` — does this horse specifically run 2nd when there is a dominant favorite?
- `h2h_place2_rate_vs_specific_styles` — when field contains N front-runners, this horse's historical place2 rate
- `competitor_dominance_index` — max(1/odds) in field relative to own 1/odds; when this index is high (one dominant horse), does the horse benefit or suffer in the 2nd/3rd slot?
- `field_pace_scenario_place2_rate` — 2着率 partitioned by predicted running-style distribution of field (slow pace / contested pace)

**Prior work assessment:**  
The near-miss features (2026-05-20 Phase A) included `career_place2_rate` and `field_dominant_favorite_indicator` (odds ratio of 1st/2nd favorite), giving +0.14pp place2 on the v7 baseline. The relationship/per-class investigation (2026-06-12) showed that partial ρ is necessary but not sufficient — GBDT may already capture these non-linearly. However, the specific conditioning on "rival horse identity/style" is distinct from the unconditional `career_place2_rate` that was tried.

**What is genuinely untried:**  
`h2h_avg_finish_diff_vs_field` exists (I6 §2.3). What does NOT exist: the conditional variant that looks specifically at "finish position when GBDT-predicted rank-1 horse from THIS field is in the race." This requires knowing the predicted winner at feature engineering time (creating a leak risk) — OR using the historical identity of dominant horses that this horse has repeatedly raced against.

**Feasibility:**  
Pure horse-vs-horse historical rivalry: `GROUP BY (horse_A_id, horse_B_id)` → `avg(horse_A_position WHERE horse_B_position = 1)`. This is leak-free (all prior races). The challenge: sparse cells (most horse pairs rarely meet), especially for young horses. JRA has more repeated matchups than NAR.

**Expected place2/3 lift:**  
Near-miss v7 features gave +0.14pp on a CatBoost that was below production (~47% vs 52% production baseline). On production baseline, the proportional gain is likely +0.05–0.12pp — below the >40% target. The I6 verdict: "Rivalry/competition-dynamic features: where the remaining headroom lies, regardless of architecture." However, the 2026-06-11 saturation conclusion ("all levers exhausted") reduces confidence. The relationship features investigate (2026-06-12) specifically tested rivalry-adjacent signals (h2h, within-race-relative) and found GBDT already non-linearly captures them.

**Verdict: MARGINAL NOVEL** — rivalry features are not a prior-rejected rehash, but the near-miss Phase A and relationship investigation both found incremental gains of 0.1–0.2pp at best. This will not reach the >40% target without additional stacked levers.

---

## Ranked Plan

| Rank | Angle                                                                  | Novelty                                      | Expected place2/3 lift                                  | Cost       | Gate                                         | Status                                             |
| ---- | ---------------------------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------- | ---------- | -------------------------------------------- | -------------------------------------------------- |
| 1    | **4b: Ban-ei dedicated place model** (exotic odds + place objective)   | GENUINE — separate place model never tried   | place3 +1.4pp / fukusho_2p +0.77pp (from verified data) | Medium     | Top1 no constraint; place3 / fukusho_2p gate | UNTRIED — recommended                              |
| 2    | **4c: NAR 10:00 JST re-predict pass** (after sanrenpuku available)     | GENUINE — requires infra + ingest fix        | place2 +0.16pp (rough estimate)                         | High infra | fuku LB95 > 0 on fixed 2024 data             | DEFERRED — needs 10:00 JST cron + o3/o2 ingest fix |
| 3    | **Rivalry/suppression features** (horse-vs-dominant-field composition) | MARGINAL — sub-species of near-miss features | place2 +0.05–0.12pp                                     | Medium     | Incremental gate vs iter14/iter12            | NOT YET TRIED at this specificity                  |
| 4    | Conditional per-rank isotonic calibration                              | NOT NOVEL — variant of C4                    | ~0pp                                                    | Low        | —                                            | EFFECTIVELY TESTED                                 |
| 5    | Two-stage conditioned-on-rank place classifier                         | REHASH — MLX conditional head + I6 sim D     | Negative                                                | High       | —                                            | REJECTED                                           |
| 6    | Trained PL / sub-4 graded relevance                                    | REHASH — scheme-D full-system REJECT         | 0pp net                                                 | High       | —                                            | REJECTED                                           |

---

## Top 2 Genuinely Novel Place-Specific Levers

### Lever 1: Ban-ei Dedicated Place Model with Exotic Odds

**What is new:** Treating the Ban-ei place prediction as a SEPARATE model objective from the win model. Currently, the single CatBoost YetiRank model optimizes top1 ranking and that implicitly produces place predictions. Exotic odds (wide/sanrenpuku/umaren) were REJECT for the unified model because they hurt top1 (−0.234pp), but there is NO reason to include them in a model that does NOT optimize for top1. A dedicated model trained with a place3/fukusho_2p-weighted objective or direct classification target `is_top3` would be free to use exotic odds without the top1 trade-off.

**Expected 2着/3着 effect:**  
From the Ban-ei exotic verify (2023–2026, N=5,976 races):

- place3 +1.774pp (LB95 +0.853pp) — verified from add_exotic_odds_features.py
- fukusho_2p +0.770pp (LB95 +0.167pp)
- top2 (place2) +0.686pp (LB95 −0.050pp, weak)

These numbers are from adding exotic features to a WIN model. A place-objective model will likely show higher place3/fukusho_2p gains because it can weight place-correctness more strongly.

**Scope limitation:** This applies to Ban-ei only. JRA exotic odds are redundant with tansho (ρ < 0.03 for is_top3 residual). NAR is blocked by ingest gap and serve timing.

**Implementation path:**

1. Train a Ban-ei place model: same feature set as `feat-ban-ei-v7-lineage-21y` + exotic features from `feat-banei-v7grade-exotic`, but with a classification target `is_top3` (binary per horse) or a place-weighted ranking objective.
2. Run 21-year WF using 2015–2025 train / 2023–2026 holdout.
3. Gate: fukusho_2p LB95 > 0 AND place3 LB95 > 0; top1 is NOT a gate (this is a place model).
4. Serve as a separate prediction column; viewer shows both win-rank and place-rank.
5. Exotic odds available at 12:00 JST Ban-ei serve time (confirmed from sanrenpuku-serve-gate.md).

**Compute cost:** ~30 min (Ban-ei is the smallest dataset, ~160K rows).

---

### Lever 2: NAR 10:00 JST Re-Predict Pass with Exotic Odds (Conditional-Go)

**What is new:** The sanrenpuku-serve-gate.md STOP was explicitly declared "until (a) a 10:00 JST re-predict cron is implemented and validated, or (b) NAR advance sanrenpuku odds become available." A 10:00 JST NAR re-predict pass:

- Sanrenpuku (3renpuku) is available from 10:00 JST for all standard NAR venues.
- The 2024 o3/o2 ingest gap caused the REJECT in prior verify; fixing this gap and re-running would remove the primary regression driver.
- The launchd 03:00 JST cron provides baseline predictions; the 10:00 JST pass would UPDATE them with the exotic-enhanced model.

**Expected 2着/3着 effect (estimate):**  
The first NAR verify (2023–2025, original feat-nar-exotic-v8) showed +0.16pp place2 pooled (before 2024 gap regression). The re-run (feat-nar-exotic-v1, 2023–2026) showed −0.035pp top1, −0.026pp place2 (driven by 2024). If the 2024 ingest gap is fixed and sanrenpuku is available at serve:

- Estimated place2: +0.10–0.20pp (small but real signal from 2023/2025 years)
- The effect is modest — well below the >40% target by itself.

**Scope limitation:** Small gain for NAR. This lever is more about "recovering available signal that was deferred" than providing a breakthrough.

**Implementation path:**

1. Fix o3/o2 NAR 2024 ingest gap in the warehouse (source: nvd_o3/nvd_o2 for 2024 NAR missing).
2. Verify fixed 2024 data in the exotic verify (re-run `add_exotic_odds_features.py` for 2024 NAR).
3. Add 10:00 JST launchd slot that re-runs `finish_position_predict.py` for NAR with exotic features populated.
4. Gate: fuku LB95 > 0 on the re-run verify before deploying.

**Compute cost:** ~1h for infra + verification (DuckDB NAR exotic feature rebuild ~20 min, WF verify ~30 min).

---

## Why Neither Lever Reaches >40% Place2/3 Globally

The target ">40% 2着/3着" is ambitious:

- Current best: JRA place2 ~28.6%, NAR place2 ~35.8%
- The identified levers are Ban-ei place3 (+1.4pp) and NAR place2 (+0.1–0.2pp)
- JRA has no identified lever for place improvement at this stage (fukusho ABORT, exotic ABORT, graded-relevance WF REJECT, per-class LGB REJECT)

The structural constraint from the science saturation analysis (2026-06-11) remains: exact-ordinal place2/3 is partially ill-posed (adjacent swaps), and market efficiency already absorbs most predictable signal. Reaching >40% globally requires either:  
(a) New data sources not currently available (per-horse sectional splits, pre-race workout times, auction prices for maiden)  
(b) Resolving the exact-ordinal ill-posedness by redefining the metric as fukusho_2p (combinatorial top-3 set) rather than exact place2/place3  
(c) Significant improvement in NAR top1 (+2–5pp) that would carry place2/3 with it

The two levers above are the **only genuinely novel place-specific improvements identified** given the current data envelope.

---

## Relation to Existing History

- `project_place_improvement_infeasible.md` — covers 2026-05-20 conclusion; Levers 1 and 2 above are post-that-date findings (exotic odds verification came in 2026-06-12)
- `project_science_track_saturation_2026_06_11.md` — Lever 1 (Ban-ei place model) is NOT covered by that saturation analysis (which focused on unified models); Lever 2 (NAR ingest fix) is the deferred conditional-go from sanrenpuku-serve-gate.md
- `exotic-odds-place-verify.md` — provides the numerical basis for Lever 1 estimates
- `sanrenpuku-serve-gate.md` — defines the pre-conditions for Lever 2
