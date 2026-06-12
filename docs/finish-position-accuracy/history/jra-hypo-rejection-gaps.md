---
doc_type: gap-analysis
title: JRA Hypothesis Rejection-Space Gap Analysis
date: 2026-06-13
scope: JRA finish-position prediction — meta-analysis of rejection corpus
method: devil's advocate / methodology-hole detection + composition-gap detection
verdict: 2 clean flawed-test re-opens + 1 untested feature candidate; all other rejects confirmed clean
production_change: none (read-only analysis)
---

# JRA Hypothesis Rejection-Space Gap Analysis

## 1. Purpose

Read every rejection/ABORT in the JRA corpus and map:

- **Methodology holes**: rejects decided by an artifact that was never corrected
- **Composition gaps**: A∘B where A alone and B alone were rejected but the joint was never tested
- **Asymmetry gaps**: things tested NAR/Ban-ei but never JRA (or vice versa) without categorical justification
- **Serve-distribution re-opens**: anything decided on pre-serve-fix WF that would behave differently now

Novelty discipline: re-proposing anything with a clean, confirmed reject is forbidden.

---

## 2. Rejection Map (JRA-relevant only)

| ID  | Hypothesis                                                               | Category     | Verdict                           | Deciding number                                                               | Methodology note                                                                                      |
| --- | ------------------------------------------------------------------------ | ------------ | --------------------------------- | ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| R01 | Graded relevance scheme D (sub-4 epsilon tail)                           | Objective    | REJECT (base model) → FULL REJECT | f2p LB95 −0.00357 at base-model WF; full-system judge not attempted           | Base-model-only judge; full-system judge (iter25/26) never run                                        |
| R02 | Sub-4 graded relevance Scheme B (extended integers, CatBoost)            | Objective    | FAIL cheap filter                 | top1 −2.2pp                                                                   | Too aggressive label inflation — correctly killed                                                     |
| R03 | I3 re-weighting within {3,2,1,0} on NAR                                  | Objective    | REJECT                            | All 4 variants negative; production {3,2,2} already optimal within top-3 tier | JRA equivalent untested; relevance-scheme-audit confirms "genuinely untested"                         |
| R04 | Per-class LGB lambdarank residual (JRA classes 703/005) on iter14        | Residual     | REJECT                            | bootstrap LB95 top3_box −0.44pp (703) −0.81pp (005)                           | Base = iter14, not iter25/26; pre-06/11 serve distribution                                            |
| R05 | JRA pooled-system lambdarank residual                                    | Residual     | REJECT                            | fukusho_2p bootstrap LB95 = −0.002734 (n=11,703 races, 2023-26)               | Residual weights optimized on 2018-20 inner split against iter14, not freshly HPO'd against iter25/26 |
| R06 | Exotic place odds (umaren/wide/sanrenpuku) for JRA                       | Feature      | ABORT                             | max partial ρ = 0.080 (sanrenpuku)                                            | Clean: JRA market efficiency absorbs exotic; confirmed mechanism                                      |
| R07 | Fukusho implied p3 for JRA                                               | Feature      | ABORT                             | partial ρ = 0.02–0.03                                                         | Clean: co-determined with tansho pool                                                                 |
| R08 | Graded relevance (JRA trainer features audited)                          | Feature      | REJECT                            | iter18: all 4 axes negative globally                                          | Clean: class signals subsumed by existing features                                                    |
| R09 | jockey×trainer combo win-rate (JRA)                                      | Feature      | ABORT                             | partial ρ 0.002–0.007                                                         | Clean                                                                                                 |
| R10 | yoso_soha_time speed figure (JRA)                                        | Feature      | ABORT                             | partial ρ = 0.066                                                             | Clean (sub-gate)                                                                                      |
| R11 | Blinker flag, apprentice code, day-in-meet, sale price                   | Feature      | ABORT                             | partial ρ < 0.03                                                              | Clean                                                                                                 |
| R12 | kyakushitsu_keiko nige ratio                                             | Feature      | ABORT                             | within-race ρ = −0.067                                                        | Clean: FAIL gate                                                                                      |
| R13 | jvd_hc + jvd_wc workout features                                         | Feature      | ABORT                             | best ρ 0.032                                                                  | Clean: market already prices training                                                                 |
| R14 | nige_vs_field, oikomi_in_fast_field running-style relationships          | Feature      | ABORT                             | −0.5pp net LightGBM                                                           | Coverage 87% (needs 100%) — ABORT is coverage-gated, not signal-absent                                |
| R15 | Log_odds_z, within-race relative features                                | Feature      | ABORT                             | redundant; GBDT already captures non-linearly                                 | Clean                                                                                                 |
| R16 | rs*p*\* full backfill (logit probabilities)                              | Feature      | ABORT (sub-gate)                  | Same activation blocker as R14                                                | Linked to RS coverage                                                                                 |
| R17 | LGB lambdarank HPO (H4) + alt-loss on per-class residuals JRA equivalent | Residual     | NEVER RUN                         | iter36 adopted for NAR-C; JRA follow-on explicitly stated but never executed  | Asymmetry gap                                                                                         |
| R18 | Transformer (MLX RaceSetTransformer) vs GBDT                             | Architecture | REJECT                            | JRA top1 −8.5pp vs GBDT                                                       | Only 2 WF folds (2024-2025), no HPO — underpowered methodology                                        |
| R19 | Triplet rank 1: kohan_3f × corner4_norm × babajotai_code                 | Feature      | PROCEED (not yet model-tested)    | kohan3f_firm_avg5 ρ=+0.1286, kohan3f_soft_avg5 ρ=+0.0977 — gate passed        | No model test performed; features confirmed non-redundant (max corr 0.72 with kohan3f_avg_5)          |
| R20 | h-official-running-style (kyakushitsu_keiko fraction)                    | Feature      | ABORT                             | partial ρ = 0.074 full / 0.068 holdout                                        | Closest-to-gate ABORT; highest ρ of recent probe batch                                                |
| R21 | D2a locality features for NAR 43/44                                      | Feature      | ABORT                             | top1 regression −0.107pp                                                      | NAR-specific; JRA not applicable                                                                      |
| R22 | RS decision rules post-hoc (H1 bias)                                     | RS           | REJECT                            | not all-positive 2023/2024                                                    | Clean: calibrated argmax already at frontier                                                          |
| R23 | James-Stein partial pooling (F3)                                         | Architecture | REJECT                            | mathematical proof of futility                                                | JS retain=0.9937 ≈ identity; clean                                                                    |
| R24 | JRA 016 class per-class specialist                                       | Architecture | REJECT                            | n=727 underpowered                                                            | Persistently positive point estimates (+1.93pp top1 at pooled level) but statistically insufficient   |
| R25 | Band-conditional market blend (D2b JRA mod-odds calibration)             | Feature      | ABORT                             | tansho already captures interaction                                           | Clean                                                                                                 |
| R26 | Graded relevance scheme D: full-system JRA judge                         | Objective    | NEVER RUN                         | base-model reject on 3-fold WF (2023-25)                                      | Full-system judge (iter25/26) not attempted; see flaw analysis below                                  |

---

## 3. Ranked Re-Opens and Untested Compositions

### Rank 1 — Triplet 1 feature `kohan3f_firm_avg5` / `kohan3f_soft_avg5`: UNTESTED (never model-tested)

**What it is:** Going-conditional late-section speed averages. `kohan3f_firm_avg5` = avg(kohan_3f) over last 5 races where going was firm (babajotai_code 1-2); `kohan3f_soft_avg5` = same for soft (3-4). Built from `jvd_se.kohan_3f` + `jvd_ra.babajotai_code_shiba/babajotai_code_dirt`, strictly leak-free.

**Gate status (triplet-verify-p1.md, 2026-06-12):**

- `kohan3f_firm_avg5`: partial ρ = **+0.1286** (n=102,058 JRA holdout rows, 2023-2026) — PASS
- `kohan3f_soft_avg5`: partial ρ = **+0.0977** (n=20,339) — PASS
- Redundancy: max corr vs existing = 0.72 (vs `kohan3f_avg_5`) — below 0.85 threshold — not redundant
- `kohan3f_going_diff`: ρ = +0.017 (sub-gate, but low redundancy = 0.07 vs existing)

**Flaw exploited:** The partial-ρ gate was passed but no model test (incremental WF judge against iter25/26 production baseline) was ever run. This is the clearest open candidate in the corpus: gate passed, redundancy cleared, model test = 0.

**Composition note:** This feature would naturally combine with the existing unconditional `kohan3f_avg_5` (iter14 feature) and the going-conditioned course features (iter14 course layer). If the going-conditional differential `kohan3f_going_diff` is also included (ρ sub-gate but orthogonal to all existing features), the 3-feature bundle represents a genuinely novel going-sensitivity dimension.

**Cheapness:** Low — feature computation is pure SQL over existing PG columns (no new warehouse tables); pipeline addition follows the same pattern as `add-sectional-weight-features.py`. Incremental WF judge can reuse the existing parquet infrastructure.

**Risk:** Non-trivial regression risk if going-conditional features are NULL-heavy (sparse soft-going history for many horses); GBDT's NULL routing should handle this naturally per D-phase lesson.

**Rank justification:** Highest ρ of any untested feature in corpus, confirmed non-redundant, zero-cost data path, no architectural change needed.

---

### Rank 2 — Fresh LambdaRank HPO on top of iter25/26 per-class ensembles (JRA): ASYMMETRY GAP

**What it is:** The iter36 adoption for NAR class C was: fresh Optuna HPO over LightGBM lambdarank residual on top of iter12 as base, producing +0.342pp top1 (user approved win-priority override). The iter36 doc explicitly states: "The natural follow-on is to evaluate the same alt-loss + fresh-HPO LambdaRank residual on the other and B NAR classes (and the JRA per-class buckets)."

**Flaw exploited:** The JRA lambdarank tests (R04 pooled judge, R04 per-class judge) used:

- Base = iter14 (not iter25/26 per-class ensembles)
- Residual weights optimized on 2018-20 inner split against iter14 as reference
- Date: 2026-06-11 (pre-serve-fix; but this is irrelevant since WF uses historical final odds which match the NEW serve condition)

A fresh LambdaRank HPO calibrated against the iter25/26 production ensembles as baseline was **never run for any JRA class**. The pooled judge (+0.385pp top1, LB95 = −0.002734) used stale residual weights calibrated against iter14 — the ensembles already absorbed much of the iter14 residual, making the stale weights nearly orthogonal to the actual baseline.

**Mechanism for why this could flip:** The pooled judge found +0.385pp top1 (positive point) but LB95 barely negative (−0.0027). The weakness was: stale residual. A fresh HPO calibrated against iter25/26 as the actual reference score distribution could potentially recover a genuinely positive LB95 if the signal is there. The question is whether σ ≈ 0.48/√11703 ≈ 0.0044 allows LB95 > 0 at a higher point estimate. Requires nominally Δf2p > ~0.007pp for LB95 to clear zero — plausible if the residual is recalibrated.

**NAR precedent:** iter36 gained +0.342pp top1 for NAR-C with a fresh HPO. The structural argument for JRA classes 016 (+1.93pp top1 point estimate in pooled judge) and 005/703 is similar: they have positive point estimates that need better residual calibration.

**Cheapness:** Medium — requires fresh Optuna HPO for 2-4 JRA classes using iter25/26 ensemble scores as `base_score_feature`. The training data and feature parquets exist. Script pattern: identical to `tmp/jra-lgb/` with updated `base_score_feature` path.

**Risk:** Same as NAR-C: LambdaRank concentrates on top1 at the cost of place3. If place3 regression exceeds −0.05pp veto floor, user would need to apply the same win-priority override as NAR-C. **The class most worth attempting first is 016 (point estimate +1.93pp top1) — highest signal per unit effort.**

---

### Rank 3 — Graded relevance scheme D: full-system judge for JRA iter25/26: METHODOLOGY HOLE

**What it is:** Scheme D adds epsilon float labels to positions 4–12 (0.10, 0.08, 0.05, 0.02) while keeping top-3 at {3, 2, 1}. CatBoost YetiRank natively supports float labels.

**Flaw exploited:** The graded-relevance-experiments.md (R01/R26) tested scheme D at the **base-model level only** (3-fold WF 2023-2025) and got REJECT (f2p LB95 −0.00357). But per the graded-relevance supersession note, the full-system deploy judge (iter25/26 ensembles as production baseline) was **never run for JRA**. For NAR, the base-model WF ADOPTED scheme-D but the full-system judge REJECTED it because the per-class residuals were calibrated to the old base score distribution. The same full-system deploy test was never applied to JRA.

**Why this matters for JRA specifically:**

- JRA scheme D REJECTED at base-model level (3-fold). The NAR full-system judge pattern (ADOPT base → REJECT full-system) is the danger pattern.
- But the JRA base-model was already REJECT — so the full-system judge would need to IMPROVE over the base-model reject. This is possible only if iter25/26 per-class ensembles already compensate for the base-model regression, making scheme-D net-neutral or positive at the production level.
- This is a weaker re-open than Rank 1/2. However, the relevance-scheme-audit (2026-06-13) explicitly states "sub-4 graded relevance is genuinely untested" for JRA production iter14 (confirming the base-model REJECT is at the wrong baseline). A retrain of iter14 with scheme D as the objective, then judging against iter25/26 with the same full-system protocol used for NAR, would resolve this definitively.

**Cheapness:** Medium — retrain iter14 base with scheme D labels, then judge at full-system level. The training infrastructure exists. Total compute: ~3-4 hours (21-fold retrain + judge).

**Risk:** If the base-model still regresses under scheme D for JRA, the full-system judge will also fail. The NAR precedent (base PASS → full REJECT) suggests caution; but the JRA precedent is slightly different (base REJECT → unknown full-system direction). Low but non-zero probability of finding a gain.

---

### Rank 4 — h-official-running-style as a marginal re-open (with RS v3 recalibration)

**What it is:** `jvd_um.kyakushitsu_keiko` (JRA official historical nige-vs-oikomi fraction). Partial ρ = 0.074 full / 0.068 holdout — highest of any recent probe batch, just below the 0.08 gate.

**Flaw exploited:** The ABORT was decided at ρ = 0.068 holdout (sub-gate). However:

1. The probe used `rs_p_*` features as controls (the partial ρ conditioned on log-odds AND rs*p*_). The rs*p*_ features have 90.7% NULL rate (per corpus memory). Conditioning on a 90.7% NULL feature is a noisy partial-ρ computation — the effective sample size for the ρ estimate is much smaller than the raw n.
2. The RS v3 model is now deployed (accuracy 48% JRA, up from earlier versions). If rs*p*\* NULL rate has decreased since the probe, the partial ρ may be underestimated because more of the rs signal is captured, leaving a smaller residual for `kyakushitsu_keiko` to explain.

**Why this is a weak re-open:** The ρ = 0.068 is below gate by 0.012 (15% margin). Even if the measurement error is in the right direction, a re-run is unlikely to flip unless the rs*p*\* confound is substantial. This is NOT recommended as a priority experiment.

**Cheapness:** Very low — re-run the probe with updated rs*p*\* coverage.

---

### Rank 5 — Running-style coverage gate (R14) composition with CORNER_POSITION_GAIN

**What it is:** R14 (nige_vs_field, oikomi_in_fast_field) was ABORTED on JRA for coverage: running-style coverage 87% → needs 100% for training. The rationale was that RS-NULL horses would be excluded from the training split.

**Composition gap:** The `kohan3f_firm_avg5` feature (Rank 1 above) passes the gate WITHOUT requiring running-style coverage. If Rank 1 is adopted and demonstrates value, the question arises: does the RS relationship-feature bundle (R14) now become viable as a second-layer addition on top of the updated model? The coverage concern applies equally, so this is NOT a re-open in isolation — but it becomes a plausible second step if (a) Rank 1 succeeds and (b) RS coverage improves.

This is a future-conditional composition, not an immediately actionable re-open.

---

## 4. Confirmed Clean Rejects (do not re-propose)

The following received clean methodology and are not reopenable under any scenario:

- All exotic odds JRA (fukusho/umaren/wide/sanrenpuku): market efficiency is structural, confirmed multi-probe
- Log_odds_z and within-race relative features: GBDT already captures non-linearly, confirmed with NULL-routing re-verify
- All workout features (jvd_hc, jvd_wc): market efficiency absorbs training; ρ < 0.04 confirmed
- Class-signal features (iter18): fully subsumed by existing kyoso_joken/grade/trainer features
- jockey×trainer combo win-rate: ρ < 0.007; structurally redundant
- RS calibration / post-hoc decision rules: calibrated argmax is already at decision-rule frontier
- James-Stein partial pooling: mathematically futile (JS retain = 0.9937 ≈ identity)
- Transformer (MLX): underpowered test — see note below
- NAR G-1+F1 combined retrain for JRA: not directly applicable; lesson is that NULL-routing optimization is counterproductive

**Transformer note (R18 — methodology hole but DEPRIORITIZED):** The rootcause-i6-architecture doc identifies that the transformer was tested on only 2 WF folds (2024-2025) with no HPO, which is an underpowered methodology. However, this is explicitly classified as "MEDIUM EFFORT (~3 days)" and "unlikely to beat GBDT without new features." The architecture itself is not the binding constraint. This is acknowledged as a flaw but deprioritized because even a correct test is unlikely to move the needle at current data scale (Grinsztajn 2022 tabular GBDT advantage). Not ranked in the top-5.

---

## 5. Serve-Distribution Re-Open Assessment

**Conclusion: no serve-distribution re-opens apply to signal-search REJECTs.**

All WF experiments used historical final odds (the "WF" / "NEW condition") as the feature vector. The Phase 3 serve-fix (06/11-06/14) closed the gap between serve-time distribution and WF distribution — it did not change what WF experiments measure. A feature that showed no signal in WF (final odds available) would show the same no-signal in a correctly-served environment. The serve fix changes serve accuracy toward WF accuracy; it does not change which signal experiments are positive.

The only caveat: experiments run on PRE-serve-fix holdout data (pre-06/11) that used ACTUAL serve-time features (median fallback) as the baseline would be invalidated. None of the signal-search experiments did this — they all used WF features as controls.

---

## 6. Summary Table

| Rank | Re-open / Gap                                                | Type                           | Flaw exploited                                                  | JRA classes             | Cheapness       | Mechanism strength                           |
| ---- | ------------------------------------------------------------ | ------------------------------ | --------------------------------------------------------------- | ----------------------- | --------------- | -------------------------------------------- |
| 1    | `kohan3f_firm_avg5` + `kohan3f_soft_avg5` model test         | Untested (gate passed)         | No model test ever run; ρ gate already cleared                  | All (global feature)    | Low             | HIGH — ρ=0.1286, non-redundant               |
| 2    | Fresh LambdaRank HPO on iter25/26 ensembles                  | Asymmetry gap                  | iter14 used as base in all prior judges; stale residual weights | 016 first, then 703/005 | Medium          | MEDIUM — pooled point+0.385pp; NAR precedent |
| 3    | Scheme D full-system judge (iter25/26)                       | Methodology hole               | Base-model-only reject; full-system judge never run             | All (global retrain)    | Medium          | LOW-MEDIUM — base-model was already REJECT   |
| 4    | h-official-running-style with clean rs*p*\* partial ρ re-run | Marginal re-open               | Partial ρ may be underestimated with 90.7% NULL control         | All (global feature)    | Very low        | LOW — ρ=0.068, 15% below gate                |
| 5    | RS relationship-features after Rank 1 adoption               | Future conditional composition | Coverage gate still applies                                     | 005/010/016/other       | Medium (future) | LOW (conditional)                            |

---

## 7. Recommended Next Action

**Execute Rank 1 immediately:** Add `kohan3f_firm_avg5`, `kohan3f_soft_avg5`, and `kohan3f_going_diff` to the JRA feature pipeline and run an incremental WF judge against the iter25/26 production baseline. This is the only hypothesis in the corpus that passed the ρ gate, cleared redundancy, and has zero prior model-test result. All other re-opens require retraining or architectural changes.

**Rank 2 (if Rank 1 succeeds or is inconclusive):** Fresh LambdaRank HPO for JRA class 016 specifically, using `iter26-jra-cb-ensemble-016-v8` scores as `base_score_feature`. The +1.93pp top1 point estimate in the pooled judge is the largest positive signal anywhere in the JRA corpus; the only reason it was REJECT was stale residual calibration and insufficient n=727 holdout for 016 alone.

---

## 8. Sources

All findings sourced from:

- `docs/finish-position-accuracy/history/triplet-verify-p1.md` — Rank 1 PROCEED verdict
- `docs/finish-position-accuracy/history/jra-pooled-residual-judge.md` — pooled +0.385pp top1, LB95 −0.0027
- `docs/finish-position-accuracy/history/jra-lgb-lambdarank-full-judge.md` — iter14-based judge
- `docs/finish-position-accuracy/history/oi-2026-06-10-iter36-lgb-lambdarank-residual-C-adopt.md` — JRA follow-on stated
- `docs/finish-position-accuracy/history/graded-relevance-experiments.md` — scheme D base-model only
- `docs/finish-position-accuracy/history/relevance-scheme-audit.md` — "sub-4 genuinely untested" confirmation
- `docs/finish-position-accuracy/history/rootcause-i6-architecture.md` — transformer 2-fold flaw
- `docs/finish-position-accuracy/history/h-official-running-style.md` — ρ=0.074 highest ABORT
- `docs/finish-position-accuracy/history/jra-relationship-features-perclass.md` — coverage-gate ABORT
- `docs/finish-position-accuracy/history/banei-exotic-extended-rejudge.md` — extended-holdout pattern
