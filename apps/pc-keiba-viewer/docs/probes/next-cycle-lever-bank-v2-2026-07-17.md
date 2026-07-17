---
probe: next-cycle-lever-bank-v2
date: 2026-07-17
category: jra/nar/banei
method: read-only scout — no training, no code changes, no MLflow writes
status: SCOUT COMPLETE — see tmp/frontier-scout/lever_bank_v2.md for the full inventory (not committed; this is the summary)
---

# Next-Cycle Lever Bank v2 — Scout Summary (2026-07-17)

Successor to `lever_bank.md` (2026-07-11), which is now fully consumed —
every candidate it raised (wide/馬連 cross-pool divergence, the #37
volatility-tiered-fusion follow-ons, vector search per USER's new
instruction, season-conditional jockey/trainer form) was closed today. This
scout systematically searches for **structural dimensions** (learning
algorithm/loss function, architecture, and time-gated reopenable threads)
rather than more feature engineering, given how thoroughly the
feature-engineering axis has been searched (8+ lever families REJECTed today
alone). Full inventory with citations:
`apps/pc-keiba-viewer/tmp/frontier-scout/lever_bank_v2.md` (not committed,
per repo convention — this doc is the committed pointer + summary).

**Live overlap caveat**: a same-day "wave7" effort is running a CatBoost
hyperparameter HPO sweep and an XGBoost/LightGBM base-learner face-off in
parallel with this scout. Neither directly duplicates this doc's
loss-function-family or architecture-axis candidates, but check wave7's
actual outcome docs before picking up candidate 1 or 4 below in case its
face-off already covered adjacent ground.

## Ranked candidates (best-first, full detail + citations in the tmp/ doc)

1. **LightGBM `lambdarank_truncation_level` sweep + `rank_xendcg`
   feasibility** — cheapest, cleanest gap. Two existing CLI flags never
   swept away from defaults in any experiment ever run. Low-modest prior.
2. **Ban-ei loss-function-family axis** — completely untested at every
   level (not even a relevance-scheme WF was ever run for Ban-ei). Cheap
   (smallest dataset). Low prior, but fills the most complete gap.
3. **Monotonic constraint variations on the CatBoost champion** — genuinely
   untested, mechanistically distinct from every closed ensemble/blend/
   stack lever (verified structurally compatible with `CatBoostRanker` via
   the installed library source, not just doc prose). Cheap. Low-to-moderate
   prior — this repo's dominant finding pattern ("GBDT already implicitly
   learns explicitly-encoded structure") argues for caution, but a
   regularization effect in sparse tail populations is plausible.
4. **Direct CatBoost `loss_function` swap on the production JRA/Ban-ei
   path** — no CLI flag exists yet for this (needs a small addition); the
   only related test swapped loss families for a NAR residual member, never
   the CatBoost production loss itself. Modest-low prior (nearest analog
   passed inner CV/WF but failed the strengthened holdout gate for 2/3
   classes).
5. **JRA graded sub-4 relevance scheme (Scheme D), full-system judge vs. the
   current baseline** — the repo's own prior audit explicitly labels this a
   "METHODOLOGY HOLE" (JRA never got NAR's full-system judge, only a
   base-model test against a now-twice-retired baseline). Medium cost. Low
   prior (the base-model result already failed outright, baseline-independent).
6. **JRA loss-family/residual axis vs. the current baseline** — already
   failed decisively once against a retired baseline; a documented
   false-positive-then-reversed pattern on a sibling experiment is a direct
   cautionary precedent. Medium cost, low prior.
7. **NAR-C place-preserving multi-objective loss (λ-tuned)** — proposed in
   the per-class ROADMAP in June, never executed. Cheap-medium, low-modest
   prior (a harsher cousin construction was catastrophic, but this one is
   softer/constrained).

**Not recommended as a priority slot, included for completeness**: GBDT
self-distillation via soft-label smoothing — zero precedent exists anywhere
in this codebase, but the nearest tested analog (label-perturbation via an
auxiliary signal) passed loose selection and then failed blind confirmation,
and today's stacking-meta-learner REJECT (`z_base` dominates 19-49x even
with strictly more information available to it than a distillation student
would have) argues the same wall likely applies. Multi-task/multi-head and
quantile-regression-head variants are **closed by extension**: verified
structurally incompatible with the production `CatBoostRanker` loss slot in
the installed library source, and two independently-tested alternate
architectures (7-class-Hungarian, neural multi-head transformer) both
already failed on JRA.

## Time-gated reopen ledger (5 rows, full detail in the tmp/ doc)

A reservation table — threads closed **for now**, not permanently, pending a
stated precondition:

| Thread                                                      | Trigger                                                                                                  |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Champion freshness retrain (v34 challenger) blind flip gate | Blind n>=200 races, starting weekend of 2026-07-18/19                                                    |
| 269/venue02 serving-defect live confirmation                | Tonight's NAR check + tomorrow's 2026-07-18 09:25 JST JRA cron (first fixed-pipeline venue02 prediction) |
| `corner-features-refresh.ts` cron wiring confirmation       | Tonight 22:00 JST + tomorrow 09:15 JST tick                                                              |
| Upset-scan 2026-07 behavioral/mechanism mining              | 2026-07-18 clean (non-Cluster-B-contaminated) serve data                                                 |
| RS→FP linkage reopening                                     | Event-based: only if a genuine, WF-gated RS v2 artifact is ever built                                    |

Also explicitly checked and excluded from this ledger as permanent
(non-time-gated) closures: wide/馬連 cross-pool divergence, Sapporo
accepted-deficit, vector-knn/longshot-detector v1+v2, contender-set-meta-
reorder, RL-formulation, and the NAR corner-features 07-13/14/15 gap
(Neon-side already healed; local-PG residual is accepted, not pending).

## Methodology

Three parallel read-only research passes (learning-algorithm archaeology
across both WF trainer scripts + exhaustive grep sweep; time-gated-ledger
compilation from today's full campaign log + both memory index files +
every 2026-07-17 probe doc; architecture-axis assessment via the 2026-06-17/18
per-class ensemble campaign docs + direct verification of CatBoost/XGBoost's
actual installed loss-compatibility gating, not just doc prose). One stale
prior-doc claim caught and flagged rather than propagated (a relevance-scheme
audit's claim about the XGBoost trainer's hardcoded relevance mapping does
not match current code). No training runs, no MLflow writes, no code edits.
