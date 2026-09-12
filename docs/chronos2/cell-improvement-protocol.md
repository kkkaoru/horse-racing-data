# Multi-cell Chronos-2 accuracy follow-up

## Request and fixed boundaries

The follow-up requests actual improvement of Top1–Top5 on multiple cells, with independent Rustuna tuning when applying fine-tuned models. Previous global MAE improvements and the rejected 95% market blend are not success criteria.

All work stays in this checkout, with campaign artifacts in `.cache/chronos2/cell-improvement/`. Preserve incumbent predictions, heatmaps and unrelated edits. No live application without cell-specific accuracy, PIT, roster, artifact and parity approval.

## First diagnostic phase, declared before cell outcome inspection

1. Export the canonical JRA cell mapping from local PostgreSQL through 2026-09-06 using the existing named-open-aware `cell_for_race`. Check race identity uniqueness and exact joins to the frozen corrected history. Do not invent replacement broad cells from venue/distance alone.
2. Select up to six cells by **2023 complete-source race count only**, descending count and then cell ID, requiring at least eight races. Exclude unnamed generic-open identities. Do not select cells by forecast improvement.
3. Diagnose the already-produced head/LoRA portable CPU development forecasts: missing-history coverage, forecast spread/calibration, within-race ordering and Top1–Top5 versus market. Compare raw scalar blending to within-race percentile representations on development only. Label final-odds and finisher-roster proxies explicitly.
4. Locate or reconstruct the exact incumbent comparison separately. Missing incumbent data is not equivalent to a market comparison; it blocks production eligibility, not research diagnostics.

## Domain and metric clarification before any real trial

Separate **JRA / NAR flat / Ban-ei** training labels, objective instances, studies, seeds and output namespaces. Flat JRA/NAR transfer history can remain strictly earlier context; Ban-ei must not mix with flat racing. Historical Ban-ei venues 81, 82 and 84 belong with current venue 83, not NAR flat. The current six-cell cohort is the JRA phase; NAR and Ban-ei require separate cohort manifests and runs. This implementation is supervised fine-tuning, not reinforcement learning.

The repository's existing exact-position protocol defines Top1–Top5 as **exact finishing-position matches**, not winner-in-TopK recall. Make exact ranks the primary trial objective and retain winner-in-TopK only as a separately named auxiliary metric, with per-rank support counts. Earlier `development-diagnostic.json` remains an explicitly auxiliary winner-coverage diagnostic; its three passing cells are not exact-rank passes. No real training trial had started before this clarification, and cell selection remains count-based and unchanged.

## First real cell-training round (fixed before execution)

Run 16 serial Rustuna trials for each of the six count-selected cells; use TPE with four startup trials, then adaptive proposals. Each trial actually trains a fresh head/LoRA model from the pinned official base, for 100–400 steps, and evaluates the exported portable CPU artifact on the exact 2023 target races. Keep all attempted configurations, infeasible-batch records, model hashes and CPU readouts. No model/data/holdout publication or production application.

Reuse supported learning-rate/decay/warmup/rank/context/batch search bounds. Allow minimum history 1–4 to test the observed sparse-maiden-history problem. Search raw versus tie-preserving within-race percentile representation and market blend weight. These are development searches, not holdout tuning. Maximize Top1 with nonnegative Top2–Top5 development deltas; retain all failures and evaluate every selected cell, including those with no passing trial.

Inputs: frozen 2023 target entrants plus same-cell 2020–2022 peer entrants, their history from 2000 through 2023. The causal cross-source audit found 5,328 NAR context rows for 1,832 horses before their latest JRA entry in this bounded source. NAR race-relative ratings were computed on complete NAR races before selecting horses. Training and development **labels are JRA-only**; NAR observations remain available as strictly earlier context. History files have no 2024+ outcomes. The combined source has 51,120 rows, no duplicate horse/date observations, and no performance ratings outside [0,1]. This retrospective roster construction is not independent live starter/PIT attestation.

The exact incumbent comparator is not available for this phase. Therefore use a separate, explicitly **market-only research** driver; never fill the incumbent column with market metrics or weaken the incumbent-aware application tuner. A selected research trial is not production eligible. Freeze any development selections before one subsequent readout on the previously observed 2024–2026 period. If an execution/data-integrity failure interrupts the round, preserve partial evidence and resolve it before continuing; do not change the protocol based on holdout results.

## Subsequent model work

Use diagnostic findings to choose a bounded next adaptation/search protocol before opening further readouts. Distinguish optimizer/training hyperparameter tuning from post-model blend/calibration search. Cell-local Rustuna training must execute its sampled configuration and record artifact and cohort identities; cached readout searches alone are not evidence that training hyperparameters were optimized.

The primary hypothesis is objective/input mismatch: smaller unconditional scalar forecast MAE need not improve within-race ranking, and compressed forecast scales can make market blending ineffective. Investigate ranking-aware targets/losses, alternative strictly historical temporal features and race-relative calibration based on development evidence rather than assuming more updates solve this.

2023 has already been examined during global development. 2024–2026 have been examined by earlier TimesFM work and the rejected Chronos head readout. Any reuse must be reported as sequential follow-up on previously observed years, not globally untouched validation. Do not retune on annual holdout results or today's completed races.

Success requires reporting race counts and Top1–Top5 for each selected cell, not only aggregate MAE or a cherry-picked winning cell. Preserve failures and sparse-cell uncertainty. Production requires improvement against exact incumbent and market with annual Top2–Top5 nonregression and all identity/PIT/runtime gates.
