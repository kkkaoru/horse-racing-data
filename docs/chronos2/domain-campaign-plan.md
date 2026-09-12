# Separate Chronos-2 domain campaigns

## Separation contract

These are supervised fine-tuning experiments, not reinforcement learning. JRA, NAR flat and Ban-ei have separate labels, objective evaluations, Rustuna studies/seeds, selected parameters, model directories and result tables. The public pretrained Chronos-2 base can be common; fine-tuned weights and selections cannot be silently shared.

- JRA labels: domestic venue codes 01–10 from JRA sources.
- NAR flat labels: regional flat racing codes 30–79 from NAR sources.
- Ban-ei labels/history: current venue 83 and historical venues 81, 82, 84.
- Strictly prior JRA/NAR flat transfer histories may remain contextual observations. No flat/Ban-ei context mixing.
- Context selection is distinct from label selection. Do not narrow every horse to same-cell or same-current-venue history.

`chronos_domains.py`, `StudyConfig.training_domain`, `CellScope.domain` and `ResearchCell.domain` implement these boundaries. The research CLI requires an explicit domain in each cohort declaration and writes `<output>/<domain>/<cell>/`.

## Current JRA phase

Six count-selected 2023 cells; actual 16-trial/cell training and CPU evaluation is running in `cell-improvement/real-round-001/jra/`. Follow `cell-improvement-protocol.md`. Do not alter this round's settings while it runs.

## Subsequent NAR flat and Ban-ei phases

Before inspecting their Chronos outcomes, independently select up to six complete-source cells per domain by 2023 race count (descending count, deterministic cell-ID tie-break, minimum eight races). Use the existing local canonical research-cell dimensions and disclose the available venue coverage; do not describe a subset as all NAR venues. Validate identities/complete race groups against source data before fitting.

Keep the same initial 2020–2022 training-label / 2023 development split, history back to 2000, separate domain-specific 16-trial studies, and primary exact rank1–rank5 counts with support. Start with the same bounded optimizer search protocol, not JRA's selected settings. Any domain-specific objective/feature change must be declared before that round's outcome inspection.

Local NAR/Ban-ei TimesFM preparation artifacts may inform metadata/provenance discovery, but their `baseline_score` comes from earlier ablation models and is **not an independently attested current production incumbent**. The observations file includes 2024+ rows and placeholder horse IDs; never pass it wholesale to development training. Reconstruct or explicitly filter/freeze causal inputs and validate registry identities. Retain native NVD exchange-race observations rather than duplicating JVD/NVD physical races; compute race-relative values on complete races before cohort filtering.

Exact rank matches and winner-in-TopK recall are different metrics. Optimize/report exact rank1–rank5 as primary, and keep winner coverage auxiliary. No phase result is a production approval. Current incumbent/PIT/starter/parity attestation and frozen subsequent evaluation remain required before application.
