---
science_track_entry: true
hypothesis_id: H-BABA-PAR-TIME
date: 2026-06-11
based_on_iteration: iter30-nar-cb-residual-*-v8 (production baseline)
scope: NAR (all keibajo except Banei), per-class residual ensemble
status: REJECT — powered judge FAIL all 7 NAR classes
verdict: REJECT — par-time baba-adjusted features do not improve holdout accuracy
production_change: none
artifacts:
  retrain_script: tmp/nar-perclass/sci_track/v8_partime/retrain/wf_retrain_v8_partime.py
  judge_script: tmp/nar-perclass/sci_track/v8_partime/retrain/judge_v8_partime.py
  judge_holm: tmp/nar-perclass/sci_track/v8_partime/retrain/judge_holm.json
  verdict: tmp/nar-perclass/sci_track/v8_partime/verdict.json
  probe_record: docs/finish-position-accuracy/history/sci-track-2026-06-10-h-baba-par-time.md
---

## Summary

**H-BABA-PAR-TIME: REJECT** — All 7 NAR classes fail the powered judge gate.

The probe showed a strong partial Spearman ρ=0.180 (bar=0.08, 2.25×) for
`baba_adj_centered` (going-adjusted speed correction term). However, after
full walk-forward (WF) retrain on 177 features (iter30 174 + 3 baba-adj) and
powered holdout evaluation (2023–2026), none of the 7 classes pass the gate.

**Root cause:** Probe signal reflects a data-level correlation (race-condition
selection bias or par-time normalization artifact) that does not transfer to
actual predictive lift in WF holdout. Class C is the narrowest miss (3/4 axes
positive, bootstrap LB95=−0.054pp just below zero). Class A produces
w_v8=0.00 optimal blend (no contribution). Most other classes show flat or
negative top1 deltas.

## Walk-Forward Retrain

All 7 classes retrained: C, B, other, A, OP, MUKATSU, NEW.
Model: CatBoost YetiRank, depth=4, lr=0.1, l2=5.0, 500 iter early-stop=30.
Feature count: 177 (iter30 174 + `baba_adj_centered`, `past_speed_baba_adj_avg5`, `baba_adj_speed_best3`).
WF years: 2018–2026.

| Class   | Folds trained     | Total time |
| ------- | ----------------- | ---------- |
| C       | 4 new (2023–2026) | 118s       |
| B       | 2 new (2025–2026) | 60s        |
| other   | 4 new (2023–2026) | 219s       |
| A       | 9 new (2018–2026) | 338s       |
| OP      | 9 new (2018–2026) | 276s       |
| MUKATSU | 9 new (2018–2026) | 77s        |
| NEW     | 9 new (2018–2026) | 63s        |

## Powered Judge Results

Holdout: 2023–2026. Gate requires: ≥2 of {top1, place2, place3, top3_box}
positive, ≥1 place axis positive, no axis < −0.05pp, bootstrap LB95 > 0.

| Class   | Gate | n_pos | n_place_pos | LB95 (pp) | top1 (pp) | place2 (pp) | place3 (pp) | top3_box (pp) | Fail reason                          |
| ------- | ---- | ----- | ----------- | --------- | --------- | ----------- | ----------- | ------------- | ------------------------------------ |
| C       | FAIL | 3     | 1           | −0.054    | +0.096    | +0.073      | 0.000       | +0.046        | bootstrap LB95 < 0 (barely)          |
| B       | FAIL | 2     | 1           | −0.505    | −0.154    | +0.351      | −0.014      | +0.168        | top1 negative, LB95 << 0             |
| other   | FAIL | 1     | 0           | −0.443    | −0.166    | −0.028      | 0.000       | +0.028        | n_positive=1, no place axis positive |
| A       | FAIL | 0     | 0           | 0.000     | 0.000     | 0.000       | 0.000       | 0.000         | optimal blend w_v8=0.00 (no signal)  |
| OP      | FAIL | 1     | 1           | −0.975    | −0.325    | +0.163      | 0.000       | 0.000         | n_positive=1, LB95 << 0              |
| MUKATSU | FAIL | 2     | 1           | −0.180    | +1.259    | 0.000       | +0.180      | −0.180        | top3_box < −0.05pp, LB95 < 0         |
| NEW     | FAIL | 2     | 1           | −0.175    | +0.349    | +0.524      | 0.000       | −0.175        | top3_box < −0.05pp, LB95 < 0         |

Holm correction: 0 classes passed the individual gate → no Holm evaluation needed.

**Blend weights optimized (inner 2018–2020, tuning 2021–2022):**

| Class   | w_comparison | w_v8 | comparison baseline  |
| ------- | ------------ | ---- | -------------------- |
| C       | 0.30         | 0.70 | iter30 CB WF         |
| B       | 0.45         | 0.55 | iter12 (no ensemble) |
| other   | 0.45         | 0.55 | iter30 CB WF         |
| A       | 1.00         | 0.00 | iter30 CB WF         |
| OP      | (default)    | —    | iter30 CB WF         |
| MUKATSU | 0.45         | 0.55 | iter30 CB WF         |
| NEW     | 0.65         | 0.35 | iter30 CB WF         |

## Per-Year Analysis (Class C, narrowest miss)

Class C had the most promising result. The per-year breakdown shows:

| Year | top1 delta (pp) | Notes                                   |
| ---- | --------------- | --------------------------------------- |
| 2023 | +0.092          | Positive                                |
| 2024 | −0.026          | Slight negative                         |
| 2025 | +0.052          | Positive                                |
| 2026 | +0.547          | Strongly positive (partial year, n<1yr) |

2026 lift is based on only partial-year data (Jan–June) and may be noisy.
Venue concentration: BROAD (7 positive, 7 negative of 14 venues; concentration ratio=0.52).

## Interpretation

The probe signal (partial ρ=0.180) was genuine at the data level — going-adjusted
speed indices do contain orthogonal information beyond the existing speed family.
However, that information does not translate to accuracy gains in the WF holdout:

1. **Race-condition selection bias**: Horses with many heavy-going starts are
   systematically different (lower grade, rural venues, different trainer
   strategies). The par-time correction may be confounded with horse quality
   rather than measuring pure going adaptability.

2. **Par-time window mismatch**: The par-time table was frozen at 2007–2017.
   Track conditions and preparation practices at NAR venues have changed,
   reducing the calibration validity in 2023–2026.

3. **Race-level vs horse-level signal**: The correction term is a within-race
   deviation (baba_adj_centered = horse adj speed − race avg adj speed). This
   captures relative speed on a going-adjusted basis but does not capture
   horse-level ADAPTATION to different going conditions. The genuine signal
   would require a history of performance variance across going types per horse.

4. **MUKATSU/NEW: small-sample instability**: 556 and 573 races in holdout.
   The top1 deltas for MUKATSU (+1.259pp) appear large but are not bootstrap-
   confirmed (LB95=−0.180pp), and top3_box regresses, indicating overfitting
   to a small tuning set.

## Verdict

**REJECT — H-BABA-PAR-TIME does not meet the powered gate.**

No production change. The 177-feature WF models are archived in
`tmp/nar-perclass/sci_track/v8_partime/retrain/` but will not be deployed.

## Refinement Directions (Post-REJECT)

If going-adjustment is worth revisiting:

1. **Horse-level going variance**: Instead of race-centred correction, build a
   feature capturing each horse's _variance_ in speed across baba codes — horses
   that consistently run faster on heavy going score positive, those that prefer
   good going score negative. This is a horse-level interaction, not a race-level
   normalization.

2. **Update par-time window**: Refit par-time table on 2017–2022 (sliding window)
   to reflect current track conditions. The 2007–2017 freeze may be too stale.

3. **Interact with running style**: baba adaptation may differ by running style
   (pace-setter vs closer). A `baba_adj_centered × running_style` interaction
   feature could isolate the relevant slice.

4. **Within-horse going delta**: For each horse, compute the difference in average
   finishing position on heavy (baba=3,4) vs good (baba=1) going, with at least
   3 races on each type. Use as an explicit going-preference indicator.

## Hard Rules Observed

- `tmp/` only: all retrain artifacts in `tmp/nar-perclass/sci_track/v8_partime/`
- No `git add tmp/` (parquet predictions, summaries, verdicts stay untracked)
- PG read-only throughout (no writes to any table)
- No production deployment or active_models registry change
- Bootstrap n=10000, seed=42 per protocol
- Holm α=0.05, no-regression threshold −0.05pp
