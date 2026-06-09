---
iteration: 32
date: 2026-06-09T23:35:00+09:00
based_on_iteration: 30-31
lever: L-venue-targeted-Oi (reweight / venue-features / Oi-specialist / training-logic-audit)
status: REJECT all 4 levers — no new model beats production per-class config on the Ōi (keibajo=44) card
quality_gate: n/a — no code change this round (verification + docs only)
model_version_jra: iter14-jra-cb-pacestyle-course-v8 (UNCHANGED — no JRA card on 2026-06-10)
model_version_nar: per-class production config (UNCHANGED)
scope:
  venue: 大井 Ōi (keibajo_code=44)
  target_card: 2026-06-10 (12 races)
  goal: improve finish-position accuracy specifically for the Ōi card without regressing the global NAR holdout
baselines:
  source: tmp/nar-perclass/oi_round_analysis.json
  validation_years: [2018, 2019, 2020, 2021, 2022]
  holdout_years: [2023, 2024, 2025, 2026]
  oi_44_total_races: 24987
  holdout_oi_per_class: # top1 / place2 / place3 / top3_box (%) on Ōi holdout slice
    C: { top1: 47.35, place2: 25.62, place3: 18.36, top3_box: 17.74, n_races: 1928 }
    other: { top1: 52.48, place2: 29.90, place3: 18.38, top3_box: 22.67, n_races: 1050 }
    B: { top1: 51.41, place2: 29.00, place3: 21.09, top3_box: 22.22, n_races: 531 }
levers_tried:
  - id: reweight
    desc: Optuna TPE 300-trial simplex over (iter12 anchor ≥0.20, iter30 residual, iter31 mid) maximising Ōi-validation top1 under HARD global non-regression (global top1 Δ ≥ -0.05pp)
    result: REJECT — best candidate fails Ōi top1 gate (C candidate Ōi top1 Δ=-0.1037pp, global non-regression also breached); binding_constraint=oi_top1_not_improved
  - id: venue-features
    desc: "7 Ōi-specific features added to per-class CatBoost: oi_umaban_top3_rate, oi_waku_top3_rate, oi_umaban_avg_finish_norm, oi_umaban_avg_corner4_relpos, horse_oi_top3_rate, jockey_oi_win_rate, trainer_oi_win_rate (173→180 cols)"
    result: REJECT — all classes FAIL; new-feature importance share ≈0.07–0.16% (≈0%); deltas vs round-0 baseline all negative (C top1 -1.97pp, other top1 -2.10pp, B top1 -3.39pp)
  - id: oi-specialist
    desc: per-year walk-forward Ōi-specialist (train all-NAR class chain ≤Y-1 with keibajo=44 time_decay×3 group weight, predict Ōi target-class slice for Y) + pooled race-level paired bootstrap ×10000 seed=42
    result: REJECT_as_noise — top1 robust but NO place axis LB95>0 on either eligible class (other place2 LB95=-1.619; B place3 LB95=-3.0132)
  - id: training-logic-audit
    desc: audit whether per-class training applies time-decay correctly (no leak / no double-count) and whether group-constant weighting is sound
    result: NO BUG — time-decay correctly applied via group-constant weight; nothing to fix
decision: KEEP production per-class config for Ōi. The genuine deployed improvement reaching the 2026-06-10 card is the already-COMMITTED P0 per-class routing fix (commits 869c223 / b62169d / 6b21e03 / decbfc1), now GUARANTEED live by rebuilding finish-position-predict-local:split2 from HEAD=166b566.
artifacts:
  round_analysis: tmp/nar-perclass/oi_round_analysis.json
  specialist_results: tmp/nar-perclass/oi_specialist_results.json # see .robustness section
  feature_results: tmp/nar-perclass/oi_feature_results.json
  prediction_log: ~/Library/Logs/finish-position-predict/20260610.log
---

## What was tried

The accuracy investigation targeted the 2026-06-10 大井 (Ōi, NAR `keibajo_code=44`) card. The question was whether any new model — beyond the production per-class ensemble config — robustly improves finish-position accuracy on Ōi without regressing the global NAR holdout. Four levers were explored against the Ōi-slice holdout (2023–2026), and the strongest candidate (the specialist) was additionally stress-tested with a per-year walk-forward + pooled race-level paired bootstrap to separate signal from sampling noise.

The Ōi-slice baselines (production per-class config, top1/place2/place3/top3_box %, holdout 2023–2026) are:

| class | top1  | place2 | place3 | top3_box | n_races |
| ----- | ----- | ------ | ------ | -------- | ------- |
| C     | 47.35 | 25.62  | 18.36  | 17.74    | 1928    |
| other | 52.48 | 29.90  | 18.38  | 22.67    | 1050    |
| B     | 51.41 | 29.00  | 21.09  | 22.22    | 531     |

## Implementation summary

No production code changed this round — this was a verification + documentation round. The experiment scripts live under `tmp/nar-perclass/` (`oi_round_analysis.py`-style drivers, `oi_specialist_robustness.py`, `oi_feature_experiment.py`) and are intentionally NOT committed (tmp/ is git-excluded). The load-bearing artifact is the local docker image `finish-position-predict-local:split2` rebuilt from `HEAD=166b566`, which bakes the previously committed P0 routing fix.

## Results

### Lever 1 — reweight (Optuna 300-trial simplex, HARD global non-regression)

Re-optimising per-class blend weights over the available member pool failed the Ōi accept gate. The C candidate regressed Ōi top1 (Δ=-0.1037pp) and also breached the global non-regression constraint; binding constraint `oi_top1_not_improved`. **REJECT.**

### Lever 2 — venue features (7 Ōi-specific signals, 173→180 cols)

| class | Ōi top1 Δ vs round-0 | place2 Δ | place3 Δ | box Δ | new-feature importance share | verdict |
| ----- | -------------------- | -------- | -------- | ----- | ---------------------------- | ------- |
| C     | -1.97                | -2.54    | -1.24    | -1.35 | 0.069%                       | FAIL    |
| other | -2.10                | -1.05    | -0.86    | -1.14 | 0.155%                       | FAIL    |
| B     | -3.39                | -2.07    | -0.94    | -0.94 | (negative)                   | FAIL    |

The 7 venue features carry essentially **zero** model importance (≈0.07–0.16% combined share). Adding them dilutes the existing signal and regresses every axis. **REJECT.**

### Lever 3 — Ōi-specialist (per-year walk-forward + pooled paired bootstrap ×10000)

Adopt rule: `(top1 Δpp positive in ≥3/4 years OR pooled top1 LB95>0) AND (≥1 of {place2,place3} pooled LB95>0) AND no year top1 Δ < -1.0pp`.

| class | pooled top1 LB95 | place2 LB95 | place3 LB95 | worst-year top1 Δ | verdict         |
| ----- | ---------------- | ----------- | ----------- | ----------------- | --------------- |
| other | -0.2857          | -1.619      | -0.6667     | -0.3185           | REJECT_as_noise |
| B     | -0.3766          | -1.8832     | -3.0132     | 0.0000            | REJECT_as_noise |

Top1 looked directionally positive (3/4 years up for both classes), but the place axes were the binding failure: **no place axis cleared LB95>0** for either eligible class (other place2 LB95=-1.62; B place3 LB95=-3.01). The apparent top1 lift is indistinguishable from sampling noise. **REJECT_as_noise.**

### Lever 4 — training-logic audit

Audited whether the per-class training pipeline applies time-decay correctly (no leak, no double-count) and whether the group-constant weighting is sound. **No bug found** — time-decay is correctly applied via a group-constant weight. There is nothing to fix; the specialist's failure is real, not an implementation artifact.

## Per-bucket findings

The Ōi holdout decomposes into three registered classes for routing on 2026-06-10: C (47.35 top1), other (52.48 top1), B (51.41 top1). B has no registered ensemble and correctly falls back to the iter12 baseline. Across all four levers, no class showed a robust, paired-bootstrap-confirmed gain on any place axis; the only directional top1 movements were inside the bootstrap noise band.

## Decision

**Keep the production per-class config for Ōi — no new model is adopted.**

The genuine, already-committed improvement that must reach tomorrow's card is the **P0 per-class routing fix** (commits `869c223` NAR per-class ensemble routing, `b62169d` per-member feature matrices + class-code routing, `6b21e03`/`decbfc1` canonical-NULL `shusso_tosu` re-emit + member-column-gap guard, plus the JRA `kyoso_joken_code` exposure in `finish_position_features_duckdb.py`). This routing unblocked the NAR C/other ensembles from a silent baseline fallback.

This round GUARANTEED that committed state is live for 2026-06-10:

1. **Image current** — rebuilt `finish-position-predict-local:split2` from `HEAD=166b566`; build SUCCEEDED (COPY layers content-hash-matched cache = baked content equals HEAD). Verified inside the image: `/app/pipeline/finish_position_features_duckdb.py` contains `t.kyoso_joken_code as kyoso_joken_code` (line 1430) and `/app/pipeline/finish-position-features/add-near-miss-features.py` contains `cast(null as bigint) as shusso_tosu` (line 538).
2. **Predictions populated** — ran the production daily wrapper `RUN_DATE=20260610 finish-position-predict-daily.sh`; docker exit code=0, `races_predicted=518`, idempotent UPSERT into `race_finish_position_model_predictions` on Neon. Zero `score-error` / `member-column-gap` / `member-metadata-missing` / `ensemble fallback` lines in the run log.
3. **Routing verified in Neon** — read-only SELECT confirmed all 12 Ōi races present with the expected routing:
   - C (R1, R2, R6, R8, R9) → `iter30-nar-cb-ensemble-C-v8`
   - other (R3, R4, R5, R11) → `iter30-nar-cb-ensemble-other-v8`
   - B (R7, R10, R12) → `iter12-nar-xgb-hpo-v8` (baseline fallback — correct, B has no registered ensemble)

   The full 2026-06-10 NAR distribution (keibajo 30/44/47/50) also shows correct per-class routing across C, other, A, NEW, MUKATSU ensembles — exactly the behaviour the P0 fix unblocked. No JRA card on 2026-06-10.

The launchd cron (JST 03:00) re-running on 06-10 is harmless: it reuses the now-current image and re-UPSERTs the same rows. The Cloudflare Container cron remains disabled (Containers reap batch instances at ~90–110s; this workload needs ~10min), so the local image is the only load-bearing prediction artifact — no `wrangler deploy` is involved.

## Next iteration recommendation

A same-night lever (reweight / hand-engineered venue features / specialist on the existing feature set) is exhausted for Ōi — all four were rejected, three on robustness grounds and one (audit) confirming there is no bug to exploit. The next genuine improvement requires a **new horse/venue signal** (e.g. a learned Ōi-track embedding, sectional pace dynamics specific to the 1400m/1600m Ōi configurations, or a fresh class of horse-form features) trained via a **multi-hour full retrain or HPO/L4 retrain** — not a same-night blend or feature bolt-on. Until such a signal is sourced and validated under per-year WF + paired bootstrap, the production per-class config remains the best available for the Ōi card.

## Quality Gate Results

- tsc: n/a — no code change this round
- lint: n/a — no code change this round
- format:check: n/a — no code change this round
- test:coverage: n/a — no enforced-package file modified
- python:check: n/a — experiment scripts live under tmp/ (not an enforced package)
