# JRA nick / higher-grade course features (2026-10-23) — not adopted

Layer: `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-jra-nick-grade-course-features.py`
(not in `pipeline_args.LAYER_CHAIN`; no served model uses it).

Features (strictly earlier days, ASOF on per-key cumulative totals):

- nick: sire x broodmare-sire (registration ids) win/top-3 shrunk to the sire,
  lift over the sire, per surface and per distance band; sire x broodmare-sire lineage.
- higher-grade course: races of a strictly higher class at the same venue, exact
  distance, surface — sire top-3 there, runner's best time vs the winning par,
  4th-corner position vs higher-class placers.
- higher-grade par: each past run vs the open-class-and-above winning time for that
  run's course and going (per 1000m), career mean / best.

A/B on `jra-cb-v9-sim-2013-clean-noexo1` recipe, same store, 5 seeds per arm,
top-1 of the seed-ensemble score, paired race bootstrap:

| set                | 2026 holdout (2610 races)                                    | 2025 holdout, trained to 2024 (3455) |
| ------------------ | ------------------------------------------------------------ | ------------------------------------ |
| all                | -0.19pp [-0.84, +0.38] (before par) / +0.31pp [-0.38, +1.03] | -0.52pp [-1.19, +0.12]               |
| nick only          | +0.11pp [-0.38, +0.61]                                       | -                                    |
| higher-grade only  | +0.23pp [-0.38, +0.84]                                       | -                                    |
| higher-grade + par | +0.38pp [-0.34, +1.11]                                       | -0.09pp [-0.72, +0.52]               |

No set improves both years, so nothing was released. The model does split on
`hg_time_gap(_rank_in_race)` (top-20 by split count) but the information
overlaps existing course-time / sire-venue features. Campaign files:
`.cache/jra-nick-hg/` (train_eval.py, ab-\*.log).
