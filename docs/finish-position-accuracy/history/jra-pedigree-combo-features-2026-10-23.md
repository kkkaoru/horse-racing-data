# JRA pedigree-combination features (2026-10-23) — not adopted

Layer: `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-jra-pedigree-combo-features.py`
(not in `LAYER_CHAIN`). Campaign scripts: `.cache/jra-ped2/`, `.cache/jra-ped3/` (git-ignored).

## Features

Lift of an ancestor pair over the additive expectation of the two ancestors
alone, under the same surface (and distance band), from strictly-earlier-day
cumulative totals, shrunk with k = 30:

- sire x dam's sire (surface x distance band; win and top-3)
- sire's sire x dam's sire, sire x dam's sire's sire, sire's sire x dam's
  sire's sire, sire's sire's sire x dam's sire, sire x dam's dam's sire,
  sire's dam's sire x dam's sire
- sire line x dam-sire line (surface, surface x band), sire x dam-sire line,
  sire's sire x dam-sire line. Lines come from walking `jvd_hn` sire links up
  to the nearest `jvd_bt` stallion (99.9% coverage; the earlier name lookup
  covered 0.3%).
- dam's other runners (top-3 / win, excluding the runner itself), ancestor
  duplication in the 3-generation pedigree, same broad line.

Serving query: every history table is reduced to the keys the target rows carry
before accumulating. A one-day JRA card (2026-10-04, 362 rows) builds in 3.6 s /
2.2 GB versus 31 s / 13 GB for the whole store, and the values are identical
to the training build (`test_one_day_card_matches_training_values`).

## Evaluation

Top-k unordered hit: the model's k best are exactly the first k finishers.
2025 holdout trained through 2024, 2026 holdout through 2025 (all served
models end 2025-12-28). Seed ensembles, paired race bootstrap.

| comparison (all JRA, 6,065 races)    | top1  | top2  | top3  | top4  | top5  |
| ------------------------------------ | ----- | ----- | ----- | ----- | ----- |
| sim + v2 (sire x bms lines, 15 cols) | -0.15 | -0.33 | +0.03 | -0.07 | -0.13 |
| sim + v3 (deeper ancestors, 20 cols) | -0.28 | -0.25 | -0.05 | -0.02 | -0.13 |

No cell (router variant or class x surface x distance band) improved in both
years with a CI excluding zero. Joken cell models with the v3 columns (3
seeds) did not improve either: 新馬 (joken_701) top3 -1.23, 未勝利
(joken_703) top1 -0.93 [-1.6, -0.3]. The v3 columns took 3% of the sim
model's splits; the model ranks mostly on recent form.

## Routing finding (not applied yet)

On the ungated class-only cells, the sim model beats the served joken model:

| cell               | 2025 sum of top1..5 deltas | 2026 | served artifacts 2026                    |
| ------------------ | -------------------------- | ---- | ---------------------------------------- |
| joken_005 (1勝)    | +5.2                       | +4.6 | +5.1 (top3 +3.6 [+0.5, +7.2])            |
| joken_703 (未勝利) | +0.9                       | +4.8 | +1.6 (top1 +3.2, top4 -2.4 [-4.8, -0.4]) |
| joken_010 (2勝)    | 0.0                        | —    | not adopted                              |
| joken_701 (新馬)   | +3.9                       | +1.8 | 0.0 (157 races; not adopted)             |

Routing the `joken-005` and `joken-703` class-only rules to `sim` needs the
`cell_routing.json` edit plus `focused-full-completion.ts`
`JRA_JOKEN_MODEL_VERSIONS` and the viewer mirror
`finish-position-cell-routing.ts`; the gated variants keep their base.
