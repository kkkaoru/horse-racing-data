# MPS PyTorch neural cell blend

## Contract

- Trainer: PyTorch on the Apple GPU (`mps`) with a race-listwise graded
  relevance loss (`1/log2(1+finish_position)`), trained only on feature rows
  whose race date is strictly earlier than the evaluation year minus one.
- Evaluation ledger: the same persisted production prediction rows, the same
  production cell router, and the same analytic per-cell weight search as
  `prophet-cell-policy-README.md`. The metric is winner-contained-in-predicted
  Top1 through Top5 per production cell.
- Comparison authority: the effective production policy. A cell is enabled only
  when the neural signal's Top1..Top5 hit vector is lexicographically greater
  than what production currently applies (the Prophet policy where it is ON,
  no adjustment where it is OFF).
- Feature surface: a model is only considered servable when its inputs are the
  feature rows the Container already builds. The JRA ranker is trained on the
  offline copy of that snapshot (`features-2000-2026`, 123 of the 241 metadata
  features present). The NAR/ban-ei rankers reuse the portable categorical
  embedding MLP artifact, whose 379 point-in-time rolling features are not part
  of the Container feature chain.

## Machine-readable results

- [`neural-cell-policy-evaluation-2024-2026.json`](./neural-cell-policy-evaluation-2024-2026.json)

## Result

Only NAR cells improve on effective production. JRA and Ban-ei do not.

| 区分 | cell                 | races | production (Top1..Top5 delta) | neural (Top1..Top5 delta) |
| ---- | -------------------- | ----: | ----------------------------: | ------------------------: |
| nar  | `c43_tc1_rolling`    |   109 |        +4 / -1 / -1 / -2 / +1 |     +4 / 0 / -1 / -1 / +3 |
| nar  | `c50_tc1_rolling`    |    23 |            +2 / 0 / 0 / 0 / 0 |     +3 / 0 / -1 / -1 / -1 |
| nar  | `c50_tc2_consensus`  |    16 |             0 / 0 / 0 / 0 / 0 |        +1 / 0 / 0 / 0 / 0 |
| nar  | `mukatsu30_tc2_top2` |    47 |             0 / 0 / 0 / 0 / 0 |      +3 / +1 / -1 / 0 / 0 |

The JRA ranker reaches 39.3% / 36.5% / 37.0% standalone winner-in-Top1 on 2024,
2025, and 2026, but no JRA cell beats the Prophet-adjusted production score.

## Production wiring

- `predict_lib/neural_blend.py` loads a portable JSON artifact (feature order,
  train-only mean/scale, MLP weights) and applies the shared centered,
  score-scaled adjustment from `predict_lib/prophet_adjustment.py`.
- `predict_lib/neural_cell_policy.json` enables only cells whose measured
  Top1..Top5 vector beats effective production; every other cell keeps the
  existing Prophet behaviour and the artifact is never loaded.
- `predict_upcoming.py` tries the neural blend first and falls back to the
  Prophet adjustment for every cell the neural policy does not enable.

## Serving blocker for NAR

The four improving cells come from a ranker whose point-in-time rolling
features are not part of the Container feature chain, so enabling them would
require the Container to build that surface. `scripts/build_production_feature_snapshot.py`
materializes the Container's own feature chain offline so the same ranker can be
retrained and re-evaluated on a servable feature surface before any NAR cell is
enabled.
