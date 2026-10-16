# PyTorch MPS race-set ranker (odds-free)

Local unique model: `timesfm_finish_position.pytorch_mpx.RaceSetTransformer`
trained on the production JRA feature snapshot with win-odds / popularity /
current-race `finish_norm` / `target_*` columns removed (187 inputs).

Metric: winner contained in predicted Top1..Top5, 2024–2026, existing JRA cells.

## Production (v61)

| venue    | races |   top1 |   top2 |   top3 |   top4 |   top5 |
| -------- | ----: | -----: | -----: | -----: | -----: | -----: |
| Nakayama |  1190 | 40.84% | 59.33% | 69.83% | 77.90% | 84.12% |
| Hanshin  |   878 | 38.72% | 51.71% | 64.81% | 75.51% | 83.03% |

## Best leak-free PyTorch so far

All-JRA 8 epoch, dim 64, 2 layers, 4 heads, lr 5e-4
(`../pytorch-mpx-nakayama-hanshin-jra-lr5e4`). Highest Top1. Top5 still
slightly behind lr 1e-3 (`../pytorch-mpx-nakayama-hanshin-jra-pretrain`).

| venue    | races |   top1 |   top2 |   top3 |   top4 |   top5 |
| -------- | ----: | -----: | -----: | -----: | -----: | -----: |
| Nakayama |  1296 | 30.56% | 47.92% | 61.65% | 71.91% | 78.55% |
| Hanshin  |   948 | 28.59% | 44.83% | 57.59% | 68.35% | 76.69% |

Tried and rejected: 30 epochs (overfit), dim 128, 3 layers, 8 heads,
venue finetune, batch 32, per-horse MLP, dirt-703 cell specialists.
Largest cell specialist (Nakayama dirt 1200 703) reached 40% Top1 vs
production ~45% on the same cell.

Does not beat production trees as a replacement model. Ensemble (tree score +
centered MPS ranker) is the adoption path: existing cell models stay.
Per-cell weight is chosen from a score-blend and a rank-blend crossing sweep.
Among weights that do not reduce any of Top1..Top5, maximize total extra hits,
then Top1..Top5, then smallest weight. 19 JRA cells ON; net +12/+17/+12/+5/+7
winner-in-TopK hits.
Policy: `../pytorch-mpx-cell-policy.json`.
Audit: `../pytorch-mpx-cell-policy-audit.json`.

Odds features stay banned. Production `prophet_cell_policy.json` was not edited.
