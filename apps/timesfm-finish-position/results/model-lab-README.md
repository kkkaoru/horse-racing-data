# Finish-position PIT model lab (2024–partial 2026)

Machine-readable authority: [`model-lab-evaluation-2024-2026.json`](./model-lab-evaluation-2024-2026.json).

## Contract

- Scope: NAR, 1,005,267 runner histories from 2020 through partial 2026.
- Supervised models reserve `Y-1` for calibration: 2024 fits through 2022, 2025 through 2023, and 2026 through 2024.
- Frozen temporal and Prophet trend features may refresh their context through `Y-1`; they never consume target-year outcomes.
- Tests are 2024, 2025, and partial 2026 respectively. Probability calibration always uses the immediately prior year.
- Horse histories use only strictly earlier dates. Same-day rows never enter one another's history.
- Imputation, calibration, and stack standardization are fit on the training side only.
- The final stack is fit on aligned 2024–2025 OOF predictions and tested once on partial 2026.
- Random K-Fold was not used.
- This is local research. No Worker, Container, Viewer, cloud database, or production model path imports it.

Input authorities:

- Horse history SHA-256: `0c93c14ea201fbb324059a8d41ba0804f538e66d25cb1913ae22e394ada719db`
- PIT tabular features SHA-256: `0b4fd2b196e1420718a6636f014fc236f168ed246bb4a3e7316c29a968c2df90`

## Models evaluated

- LightGBM LambdaRank and rank-XENDCG
- XGBoost pairwise ranking
- CatBoost YetiRank
- official TabM
- MLX Horse History Transformer over the latest 10 strictly prior starts
- frozen TimesFM 3.0 and Chronos-2 horse-series forecasts
- Prophet monthly venue, jockey, and trainer trends
- L2-regularized Logistic Regression stacking with race-local simplex normalization

The TimesFM/Chronos horse-series contract takes one pre-year context per horse and forecasts all target-year starts without consuming target-year outcomes. It is deliberately conservative and leakage-free. TimesFM remains subject to its non-commercial license.

## Partial-2026 results

| Model                                             |   Log Loss ↓ |      Brier ↓ |     NDCG@3 ↑ |      Top1 ↑ | Winner in Top3 ↑ |        ROI |       Yield |
| ------------------------------------------------- | -----------: | -----------: | -----------: | ----------: | ---------------: | ---------: | ----------: |
| LightGBM LambdaRank                               |     0.266412 |     0.076769 |     0.553358 |     35.193% |          67.319% |     0.7700 |     -23.00% |
| Four-tree stack                                   |     0.265712 |     0.076609 |     0.553991 |     35.294% |          67.380% |     0.7769 |     -22.31% |
| + TabM                                            |     0.265586 |     0.076571 |     0.554265 |     35.375% |          67.470% |     0.7763 |     -22.37% |
| + Horse History Transformer                       |     0.265560 |     0.076564 |     0.554379 |     35.395% |          67.430% |     0.7733 |     -22.67% |
| + Prophet entity trends (accepted research stack) | **0.265305** | **0.076510** | **0.556533** | **35.778%** |      **67.713%** | **0.7851** | **-21.49%** |

The accepted seven-model research stack also reduced maximum drawdown from 2,287.0 for standalone LightGBM to 2,137.2. Its Top3 exact-set accuracy was 9.989%, slightly below LightGBM's 10.029%; it did not dominate every metric.

Against the otherwise identical six-model stack, a 2,000-replicate paired date-cluster bootstrap found the Prophet addition improved:

- Log Loss: `-0.000255`, 95% CI `[-0.000364, -0.000149]`
- Brier: `-0.0000537`, 95% CI `[-0.0000848, -0.0000207]`
- NDCG@3: `+0.002933`, 95% CI `[+0.001319, +0.004528]`
- Top1: `+0.3834pp`, 95% CI `[+0.1211pp, +0.6395pp]`
- Winner in Top3: `+0.2825pp`, 95% CI `[+0.0703pp, +0.5181pp]`
- Yield: `+1.1876pp`, 95% CI `[+0.2894pp, +2.1391pp]`

A nested 2025 check, with the stack fit only on 2024 OOF, also improved Log Loss, Brier, NDCG@3, Top1, Top3 exact-set, and winner-in-Top3. Its ROI and maximum drawdown worsened, so betting stability is not established.

## Ablation decision

CatBoost, TabM, the History Transformer, and Prophet each improved partial-2026 Log Loss and Brier when added to the accepted stack. Prophet was the largest incremental contributor. The History Transformer contribution was positive but small.

TimesFM-3 and Chronos-2 were rejected from this horse-level stack:

- standalone partial-2026 Log Loss was 0.320473 for TimesFM and 0.311259 for Chronos;
- adding both produced Log Loss 0.265661, worse than the accepted stack's 0.265305;
- removing either from the joint temporal stack improved probability metrics;
- TimesFM is additionally blocked from commercial production by license.

This does not change the separate cell/action TimesFM-controller rejection documented in [`README.md`](./README.md).

## Decision

The seven-model result is a **research candidate for additional shadow validation, not a production adoption**. Probability and ranking metrics improved, but all evaluated betting strategies remained negative-yield, partial 2026 is not a full year, and one Top3 metric regressed slightly.

A production-safe serving boundary now exists for Prophet entity trends: the annual pre-year-only forecasts are baked into a compact lookup, joined once in every category's `DAY_CHAIN`, and consumed only by a bounded post-model score adjustment. Prophet itself is absent from the Container. This adjustment is not mathematically equivalent to the evaluated seven-model logistic stack, so it was evaluated separately by current production cell on 2024, 2025, and the available partial 2026 persisted-prediction ledger. Cells with negative aggregate Top1 deltas are disabled; improved, equal, unsupported, and newly introduced cells default ON. The Top1–Top5 policy and real-race smoke authority are `prophet-cell-policy-top1-5-evaluation-2024-2026.json` and `prophet-cell-policy-smoke-2026-09-02.json`.
