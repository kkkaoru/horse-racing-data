# Prophet maximum-branch weight optimization (2024–partial 2026)

## Contract

- Evaluation unit: resolved cell + routing mode + Stage-2 outcome/model + Stage-1 gate outcome + final served model/composite version.
- Search: analytic winner-score crossing event sweep; no fixed grid, linear/binary/ternary search, or cross-cell Cartesian search.
- Objective: aggregate winner-in-Top1 hits, with Top2 through Top5 and then lower weight as lexicographic tie-breakers.
- Replay: current production graph over 2024, 2025, and partial 2026. Feature-guard failures are skipped rather than replaced with another model.
- Runtime precedence: exact maximum-branch signature → final served branch → resolved cell → default ON 0.05.

## Coverage

| Category  | Maximum branches |     ON |    OFF |      Races |
| --------- | ---------------: | -----: | -----: | ---------: |
| JRA       |               96 |     27 |     69 |      7,890 |
| NAR       |               23 |      9 |     14 |     27,084 |
| Ban-ei    |                2 |      2 |      0 |      4,551 |
| **Total** |          **121** | **38** | **83** | **39,525** |

Support distribution: 9 branches have 1 race, 26 have 2–9 races, 67 have 10–99 races, and 19 have at least 100 races. Small-support weights remain explicit experimental evidence and must be revalidated as results accumulate.

## Aggregate winner-in-TopK results

| Metric | Baseline hits | Optimized hits | Hit delta | Baseline | Optimized |  Delta (pp) |
| ------ | ------------: | -------------: | --------: | -------: | --------: | ----------: |
| Top1   |        17,066 |         17,142 |   **+76** | 43.1777% |  43.3700% | **+0.1923** |
| Top2   |        24,737 |         24,740 |        +3 | 62.5857% |  62.5933% |     +0.0076 |
| Top3   |        29,711 |         29,675 |       -36 | 75.1701% |  75.0791% |     -0.0911 |
| Top4   |        32,917 |         32,880 |       -37 | 83.2815% |  83.1879% |     -0.0936 |
| Top5   |        35,200 |         35,170 |       -30 | 89.0576% |  88.9817% |     -0.0759 |

The Top3–Top5 decreases are expected under the declared lexicographic objective: Top1 is optimized first and cannot be traded away for lower-priority TopK metrics.

## Category results

| Category |          Top1 Δ |         Top2 Δ |          Top3 Δ |          Top4 Δ |          Top5 Δ |
| -------- | --------------: | -------------: | --------------: | --------------: | --------------: |
| JRA      | +58 (+0.7351pp) | +3 (+0.0380pp) | -43 (-0.5450pp) | -40 (-0.5070pp) | -28 (-0.3549pp) |
| NAR      | +13 (+0.0480pp) | +3 (+0.0111pp) |  +1 (+0.0037pp) |  +2 (+0.0074pp) |  +1 (+0.0037pp) |
| Ban-ei   |  +5 (+0.1099pp) | -3 (-0.0659pp) |  +6 (+0.1318pp) |  +1 (+0.0220pp) |  -3 (-0.0659pp) |

Machine-readable per-signature weights, support, yearly counts, component inventory, and Top1–Top5 metrics are in `prophet-maximum-branch-weight-optimization-2024-2026.json`.
