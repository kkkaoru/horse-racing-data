# Prophet production cell policy

## Contract

- Evaluation years: 2024, 2025, and the available partial 2026 persisted-prediction ledger.
- Included races: JRA 8,250 (2024: 3,454 / 2025: 3,455 / 2026: 1,341), NAR 31,963 (13,677 / 13,400 / 4,886), Ban-ei 3,682 (1,788 / 1,656 / 238).
- Prophet inputs: venue, jockey, and trainer trends fit only on rows before each target year. The 2026 serving lookup uses only pre-2026 history.
- Adjustment weight: optimized first for all 54 configured variants, then for final served models, and finally for every observed maximum-granularity served signature over stable score-crossing intervals in `[0, 1]`.
- Maximum branch identity: `resolved cell + routing mode + Stage-2 swap/keep and Transformer outcome + Stage-2 model + Stage-1 gate outcome + final model/composite version`. Historical support applies current cell conditions while ignoring only `effective_after`; whole-card final-race routing uses the registered card maximum.
- Runtime precedence: exact maximum signature → final served branch → 54-cell fallback → unknown default ON `0.05`.
- Ensemble contract: internal members are reported, but inherit the final composite weight because independent member coefficients are not identifiable after the production graph has combined their scores.
- Metric: whether the actual winner is contained in predicted Top1, Top2, Top3, Top4, or Top5.
- Search: analytic winner-score crossing event sweep, `O(E log E)` per branch. Fixed grids, linear/binary/ternary search, and Cartesian combinations are not used because TopK is non-monotonic in weight.
- Objective: lexicographically maximize aggregate winner-in-Top1, then Top2, Top3, Top4, and Top5 hits. Observed cells are ON only when a positive stable interval improves over weight zero; no-support and unknown cells default ON at `0.05`.

The 2026 ledger is partial, not a completed calendar year. Persisted current-model coverage is 1,341 JRA, 4,886 NAR, and 238 Ban-ei races; 897 additional 2026 Ban-ei races without a complete persisted current-cell prediction set are not guessed or backfilled with another model.

## Results

### Maximum branches (v5 production authority)

| Category  | Maximum branches |     ON |    OFF | Replay races | Top1 delta |
| --------- | ---------------: | -----: | -----: | -----------: | ---------: |
| JRA       |               96 |     27 |     69 |        7,890 |        +58 |
| NAR       |               23 |      9 |     14 |       27,084 |        +13 |
| Ban-ei    |                2 |      2 |      0 |        4,551 |         +5 |
| **Total** |          **121** | **38** | **83** |   **39,525** |    **+76** |

Top1 improves `17,066→17,142` (+0.1923pp). Top2 is +3 hits, Top3 -36, Top4 -37, and Top5 -30 under the declared Top1-first lexicographic objective. Exact weights, support, and per-branch Top1–Top5 metrics are in [`prophet-maximum-branch-weight-optimization-2024-2026.json`](./prophet-maximum-branch-weight-optimization-2024-2026.json) and its [summary](./prophet-maximum-branch-weight-optimization-2024-2026.md). The evaluator is `scripts/optimize_prophet_maximum_branch_policy.py` (`bun run --filter timesfm-finish-position evaluate:prophet-maximum-branches`).

### Served-model fallback (v4)

| Category | Configured routes | Observed top-level routes | Served branches |  ON | OFF | Replay races | Top1 delta |
| -------- | ----------------: | ------------------------: | --------------: | --: | --: | -----------: | ---------: |
| JRA      |                39 |                        26 |              61 |  17 |  44 |        7,890 |         +9 |
| NAR      |                13 |                        11 |              12 |   7 |   5 |       27,084 |        +11 |
| Ban-ei   |                 2 |                         2 |               2 |   2 |   0 |        4,551 |         +5 |

The branch replay omits races that the current production feature guard rejects rather than substituting a different model. JRA routes without replay support retain their independently evaluated cell-level v3 decision. NAR's two consensus members and JRA's two rerank variants are internal-only routes and inherit their parent composite decision.

Exact branch ON/OFF, weight, component inventory, yearly support, and Top1–Top5 metrics are in [`prophet-served-branch-weight-optimization-2024-2026.json`](./prophet-served-branch-weight-optimization-2024-2026.json) and its [human-readable table](./prophet-served-branch-weight-optimization-2024-2026.md). The reproducible evaluator is `scripts/optimize_prophet_served_branch_policy.py` (`bun run --filter timesfm-finish-position evaluate:prophet-served-branches`).

### Cell fallback (v3)

The complete 54-route fallback policy remains documented in [`prophet-cell-weight-optimization-2024-2026.json`](./prophet-cell-weight-optimization-2024-2026.json) and its [table](./prophet-cell-weight-optimization-2024-2026.md). It contains JRA 32 ON / 7 OFF, NAR 10 ON / 3 OFF, and Ban-ei 2 ON / 0 OFF across 43,895 races.

## Real-race smoke

- OFF exact branch: JRA `2026-08-16 01-03`, cell `joken_005`, served Stage-1 branch `jra-cb-stage1-marketfree235-2013`, 14 runners, zero scores changed.
- ON exact branch: NAR `2026-09-02 30-02`, cell `sim`, Transformer composite `iter40-nar-settransformer-blend-v1`, weight `0.001778529223552035`, 11 scores changed.
- ON unseen-branch fallback: Ban-ei `2026-08-16 83-09`, cell `base`, persisted branch `banei-cb-v8-window2011-wf-15y`, cell fallback weight `0.08656703186270287`, 10 scores changed.

The smoke uses the annual baked lookup, production day/category join, production router/policy, and persisted production prediction rows. Machine-readable deployment evidence is in [`prophet-cell-policy-smoke-2026-09-02.json`](./prophet-cell-policy-smoke-2026-09-02.json).
