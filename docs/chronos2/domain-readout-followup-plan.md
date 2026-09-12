# Prospective development-only readout diagnostic

Declared while the fixed Ban-ei round is still running, before NAR training and before either domain's new later-year readout. This does **not** change the running studies, their budgets or their original acceptance decisions. Rejected JRA results remain rejected.

## Hypothesis

The current blend uses equally spaced market rank percentiles. This discards odds confidence: a narrow price difference and a very strong favourite receive the same adjacent-rank gap. It also assigns different prior scores to equal-price horses using an arbitrary deterministic identity tie-break.

A separate diagnostic may compare an odds-aware prior on 2023 development only. A fixed, non-fitted prior is the Plackett–Luce expected normalized finishing position: for runner i, average `p_i / (p_i + p_j)` over other starters j, where `p_i = 1 / decimal_odds_i`. This remains in [0,1], preserves market ordering and equal-price ties, and uses the same current odds already present in the existing market comparator.

Use only already trained trial artifacts and their saved causal CPU forecasts initially. Label this **readout analysis, not additional training**. Preserve and report the original rank-percentile results alongside it. Any selection under a new readout must be frozen on development before its later-year evaluation; no later-year-driven weight or representation adjustment is allowed. Untrained Chronos controls would be needed before attributing any additional gain specifically to fine-tuning.

This is a hypothesis and plan, not implemented performance evidence. Market odds still lack historical PIT attestation, and no result authorizes deployment.

## Completion addendum

The original declaration above is retained. Development comparisons, matched step-zero controls, and the separately frozen five-case/four-arm later readout are now complete. No new training, weight search, or later-outcome rescue was performed. All five trained primary arms—and all twenty arms including controls—failed the combined aggregate Top1 / annual Top2–5 market guard. All five later matched-target MAEs nevertheless improved. See [the investigation](fine-tuning-effect-investigation.md) and [the fixed later results](later-frozen-readout-results.md). These observed years and retrospective source repairs do not establish untouched holdout performance or PIT approval.

## Storage before the separate NAR round

Retain all model/trial artifacts. Before launching another 96-model round, reserve at least 15 GiB beyond the observed worst-case round footprint. After the active heavy job ends, a byte-verified APFS transparent-compression probe may reduce allocated storage without changing model paths or contents. Adopt it only if SHA-256 is unchanged and measured allocated bytes decrease; otherwise do not assume savings or run the disk to exhaustion.
