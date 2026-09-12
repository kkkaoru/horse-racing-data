# Fine-tuning effect and failure investigation

## Question and evidence boundary

Investigate when actual Chronos-2 fine-tuning helps, when it fails, and why. There are 288 completed cell-training trials: 96 each in JRA, NAR flat and Ban-ei. Original development selections remain immutable: three JRA cells (all subsequently rejected), zero NAR cells, one Ban-ei cell. A development selection or comparison against market alone does not establish a fine-tuning benefit.

The original NAR freeze is `3b2aa1e9793b002aabac8985d01d12d9d71bb51ee775ad34892884b57f03f8f2`; Ban-ei is `a6019949ebe0340d329c3f74392bbe45dfaa0621b235d4731486991d927df53a`. Later years were previously observed in other work. No new later-year results may determine parameters or diagnostic selections.

## Required comparisons

1. **Within-trial learning:** native baseline/final quantiles, MAE and loss trajectory at identical precision, context, history eligibility and rows. Report distribution changes, not just mean losses. These native development windows require finite current labels; they are a conditional diagnostic, not a full-starter ranking evaluation.
2. **Causal model ablation:** step-zero versus trained portable CPU forecasts on identical complete starter cohorts, using identical context/minimum history, representation, blend and tie-breaking. Reconstruct step-zero through the same BF16 loading, adaptation and master-weight initialization as the original training. Do not compare an unrelated FP32 parent and attribute quantization differences to gradients. Head and LoRA controls may differ in initialization precision; verify this rather than assume interchangeability.
3. **Readout ablation:** rank-percentile versus prospectively declared odds-aware PL prior, with all existing trial weights/configurations held fixed initially. Reproduce original cached metrics before calculating alternatives. Label this as cached readout, with zero additional training steps.
4. **Forecast information controls:** market only, untrained model, and simple history/constant controls where needed. Establish whether any improvement comes from learning, pretrained information, coverage/ties, or the blend itself.
5. **Generalization:** freeze selections before one coordinated later-year readout. Keep original selections and any new development-only alternatives separately identifiable. Never rescue a failed selection by changing settings after readout.

## Failure mechanisms to test

- MAE/pinball improvements versus exact-rank objective mismatch.
- Too-small forecast changes to alter market-blended ordering; adjacent score margins and price ties.
- Forecast-scale compression or displacement, raw versus relative representation, and differences between strong favourites and close-price pairs.
- Successful and harmful swaps: exact ranks 1–5, number of changed races, dead heats and supports.
- Learning rate, steps, head/LoRA and history effects. Adaptive TPE trial correlations are descriptive, not randomized causal comparisons.
- Sparse histories and fallback usage without dropping DNF/DQ starters or conditioning inference eligibility on current outcomes.
- Domain, season and price-distribution changes. Proxy grade/condition pools must not be described as precise B4/C3 classes.
- Training-scope dilution: `run_study` filters label dates and domain/prefix, but does not receive a cell-specific label-race whitelist. Quantify how much of each cohort's training and native development data belongs to the target cell. Independent horse-cohort studies are not necessarily strict cell-label fine-tuning. Also measure cross-cell horse/label overlap and distinguish the broad native MAE cohort from the exact target-race scoring cohort.
- Missing conditioning information: verify whether calendar gaps, current race conditions, field strength and class changes reach the scalar forecasting input. A plausible omission is not proof of causal failure; establish effects with matched ablations before claiming a cause.
- Selection noise: per-race paired uncertainty, multiplicity across searches, and evidence concentrated in a few races. Do not claim statistical confirmation from a small positive count alone.

## Reporting

Separate **observed facts**, **mechanisms demonstrated by matched ablation**, **supported hypotheses**, and **unresolved questions**. Give per-domain and per-cell supports and raw hit deltas, plus losses, forecast drift, rank changes and harmful/beneficial swaps. Quantify uncertainty without treating the 16 adaptive trials as independent samples.

No production eligibility follows from this investigation: exact incumbent comparisons, PIT/roster attestation and runtime approval remain outstanding.
