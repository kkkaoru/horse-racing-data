# Mandatory cell-local Rustuna tuning before application

The user's application requirement is **per-cell dynamic Rustuna optimization**, not a globally fixed configuration. This applies to future fine-tuned model application. The rejected global head/LoRA studies are not a substitute for cell studies.

## Implemented boundary

`timesfm_finish_position.chronos_cell_tuning.tune_cell` creates a separate serial TPE study from each immutable `CellScope`. Its seed/name derive from the full scope: cell ID, source-data SHA-256, exact development-runner identity SHA-256, training/development/application dates and annual race counts. No shared global winner is reused between cells.

Each trial supplies `CellParameters(training=StudyConfig(...), market_weight=...)`:

| Parameter                      | Current supported search space                               |
| ------------------------------ | ------------------------------------------------------------ |
| Adaptation                     | head-only or LoRA                                            |
| Optimizer steps                | 100 to the explicit `max_steps` budget, in increments of 100 |
| Effective batch size           | 8, 16, 32                                                    |
| Context length                 | 32, 64, 128                                                  |
| Minimum history                | 2–8                                                          |
| AdamW learning rate            | log-uniform 1e-6–5e-5                                        |
| Weight decay                   | 0–0.1                                                        |
| Warmup                         | 0–20% of optimizer steps                                     |
| LoRA rank                      | 4, 8, 16; alpha = 2 × rank                                   |
| Market-percentile blend weight | 0–1                                                          |

The search adapts suggestions through Rustuna TPE within these bounds; it is not unrestricted online learning. QLoRA/full tuning are deliberately not enabled. Deployment latency, memory and final production gates can reject a development-selected configuration.

The MLX study driver now accepts the corresponding optimizer/LoRA/date settings through `StudyConfig` and CLI flags. Original experiment defaults remain the same. Newly explicit configuration fields strengthen checkpoint identity; older historical checkpoints are not silently relabeled to match the new CLI schema. Their original identity/config remain available to the lower-level checkpoint API.

## Evaluator contract

A caller supplies `evaluate(scope, parameters) -> CellEvaluation`. This is the integration boundary for **real** cell data, not a fake scoring function to deploy:

1. Construct target cell cohorts from an authoritative cell mapping and independent starter/scratch roster. Freeze identities and available-before-cutoff odds and incumbent predictions.
2. Keep strictly earlier **all-venue entrant history**. Cell evaluation scope must not truncate source history to same-cell races. Sparse histories preserve incumbent/market fallback, never disappear.
3. Train a fresh model with exactly `parameters.training`; `run_study(..., config=parameters.training)` supports these settings. Restrict target evaluation to the frozen cell, not the study driver's broader development export. Application date and holdout labels must never enter the objective.
4. Export and evaluate standard **portable CPU** weights, rather than relying on native BF16 rankings. Derive the returned configuration from the actual study report, and verify its model hash. Report full annual Top1–Top5 integer deltas against both market and exact incumbent.
5. Return the identical `scope`, actual model digest, annual race counts/deltas and evaluated `parameters`. The tuner rejects cross-cell/cohort/configuration mismatch and missing annual cohort rows.

`CellScope` and parameter equality checks validate the supplied contract; they do **not** independently prove that a callback's data provenance or claimed training execution is truthful. An external artifact/runner/PIT attestation is still required.

Illustrative driver boundary (the cohort-specific evaluator must be supplied):

```python
result = tune_cell(
    scope=frozen_scope,
    evaluate=train_and_evaluate_cell,
    n_trials=32,
    max_steps=1000,
)
# result.trials retains every completed configuration, model hash and metric.
# result.selected is only a development selection, never deployment approval.
```

No real cell cohort or incumbent readout is fabricated by this module. **Current evidence is tests of real Rustuna mechanics with deterministic evaluator doubles, not completed real-data per-cell fine-tuning.** The current rejected global models are not being applied, so no production cell study has been invoked or bypassed.

## Selection and application gates

- Every development year's Top2–Top5 deltas must be nonnegative against **both** baselines.
- Aggregate development Top1 must improve against **both**; maximize the lesser aggregate Top1 gain. Otherwise `selected=None` and preserve incumbent.
- Freeze the selected configuration before independent holdout readout. Never retune on holdout or on today's outcomes; record that previous campaigns already examined some years.
- Verify immutable model/config/tuning-report hashes, exact target cell and runner identities, PIT history/odds, runtime parity and annual nonregression before staging any artifact.
- Every result remains `production_eligible:false`. No callback, study score, or result object grants production permission.
- Missing baselines, insufficient complete cohorts, unsuccessful tuning or unapproved artifacts retain incumbent. Never affect unrelated cells or heatmaps.

No Cloudflare Worker binding, cell artifact publication, routing change or live model replacement has been performed. Application requires this completed per-cell evidence in addition to the existing deployment gates.
