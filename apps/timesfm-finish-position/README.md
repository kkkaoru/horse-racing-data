# TimesFM 3.0 finish-position experiment

Local, research-only evaluation of TimesFM 3.0 as a temporal controller for the
existing NAR finish-position ensemble. Nothing in this package is imported by a
Worker, Container, viewer route, or production prediction path.

## License boundary

The TimesFM source is Apache-2.0, but the `google/timesfm-3.0-pytorch` weights
use `timesfm-non-commercial-license-v1.0`. They must not be deployed or used
commercially. The CLI therefore requires `--accept-non-commercial-license`, and
every report records `research_only=true` and `production_integration=false`.

## Six arms

The experiment crosses three integrations with two temporal origins:

| Integration        | Meaning                                                                                                                                                                                                                       |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `policy-and-data`  | Refit the existing full-information contextual gain policy on prior years, then confidence-blend its predicted arm gains with the temporal forecast. This uses both the policy result structure and its raw race/action data. |
| `router-only`      | Reuse data loading and exact cell routing only. The historical cell action is allowed through only when the temporal forecast says its gain remains positive. No contextual-policy output is consumed.                        |
| `dynamic-ensemble` | Convert forecast action gains to a softmax barycentre and dynamically choose one of the existing 0.00–1.00 ensemble blend weights.                                                                                            |

| Temporal origin       | Meaning                                                                                                                                                                                                                |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `timesfm3-pretrained` | Frozen official TimesFM 3.0 multivariate checkpoint. The 21 variates are realized Top1–3 gains of blend weights 0.00–1.00 versus the deployed 0.50 blend.                                                              |
| `scratch`             | No foundation pretraining. A task-only pooled autoregression is fit strictly on earlier-year data. This is the honest no-pretraining ablation; it does not pretend that an untrained 1.3 GB TimesFM network is useful. |

The outer folds are 2024, 2025, and 2026. A fold sees only races whose year is
strictly earlier. Cell series are aggregated by race date; all forecasts for an
outer year are generated before that year's outcomes are exposed. The available
2026 ledger is partial, and the report preserves its actual race count.

## Mac and other platforms

On Apple Silicon, task fitting and contextual-policy matrix optimization run in
MLX. The official TimesFM 3.0 implementation has no MLX backend, so checkpoint
inference uses its supported PyTorch MPS path. This hybrid avoids claiming an
unverified weight conversion while still keeping all task optimization on MLX.
On Linux/Windows, the same code uses CUDA when available (otherwise CPU) and
NumPy for the small task-only optimizers. Only Mac execution is performed during
this validation phase.

## Run

```bash
cd apps/timesfm-finish-position
uv sync
uv run timesfm-finish-position --accept-non-commercial-license
uv run pytest
```

Default input is the existing immutable local ledger:

`apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/nar_current_exact_race_rows.parquet`

Default output is ignored local state under
`apps/timesfm-finish-position/tmp/timesfm-3-evaluation-2024-2026.json`.

## Result

The reproducible report and interpretation are checked in under
[`results/`](results/README.md). Across 32,512 evaluated races, no arm improves
all three years. The strongest aggregate arm (`policy-and-data` with the frozen
checkpoint) is +0.0646 percentage points overall but -0.2958 points in partial
2026, with a fully negative date-cluster 95% interval. It is therefore rejected
for production on both accuracy/stability and license grounds.

## Horse-level model lab

A separate PIT walk-forward lab evaluates LightGBM/XGBoost ranking, CatBoost,
TabM, an MLX Horse History Transformer, horse-level TimesFM-3/Chronos-2,
Prophet entity trends, and regularized stacking. Its contract, ablations, and
partial-2026 decision are documented in
[`results/model-lab-README.md`](results/model-lab-README.md), with machine-readable
results in
[`results/model-lab-evaluation-2024-2026.json`](results/model-lab-evaluation-2024-2026.json).
This remains local research and has no production integration.
