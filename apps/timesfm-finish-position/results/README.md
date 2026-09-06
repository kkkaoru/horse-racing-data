# TimesFM 3.0 finish-position experiment results

## Decision

**Do not promote this experiment into production.** None of the six arms improves the
current 0.50-weight baseline consistently in 2024, 2025, and the available part of 2026. The strongest point estimate (`policy-and-data / timesfm3-pretrained`) regresses
by **-0.2958 percentage points** in 2026; its date-cluster 95% interval is entirely
negative (`[-0.5811, -0.0124]`). In addition, the public TimesFM 3.0 weights use the
`timesfm-non-commercial-license-v1.0` license, so they are not eligible for this
project's commercial production path.

The versioned machine-readable authority is
[`timesfm-3-evaluation-2024-2026.json`](timesfm-3-evaluation-2024-2026.json).

## Experiment contract

- Evaluation ledger: 46,119 NAR races; SHA-256
  `9eac6154e75fb59c270fe0753b02898a0e52b4d35415ccaff7950af59aff0039`.
- Outer evaluation years: 2024 (13,677 races), 2025 (13,426), and partial 2026
  (5,409), for 32,512 evaluated races.
- Each fold can use only races strictly before its target year. No future target-year
  outcome enters the policy fit, scratch forecaster, or TimesFM context.
- Forecast target: the 21 realized Top1-Top3 utility gains of existing ensemble blend
  actions (weights 0.00 through 1.00 in 0.05 steps), aggregated by temporal router
  cell. This evaluates a temporal controller over existing predictions; it is not a
  new horse-level foundation model.
- Baseline: existing fixed blend action 0.50. Aggregate baseline Top1-Top3 mean is
  63.4340%.
- Confidence intervals: 2,000 race-count-weighted date-cluster bootstrap samples.
- Checkpoint: `google/timesfm-3.0-pytorch` revision
  `c71907076f28b1241d1fccc37efd183d0912cd13`.
- Mac runtime: official frozen checkpoint inference on PyTorch MPS; scratch
  autoregression and contextual ridge-policy optimization on MLX. The portable path
  routes checkpoint inference to CUDA/CPU and small optimizers to NumPy.

“Existing reinforcement learning” is described narrowly here: the repository artifact
is a full-information contextual multi-rank ridge policy with
`no_confirmed_candidate`, not a deployed online-RL policy.

## 32,512-race weighted point estimates

| Integration                           |     Pretraining |     Top1 |     Top3 | Top1-3 mean | vs baseline | Activation |
| ------------------------------------- | --------------: | -------: | -------: | ----------: | ----------: | ---------: |
| Policy result structure + action data |     TimesFM 3.0 | 45.9615% | 78.2419% |    63.4986% |  +0.0646 pp |    98.751% |
| Router/data only                      |     TimesFM 3.0 | 45.8815% | 78.2757% |    63.4402% |  +0.0062 pp |    29.949% |
| Dynamic ensemble weight               |     TimesFM 3.0 | 45.9031% | 78.2573% |    63.4330% |  -0.0010 pp |     0.089% |
| Policy result structure + action data | Scratch task AR | 45.9646% | 78.1896% |    63.4853% |  +0.0513 pp |    96.527% |
| Router/data only                      | Scratch task AR | 45.9215% | 78.2511% |    63.4330% |  -0.0010 pp |     1.590% |
| Dynamic ensemble weight               | Scratch task AR | 45.9061% | 78.2573% |    63.4340% |  +0.0000 pp |     0.000% |

## Annual Top1-Top3 delta and date-cluster 95% interval

| Integration    | Pretraining     |                        2024 |                        2025 |                    partial 2026 |
| -------------- | --------------- | --------------------------: | --------------------------: | ------------------------------: |
| Policy + data  | TimesFM 3.0     | +0.0999 `[-0.0879,+0.2937]` | +0.1738 `[-0.0101,+0.3677]` | **-0.2958 `[-0.5811,-0.0124]`** |
| Router only    | TimesFM 3.0     | +0.0219 `[-0.0593,+0.1041]` | +0.0497 `[-0.0371,+0.1418]` |     -0.1417 `[-0.3223,+0.0491]` |
| Dynamic weight | TimesFM 3.0     | -0.0024 `[-0.0075,+0.0000]` | +0.0000 `[+0.0000,+0.0000]` |     +0.0000 `[+0.0000,+0.0000]` |
| Policy + data  | Scratch task AR | +0.0658 `[-0.1099,+0.2497]` | +0.1366 `[-0.0450,+0.3260]` |     -0.1972 `[-0.4749,+0.0750]` |
| Router only    | Scratch task AR | -0.0073 `[-0.0305,+0.0147]` | +0.0124 `[-0.0126,+0.0417]` |     -0.0185 `[-0.0765,+0.0431]` |
| Dynamic weight | Scratch task AR | +0.0000 `[+0.0000,+0.0000]` | +0.0000 `[+0.0000,+0.0000]` |     +0.0000 `[+0.0000,+0.0000]` |

All values in the last table are percentage points. The pretrained checkpoint has a
small aggregate advantage over scratch for policy-and-data (+0.0133 pp), but that
advantage reverses in 2026 and is not an adoption signal. The dynamic controller
almost always rounds back to the baseline weight, so it adds complexity without
measurable benefit.
