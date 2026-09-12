# Chronos-2 MLX measurements

## Host and environment

Apple M5 Pro, 48 GiB; MLX 0.32.2, PyTorch 2.13.0, chronos-forecasting 2.3.1. Upstream MLX revision `5c29d275fa3c93d6dea5d975e5eb68da71bd6f3b`. Standard model cached snapshot `29ec3766d36d6f73f0696f85560a422f50e8498c`.

All campaign data is under repository `.cache/chronos2/`; reusable sources/tests are in `apps/timesfm-finish-position`. Launch subsequent processes through `bash scripts/run_chronos_mlx.sh <command>` from that package, or its repository-relative path. It fixes TF32 off before import and sets repository-local Hugging Face, uv and temporary paths.

## Initial exploratory benchmark — NOT the final optimized baseline

Synthetic data, seed 42, output-head-only, 12 optimizer steps, batch 8, context 128, horizon 1. AdamW LR 1e-5 without warmup for this controlled comparison. These runs predate discovery of default GPU TF32 behavior, so the label FP32 means tensor dtype, **not strict FP32 arithmetic**. Do not use these results as the final strict-FP32 speedup claim. First-step latency includes compilation and execution, not isolated compiler time. Warm rate uses median steps 3–12, single run. No racing accuracy conclusion is possible.

| Variant | Dtype | Compile   | Fused SDPA | Warm samples/s | Peak bytes | Final validation loss |
| ------- | ----- | --------- | ---------- | -------------: | ---------: | --------------------: |
| A       | fp32  | off       | no         |            921 |  680072084 |              10.71065 |
| B       | bf16  | off       | no         |            958 |  442031820 |              10.70974 |
| C       | bf16  | gradient  | no         |           1124 |  444488502 |              10.72501 |
| D       | bf16  | gradient  | yes        |           1149 |  465248898 |              10.71815 |
| E       | bf16  | full step | yes        |           1430 |  409811780 |              10.71815 |

D improves C by only ~2.3%, below the default 5% adoption threshold for added complexity. E combines two changes and cannot isolate the SDPA contribution. Repeat with a non-fused full-step control, longer runs and strict FP32 arithmetic before choosing defaults. Trainable-weight and optimizer dtype policy also require explicit auditing rather than inferring precision from initial model dtype.

## Portable export diagnostic

One head-update synthetic FP32 model exported to standard safetensors, hash `4bb867e72c6d53e73a9f9b509b7e62b566bdeb87e8dfc0bf545c028b06675faa`. Standard PyTorch Chronos2Pipeline successfully loads all weights.

- Exported/reloaded weight difference: **exactly zero**.
- Initial GPU MLX vs PyTorch forecast difference: max 0.00350308, failed declared atol=rtol=1e-4.
- Against a NumPy FP64 linear oracle, GPU MLX error was 0.000398934, while MLX CPU and PyTorch each had 1.19209e-7 error despite all tensor dtypes being float32.
- A fresh process with **`MLX_ENABLE_TF32=0`**, identical exported weights and input, reduced maximum forecast error to **1.90735e-6**, mean 3.55543e-7: passes original tolerances without relaxing them.

This establishes one FP32 export parity case, not BF16, LoRA or QLoRA parity, nor production/racing eligibility. Preserve the failed first readout separately. Metadata remains fail-closed until a reusable attestation flow is complete. The model was trained with the earlier default arithmetic; the successful diagnostic verifies its identical weights using strict FP32 inference.

## Final strict benchmark and real studies

The completed strict benchmark uses six configurations × three fresh processes × 100 steps with TF32 disabled. Median samples/s A–F: **729.9 / 980.9 / 1144.5 / 1154.3 / 1452.8 / 1434.7**. F (BF16 frozen base, FP32 trainables/state, full compile, no fused SDPA) is the provisional simpler default: ~1.97× A and ~41% lower peak memory. Full configurations and per-run hashes are in [results/20260912-summary.json](results/20260912-summary.json).

Head-only and LoRA each completed 1,000 real updates. Portable CPU development MAE is 0.242931 / 0.243047; all 38,116 eligible development windows were rescored. Native-to-portable point maxima over that cohort are 0.02448 / 0.03097, distinct from strict FP32 export parity maxima of 1.43e-5 / 7.87e-6 on the first 128 windows. The fixed head market blend failed holdout: aggregate Top1–Top5 deltas [-2,-2,0,-2,+1], including 2025 annual regression. No promotion.

An isolated, non-root Linux/amd64 CPU image was built and ran actual inference with networking disabled and a read-only filesystem/model mount. One input matched Mac CPU exactly; cold request 6.72 s under Mac emulation. This is not whole-cohort Linux parity or Cloudflare latency. No live Cloudflare deployment or model publication occurred. See [README.md](README.md) for commands and precise validation boundaries.

## Data residency probe

Three repetitions of 1,000 synchronized acquisitions after 20 warmups, 114,967 synthetic windows, batch 8/context 128: host NumPy selection/conversion median **3.25 µs**, resident MLX gather **145.6 µs**, resident arrays **59,322,972 bytes**. Host acquisition is ~0.058% of the earlier F-step duration; this is an isolated acquisition comparison, not a measured end-to-end speedup. Do not adopt resident gather from these results. Full report: `.cache/chronos2/data-residency-probe.json`.

## Current verification

**262 tests passed; package branch-inclusive coverage 96.64%.** Every new Chronos source file meets the existing per-file 90% rule. Tests include fresh-process CLI checkpoint/resume equality, local-only checkpoint loading, sparse identity retention, HTTP authentication and image input contracts. Full-package Ruff format/check and basedpyright pass; Chronos-scoped `ty` passes.

The full configured `ty check` still reports **42 diagnostics outside Chronos**, in existing other-experiment code/import paths. They are preserved in `.cache/chronos2/logs/full-ty-postfix.log`; no exclusions, suppressions or gate changes were used. Therefore the package's aggregate `check` command is **not claimed green**, and no hook bypass or commit was attempted. Final successful Chronos/full-test evidence after the mandatory cell-local Rustuna extension: `.cache/chronos2/logs/cell-rustuna-full-gates.log`. Cell tuning has 96.69% coverage; its tests use real Rustuna with deterministic evaluator doubles, not actual per-cell model accuracy trials. See [cell-rustuna.md](cell-rustuna.md).
