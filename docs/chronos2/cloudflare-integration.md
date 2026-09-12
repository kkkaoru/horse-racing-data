# Cloudflare integration contract (not activated)

## Runtime boundary

Use a Linux CPU Cloudflare Container, not Worker JavaScript or MLX. A Worker orchestrates an immutable artifact and prediction request; the container executes standard Chronos-2 with PyTorch CPU. The current prediction container does not install PyTorch/Chronos; modifying its already-dirty Dockerfile or silently enlarging the active image is not acceptable. The separate `apps/timesfm-finish-position/Dockerfile.chronos` now builds a hash-locked Linux/amd64 CPU image. Routing remains off; no Worker binding or Cloudflare deployment has been added.

A portable model directory contains standard `config.json`, `model.safetensors`, and research/export metadata. Production additionally requires an independently checked manifest with model/config hashes, base revision, source data identity, training cutoff, model context contract, routing scope, accuracy comparison and runtime parity evidence. Research metadata's `production_eligible: false` must never be interpreted as approval.

## Inference request

- Exact race and horse identifiers, evaluation date, strictly prior history dates and scalar feature values.
- All-venue entrant history is mandatory source scope. A context cap is a declared model input limit, not permission to narrow source selection.
- Preserve the entire race. No history / insufficient history must preserve incumbent score, not omit the runner.
- Results keyed by exact runner identity; never join by row order or horse display name.
- Forecasts are performance features, not calibrated win probabilities.
- Use deterministic tie-breaking and an attested market/current-model blending policy. Never replace the incumbent just because forecast MAE improved.
- No writes to jockey/trainer/pedigree heatmaps; temporal overlay is isolated from those serving paths.

## Required gates before activation

1. Verify both safetensors and configuration digests before loading; no runtime remote model downloads.
2. Portable MLX FP32 vs standard CPU FP32 predictions pass fixed tolerance. Evaluate native BF16 to portable conversion separately: whole-development native-to-CPU point drift reaches 0.02448 for head and 0.03097 for LoRA; quantile tails differ more. Full development CPU ranking retained head's weak +1 Top1 result, but its frozen 2024–2026 blend subsequently failed: aggregate Top1–Top5 deltas [-2,-2,0,-2,+1], with 2025 annual regression.
3. Whole-race target identity, scratch exclusion and event-time history audit, including available-before-cutoff odds provenance. Existing finisher-only research export is not a production starter-roster attestation.
4. Separate development selection from holdout results. Require Top1 improvement and no annual Top2–Top5 regression against market and exact incumbent predictions. Missing incumbent identities fail closed.
5. Measure Linux CPU p50/p95 latency, memory, cold load, batching and concurrency on the real deployment architecture. Mac CPU timings are not Cloudflare latency claims.
6. Before applying a fine-tuned model, run **independent Rustuna tuning per target cell**, including supported training settings and blending. Bind the selected model/configuration to the cell/cohort and immutable tuning report. A globally tuned head/LoRA blend is insufficient. See [cell-rustuna.md](cell-rustuna.md); the framework exists, but real cell studies and application are not yet performed.
7. Stage only immutable validated artifacts; default-off routing and target-only rescoring; retain incumbent fallback and rollback.

## Current evidence and non-goals

MLX head/LoRA training and portable loading work locally. The non-root Linux/amd64 image ran with networking disabled and a read-only filesystem/model mount: unauthenticated requests returned 401, and one authenticated real inference matched Mac CPU exactly. Its 6.72-second cold request was under Mac emulation, not Cloudflare. See [README.md](README.md) for commands, API schema and scope limitations. The image contains no model: Cloudflare deployment still requires an approved immutable artifact layer or authenticated bootstrap (a local bind mount is not available there). No Chronos model is enabled in Cloudflare and no production predictions have been overwritten. ONNX/OpenVINO/INT8 are optional later optimizations, not prerequisites if PyTorch CPU meets measured budgets. QLoRA is not chosen merely to claim all tuning methods were used.
