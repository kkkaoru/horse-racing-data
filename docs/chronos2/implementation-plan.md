# Chronos-2 MLX implementation plan

## Authority and boundaries

- Requirements: `learn.md` (all 79 sections).
- Measured host: Apple M5 Pro, 48 GiB unified memory. Do not assume Max hardware.
- Upstream baseline: `tsfm-ai/chronos2-mlx` commit `5c29d275fa3c93d6dea5d975e5eb68da71bd6f3b`.
- Research source cache: `.cache/chronos2/research/chronos2-mlx` inside this repository.
- All campaign files, downloads, model caches, logs and checkpoints must live inside this repository. Do not use `.local` or worktrees. Use repository-local `HF_HOME`, `UV_CACHE_DIR` and `TMPDIR` for subsequent commands. Large generated files remain gitignored; reusable code and compact reports are tracked.
- Owned implementation belongs in the primary checkout. Preserve unrelated dirty changes.
- Consider LoRA, QLoRA, head-only and full tuning, but implement/train only justified candidates; using every method is not a requirement.
- Use the existing `apps/timesfm-finish-position` uv environment. One heavy process at a time.
- Mac MLX training is separate from portable standard Chronos-2 safetensors and Cloudflare Linux CPU inference. No MLX artifact is a production contract.

## Ordered work and acceptance

1. Inspect upstream model, adapters, losses, loader and existing Chronos forecasting integration. Pin the dependency and record environment versions. Measure unchanged baseline before optimization.
2. Benchmark identical seed/data/steps: fp32 eager, bf16 eager, bf16 compiled gradient, bf16 compiled with fused SDPA. Also compare full-step compile where optimizer state capture is safe. Record first-step/compile latency separately from warm latency, samples/sec, peak memory, losses and unavailable utilization metrics explicitly.
3. Implement fp32-sensitive reductions, static batches, AdamW warmup/scheduler, gradient accumulation after averaging and before clipping. Test gradient/update parity. Benchmark resident data versus conversion per batch.
4. Assess head-only, LoRA, QLoRA and full tuning and prioritize the best-supported candidates. Audit trainable parameter names and optimizer moments; QLoRA quantized weights/scales/biases must remain exactly fixed. Compare optimizer memory and checkpoint/recompute only where useful. Bound all memory trials conservatively below host capacity.
5. Save restartable checkpoints including optimizer, step, scheduler, RNG and data ordering. Test resume equivalence.
6. Export dequantized/fused standard weights, source revision, training/data hashes, configuration and metadata. Load through standard Chronos-2 PyTorch and measure same-input prediction parity. Optimization is not complete before this passes.
7. Build PIT-safe horse histories with packed elapsed-clock decoding, exact runner identity, complete race groups, strictly earlier all-venue entrant history and scratch exclusion. Keep evaluation cell routing distinct from training scope. Select adaptations using development only; record sequential holdout use explicitly.
8. Compare zero-shot and selected fine-tuned modes against market and incumbent on Top1–Top5, annual regressions and forecast loss. Accuracy improvement is an empirical result, not assumed from faster training.
9. Add default-off portable-artifact inference to existing Cloudflare container architecture, preserving incumbent fallback and heatmaps. Benchmark CPU PyTorch first, then supported ONNX/OpenVINO/INT8 paths. Require artifact hashes, runtime parity and strict accuracy gates before activation.
10. Run full configured tests, coverage, strict typing, lint and formatting without suppressions or gate reductions; document benchmark results, unsupported experiments and final commands.

## Updated application requirement

The user's latest instruction makes **independent Rustuna tuning per cell mandatory before applying a fine-tuned model**. A globally selected head/LoRA configuration or blend is not a cell application policy.

Implementation extension: add a typed, development-only cell study boundary with independently seeded Rustuna studies and complete trial audit. Expose supported fine-tuning knobs (learning rate, decay, warmup, LoRA rank/alpha) in the study driver; sample training settings, context/history gates and blending per cell. Bind results to immutable cell/cohort/runner identities and dates. Require comparison to both market and exact incumbent, then independent holdout/PIT/parity approval. Missing evidence or no strict improvement retains the incumbent. The existing rejected global experiments do not satisfy this requirement and must not be reused as approval.

## Implementation status

- Completed: pinned repo-local MLX environment; strict six-way repeated benchmarks; finite BF16 masks; FP32 trainables/optimizer/loss; accumulation/clipping/compiled steps; local checkpoint loading; periodic saving and fresh-process CLI resume; head/LoRA real-data studies; standard portable export and declared FP32 parity checks.
- Completed: whole-development Mac CPU rescoring, frozen head holdout rejection, isolated Linux/amd64 CPU image and offline/read-only authenticated single-input parity smoke. This does not certify whole-cohort Linux parity or Cloudflare performance.
- Assessed and not adopted: GPU-resident gather (145.6 µs vs 3.25 µs host acquisition in the isolated probe, plus 59.3 MB storage), fused SDPA as default (small incremental gain), QLoRA/full tuning and ONNX/OpenVINO/INT8 without supporting need.
- Added for latest requirement: independent typed per-cell Rustuna search and configurable training knobs/date boundaries. Guard tests use deterministic evaluators; actual per-cell fine-tuning/CPU/incumbent evaluation remains required **before application**.
- Not activated: no approved Chronos winner, cell model publication, Worker binding, live Cloudflare deployment, incumbent overwrite or heatmap changes. Independent starter/PIT/incumbent attestation and per-cell/holdout gates remain mandatory.
- Verification: 262 tests, 96.64% package coverage; all new Chronos files >=90%; full-package Ruff/basedpyright and scoped Chronos ty pass. Full configured ty retains 42 unrelated diagnostics; no exclusions, suppressions, hook bypass or commit. See [README.md](README.md) and [measurements.md](measurements.md).

## Initial findings

Upstream `train.py` has AdamW but does not apply `warmup_steps`; it constructs all rolling windows in memory, permits short final batches and has no compiled training step or accumulation. Head mode also unfreezes final layer norm, which must be stated rather than described as output-head-only. Existing upstream code requires inspection/testing rather than treating the proposed APIs in `learn.md` as already implemented.
