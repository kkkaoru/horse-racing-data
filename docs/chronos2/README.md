# Chronos-2 MLX lab and isolated CPU runtime

## Result and decision (2026-09-12)

Implemented and tested on **Apple M5 Pro, 48 GiB**. This is not an M5 Max benchmark.

- MLX inference/training environment, finite BF16 padding masks, FP32 loss and optimizer states, gradient accumulation/clipping, compiled steps, atomic checkpoint/restore and portable export are available.
- Head-only and rank-8 Q/V LoRA each completed 1,000 real training updates. QLoRA and full tuning are deferred: the model fits comfortably, and neither tested adaptation established a production ranking gain.
- Head-only improves 2023 forecast MAE approximately **7.3%** versus zero-shot, but its frozen market blend fails the 2024–2026 guard. **Do not promote either artifact.** Incumbent predictions and heatmaps remain unchanged.
- A separate **Linux/amd64 CPU image builds and performs authenticated inference offline**. It is not deployed to Cloudflare, and its local emulated timings are not Cloudflare latency measurements.

Machine-readable measurements, report hashes and configurations: [`results/20260912-summary.json`](results/20260912-summary.json); immutable artifact inventory: [`results/20260912-artifact-manifest.json`](results/20260912-artifact-manifest.json). Large weights, checkpoints, quantiles, keyed predictions and downloaded source remain under repository-local `.cache/chronos2/`, not in Git or `.local`.

## Independent cell campaign and cause investigation

The subsequent JRA / NAR-flat / Ban-ei campaign completed **288 real trials**, matched step-zero attribution, and the five-case/four-arm frozen NAR/Ban-ei later readout. All five later matched MAEs improved, but no primary case met aggregate Top1 improvement plus annual Top2–5 nonregression. The investigation distinguishes training gains from market/readout gains and demonstrates cases where the blend prevents changed model scores from changing ranks. Original JRA rejections remain unchanged; nothing is deployed.

- [Detailed fine-tuning effect investigation (Japanese)](fine-tuning-effect-investigation.md)
- [Frozen later results, supports and paired uncertainty](later-frozen-readout-results.md)
- [Official-source repair provenance and limitations](later-source-repair-plan.md)

Latest checks: 415 tests, 97.12% full-package coverage; new uncertainty module 100%, updated ranking diagnostics 99%. These are research comparisons on previously observed years, not new holdout or PIT certification.

## Reproduce MLX setup and a new study

From repository root:

```bash
ROOT="$PWD"
bash apps/timesfm-finish-position/scripts/run_chronos_mlx.sh uv sync --group lab
bash apps/timesfm-finish-position/scripts/run_chronos_mlx.sh \
  uv run --group lab python -c 'from huggingface_hub import snapshot_download; snapshot_download("amazon/chronos-2", revision="29ec3766d36d6f73f0696f85560a422f50e8498c")'

MODEL="$ROOT/.cache/chronos2/huggingface/hub/models--amazon--chronos-2/snapshots/29ec3766d36d6f73f0696f85560a422f50e8498c/config.json"
HF_HUB_OFFLINE=1 bash apps/timesfm-finish-position/scripts/run_chronos_mlx.sh \
  .venv/bin/python -m timesfm_finish_position.chronos_mlx_study \
  --history "$ROOT/.cache/chronos2/jra-horse-history-2000-2026-corrected.parquet" \
  --source-config "$MODEL" --mode head \
  --output "$ROOT/.cache/chronos2/new-head-study"
```

The launcher sets `HF_HOME`, `UV_CACHE_DIR`, `TMPDIR`, `CHRONOS_CAMPAIGN_ROOT` and **`MLX_ENABLE_TF32=0` before importing MLX**. The pinned MLX dependency is in the package uv lock. `load_local_pipeline` reads the supplied configuration and adjacent standard safetensors directly; it does not resolve a mutable Hub model reference. Use a new output directory: existing studies are not overwritten.

The supplied history is a separately authorized database export, not distributed with the source. SHA-256: `9f12304770710ae59af91d662279656534330b5457be23eb3d6e5a91f17948e9`. See [`first-training-protocol.md`](first-training-protocol.md) for labels, chronology, history windows and the frozen evaluation decisions.

Change `--mode head` to `--mode lora` for the tested rank-8/alpha-16 adaptation. AdamW is the optimizer used for both, not another adaptation method. `save_checkpoint` / `restore_checkpoint` restore model, optimizer, RNG and step while validating dataset/config/shape identity. The study CLI saves every 250 steps by default (`--checkpoint-every`) and at completion. Resume into a **new output directory** using `--resume /absolute/path/to/prior/checkpoints/step-000250` and the same study configuration/total step budget. Changing the dataset, seed, model adaptation or optimizer schedule is rejected rather than silently treated as a continuation. Reported training losses cover post-resume updates; `resumed_step` records their starting offset.

## Optimization evidence

Six configurations × three fresh processes × 100 measured steps, synthetic batch 8/context 128:

| Configuration                  | Median samples/s |
| ------------------------------ | ---------------: |
| A: FP32 eager                  |            729.9 |
| B: frozen BF16, eager          |            980.9 |
| C: compiled gradient           |           1144.5 |
| D: compiled gradient + SDPA    |           1154.3 |
| E: compiled full step + SDPA   |           1452.8 |
| F: compiled full step, no SDPA |           1434.7 |

F is the provisional default: approximately **1.97×** A, with approximately **41% less peak memory**. E's extra gain is only about 1.3%; fused attention stays opt-in. These synthetic warmed training rates are not end-to-end racing training or inference throughput. See [`measurements.md`](measurements.md).

A separate batch-acquisition probe compared host conversion with GPU-resident gather (114,967 synthetic windows, batch 8/context 128; three repetitions). Median synchronized acquisition was **3.25 µs host vs 145.6 µs resident**, with an extra 59.3 MB resident allocation. Host acquisition was ~0.058% of the measured F-step duration. This isolated probe is not end-to-end training, but gives no reason to adopt the more expensive resident gather path.

## Mandatory cell-local tuning before application

Per the latest user instruction, fine-tuned models must use **independent Rustuna hyperparameter tuning per cell** before application. The new typed search boundary tunes supported training settings and blending, binds each trial to the exact cell/cohort/configuration, and rejects market/incumbent regressions. See [`cell-rustuna.md`](cell-rustuna.md) for search ranges, evaluator contract and application gates. This framework is tested; real-data per-cell trials have **not** been run, and no global configuration has been applied as a substitute.

## Real forecast and ranking evidence

2020–2022 labels, strictly prior histories from 2000; 114,967 training windows. Development uses 38,116 eligible windows while retaining all 47,440 source runners for fallback ranking.

| Study     | Native development MAE | Portable CPU MAE | Whole-development CPU inference |
| --------- | ---------------------: | ---------------: | ------------------------------: |
| Head-only |               0.242938 |         0.242931 |                         68.80 s |
| LoRA      |               0.243059 |         0.243047 |                         67.16 s |

CPU measurements above are **Mac CPU**, 4 Torch threads, 128-runner batches. Native BF16-to-portable FP32 point drift across all development windows reaches 0.02448 (head) and 0.03097 (LoRA). Do not confuse that with export parity: FP32 MLX-to-PyTorch first-128-window parity passes the original `atol=rtol=1e-4` (max errors 1.43e-5 and 7.87e-6). Mixed-precision quantile tails differ more and are recorded separately.

Head's fixed 95% market-percentile blend gives development deltas `[+1, 0, 0, 0, 0]`; LoRA has no strict development winner. Freeze the head blend before opening holdout:

| Year                       | Valid-odds source races | Top1–Top5 delta vs market |
| -------------------------- | ----------------------: | ------------------------- |
| 2024                       |                   3,454 | `[0, 0, +1, 0, 0]`        |
| 2025                       |                   3,455 | `[-2, -3, -1, -2, +1]`    |
| 2026 through export cutoff |                   2,442 | `[0, +1, 0, 0, 0]`        |
| Aggregate                  |                   9,351 | `[-2, -2, 0, -2, +1]`     |

**Rejected.** No holdout blend search and no LoRA holdout opening. Earlier TimesFM work had already examined these years, so they are not globally untouched. Final-odds and finisher-source proxies do not establish PIT odds or an independent starter/scratch roster. Exact incumbent comparison and production attestation remain absent; the market guard already fails.

## CPU service / Cloudflare boundary

See [`cloudflare-integration.md`](cloudflare-integration.md). Package files:

- `Dockerfile.chronos` and its own `.dockerignore`: independent of the active prediction Dockerfile; pinned base-image digests, 41 hash-locked CPU dependencies, no MLX/CUDA/training source, no baked experimental weights.
- `chronos_portable.py`: local digest verification, identity/chronology/finite-value guards, sparse-history fallback and exact keyed features.
- `chronos_service.py`: serial HTTP service on port 8080; `/health` reports **artifact-verified**, not model-warmed. `/forecast` requires a >=32-character bearer token, schema version 1, 1–32 runners and a bounded body.

Build from the package directory:

```bash
cd apps/timesfm-finish-position
docker build --platform linux/amd64 -f Dockerfile.chronos \
  -t horse-chronos-cpu:research .
```

Runtime variables: `CHRONOS_ARTIFACT_DIR`, `CHRONOS_MODEL_SHA256`, `CHRONOS_CONFIG_SHA256`, `CHRONOS_SERVICE_TOKEN`. Mount a verified local artifact read-only for local tests. **Cloudflare cannot use a developer-machine bind mount**: an approved artifact needs an immutable image layer or authenticated artifact bootstrap before deployment. Do not bake the rejected study into a production image.

Request schema:

```json
{
  "schema_version": 1,
  "runners": [
    {
      "race_id": "r",
      "horse_id": "h",
      "evaluation_date": "2023-01-03",
      "history": [
        { "race_date": "2023-01-01", "value": 0.2 },
        { "race_date": "2023-01-02", "value": 0.6 }
      ]
    }
  ]
}
```

Every response and feature has `production_eligible:false`. This is a research feature boundary, **not an approval token or ranker**. No Worker binding, routing switch, model publication or Cloudflare deployment has been performed. Live integration remains default-off and requires independent approval/gating.

Local Linux/amd64 (emulated on Mac) validation used `--network none --read-only`, non-root UID, a read-only model mount and 3 GiB memory limit. Unauthorized requests returned 401; an authenticated two-history input produced exactly the Mac CPU result (`0.429196834564209`). Cold request was 6.72 s. This is a **single-input portability smoke test**, not whole-cohort Linux parity or Cloudflare capacity certification.

## Quality checks

```bash
bash apps/timesfm-finish-position/scripts/run_chronos_mlx.sh bash -c \
  '.venv/bin/ruff check src/timesfm_finish_position/chronos*.py tests/test_chronos*.py && .venv/bin/basedpyright --project pyproject.toml && .venv/bin/pytest'
```

Final result after cell-local Rustuna extension: **262 tests, 96.64% branch-inclusive package coverage**; all new Chronos files >=90%. Full-package Ruff format/check and basedpyright pass, as does Chronos-scoped `ty`. The full configured `ty check` retains **42 diagnostics outside Chronos** (other-experiment scripts/source and import paths); the aggregate package `check` is not green. These failures were not hidden or fixed by modifying unrelated work. No commit was attempted.

No lowered thresholds, ignored branches, lint suppressions or hook bypass. Preserve unrelated working-tree changes. Additional extensions—full Linux cohort parity, approved artifact bootstrap and Cloudflare deployment measurements—must be separately validated rather than inferred from these results.
