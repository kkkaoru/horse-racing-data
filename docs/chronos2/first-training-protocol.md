# First real-history adaptation protocol

Registered before running the experiment. This is a bounded head-only feasibility study, not a production promotion.

- Data: repository `.cache/chronos2/jra-horse-history-2000-2026-corrected.parquet`, SHA-256 `9f12304770710ae59af91d662279656534330b5457be23eb3d6e5a91f17948e9`.
- Mandatory entrant context: all prior JRA venues from 2000 onward, strictly earlier dates. Same-day outcomes excluded. Explicit model context cap 128 starts, minimum history 2. Record uncapped history counts.
- Training labels: 2020-01-01 through 2022-12-31; scalar `performance_rating` from completed race results.
- Development: 2023-01-01 through 2023-12-31; no training-label updates on this interval. Prior 2023 outcomes may enter later 2023 contexts strictly chronologically. All eligible source runner identities retained in development output; sparse histories have an explicit missing temporal prediction and must keep incumbent scoring.
- Holdout: 2024 onward, not used by this study. Earlier TimesFM holdout examination remains documented separately; these years are not claimed globally untouched.
- Base: pinned official Chronos-2 cached revision `29ec3766d36d6f73f0696f85560a422f50e8498c`.
- Tuning: output patch head only (final layer norm stays frozen), BF16 frozen base, FP32 trainable masters and AdamW moments, TF32 disabled.
- AdamW: LR 1e-5, weight decay 0.01, linear warmup 100 optimizer steps then constant, clip norm 1, batch 32, 1000 steps, seed 42, static drop-last sampling, full-step compile, no fused attention.
- Save zero-shot and final forecasts for all development windows, validation loss summaries and a full restartable checkpoint. Do not select intermediate checkpoints by development results in this first run.
- Primary forecast diagnostics: MAE and pinball loss against the unadapted same-dtype baseline. Racing rank/market blend analysis comes separately on complete development races with explicit zero-history fallback; forecast loss alone never qualifies for production.
- Native BF16 runtime vs portable FP32 export requires its own dtype-specific numerical/rank-error evidence. A prior FP32 synthetic export parity pass is not transferable automatically.
- No model activation, no live prediction writes, no Cloudflare deployment from this experiment.

## Implementation correction and follow-up

The initial real-head-001 attempt failed on nonfinite baseline forecasts before successful updates: upstream FP32 minimum mask values overflow to `-inf` in BF16. Finite-mask correction preserves BF16's own finite minimum and is tested with left-padded histories. The unchanged head protocol completed as real-head-002. Its development forecast MAE improved from 0.262059 to 0.242938. Market blend development gains were only +1/+0/+0/+0/+0 at market weight 0.95; no production superiority is established.

Next compare a separately reported LoRA adaptation, not another selected head checkpoint: same data/split/steps/LR/seed, rank 8, alpha 16, q/v projections in time and group attention, frozen output head and base, FP32 adapter parameters/moments. The reason is to test whether adapting temporal representation improves race ranking beyond output-only calibration. QLoRA is deferred because the 120M model already fits comfortably; full tuning is deferred to limit optimization variance and implementation cost until parameter-efficient evidence warrants it. AdamW is the optimizer for both, not a competing tuning method.

The previous 2023 development observation is explicitly visible. No 2024+ holdout result is used to choose this follow-up.

## Frozen holdout readout

After complete 2023 CPU-FP32 evaluation, head-only retains +1/+0/+0/+0/+0 at market percentile weight 0.95; LoRA has no strict development winner. Freeze **head-only portable FP32, market weight 0.95** for a single 2024–2026 holdout readout. Do not search blend weights on holdout. Require aggregate Top1 > 0 and each year's Top2–Top5 deltas >= 0; still no promotion without exact incumbent, PIT odds and starter-roster attestation. LoRA holdout is not opened. The +1 development gain is weak evidence, not a significant claimed production improvement.
