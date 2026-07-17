# CatBoost Training-Time Optimization Investigation (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position CatBoost training pipeline — performance/engineering investigation, not an accuracy experiment. **Hard constraint: any proposed speedup must produce a numerically bit-identical model (verified by hash of the saved `model.json`, methodology note below).** Anything that doesn't verify as bit-identical is recorded honestly as "non-equivalent, not adopted," never silently accepted anyway.
- **Scope**: `tmp/training-time-optimization-2026-07-17/` (gitignored scratch: `bench_common.py` + 4 focused driver scripts, mirroring `tmp/hpo-catboost-2026-07-17/`'s style). Recipe pinned to the live champion `jra-cb-v9-sim-2013-clean`: armB (250-feat leak-clean) feature spec, `iterations=300, depth=8, learning_rate=0.05, l2_leaf_reg=3.0, grow_policy=SymmetricTree, loss_function=YetiRank, border_count=254, cat_indices=[]`, fold-B (train ≤2023-12-31, blind 2024: 532,549 train rows / 37,998 train races). `thread_count=6` (repo standing default) throughout except Task 3's deliberate sweep. All CatBoost params built via `tmp/hpo-catboost-2026-07-17/hpo_common.py`'s own `build_catboost_params()`, so every number here is apples-to-apples with the live HPO campaign's real fit path. Every run checked `ps aux` for concurrent `catboost|hpo_|pool_wf` jobs immediately before firing — all four benchmark runs found a clean window, so none of the numbers below are contention-inflated.

## Headline result

| #   | Optimization                                           | Bit-identical?               | Adopt?                                                                                                                                           |
| --- | ------------------------------------------------------ | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2   | Reuse a pre-computed CatBoost quantization across fits | **FAIL** (0/4 comparisons)   | **NOT ADOPTED** — genuine, reproducible non-equivalence at production data scale                                                                 |
| 3   | Raise `thread_count` 6→8/12                            | **FAIL** (verified directly) | **NOT ADOPTED AS A FREE WIN** — real 1.25×/1.63× speedup, but changes the model at the bit level; proposal-only, needs accept-gate re-validation |
| 4   | Shared fold-slice Parquet disk cache                   | **PASS**                     | **ADOPTED (verified-safe)** — ~85% cut on the data-loading phase, ~1.1s/invocation                                                               |

**Only Task 4 is a pure "same result, less time" win.** Combining only bit-identity-verified changes into an 18-fit projection (§ closing) saves **~20s out of ~556s (~3.6%)** — small, because `model.fit()` (boosting proper) is >90% of wall-clock and neither of the two changes that targeted that dominant cost (Tasks 2 and 3) survived the bit-identity bar. Task 1's decomposition independently explains _why_ Task 2 had so little room to win in the first place: quantization itself is under 1% of fit time, so even a hypothetical successful reuse would have saved almost nothing.

**Methodology note (load-bearing for every verdict below):** CatBoost's saved `model.json` embeds two fields that vary on literally every `.fit()` call regardless of numerical determinism — `model_info.model_guid` (a random UUID) and `model_info.train_finish_time` (a wall-clock ISO-8601 timestamp). A raw `md5(open(path,"rb").read())` on two `model.json` files from two separate `.fit()` calls **always** differs, even for two 100%-numerically-identical fits. Verified directly: two back-to-back fits of the identical seed+config+data, compared with `diff` on pretty-printed JSON, differed in exactly one line (`model_guid`) the first time and exactly one line (`train_finish_time`) once `model_guid` was excluded — nothing else. If this investigation had used raw file hashing as the literal instruction states, **every single comparison below would read FAIL, including the completely-safe same-seed-same-everything case** — a misleading, useless report. So every verdict here uses a **canonical hash**: `model.json` parsed, `model_info.model_guid` and `model_info.train_finish_time` stripped, keys sorted, then hashed. `bench_common.py`'s `canonical_model_hash()` / `raw_file_md5()` / `diff_model_json()` implement and document this. As a sanity cross-check on the methodology itself: three independent script runs (Task 2's isolation-test "fresh" fit, Task 3's `thread_count=6` fit, Task 4's "fresh-path" fit) all fit the _identical_ champion config/seed/thread_count/data and all three produced the exact same canonical hash, `420a6d898c9f6556f50680388f28767d` — strong evidence the canonicalization is correct and CatBoost is genuinely deterministic at fixed `thread_count`.

---

## Task 1 — Fit-time decomposition

`tmp/training-time-optimization-2026-07-17/bench_decompose.py`. 3 reps (store load is invariant, 1 rep). fold-B split: 532,549 train rows / 37,998 races → 490,580 fit rows / 41,969 OD-eval rows after the OD carve-out; blind 2024 = 46,752 rows / 3,454 races.

| Stage                                            |   mean (s) | stdev (s) | n   |
| ------------------------------------------------ | ---------: | --------: | --- |
| (a) DuckDB/parquet store load                    |      1.781 |     0.000 | 1   |
| (b) fold split + `prepare_feature_matrix`        |      1.059 |     0.728 | 3   |
| (c) `Pool()` construction                        |      0.143 |     0.099 | 3   |
| (d) `model.fit()` (plain, implicit quantization) |     27.701 |     6.538 | 3   |
| (e) `predict()` + metric computation             |      0.220 |     0.020 | 3   |
| **Total (mean stages)**                          | **30.904** |           |     |

**Quantization vs. boosting, inside stage (d)**: a plain `Pool()`→`.fit()` call was compared against an explicit `pool.quantize(border_count=254, random_seed=<matching model seed>)` followed by `.fit()` on the now-pre-quantized pool.

| Sub-measurement                    | mean (s) | stdev (s) | n   |
| ---------------------------------- | -------: | --------: | --- |
| `quantize()` only                  |    0.183 |     0.047 | 3   |
| `.fit()` on the pre-quantized pool |   31.361 |     3.851 | 3   |

Plain-fit mean (27.701s) minus prequantized-fit mean (31.361s) = **−3.660s** — i.e. no measurable shave; the delta is well within the ~4-6.5s run-to-run noise floor observed at both variants. **Quantization (border computation) is under 1% of total fit time (0.18s of ~28-31s) — it was never a meaningful cost to begin with.** Nearly all of stage (d) is boosting proper (300 trees, depth 8, 490K rows), which independently explains why Task 2's reuse attempt had very little theoretical upside even before it failed correctness.

---

## Task 2 — Quantized-pool reuse: bit-identity verification (load-bearing)

`tmp/training-time-optimization-2026-07-17/bench_quantize_reuse.py`. API confirmed via `help(Pool.quantize)` / `help(Pool.save_quantization_borders)` / `inspect.signature(Pool.__init__)` against the installed `catboost==1.2.10` (fast, conclusive — no context7 lookup needed): `Pool.__init__` has no `input_borders` kwarg; quantization is a post-construction in-place mutator (`pool.quantize(...)`, returns `None`, flips `pool.is_quantized()` False→True); `pool.save_quantization_borders(path)` writes a plain-text `<feature_index>\t<border_value>` file; reuse = build a fresh `Pool(...)` from the same data and call `.quantize(input_borders=<path>)` on it.

| #   | Comparison                                                               | PASS/FAIL                 | fresh fit (s) |                   reused total (s) | real (non-metadata) diff count |
| --- | ------------------------------------------------------------------------ | ------------------------- | ------------: | ---------------------------------: | -----------------------------: |
| 1   | Isolation: seed=20260717 throughout, fresh vs. reused-quantization       | **FAIL (non-equivalent)** |        22.419 | 29.177 (reload 0.108 + fit 29.069) |                            227 |
| 2   | Use-case A: seed=20260718, fresh vs. reused-717-borders                  | **FAIL (non-equivalent)** |        32.518 | 22.529 (reload 0.117 + fit 22.412) |                            209 |
| 2   | Use-case A: seed=20260719, fresh vs. reused-717-borders                  | **FAIL (non-equivalent)** |        26.107 | 37.977 (reload 0.139 + fit 37.837) |                            330 |
| 3   | Use-case B: depth 8→6, seed=20260717 fixed, fresh vs. reused-717-borders | **FAIL (non-equivalent)** |        24.678 | 22.677 (reload 0.115 + fit 22.561) |                            215 |

**0/4 PASS.** The one-time quantize+save that produced the reusable borders file cost 0.183s (quantize 0.140s + save 0.043s), amortized across all four reuses above — but it doesn't matter, because the borders it produced don't reproduce what `.fit()` computes internally at this data scale.

### Root-cause investigation (not a deliverable script, but load-bearing for the verdict)

The first raw-md5 run showed _every_ row failing, including the same-seed isolation test — before the methodology note above was understood, this looked like the model-guid artifact rather than a real signal. Ruling that out took a systematic elimination, cheapest hypothesis first:

1. **Metadata artifacts** (`model_guid`, `train_finish_time`) — controlled for via canonical hashing (see methodology note). Re-running with the fix still showed **0/4 PASS**, with hundreds of real `features_info.float_features[i].borders` differences per comparison — a genuine signal, not noise.
2. **File-round-trip precision loss** — tested by comparing (a) `quantize()` on the _same in-memory pool object_ immediately before `.fit()`, no file involved, against (b) the full quantize→save→new-Pool→reload→`.fit()` path. **These two always produced byte-identical canonical hashes**, at both a 100K-row and the full 490K-row scale. The text-file border serialization is lossless — ruled out.
3. **Small-scale false match** — a 100K-row / 30-iteration slice showed all three variants (fresh / same-object-quantize / file-roundtrip) matching exactly. The **full 490,580-row fit slice** with even a _cheap_ 10-iteration/depth-4 config showed the same fresh-vs-explicit-quantize divergence as the full 300-iteration champion config — confirming this is genuinely **scale-triggered**, not a config/iteration-count artifact.
4. **`dev_max_subset_size_for_build_borders`** — CatBoost source (`catboost.core.Pool.quantize`) exposes this undocumented dev kwarg, evidently an internal row-subsampling threshold for border computation. Forcing it to 500,000/1,000,000 (well above the 490,580-row dataset, which should disable any subsampling) changed the explicit-quantize result, confirming this parameter does affect border computation — but the result **still did not match** the fresh/implicit path. So subsampling-threshold mismatch is _a_ factor CatBoost exposes, but not the _complete_ explanation.

**Conclusion**: `Pool.quantize()` (the standalone, explicit API) and `.fit()`'s own internal/lazy quantization are genuinely different code paths inside CatBoost 1.2.10 that compute different quantile borders once the training set is large enough (matches at 100K rows, diverges at 490K rows) — and the public Python API doesn't expose enough control to force them to agree. This is a real CatBoost-internal behavior, not a bug in this investigation's harness. **Recorded honestly as non-equivalent, not adopted** — and, per this codebase's do-not-retest convention, this specific avenue (explicit quantization reuse via `Pool.quantize()`/`save_quantization_borders()`/`input_borders=`) should not be retried against this fold's data scale without new evidence.

**Timing, independent of correctness**: reused-path totals (22.5s–38.0s) were not reliably faster than fresh (22.4s–32.5s) — consistent with Task 1's finding that quantization is too small a fraction of fit time to matter even when reuse works.

---

## Task 3 — `thread_count` scaling curve

`tmp/training-time-optimization-2026-07-17/bench_thread_scaling.py`. Sequential (not concurrent) fits of the identical champion config + seed=20260717 at each `thread_count`. `ps aux` checked immediately before AND after the whole sweep — **clean window both times**, so these are uncontended numbers.

|             thread_count |    fit (s) | speedup vs. tc=6 | bit-identical to tc=6? |
| -----------------------: | ---------: | ---------------: | :--------------------: |
|                        4 |     30.112 |           0.752× |           No           |
| **6** (standing default) | **22.644** |       **1.000×** |           —            |
|                        8 |     18.170 |           1.246× |           No           |
|                       12 |     13.927 |           1.626× |           No           |

`thread_count=12` gives a real, substantial, monotonic **1.63× speedup**. But — checked directly, not assumed — **every non-6 thread*count produced a different (though internally reproducible, i.e. deterministic \_for that thread_count*) canonical hash** than `thread_count=6`. This matches CatBoost's general CPU behavior (parallel histogram summation order depends on thread partitioning, so floating-point accumulation differs by a few ULPs, which cascades into different split choices over 300 boosting iterations). **`thread_count` is a real lever, but it is not a pure speed knob — raising it changes the production model at the bit level**, unlike Tasks 2/4's "same result" framing. It is therefore a _different category_ of change: like any hyperparameter tweak, it needs the existing accept-gate / blind-confirm treatment before being adopted as a new default, not a free win to combine into the verified-safe total below.

---

## Task 4 — Shared fold-slice disk cache

`tmp/training-time-optimization-2026-07-17/bench_data_cache.py`. Format: **Parquet**, not IPC/feather. Checked this codebase's own precedent first, per the task brief's instruction to prefer it over the suggested default: `grep -rl '.write_parquet(' tmp/*/*.py` matches **82** files; `.write_ipc(` matches **0**. Parquet is this repo's actual convention (and the source store itself is Parquet), so the cache uses `pl.DataFrame.write_parquet()` / `pl.read_parquet()`.

**Related-work check** (per the orchestrator's note about a possibly-parallel "fold-slice caching + parallel fits" investigation under a different task name tonight): checked `git status` and `ls tmp/` / `find tmp -maxdepth 1 -type d -newer <this investigation's start>` at both the start and end of this investigation. **No directory matching "fold-slice", "parallel", or "学習時間" naming was found at any point** — nothing to reconcile with as of this writing, but the orchestrator should re-check if such work lands later.

| Measurement                                                                     |               Value |
| ------------------------------------------------------------------------------- | ------------------: |
| `load_store()`                                                                  |              1.189s |
| `fold_b()`                                                                      |              0.004s |
| `sort(race_id, umaban)`                                                         |              0.086s |
| `prepare_feature_matrix()` (train+valid combined)                               |              0.017s |
| Baseline, task's literal 3 fns (`load_store`+`fold_b`+`prepare_feature_matrix`) |          **1.210s** |
| Baseline, fair total (+ sort — what the cache actually replaces)                |          **1.296s** |
| Cache write (one-time, 195.6 MB)                                                |              0.167s |
| **Cache reload**                                                                |          **0.197s** |
| **Savings vs. fair baseline**                                                   | **+1.099s (84.8%)** |
| Savings vs. task's literal 3-fn baseline                                        |     +1.013s (83.7%) |

**Correctness** (the part that matters most, per this investigation's constraint): the cache round-trip was independently verified NOT to silently perturb anything before even looking at model output —

- Row order preserved (`race_id`, `umaban` sequences identical pre-write vs. post-reload): **True**
- Dtypes match, including confirming Parquet did **not** silently upcast the pre-cast `Float32` feature columns to `Float64`: **True**
- Fit once via the fresh path, once via the cache-reload path (same seed, same champion config): canonical hashes **both `420a6d898c9f6556f50680388f28767d`** — **bit-identical, PASS**. (This is also the same hash Task 2's isolation "fresh" fit and Task 3's `thread_count=6` fit produced independently — see the methodology note's cross-check.)

**Verdict: ADOPTED (verified-safe).** ~85% cut on the data-loading phase, ~1.1s saved per invocation that currently pays it from scratch, zero risk to model output.

---

## Closing projection: cost per fit, and an 18-fit run

**What "18 fits" means in this codebase**: confirmed as an established convention, not assumed — `docs/probes/nar-clean-retrain-fullstore-2026-07-04.md`: _"18 models trained (2 arms x 3 folds x 3 seeds)"_; `docs/probes/jra-meet-repeat-2026-07-04.md`: _"18 CatBoost models: base+cand x 3 folds x 3 seeds"_. So **18 fits = 2 arms (base + candidate) × 3 folds × 3 seeds**, the standard blind-confirm/gate-check campaign shape.

**Per-fit cost today** (Task 1's full stage total, one representative fold-B-sized fit): **30.904s**.

**18-fit baseline, independent invocations** (the framing Task 4's own brief uses — "paid on every single script invocation" — realistic for standalone verification runs or a parallelized-fits pattern; see caveat below for the alternative):

```
18 x 30.904s = 556.3s (~9.3 min)
```

**Combining only the bit-identity-verified change (Task 4)** into the total — Task 2 contributes nothing (failed verification), Task 3 contributes nothing to this "verified-safe" total (real speedup, but not bit-identical, so excluded per this investigation's own combination rule):

```
18 x (30.904s - 1.099s) = 18 x 29.805s = 536.5s (~8.94 min)
Total saved: 18 x 1.099s = ~19.8s (~3.6% of total wall-clock)
```

**Caveat, for honesty**: this 18× multiplier on the data-loading phase is the conservative/generic framing, and it's the one Task 4's own brief calls for — but it's worth noting today's actual campaign scripts (e.g. `tmp/hpo-catboost-2026-07-17/hpo_blind_confirm.py`) already load the store **once** and loop fits **in-process**, so within _that specific script_ the store-load portion is already naturally amortized across all seeds/configs in one run (not re-paid 18×). In that in-process scenario the cache's addressable saving shrinks to just the per-fit re-sort/re-cast `fit_with_od()` currently repeats on every call (~0.1s × 17 ≈ 1.7s total, <1%) — real but smaller. The 18×-independent-invocations framing is realistic for standalone/parallelized-fit patterns (relevant if the concurrent "fold-slice + parallel fits" work mentioned by the orchestrator materializes).

**For contrast, NOT combined into the verified-safe total** (real lever, not bit-identical, needs accept-gate re-validation before adoption): if `thread_count=12` were accuracy-revalidated and adopted, it would cut the _dominant_ fit()-stage cost by 8.717s/fit:

```
18 x 8.717s = ~156.9s (~2.6 min, ~28% of the 556.3s baseline)
```

This is roughly **8× bigger** than Task 4's verified-safe saving — the practical takeaway of this whole investigation is that **the two big potential wins (quantization reuse, more threads) both touch the dominant `.fit()` cost but neither survived bit-identity as a free lunch; the one change that did survive only touches a ~4% slice of total wall-clock.**

---

## Proposals for production rails (proposal-only — `train_finish_position_catboost_walk_forward.py` / `§8.1` WF harness NOT modified here)

Read `docs/finish-position-prediction-system.md` §8 (training pipeline) and the actual source (`src/scripts/train_finish_position_catboost_walk_forward.py`, `src/scripts/walk_forward_common.py`, `src/scripts/finish_position_catboost.py`) for grounding. Production is already ahead of this campaign's own `tmp/hpo-catboost-2026-07-17/` ad-hoc scripts in one respect worth calling out: `run()` calls `wfc_common.sort_full_dataset()` **once** per process and loops `train_fold()` over `fold_years` in-memory, with `sort_train_valid_for_grouping()` skipping the redundant per-fold sort entirely when `presorted=True` (line 332). `hpo_common.fit_with_od()`, by contrast, unconditionally re-sorts on every single call — so the sort-cost part of Task 4's finding is already solved in production _within one run_; nothing to change there.

1. **Task 4 (verified-safe) — fold-slice Parquet cache, scoped to _across-process_ reuse.** Since production already avoids intra-run redundant sorting, the applicable win is the store **load** + `prepare_feature_matrix` cast being repeated across _separate_ process invocations: `continuous_learner.py`'s repeated train→predict→verify iterations, `feature_explorer.py`'s Optuna trials, and any future multi-invocation HPO/campaign harness (this campaign's own `hpo_selection.py`/`hpo_blind_confirm.py` included) all plausibly reload the same underlying parquet store from scratch every time. Concrete proposal: add an optional `--fold-cache-dir <path>` to `train_finish_position_catboost_walk_forward.py` (or a shared helper alongside `wfc_common.sort_full_dataset()`), keyed by a content hash (source parquet mtime + row count + feature-list hash — **not** just a path, so a stale cache can never silently serve outdated features after a data refresh, matching the provenance discipline in §8.12's routing-JSON rules) of `(features_parquet, feature_cols, category)`. On a cache hit, skip `deps["parquet_reader"]` + `sort_full_dataset()` entirely and load the pre-cast Parquet cache instead. **Must be re-verified the same way this investigation did** (canonical-hash bit-identity check, row-order/dtype assertions) against production's real (larger, non-armB) feature set before being trusted — this investigation only verified it on the 250-feature HPO-campaign slice.

2. **Task 3 (real lever, not free) — `thread_count` 6→8/12 for off-peak/dedicated runs.** `--thread-count` is already a first-class CLI flag (`DEFAULT_THREAD_COUNT = 6` at `finish_position_catboost.py:54`), so trying this needs zero code change — only a decision process. Since it's verified to change the model at the bit level, it must go through this codebase's existing accept-gate / blind-confirm scrutiny (§7.2-style: run the 3-seed blind-confirm at `thread_count=12` vs. the `thread_count=6` champion and confirm no gated metric moves beyond the established noise floor, `project_training_noise_floor_2026_07_11.md`'s ±0.4pp top1) before ever flipping the _default_. Given this machine's actual standing condition — other agents training concurrently most nights — treat it as an opportunistic, `ps aux`-gated lever for confirmed-idle windows (exactly how this investigation itself checked before every run), not a blanket default bump that could starve concurrent jobs.

3. **Task 2 (failed) — no action.** Explicit quantization reuse via `Pool.quantize()`/`save_quantization_borders()`/`input_borders=` does not reproduce `.fit()`'s implicit quantization at this codebase's real fold sizes (verified on the full 490,580-row fold-B slice). Not proposed for production; do not retry against this data scale without new evidence (e.g. a CatBoost version bump that changes the internal quantization implementation).

4. **Task 1 — no direct action item**, but its finding usefully closes off a whole class of future "optimize quantization" proposals: at production data scale, quantization is under 1% of `model.fit()`'s wall-clock, so no version of Task 2 (however it were implemented) could have saved more than a rounding error. Anyone tempted to revisit CatBoost quantization-level optimizations for this pipeline should read this section first.
