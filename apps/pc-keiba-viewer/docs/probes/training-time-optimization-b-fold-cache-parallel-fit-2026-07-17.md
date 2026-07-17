---
probe: training-time-optimization-b
date: 2026-07-17
category: infra (WF orchestration)
method: shared fold-slice cache + safe parallel CatBoost fit runner, verified bit-identical against the existing harness before any adoption
status: BUILT AND VERIFIED — bit-identical proven, 1.64x wall-clock on a 9-fit benchmark, honest caveat on where the speedup actually comes from
---

# Training-Time Optimization B: Fold-Slice Cache + Safe Parallel Fit (2026-07-17)

Companion to "training-time optimization A" (fit decomposition + quantization
reuse bit-identical verification, tracked separately by a different agent —
no doc existed yet to merge into at the time this was written, hence a new
doc rather than a section append). Prompted by a same-day observation: many
parallel agents today (myself included, twice — the vector-knn probe and the
longshot detector) each independently re-scanned the same underlying store
and re-derived the same fold splits before training. This doc builds and
verifies shared infrastructure to eliminate that duplicated work, under a
hard non-negotiable bar: **every result produced through this infrastructure
must be bit-identical to what the existing, already-trusted harness produces
without it.**

## 1. Design

**Fold definitions** (this repo's standard convention,
`docs/finish-position-prediction-system.md` §8.9): Fold A = train
`race_year<=2022`, blind `race_year==2023`. Fold B = train `race_year<=2023`,
blind `race_year==2024`. Fold C = train `race_year<=2024`, blind
`race_year==2025`.

**Feature spec**: armB-250, read directly from
`tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` (key
`"armB"`) — not re-typed, so the cache can never silently drift from the
actual champion-baseline spec other WF harnesses use.

**Source**: `tmp/candidate-eval-jra/augmented/**/*.parquet` — the exact
store `retest_wf.py`, the vector-knn probe, and the longshot detector all
used today.

**Cache layout** (`apps/pc-keiba-viewer/tmp/shared-fold-cache/`, not
committed — gitignored `tmp/` per repo convention, same as every other
harness artifact today):

```
manifest.json
fold-A/{train,blind}.parquet   fold-B/{train,blind}.parquet   fold-C/{train,blind}.parquet
build_cache.py   # builder, safely re-runnable
loader.py        # thin loader other scripts import
verify_cache.py  # the correctness proof below, re-runnable
```

`manifest.json` carries `feature_set_hash` (SHA256 of the sorted armB-250
column list — computed value: `e931dd0b1259ab53c2f723202f37ca0fe4a310e4caba7bce1ef34f0ec3b0eaa7`),
the full `feature_columns`/`identity_columns` lists, and per-fold
`{train_years, blind_year, train_rows, blind_rows}`.

**Loader contract**: `load_fold(fold, split, expected_feature_hash=None) ->
DataFrame`, which asserts the manifest's `feature_set_hash` matches a
caller-supplied hash (computed independently by the caller from the feature
list it actually intends to use — not read back off the cache itself, which
would be circular) when provided, and asserts the loaded row count matches
the manifest's recorded count, before returning. This is the safety rail
against silently training on a stale or wrong-spec cache.

## 2. Correctness verification (the actual gate)

**Method**: for each of the 3 folds, `retest_wf.py`'s own `load_store()` +
`train_finish_position_catboost_walk_forward.split_train_valid()` were
called directly (imported, not reimplemented) to produce ground-truth
train/blind DataFrames, then compared against the cache-loaded equivalents:
row-count match, `(race_id, ketto_toroku_bango)` key-set equality, then
`polars.DataFrame.equals()` (exact value + dtype match) across all 266
columns after sorting both frames by `(race_id, umaban)`.

| Fold           | train        | blind        | train rows | blind rows |
| -------------- | ------------ | ------------ | ---------: | ---------: |
| A (blind 2023) | PASS (exact) | PASS (exact) |    485,275 |     47,274 |
| B (blind 2024) | PASS (exact) | PASS (exact) |    532,549 |     46,752 |
| C (blind 2025) | PASS (exact) | PASS (exact) |    579,301 |     47,497 |

Zero discrepancies in any of the 6 comparisons. (The row counts above
independently cross-validate against the same numbers this session already
observed today from `retest_wf.py`'s own printed fold stats during the
vector-knn probe — a useful sanity check that this is measuring the same
thing, not a coincidentally-matching different pipeline.)

**Stronger check — an actual model, not just a DataFrame comparison**: fold
A, CatBoost (iterations=50 — a light verification fit, not a real WF result;
see §4 on why iteration count was deliberately kept low), seed=42,
thread_count=4, depth=8, lr=0.05, l2=3.0, trained once on the
`load_store()`-derived train set and once on the cache-loaded train set.
Predictions on the 47,274-row blind set: **byte-identical**
(`np.array_equal=True`, `max_abs_diff=0.0`).

**One honestly-reported non-identical finding**: the raw `model.json` files
were _not_ byte-identical. Full recursive key-diff isolated this to exactly
two fields — `model_info.model_guid` (a random UUID CatBoost stamps at every
`save_model()` call) and `model_info.train_finish_time` (a wall-clock
timestamp) — both non-deterministic save-time metadata unrelated to data,
seed, or the cache. `oblivious_trees` (the actual tree structure/splits/leaf
values), `features_info`, `scale_and_bias`, and every other field verified
identical via SHA256. This is the one and only non-bit-identical result
across the entire verification, and it does not affect predictions or tree
content — reported here rather than silently smoothed over.

## 3. Safe parallel fit runner

`apps/pc-keiba-viewer/tmp/training-time-optimization-b/parallel_fit_runner.py`:
`concurrent.futures.ProcessPoolExecutor`-based, 2-3 workers, `thread_count=4`
per worker (8-12 total threads — reasonable given ~15 cores shared with
other concurrent agents today). Checks `memory_pressure -Q` before launching
and aborts if free memory is below the team's standing 15% floor.

**Correctness**: fold A x 3 seeds (42/101/2026) trained sequentially, via
2-worker parallel, and via 3-worker parallel. Predictions and content-hashed
models (same "minus the 2 volatile fields" method as §2) matched exactly
across all three modes, for all 3 seeds.

**RNG-seeding check**: seed 42 ran in three different OS processes across
the three modes (pid 25308 sequential, pid 25551 2-worker, pid 25715
3-worker) and produced bit-identical output every time — confirming the
seed is driven solely by the explicit `task.seed` argument, not by process
identity or spawn order (a real bug class this specifically guards against —
process-pool workers can silently inherit RNG state from spawn order if a
seed isn't threaded through explicitly).

| Mode                | wall-clock (fold A x 3 seeds) |
| ------------------- | ----------------------------: |
| Sequential          |                        34.12s |
| Parallel, 2 workers |                        25.36s |
| Parallel, 3 workers |                        19.81s |

## 4. Benchmark: WF wall-clock, before vs. after

Shape: 3 seeds x 3 folds = 9 fits, `iterations=50` (deliberately reduced from
the champion's 300 — this benchmark's purpose is proving the mechanism and
measuring overhead/speedup shape, not producing a real WF accuracy result;
kept light per the orchestrator's explicit ask to measure considerately
given other agents were running concurrently on this machine today).

|                                                                                       | wall-clock | load/rebuild |  train |
| ------------------------------------------------------------------------------------- | ---------: | -----------: | -----: |
| **Before** (fresh `load_store()`+split per fold, sequential — today's actual pattern) |    101.47s |        3.32s | 97.40s |
| **After** (cache + 3-worker parallel runner)                                          |     61.74s |        0.57s | 61.17s |

**Speedup: 1.64x overall wall-clock.** Load/rebuild-specific speedup: 5.8x
(3.32s -> 0.57s).

### Honest caveat — read before assuming this generalizes

Most of today's 1.64x came from parallelizing fits, not from caching. On
this store, the DuckDB scan itself is already cheap (~1.1-1.7s per fold
call, 3.32s total for all 3) — eliminating it saved only ~2.75s in absolute
terms against a 101s baseline. Individual per-fit training time actually got
_slower_ under 3-way parallelism (fold-A fits: ~9.1s solo vs. ~16.4s when 3
ran concurrently; fold-C fits up to 23.75s) — genuine CPU contention from
stacking 3x4=12 threads on a shared 15-core machine that had other agents
active in this same session today. The net win comes from overlapping fits
despite each one individually slowing down, not from anything close to
linear 3x scaling.

The cache's real practical value is **not fully captured by this
single-script before/after number**: it eliminates the _same_ store rescan
being separately paid by _every different agent's own script_ across a
session (the actual problem this task was scoped to solve, per the
orchestrator's framing) — value that compounds with more concurrent
consumers reusing the cache, not something one benchmark run can show in
full. On a less-contended machine, or with more fits sharing one cache
build, the parallel-fit speedup specifically would likely look better than
1.64x; on this specific measurement, contention ate a meaningful chunk of
the theoretical gain, and that's reported honestly rather than rounded up.

## 5. Usage (for other agents/sessions to adopt)

```python
# from a script run under apps/pc-keiba-viewer:
import hashlib, json, sys
sys.path.insert(0, "tmp/shared-fold-cache")
from loader import load_fold

my_feats = json.load(open("tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json"))["armB"]
my_hash = hashlib.sha256(json.dumps(sorted(my_feats)).encode()).hexdigest()  # compute independently, never read back off the cache
train_df = load_fold("A", "train", expected_feature_hash=my_hash)
blind_df = load_fold("A", "blind", expected_feature_hash=my_hash)
```

`expected_feature_hash` only protects you if computed independently from
the feature list you actually intend to use (circular otherwise) — it
guards against silently training against a cache built for a different or
stale feature spec (armA instead of armB, or a future armB revision). The
row-count assert (automatic, no argument needed) guards against a
truncated/corrupted parquet being read as if complete.

For parallel fits:
`sys.path.insert(0, "tmp/training-time-optimization-b"); from parallel_fit_runner import FitTask, run_parallel`
— `run_parallel(tasks, max_workers=3)` checks `memory_pressure -Q` first and
aborts below the 15% floor.

**Rebuilding**: the cache is a local, uncommitted, machine-specific artifact
(637MB across 6 parquet files) — anyone who needs it re-runs
`uv run python tmp/shared-fold-cache/build_cache.py` from
`apps/pc-keiba-viewer` (a few seconds; the store scan itself is cheap, per
§4's own finding). This is the same "uncommitted but discoverable via doc
reference" pattern every other tmp/ harness script in this repo already
uses (`retest_wf.py`, `probe_partial_rho.py`, etc.) — nothing new is added
to git beyond this doc.

## 6. Recommendation

Adopt for future WF-heavy sessions: the cache is proven bit-identical
against the trusted harness and costs almost nothing to build/rebuild
(2.9s). The parallel runner is proven bit-identical and does help, though
its real-world speedup depends heavily on how much else is running on the
machine concurrently — treat the 1.64x as a today-specific, contended-machine
data point, not a guaranteed multiplier. Recommend defaulting to 2 workers
rather than 3 on a machine with other concurrent agents active (the 2-worker
number showed less per-fit slowdown from contention in §3's data), reserving
3 workers for genuinely idle-machine windows.

## Artifacts

- `apps/pc-keiba-viewer/tmp/shared-fold-cache/{manifest.json, fold-{A,B,C}/{train,blind}.parquet, build_cache.py, loader.py, verify_cache.py, _verify_report.json}`
- `apps/pc-keiba-viewer/tmp/training-time-optimization-b/{parallel_fit_runner.py, verify_parallel.py, benchmark_wf.py, _verify_parallel_report.json, _benchmark_report.json}`
- (not committed — `tmp/` per repo convention; this doc is the durable,
  committed reference)
