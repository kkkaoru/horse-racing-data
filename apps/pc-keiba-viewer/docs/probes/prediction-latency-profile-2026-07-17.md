# Per-Race Feature-Build Latency Profile + Safe Speedups (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position production serving — performance investigation. **Hard constraint: accuracy must be provably unchanged.** Any candidate without an explicit zero-accuracy-impact argument is a proposal only, never implemented here.
- **Task**: 性能調査① (USER-directed, team-lead-assigned). Profile the per-race feature-build pipeline, rank safe speedup candidates, implement only the trivial+safe ones.

## 0. A better data source than a fresh local reproduction

Before building a new local profiling harness, I found that production already
carries real per-layer timing data: `pipeline_runner.record_layer_timing_row`
writes to a temporary Neon table (`_debug_finish_position_layer_timing`),
added 2026-07-02 to investigate a container-hang incident, gated by
`PREDICT_DEBUG_LOGS`. It stopped receiving new rows after 2026-07-11 (the
env var was presumably turned back off once that investigation closed), but
the 364 rows it already collected (2026-07-02 → 07-11, JRA/NAR/Ban-ei) are
**real production timings against the actual R2 Iceberg Catalog**, not a
local approximation — strictly better evidence than anything I could
reproduce locally, so I used it as primary evidence and only ran a small
local comparison (section 3) for the local-vs-production I/O question team-lead specifically asked about.

## 1. Real per-race breakdown (primary evidence)

Of the 9 distinct JRA runs in the table, exactly one used `--target-race`
(a genuine single-race build, not a whole-day batch): `keibajo=10 (Kokura)
race=07`, 2026-07-05, 17 layers (16 `LAYER_CHAIN` scripts + the base build).
**Total: 502.93s ≈ 8.4 minutes** — close to the ~7min average team-lead
cited (this is one sample, not a distribution, but it's the right order of
magnitude and not an outlier relative to it).

| Layer                                        | Elapsed (s) | % of total |
| -------------------------------------------- | ----------- | ---------- |
| `__base_build__`                             | 31.30       | 6.2%       |
| `add-race-internal-features.py`              | 3.41        | 0.7%       |
| `add-market-signal-features.py`              | 26.76       | 5.3%       |
| `add-sectional-and-weight-features.py`       | 51.09       | 10.2%      |
| `add-futan-juryo-features.py`                | 40.17       | 8.0%       |
| `add-workout-features.py`                    | 27.25       | 5.4%       |
| `add-near-miss-features.py`                  | 42.54       | 8.5%       |
| `add-grade-race-lineage-features.py`         | 41.29       | 8.2%       |
| `add-head-to-head-features.py`               | 23.04       | 4.6%       |
| **`add-baba-pedigree-affinity-features.py`** | **86.32**   | **17.2%**  |
| `add-trainer-stable-affinity-features.py`    | 28.41       | 5.6%       |
| `add-pacestyle-features.py`                  | 3.98        | 0.8%       |
| `add-course-numerical-features.py`           | 3.07        | 0.6%       |
| `add-relationship-r1-features.py`            | 26.07       | 5.2%       |
| `add_kohan3f_going_features.py`              | 5.77        | 1.1%       |
| `add-similar-race-features.py`               | 26.05       | 5.2%       |
| `add-sire-venue-bias-features.py`            | 18.19       | 3.6%       |
| `add-jra-jockey-pedigree-cell-features.py`   | 16.95       | 3.4%       |

**Top 3 sinks in this per-race sample: `baba_pedigree` (17.2%, the single
largest layer by a wide margin), `sectional_weight` (10.2%), `near_miss`
(8.5%)**, with `lineage` (8.2%) and `futan_juryo` (8.0%) close behind.
`head_to_head` — the layer this campaign's own offline replay work needed
to raise DuckDB memory to 12GB for (OOM risk) — is a modest 4.6% of wall
time here. **Memory risk and time cost are two different axes**: h2h is
memory-hungry but not the dominant time sink; `baba_pedigree` is the
opposite (no known memory pressure, but the biggest single time cost).

**Whole-day builds show a different top-2** (3 JRA runs with a full
16-layer breakdown, `--target-date` mode covering an entire day's card):
`near_miss` (65.0s / 41.2s / 64.8s across the three days) and
`head_to_head` (52.4s / 36.1s / 60.2s) are consistently the top two —
`baba_pedigree` is comparatively small (17-24s) in every whole-day sample.
This is a real, worth-flagging discrepancy from the per-race sample, most
likely because `near_miss`/`head_to_head` scan cost scales with the
**history population**, not the target-race count, so it doesn't shrink
much in a whole-day batch even though it's amortized over more races —
while `baba_pedigree`'s outsized 86.3s in the one per-race sample could
be I/O variance specific to that race/date rather than a systematic
per-race-vs-whole-day pattern. **n=1 for the per-race full-chain sample,
stated plainly** — worth a second per-race sample once `PREDICT_DEBUG_LOGS`
is re-enabled, if this investigation continues.

## 2. Serve-defect's own worst-case figure, contextualized

`docs/probes/jra-serving-audit-jun-jul-2026-07-17.md` cites a documented
JRA worst-case of **~27.5 minutes/race** (§ re: self-heal re-enqueue slot
occupation) — roughly 3.3x the 8.4-minute sample above. `pipeline_runner.py`'s
own subprocess-timeout comment independently corroborates the same range:
"the whole chain, all layers combined, has been observed taking up to
~25-27 minutes for JRA." Both of these describe the **tail**, not the
typical case — `predict_upcoming.py`'s own code comment states "typically
2-5 min" for the design assumption. The 8.4-minute real sample sits between
those two reference points, consistent with being a normal (not
worst-case, not best-case) instance.

## 3. Local vs. production I/O — quick comparison, not a full re-profile

A base-build-only comparison, from a local run already on disk this
session (`tmp/candidate-jra-champion-fresh2026h1-2026-07-17/
blind_gate_smoketest.log`, local PG, 3-day window `20260710..20260712`,
946 target rows) against the production per-race sample above:

|                                | Local (PG, 3-day window)                               | Production (R2 Catalog, 1 race)                                                                        |
| ------------------------------ | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| Base build                     | 24.25s                                                 | 31.30s                                                                                                 |
| Full chain (base + all layers) | 224.7s (16 layers, no `all`-mode race count breakdown) | 502.93s (17 layers, incl. the same-day-cumulative `jockey_pedigree_cell` layer local runs don't build) |

**The base build itself is not dramatically slower against R2 Catalog than
local PG** (31s vs 24s, ~30% overhead) — modest, not an order of
magnitude. This is evidence AGAINST the naive assumption that "production
is slow because R2 catalog reads are inherently much slower than
Postgres" — at least for the base build step specifically. The full-chain
gap (503s prod vs 225s local) is larger, but the two runs aren't a clean
apples-to-apples comparison (different population sizes, different target
scope, and the local run is missing the same-day-cumulative layer) — **not
strong enough evidence to conclude catalog reads dominate the full-chain
gap**; the per-layer breakdown in section 1 (all from real production
catalog reads) is the more trustworthy source for "where does time go",
and it points at specific layers' own PG-history-scan costs
(`baba_pedigree`, `near_miss`, `head_to_head`, `sectional_weight`), not at
a generic "catalog is slow" story.

## 4. Ranked safe-speedup candidates

Each candidate states its accuracy-impact argument explicitly, per the
hard constraint.

### (a) Early presence guard — IMPLEMENTED (section 5)

**Zero accuracy impact by construction**: reuses an existing query
(`_query_upcoming_race_keys`, already run before every per-race build for
an unrelated purpose) to skip the ~2-9 minute build entirely when the
source catalog has zero rows for the requested race — reaching the
**same, already-existing** empty-mapping return path
(`build_upcoming_feature_rows`'s own `if not built: return {}`), just
earlier. Cannot change which races get scored; can only change how fast a
doomed build reaches the outcome it was always going to reach. See
section 5 for the full argument, the diff, and the 3 new tests that pin
this contract down mechanically (not just by prose).

**Estimated savings**: full per-race cost (≈8.4 min typical, up to ~27.5
min worst-case) for every race this guard actually catches — the size of
the win depends entirely on how often a per-race build is requested for a
race the catalog doesn't have entries for yet (a genuinely "doomed"
request), which this investigation did not have data to quantify (the
debug timing table doesn't record catalog-empty attempts, only completed
layer runs).

### (b) Layer parallelization — proposed only, not implemented

The `DAY_CHAIN`/`RACE_CHAIN` split (`pipeline_args.py`, already live
infrastructure — see section 6) already documents which of the 17 layers
are DAY-STABLE vs RACE-FRESH, but within either chain the layers still run
**sequentially**, each waiting on the previous layer's parquet output.
Several `LAYER_CHAIN` scripts read independent PG history tables and only
join back to the SAME upstream base parquet — e.g. nothing in section 1's
data suggests `add-workout-features.py` (27.25s) depends on
`add-futan-juryo-features.py` (40.17s)'s own output beyond the shared base
columns both read.

**Accuracy argument, not yet fully verified**: parallelizing independent
layers is accuracy-neutral ONLY if (i) no two layers write to the same
output column (a silent overwrite would corrupt, not just slow, the
build) and (ii) no layer's own script reads a column another
LAYER_CHAIN script — not the base build — produces (an actual sequential
dependency, not just file-order coincidence). Neither was verified column-
by-column across all 17 scripts in this task's time budget — this is
exactly the kind of "looks safe, could be wrong" claim that needs the
same rigor as section 5's tests before touching production, not a
same-session implementation.

**Estimated savings**: if the ~9 DAY_CHAIN-eligible-but-currently-
sequential layers could run in 2-3 concurrent groups, a rough
lower-bound estimate (sum of the top 2-3 layers per group, not the full
serial sum) suggests **cutting DAY_CHAIN's own wall time by roughly
30-50%** — speculative, pending the dependency-graph verification above.

### (c) DuckDB config (threads/memory/attach reuse) — proposed only

Section 1's finding that h2h (memory-hungry, per this campaign's own
offline-replay experience) is NOT the dominant time sink suggests raising
memory alone would help avoid spill-driven stalls under load, but is
unlikely to be the biggest lever for the layers that ARE the dominant time
sinks (`baba_pedigree`, `sectional_weight`, `near_miss`) unless those are
ALSO memory-constrained in production — not established either way by the
data gathered here. **Accuracy argument**: raising `--memory-limit`/
`--threads` or reusing a DuckDB `ATTACH` connection across layers (instead
of each subprocess reconnecting to the catalog fresh) changes performance
characteristics only, never query results, for read-only aggregation
queries — genuinely zero accuracy risk **if** implemented as a pure
resource/connection-reuse change with no query-logic edits. Not
implemented here because production's actual per-layer memory ceiling and
whether `ATTACH`/subprocess-per-layer is a hard architectural constraint
(each layer script is invoked as a **separate subprocess** today, per
`pipeline_args.py`'s design — connection reuse would need each script to
stop being an independent subprocess, a more invasive change than "tune a
flag") were not verified in this task's scope.

**Estimated savings**: modest for the top-3 sinks specifically unless they
are independently confirmed memory-constrained; potentially meaningful
for h2h's own OOM-avoidance margin. Not quantified.

### (d) Per-day intermediate result sharing — already exists, owned by investigation ②

This is **not a new proposal** — `pipeline_args.py`'s `DAY_CHAIN`/
`RACE_CHAIN` split and `pipeline_runner.py`'s `build_day_base`/
`ensure_day_base`/`build_upcoming_feature_rows_split` are already-live
production infrastructure (a day-base cache keyed by category+day, reused
across every race of that day so only the 5 RACE_CHAIN JRA layers
[`market_signal`, `near_miss`, `baba_pedigree`, `relationship`,
`jockey_pedigree_cell`] re-run per race instead of all 17). Per team-lead's
own framing, extending/completing this is the explicit scope of the
parallel "調査②" (performance investigation #2, task #116 on today's
board) — not duplicated here. One relevant cross-reference for that
investigation: of section 1's top-3 sinks, `baba_pedigree` and
(indirectly, via history-scan cost) `near_miss` are **both RACE_CHAIN
members already** — the day-base split doesn't remove their cost per-race
today, so a further optimization there (e.g. can baba_pedigree's own
current-day-condition lookup be made cheaper without becoming stale)
could plausibly be higher-leverage than moving MORE layers into
DAY_CHAIN, worth flagging to whoever owns investigation ②.

### (e) Projection pushdown to declared `feature_names` — proposed only

**Accuracy argument, in principle sound but not implemented**: reading
only the columns a model's declared `feature_names` actually needs (vs.
each layer script's current full-`SELECT *`-style output) cannot change
prediction values for a well-formed pipeline, since the final scoring step
already discards every non-`feature_names` column at `build_feature_matrix`
time (confirmed directly: `predict_lib.scorer.build_feature_matrix` takes
an explicit `feature_names` sequence and only reads those keys from each
entry dict) — the columns being trimmed are already dead weight by the
time scoring happens. **Why not implemented here**: several `LAYER_CHAIN`
scripts pass their FULL output forward as the _input_ to the next layer in
the chain (verified in `pipeline_args.py`'s own docstring: "reproduces the
exact feature set... plus the v8 layers" — layers build cumulatively), so
naively projecting to the FINAL model's feature set at any INTERMEDIATE
layer risks dropping a column a LATER layer's own join/derivation still
needs, even though that column never reaches the final model. Getting this
right requires either (i) computing the full transitive column
dependency graph across all 17 scripts (not done in this task's budget) or
(ii) only projecting at the very last step (reading the final parquet),
which is a much smaller, safer win than pushing projection down INTO each
layer's own read.

**Estimated savings**: the safe version (project only at final-parquet-read
time, not per-layer) saves I/O on the pandas `read_parquet` +
`groupby`/`to_dict` step in `build_upcoming_feature_rows` (currently reads
every column the last layer wrote, likely 100+ columns, when a given
model variant needs at most 274 of those — actually most models need a
STRICT SUBSET already close to the full column count, so the realistic
saving here is small, likely single-digit seconds, not a major lever). Not
implemented in this task given the modest expected return relative to
verification cost.

## 5. Implemented: early presence guard

**File**: `apps/finish-position-predict-container/src/pipeline_runner.py`,
`build_upcoming_feature_rows`.

```python
race_keys = _query_upcoming_race_keys(
    database_url, target_date, days_ahead, category, target_race
)
if target_race is not None and not race_keys:
    print(
        f"[pipeline] presence-guard: target_race={target_race} category={category} "
        "has zero upcoming rows in source catalog -> skipping feature build",
        file=sys.stderr,
    )
    return {}
realtime_odds_path = fetch_realtime_odds_parquet(category, target_date, WORK_DIR, race_keys)
venue_weather_dir = fetch_venue_weather_dir(target_date, WORK_DIR)
built = build_pipeline(...)
```

**Why this is safe, stated precisely**: `_query_upcoming_race_keys` already
ran, unconditionally, before every per-race build call, on every call to
this function — it was just used only to scope the realtime-odds fetch.
It queries the exact same `jvd_se`/`nvd_se` source tables, with the same
target-date-window + target-race + "unsettled" (`kakutei_chakujun` blank)
filter, that the DuckDB base build itself applies. If it returns zero rows
for the one race requested, the base build is guaranteed to also find zero
target rows for that race — which already makes this function return `{}`
via the pre-existing `if not built: return {}` path a few lines below,
just after paying the full build cost first. **The change moves an
already-inevitable outcome earlier; it does not add a new outcome.**
Deliberately scoped to `target_race is not None` only — a whole-day/
whole-category call is completely unaffected, even in the (untested-in-
practice) case where the race-keys query happens to return empty for
other reasons.

**Verification, not just argument**: 3 new tests in
`tests/test_pipeline_runner.py`:

1. `test_build_upcoming_feature_rows_skips_pipeline_when_target_race_has_no_upcoming_rows`
   — `target_race` set + empty race keys → `build_pipeline` is never
   called (asserted via a spy, not just the return value), neither is the
   realtime-odds or venue-weather fetch, and the presence-guard log line
   is emitted.
2. `test_build_upcoming_feature_rows_runs_pipeline_when_target_race_has_upcoming_rows`
   — `target_race` set + non-empty race keys → `build_pipeline` runs
   exactly as before (unchanged path).
3. `test_build_upcoming_feature_rows_runs_pipeline_when_no_target_race_even_if_keys_empty`
   — `target_race=None` (whole-day call) + empty race keys → `build_pipeline`
   still runs (guard correctly scoped, does not fire outside its intended
   case).

**Checks run** (container package, from `apps/finish-position-predict-container`):

- `uv run pytest -q`: **1300 passed, 1 skipped**, coverage **99.81%**
  (`predict_lib`-scoped gate, unchanged from before this change —
  `pipeline_runner.py` itself sits outside the `--cov=predict_lib` measured
  surface per this package's own `pyproject.toml` comment, same as before;
  the 3 new tests exist regardless, following this file's own established
  testing convention for exactly this kind of I/O-glue logic).
- `uv run ruff check` / `uv run ruff format --check`: clean, 0 issues (one
  round of 3 line-length fixes applied before the final clean run).
- `uv run basedpyright`: 0 errors, 0 warnings, 0 notes.

**Not deployed** — per instruction, deployment is a separate,
team-lead-owned decision. No production-facing config, model routing, or
deploy manifest was touched.

## 6. Summary

| Candidate                  | Accuracy risk                      | Status                   | Estimated savings                                                                                 |
| -------------------------- | ---------------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------- |
| (a) Early presence guard   | Zero (proven, tested)              | **Implemented**          | Full per-race cost (~8.4min typical / up to ~27.5min worst-case) per doomed-build request avoided |
| (b) Layer parallelization  | Plausibly zero, not fully verified | Proposed                 | ~30-50% of DAY_CHAIN wall time (speculative)                                                      |
| (c) DuckDB config tuning   | Zero if resource-only              | Proposed                 | Modest for top sinks; helps h2h OOM margin                                                        |
| (d) Per-day result sharing | N/A — already exists               | Owned by investigation ② | N/A                                                                                               |
| (e) Projection pushdown    | Zero if done at final read only    | Proposed                 | Likely single-digit seconds — smaller win than expected                                           |

The one implemented change is deliberately the smallest-blast-radius,
most-rigorously-verified item on this list — consistent with "accuracy
must be provably unchanged" being the binding constraint on this whole
task, not merely a preference.
