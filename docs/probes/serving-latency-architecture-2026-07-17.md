# Serving latency architecture — performance investigation #2 (2026-07-17)

- **Date**: 2026-07-17
- **Category**: `finish-position-predict-container` / `finish-position-cron` serving path
- **Constraint**: prediction accuracy must remain unchanged. This is evaluation
  and procedure documentation only — **no code was changed, no flags were
  touched** (`DAY_BASE_SPLIT_ENABLED` in particular was not read, written, or
  flipped by this investigation).
- **Trigger**: today's own focused-full-race dispatch observed R01 holding the
  per-category slot while R02/R03 got `"busy"` responses. That plus the
  dormant `DAY_BASE_SPLIT_ENABLED` secret prompted a request for a 3-part
  speed evaluation (day-base split, slot serialization, model residency).

## TL;DR — 3-tier recommendation

| Tier                                     | Item                                                                                        | Verdict                                                                                                                                                                                                                                 |
| ---------------------------------------- | ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Safe to add before tomorrow morning**  | —                                                                                           | **Empty.** All three investigated items resolve to either "delivers no benefit as currently built" or "needs a real design change with correctness implications" — none is a same-night patch. See per-item reasoning below.            |
| **Next cycle, with proper verification** | Category slot parallelism, via **DO-level sharding** (not slot-count bump)                  | Real throughput win, real headroom (7 of 10 `max_instances` unused today), but requires a `buildPredictDoName` routing change + call-site updates + a light Neon connection-count check — a design+test task, not a flag flip.          |
|                                          | Day-base split, **after** a prerequisite fix to `ensure_day_base`'s catalog-source handling | Currently delivers **zero** production benefit (see §1a) regardless of the flag. Worth revisiting only once someone redesigns the catalog-source cache-trust boundary; running the existing parity test alone will not unlock anything. |
| **Should not add**                       | Flipping `DAY_BASE_SPLIT_ENABLED` today, as-is                                              | Structurally neutralized for production (§1a) — pure downside (an additional, under-tested code path) for zero upside.                                                                                                                  |
|                                          | Raising the focused-full slot count without the DO-sharding redesign                        | Confirmed correctness hazard, not just a resource question — concurrent builds for different races corrupt each other's shared category-scoped work directories (§2).                                                                   |
|                                          | Model booster caching / residency                                                           | Measured directly (§3): ~9 ms per reload once the process is warm. Not worth the implementation and test-coverage cost for sub-second/day savings.                                                                                      |

---

## 1. Day-base split re-enablement evaluation

### Background — what the split is

`apps/finish-position-predict-container/src/pipeline_runner.py` partitions the
17-script `LAYER_CHAIN` into `DAY_CHAIN` (day-stable, meant to be built once
per category+day) and `RACE_CHAIN` (race-fresh, built per race against the
cached day-base). `build_day_base()` (`pipeline_runner.py:797`) runs the
DuckDB base build + `DAY_CHAIN`; `build_pipeline_from_day_base()`
(`pipeline_runner.py:1129`) runs only `RACE_CHAIN` against that cached output.
`build_upcoming_feature_rows_split()` (`pipeline_runner.py:1240`) is the
per-race entry point, gated by `is_day_base_split_enabled(category)`
(`predict_lib/pipeline_args.py:642`), itself gated by the
`DAY_BASE_SPLIT_ENABLED` Worker secret (comma-separated category allowlist,
`apps/finish-position-cron/src/types.ts:73`, forwarded into the container env
by `container-class.ts:85`). It is committed but unset — dormant, exactly as
described in the task brief.

### 1a. Estimated time savings — **currently ~zero in production**, not measured-positive

This is the headline finding, and it required tracing `ensure_day_base()`
(`pipeline_runner.py:1006`) against commit `e6111ca6` ("fix(prediction):
reject processed feature caches", 2026-07-15) — the same commit that
originally wired up `DAY_BASE_SPLIT_ENABLED`.

`ensure_day_base()`'s very first check, added by that commit:

```python
if is_catalog_source_url(database_url):
    return None
```

`is_catalog_source_url()` (`predict_lib/conn_url.py:43`) matches
`r2-catalog://` URLs. Production's `SOURCE_DATABASE_URL` is
`r2-catalog://pc-keiba` (`apps/finish-position-cron/wrangler.jsonc` vars) — so
in production, `ensure_day_base()` **unconditionally returns `None`**, before
even checking the local-disk fast path (step 2 in its own docstring: "the
common case for the 2nd+ race of the day served by the same long-lived
container process") or the R2 fast path (step 3, the `/prewarm-day-base`
upload target). Both fast paths exist specifically to give the 2nd, 3rd, ...
Nth race of the day a cheap day-base reuse — and both are skipped entirely
for the catalog source.

`build_upcoming_feature_rows_split()`'s own resolution order
(`pipeline_runner.py:1263-1271`) is: call `ensure_day_base()` first; if `None`,
call `build_day_base()` synchronously instead (full DAY_CHAIN build, inline,
for this one race). For a catalog source that `None` fires on **every single
call** — meaning every focused-full-race request rebuilds the day-base from
scratch, with no cross-race reuse ever occurring. The split still executes
(DAY_CHAIN via `build_day_base()`, then RACE_CHAIN via
`build_pipeline_from_day_base()`), so it isn't broken, but the total script
work done is the same set of scripts the old `LAYER_CHAIN` path already runs
— just split into two function calls instead of one — plus extra overhead:
a fresh `shutil.rmtree`/`mkdir` of the day-base dir per race
(`pipeline_runner.py:866-867`) and a `day_base_covers_entry_list()` check
(an extra DB round-trip) that the old path never paid.

**Net**: flipping `DAY_BASE_SPLIT_ENABLED=jra` on today would not measurably
speed up any focused-full-race request in production — the one case this
whole feature optimizes for (2nd+ race of the day reusing race 1's day-base)
cannot occur while the catalog-source trust boundary short-circuits
`ensure_day_base()`. This is consistent with the fact that
`/prewarm-day-base` (the endpoint meant to pre-populate the R2 day-base
fast path ahead of race hours) is also structurally a no-op for JRA today —
its upload would never be read back for a catalog source either.

This also explains why nobody has noticed this dormant flag doing nothing:
it was never turned on. The finding here is that turning it on **would still
do nothing** for production, not that it's risky to turn on.

### 1b. Accuracy-invariance argument — sound, and already written down

`build_day_base()`'s own docstring (`pipeline_runner.py:811-852`) makes an
explicit, three-part argument for why freezing 7 race-fresh-in-principle
columns (`tansho_odds`, `tansho_ninkijun`, `popularity_score`, `odds_score`,
`current_bataiju`, `target_zogen_sa`, `babajotai_code_shiba`/`_dirt`) at
day-base build time does not stale the served vector:

1. **Verified via the actual SQL** (not assumed) that no `RACE_CHAIN` script
   re-fetches these columns independently — they're read from the base
   build's own output by every downstream layer (cites
   `add-similar-race-features.py`'s `current_baba_condition` as the concrete
   example).
2. The already-deployed `mode=rescore` path
   (`predict_lib.rescore.apply_fresh_snapshots`) is the actual freshness
   layer for late-binding odds/weight near post time, and runs independently
   of this split either way.
3. For the mandatory parity test's chosen inputs — a **past, settled** date —
   these columns are fixed in `jvd_se`/`nvd_se` regardless of when either
   pipeline variant runs, so OLD and NEW necessarily agree for historical
   replay.

This is a genuine argument, not a hand-wave, and I did not find a hole in it
during this review. The `JRA_JOCKEY_PEDIGREE_CELL_SCRIPT` cell-routing script
is explicitly kept in `RACE_CHAIN` forever (never promoted to `DAY_CHAIN`)
specifically because its value is same-day-cumulative and would be wrong if
frozen at day-base time — the split's designer already reasoned about this
class of hazard.

**Interaction with e6111ca6's "processed cache rejection"**: no tension.
e6111ca6 does not challenge the accuracy argument above — the `DAY_CHAIN` vs
`RACE_CHAIN` partition is still correct wherever the split actually runs. What
e6111ca6 does is separately veto _cross-request_ reuse of that `DAY_CHAIN`
output for catalog sources (§1a) — a throughput decision, not an accuracy
one. Given the same 2026-07-15 incident context (this repo's memory: cached/
processed feature data being trusted over a fresh raw-Catalog rebuild was
the root cause of a distinct stale-serving defect that day), the blanket
`is_catalog_source_url → return None` was written intentionally broad —
"never trust a pre-built cache for the live source" — and the day-base split
was an incidental casualty of that same hardening, not a target of it.

### 1c. Verification procedure — exists, unrun, and doesn't cover the production branch

`apps/finish-position-predict-container/tests/test_day_base_parity.py`
already is the exact OLD-vs-NEW equivalence test item 1(c) asked for: it runs
`build_upcoming_feature_rows` (full `LAYER_CHAIN`) and
`build_day_base()`+`build_pipeline_from_day_base()` (split) against the same
category/date/window and asserts row-for-row equality via
`_assert_frames_match` (exact match for non-float columns,
`np.isclose(rtol=1e-9, equal_nan=True)` for floats). It is gated behind
`RUN_DAY_BASE_PARITY=1` and requires `DATABASE_URL`,
`DAY_BASE_PARITY_CATEGORY`, `DAY_BASE_PARITY_TARGET_DATE`,
`DAY_BASE_PARITY_DAYS_AHEAD`. `git log --all -S "RUN_DAY_BASE_PARITY"` shows
only the test's original creation commit touching that string — **it has
never been executed since it was written.**

More importantly, the test's own docstring documents
`DATABASE_URL="postgresql://user:pass@host/db"` as the required input — a
plain Postgres/Neon connection string. `is_catalog_source_url()` only matches
`r2-catalog://` prefixes, so this test's `database_url` never satisfies that
predicate, meaning **the test cannot and does not exercise
`ensure_day_base()`'s catalog-source short-circuit** — the exact branch that
governs every real production call (§1a). Running this test today would
validate the DAY_CHAIN/RACE_CHAIN script partition's row-level correctness in
general (genuinely useful, and cheap — it's a feature-build comparison, not a
training run), but would say nothing about whether the split helps or hurts
in production, because production's dominant code path is a branch this test
structurally cannot reach.

**Recommended verification procedure, in order**:

1. Run the existing test as-is (Postgres source, a past settled date, e.g. a
   day already used in this campaign's other replay work) to confirm the
   DAY_CHAIN/RACE_CHAIN partition itself is still byte-parity-correct. Cheap,
   safe, no production contact — but treat a pass as necessary, not
   sufficient.
2. Before considering enabling the flag for the catalog-source category,
   someone needs to design and add a **second** parity/behavior test that
   actually drives the `r2-catalog://` branch of `ensure_day_base()` — at
   minimum a unit test asserting `ensure_day_base()` returns `None` for a
   catalog URL (documenting the current no-reuse behavior as intentional,
   not a latent bug) and, separately, a test proving that IF that
   short-circuit is ever narrowed (e.g. to a watermark-validated reuse
   window) the reused day-base still matches a fresh rebuild. Neither of
   these exists today.
3. Only after both of the above would enabling the flag be evidence-backed —
   and even then, per §1a, it would need the `ensure_day_base()` catalog
   short-circuit itself redesigned first, or the flag would still measure a
   no-op.

### 1d. Rollout / rollback procedure

Confirmed config-only, no image rebuild: `DAY_BASE_SPLIT_ENABLED` is a Worker
secret (`wrangler secret put DAY_BASE_SPLIT_ENABLED`, per-category
comma-separated allowlist), forwarded into the container's OS environment at
container start (`container-class.ts:85`), read fresh on every call inside
the container process (`pipeline_args.py:650`, deliberately not cached at
import time so a rollout can add categories without a redeploy). One real
nuance: the container process itself only picks up a NEW env value at its
next start (Cloudflare Containers fix env at `container.start()`), so an
already-warm container keeps running with its old value until it naturally
idles out and restarts — in practice within one self-heal/coordinator cron
cycle (≤15 min), not instant, but bounded and self-resolving. Rollback is the
same mechanism in reverse: unset/empty the secret. No code path deletion is
needed either way since `build_upcoming_feature_rows_split()`'s contract is
fail-open to the old full path on any `None`/exception
(`pipeline_runner.py:1250-1256`) — this was already true before today's
investigation and remains the safety net regardless of what happens with the
flag.

---

## 2. Category slot serialization

### Why it's serialized — confirmed correctness constraint, not an arbitrary throttle

`_claim_focused_full_slot()` / `_release_focused_full_slot()`
(`apps/finish-position-predict-container/src/predict_upcoming.py:1077,1105`)
implement a single per-process slot for focused-full pipelines. The
docstring states the reason plainly, and it checks out against
`pipeline_runner.py`: the DuckDB base build + layer chain writes to
**category-scoped**, not race-scoped, work directories —
`WORK_DIR/feat-{category}-base`, `feat-{category}-layer-N`,
`feat-{category}-v7-final` — and one container instance serves exactly one
category (confirmed below), so two concurrent focused-full builds for
_different races of the same category_ would read/write/rename the same
paths and corrupt each other's intermediate files. This is a filesystem
collision hazard, i.e. a correctness bug waiting to happen, not a CPU/memory
resource guard.

There is a second, broader lock backing this up:
`_PIPELINE_EXEC_LOCK` (`predict_lib/serve.py:760`) serializes **every**
`PredictCategoryFn` execution across all call paths (full, rescore, and the
focused-full detached thread) for two reasons: the same WORK_DIR collision
as above, plus mutation of the process-wide `PREDICT_DEBUG_LOGS` environment
variable during the call (`serve.py:773-775` — two interleaved calls would
stomp each other's debug-logging setting). `_FOCUSED_FULL_LOCK` is the fast
"busy"-response bookkeeping layer in front of this (rejects a redundant
request before it ever reaches the lock); `_PIPELINE_EXEC_LOCK` is the actual
mutex that would otherwise let a rescore body run concurrently with a
detached focused-full pipeline. Both exist because `ThreadingHTTPServer`
(not the old single-threaded `HTTPServer`) is what accepts connections now
(`predict_upcoming.py:2141-2153` docstring: the single-threaded server used
to let one wedged `/predict` handler starve `/ping` health checks behind it,
which is why it was replaced — but that same concurrency now needs its own
serialization for the pipeline body).

**Verdict: simply raising the slot count to 2-3 inside the current
architecture is not safe.** It would let two `score_races()`/DuckDB-pipeline
executions collide on the same category-scoped directories in the same
container filesystem — a data-corruption bug, not a throughput knob.

### The actual headroom, and how to use it safely: DO-level sharding, not in-process slots

`apps/finish-position-cron/wrangler.jsonc` sets
`"instance_type": "standard-4"`, `"max_instances": 10` for the
`FinishPositionPredictContainer` container. `buildPredictDoName()`
(`apps/finish-position-cron/src/queue-consumer.ts:166`) is what actually
determines how many _separate_ container processes exist:

```ts
const buildPredictDoName = ({ category }: PredictDoNameParams): string => {
  return `${PREDICT_DO_NAME_PREFIX}${category}`;
};
```

It's keyed by `category` **only** — every request for `jra` (regardless of
race) resolves to the exact same Durable Object name, hence the exact same
warm container process. With 3 categories (jra/nar/ban-ei), that's 3
concurrent Container DOs in steady state today — matching the queue
consumer's `max_concurrency: 3` comment (`wrangler.jsonc`, "well under
`max_instances: 10` above") — so 7 of the 10 configured instance slots are
genuinely idle capacity, exactly as observed. But that headroom is reachable
only by running _more DO instances_, not by adding slots inside the one DO
each category already has.

Sharding `buildPredictDoName` (e.g.
`${PREDICT_DO_NAME_PREFIX}${category}-${hash(raceBango) % N}`) so a
category's races spread across N DOs would use exactly this headroom, and —
important simplification — it sidesteps the WORK_DIR collision entirely
without touching a single line of the Python container: each DO is a
**separate container instance with its own filesystem**, so
`feat-{category}-base` in shard 0's container and the identically-named
path in shard 1's container are physically different directories. Each
shard keeps its own process-local `_FOCUSED_FULL_LOCK` /
`_PIPELINE_EXEC_LOCK`, so within a shard the existing single-slot
correctness guard is untouched and still necessary; across shards, true
parallelism is safe by construction.

**Neon write contention**: checked directly — predictions are written via
`ON CONFLICT DO UPDATE` UPSERT (`predict_lib/upsert_sql.py`), keyed
per-row (race/umaban/model_version-scoped, not a table-wide lock). Different
races scored by different shards write disjoint keys, so there is no
row-level lock contention from parallelizing across races — Postgres MVCC
handles concurrent UPSERTs to different keys natively. The only real cost is
connection count: `_flush_scored()` opens "a fresh Neon connection" per
category-run (`predict_upcoming.py:914` docstring) — going from 3 total
warm connections (one per category DO) to, say, 5 (JRA sharded ×3 + NAR + Ban-ei)
is a small absolute increase, well within normal Postgres/Neon connection
limits, but worth a one-line check against the current Neon compute's
`max_connections` before shipping, since this repo's memory flags Neon cost
as an always-on concern.

**Billing**: total CPU/memory-seconds for the actual prediction compute
should be roughly neutral — the same total work spread over more
concurrent instances finishes in less wall-clock time, not more total
instance-time. The two real added costs are (a) each shard reloads its own
category model set independently rather than sharing one warm process's
cache — see §3, this is measured negligible — and (b) N shards each have
their own idle/`sleepAfter` tail before reaping, so base idle-time billing
scales roughly with shard count, bounded by `max_instances: 10`.

**This is a real design task**, not a same-night change: it needs the
`buildPredictDoName` shard key design, updates to every other call site that
currently assumes one DO name per category (self-heal's `claimFocusedFullRace`
DO-based claim, the coordinator, the admin stop endpoint), a rollout plan,
and the light Neon connection-count check above. Recommended for next cycle,
gated on whether the R01-blocks-R02/R03 latency is judged worth the added
architectural surface — it is a genuine fix for the observed symptom, just
not a small one.

---

## 3. Model residency / load time

### Measured, not assumed

The container's model files are baked into the image at build time
(`COPY apps/finish-position-predict-container/models /models`, Dockerfile) —
loading is a **local disk read**, never a network fetch. `_load_booster()`
(`predict_upcoming.py:1001`) / `_load_booster_by_arch()` (`:985`) have no
`@lru_cache` or module-level cache; `score_races()`
(`predict_upcoming.py:601`) calls `_load_booster()` for the category fallback
plus `_load_booster_by_arch()` again for every non-default cell-routing
variant (currently 2 for JRA: `jockey-pedigree269`, `prior-corner274`), fresh,
on every call — confirmed via `grep` finding zero cache-decorator patterns
anywhere in the file.

Timed directly against the real production JRA model files
(`load_catboost_booster`, no mocking):

| Load                                                                          | Time                                                          |
| ----------------------------------------------------------------------------- | ------------------------------------------------------------- |
| Champion (`jra-cb-v9-sim-2013-clean`), 1st load in a fresh process            | 706.8 ms (and 212.7 ms on a repeat run — first-load variance) |
| Same champion file, 2nd load, same process                                    | 8.3 ms                                                        |
| Same champion file, 3rd load, same process                                    | 8.3 ms                                                        |
| `jockey-pedigree269` variant, loaded right after champion in the same process | 9.4 ms                                                        |
| `prior-corner274` variant, loaded right after champion in the same process    | 9.3 ms                                                        |

All three files are ~4.0-4.2 MB. The pattern (one large cost on the very
first load, ~9 ms on every subsequent load regardless of which file) is the
signature of a one-time CatBoost native-library initialization cost, not a
per-file cost — confirmed by loading the _same_ file three times in one
process and seeing the same 700ms→8ms→8ms drop.

### Conclusion — no caching needed

The container runs `serve_forever()` via `ThreadingHTTPServer`
(`predict_upcoming.py:2141-2168`) and stays warm across many requests — this
was confirmed by reading the actual server setup, not assumed. Given that,
the ~700 ms tax is paid once per container process lifetime (on whichever
request happens to be first after a cold start / restart), and every
subsequent focused-full-race request pays roughly 3 × 9 ms ≈ 27 ms in
booster-reload overhead — against a pipeline this codebase's own comments
describe as taking "~15-27 min" end to end. That's on the order of
0.003% of one request's total latency, and the aggregate across a full
day's races is comfortably under one second. There is no measurable
throughput problem here to solve. A residency/cache layer (keeping loaded
boosters in a module-global keyed by category, safe to do given
`_PIPELINE_EXEC_LOCK` already guarantees only one pipeline body runs at a
time per process) would be low-risk to build, but the coverage rules this
repo enforces (95%+ on this package) mean it still costs real engineering
and review time to add and test correctly — not worth spending against a
sub-second/day payoff. **Should not add**, on cost/benefit grounds, not
safety grounds.

---

## Summary of code read (no changes made)

- `apps/finish-position-predict-container/src/pipeline_runner.py` — `build_day_base`, `ensure_day_base`, `build_pipeline_from_day_base`, `build_upcoming_feature_rows_split`, `_day_base_dir`, `DAY_CHAIN`/`RACE_CHAIN` split header.
- `apps/finish-position-predict-container/src/predict_lib/pipeline_args.py` — `is_day_base_split_enabled`.
- `apps/finish-position-predict-container/src/predict_lib/conn_url.py` — `is_catalog_source_url`, `resolve_source_url`.
- `apps/finish-position-predict-container/src/predict_upcoming.py` — `score_races`, `_load_booster`/`_load_booster_by_arch`, `_claim_focused_full_slot`/`_release_focused_full_slot`, `_make_predict_fn`, `_PredictHandler.do_GET`, `serve_http`, `main`.
- `apps/finish-position-predict-container/src/predict_lib/serve.py` — `_PIPELINE_EXEC_LOCK`, `_run_predict_fn`, `_focused_full_preflight`, `iter_predict_chunks`.
- `apps/finish-position-predict-container/tests/test_day_base_parity.py` — full read, confirmed never executed (`git log --all -S "RUN_DAY_BASE_PARITY"`), confirmed Postgres-only `DATABASE_URL` usage.
- `apps/finish-position-cron/wrangler.jsonc`, `src/queue-consumer.ts` (`buildPredictDoName`), `src/types.ts`, `src/container-class.ts` — container instance config, DO naming, `DAY_BASE_SPLIT_ENABLED` wiring.
- `apps/finish-position-predict-container/src/predict_lib/upsert_sql.py` — UPSERT key shape, for the Neon-contention check in §2.
- `git show e6111ca6` — full diff read for `pipeline_runner.py` / `predict_upcoming.py`, the source of the catalog-source trust boundary that drives §1a's finding.
- Booster-load timing (§3): measured directly with the real production model files via `catboost_adapter.load_catboost_booster`, no mocking, no writes, no network calls — read-only local execution.
