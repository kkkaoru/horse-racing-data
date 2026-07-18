# JRA `/v1/running-style-features` R2 SQL "expression too deep" — fix option design (2026-07-18)

- **Status**: design-only, not implemented today. Root-cause incident write-up lives in
  `jra-serving-audit-jun-jul-2026-07-17.md` §2 Defect G (feature_guard blind spot) — this
  doc is specifically about the upstream R2 SQL failure that causes RS to never generate
  for JRA in the first place, discovered while triaging Defect G on 2026-07-18 (first JRA
  raceday since the 07-15 raw-iceberg-v1 migration).
- **Decision (team-lead, ~10:1x JST)**: today is a write-off for JRA RS/FP quality. No code
  change to `apps/pc-keiba-r2-catalog` today. This doc exists so the 17:00+ JST window (or
  later, if the fix needs a full WF accuracy validation cycle) has a menu of options with
  risk/feasibility already scoped, instead of starting from zero.

## 1. What's actually broken (recap, verified 2026-07-18 ~10:05-10:15 JST)

- `pc-keiba-r2-catalog`'s `/v1/running-style-features?date=20260718&source=jra&keibajoCode=02&raceBango=01`
  returns **HTTP 502** in production (`{"error":"r2_sql_unavailable"}`, ~13.6s). NAR's
  equivalent works (per team-lead/morning-ops's D1 `running_style_inference_state` reading:
  JRA failed=6/pending=29-23/processing=1, NAR completed=34/34).
- The 502 is `worker.ts`'s catch-all: `handleRunningStyleFeatures` → `executeR2Sql` throws on
  ANY non-2xx R2 SQL response, and the handler collapses every such throw into a generic 502,
  discarding the real R2 SQL status/message.
- Querying R2 SQL **directly** (bypassing the Worker, using `CLOUDFLARE_DEBUG_TOKEN` as a
  drop-in `R2_SQL_TOKEN`, confirmed to work) with the exact production query
  (`buildRunningStyleFeaturesQuery(config, {date:"20260718", source:"jra", keibajoCode:"02",
raceBango:"01"})`, 60KB / 44 chained CTEs) returns, deterministically and reproducibly:
  ```
  HTTP 400 (9.8s): {"errors":[{"code":40018,"message":"query expression too deep: nesting
  depth exceeds the protocol's limit; rewrite long chains of AND/OR operators using IN/NOT IN
  lists or shorter predicate groups"}]}
  ```
- **This is NOT a missing/empty-data problem.** Verified directly against R2 SQL: `jvd_ra`
  has exactly 36 rows for `kaisai_nen='2026' AND kaisai_tsukihi='0718'` (matches expected JRA
  race count), catalog data extends through `0719`, 10-year `jvd_ra` count = 37,324 rows
  (1.4s), 10-year `jvd_se` count = 775,126 rows (1.4s). Plain scans are fast and correct on
  both tables.
- **This is NOT (obviously) a timeout either.** `EXPLAIN FORMAT JSON` of the exact same
  60KB query **succeeds** (200, 10.8s) and returns a valid, deeply-nested physical plan
  (`DistributedExec` → `SortPreservingMergeExec` → `NetworkCoalesceExec` → many more nested
  stages, `output_partitions: 12`). Only **actual execution** of that same query hits the 400.
  This strongly suggests the "too deep" limit is about the **distributed execution plan's**
  depth/wire-serialization (which EXPLAIN's JSON dump apparently isn't subject to, or has a
  different ceiling for), not a literal long chain of `AND`/`OR` tokens sitting in the SQL
  text — I searched `running-style-sql.ts` and `running-style-feature-ctes.ts` for
  dynamically-generated predicate chains (`.map()`/`.join()` patterns building `OR`/`IN`
  lists) and found none; every `OR` usage in the file is a small, static, hand-written
  handful of clauses (max ~7, in a peer-tiebreak block). The query chains **44 CTEs**, many
  referenced by multiple downstream CTEs (`target`/`rec` alone feed a dozen+ later CTEs) —
  if R2 SQL's planner inlines CTE definitions at each reference point (no true CTE
  materialization), physical plan depth could scale combinatorially with reuse and with the
  partition/file count the query has to touch, which is exactly where the identical
  SQL-text-for-JRA-and-NAR asymmetry comes from: JRA's absolute historical data volume is
  much larger, needing more distributed stages/partitions to execute the same logical query,
  which is what plausibly pushes past the depth ceiling that NAR's smaller footprint doesn't
  reach for a typical race.
- **A same-source comparison point (inconclusive, not fully trusted)**: the equivalent NAR
  query for `keibajoCode=54, raceBango=01` (confirmed via D1 `realtime_race_sources` that
  NAR venue 54 genuinely races today, so this wasn't a nonexistent-data artifact) **timed
  out** after 30s with zero bytes, rather than either succeeding or hitting the same 400.
  This muddies "JRA-only" as a clean characterization — it may be that some NAR races are
  also marginal/slow, just not enough to fail production's more patient retry/timeout
  budget. Do not over-index on this single data point; it wasn't rabbit-holed further per
  team-lead's explicit steer.

## 2. Option (a): rewrite deep AND/OR chains → IN/NOT IN lists (per R2 SQL's own error hint)

- **What it would be**: find the specific predicate structure R2 SQL's error message is
  pointing at and mechanically rewrite it, preserving the exact same row set (pure syntactic
  transform, zero semantic/accuracy change).
- **Investigated, not found**: no dynamically-generated long `AND`/`OR` chain exists in the
  query-generation source. The static `OR` blocks present (peer-dominance tiebreak, ~7
  clauses) are far too short to plausibly be _the_ thing tripping a "nesting depth" limit on
  their own, and are identical for JRA and NAR (so alone they can't explain the asymmetry).
- **Risk to the diagnosis itself**: given `EXPLAIN` succeeds and only real execution fails,
  this option may be targeting the wrong layer entirely — the depth in question could be a
  property of the _distributed physical plan_ (CTE reuse × partition/file count), not the
  _logical SQL text_, in which case no text-level rewrite fixes it regardless of how
  carefully done.
- **Feasibility**: **low confidence** without R2 SQL platform-side clarification on exactly
  what "protocol's limit" measures (ideally: ask Cloudflare support/docs what
  error code 40018 specifically counts, and whether it's query-text-based or plan-based).
  Accuracy risk: **none**, if a genuine text-level culprit is later found and the rewrite is
  provably row-set-identical (verified against R2 SQL directly + golden-output diff, as
  team-lead's guardrails already specify for whichever option ships).

## 3. Option (b): shrink the 10-year lookback window

- **Current behavior**: `historyStart(date)` (`running-style-sql.ts:76-77`) computes
  `date.year - 10`; `historyPredicates` scans `kaisai_nen` in `[year-10, year]` with **no
  venue restriction** for the JRA/NAR-combined scope (`venuePredicate` returns `1 = 1` for
  the "all" scope, `keibajo_code <> '83'` for NAR-excluding-Ban-ei — i.e. NAR's predicate is
  _more_ restrictive than JRA's, not less, which further argues against a simple
  static-text-complexity explanation for the asymmetry).
- **Feature-level lookback need is a genuine mix, not uniformly "needs 10 years"** (read
  directly from `running-style-feature-ctes.ts` constants and CTE joins):
  - Most "recent form" signals only look at the horse's own last few starts:
    `RECENT_WINDOW_SIZE = 5` (speed index avg/best-5, kohan-3f avg-5, corner-pass avg-5,
    etc.), `JOCKEY_RECENT_DAYS = 60`, `TRACK_BIAS_WINDOW_DAYS = 5` (days),
    `CONSECUTIVE_RACE_WINDOW_DAYS = 30`. None of these need more than a few months of raw
    history to compute correctly for an _active_ horse/jockey — but the raw history table
    still has to be scanned back far enough to find that horse's/jockey's most recent N
    races, which for a horse that hasn't raced in a while (or a comeback horse) could
    legitimately require looking back further than a fixed short window if it's date-bounded
    rather than count-bounded (the current design is date-range-bounded, not
    count-bounded, so a fixed 3-year window still has this same recency-vs-window tension,
    just less of it than 10 years).
  - **Sire/damsire performance stats are the opposite case**: `sire_distance_monthly`,
    `sire_track_monthly`, `sire_running_style_monthly` and their damsire equivalents
    (`running-style-feature-ctes.ts:245-334`) join on `race_year_month < tm.stats_year_month`
    with **no lower bound at all** — these are designed to accumulate as much sample size as
    possible for statistical stability of a sire's per-distance/per-track/per-running-style
    win rates. `PEDIGREE_MIN_RACES = 5` is the _minimum_ sample floor already built in, which
    implies the feature design already assumes access to a large history; shrinking the
    window would reduce sample size for less-common sires specifically (the ones a shorter
    window would hurt: sires whose runners debut/place rarely, or who've been retired long
    enough that most of their runners' careers fall outside a shortened window).
- **Risk**: real, and NOT free — this is an accuracy-affecting change to feature
  construction, squarely in the territory this repo's own evaluation discipline governs
  (memory: cell-level, rank1-5, blind-holdout WF evaluation required before any accuracy-
  affecting change is accepted; no same-day flips without that cycle). A same-day shrink to
  "fix a 502" without WF validation would violate that discipline and could silently
  degrade sire/damsire feature quality in a way that's much harder to notice than an outage
  (an outage is loud; a quietly-worse sire feature is not).
- **Feasibility**: mechanically simple (change one constant, `historyStart`'s `- 10`), but
  **not same-day-safe** given the accuracy-validation requirement above. If R2 SQL's depth
  limit does scale with partition/file count touched (per §1's hypothesis), this option is
  also the most direct lever on that count — fewer years scanned, fewer distributed stages,
  most likely to actually fix the 400/502 if the hypothesis is right. Recommend: if pursued,
  run it as a scoped experiment (e.g. 5-year window) with the r2-sql-namespace query smoke-
  tested directly (bypassing the Worker, same technique used in this investigation) _and_ a
  proper WF re-evaluation of RS output quality (per-cell, rank1-5) before considering it for
  production, not a same-day emergency patch.

## 4. Option (c): Iceberg compaction / partition optimization (platform-side, zero logic change)

- **Idea**: if the physical file/manifest count behind `jvd_ra`/`jvd_se`'s Iceberg tables is
  large/fragmented (plausible: `sync_r2_catalog.py`'s "Full mode" rewrites data in 5-year
  chunks per its own README, and JRA's absolute row volume is much larger than NAR's), then
  compacting those files into fewer, larger ones could reduce the number of distributed
  stages the _same_ query needs to plan/execute, without touching the SQL or any feature
  logic at all.
- **Zero accuracy risk** — this is pure storage-layout housekeeping, not a query or feature
  change. This makes it the most attractive option from a "ship without an evaluation cycle"
  standpoint, _if_ it's actually the bottleneck.
- **Not currently supported by existing tooling**: `sync_r2_catalog.py` (checked its
  `scripts/README.md` and the script itself) has no `--compact`/maintenance mode — it only
  performs full-partition or year-scope rewrites triggered by a sync run, which happen to
  _incidentally_ rewrite/consolidate files for the years touched, but there's no standalone
  "just compact, don't otherwise change data" operation. Would need new tooling (e.g. an
  Iceberg `rewrite_data_files`-equivalent procedure via R2 Data Catalog's REST API/PyIceberg)
  to test this hypothesis cheaply.
- **Feasibility**: unverified whether this is even the actual bottleneck (the 44-CTE-reuse
  hypothesis in §1 is a _plausible_ explanation for query-plan depth scaling with data
  volume, but "fragmented small files" is a _different_, not-yet-confirmed contributing
  mechanism for the same symptom). Recommend as a **cheap diagnostic first step** for the
  17:00+ window: use R2 SQL's own tooling (or an Iceberg metadata query, e.g.
  `SELECT * FROM pc_keiba.jvd_ra.files` if R2 SQL exposes Iceberg metadata tables, or
  inspect the R2 bucket object listing under the table's data path) to actually count files/
  manifests for `jvd_ra`/`jvd_se`'s 2016-2026 partitions and compare JRA vs NAR fragmentation
  before assuming this is or isn't the lever — much cheaper to check than to implement a
  compaction pass blind.

## 5. Recommended path (not a decision — for whoever picks this up at 17:00+)

1. **First, cheaply falsify/confirm the two competing physical-cause hypotheses** before
   picking a fix: (a) CTE-reuse-driven plan-depth (§1/§3) vs (b) file/manifest fragmentation
   (§4) vs (c) genuinely just "10 years of JRA is too much data regardless of plan shape"
   (§3's lookback-need analysis suggests some features could tolerate a shorter window, but
   sire/damsire stats specifically would not without accuracy validation). A quick way to
   discriminate: bisect the CTE chain (progressively drop later CTEs from the query and see
   at what point execution stops 400-ing) — I did not have time to do this bisection today;
   it's a fast, zero-risk (read-only against R2 SQL) diagnostic that would tell you whether
   the depth blows up gradually across many CTEs (favors restructuring/materialization) or
   sharply at one specific CTE (favors a targeted fix at that CTE, likely the sire/damsire
   ones given their unbounded joins).
2. **Prefer §4 (compaction) or a CTE-restructuring form of §2 first** — both are zero-
   accuracy-risk if verified row-set-identical, and should be tried before touching the
   lookback window (§3), which has a genuine, real accuracy tradeoff that requires this
   repo's full WF evaluation discipline (not a same-day change) regardless of how well it
   might fix the immediate outage.
3. Whatever is chosen, team-lead's guardrails from this morning still apply and are the
   right bar: (a) rewritten query returns 200 + non-empty valid rows against R2 SQL directly
   for a real JRA race, (b) NAR still 200 (no regression), (c) full package test/lint/tsc
   green with new coverage asserting the generated SQL's shape/row-set. Do not skip these
   even under time pressure the second time around.

## 6. What's unaffected / doesn't need to wait for this fix

- Today's `COORDINATOR_ENABLED=0` rollback (commit `09780911`, deploy `abae300a`) is
  independent of this fix and should be re-enabled once RS is confirmed generating for JRA
  again — it does not need to wait for the R2 SQL query fix specifically, just for RS
  health to be restored (whichever mechanism restores it).
- Defect F (force bypass) and the race-sharded DO work are both already shipped and
  unrelated to this incident.
