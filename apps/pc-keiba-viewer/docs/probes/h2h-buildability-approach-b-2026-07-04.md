# H2H feature build — Approach B: lower-memory query rewrite (2026-07-04)

Status: **prototype validated, full-window success — proposal only, source unchanged**

Investigates whether `add-head-to-head-features.py`'s `stage_current_pair_aggregates()`
(the operation that OOMs on full-window NAR data at both 6GB and 12GB
`memory_limit`, "failed to pin block ... 11.1GiB/11.1GiB") can be reformulated
to use materially less peak memory at identical output. Prototype lives in
`tmp/candidate-h2h-approach-b/` (not wired into any pipeline; package source
was not modified).

## 1. Semantics of the original query

`src/scripts/finish-position-features/add-head-to-head-features.py` builds
h2h features in 5 stages (training mode, `--target-race` unset):

1. `race_history` — all `(source, race_date, race key, ketto_toroku_bango,
finish_position)` rows from `pg.race_entry_corner_features` with
   `race_date >= from_date`. Not scoped to the target years at all — it is
   rebuilt to the FULL from-date range on every invocation.
2. `pair_history` — self-join of `race_history` against itself on
   `(source, race key)` with `horse_b.ketto_toroku_bango >
horse_a.ketto_toroku_bango` (canonical `a < b` ordering, dedup). One row
   per historical (pair, race, finish_diff). **Unscoped** — covers every pair
   that ever shared a race across the entire `from_date` range, regardless of
   which races are the "target" ones.
3. `target_races` — distinct race keys from the input parquet (i.e. the races
   we need output rows for).
4. `current_field` / `current_pairs` — `race_history` restricted to
   `target_races`, then self-joined the same way as step 2, to get the field
   pairs for just the target races.
5. `stage_current_pair_aggregates` — `current_pairs LEFT JOIN pair_history ON
(source, horse_a, horse_b) AND ph.race_date < cp.current_date GROUP BY
ALL`. This is the reported OOM point.

The leakage guard (`race_date < current_date`, strictly prior) is an
**inequality**, which DuckDB cannot push into the hash-join's equality keys.
The join operator must therefore materialize every row where
`(source, horse_a, horse_b)` matches — regardless of date — and only then
apply the residual date filter, before `GROUP BY ALL` collapses it back down.
For pairs that recur across many seasons (NAR horses at small regional
circuits can face the same rivals dozens of times — measured max 79
recurrences in `current_pairs`, max 113 in `pair_history` over the full
2006–2026 NAR window), this is a combinatorial fan-out relative to the
`GROUP BY`'s final output size.

## 2. Candidate formulations considered

- **(a) Semi-join pre-filter pair_history to target-relevant pairs.**
  Rejected as the primary lever: in the training/regen case, `current_pairs`
  (scoped to target races) and `pair_history` (unscoped) are generated from
  the _same underlying race_history self-join_ — nearly every historical pair
  also recurs as a target-race pair, so filtering `pair_history` down to
  "pairs that appear in some target race" removes very little. A _cheaper_
  variant of this idea (bounding `pair_history`/`pair_events` by
  `race_date <= max(target_races.race_date)`) **is** included below — it's a
  free scalar predicate, not a join, and materially helps the per-year-scoped
  case (removes all strictly-future years' rows) though it is a no-op for the
  full-window case (target already spans the whole range).
- **(b) Canonical pair-key ordering + sort-merge instead of hash join.** The
  self-join already used `a < b` canonical ordering (`ketto_toroku_bango`
  comparison) — this part of the original design was already correct. The
  missing piece was replacing the _hash join_ between `current_pairs` and
  `pair_history` with something sort-based.
- **(c) Two-phase: aggregate the compact pair key first, then join to the
  field expansion.** The task's framing of this (a single lifetime `GROUP BY`
  per pair, joined afterward) does not preserve correctness here: the desired
  aggregate is not "career totals for the pair" but "totals as of just before
  _this specific_ current race" — different current races for the same
  recurring pair must see different (growing) subsets of prior history. A
  plain pre-aggregated `GROUP BY` cannot express that per-occurrence
  as-of cutoff.

**Adopted formulation — (b)+(c) combined via a WINDOW function**, which is
what actually satisfies the temporal semantics that (c) alone cannot:

Observation: because `pair_history` and `current_pairs` are both derived from
the identical `race_history` self-join (just scoped differently), every
target-race pair-occurrence already has a corresponding row in the
_unscoped_ self-join with its own `finish_diff`. So there is no need for two
separate self-joins (`pair_history` and `current_pairs`) plus a fan-out join
between them at all:

1. `pair_events` — **one** self-join of `race_history` (same shape/cost as
   the original `pair_history` step), but retaining the **full race key**
   (`kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`) instead of
   collapsing to just `race_date`, plus the cheap `race_date <=
max(target_races.race_date)` bound.
2. A single **window function** computes, at every `pair_events` row, the
   cumulative aggregate of all _strictly prior_ rows for that
   `(source, horse_a, horse_b)` key:
   ```sql
   count(finish_diff_a_minus_b) over w        -- enc_count
   sum(case when finish_diff_a_minus_b<0 ...) over w   -- a_wins
   sum(case when finish_diff_a_minus_b>0 ...) over w   -- b_wins
   avg(finish_diff_a_minus_b) over w          -- avg_diff_a_minus_b
   window w as (partition by source, horse_a, horse_b
                order by race_date
                rows between unbounded preceding and 1 preceding)
   ```
   DuckDB implements this via a **sort**, not a hash join — the operator's
   output row count equals its input row count exactly, independent of how
   many times any given pair recurs. No fan-out is possible by construction.
3. Filter down to `target_races` via a cheap semi-join (`target_races` is
   small — the join is a filter, not a fan-out producer).

This eliminates the separate `current_field`/`current_pairs` self-join
entirely (one fewer large intermediate) as well as the fan-out-prone hash
join, replacing both with one self-join (already required) + one sort.
Prototype: `tmp/candidate-h2h-approach-b/h2h_window.py`.

## 3. EXPLAIN findings (DuckDB 1.5.3)

Single-year (2006) scope, `EXPLAIN`/`EXPLAIN ANALYZE`:

- **Baseline**: `HASH_JOIN (Join Type: RIGHT)` on
  `(source, horse_a, horse_b, race_date < current_date)` directly above two
  `TABLE_SCAN`s of `pair_history`/`current_pairs`, feeding a `HASH_GROUP_BY`.
  At single-year target scope, DuckDB derives a **dynamic filter** from
  `current_pairs`' min/max (`horse_a`/`horse_b` string range, `race_date <=
'20061231'`) and pushes it into the `pair_history` scan — this is why the
  original formulation is _not_ pathological at small target scope (see
  benchmark below): only ~1.34M of `pair_history`'s 23.5M rows actually get
  scanned. At full-window target scope this pushdown loses essentially all
  its selectivity (the derived range covers nearly the whole table), which is
  the mechanism by which the same query degrades from fine to OOM as target
  scope widens.
- **Candidate**: single `WINDOW` operator (sort-based, the plan explicitly
  shows the `PARTITION BY ... ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING
AND 1 PRECEDING` frame) directly over `pair_events`, then one small
  `HASH_JOIN (Join Type: INNER)` against `target_races` (15,656 rows) as a
  cheap filter — no operator in the candidate plan has fan-out potential.

## 4. Benchmarks

Environment: DuckDB 1.5.3, `threads=4`, `from-date=20060101` (matches the
production wrapper `run_stage9_h2h_peryear.sh`), local Postgres at
`127.0.0.1:15432`. Peak = worker RSS sampled every 3–5s for the whole process
(PG attach + all 5 stages + final parquet write), matching the wrapper's own
measurement methodology (`s9-peryear-peak-mem.log`).

| Scope                          | Formulation                               | memory_limit | Peak RSS     | Wall time        | Outcome                                                                                                                    |
| ------------------------------ | ----------------------------------------- | ------------ | ------------ | ---------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Year 2006 only                 | Baseline (production wrapper, actual log) | 12GB         | 7,757 MB     | ~54s (batch avg) | success                                                                                                                    |
| Year 2006 only                 | Candidate (window)                        | 12GB         | **1,634 MB** | 3s               | success                                                                                                                    |
| Year 2006 only                 | Candidate (window)                        | 6GB          | **1,833 MB** | 10s              | success                                                                                                                    |
| **Full window (all 21 years)** | Baseline                                  | 6GB / 12GB   | —            | —                | **OOM** (reported: "failed to pin block ... 11.1GiB/11.1GiB" — not re-run here to avoid reproducing a known crash; see §5) |
| **Full window (all 21 years)** | Candidate (window)                        | 12GB         | **9,070 MB** | **~85s**         | **success**                                                                                                                |
| **Full window (all 21 years)** | Candidate (window)                        | 6GB          | **6,032 MB** | **25s**          | **success**                                                                                                                |

All four candidate runs above are real, measured executions of the full CLI
(`tmp/candidate-h2h-approach-b/h2h_window.py`, matching the production
wrapper's argument set exactly: `--from-date 20060101 --threads 4`), each
verified to produce the exact expected row count (153,617 for year 2006 alone;
2,782,381 for the full 21-year window) before moving to the next benchmark.
Peak RSS was sampled every 3–4s across the whole process lifetime (PG attach
through final parquet write), one run at a time (never concurrently), gated
on `memory_pressure -Q` free% before each launch (64–85% free at each
launch; lowest observed mid-run was 37%, well above the 15% floor).

Scale figures behind the full-window number (NAR, `from_date=20060101`,
21 target years 2006–2026): `race_history`=4.63M rows, `pair_events`
(=old `pair_history` shape)=23.5M rows, `current_pairs`(old formulation,
built only for comparison)=12.7M rows over 7.93M distinct pair keys (max
recurrence 79 in `current_pairs`, 113 in `pair_history`). A row-count fanout
estimate (`sum(current_count * history_count)` per pair, ignoring the date
predicate) comes to ~33.7M rows — only ~2.7x `current_pairs` alone — which by
itself doesn't obviously explain an 11GB pin failure; the more likely
compounding factor is that all six join/partition keys
(`source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
ketto_toroku_bango`) are `VARCHAR`, not integers, so the baseline's
`HASH_JOIN` build side carries full string-hashing/storage overhead across
both the ~23.5M-row build side and the fan-out output, on top of whatever
selectivity the dynamic filter pushdown loses at full-window target scope.

## 5. Equivalence check

Single-year (2006), aggregation stage only, executed both formulations for
real (not estimated) and diffed `current_pair_aggregates`:
`mismatched rows: 0`, `rows only in baseline: 0`, `rows only in candidate: 0`.

Full window (all 21 years, end-to-end including the final feature-append
join + parquet write), diffed against the production ground truth at
`tmp/candidate-leak-clean-retrain/nar-full-regen/s9-h2h/` (the wrapper's
real, already-verified 21-partition consolidated output):

- Row count per year: **21/21 years match exactly** (e.g. 2006: 153,617 vs
  153,617; 2026: 58,291 vs 58,291; total 2,782,381 vs 2,782,381).
- Full-row, full-column `EXCEPT` diff (all 200+ feature columns, not just the
  6 h2h\_\* ones): 189 rows differ out of 2,782,381 (99.9932% exact match).
- Root-caused the 189: **1-ULP floating point noise only.**
  `max(abs(h2h_avg_finish_diff_vs_field_gt - h2h_avg_finish_diff_vs_field_cand))
= 4.44e-16` (machine epsilon for `double`), from different floating-point
  summation order (hash-aggregate iteration order vs. window
  partition-sorted order — `AVG`/`SUM` over doubles is not associative).
  Every integer-valued column (`h2h_encounter_count`, `h2h_win_count_vs_field`,
  `h2h_loss_count_vs_field`, `h2h_unique_rivals_count`) matched **exactly** in
  all 189 rows, and `h2h_win_rate_vs_field`'s max delta was `0.0`. This is
  not a correctness regression — it's the same class of noise the original
  baseline formulation itself would produce differently across DuckDB
  versions or thread counts, several orders of magnitude below the feature's
  modeling-relevant precision.

## 6. Verdict

**The window-function reformulation eliminates the OOM at both memory limits
named in the task.** Full 21-year NAR window (the exact scope that currently
requires the per-year wrapper + 20x redundant `race_history`/`pair_history`
rebuild) completes in a single invocation at **9.07GB peak / ~85s under
12GB**, and **6.03GB peak / ~25s under 6GB** — succeeding at both limits the
baseline is reported to OOM at. Output is identical to the wrapper's ground
truth to within 1-ULP floating-point noise (§5). Single-year peak dropped
from 7.76GB (baseline, production log) to 1.63–1.83GB (candidate, 12GB/6GB)
— a ~4.2–4.8x reduction even at the scope where the baseline was already not
OOMing. Wall time also improved sharply: the per-year wrapper's 21 separate
invocations take on the order of 20+ minutes total (each rebuilding
`race_history`/`pair_history` from scratch); the candidate does the entire
21-year window in under 90 seconds in one shot.

**Recommendation: adopt.** This is a straightforward, low-risk, verified-
equivalent rewrite of two internal staging functions with no schema or CLI
change. It removes the need for the per-year wrapper entirely for NAR, and
directly de-risks the JRA-scale regen (JRA's `pair_history` is ~150M rows per
the script's own docstring estimate, ~6x NAR's 23.5M — the fan-out mechanism
this fixes only gets worse at that scale, so the win should be larger, not
smaller, for JRA). Recommend a maintainer review of
`tmp/candidate-h2h-approach-b/h2h_window.py` against the proposed diff below,
then porting it into `add-head-to-head-features.py` under normal review.

### Proposed change to the package script (proposal only — needs sign-off)

In `src/scripts/finish-position-features/add-head-to-head-features.py`,
replace `stage_pair_history()` + `stage_current_pair_aggregates()`
(lines ~125–246) with the `stage_pair_events()` +
`stage_current_pair_aggregates()` (window-based) pair from
`tmp/candidate-h2h-approach-b/h2h_window.py`. Concretely:

- `stage_pair_history` gains the full race-key columns (not just
  `race_date`) and the `where h1.race_date <= (select max(race_date) from
target_races)` bound — note this makes it depend on `target_races` now
  existing, so it must run _after_ `stage_target_races` (current call order
  in `main()` already has `stage_target_races` before `stage_pair_history`,
  so no reordering needed).
- `stage_current_pairs`/`current_field` (the old self-join used only to
  produce `current_pairs`) can be **deleted outright** — the window query
  reads directly from the renamed `pair_history`/`pair_events` table and
  semi-joins `target_races`.
- `stage_current_pair_aggregates` becomes the window-based version; its
  output schema (`source, kaisai_nen, kaisai_tsukihi, keibajo_code,
race_bango, horse_a, horse_b, enc_count, a_wins, b_wins,
avg_diff_a_minus_b`) is unchanged, so `stage_h2h_horse_summary` and
  `append_features_sql` need **no changes**.
- The `--target-race` focused/inference-mode path
  (`target_pair_filter_sql`/`stage_target_horses`) was not exercised by this
  investigation (training-mode only, `--target-race` unset, matching how the
  wrapper invokes it) — if adopted, that path needs its own equivalence pass
  before relying on it in production single-race inference.
- If adopted, this would let `run_stage9_h2h_peryear.sh`'s whole per-year
  wrapper (and the 20x redundant `race_history` rebuild it accepts as a
  known cost) be retired in favor of one direct invocation with
  `--input-dir` pointed at the full `s8-grade-lineage` tree.

Not touched: `src/scripts/finish-position-features/add-head-to-head-features.py`
(read-only per task instructions). All prototype code and this doc are the
only artifacts from this investigation; adopting the change is a separate,
user-approved step.
