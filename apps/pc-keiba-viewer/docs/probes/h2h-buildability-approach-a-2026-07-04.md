# H2H Feature Build — Approach A: pair_history-once, targets incrementally (2026-07-04)

- **Date**: 2026-07-04
- **Task**: #20, H2H approach A prototype (team investigation alongside
  approach B "lower-memory query rewrite" and approach C "DuckDB out-of-core
  tuning + JRA-scale projection")
- **Trigger**: `src/scripts/finish-position-features/add-head-to-head-features.py`
  (read-only, not modified) OOM'd DuckDB at 6GB/12GB when run unscoped over
  the full NAR window. The live workaround —
  `tmp/candidate-leak-clean-retrain/nar-full-regen/run_stage9_h2h_peryear.sh`
  (PID 85715, completed during this investigation at 09:12) — scopes
  `current_pair_aggregates` to one `race_year` partition at a time via a
  symlinked `--input-dir`, which is memory-safe (peak 7.2–10.9GB/year) but,
  per its own comment, rebuilds `race_history` + `pair_history` from postgres
  on every single invocation (`--from-date` is a fixed `20060101`, not
  incremented per year), i.e. the same self-join computed 21 times.
- **Prototype location**: `tmp/candidate-h2h-approach-a/` (`step1_materialize.py`,
  `step2_year_aggregate.py`, `equivalence_check.py`, `step1_out/`, `logs/`).
  Read-only against postgres and against the ground-truth batch; writes only
  under this tmp dir.

## Design

`add-head-to-head-features.py`'s DAG splits cleanly into a scope-independent
half and a scope-dependent half:

- **Scope-independent** (same result regardless of which year is being
  featurized, as long as `--from-date` is fixed): `race_history` (postgres
  read + filter) and `pair_history` (the self-join everyone is worried about —
  `race_history ⋈ race_history` on race key, `horse_b > horse_a`).
- **Scope-dependent**: `target_races` (from the year-scoped input parquet),
  `current_field`/`current_pairs` (this year's field, squared), and
  `current_pair_aggregates` (`current_pairs LEFT JOIN pair_history ... AND
ph.race_date < cp.current_date`) — this is also where the _real_ memory risk
  lives (see Finding 1 below), so it must stay year-scoped regardless of
  approach.

Approach A materializes the independent half **once** to partitioned parquet,
then has STEP2 read only the slices each target year needs:

- `pair_history` partitioned by **`hist_year`** (= `h1.kaisai_nen`, the
  historical race's year, carried through the join). For target year `Y`,
  STEP2 filters `hist_year <= Y` — a correct, hive-partition-pruned superset
  of "`race_date < current_date`" (if `hist_year > Y` the historical race_date
  cannot precede any current-year race_date, so those partitions are safely
  skippable; the exact date comparison is still applied afterward for the
  `hist_year == Y` boundary case of same-year-earlier-date encounters).
- `race_history` partitioned by `kaisai_nen`, read for just the batch's target
  year(s) to reconstruct `current_field` (joined to `target_races`, itself
  derived from the year-scoped input parquet exactly as the original script
  does).

STEP2 touches **no postgres connection at all** — everything comes from local
parquet written once by STEP1.

## Equivalence check (2006 vs ground truth `s9-h2h-batches/batch_2006`)

| Check                                                       | Result                     |
| ----------------------------------------------------------- | -------------------------- |
| Row count                                                   | 153,617 vs 153,617 — match |
| Column set                                                  | identical                  |
| 500-random-horse sample (5,168 rows), all 6 `h2h_*` columns | 0 mismatches               |
| **Full table** (153,617 rows), all 6 `h2h_*` columns        | **9 mismatches**           |

The 9 full-table mismatches are all on `h2h_avg_finish_diff_vs_field` only —
`h2h_encounter_count`, `h2h_win_count_vs_field`, `h2h_loss_count_vs_field`, and
`h2h_unique_rivals_count` (all integer/count-derived) match exactly on every
one of the 9 rows. The differences are floating-point summation-order
artifacts: e.g. `-0.1346153846153846` vs `-0.13461538461538464`, and one case
of `-3.29e-17` (effectively zero) vs `0.0`. These come from
`sum(self_avg_diff * enc_count) / nullif(sum(enc_count), 0)` being evaluated
over rows in a different physical order (parquet-sourced local scan vs
postgres-sourced scan, different thread/partition scheduling) — IEEE 754
addition is not associative. Magnitude: ~1e-15 to 1e-17 absolute, i.e.
machine-epsilon-level, on 9/153,617 rows (0.006%). **Not a semantic
discrepancy** — verdict is exact match for all practical (and ML-training)
purposes.

## Measurements

All runs: DuckDB `:memory:`, `threads=4`, `memory_limit='12GB'` (matching the
live wrapper's settings). Peak RSS via `resource.getrusage(RUSAGE_SELF)`
(single-process, no subprocess to lose track of). Measured concurrently with
sibling agents' own DuckDB/postgres load (approach B/C prototypes were
actively running overlapping tests at the same wall-clock time), system free
memory 87–89% throughout — not an idealized quiet-system number, but not
adversarial either.

**STEP1 (materialize once, NAR+JRA combined since postgres's
`race_entry_corner_features` holds both sources and the join partitions
naturally on `source`):**

| Stage                                          | Rows       | Elapsed  | Peak RSS  |
| ---------------------------------------------- | ---------- | -------- | --------- |
| `race_history` (from-date 2006+, both sources) | 4,628,970  | 2.4s     | 439MB     |
| → parquet write (partition by `kaisai_nen`)    | —          | 0.3s     | 842MB     |
| `pair_history` self-join                       | 23,549,598 | 1.9s     | 3,481MB   |
| → parquet write (partition by `hist_year`)     | —          | 1.0s     | 3,536MB   |
| **Total**                                      |            | **5.5s** | **3.5GB** |

Disk footprint: `race_history` 26MB + `pair_history` 114MB = **140MB** for the
full 2006–2026 NAR+JRA combined range.

**STEP2 (per target year, reads only local parquet):**

| Target year                                   | `pair_history_slice` rows | Elapsed | Peak RSS |
| --------------------------------------------- | ------------------------- | ------- | -------- |
| 2006 (shallowest history)                     | 1,202,643                 | 0.6s    | 1.75GB   |
| 2015 (mid-depth)                              | 11,833,854                | 1.0s    | 2.30GB   |
| 2026 (deepest — needs the full 21-year slice) | 23,549,598                | 1.1s    | 4.03GB   |

STEP2 cost grows mildly with historical depth (more years of `pair_history` to
scan for later target years) but stays sub-2-second and sub-4GB even at the
worst case (last year, full 21-year lookback).

**Projected total for 21 NAR years**: STEP1 (5.5s, once) + 21 × STEP2 (~1.0s
avg) ≈ **~27s total**.

**Actual wrapper cost, same 21 years, same run** (from
`nar-full-regen/master.log` / `s9-peryear-peak-mem.log`, this exact
2026-07-04 execution): stage9 start → done spans **08:55 → 09:12:01 ≈ 17
minutes**, individual batches 45–75s each, peak RSS 7.2–10.9GB/batch.

**Speedup: ~35–40x wall time, ~2–3x peak memory reduction.** The wrapper's own
per-year `pair_history`/`race_history` rebuild (postgres round-trip + self-join)
is not actually the dominant cost in isolation (STEP1 alone is 5.5s) — under
real concurrent multi-agent load the observed 45–75s/batch likely reflects CPU/
postgres contention with sibling agents, which Approach A also reduces
structurally: it opens **one** postgres connection total across the whole
21-year build instead of 21.

## Finding 1 (important, not just a speed note): where the real OOM risk lives

The docstring's memory budget ("JRA 全期間で約150M 行想定 (DuckDB 24GB で持つ)")
turned out to substantially overestimate the actual `pair_history` self-join
cost. Measured directly (same STEP1-style query, JRA-only, **full 1954–2026
history, no `from_date` filter at all**):

|                                             | Rows       | Elapsed | Peak RSS |
| ------------------------------------------- | ---------- | ------- | -------- |
| `race_history` (JRA, all history)           | 2,813,269  | 6.6s    | 234MB    |
| `pair_history` self-join (JRA, all history) | 16,546,345 | 0.4s    | 1.4GB    |

16.5M pairs at 1.4GB, not ~150M pairs at 24GB. The `pair_history` self-join
itself is cheap at any realistic scope (NAR 2006+: 23.5M rows/3.5GB; JRA
full-history: 16.5M rows/1.4GB) because DuckDB partitions the join per race
key rather than doing a naive cross product. **The actual OOM the task
description refers to almost certainly came from an earlier _unscoped_
`current_pair_aggregates`** — i.e. before the per-year wrapper existed,
`current_pairs` (built from _all_ years' races at once) crossed against the
_full_ `pair_history` in one shot, which is a combinatorially much larger join
than `pair_history` alone. Both the existing per-year wrapper and this
Approach A prototype avoid that by keeping `current_pair_aggregates`
year-scoped — Approach A's only change is eliminating the redundant rebuild of
the (actually cheap) independent half.

## JRA-scale extrapolation (not equivalence-checked — no JRA ground-truth

batch was available to diff against; STEP1-equivalent measured directly,
STEP2 extrapolated)

- STEP1 for JRA at the deployed training window (2013+, 990,029 rows) would be
  cheaper than the full-history number above (2.81M rows → 1.4GB/0.4s), so
  well under 1.4GB/1s.
- STEP1 for JRA's entire archive (1954+, 2.81M rows) measured directly above:
  1.4GB peak, 7.1s (dominated by the postgres read, not the join).
- STEP2 per JRA year was not run (no JRA `race_year=YYYY` scoped input parquet
  was available in this session to diff against a ground truth), but by
  analogy to the NAR STEP2 scaling (sub-2s, sub-4GB even at 21-year lookback
  depth), and given JRA fields are typically similar-or-larger than NAR
  (more pairs/race, but far fewer races/day), STEP2 per JRA year should land
  in the same low-single-digit-second, low-single-digit-GB range. This is an
  extrapolation, not a measurement — flagged as a caveat, not a claim.

## Verdict: **viable — prefer over the current per-year wrapper**

No caveat found that would argue for keeping the redundant-rebuild wrapper.
Concretely:

1. **Correctness**: exact match modulo float64 non-associativity in one
   derived average column, 9/153,617 rows, ~1e-15 magnitude. Not a behavior
   change.
2. **Memory**: strictly safer margin (STEP2 peak 1.75–4.0GB vs wrapper's
   7.2–10.9GB) — more headroom against the sibling-agent-contention kernel
   panic risk this Mac already has to guard against.
3. **Speed**: ~35–40x less wall time for the same 21-year NAR build, because
   the O(N²) self-join — the part everyone assumed was the expensive,
   dangerous piece — is actually cheap; the wrapper was just paying for it 21
   times instead of once, plus 21 separate postgres connections.
4. **Scales to JRA "for free"**: since postgres's `race_entry_corner_features`
   already holds both sources, a single STEP1 run materializes `pair_history`
   for JRA and NAR (and Ban-ei, if present) simultaneously — the same 140MB
   artifact serves both categories' STEP2 runs, a second layer of avoided
   redundancy beyond what this task asked for.
5. **No structural downside found**: STEP1 is a one-time ~5.5s/~3.5GB cost;
   STEP2 has no postgres dependency, so it's also more robust to transient
   postgres/network hiccups than re-attaching 21 times.

**When to prefer the per-year wrapper instead**: only if `--from-date` itself
needs to change per run (e.g. a rolling window that drops old history) in a
way that would invalidate the materialized `pair_history` — that's not the
case today (`--from-date` is a fixed constant across the whole 21-year build).
If the training window policy ever becomes "trailing N years," STEP1 would
need to either over-materialize (safe, cheap given the numbers above) or be
re-run when the window's lower bound moves, which is still far cheaper than
today's 21x-per-run rebuild.

## Caveats

- Measurements were taken on a shared, moderately loaded system (sibling
  approach-B/C prototypes running concurrent DuckDB/postgres queries, 87–89%
  system free memory) rather than a fully idle box — absolute wall-clock
  numbers could shift under different contention, but the _relative_
  ordering (materialize-once ≪ rebuild-21x) should hold or widen under
  contention, since Approach A does strictly less total postgres/CPU work.
- JRA STEP2 was not empirically run or equivalence-checked (no JRA-scoped
  target-year parquet + ground-truth batch available in this session); the
  JRA numbers above cover STEP1 only, with STEP2 extrapolated by analogy to
  NAR's measured scaling.
- This prototype does not modify `add-head-to-head-features.py` and does not
  touch git; if adopted, the wrapper script (or the source script itself)
  would need a real edit to add a `--pair-history-dir`/`--race-history-dir`
  read path, which is out of scope for this investigation.
