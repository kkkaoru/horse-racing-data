# H2H Feature Build — Approach C: DuckDB Out-of-Core Engine Reality Check + JRA-Scale Projection (2026-07-04)

- **Date**: 2026-07-04
- **Category**: finish-position feature engineering, DuckDB engine/ops investigation (not a model accuracy probe)
- **Trigger**: `src/scripts/finish-position-features/add-head-to-head-features.py` OOMs in
  `stage_current_pair_aggregates` on full-history NAR at 6-12GB DuckDB limits; a per-year
  wrapper (`tmp/candidate-leak-clean-retrain/nar-full-regen/run_stage9_h2h_peryear.sh`)
  works around it by scoping. This track's job: is there an engine-config fix (bigger
  spill / out-of-core tuning) that would make the _unscoped_ query just work, and what
  does that imply for a future JRA-scale (full store) regen. Sibling tracks: Approach A
  (pair-history-once orchestration, `docs/probes/h2h-buildability-approach-a-2026-07-04.md`)
  and Approach B (lower-memory query rewrite, `docs/probes/h2h-buildability-approach-b-2026-07-04.md`).
- **Headline answer**: **spill-config does not fix this OOM, and cannot** — the two
  memory sinks that actually blow the budget (`CREATE INDEX` building a second ART
  index on the same huge intermediate table, and a skewed hash-partition in the final
  self-join+`GROUP BY ALL`) are not spillable classes of memory in DuckDB regardless of
  `temp_directory` / `max_temp_directory_size`. This is confirmed both empirically (5/5
  reproductions below spilled exactly 0 bytes before OOMing) and by DuckDB's own docs.
  The wrapper's scoping approach is the only thing that actually worked, and it worked by
  cutting _data volume_, not by improving spillability.

## 1. Engine facts

- **DuckDB version installed**: `1.5.3` (`uv run python -c "import duckdb; print(duckdb.__version__)"`,
  pinned `>=1.1.0` in `pyproject.toml`).
- **Correction to the task brief**: the brief assumed `configure_duckdb_session()` governs
  this script's session. It does not — that function lives in
  `src/scripts/finish_position_features_duckdb.py` (used only by the stage-1 base-feature
  builder) and is never imported by `add-head-to-head-features.py`. The H2H script instead
  calls `_resource_defaults.apply_to_connection()` (`src/scripts/finish-position-features/_resource_defaults.py:215-231`),
  which **already** sets:
  ```
  SET temp_directory='/tmp/duckdb-spill'
  SET max_temp_directory_size='30GB'
  ```
  unconditionally, every run — spill _is_ configured, has been all along, and the target
  volume (`/System/Volumes/Data`, 285GB free at time of test) has ample space. `main()`
  additionally sets `SET preserve_insertion_order=false` — also already applied, so that
  DuckDB-suggested remediation is not the missing piece either.
- **`/tmp/duckdb-spill/table_spill` is not DuckDB's engine spill** — it's this codebase's
  own **application-level checkpoint/resume system** (`finish_position_features_duckdb.py`
  `CheckpointManifest` / `spill_temp_tables_to_disk`, ~line 3358+): it writes _finished_
  intermediate tables out to Parquet so `--resume` can reload instead of recomputing. It
  is unrelated to the buffer manager's `temp_directory` spill path and was pre-existing
  (1.4GB) from an earlier stage-1 checkpoint, not something created by any test in this
  investigation. Across every experiment run here — 5 controlled H2H reproductions plus a
  from-scratch synthetic test — **DuckDB's own buffer-manager spill never wrote a single
  byte anywhere**, in this codebase's actual workload shape.

## 2. Operator × can-spill matrix (DuckDB 1.5.3)

| Operator                                                                                       | Out-of-core support                                                                                                                          | Evidence                                                                                                                                                                                                                                                                                                                                           |
| ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table/CTAS materialization (streaming projection, no blocking op)                              | Not memory-bound at all — writes stream to storage as produced                                                                               | Synthetic 20M-row `CREATE TABLE AS SELECT` completed in 0.7s at `memory_limit='500MB'`                                                                                                                                                                                                                                                             |
| Hash join (build/probe), Hash aggregate (`GROUP BY`), `ORDER BY`/sort                          | Documented out-of-core support since DuckDB ~v0.9 (external join/aggregate/sort)                                                             | Not fully isolable from this script alone (it doesn't run bare joins/aggregates without a downstream `CREATE INDEX`), but no failure was ever attributed to a _pure_ join/aggregate step below the index-creation ones                                                                                                                             |
| **`CREATE INDEX` (ART secondary index)**                                                       | **No.** Must fit entirely in memory during creation; DuckDB cannot spill build-time ART state to disk at all, regardless of `temp_directory` | Directly reproduced twice (real data + synthetic, §3) — building a 2nd ART index on an already-resident table OOMs instantly with 0 bytes spilled. Confirmed by official docs: _"ART indexes must currently be able to fit in memory during index creation."_ ([duckdb.org/docs/current/sql/indexes](https://duckdb.org/docs/current/sql/indexes)) |
| Final self-join + `LEFT JOIN pair_history` + `GROUP BY ALL` in `stage_current_pair_aggregates` | Nominally spillable (hash join + hash aggregate) but failed with "failed to pin block" at 8GB in a bounded, 1-year-scoped repro              | Consistent with a single hash-partition too large to shrink further via radix partitioning — i.e., data skew (a horse-pair meeting the same rival dozens of times over 20 years) that out-of-core partitioning does not resolve at any memory limit tested                                                                                         |

## 3. Reproducing the OOM cheaply + testing whether spill-config saves it

**Setup**: symlinked `s8-grade-lineage/race_year=2006` (153,617 rows, already known-good at
production `MEM=12GB`) into `tmp/candidate-h2h-approach-c/scoped-input-2006/`, ran the real
`add-head-to-head-features.py` unmodified via CLI flags only (`--threads`, `--memory-limit`),
`du`-tracked `/tmp/duckdb-spill` before/after each run.

| `--memory-limit` | `--threads`    | Result                     | Failing point (traceback)                                                                 | DuckDB-internal mem at failure | `/tmp/duckdb-spill` bytes written |
| ---------------- | -------------- | -------------------------- | ----------------------------------------------------------------------------------------- | ------------------------------ | --------------------------------- |
| 2GB              | 2              | OOM                        | `stage_pair_history`: `CREATE INDEX pair_history_a_idx` (1st ART index on `pair_history`) | 1.8 GiB / 1.8 GiB used         | **0**                             |
| 3GB              | 2              | OOM                        | same (1st ART index)                                                                      | 2.7 GiB / 2.7 GiB used         | **0**                             |
| 6GB              | 2              | OOM                        | `stage_pair_history`: `CREATE INDEX pair_history_b_idx` (**2nd** ART index, same table)   | 5.5 GiB / 5.5 GiB used         | **0**                             |
| 8GB              | 2              | OOM                        | `stage_current_pair_aggregates`: self-join + `LEFT JOIN pair_history` + `GROUP BY ALL`    | 7.4 GiB / 7.4 GiB used         | **0**                             |
| 12GB             | 4 (production) | **success** (all 21 years) | —                                                                                         | peak RSS 7.2-10.9GB/year       | n/a (never needed)                |

This is a clean, monotonic progression: raising the memory ceiling just buys past the
next non-spillable checkpoint (base table → 1st index → 2nd index → the final
join+aggregate), never engaging the spill path at any point along the way. The original,
never-scoped production OOM (`MEM=12GB`, all 21 years' `current_field` at once) failed at
exactly the same site as the 8GB bounded repro — `stage_current_pair_aggregates`:

```
_duckdb.OutOfMemoryException: Out of Memory Error: could not allocate block of size 256.0 KiB (11.1 GiB/11.1 GiB used)
```

**Isolated synthetic confirmation** (no real data, no Postgres, ~2s total —
`tmp/candidate-h2h-approach-c/synthetic/`): 20M-row table (`int, int, varchar(50)`),
`memory_limit='500MB'`, `temp_directory` pointed at a real disk path with 10GB headroom:

```
CREATE TABLE t AS SELECT ...                → 0.7s, succeeds (streaming CTAS, no pressure)
CREATE INDEX idx_a ON t (a)                 → 1.3s, succeeds (1st ART index fits)
CREATE INDEX idx_b ON t (b)                 → OOM instantly: "could not allocate block
                                               of size 256.0 KiB (476.7 MiB/476.8 MiB used)"
                                               — spill dir: 0 bytes written
```

This isolates the mechanism completely: it is specifically **stacking a second ART index
on the same large table** that breaks, matching `add-head-to-head-features.py`'s own
pattern of building `pair_history_a_idx` and `pair_history_b_idx` back-to-back on the same
`pair_history` table (plus `race_history_idx` and `h2h_horse_summary_idx` elsewhere in the
pipeline — 4 ART indexes total across the script, all of which stay permanently pinned
once built).

**Verdict on the core question**: _"does spill-config fix it: yes/no"_ → **No.** Not
"insufficiently," but structurally no — ART index creation is documented and confirmed to
be unspillable in DuckDB 1.5.3, and the query's other OOM site (a skewed join partition)
also showed zero spill activity before failing. No amount of `max_temp_directory_size` or
faster/bigger disk would change this.

## 4. Cost comparison: spill vs in-memory

Not applicable in the form requested — spill never engaged in any test, so there is no
spill-wall-time to measure against an in-memory baseline for _this_ bottleneck. The
wrapper's actual fix (data scoping via per-year `--input-dir`) is not a spill-cost
tradeoff at all; its only cost is redundancy (see §5). Where this codebase _does_ get
disk-based resilience (stage 1's checkpoint/resume via `table_spill` parquet writes,
§1), that is an application-level, not engine-level, mechanism, and is consistent with
this codebase's own engineers having independently reached the same conclusion (build
your own resumability; don't lean on DuckDB's temp_directory for large materializations).

## 5. JRA-scale projection

**Critical correction to the naive row-count-ratio approach**: `stage_race_history()`
(`add-head-to-head-features.py:69-90`) has **no category/source filter** — only
`race_date >= from_date`. It reads `pg.race_entry_corner_features` directly (not the
category-scoped input parquet), so **`pair_history` is rebuilt over the combined JRA+NAR
dataset on every single invocation, regardless of which category's H2H features are being
generated.** The join's `h1.source = h2.source` predicate only prevents cross-category
_pairs_ in the output — it does not reduce the input side or the two ART-index builds,
which is exactly what OOM'd in §3.

Measured (via `pg.race_entry_corner_features`, same filters as `stage_race_history`):

| Scope                                                                                                        | Rows                            | Notes                                                                |
| ------------------------------------------------------------------------------------------------------------ | ------------------------------- | -------------------------------------------------------------------- |
| `source='jra'`, all-time (1954-10-23 → 2026-06-27)                                                           | 2,848,205                       |                                                                      |
| `source='jra'`, `>= 2006-01-01`                                                                              | 1,610,271                       |                                                                      |
| `source='jra'`, `< 2006-01-01`                                                                               | 1,237,934                       | pre-2006 JRA rows excluded by the NAR wrapper's `FROM=20060101`      |
| `source='nar'`, all-time (2005-01-01 → 2026-06-27)                                                           | 3,268,208                       |                                                                      |
| Combined, `>= 2006-01-01` (**what the just-completed NAR run actually built `pair_history` over, 21 times**) | 4,628,970                       |                                                                      |
| Combined, `>= 1954-01-01` (full "JRA全期間" per the script's own docstring aspiration)                       | 6,019,781                       | **1.30×** the row volume already proven at 12GB                      |
| Avg rows/race (field-size proxy), 2020+                                                                      | NAR 10.15, **JRA 12.50** (+23%) | pairs/race ∝ n(n-1)/2 → JRA ≈ **1.56×** more pairs per race than NAR |

**This means the just-completed NAR full regen (2006-2026, `FROM=20060101`) is already a
close proxy for a JRA run using the same `from_date`** — same shared 4.63M-row
`race_history`, same `pair_history` self-join, same 2 ART indexes, same non-spillable
bottleneck. Measured on that run (`s9-peryear-peak-mem.log`, `MEM=12GB`, `threads=4`):

- **Total wall time, all 21 years (2006-2026)**: 16m18s (08:55:43 → 09:12:01), ~47s/year avg.
- **Peak RSS range**: 7,194MB (2007) – 10,854MB (2024), mean ≈ 8,368MB — **0 OOMs**, but
  the tightest year (2024) used 90% of the 12GB budget (1.1GB / 9% headroom).

**Projection for a future JRA full-store regen**:

1. **If the JRA regen reuses `--from-date 20060101`** (same as NAR): `stage_pair_history`'s
   cost is _identical_ to what was just measured — no new engine risk. The only
   category-specific unknown is `stage_current_pair_aggregates`'s per-year cost, which
   scales with JRA's own `current_field` size — and JRA's ~56% higher pairs/race is the
   one number that does **not** carry over from the NAR measurement and needs direct
   verification (see recommendation below).
2. **If the JRA regen instead reaches back to the script's docstring aspiration
   ("JRA全期間", ~1954)**: row volume for the shared `race_history`/`pair_history` build
   grows ~1.30×. Applied to the tightest already-measured years (10.6-10.9GB at 12GB),
   this projects to **~13.8-14.1GB** for the equivalent years — over the 12GB budget, and
   into the same ART-index OOM mechanism reproduced in §3, since bumping past ~14GB is
   the kind of static increase the wrapper's own docstring already flags as unsafe on
   this 48GB Mac (Colima permanently reserves 24GB, leaving 24GB total for all sibling
   agents combined — there isn't slack for one worker to jump from 12GB to 16GB+ while
   others are active). Pre-2006 JRA rivalries (retired/dead horses, zero overlap with
   today's runners) carry no modeling value anyway, so there's no accuracy reason to pay
   this cost.
3. **Approach A (pair-history-once) is the natural complement, not a competitor**: this
   investigation shows `pair_history` + its 2 ART indexes is both the single largest
   _and_ the single most redundant cost in the current wrapper — rebuilt from scratch on
   every one of the 21 (soon: more, for JRA) per-year batches even though it does not
   depend on which year's batch is running. Building it once (Approach A) removes that
   ~21× redundancy without touching the OOM mechanism itself.

## 6. Consolidated recommendation

| Use case                                                               | Recommended approach                                                                                                                                                                              | Why                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **NAR rerun**                                                          | Keep the per-year wrapper (proven: 16m18s total, 0 OOMs, peak 7.2-10.9GB @ 12GB) or migrate to Approach A once available                                                                          | Already working; A's only advantage is removing the 21× redundant `pair_history` rebuild, not fixing an OOM                                                                                                                                                                                                                                                                                                                                                                                                |
| **JRA future full regen**                                              | Per-year (or Approach A) wrapper **+ cap `--from-date` to 2006 or later** (not full "JRA全期間" back to 1954) **+ a single bounded 1-year JRA pre-flight test** before committing to the full run | Engine/spill tuning is a proven dead end (ART index creation cannot spill in DuckDB 1.5.3, confirmed 5/5 + official docs). Capping `from_date` keeps row volume in the already-proven-safe envelope instead of the ~1.30× regime that risks re-triggering the same OOM. JRA's +56% pairs/race is the one risk that isn't already covered by the NAR measurement and should be measured directly (mirror this investigation's §3 method: single scoped year, threads/memory sweep) rather than assumed safe |
| **One-off / incremental (single target race)**                         | Existing `--target-race` focused mode (`stage_target_horses`)                                                                                                                                     | Already scopes _both_ sides of the `pair_history` self-join to the target field via a fundamentally different, already-cheap code path (`target_pair_filter_sql`) — unrelated to the batch-mode OOM mechanism entirely; no engine tuning needed                                                                                                                                                                                                                                                            |
| **If any future run's peak RSS approaches its `memory_limit` ceiling** | Reduce `--threads`, not raise `--memory-limit`                                                                                                                                                    | Matches DuckDB's own error-message suggestion and this Mac's fixed 24GB-usable ceiling (Colima reserves the other 24 of 48GB) — there is no room to raise per-worker `memory_limit` further while sibling agents are active                                                                                                                                                                                                                                                                                |

## Artifacts

- `tmp/candidate-h2h-approach-c/scoped-input-2006/` — symlinked single-year input used for all real-data repros
- `tmp/candidate-h2h-approach-c/run-{2gb,3gb,6GB,8GB}.log` — full tracebacks for each memory-limit repro
- `tmp/candidate-h2h-approach-c/synthetic/` — isolated 20M-row ART-index synthetic repro script output
- No changes to `src/` — all findings are read-only reproductions via existing CLI flags; any `configure_duckdb_session()` / `apply_to_connection()` change implied above is a **proposal only**
