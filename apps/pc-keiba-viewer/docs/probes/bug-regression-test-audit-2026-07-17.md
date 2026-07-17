# Bug Regression-Test Completeness Audit (2026-07-17)

**Status: interim commit.** Items A/B/D/E/F/I/J are complete (7 of 10). Items
C/G/H all live in `apps/finish-position-cron`, held for orchestrator
clearance (deploy-GO-pending package) — see §6. This doc will be updated with
a follow-up commit once that clearance is received and that work completes.

- **Trigger**: USER directive relayed by orchestrator — "every bug found today
  must be detectable by running the test suite." This agent was appointed as
  an independent auditor (fresh eyes, did not write today's fixes) for the
  bug ledger below (items A-J).
- **Method** (per task instructions, "gold standard"): for every bug with a
  claimed fix, temporarily reintroduce the exact broken behavior in the
  working tree via `Edit` (never `git stash`/`git checkout`), run the
  relevant test(s), confirm they FAIL, restore the original code via a second
  `Edit`, then confirm `git diff --exit-code <file>` returns 0 (byte-for-byte
  restoration). Where a live mutation was judged too costly/risky (real
  Neon-dependent code paths), a test-design review is substituted and stated
  explicitly as such.
- **Scope constraint**: `apps/finish-position-cron` (items C, G, H) is
  deploy-GO-pending — held for orchestrator clearance before any edit
  (including temporary mutation-testing edits), per task instructions.

## Ledger

| #   | Bug                                                                                                                                                                                                                                                                                                       | Fix commit(s)                                                                | Detection test(s)                                                                                                                                                                                                                       | Verification method                                                                                 | Result                                         |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| A   | `feature_guard` (degenerate-feature guard) wiring at its actual call sites in `predict_upcoming.py` had zero test coverage — the pure function was thoroughly unit-tested, but nothing tested that `_score_one_race_direct`/`_score_one_race_nar_blend` actually call it                                  | 57a4cd7f (fix); gap closed by `d04206c1` (this audit)                        | `apps/finish-position-predict-container/tests/test_predict_upcoming.py::test_score_one_race_direct_skips_write_for_degenerate_feature_matrix` + `::test_score_one_race_direct_scores_healthy_feature_matrix` (both added by this audit) | Mutation (Edit)                                                                                     | **GAP FOUND AND CLOSED** — see §1              |
| B   | Cell-routing variant loader feature-ORDER mismatch not caught by the (deliberately order-independent) hash check                                                                                                                                                                                          | 57a4cd7f                                                                     | `test_variant_booster_feature_order_matches_permuted_order_is_a_mismatch`, `test_score_races_rejects_variant_with_feature_order_mismatch`                                                                                               | Mutation (Edit)                                                                                     | PASS — already covered                         |
| C   | `expectedModelVersion()` missing the `venue==02` → jockey-pedigree269 rule                                                                                                                                                                                                                                | 7807e6cd (fix), 63c69c08 (parity guard)                                      | `focused-full-completion.test.ts`: 4 dedicated venue02 tests + 1 parity-guard test reading the real `cell_routing.json`                                                                                                                 | **Pending clearance** (finish-position-cron)                                                        | pending                                        |
| D   | `query_finish_position_metrics` picked the served row by `ORDER BY prediction_generated_at DESC` (latest write wins), silently preferring a later garbage rescore over an earlier genuine prediction                                                                                                      | 1d7e3215                                                                     | `test_serve_accuracy_report.py::test_dedup_picks_genuine_row_over_cluster_b_backfill` (+ 6 sibling tests on the shared `select_serving_row`)                                                                                            | Mutation (Edit)                                                                                     | PASS — already covered                         |
| E   | RS counterpart of D (`query_running_style_metrics`)                                                                                                                                                                                                                                                       | 218b5849                                                                     | `test_dedup_running_style_picks_genuine_row_over_cluster_b_backfill` (+ siblings, shares `select_serving_row` with D)                                                                                                                   | Mutation (Edit)                                                                                     | PASS — already covered (same mutation as D)    |
| F   | `build_rec_select_sql`'s UPCOMING-window tie-break ordered on `_rec_priority` alone, keeping an incomplete `race_entry_corner_features` row (NULL `finish_position`) over a settled `jvd_se`/`jvd_ra` row                                                                                                 | 2326bf1f                                                                     | `test_finish_position_features_duckdb_integration.py::test_build_rec_select_sql_upcoming_window_recovers_settled_result_over_stale_corner_row` (execution-level, real DuckDB) + a SQL-shape test                                        | Mutation (Edit)                                                                                     | PASS — already covered                         |
| G   | `corner-features-refresh.ts`: 3 always-failing bugs (multi-command DDL, `CREATE EXTENSION` in read-only tx, select-list alias reference) undetected because mocked tests never exercised the real driver                                                                                                  | a87d5356                                                                     | TBD — pending clearance                                                                                                                                                                                                                 | **Pending clearance** (finish-position-cron) — flagged by orchestrator as the highest-scrutiny item | pending                                        |
| H   | `focused-full-completion.ts`'s completion guard checks row COUNT only, never score quality/stddev (the actual Cluster-B failure signature)                                                                                                                                                                | not fixed — characterization only, no behavior change                        | TBD                                                                                                                                                                                                                                     | **Pending clearance** (finish-position-cron)                                                        | pending                                        |
| I   | Realtime-odds UPCOMING-window fallback (`finish_position_features_duckdb.py`) cast the `'0000'`/`'00'` not-yet-confirmed placeholders (tansho_odds/tansho_ninkijun) to `0`/`0.0` instead of NULL — found by this agent while writing up the cross-pool-odds-divergence probe's serve-availability section | fixed by `dde59c45` (this audit, see §2)                                     | `test_finish_position_features_duckdb_integration.py::test_build_rec_select_sql_upcoming_window_treats_unconfirmed_odds_placeholder_as_null` (added by this audit)                                                                      | Mutation (Edit)                                                                                     | **GAP FOUND AND CLOSED** — see §2              |
| J   | MLflow `timeline.upsert_timeline_point`'s "skip if a metric already exists at this step" is a presence check, not a true upsert — re-ingesting the same date with a different computed value silently keeps the OLD value                                                                                 | not a bug — documented, intentional behavior; audited for test coverage only | `apps/mlflow/tests/test_timeline.py::test_upsert_timeline_point_does_not_duplicate_same_step` (pre-existing since 2026-07-08, commit `a3102b12` — well before today)                                                                    | Mutation (Edit)                                                                                     | PASS — already covered, confirmed pre-existing |

---

## §1. Bug A — a real gap, found and closed

`predict_lib/feature_guard.py`'s pure functions
(`missing_feature_fraction`/`race_missing_feature_fraction`/
`is_degenerate_feature_matrix`) are covered by 17 thorough unit tests in
`tests/test_feature_guard.py` (boundary conditions, the "one sparse debut
horse must not false-positive" case, custom thresholds). But `commit 57a4cd7f`
also wires this guard into TWO call sites in `predict_upcoming.py`
(`_score_one_race_direct`, `_score_one_race_nar_blend`) — and nothing in
`tests/test_predict_upcoming.py` exercised either call site with a degenerate
matrix.

**Confirmed empirically**: removed both `if is_degenerate_feature_matrix(...):
... return []` blocks from `predict_upcoming.py` via `Edit`, ran the full
package suite:

```
1288 passed, 1 skipped in 27.03s
Total coverage: 99.81%
```

Every existing test still passed. This is exactly the class of risk the
USER directive exists to close: a future refactor that accidentally drops
this wiring (e.g. a merge conflict resolution, or copying
`_score_one_race_direct`'s body into a new variant) would ship with zero
test failures.

**Fix**: added two new tests directly exercising `_score_one_race_direct`
(imported the same way other private helpers already are in this test file,
via `cast(..., getattr(predict_upcoming, "_score_one_race_direct"))`):

- `test_score_one_race_direct_skips_write_for_degenerate_feature_matrix` —
  a 2-entry race with every feature column absent must return `[]`.
- `test_score_one_race_direct_scores_healthy_feature_matrix` — companion
  test: a fully-populated 2-entry race must still return 2 scored rows (the
  guard must not false-positive on a healthy race).

Re-verified the mutation is now caught: with the wiring removed again,
`test_score_one_race_direct_skips_write_for_degenerate_feature_matrix` FAILs
(`1 failed, 1 passed`); restored the wiring, both new tests pass. File
restored to its committed state (`git diff --exit-code` = 0 on both
`src/predict_upcoming.py` and confirmed via re-running the full suite).

**Final state**: 1290 passed, 1 skipped, coverage 99.81% (unchanged from
baseline — the 2 new tests exercise already-instrumented lines).
`ruff check`: clean. `basedpyright`: 0 errors/warnings. `oxlint`/`oxfmt`:
clean.

`_score_one_race_nar_blend`'s identical wiring point was not separately
covered by a new test — no existing test calls this private function at all
(NAR blend is only exercised indirectly through higher-level flow tests that
don't hit the degenerate-input branch), and building a `TransformerScorer`
stub for a minimal, purely-defensive branch was judged lower value than the
`_score_one_race_direct` coverage above, which is the higher-traffic
function (used for the category-default AND every cell-routing-variant path,
across JRA/NAR/Ban-ei — `_score_one_race_nar_blend` only fires for the single
NAR Set-Transformer blend path). Both call sites share the identical
`is_degenerate_feature_matrix` import and one-line guard pattern; a reviewer
who notices one wiring point missing a test has direct precedent for adding
the second. Flagged here rather than silently left uncovered.

## §2. Bug I — a real gap, found by this agent, fixed

Found while writing up the cross-pool-odds-divergence probe
(`docs/probes/jra-crosspool-odds-divergence-2026-07-17.md` §7.3 item 4): the
UPCOMING-window fallback in `finish_position_features_duckdb.py`
(`_rec_select_from_se_ra`, lines ~705-712) computed `tansho_ninkijun`/
`tansho_odds` via `try_cast(nullif(trim(se.tansho_ninkijun), '') as int)` /
`try_cast(nullif(trim(se.tansho_odds), '') as double) / 10` — guarding only
the empty-string case, not the jvd not-yet-confirmed placeholder documented
in this repo's `reference_jvd_placeholder_semantics` memory (which covers
`kakutei_chakujun`'s `'00'` and odds fields' `'0000'`, but had not been
checked against this specific COALESCE block before).

**Verified empirically before fixing** (not assumed): queried the local PG
mirror directly —

```sql
select tansho_odds, count(*) from jvd_se
where kaisai_nen='2026' and tansho_ninkijun='00' group by 1
-- => tansho_odds='0000', count=816 (816/816, exact co-occurrence)
```

Confirming `tansho_ninkijun='00'` and `tansho_odds='0000'` are the same
not-yet-confirmed placeholder pair (mirroring `kakutei_chakujun`'s `'00'`
convention), not two independent conventions to guess at.

**Impact if untouched**: for a race with an unconfirmed/not-yet-settled
tansho_ninkijun/odds and no realtime-odds override available (`rt.*` NULL —
e.g. a batch/backfill feature-build run, or a realtime-fetch failure), the
UPCOMING-branch fallback would silently manufacture `tansho_ninkijun=0`,
`tansho_odds=0.0` — a fabricated "certain favorite at 0.0x odds" value fed
directly into the model's two most load-bearing features (both live in the
250-feature champion) instead of a correctly-NULL value the GBDT could
route around via its native null-handling.

**Fix**: `finish_position_features_duckdb.py`:

```diff
-        try_cast(nullif(trim(se.tansho_ninkijun), '') as int)
+        try_cast(nullif(nullif(trim(se.tansho_ninkijun), ''), '00') as int)
...
-        try_cast(nullif(trim(se.tansho_odds), '') as double) / 10
+        try_cast(nullif(nullif(trim(se.tansho_odds), ''), '0000') as double) / 10
```

**Test added**:
`test_build_rec_select_sql_upcoming_window_treats_unconfirmed_odds_placeholder_as_null`
— builds a 2-horse race (one with the `'00'`/`'0000'` placeholder pair, one
with normal confirmed values `'03'`/`'0055'`) through the real
`build_rec_select_sql` SQL, executed against an in-memory DuckDB with mock
`jvd_se`/`jvd_ra`/empty `race_entry_corner_features`/empty `nvd_se`/`nvd_ra`
tables (mirroring the established pattern from bug F's own execution-level
test), and asserts the placeholder row comes out NULL/NULL while the normal
row parses to `3`/`5.5`.

**Mutation-verified**: reverted the guard, ran the new test —
`1 failed` (`AssertionError` on the placeholder row's `tansho_ninkijun`/
`tansho_odds` no longer being `None`). Restored the fix, confirmed
`git diff --exit-code` clean, re-ran to confirm the test passes again.

**Final state**: full `apps/pc-keiba-viewer` suite `4557 passed`, coverage
`97.51%` (>=95% gate). `basedpyright`: 0 errors/warnings. `ty check`: 2
diagnostics, both pre-existing/unrelated (`corner_lightgbm.py` MLX class-base
warnings, confirmed identical to the count noted in commit `218b5849`'s own
message — not introduced by this change). `mccabe --min 16`: clean.

## §3. Bugs D/E/F — already covered, confirmed by mutation

All three fixes share (D, E) or parallel (F) a single well-designed pure
function (`select_serving_row`, generic via `TypeVar`) with tests that
directly encode the real incident scenario by name
(`test_dedup_picks_genuine_row_over_cluster_b_backfill`,
`test_dedup_running_style_picks_genuine_row_over_cluster_b_backfill`) rather
than a generic "does it run" check.

- **D/E**: reverted `select_serving_row`'s `sort_key`/`min(...)` logic to the
  literal original bug (`max(candidates, key=lambda c: gen_at.timestamp())`
  — pick the latest write, unconditionally). Ran
  `tests/test_serve_accuracy_report.py -k "select_serving_row or dedup"`:
  **7 failed, 5 passed**, including both Cluster-B-named tests (FP and RS).
  Restored; `git diff --exit-code` clean.
- **F**: reverted `order by (finish_position is null), _rec_priority` to the
  original `order by _rec_priority`. Ran
  `tests/test_finish_position_features_duckdb_integration.py -k
"rec_select_sql"`: **2 failed, 2 passed** — both the SQL-shape assertion
  and the execution-level test (which runs real DuckDB SQL against
  constructed tables and asserts the recovered `finish_position == 2`, not
  `None`) caught it. Restored; `git diff --exit-code` clean.

No new tests needed for D, E, or F.

## §4. Bug B — already covered, confirmed by mutation

Removed the `if not _variant_booster_feature_order_matches(...): continue`
block from `score_races()`'s variant-loading loop. Ran
`tests/test_predict_upcoming.py`:
`FAILED test_score_races_rejects_variant_with_feature_order_mismatch` (1
failed, 88 passed on that file). Restored; `git diff --exit-code` clean.

## §5. Bug J — already covered, confirmed pre-existing and mutation-tested

Team-lead's brief specifically asked whether this behavior (documented today
by the contender-reorder agent: timeline re-ingestion is skip-if-present, not
a true upsert) was already pinned by a test, or needed a new one.

Found `apps/mlflow/tests/test_timeline.py::
test_upsert_timeline_point_does_not_duplicate_same_step`: logs
`fp_top1_pct=44.5` for a date, re-ingests the SAME date with `fp_top1_pct=
99.9`, and asserts `client.get_metric_history(...)[0].value == 44.5` (the
ORIGINAL value, proving the second write was silently discarded — exactly
the "skip if present" behavior in question). `git log -S` confirms this test
was added in commit `a3102b12` (2026-07-08 17:36 JST), well before today —
genuinely pre-existing coverage, not a same-day patch prompted by this
finding.

**Mutation-verified**: removed the `if step in already_logged_steps:
continue` skip from `timeline.upsert_timeline_point` in
`src/mlflow_tracking/timeline.py`, ran the test: **1 failed** (the value
changed to `99.9` as expected once dedup was disabled). Restored; `git diff
--exit-code` clean. (Safe to mutate-test in this package specifically
because `apps/mlflow`'s pytest suite is isolated from the real Neon-backed
production tracking store by `tests/conftest.py`'s autouse fixtures, per
this repo's own documented incident history — the same isolation this
session's earlier MLflow work already relied on and verified.)

No new test needed — this is documented, intentional behavior, already
correctly tested; not a bug.

## §6. Bugs C, G, H — pending

`apps/finish-position-cron` is currently deploy-GO-pending. Per task
instructions, orchestrator clearance was requested before any edit
(including temporary mutation-testing edits) to this package. This section
will be completed once clearance is received.

---

## Artifacts

- `apps/finish-position-predict-container/src/predict_upcoming.py` — no
  functional change (mutation-tested only); `tests/test_predict_upcoming.py`
  — 2 new tests (bug A).
- `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py` —
  placeholder-guard fix (bug I);
  `tests/test_finish_position_features_duckdb_integration.py` — 1 new test.
- `apps/mlflow/src/mlflow_tracking/timeline.py` — no functional change
  (mutation-tested only).
- `apps/pc-keiba-viewer/src/scripts/serve_accuracy_report.py` — no
  functional change (mutation-tested only, bugs D/E).
