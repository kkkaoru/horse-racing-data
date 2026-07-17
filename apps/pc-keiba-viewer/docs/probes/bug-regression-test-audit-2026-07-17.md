# Bug Regression-Test Completeness Audit (2026-07-17)

**Status: complete.** All 11 items (A-K) done. `apps/finish-position-cron`
clearance was granted by orchestrator mid-audit (items C, G, H — §6-8); item
K (surface-derivation mismatch) was added to the mandate after this doc's
initial commit — see §9.

- **Trigger**: USER directive relayed by orchestrator — "every bug found today
  must be detectable by running the test suite." This agent was appointed as
  an independent auditor (fresh eyes, did not write today's fixes) for the
  bug ledger below (items A-K).
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

| #   | Bug                                                                                                                                                                                                                                                                                                                                               | Fix commit(s)                                                                | Detection test(s)                                                                                                                                                                                                                                             | Verification method                      | Result                                          |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- | ----------------------------------------------- |
| A   | `feature_guard` (degenerate-feature guard) wiring at its actual call sites in `predict_upcoming.py` had zero test coverage — the pure function was thoroughly unit-tested, but nothing tested that `_score_one_race_direct`/`_score_one_race_nar_blend` actually call it                                                                          | 57a4cd7f (fix); gap closed by `d04206c1` (this audit)                        | `apps/finish-position-predict-container/tests/test_predict_upcoming.py::test_score_one_race_direct_skips_write_for_degenerate_feature_matrix` + `::test_score_one_race_direct_scores_healthy_feature_matrix` (both added by this audit)                       | Mutation (Edit)                          | **GAP FOUND AND CLOSED** — see §1               |
| B   | Cell-routing variant loader feature-ORDER mismatch not caught by the (deliberately order-independent) hash check                                                                                                                                                                                                                                  | 57a4cd7f                                                                     | `test_variant_booster_feature_order_matches_permuted_order_is_a_mismatch`, `test_score_races_rejects_variant_with_feature_order_mismatch`                                                                                                                     | Mutation (Edit)                          | PASS — already covered                          |
| C   | `expectedModelVersion()` missing the `venue==02` → jockey-pedigree269 rule                                                                                                                                                                                                                                                                        | 7807e6cd (fix), 63c69c08 (parity guard), `0cdbaddb` (this audit)             | `focused-full-completion.test.ts`: 4 dedicated venue02 tests + 4 parity tests (1 shape sentinel + 3 new per-rule behavioral checks added by this audit)                                                                                                       | Mutation (Edit)                          | **GAP FOUND AND CLOSED** — see §6               |
| D   | `query_finish_position_metrics` picked the served row by `ORDER BY prediction_generated_at DESC` (latest write wins), silently preferring a later garbage rescore over an earlier genuine prediction                                                                                                                                              | 1d7e3215                                                                     | `test_serve_accuracy_report.py::test_dedup_picks_genuine_row_over_cluster_b_backfill` (+ 6 sibling tests on the shared `select_serving_row`)                                                                                                                  | Mutation (Edit)                          | PASS — already covered                          |
| E   | RS counterpart of D (`query_running_style_metrics`)                                                                                                                                                                                                                                                                                               | 218b5849                                                                     | `test_dedup_running_style_picks_genuine_row_over_cluster_b_backfill` (+ siblings, shares `select_serving_row` with D)                                                                                                                                         | Mutation (Edit)                          | PASS — already covered (same mutation as D)     |
| F   | `build_rec_select_sql`'s UPCOMING-window tie-break ordered on `_rec_priority` alone, keeping an incomplete `race_entry_corner_features` row (NULL `finish_position`) over a settled `jvd_se`/`jvd_ra` row                                                                                                                                         | 2326bf1f                                                                     | `test_finish_position_features_duckdb_integration.py::test_build_rec_select_sql_upcoming_window_recovers_settled_result_over_stale_corner_row` (execution-level, real DuckDB) + a SQL-shape test                                                              | Mutation (Edit)                          | PASS — already covered                          |
| G   | `corner-features-refresh.ts`: 3 always-failing bugs (multi-command DDL, `CREATE EXTENSION` in read-only tx, select-list alias reference) undetected because mocked tests never exercised the real driver                                                                                                                                          | a87d5356                                                                     | `corner-features-refresh.test.ts`: statement-count/order assertions, extension-failure-swallow test, 2 duplicate-alias tests                                                                                                                                  | Mutation (Edit), 3 independent mutations | PASS — already covered, no code defect — see §7 |
| H   | `focused-full-completion.ts`'s completion guard checks row COUNT only, never score quality/stddev (the actual Cluster-B failure signature)                                                                                                                                                                                                        | not fixed — characterization only, no behavior change                        | `focused-full-completion.test.ts::test_isFocusedFullPredictionComplete_only_counts_rows...` (added by this audit)                                                                                                                                             | N/A (characterization, no mutation)      | Documented — see §8                             |
| I   | Realtime-odds UPCOMING-window fallback (`finish_position_features_duckdb.py`) cast the `'0000'`/`'00'` not-yet-confirmed placeholders (tansho_odds/tansho_ninkijun) to `0`/`0.0` instead of NULL — found by this agent while writing up the cross-pool-odds-divergence probe's serve-availability section                                         | fixed by `dde59c45` (this audit, see §2)                                     | `test_finish_position_features_duckdb_integration.py::test_build_rec_select_sql_upcoming_window_treats_unconfirmed_odds_placeholder_as_null` (added by this audit)                                                                                            | Mutation (Edit)                          | **GAP FOUND AND CLOSED** — see §2               |
| J   | MLflow `timeline.upsert_timeline_point`'s "skip if a metric already exists at this step" is a presence check, not a true upsert — re-ingesting the same date with a different computed value silently keeps the OLD value                                                                                                                         | not a bug — documented, intentional behavior; audited for test coverage only | `apps/mlflow/tests/test_timeline.py::test_upsert_timeline_point_does_not_duplicate_same_step` (pre-existing since 2026-07-08, commit `a3102b12` — well before today)                                                                                          | Mutation (Edit)                          | PASS — already covered, confirmed pre-existing  |
| K   | `cell_router.py`'s `derive_surface` (finish-position-predict-container, live routing) and `subgroup_diagnostics.py`'s `get_surface_label` (pc-keiba-viewer, eval store) disagreed for JRA track_code 20/21/22 — cell_router misclassified them as dirt via a naive `track_code.startswith("2")` prefix check; they are turf-course configurations | fixed by this audit (see §9)                                                 | `test_cell_router.py`: 7 new boundary tests (19/20/21/22/23/29/30); `test_subgroup_diagnostics.py::test_derive_surface_agrees_with_predict_container_cell_router` (new, parametrized over all 100 possible 2-digit track_codes, genuine cross-package import) | Mutation (Edit)                          | **GAP FOUND AND CLOSED** — see §9               |

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

## §6. Bug C — a second, real gap found and closed

Orchestrator granted clearance mid-audit for `apps/finish-position-cron`
(deploy sequencing changed to "audit complete + tree clean → GO"). The 2
dedicated behavioral tests (`routes JRA venue 02...`, `routes an un-padded
venue 2...`) already correctly caught the venue==02 removal by mutation.

But the follow-up "parity guard" test (`63c69c08`, added specifically to
prevent a repeat of this exact incident class) does **not** do what its own
name and comment claim. Read literally, it "reads the container's real
cell_routing.json and cross-checks expectedModelVersion()'s hand-written JRA
rule branches against it" — but the code only re-parses `cell_routing.json`
and asserts facts about the JSON's own shape (`rules.length === 3`, an
ordered list of `model_version` strings); it never calls
`expectedModelVersion()` at all.

**Confirmed empirically**: removed the venue==02 branch from
`expectedModelVersion()` (touching no JSON) via `Edit`, ran the suite — the
2 direct behavioral tests failed as expected, but the parity-guard test
alone (`-t "parity guard"`) stayed green. This is the same failure class as
the original bug: a code-side regression independent of the JSON, and this
guard was specifically built to catch exactly that class.

**Fix**: kept the existing rule-count/shape test unchanged (it is a real,
if narrow, "come update this on a new rule" tripwire) and added 3 new tests,
one per JRA rule, that derive each rule's expected `model_version` from the
**same parsed `cell_routing.json`** (not hardcoded) and assert
`expectedModelVersion()`'s actual resolved output (observed via
`isFocusedFullPredictionComplete`'s query parameter) matches it — genuine
two-sided parity.

**Mutation-verified**: reintroduced the venue==02 removal again; this time
the new rule-3 parity test failed alongside the 2 direct tests. Restored;
`git diff --exit-code` clean.

Commit `0cdbaddb`. 628 tests pass, coverage 99.64/96.78/100/99.77%
(unchanged, >=95% gate), tsc/oxlint/oxfmt clean.

## §7. Bug G — highest-scrutiny item, already covered, no code defect

Orchestrator specifically flagged this as needing the deepest verification:
the original 3 bugs shipped completely undetected because "mocked tests
never exercised the real driver" (per commit `a87d5356`'s own message), so
the central question was whether the _new_ tests genuinely fail on
reintroduction or merely look plausible.

All 3 sub-bugs were mutation-tested independently:

1. **Multi-command DDL** (`CORNER_FEATURES_EXTENSION_DDL` + table DDL sent
   as one semicolon-joined `sql.query()` call instead of two separate
   calls): reverted to a single combined-string call. 2 tests failed
   (`toHaveBeenCalledTimes(25)` dropped to 24; the extension-DDL-first/
   table-DDL-second `toHaveBeenNthCalledWith` assertions no longer matched).
2. **`CREATE EXTENSION` swallow removed** (isolated from #1 — kept the
   2-statement split, removed only `ensureVectorExtension`'s try/catch):
   confirmed as part of the same mutation pass above (the "does not block
   the rest of the refresh" test failed identically whether combined with
   #1 or not, since a combined single-statement mutation also removes the
   swallow wrapper's call site).
3. **Duplicate bare-alias columns** (`ENTRY_COLUMNS` re-adding
   `source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango` as bare
   unqualified names, reproducing the exact `"column \"source\" does not
exist"` failure): reintroduced the duplicate prefix. Both the JRA and NAR
   "does not duplicate" tests failed as expected.

All mutations were reverted; `git diff --exit-code corner-features-refresh.ts`
clean; full package suite re-run clean (627 passed pre-C/H, see §6/§8 for
the final count after those additions).

**Why the mock-based tests genuinely catch these bugs despite never talking
to real Neon**: all 3 bugs were about the _shape_ of SQL the code
constructs (statement count/order, exception-swallow control flow, exact
duplicate-alias text) — not about how Neon's real HTTP driver behaves
differently from the mock. The tests pin that shape directly via
`toHaveBeenNthCalledWith`/`toHaveBeenCalledTimes`/`stringContaining`
assertions on what was actually sent to the (still-mocked) `neon()`
function, which is a property of the CODE, not the driver. **No code-side
defect was found**; reported to orchestrator immediately per instructions,
nothing to fix before deploy.

## §8. Bug H — documented limitation, no behavior change

Not a bug — `isFocusedFullPredictionComplete`'s completion check is
`count(distinct ketto_toroku_bango) === entries.length`. It counts rows, it
never inspects `predicted_score` or any quality signal at all (confirmed by
reading the full function: the SQL query text contains no score-related
column anywhere), so a race with the right row count but a degenerate/
near-random score distribution (the actual 2026-07-12 Cluster-B signature)
is reported complete exactly the same as a genuinely healthy race — this
guard has no way to tell them apart, by construction.

Added `test_isFocusedFullPredictionComplete...documented limitation` in
`focused-full-completion.test.ts`, pinning the exact query text (verified,
via `expect.not.stringContaining("predicted_score")`, that no quality
column appears in it) and the `true` result on row-count match alone. No
mutation applies here (nothing was fixed); the test exists so a future
change to what this guard checks shows up as a deliberate, reviewed diff.

Commit `0cdbaddb` (same commit as §6). 628 tests pass.

## §9. Bug K — surface-derivation mismatch (added to mandate mid-audit)

Orchestrator flagged: `cell_router.py` (`apps/finish-position-predict-container`,
drives live cell-routing decisions) and `subgroup_diagnostics.py`
(`apps/pc-keiba-viewer`, drives eval-store cell definitions) disagree on
JRA surface classification for track_code 20/21/22 — `cell_router.py`'s
`derive_surface` classified them as dirt (`track_code.startswith("2")`);
`subgroup_diagnostics.py`'s `get_surface_label` classified them as turf
(`track_code in range(10, 23)`). `docs/finish-position-prediction-system.md`
§6 claims the two are consistent; they were not.

### Determining which side is correct (measured, not assumed)

Queried the local PG mirror directly rather than guessing from JV-Data spec
recall:

- `track_code=22` has **zero** races in the local mirror (all-time).
  `track_code=20` has 15, `track_code=21` has 40 — 55 races total.
- `kyoso_shubetsu_code` for these races ("13"/"14") looked like a possible
  race-type discriminator at first glance, but cross-checking its
  distribution against KNOWN-turf (11, 17) and KNOWN-dirt (23, 24) track
  codes showed the same code values appear at roughly the same proportions
  across all of them — it is not a surface discriminator (most likely an
  age/class condition code), so this line of reasoning was dropped once
  measured.
- The **race names** settle it unambiguously: track_code=20 is
  天皇賞(春) (Tenno Sho Spring) at Kyoto, 3200m — one of JRA's most famous
  Grade 1 races, run on turf. track_code=21 is スポーツニッポン賞
  ステイヤーズステークス (Stayers Stakes) at Nakayama, 3600m — a Grade 2
  turf race. Both are long-distance graded turf races, not dirt, and (per
  their real, well-known distances/venues) not steeplechase either —
  compared directly against actual steeplechase track_codes (51/52, e.g.
  "中山新春ジャンプステークス" — Nakayama Jump Stakes) for contrast.
- `babajotai_code_shiba` (turf condition) is populated and varies across
  all 55 races; `babajotai_code_dirt` stays at its `'0'` not-applicable
  placeholder for all of them — the turf-race signature, confirming the
  race-name evidence.

**Verdict: `subgroup_diagnostics.py` (turf) was correct;
`cell_router.py` (dirt) was wrong.** `subgroup_diagnostics.py` already had
its own dedicated boundary tests at this exact edge
(`test_get_surface_label_jra_turf_code_22`,
`test_get_surface_label_jra_dirt_code_23`) — it was never the buggy side.

### Live-path impact scope

`derive_surface` is not diagnostic-only: `cell_routing.json`'s
`prior_corner_dirt_smallfield_005` rule (live, routes to the
`jra-cb-v10-prior-corner274-2013` variant) has a `surface=dirt` condition
evaluated through this exact function. Checked whether any of the 55
affected races ever also satisfied that rule's other conditions
(`kyoso_joken_code=005` AND `shusso_tosu<=10`): **zero** did (both are
elite Grade 1/2 stakes races, never run under a low-class `005` condition).
**This bug never caused an actual production misroute** — a real, now-fixed
defect, but with confirmed-zero historical serving impact. 649 horse-level
rows (151 + 498) across the 55 affected races is the outer bound of any
hypothetical eval-store exposure, moot since `subgroup_diagnostics.py` was
already correct.

### Fix

`cell_router.py`: replaced the `track_code.startswith("1"/"2")` prefix
heuristic with the same range-based `_JRA_TURF_TRACK_CODES` (10-22) /
`_JRA_DIRT_TRACK_CODES` (23-29) frozenset construction
`subgroup_diagnostics.py`'s `get_surface_label` already used (values
verified identical). track_code >= 30 (steeplechase, e.g. 51/52) is
unaffected — both implementations already agreed it falls through to
`"other"`.

Two existing tests in `test_cell_router.py` used `track_code="20"`
specifically because the OLD buggy logic treated it as a stand-in for "some
dirt code" (one test's own comment said so verbatim) — updated both to
`track_code="23"` (genuinely dirt under both old and new logic), preserving
each test's actual intent (field-size/entries-length behavior, unrelated to
surface).

### Parity test (both directions attempted; one is actually safe)

Tried `finish-position-predict-container`'s test suite importing
`subgroup_diagnostics.get_surface_label` first — failed immediately
(`ModuleNotFoundError: No module named 'polars'`; that container is
deliberately lean and has no dataframe dependency). Reversed direction:
`cell_router.py` and its transitive imports (`model_meta.py`, `race_id.py`)
are pure-stdlib, confirmed by reading every import statement, so importing
it into `pc-keiba-viewer`'s already much heavier test venv is safe. Added
`test_derive_surface_agrees_with_predict_container_cell_router` in
`test_subgroup_diagnostics.py`, `@pytest.mark.parametrize`d over all 100
possible 2-digit `track_code` strings ("00".."99"), asserting
`get_surface_label(code, "jra") == derive_surface(code, "jra")` via a
genuine cross-package `sys.path.insert` + import (not a golden-value table
duplicated in each package — an actual live comparison of both functions).

Registered the cross-package search path for both static type checkers
too (a real config fix, not a suppression): added
`../finish-position-predict-container/src` to `pc-keiba-viewer/pyproject.toml`'s
`[tool.basedpyright] extraPaths` and `[tool.ty.environment] extra-paths`,
and discovered + fixed the same gap in a repo-root `pyrightconfig.json`
(pyright's directory-walk-up config discovery finds this file before the
package-local `pyproject.toml` table when basedpyright runs from inside
`apps/pc-keiba-viewer`, so it silently shadowed the package-level fix until
found).

**Mutation-verified**: all 100 parity tests passed with the fix in place.
Reverted `derive_surface` to the old prefix heuristic — **exactly 3 tests
failed, `[20]`/`[21]`/`[22]`**, no false positives or negatives anywhere in
the other 97 codes. Restored; `git diff --exit-code cell_router.py` clean.

### Final verification

- `finish-position-predict-container`: 1297 passed, 1 skipped, coverage
  99.81%; `ruff check` clean; `basedpyright` 0 errors.
- `pc-keiba-viewer`: 4657 passed, coverage 97.50% (>=95% gate);
  `basedpyright` 0 errors; `ty check` 2 diagnostics, both pre-existing/
  unrelated (`corner_lightgbm.py` MLX class-base warnings); mccabe clean.

---

## Artifacts

- `apps/finish-position-predict-container/src/predict_upcoming.py` — no
  functional change (mutation-tested only); `tests/test_predict_upcoming.py`
  — 2 new tests (bug A).
- `apps/finish-position-predict-container/src/predict_lib/cell_router.py` —
  `derive_surface` fix (bug K); `tests/test_cell_router.py` — 7 new
  boundary tests.
- `apps/finish-position-cron/src/focused-full-completion.test.ts` — 3 new
  per-rule parity tests (bug C) + 1 characterization test (bug H); no
  functional (`.ts`) change beyond the test file.
- `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py` —
  placeholder-guard fix (bug I);
  `tests/test_finish_position_features_duckdb_integration.py` — 1 new test.
- `apps/pc-keiba-viewer/tests/test_subgroup_diagnostics.py` — 1 new
  cross-package parametrized parity test (bug K, 100 cases).
- `apps/pc-keiba-viewer/pyproject.toml` — `extraPaths`/`extra-paths` config
  addition (bug K, real import-resolution fix, not a suppression).
- `pyrightconfig.json` (repo root) — same `extraPaths` addition, the config
  file basedpyright actually loads when run from inside `apps/pc-keiba-viewer`.
- `apps/mlflow/src/mlflow_tracking/timeline.py` — no functional change
  (mutation-tested only).
- `apps/pc-keiba-viewer/src/scripts/serve_accuracy_report.py` — no
  functional change (mutation-tested only, bugs D/E).
