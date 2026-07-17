# Cross-Harness Metric Implementation Parity Audit (Bug Investigation B)

- **Date**: 2026-07-17
- **Trigger**: today's entire campaign relies on top1/place2-6/top3_box/
  fukusho_2p/LB95-bootstrap metric implementations that have been
  independently copy-pasted or reimplemented across multiple harnesses.
  One divergence (`SUMMER_VENUES` missing venue `01`) was already found in
  one copy; this is the systematic audit for others.
- **Method**: a small synthetic "golden dataset" (5 races: clean 8-horse,
  clean 6-horse, clean 18-horse, a dead-heat tie, and a predicted-rank gap)
  fed through each harness's _actual_ functions (not reimplementations of
  them), diffing the outputs directly. Scripts not committed (`tmp/`, per
  repo convention) but retained for reproducibility.

## Harness inventory

| #   | Harness                                                                        | Where the hit-counting logic actually lives                                                                                                                                                                        |
| --- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | `retest_wf.py` (`tmp/candidate-masked-lever-retest/`)                          | Inline, own `per_race_hits`. Also the pattern this agent's own crosspool-divergence/crosspool-level WF scripts copied verbatim earlier today.                                                                      |
| 2   | `score_cells.py` (`tmp/candidate-fp-cells/`)                                   | Imports `_top1_per_race`/`_placeN_per_race`/`_top3_box_per_race` from `learning/subgroup_diagnostics.py` (enforced package).                                                                                       |
| 3   | `dead-lever-retest/architecture/common_eval.py`                                | Inline, own `per_race_hits`.                                                                                                                                                                                       |
| 4   | `serve_accuracy_report.py` (`src/scripts/`, enforced)                          | Own `aggregate_fp_metrics` — different metric _semantics_ (see Finding 3).                                                                                                                                         |
| 5   | `blind_gate_runbook.py` (`tmp/candidate-jra-champion-fresh2026h1-2026-07-17/`) | Imports the same `learning/subgroup_diagnostics.py` functions as #2.                                                                                                                                               |
| 6   | `eval_arch_candidate.py` (`tmp/arch-bakeoff-2026-07-17/eval/`)                 | Imports `per_race_hits`/`block`/`gate_verdict`/`bootstrap_lb95_ub95` from `common_eval.py` (#3) directly — same code, not a separate copy. Already carries its own corrected `SUMMER_VENUES`.                      |
| 7   | `serve_health_check.py` (`src/scripts/`, enforced)                             | **Out of scope** — grepped for `top1`/`place2`/`place3`/`fukusho_2p`/`top3_box`, zero matches. It's a routing/coverage/quality-group tool, a different metric family entirely; nothing to diff against the others. |

So there are really only **3 independent hit-counting implementations**
(`subgroup_diagnostics.py`, `common_eval.py`, `retest_wf.py`'s inline copy)
plus one **deliberately different** one (`serve_accuracy_report.py`), not 7.

## Finding 1 — `SUMMER_VENUES` missing venue `01` (Sapporo) — FIXED

`common_eval.py` line 21: `SUMMER_VENUES = {"02", "03", "10"}`. This is the
issue team-lead had already spotted in one copy. `eval_arch_candidate.py`
had already noticed it independently (its own docstring: _"IMPORTANT
correction vs. common_eval.py: common_eval.SUMMER_VENUES ... is missing
'01' ... it defines its own, correct SUMMER_VENUES below"_) but only
patched around it locally — the source constant in `common_eval.py` itself
was still wrong.

**Fixed** at the source (commit below): `common_eval.py`'s `SUMMER_VENUES`
now includes `"01"`, with a comment explaining why, so any _future_ caller
gets the right set without needing its own local override.

**Impact scope**: three older architecture-family re-test scripts import
this constant for their summer4-restricted gate block —
`eval_pairwise_vs_listwise.py`, `eval_etop2_revival.py`,
`eval_hybrid_a.py` (all `tmp/dead-lever-retest/architecture/`). Their
historical summer4 verdicts were computed without Sapporo races. All three
are from the "architecture-family dead-lever re-tests" (per
`common_eval.py`'s own docstring) and were REJECTed. Not re-run here:
dropping one of four venues from a restriction mask can only _reduce_
statistical power (smaller n), it cannot manufacture a false ADOPT out of
an already-negative aggregate — re-running would need a specific,
unverified claim that Sapporo alone reverses the sign. Flagged as a
residual, not re-verified within this task's scope; a decision on whether
to re-run is left open.

## Finding 2 — `top3_box` dead-heat tie-break diverges by harness — FIXED 2026-07-18 (see Resolution)

Golden-dataset race `tie3` (8 horses, T3 and T4 both `finish_position=3`,
predicted ranks a clean 1..8 permutation with T3 predicted 3rd):

| harness                                                             | `top3_box` |
| ------------------------------------------------------------------- | ---------- |
| `subgroup_diagnostics.py` (→ score_cells.py, blind_gate_runbook.py) | **True**   |
| `common_eval.py` (→ eval_arch_candidate.py)                         | **False**  |
| `retest_wf.py`                                                      | **False**  |
| `serve_accuracy_report.py`                                          | **True**   |

Mechanism: `subgroup_diagnostics._top3_box_per_race` derives the "actual
top 3" set via `.rank("ordinal")` on `finish_position`. Ordinal rank breaks
ties by **row order**, not by treating a shared value as ambiguous — so of
the two horses tied at 3rd, exactly one is arbitrarily admitted to the
"actual top 3" set depending on incidental row order, and here it happens
to be the one that was also predicted 3rd, producing a "hit." `common_eval`
and `retest_wf.py` instead do a direct `finish_position <= 3` count/set
check: a tie at the boundary makes the actual-top3 set 4 members, which can
never equal the exactly-3-member predicted set, so the race is _always_ a
miss when the boundary is ambiguous — a more conservative, order-independent
rule. `serve_accuracy_report.py` diverges for an unrelated third reason (see
Finding 3's mechanism — it checks each of the 3 _predicted_ picks
individually against `actual_rank <= 3`, so a tie doesn't matter as long as
none of the 3 picks themselves lost their spot).

**Not fixed.** `subgroup_diagnostics.py`'s behavior is deliberate and
covered by an existing test
(`test_evaluate_subgroup_top3_box_tie_uses_stable_order`), added docstring
notes to both affected functions instead. Important nuance found while
reading that test: it only asserts the vectorized implementation matches
this module's own scalar-loop reference (`_reference_metrics` in the test
file, which itself calls the same production `compute_race_top3_box`) —
i.e. it's a refactor-safety test ("fast code == slow code"), not an
independent check that stable-order tie-breaking is the _semantically
correct_ way to handle a real dead heat. Given the behavior is intentional,
tested, and lives in an enforced production module, changing it needs a
deliberate decision (and touches the scalar reference too), not a
unilateral fix inside a 60-75min bug audit.

## Finding 3 — `predicted_rank` gap-handling diverges by harness — FIXED 2026-07-18 (see Resolution)

Golden-dataset race `gap_pred` (G3's prediction was filtered upstream —
`predicted_rank=None`; G4's _raw_ `predicted_rank=4` because of the
resulting gap at 3, but G4 actually finishes 3rd):

| harness                    | `place3`                               | `top3_box` |
| -------------------------- | -------------------------------------- | ---------- |
| `subgroup_diagnostics.py`  | **True**                               | **True**   |
| `common_eval.py`           | **False**                              | **False**  |
| `retest_wf.py`             | **False**                              | **False**  |
| `serve_accuracy_report.py` | True (different metric, see Finding 4) | False      |

Mechanism: `subgroup_diagnostics._placeN_per_race` filters out null
`predicted_rank` rows, then **re-ranks the survivors** via
`.rank("ordinal")` before checking "who's in slot N." For this race the
survivors are {1, 2, 4} → re-ranked to ordinal slots {1, 2, 3}, so G4 (raw
rank 4) becomes "the slot-3 pick," and since G4 really did finish 3rd, the
check reports a hit. `common_eval.py`/`retest_wf.py` instead look for a
literal `predicted_rank == 3` value; no row has one (the true gap), so they
report a miss regardless of what anyone's `finish_position` is.

**Not fixed**, same reasoning as Finding 2 (deliberate, tested behavior in
an enforced module — `_top1_per_race`'s twin
`test_evaluate_subgroup_top1_tie_uses_first_in_stable_order` and the
messy-multi-race reference test both exercise gapped `predicted_rank`
inputs already). Added a docstring note. In practice this is unlikely to
bite: `predicted_rank` is always assigned via `.rank(method="ordinal", ...)`
on a continuous score within each race in every harness read today, so it
should already be a clean gap-free 1..N sequence by the time it reaches
these functions — the divergence only matters if some _other_ upstream
step drops individual horse-rows (not whole races) after rank assignment.

## Finding 4 — `serve_accuracy_report.py`'s `place2`/`place3` measure a different thing entirely — DOCUMENTED

Golden-dataset race `clean8` (predicted #1 correctly wins; predicted #2/#3
picks are swapped vs. actual — H2 predicted 2nd finishes 3rd, H3 predicted
3rd finishes 2nd):

| harness                    | `place2` | `place3` |
| -------------------------- | -------- | -------- |
| `subgroup_diagnostics.py`  | False    | False    |
| `common_eval.py`           | False    | False    |
| `retest_wf.py`             | False    | False    |
| `serve_accuracy_report.py` | **True** | **True** |

This is not a bug in either side — it's two genuinely different metrics
sharing a name. The three WF-lever-testing harnesses define `place{k}` as
exact-slot accuracy: "did the horse I predicted to finish *k*th actually
finish *k*th." `serve_accuracy_report.py` (`aggregate_fp_metrics`) instead
tracks whether the single **#1 pick** (`pred_rank == 1`) finished within the
top 2 / top 3 — real 複勝 (fukusho/"place") betting semantics for one
horse, cascaded across thresholds, which is the right question for a
production serve-accuracy dashboard and matches why that harness has no
`place4`/`place5`/`place6` at all (real fukusho betting doesn't go that
deep). `fukusho_2p` is the one metric name that _does_ mean the same thing
everywhere ("any predicted top-2 finished <=2"), confirmed on the same
golden races.

**Documented, not renamed.** Renaming a widely-referenced production
metric name is a much larger, riskier change than this audit's scope
justifies, and both implementations are internally correct for their own
purpose. Added a docstring note to `aggregate_fp_metrics` explicitly
warning against comparing its `place2`/`place3` numbers directly against a
WF-harness `place2`/`place3` figure.

## Bootstrap LB95 implementation comparison (team-lead's step 3)

| harness                  |                 `N_BOOT` | seed                                   | resample unit                   |
| ------------------------ | -----------------------: | -------------------------------------- | ------------------------------- |
| `retest_wf.py`           |                     2000 | `20260519` (hardcoded)                 | race (per-race hit-delta array) |
| `common_eval.py`         |                     2000 | caller-supplied (no hardcoded default) | race                            |
| `eval_arch_candidate.py` | 2000 (via `common_eval`) | `--bootstrap-seed`, default `20260519` | race                            |
| `score_cells.py`         |                     2000 | `42` (hardcoded)                       | race                            |
| `blind_gate_runbook.py`  |                     2000 | `20260717` (hardcoded)                 | race                            |

**Consistent** (the two invariants that actually matter statistically):
`N_BOOT=2000` everywhere, and every harness resamples at **race level**
(bootstrapping the per-race hit-delta array), never at row level — this
matters because races have 5-18 correlated within-race observations, and
row-level resampling would silently break the independence assumption
central to this repo's whole evaluation methodology
(`feedback_harness_sort_before_mask` and friends). No divergence found
here.

**Different but harmless**: three distinct hardcoded seeds
(`20260519`/`42`/`20260717`) across harnesses. With 2000 iterations a
bootstrap CI should be effectively seed-invariant; different seeds produce
Monte Carlo noise, not a systematic bias, so this is not treated as a bug.

## Resolution (2026-07-18, USER decision item 8)

USER decided to unify Findings 2 and 3 rather than leave them as
documented-but-divergent: `subgroup_diagnostics.py`'s `_top1_per_race`,
`_placeN_per_race`, `_top3_box_per_race` (and their scalar test-reference
twins `compute_race_top1`/`compute_race_top3_box`) now use the same literal
`predicted_rank == n` / `<= 3` filtering as `common_eval.py`/`retest_wf.py`,
replacing the `.rank("ordinal")` re-derivation on both the predicted and
actual sides. Net effect: `top3_box` now scores a dead-heat tie at the
boundary as a conservative miss (an ambiguous actual-top3 set can never
equal the exactly-3-member predicted set), and `placeN`/`top3_box` no
longer silently reassign "the Nth pick" around a gap in `predicted_rank`.

**Explicitly out of scope for this change**: genuine _predicted-side_ ties
(e.g. two horses literally sharing `predicted_rank`, as opposed to a gap)
still resolve via first-in-stable-order, since predicted_rank is always
derived from a continuous model score via `.rank(method="ordinal")` in
every harness read during this audit and a real value-duplicate there is
not a realistic occurrence (unlike a genuine finish_position dead heat,
which is a real racing outcome).

**Test updates**: `_reference_placeN` (test-file-local helper) updated to
literal-match, matching the production functions. The two existing
tie-scenario tests were updated in place --
`test_evaluate_subgroup_top1_tie_uses_first_in_stable_order` now asserts
the changed `place2_accuracy` value explicitly (not just self-consistency
against the reference), and
`test_evaluate_subgroup_top3_box_tie_uses_stable_order` was renamed to
`..._predicted_and_actual_tie_at_boundary_still_matches_as_sets` with a
comment explaining why a _symmetric_ tie (both sides reflect the same
ambiguity) is a different case from the asymmetric dead heat this fix
targets. Two new dedicated regression tests were added --
`test_top3_box_per_race_dead_heat_at_boundary_is_conservative_miss` and
`test_placeN_per_race_gap_uses_literal_match_not_reassignment` -- each
mutation-verified by temporarily reverting the corresponding function to
its pre-2026-07-18 `.rank("ordinal")` implementation and confirming the
test fails (`[True] == [False]` in both cases), then restoring.

**Real-data impact scale** (measured against the local PG mirror,
`jvd_se.kakutei_chakujun`, confirmed races only,
`trim(kakutei_chakujun) not in ('', '00')`): a dead-heat tie touching the
top-3 boundary occurs in **0.559% of JRA races** (778 / 139,195) and
**0.485% of NAR races** (482 / 99,297) -- small but not negligible; this is
the population where `top3_box` verdicts can change under the new
semantics. Ban-ei uses a separate table (`nvd_se`, per
`reference_banei_data_location`) not included in this count.
`predicted_rank` gaps were **not independently measured**: this is a
serving-pipeline artifact (a per-horse row dropped between feature-matrix
construction and rank assignment), and every harness read during this
audit assigns `predicted_rank` via `.rank(method="ordinal")` over a whole
race's entrants uniformly -- `feature_guard.py` (today's earlier serving-
incident fix) rejects at whole-race granularity, not per-horse, so a
genuine mid-race gap is a defensive edge case the fix protects against
rather than something currently observed in `cell_training_evaluations`.

**Not retroactive**: historical `cell_training_evaluations` (and any other
already-recorded MLflow/eval-store rows) computed under the pre-2026-07-18
semantics are **not** recomputed by this change. The new semantics apply
from the next re-evaluation onward; a `top3_box_accuracy` figure recorded
before 2026-07-18 may not be bit-for-bit comparable to one recorded after,
specifically for the ~0.5%-of-races population where a boundary tie was
present.

## Verdict

- **2 real bugs fixed**: `common_eval.py`'s `SUMMER_VENUES` (2026-07-17),
  and `subgroup_diagnostics.py`'s `top3_box` tie-handling +
  `predicted_rank` gap-handling, unified to the conservative/literal-match
  side per USER decision (2026-07-18, see Resolution above).
- **1 documented, intentional difference remains**:
  `serve_accuracy_report.py`'s different `place2`/`place3` semantics
  (real fukusho-betting cascading accuracy, not exact-slot accuracy) --
  not a bug in either side, left as-is with a cross-referencing docstring.
- **Bootstrap LB95 mechanics**: consistent on `N_BOOT` and resample unit;
  seeds differ across harnesses but this doesn't affect correctness.
- **`serve_health_check.py`**: confirmed out of scope, doesn't implement
  these metrics.

No new lint-disable/type-ignore/coverage-threshold changes throughout
either the 2026-07-17 audit or the 2026-07-18 resolution. 2026-07-17:
`subgroup_diagnostics.py`/`serve_accuracy_report.py` touched
docstring-only, no behavior change. 2026-07-18: `subgroup_diagnostics.py`
got the real semantic change described in Resolution above (plus a
docstring update to `serve_accuracy_report.py`'s already-documented
Finding 4, unchanged); verified via `uv run basedpyright` (0 errors) and
`uv run pytest` (4659 passed, 97.51% coverage, `subgroup_diagnostics.py`
itself at 100%, no regression) after the 2026-07-18 edits.

## Artifacts

- `tmp/metric-parity-audit-2026-07-17/golden_dataset.py` — the 5-race
  synthetic dataset.
- `tmp/metric-parity-audit-2026-07-17/run_parity_check.py` — imports and
  runs the 4 real implementations against it, prints a diff table.
