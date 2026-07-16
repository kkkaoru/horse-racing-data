# JRA 夏4場 (Sapporo/Hakodate/Fukushima/Kokura) Cell-Level Accuracy Baseline (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position — cell-level baseline ledger for the 2026 summer-4-venue accuracy campaign
- **Goal**: establish a confirmed, measured (not assumed) current-state ledger of cell-level accuracy for venues 01 Sapporo / 02 Hakodate / 03 Fukushima / 10 Kokura, combining (a) a WF blind-backtest ledger for the champion model and (b) an actual 2026-YTD serve-realism check against Neon production data, per docs/finish-position-prediction-system.md §6/§7/§9.
- **Training performed**: **none**. All numbers below come from either (1) predict-only inference using the 9 already-trained CatBoost artifacts in `tmp/candidate-masked-lever-retest/models/base/` (3 seeds × 3 folds, spec-identical clone of live `jra-cb-v9-sim-2013-clean`), (2) an existing cached WF gate output (`tmp/candidate-jra-jockey-pedigree-cell/gate-v9sim-exact/`), or (3) read-only SQL against production Neon.
- **Headline finding (not a cell-quality issue — a serving issue)**: 2026 YTD
  production predictions for these venues include a large share of
  near-random-quality rows, against a healthy WF backtest (§3) and a healthy
  market baseline in the same races. This was reported to team-lead directly
  on discovery (2026-07-17). **Superseded interpretation notice**: this
  probe's own §4 initially attributed the defect to the
  `jockey-pedigree269` model_version specifically (0/29 top1 measured under
  that version). A parallel, more forensic audit
  (`docs/probes/jra-serving-audit-jun-jul-2026-07-17.md`, committed
  concurrently) has since shown the correct decomposition is by **write-time
  cluster, not model_version**: a healthy cluster (2026-07-11 10:47 JST,
  score stddev ~1.08, top1 42.86%) and a degraded cluster (2026-07-12
  14:51-14:52 JST, score stddev ~0.10, top1 8.33%) each contain a mix of
  `jra-cb-v9-sim-2013-clean` and `-jockey-pedigree269` rows performing
  equally (badly, or well) within their own cluster — i.e. **both
  model_versions are equally affected**, not jockey-pedigree269 specifically.
  §4 below is kept as originally measured (the raw numbers are accurate) with
  an added note at 4.3 pointing to the corrected attribution; treat the
  sister doc as authoritative for root-cause framing and this doc as
  authoritative for the WF cell-level baseline (§2/§3, unaffected by this
  correction) and the raw 2026 numbers themselves.

---

## 1. Methodology

### 1.1 Cell definition and metrics

Cell key follows docs/finish-position-prediction-system.md §6.1:
`category × surface × distance_band × class_label × season × venue`. This
probe fixes `category=jra` and reports the venue-anchored subsets team-lead
asked for: **venue**, **venue×surface**, **venue×distance_band**,
**venue×class_label**, **venue×surface×distance_band**. `surface` /
`distance_band` / `class_label` (= `grade_code`, not `kyoso_joken_code`) /
`season` are derived via `src/scripts/learning/subgroup_diagnostics.py`'s
canonical expressions (`_surface_expr`, `_distance_band_expr`, `_class_expr`,
`_season_expr`) — the same module `score_cells.py` uses — so cell boundaries
are identical to every other cell claim in this campaign. Metrics are
top1/place2/place3/place4/place5/place6/top3_box (doc §7.1); `n>=200` races is
the WF adoption-grade threshold (doc §6.3/§7.3) and is enforced for every WF
cell reported below. 2026 serve-side samples are far smaller (§4) and are
explicitly **not** held to n>=200 — they are reported as descriptive current-
state numbers, same convention as `jra-summer-2026-clean-serve-eval-2026-07-04.md`.

**Market baseline**: `tansho_ninkijun` (post-time favorite rank) is used as a
full predicted-rank column, not just a favorite/non-favorite split — i.e.
market's "place3" claim is "the 3rd-favorite finished 3rd", matching the
model's own metric definitions exactly. This mirrors the convention already
used in `tmp/candidate-2026-summer-serve-eval/fetch_score_eval.py`
(`market_rank = tansho_ninkijun_raw`).

### 1.2 Sort-before-mask compliance (doc §7.3 / anti-pattern #11)

The WF ledger script (`champion_ledger.py`, written for this probe) does
**not** use a positional numpy boolean mask against a separately-sorted frame
at all — the exact failure mode documented in the 2026-07-04 incident. Every
cell slice is produced by `.filter(pl.col(dim) == v)` independently on the
model-hits and market-hits frames (both keyed by `race_id`, both carrying the
same `dims` join), and the paired-bootstrap comparison joins on `race_id`
(key-based, never positional). This sidesteps the row-order hazard
structurally rather than relying on sort-then-positional-mask ordering.

### 1.3 Cached assets reused (no new training)

| Asset                                                                                                           | Used for                                                                                                                            |
| --------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `tmp/candidate-masked-lever-retest/models/base/seed{42,101,2026}/fold-{2023,2024,2025}/model.json`              | Champion 3-seed × 3-fold WF ledger (§3) — **predict-only**, all 9 artifacts pre-existed                                             |
| `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` (`armB`, 250 feat)                               | Feature list for the above (confirmed identical count to `model_meta.json`'s `jra: 250`)                                            |
| `tmp/candidate-eval-jra/augmented/**/*.parquet` (hive-partitioned by `race_year`)                               | WF store, partition-pruned to `race_year in (2023,2024,2025)` and `keibajo_code in ('01','02','03','10')`                           |
| `tmp/candidate-jra-jockey-pedigree-cell/gate-v9sim-exact/{base_preds,cand_all_preds,cell_routing_full}.parquet` | Venue02 routed-arm (jockey-pedigree269) WF delta vs champion, single-seed, all-JRA 3-fold WF (§3.4) — reused as-is, not regenerated |
| Production Neon (`NEON_PRIMARY_URL`, read-only)                                                                 | 2026 YTD serve reality (§4) — `race_finish_position_model_predictions` × `jvd_se` / `jvd_ra`                                        |

New scripts written for this probe (not committed, per tmp/ convention):
`tmp/candidate-jra-summer4-cell-baseline-2026-07-17/{champion_ledger.py, sapporo_perfold_check.py, serve_2026_eval.py}` (+ JSON outputs).

### 1.4 jvd placeholder handling

`kakutei_chakujun` predicate is `trim(col) ~ '^[0-9]+$' AND CAST(col AS int) > 0`
(identical to `src/scripts/serve_accuracy_report.py`'s proven query) — this
correctly excludes the non-NULL `'00'` placeholder used for unsettled rows
(memory `reference_jvd_placeholder_semantics`).

---

## 2. WF champion baseline ledger (3-seed × 3-fold blind, 2023-2025, summer 4 venues)

Champion = spec-identical clone of live `jra-cb-v9-sim-2013-clean` (CatBoost
YetiRank, 250 leak-free features, iterations=300/depth=8/lr=0.05/l2=3.0).
n=2448 races total (01: 504, 02: 432, 03: 720, 10: 792 — **exact match** to
the independently-computed `cell_routing_full.parquet` population, a useful
internal consistency check that both harnesses are drawing from the same
corpus). Delta convention: **model − market**, paired bootstrap LB95/UB95
(2000 iters).

### 2.1 Pooled (all 4 summer venues, n=2448)

| Metric   | Model  | Market | Delta  | LB95   | UB95   |
| -------- | ------ | ------ | ------ | ------ | ------ |
| top1     | 32.108 | 32.271 | −0.163 | −1.008 | +0.640 |
| place2   | 16.217 | 16.503 | −0.286 | −1.212 | +0.613 |
| place3   | 13.508 | 13.235 | +0.272 | −0.736 | +1.334 |
| place4   | 11.560 | 11.683 | −0.122 | −1.022 | +0.762 |
| place5   | 11.234 | 11.070 | +0.163 | −0.640 | +0.967 |
| place6   | 10.063 | 10.008 | +0.054 | −0.776 | +0.899 |
| top3_box | 8.456  | 8.333  | +0.122 | −0.286 | +0.504 |

Pooled summer-4 is statistically indistinguishable from market on every
metric (all LB95<0<UB95) — the champion neither dominates nor loses to market
in aggregate at these venues. This matches `project_accuracy_stagnation_root_cause_2026_07_11`'s
"market null upper-bound already exceeded" finding: no free lunch at the
pooled level.

### 2.2 By venue (top1 / place2 / place3, `delta[LB95,UB95]`)

| Venue        | n   | top1                       | place2                 | place3                 |
| ------------ | --- | -------------------------- | ---------------------- | ---------------------- |
| 01 Sapporo   | 504 | **−2.910 [−4.762,−1.124]** | −0.926 [−3.108,+1.124] | +0.066 [−2.052,+2.183] |
| 02 Hakodate  | 432 | +1.698 [−0.156,+3.704]     | +0.617 [−1.466,+2.778] | −1.620 [−3.858,+0.463] |
| 03 Fukushima | 720 | +0.648 [−0.741,+1.991]     | −0.417 [−2.315,+1.343] | +0.648 [−1.298,+2.639] |
| 10 Kokura    | 792 | −0.168 [−1.599,+1.305]     | −0.253 [−1.936,+1.389] | +1.094 [−0.631,+2.736] |

**Sapporo (01) top1 is the only venue-level cell with UB95<0 — a robustly
negative delta vs market** (bold). No other venue/metric combination in this
table clears significance in either direction.

Full place4/place5/place6/top3_box for Sapporo (for completeness — this
weakness is top1-specific, not a general ranking failure):

| Metric   | Delta  | LB95   | UB95   |
| -------- | ------ | ------ | ------ |
| place4   | −0.529 | −2.712 | +1.720 |
| place5   | +1.190 | −0.860 | +3.307 |
| place6   | −0.926 | −2.712 | +0.860 |
| top3_box | +0.132 | −0.595 | +0.860 |

### 2.3 Finer cuts (n≥200 only)

`venue×surface`:

| Cell      | n   | top1 delta [LB95,UB95]     |
| --------- | --- | -------------------------- |
| 01 × dirt | 207 | **−3.543 [−6.924,−0.483]** |
| 01 × turf | 297 | **−2.469 [−4.826,−0.449]** |
| 02 × turf | 255 | +1.830 [−0.526,+4.314]     |
| 03 × dirt | 288 | −0.463 [−3.125,+2.083]     |
| 03 × turf | 379 | +1.495 [−0.352,+3.342]     |
| 10 × dirt | 265 | −1.384 [−3.774,+1.132]     |
| 10 × turf | 449 | +0.742 [−1.188,+2.598]     |

(02 × dirt and 10 × dirt's obstacle/other-surface complements fall below n=200 and are omitted; no venue has an "other"-surface cell ≥200.)

`venue×distance_band` (only `intermediate` clears n≥200 for any venue — sprint/mile/long/extended all fragment below threshold once venue-restricted):

| Cell              | n   | top1 delta [LB95,UB95]     |
| ----------------- | --- | -------------------------- |
| 01 × intermediate | 215 | **−3.721 [−6.822,−0.775]** |
| 03 × intermediate | 269 | −0.372 [−3.098,+2.354]     |
| 10 × intermediate | 295 | −0.791 [−2.938,+1.469]     |

`venue×class_label` (grade_code): only `unknown` (non-graded/ordinary
conditions racing) clears n≥200 at any venue — see §2.4 for why.

| Cell         | n   | top1 delta [LB95,UB95]     |
| ------------ | --- | -------------------------- |
| 01 × unknown | 365 | **−2.740 [−4.840,−0.820]** |
| 02 × unknown | 321 | +1.765 [−0.311,+3.949]     |
| 03 × unknown | 537 | +1.055 [−0.683,+2.855]     |
| 10 × unknown | 584 | −0.228 [−1.998,+1.484]     |

`venue×surface×distance_band`: **0 cells clear n≥200** — the 3-way cross
fragments the ~2000-race summer-4 population too finely. Not reportable at
gate-grade sample size; not included.

### 2.4 Why `class_label` is mostly "unknown" here

`grade_code` is blank for 1807/2448 (74%) of summer-4 races (`_class_expr`
maps blank → `"unknown"`); only E (tokubetsu, 577), C (46), L (13), B (3), H
(2) carry an actual code. This is expected JRA domain semantics — most races
run at these venues are ordinary (non-graded) conditions races, and
`grade_code` is populated only for graded/tokubetsu-labeled races — **not** a
data quality gap. `class_label` is consequently a low-information axis for
these venues specifically; `cell_routing.json`'s actual routing dimension for
the jockey-pedigree269 variant is the (much finer, much more common)
`kyoso_joken_code` condition code, a different field entirely (see §3).

### 2.5 Per-fold robustness check on Sapporo (is −2.9pp one bad year?)

Because Sapporo's venue-level top1 delta is the only robust finding in the WF
ledger, it was checked per blind fold-year (`sapporo_perfold_check.py`,
n=168/fold, same 9 cached models):

| Fold | n   | top1 delta | LB95   | UB95   |
| ---- | --- | ---------- | ------ | ------ |
| 2023 | 168 | −2.976     | −5.759 | −0.595 |
| 2024 | 168 | −2.778     | −5.952 | +0.198 |
| 2025 | 168 | −2.976     | −6.746 | +0.397 |

All three independent blind years land within 0.2pp of each other
(−2.976/−2.778/−2.976) — remarkably stable, well outside the documented
±0.4pp single-arm retraining noise floor (`project_training_noise_floor_2026_07_11`).
2023 alone clears UB95<0; 2024/2025 don't individually (n=168 is under the
n≥200 gate line) but point the identical direction with near-identical
magnitude. Combined with the pooled n=504 result being robust (§2.2), this
reads as a **genuine, persistent, venue-specific top1 weak spot**, not a
single-year artifact — see §6 for the "weak cell = mirage" caution and why
this one clears the bar anyway.

---

## 3. Venue02 (Hakodate) routed-arm: jockey-pedigree269 vs champion (WF)

`cell_routing.json` routes **all** Hakodate races (`venue==02`, regardless of
`kyoso_joken_code`) plus any JRA race with `kyoso_joken_code==703` (any
venue) to `jra-cb-v9-sim-2013-clean-jockey-pedigree269` (269 feat). This is
the model_version actually live at Hakodate today — reused from the existing
cached gate (`gate-v9sim-exact/`, single-seed, all-JRA 3-fold WF blind
2023-2025, n=10,365 races pooled JRA-wide). **Not regenerated** — this is
exactly the cached asset the task named.

Pooled JRA-wide (n=10,365, all venues — the original ADOPT evidence basis, single-seed):

| Metric | Delta   | LB95    |
| ------ | ------- | ------- |
| top1   | +0.2605 | −0.0482 |
| place2 | +0.0772 | −0.2991 |
| place3 | +0.0000 | −0.4245 |
| place6 | +0.3956 | +0.0480 |

`kyoso_joken_code=703` cell (n=3710, the actual ADOPT basis per doc §2): top1
+0.782pp [LB95 +0.270] — robust, this is why the rule exists.

**Venue=02 alone (n=432, the routing rule's actual footprint at Hakodate)**:

| Metric   | Delta  | LB95   | UB95     |
| -------- | ------ | ------ | -------- |
| top1     | +0.926 | −0.463 | +3.704   |
| place2   | +0.694 | −1.389 | +4.183\* |
| place3   | +3.009 | +1.157 | —        |
| place4   | −0.463 | −2.315 | —        |
| place5   | +0.000 | −2.083 | —        |
| place6   | −0.231 | −1.852 | —        |
| top3_box | +0.000 | −0.926 | —        |

_(place2 UB95 not separately re-extracted; not material to the point below.)_

**Only place3 is independently robust at venue=02 alone (LB95>0); top1/place2/top3_box
all cross zero.** The venue==02 rule was adopted as an _extension_ of the
robust 703-cell finding (§ doc 2.4: "kyoso_joken_code=703 → jockey_pedigree_703,
さらに venue==02 全レース → 同 variant"), not independently re-gated at n=432 —
worth stating plainly since it means Hakodate's routing is riding on the
broader 703-class evidence, not a standalone Hakodate-specific win. This
**pre-existing, already-adopted** state is unaffected by anything in this
probe; it is reported here purely as the "what does WF say the effective
Hakodate baseline should be" reference point for §4.

---

## 4. 2026 YTD serve reality (Neon, read-only)

### 4.1 Scope and a methodology trap found and fixed

- **Sapporo (01): zero jvd_ra rows for kaisai_nen=2026** as of this run —
  confirmed against raw results, not just the predictions table. The 2026
  meet has **not started yet**; this is not a serving gap.
- **Fukushima (03) and Kokura (10) each ran an earlier, non-summer meet in
  2026** under `kaisai_kai='01'` (Fukushima: spring, 0411-0426; Kokura:
  winter, 0124-0301) before their summer meet (`kai='02'`, both 0627-0712).
  These were excluded via a `jvd_ra` kaisai_kai join — mixing them in would
  have silently pulled in different-model-version, different-season history.
  Hakodate has only `kai='01'` in 2026 so far (0613-0712) and needed no
  filter.
- **Backfill-contamination trap (found while building this)**: the obvious
  query pattern — `DISTINCT ON (horse) ... ORDER BY prediction_generated_at
DESC` (i.e. "latest wins"), which is what
  `src/scripts/serve_accuracy_report.py::query_finish_position_metrics` uses
  — silently picks up **post-race backfill/rescore rows** instead of the
  genuine pre-race serve prediction whenever a race was rescored after it
  ran. Confirmed directly: Kokura race 0711/01 (already completed) has a
  `jra-cb-v9-sim-2013-clean` row generated 2026-07-11 10:47 JST (pre-race,
  one identical timestamp shared by the whole day's card → a single morning
  batch write) **and** a `jra-cb-v9-sim-2013-clean-jockey-pedigree269` row
  generated 2026-07-12 14:52 JST — a full day after that race concluded, i.e.
  a retroactive backfill (`first_served_at` was checked as a disambiguator
  but is NULL on every row in this dataset, so it can't be used instead).
  Every horse in every pre-07-12 race checked shows this same two-row
  pattern. Taking DESC/latest produced a nonsensical top1 rate (~9-13pp,
  worse than random) before this was caught and fixed to
  `ORDER BY ... ASC` (earliest = genuine pre-race row). **Any future serve
  eval reusing `serve_accuracy_report.py`'s query pattern on a date range
  that includes a post-hoc backfill should re-check this.**
- Model*version restricted to the three current champion-family versions
  (`jra-cb-v9-sim-2013-clean` / `-jockey-pedigree269` / `jra-cb-v10-prior-corner274-2013`);
  `win5-xgb-v7-lineage-v1-rs-overlay-*` (a different pipeline, WIN5-leg-race
scoped) and legacy versions seen dominating \_other* (non-summer) JRA venues'
2026 predictions (`iter14-jra-cb-pacestyle-course-v8` et al. — healthy
  38-48% top1 there, beating market, so evidently a real and currently-active
  parallel path, just out of this campaign's scope) are excluded rather than
  silently blended in.

### 4.2 Coverage gap (separate from the accuracy finding below)

Within the correctly-scoped summer-meet window, a large share of races never
received **any** finish-position prediction row (any model_version) in Neon:

| Venue | Races run (jvd_ra) | Races w/ ≥1 prediction row | Races w/ champion-family row |
| ----- | ------------------ | -------------------------- | ---------------------------- |
| 02    | 120                | 37 (31%)                   | 25 (21%)                     |
| 03    | 72                 | 36 (50%)                   | 24 (33%)                     |
| 10    | 72                 | 37 (51%)                   | 25 (35%)                     |

This is reported descriptively; root cause (pipeline trigger completeness,
timing of this snapshot vs daily cron, etc.) was not investigated — flagged
as a fact worth someone's attention, not diagnosed.

### 4.3 Accuracy by model_version × venue (earliest-per-horse, champion family only)

| model_version                               | Venue | n   | Model top1 | Market top1 | Delta         |
| ------------------------------------------- | ----- | --- | ---------- | ----------- | ------------- |
| jra-cb-v9-sim-2013-clean                    | 02    | 12  | 41.67%     | 41.67%      | 0.00          |
| jra-cb-v9-sim-2013-clean                    | 03    | 19  | 10.53%     | 42.11%      | −31.58        |
| jra-cb-v9-sim-2013-clean                    | 10    | 20  | 15.00%     | 25.00%      | −10.00        |
| jra-cb-v9-sim-2013-clean-jockey-pedigree269 | 02    | 12  | **0.00%**  | 16.67%      | −16.67        |
| jra-cb-v9-sim-2013-clean-jockey-pedigree269 | 03    | 5   | **0.00%**  | 20.00%      | −20.00        |
| jra-cb-v9-sim-2013-clean-jockey-pedigree269 | 10    | 5   | **0.00%**  | 40.00%      | −40.00        |
| jra-cb-v10-prior-corner274-2013             | 02    | 1   | 0.00%      | 100.00%     | (n=1, ignore) |

**jockey-pedigree269: 0/29 (0%) top1 across every venue currently routing to
it in 2026** — market is healthy in the same races (16.7-40%), and WF says
this variant should be roughly market-neutral-to-positive (§3). `predicted_score`
was checked for degeneracy (a plausible "NULL features collapse to a near-
constant score" explanation) and rejected: scores are well-spread and
monotonically ordered, matching a normal CatBoost YetiRank output shape — the
model is producing a **confident, differentiated, but apparently
uncorrelated-with-outcome** ranking, which points more toward a
feature-content or feature-alignment problem than a missing-feature/NaN
collapse.

**Correction (added after this section was first written)**: the table above
groups by `model_version`, which makes the defect look
`jockey-pedigree269`-specific — it isn't. `jra-serving-audit-jun-jul-2026-07-17.md`'s
finer decomposition by `prediction_generated_at` write-cluster shows the
plain champion rows in this same table are themselves a mix of a healthy
write-cluster (2026-07-11 10:47 JST, an unrouted Mac-batch-fallback write,
top1 42.86% pooled) and a degraded write-cluster (2026-07-12 14:51-14:52 JST,
top1 8.33% pooled, both model_versions equally affected within it — their
pooled Cluster-B breakdown: champion n=30 top1=3.33%, jockey-pedigree269
n=42 top1=9.52%). Hakodate's 12/12 healthy champion rows here are entirely
Cluster-A; Hakodate's 12/12 zero-hit jockey-pedigree269 rows here are
entirely Cluster-B (the Mac-batch fallback used a stale image with no cell
routing, so it only ever wrote the plain champion version — never
jockey-pedigree269 — which is why Cluster A shows up as 100% champion in this
venue's table row). Fukushima/Kokura's champion rows in this table are a
blend of a few genuine Cluster-A rows and many Cluster-B rows, which is why
they read as intermediate (10.53%/15.00%) rather than either extreme. **Root
cause (why Cluster B's inputs are degraded) was not investigated further by
this probe — see the sister doc for the full forensic breakdown and code-level
findings, and §5 below.**

### 4.4 Pooled 2026 serve (all venues, all champion-family rows blended — for reference only)

| Metric   | Model  | Market | Delta  | n   |
| -------- | ------ | ------ | ------ | --- |
| top1     | 13.51% | 32.43% | −18.92 | 74  |
| place2   | 13.51% | 22.97% | −9.46  | 74  |
| place3   | 9.46%  | 9.46%  | 0.00   | 74  |
| top3_box | 1.35%  | 2.70%  | −1.35  | 74  |

This pooled number is presented **only for completeness** — per §4.3's
correction it is a blend of a healthy write-cluster and a degraded
write-cluster (both spanning both model_versions) and should not be read as
"the champion model is currently 19pp worse than market"; it is "whatever
produced the 07-12 14:51-14:52 JST write-cluster is dragging the blended 2026
number down, independent of which model_version got routed."

---

## 5. Escalation record

Reported directly to team-lead via SendMessage on 2026-07-17 upon discovery
(both the jockey-pedigree269 0/29 finding and the `serve_accuracy_report.py`
DESC/backfill trap), before this doc was written, given the apparent
production-impact urgency. No remediation was attempted by this probe
(read-only measurement task; data mutation and code changes to the
container/routing were out of scope and are not proposed here).

A separate, more forensic audit landed concurrently as
`docs/probes/jra-serving-audit-jun-jul-2026-07-17.md` (commit `b58a2851`),
independently confirming and substantially extending this finding: precise
write-cluster decomposition (§4.3 above), a 58% total coverage gap across
the 3 venues (154/264 confirmed races have zero prediction rows at all,
separate from the accuracy defect), a code-level root-cause candidate for a
related coverage-self-heal false-positive bug
(`focused-full-completion.ts::expectedModelVersion()` missing the
`venue==02` cell-routing rule), and a documented reason for not attempting
backfill. Follow-on root-cause/fix work for the write-cluster defect itself
is being tracked separately (not owned by this probe).

---

## 6. Weak-cell candidates (n≥200 WF basis, LB95/UB95-qualified)

Per the "weak cell = mirage" lesson (`project_accuracy_stagnation_root_cause_2026_07_11`):
most cells that look weak at a glance are n-starved noise. The list below is
restricted to cells whose **UB95 < 0** (i.e. robustly, not just
point-estimate, worse than market) — the mirror-image of the campaign's usual
LB95>0 ADOPT bar.

| Cell                                  | n   | Metric | Delta  | UB95   | Confidence                                                       |
| ------------------------------------- | --- | ------ | ------ | ------ | ---------------------------------------------------------------- |
| venue=01 (Sapporo)                    | 504 | top1   | −2.910 | −1.124 | High — robust pooled + 3/3 folds same direction/magnitude        |
| venue=01 × surface=dirt               | 207 | top1   | −3.543 | −0.483 | High — consistent with venue-level                               |
| venue=01 × surface=turf               | 297 | top1   | −2.469 | −0.449 | High — consistent with venue-level                               |
| venue=01 × distance_band=intermediate | 215 | top1   | −3.721 | −0.775 | High — consistent with venue-level                               |
| venue=01 × class_label=unknown        | 365 | top1   | −2.740 | −0.820 | High — consistent with venue-level (dominant subgroup, see §2.4) |

All five rows are the **same underlying signal** viewed through different
cuts of the same venue (Sapporo top1 specifically; place2-6/top3_box are
NOT robust, see §2.2) — this is one finding, not five independent ones.
**No other venue or cell in the WF ledger clears UB95<0 on any metric.**

The 2026-serve-side jockey-pedigree269 finding (§4.3) is **not** listed in
this WF-basis table because it is a serve/routing-layer phenomenon, not a WF
cell-accuracy phenomenon — WF says this variant is fine-to-good (§3); it is
2026 production specifically that is broken. Conflating the two would
misattribute a serving defect as a modeling weakness.

---

## 7. Caveats

- The champion WF ledger (§2) is **3-seed averaged**; the venue02 routed-arm
  comparison (§3) reused from `gate-v9sim-exact/` is **single-seed** — stated
  explicitly rather than silently blended; the two are not directly
  comparable in precision, only in direction/magnitude.
- 2026 serve samples (§4) are small (5-25 races per cell) and **not** held to
  the n≥200 gate — read as descriptive current-state, not an adoption-grade
  claim, except where noted (the 0/29 jockey-pedigree269 finding, whose
  improbability under a healthy-model null is large enough to stand on its
  own despite small n).
- `venue×surface×distance_band` (the finest requested WF cut) has zero cells
  clearing n≥200 for the summer-4 population — reported as a limitation, not
  papered over with a sub-200 table.
- Season was not used as an additional WF cut (team-lead's requested
  granularities are venue-anchored, and these 4 venues' race population is
  already 58% `summer`-banded by the campaign's own season derivation, with
  spring/autumn/winter as calendar-month spillover from meet boundaries
  rather than a distinct racing condition) — full season distribution:
  summer 1428 / autumn 300 / winter 456 / spring 264 (n=2448).

---

## 8. Artifacts

- `apps/pc-keiba-viewer/tmp/candidate-jra-summer4-cell-baseline-2026-07-17/champion_ledger.py` — WF 3-seed ledger script (predict-only)
- `apps/pc-keiba-viewer/tmp/candidate-jra-summer4-cell-baseline-2026-07-17/champion_ledger.json` — full WF output (all cells, all metrics, all LB95/UB95)
- `apps/pc-keiba-viewer/tmp/candidate-jra-summer4-cell-baseline-2026-07-17/sapporo_perfold_check.py` + inline output (§2.5)
- `apps/pc-keiba-viewer/tmp/candidate-jra-summer4-cell-baseline-2026-07-17/serve_2026_eval.py` — 2026 Neon serve-reality script (read-only)
- `apps/pc-keiba-viewer/tmp/candidate-jra-summer4-cell-baseline-2026-07-17/serve_2026_eval.json` — full 2026 output
- Reused unchanged: `apps/pc-keiba-viewer/tmp/candidate-masked-lever-retest/models/base/**/model.json` (9 artifacts), `apps/pc-keiba-viewer/tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`, `apps/pc-keiba-viewer/tmp/candidate-jra-jockey-pedigree-cell/gate-v9sim-exact/*.parquet`
- Neon access: root `.env` `NEON_PRIMARY_URL`, read-only, no writes, no deletes attempted
