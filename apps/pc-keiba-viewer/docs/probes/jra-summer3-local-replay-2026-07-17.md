# JRA 夏3場 (Hakodate/Fukushima/Kokura) 264-Race Local Serve-Parity Replay (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position — local reproduction of production-identical scoring for the 264 confirmed 2026-06-01..07-12 races at venues 02/03/10, per user instruction relayed by team-lead (follow-on to `jra-summer4-cell-baseline-2026-07-17.md` and `jra-serving-audit-jun-jul-2026-07-17.md`).
- **Goal**: (1) locally reproduce the same cell-routed predictions the Cloudflare Container would genuinely serve (no Neon writes — local analysis only), bypassing the write-cluster serving defect documented in the sister audit; (2) compare routed-vs-champion-only, both-vs-market; (3) validate the reproduction against the one confirmed-healthy production sample (the 2026-07-11 Mac-batch, "Cluster A").
- **v1 headline (SUPERSEDED, kept below for the audit trail — see §8 for the current answer)**: the reproduction pipeline was built and fully scores all 264 races, and shows a **robust, positive routed-vs-champion-only effect** (pooled top1 +4.17pp [LB95 +1.89]), replicated at all 3 venues individually (venue02 +5.0pp [LB95 +1.67], venue03 +4.17pp [LB95 0.00], venue10 +2.78pp [LB95 0.00]). **But** a parity check against the one known-genuinely-healthy production sample (07-11 Mac-batch, 21 races, plain champion) found only **weak agreement** (mean Spearman ρ=0.30, top1 exact-match 4.8%, mean |rank diff| 3.7) — i.e. this reproduction captures directional signal but is **not a faithful bit-exact replica** of true serve-time scoring. Every number in §1-§7 below should be read with that caveat.
- **★ v2 headline (CURRENT, §8)**: the two gaps §5 identified are now closed — (a) the `finish_position_features_duckdb.py` row-priority bug is fixed (commit `2326bf1f`) and (b) all 18 previously-missing champion-family features are now built (h2h layer rerun at 12GB memory, a `grade_race_lineage` layer added ahead of `trainer` to supply `target_race_id`). **Parity against Cluster A is now strong**: Spearman ρ=0.30→**0.93**, top1 exact-match 4.8%→**76.2%**, mean |rank diff| 3.72→**0.91**. With genuinely complete features, **the v1 routing effect mostly evaporates**: pooled routed-vs-champion-only top1 is now +0.76pp [LB95 −0.76, does not clear 0], and **no venue or cell (surface/distance_band/class_label) shows a robust (LB95>0 or UB95<0) result on any of rank1-6** at n=264. Per the 2026-07-17 user instruction relayed after v1, §8's tables are cell × rank1-6 first, pooled numbers are reference-only.

---

## 1. Methodology

### 1.1 Feature build

`src/scripts/finish_position_features_duckdb.py` — the same per-race builder
the Cloudflare Container itself uses (doc §3.1) — run against **local
PostgreSQL** (`127.0.0.1:15432`, Apple container runtime instance, confirmed
via `pg_postmaster_start_time()`/`inet_server_addr()` per the dual-postgres
trap memory; confirmed current through 2026-07-12). Base build:
`--category jra --target-date 20260613 --days-ahead 29` (one invocation,
27-30s, 4788 rows / 360 races / 5 venues; venues 02/03/10 alone: 120/72/72
races — **exact match** to the live-audit ledger).

### 1.2 Enrichment layers

The base builder alone only supplies 116/250 of the champion's declared
features (running-style/pacestyle, market/odds, futan, similar-race and
other families are separate post-processing layers per doc §3's mermaid
chain). Reused and extended the prior session's asset
`tmp/candidate-jra-2026-scoring/eval/build_full_harvest.py` (validated
2026-07-04, covered 0613-0628 only) into
`tmp/candidate-jra-summer3-local-replay-2026-07-17/build_harvest_layers.py`,
adding the layers that asset's own `base_fixed` already had pre-baked
(race*internal, market, course, class) plus the champion-specific ones
(similar_race for `sim*\*`, exotic_odds, jockey_pedigree_cell for the 269
variant's 19 extra columns) — 15 layers total + an inline pacestyle join
(running-style predictions × field-pressure interaction terms, matching the
prior asset's proven SQL). **13/15 layers succeeded**:

| Layer                                                                                                                                     | Status   | Note                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ----------------------------------------------------------------------------------------------------------------------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| race_internal, market, course, class, kohan3f, baba, futan, nearmiss, workout, sectional, similar_race, exotic_odds, jockey_pedigree_cell | OK       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| h2h                                                                                                                                       | **FAIL** | DuckDB OOM even at `--memory-limit 4GB` explicit override (self-join over 13y pairwise race history). 6 features lost (all `h2h_*`).                                                                                                                                                                                                                                                                                                                                             |
| trainer                                                                                                                                   | **FAIL** | References `target_race_id`, which this build's base output doesn't carry (that column comes from a lineage layer not built here). ~19 `trainer_*`/`sim_trainer_*` features lost, though most `trainer_*` core features come from a _different_, successful layer — the loss is specifically `trainer_grade_career_starts` / `trainer_grade_top3_rate` / `trainer_target_race_*` (4 cols) + the 6 `target_grade_trial_*` cols (a separate, apparently lineage-dependent family). |

Net: **18/250 champion features (7.2%) are NULL** in this reproduction
(CatBoost handles NaN as missing natively, same as any serve-time gap) —
`h2h_*` (6) + `target_grade_trial_*` (6) + `trainer_grade_*`/`trainer_target_race_*` (6).
jockey-pedigree269's own 19 extra columns are **fully present** (built
successfully) with non-null rates 78.0-99.9%, except `keito_dist_surface_win_eb`
at **0.18%** (near-total NULL — flagged for `serve-defect-269`'s root-cause
work per team-lead's request, §6).

### 1.3 A base-builder date-specific bug found and worked around

Initial runs showed `finish_position` NULL for **100% of rows on exactly 3
of the 10 target dates** (0627, 0711, 0712) — reproducibly, across both
`--target-date/--days-ahead` and pure `--from-date/--to-date` invocation
modes, and even in a single-date-only rebuild of just 0711. Directly querying
local PG's `jvd_se`/`jvd_ra` for these dates shows fully settled data (95-99%
non-placeholder `kakutei_chakujun`, 100% non-placeholder `shusso_tosu`) — the
raw source is fine; some other internal step of
`finish_position_features_duckdb.py` resolves these 3 specific dates
differently. **Root cause not isolated further** (would require reading the
full 4126-line script's date-branch logic; out of time budget for this
probe). **Workaround**: this script's own `finish_position`/actuals need
were already independently and correctly sourced via a placeholder-safe
`jvd_se` query (§1.4) for the join against predictions, so the fix was simply
to **not** pre-filter on the base builder's own (buggy) `finish_position`
column before that join — the later inner join is the real completeness
gate. This recovered full 264/264 coverage. **Flagged for follow-up**: if
this script's date-branch bug also affects the genuine Cloudflare Container
serve path for these (or similarly-shaped) dates, it could be a related or
contributing factor to the write-cluster degradation the sister audit
documents — not confirmed, just noted as a plausible adjacent lead.

### 1.4 Actuals and market baseline

Own placeholder-safe query against local PG `jvd_se`
(`trim(kakutei_chakujun) ~ '^[0-9]+$' AND CAST(...) > 0`, per
`reference_jvd_placeholder_semantics`), joined on
`(keibajo_code, race_bango, kaisai_tsukihi, ketto_toroku_bango)`. Market
baseline uses `tansho_ninkijun` (from the same query) as a full predicted-rank
column, identical convention to the sister baseline doc.

### 1.5 Scoring (production-identical)

Verified the actual Container scoring path
(`apps/finish-position-predict-container/src/predict_lib/scorer.py`'s
`build_feature_row`, `catboost_adapter.py`) casts every feature to plain
`float` with **no categorical-feature handling at all** — i.e.
`prepare_feature_matrix(df, feature_names, cat_indices=[])`, matching
`gate_v9sim_exact.py`'s "no_cat_features=True" convention and this session's
own Phase-1 WF ledger, **not** the `use_cat_features=True` convention used by
the unrelated, non-current armA/armB experiment in
`tmp/candidate-jra-2026-scoring/eval/score_arms_2026.py` (a different,
now-superseded feature-set generation — do not conflate the two).

### 1.6 Cell routing (faithful reimplementation)

Read the live `cell_routing.json` at runtime (not hardcoded) and
reimplemented `cell_router.py`'s `resolve_dimension` /
`all_conditions_match` / `resolve_variant` exactly (surface from
`track_code`, distance_band/field_band thresholds identical to production,
`kyoso_joken_code` raw). Each race resolves to exactly one of: `sim`
(champion, 250 feat), `jockey_pedigree_703` (269 feat, fires on
`kyoso_joken_code==703` OR `venue==02`), `prior_corner_dirt_smallfield_005`
(274 feat). Observed distribution across the 264 races: **sim 87, jockey_pedigree_703 168, prior_corner_dirt_smallfield_005 9**.

Two parallel scores are produced per race: **routed** (the variant
`resolve_variant` picks) and **champion_only** (always `sim`, ignoring
routing) — a natural paired design for measuring the routing rules'
incremental effect.

---

## 2. Results (264 races, 2026-06-13..07-12, venues 02/03/10)

### 2.1 Overall (pooled)

| Arm           | n   | top1   | place2 | place3 | top3_box |
| ------------- | --- | ------ | ------ | ------ | -------- |
| routed        | 264 | 24.242 | 14.394 | 10.227 | 4.924    |
| champion_only | 264 | 20.076 | 11.742 | 11.364 | 4.924    |
| market        | 264 | 33.712 | 18.939 | 13.258 | 9.091    |

### 2.2 Paired comparisons (delta [LB95, UB95], pp)

| Comparison                 | top1                     | place2                   | place3               | top3_box             |
| -------------------------- | ------------------------ | ------------------------ | -------------------- | -------------------- |
| **routed − champion_only** | **+4.17 [+1.89, +6.82]** | **+2.65 [+0.38, +4.92]** | −1.14 [−3.41, +1.14] | 0.00 [−1.14, +1.14]  |
| champion_only − market     | −13.64 [−18.94, −9.09]   | −7.20 [−11.36, −3.03]    | −1.89 [−5.68, +1.89] | −4.17 [−6.82, −1.52] |
| routed − market            | −9.47 [−14.39, −4.55]    | −4.55 [−9.09, −0.38]     | −3.03 [−6.82, +0.38] | −4.17 [−6.82, −1.52] |

**Routing is robustly positive vs champion-only on top1 and place2** (both
LB95 clear of 0), replicated in the same direction at all 3 venues
individually (§2.3). **Both arms robustly underperform market on top1/place2/top3_box**
in this 2026 sample — a materially larger gap than the WF backtest's
"market-neutral" pooled summer-4 finding (`jra-summer4-cell-baseline-2026-07-17.md`
§2.1). Given §5's parity caveat, this pooled market gap should **not** be
read as a confirmed 2026 degradation without first closing the fidelity gap
— it is at least partly (possibly wholly) an artifact of this reproduction's
own missing/imperfect features, not necessarily a genuine production
shortfall.

### 2.3 By venue (routed − champion_only, top1)

| Venue        | n   | delta | LB95      | UB95  |
| ------------ | --- | ----- | --------- | ----- |
| 02 Hakodate  | 120 | +5.00 | **+1.67** | +9.17 |
| 03 Fukushima | 72  | +4.17 | 0.00      | +9.72 |
| 10 Kokura    | 72  | +2.78 | 0.00      | +6.94 |

Venue02 is the only one with strictly positive LB95 (routing effect
independently robust there); Fukushima/Kokura touch exactly 0.00 (boundary,
not negative, but not independently robust alone at this n). All three point
the same direction and are consistent with the pooled result and with the
pre-existing WF gate evidence for `kyoso_joken_code=703`
(`jra-summer4-cell-baseline-2026-07-17.md` §3: +0.782pp [LB95 +0.270], n=3710) —
**this local replay is the closest thing yet to a direct 2026-OOS
confirmation that the routing rule is earning its keep**, subject to §5.

### 2.4 By cell (routed vs market, top1, n≥30)

| Dim           | Value         | n   | delta  | LB95   |
| ------------- | ------------- | --- | ------ | ------ |
| surface       | dirt          | 100 | −8.00  | −16.00 |
| surface       | turf          | 157 | −11.47 | −18.47 |
| distance_band | sprint        | 35  | −2.86  | −14.29 |
| distance_band | mile          | 76  | −10.53 | −19.74 |
| distance_band | intermediate  | 104 | −11.54 | −19.23 |
| class_label   | E (tokubetsu) | 62  | −8.06  | −16.13 |
| class_label   | unknown       | 196 | −10.20 | −16.33 |

No cell here clears n≥200 (the WF adoption-grade threshold) — these are
descriptive only, not gate-grade, and inherit the same §5 fidelity caveat.

---

## 3. Serving-tax framing (what users actually saw vs this clean replay)

Per the sister audit (`jra-serving-audit-jun-jul-2026-07-17.md`), essentially
**0 of these 264 races** ever displayed a genuinely healthy prediction to a
viewer (154 dark, 36 WIN5-overlay-only, 72 from the degraded write-cluster,
2 tiny stale backfills) — the one healthy sample (21 Mac-batch races) was
shadowed by routing priority. This replay's **top1=24.2% (routed) / 20.1%
(champion_only)**, whatever their absolute accuracy once §5 is resolved, are
both **far above** the degraded write-cluster's measured 8.33% top1
(`jra-serving-audit...md` §2 Defect A) — i.e. the generation gap between
"what a healthy pipeline should produce" and "what was actually served" is
large and this replay sits on the healthy side of it, consistent with (not
independently proving) the sister audit's root-cause framing that Defect A is
an input-degradation problem, not a model-capability problem.

---

## 4. Mac-batch (Cluster A) parity check

Compared this replay's `champion_only` predicted_rank against the genuine
Neon-served plain-champion predictions from the 2026-07-11 10:47 JST
single-batch write (`prediction_generated_at < 2026-07-11 12:00 UTC`,
`model_version='jra-cb-v9-sim-2013-clean'`) — the one production sample
independently confirmed healthy by the sister audit (top1=42.86% there,
beating market, normal score-variance signature).

| Metric                       | Value                                                 |
| ---------------------------- | ----------------------------------------------------- |
| Races compared               | 21                                                    |
| Rows joined                  | 285                                                   |
| Exact rank match (per horse) | **11.6%**                                             |
| Races with 100% exact match  | **0 / 21**                                            |
| Mean \|rank diff\|           | 3.72                                                  |
| Top1-pick agreement          | **4.8%** (1/21)                                       |
| Mean per-race Spearman ρ     | **0.30** (range −0.26 to +0.72, 19/21 races positive) |

**Interpretation**: weak-but-real positive correlation, not zero and not
negative — this replay is picking up _some_ of the same signal the genuine
model uses, but is far from a faithful reproduction. The most likely
contributors, in rough order of suspected weight: (a) the 18/250 (7.2%)
structurally-missing features (§1.2); (b) unverified value-level differences
in the ~230 "present" features between this local-PG-direct computation path
and the true Cloudflare Container's R2-Data-Catalog-attached computation path
(same SQL logic, but the doc explicitly notes Catalog is the sole authority
and local PG direct-attach is only sanctioned for "offline learning/verification",
not guaranteed byte-identical); (c) possible residual effects of the §1.3
date-branch anomaly on feature quality for some rows even after the
finish_position workaround. **Not isolated further given time budget** — see
§6 for suggested follow-up.

---

## 5. What this means for §2's numbers — read them as directional, not final

Given §4's weak parity, **the specific point estimates and LB95/UB95 bounds
in §2 should be treated as suggestive of the true production behavior, not
as a validated substitute for it.** The _within-comparison_ result (routed
vs champion*only, both scored through the \_same* imperfect pipeline) is more
trustworthy than any comparison _against market_ (an external, perfectly-
measured baseline that the imperfect pipeline is being unfavorably compared
to) — this is why §2.2/§2.3's routed-vs-champion_only findings are reported
with more confidence than the vs-market findings. Closing the fidelity gap
(fixing h2h's OOM, sourcing `target_race_id`/lineage for the trainer layer,
and ideally a byte-level feature-value diff against a genuine R2 feat-cache
snapshot for a handful of races) would be required before this replay could
stand in as a gate-grade OOS confirmation.

---

## 6. For serve-defect-269 (jockey-pedigree269 19-column non-null rates, local build)

| Column                                                                                 | Non-null rate |
| -------------------------------------------------------------------------------------- | ------------- |
| jk_venue_nichime_win_eb / top3_eb / edge / logn / win_rank_in_race / edge_rank_in_race | 98.8%         |
| jk_nichime_win_eb                                                                      | 99.9%         |
| jk_fullcell_win_eb / jk_fullcell_edge                                                  | 80.1%         |
| gsire*dist_surface*\* (win_eb/top3_eb/edge/logn/win_rank_in_race/edge_rank_in_race)    | 99.8%         |
| gsire_venue_win_eb                                                                     | 99.2%         |
| sire_class_surface_win_eb / edge                                                       | 98.6%         |
| **keito_dist_surface_win_eb**                                                          | **0.18%**     |

`keito_dist_surface_win_eb` is essentially all-NULL in this local build —
flagged verbatim per team-lead's request, for `serve-defect-269` to
cross-check against whatever it finds in the genuine Cloudflare feature path
(if this column is similarly near-all-NULL at true serve time, it is
structurally incapable of contributing signal for the 269 variant regardless
of the write-cluster issue).

---

## 7. Artifacts

- `apps/pc-keiba-viewer/tmp/candidate-jra-summer3-local-replay-2026-07-17/build_harvest_layers.py` — final (16-layer, full-base-input, per-layer input override for `trainer`) harvest builder
- `.../score_and_compare.py` — cell routing reimplementation + scoring + all comparisons
- `.../scored.parquet`, `.../comparison.json` — **v2 (post-fix) outputs**, all §8 numbers
- `.../features_v2/` — base per-race-builder output, rebuilt with the `2326bf1f` fix (0613-0712, all JRA venues; superseded `.../features/` from v1, kept for the audit trail)
- `.../work/out/*` — all 16 harvest layers (v1's 13; v2 adds `grade_lineage`, and `h2h`/`trainer` are the v1 failures now fixed)
- `.../harvest_v4.log`, `base_build3.log`, `score_compare3.log` — v2 build/score logs
- `.../harvest_v3.log`, `score_compare2.log` (v1, 13/15 layers, kept for the audit trail) — superseded by the above
- Reused unchanged: `tmp/candidate-jra-2026-scoring/eval/build_full_harvest.py` (pattern source), `finish-position/lookups/course-numerical-features.parquet` (baked course lookup), `tmp/candidate-leak-clean-retrain/nar-full-regen/run_stage9_h2h_peryear.sh` (consulted for the h2h OOM fix, not directly executed -- see §8.1)
- Code fix: `src/scripts/finish_position_features_duckdb.py` + `tests/test_finish_position_features_duckdb_integration.py` (commit `2326bf1f`)
- Local PG: `127.0.0.1:15432` (Apple container runtime instance, verified not the colima shadow twin). Neon: `NEON_PRIMARY_URL`, read-only, used only for the §4/§8.2 Cluster-A parity comparison. No writes, no deletes, no admin API calls.

---

## 8. v2 — fidelity gap closed, confirmed values (2026-07-17, continued session)

### 8.1 What changed since v1

1. **Root cause found and fixed for the §1.3 `finish_position` NULL bug**, in `finish_position_features_duckdb.py`, not upstream: `build_rec_select_sql`'s row-priority dedup between the `race_entry_corner_features`-derived row (priority 0) and the direct `jvd_se`/`jvd_ra` row (priority 1) ordered on `_rec_priority` alone, i.e. "does a corner-features row exist" rather than "is it complete". A race can have a corner-features row (written by the running-style Worker for its own purposes) whose `finish_position` was never backfilled after settlement while `jvd_se` already has the genuine result — the old ordering always kept the incomplete row. Fixed (commit `2326bf1f`, tests + cov97.51% + type-check clean) by ordering on `(finish_position is null)` before `_rec_priority`: a row that actually carries the outcome now always wins the tie-break; genuinely-upcoming races (both NULL) and normal already-materialised races (both agree) are unaffected. Rebuilding the base confirmed all 10 target dates now have the expected ~95-99% settled `finish_position` (was 0% on 0627/0711/0712). The _upstream_ non-backfill in `race_entry_corner_features` itself (a `sync-realtime-data` asset) was separately root-caused and healed by another agent (commit `46ac761b`) — this fix stands as defense-in-depth regardless.
2. **All 18 previously-missing champion-family features recovered**: `h2h` (OOM at 4GB) reran successfully at `--memory-limit 12GB --threads 3` — its target-side input is already tiny (360 races) so the NAR precedent's per-year input-scoping wrapper (`tmp/candidate-leak-clean-retrain/nar-full-regen/run_stage9_h2h_peryear.sh`) wasn't needed, just more headroom for the history-side self-join it itself budgets 12GB for. `trainer`'s `target_race_id` dependency is supplied by a new `grade_race_lineage` layer (`add-grade-race-lineage-features.py --config lineage-races/jra.json`) run immediately before it, with `trainer`'s `--input-dir` pointed at `grade_race_lineage`'s output instead of the shared base (every other layer is independent and still reads the base directly). **Result: 250/250, 269/269, 274/274 declared features present for all three variants — 0 missing**, vs 18/250 (7.2%) missing in v1.

### 8.2 Parity re-check against Cluster A (07-11 Mac-batch, 21 races, plain champion) — before/after

| Metric                     | v1 (18/250 missing) | v2 (0/250 missing) |
| -------------------------- | ------------------- | ------------------ |
| Mean Spearman ρ            | 0.30                | **0.93**           |
| Top1 exact-match           | 4.8% (1/21)         | **76.2%** (16/21)  |
| Per-horse exact rank match | 11.6%               | **44.9%**          |
| Mean \|rank diff\|         | 3.72                | **0.91**           |

This is now a high-fidelity reproduction, not merely a directional one. The
missing 7.2% of features (all `h2h_*`, `target_grade_trial_*`, and the
`trainer_grade_*`/`trainer_target_race_*` family) were evidently far more
load-bearing for matching genuine serve-time scores than their column-count
share would suggest.

### 8.3 Confirmed values — cell × rank1-6 (primary, per 2026-07-17 user instruction: evaluate by cell × individual rank, never by a summarized/pooled number)

Delta convention throughout: **routed − champion_only** (the routing rule's
own incremental effect) and, where noted, **arm − market**. Bold marks
LB95>0 or UB95<0 (robust in either direction); n=264 total, no venue/cell
below reaches the WF n≥200 gate, so nothing here is adoption-grade — this is
a confirmed **2026 OOS descriptive read**, not a gate decision.

**By venue (routed − champion_only)**:

| Venue        | n   | top1                 | place2               | place3               | place4               | place5              | place6               |
| ------------ | --- | -------------------- | -------------------- | -------------------- | -------------------- | ------------------- | -------------------- |
| 02 Hakodate  | 120 | +2.50 [0.00, +5.83]  | −0.83 [−4.17, +1.67] | −0.83 [−5.00, +2.50] | −3.33 [−7.50, +0.02] | 0.00 [−4.17, +4.17] | −0.83 [−5.00, +2.50] |
| 03 Fukushima | 72  | 0.00 [0.00, 0.00]    | +1.39 [0.00, +5.56]  | 0.00 [−4.17, +4.17]  | −1.39 [−4.17, 0.00]  | 0.00 [0.00, 0.00]   | −1.39 [−4.17, 0.00]  |
| 10 Kokura    | 72  | −1.39 [−5.56, +2.78] | 0.00 [0.00, 0.00]    | −1.39 [−4.17, 0.00]  | 0.00 [0.00, 0.00]    | +1.39 [0.00, +4.17] | −4.17 [−9.72, 0.00]  |

**No venue clears a robust (LB95>0) top1 win any more.** Venue02's +2.50pp
touches LB95=0.00 exactly (boundary, not >0). Venue03/venue10 show
essentially zero-to-slightly-negative routing effect. This is a substantial
downgrade from v1's "+5.0pp [LB95 +1.67] at venue02" claim — that claim did
not survive the fidelity fix.

**By venue (routed vs market — is the served config, imperfections included, actually beating the favorite?)**:

| Venue | n   | top1                 | place2               | place3               | place4               | place5               | place6               |
| ----- | --- | -------------------- | -------------------- | -------------------- | -------------------- | -------------------- | -------------------- |
| 02    | 120 | −0.83 [−3.33, +1.67] | +1.67 [−1.67, +5.00] | −1.67 [−6.67, +3.33] | −3.33 [−9.17, +1.67] | +2.50 [−2.50, +7.50] | +0.83 [−2.50, +4.17] |
| 03    | 72  | +2.78 [0.00, +6.94]  | −2.78 [−9.72, +4.17] | +1.39 [−2.78, +5.56] | 0.00 [0.00, 0.00]    | 0.00 [0.00, 0.00]    | 0.00 [−5.56, +5.56]  |
| 10    | 72  | −1.39 [−6.94, +4.17] | +1.39 [0.00, +4.17]  | +1.39 [−2.78, +6.94] | −1.39 [−6.94, +4.17] | +1.39 [−4.17, +6.94] | −1.39 [−6.94, +4.20] |

**No venue shows a robust deviation from market on any rank.** Fukushima's
+2.78pp top1 touches LB95=0.00 exactly; everything else brackets zero.

**By cell (surface / distance_band / class_label; routed vs market)**:

| Cell                  | n   | top1                 | place2               | place3               | place4                | place5               |
| --------------------- | --- | -------------------- | -------------------- | -------------------- | --------------------- | -------------------- |
| surface=dirt          | 100 | +1.00 [−3.00, +6.00] | +1.00 [−2.00, +4.00] | −3.00 [−7.00, +1.00] | −3.00 [−7.00, 0.00]   | 0.00 [−5.00, +5.00]  |
| surface=turf          | 157 | −0.64 [−3.82, +1.91] | 0.00 [−3.19, +3.82]  | +1.91 [−1.91, +6.37] | −1.27 [−6.37, +3.19]  | +2.55 [−0.64, +6.37] |
| distance=intermediate | 104 | −1.92 [−6.73, +2.89] | 0.00 [−2.89, +2.89]  | −1.92 [−6.73, +2.89] | −2.89 [−7.69, +1.92]  | −1.92 [−6.73, +2.89] |
| distance=mile         | 76  | +1.32 [0.00, +3.95]  | −1.32 [−6.58, +3.95] | +1.32 [−3.95, +6.58] | 0.00 [−5.26, +5.26]   | +3.95 [−1.32, +9.21] |
| distance=sprint       | 35  | +2.86 [0.00, +8.57]  | +2.86 [0.00, +8.57]  | 0.00 [−8.57, +8.57]  | −2.86 [−8.57, 0.00]   | +5.71 [0.00, +14.29] |
| class=E (tokubetsu)   | 62  | −1.61 [−4.84, 0.00]  | +1.61 [−3.23, +8.06] | 0.00 [−8.06, +8.06]  | −4.84 [−11.29, +1.61] | +3.23 [0.00, +8.06]  |
| class=unknown         | 196 | +0.51 [−2.55, +3.57] | 0.00 [−3.06, +2.55]  | −0.51 [−3.57, +2.55] | −1.02 [−4.59, +2.55]  | +1.02 [−2.05, +4.59] |

**Every cell brackets zero on every rank** (several touch a bound exactly at
0.00 without crossing it). At n=264 total, none of these cuts — venue,
surface, distance_band, or class_label — shows a statistically robust
deviation from market on any individual rank 1 through 6.

### 8.4 Reference only — pooled (do not use for any adoption/verdict decision, per the 2026-07-17 user instruction)

| Comparison             | top1                 | place2               | place3               | place4               | place5               | place6               |
| ---------------------- | -------------------- | -------------------- | -------------------- | -------------------- | -------------------- | -------------------- |
| routed − champion_only | +0.76 [−0.76, +2.65] | 0.00 [−1.52, +1.52]  | −0.76 [−3.03, +1.14] | −1.89 [−3.79, 0.00]  | +0.38 [−1.52, +2.27] | −1.89 [−4.17, +0.38] |
| routed − market        | 0.00 [−2.27, +2.27]  | +0.38 [−1.89, +2.65] | 0.00 [−3.03, +3.03]  | −1.89 [−4.92, +1.14] | +1.52 [−1.14, +4.17] | 0.00 [−3.03, +2.65]  |
| champion_only − market | −0.76 [−3.41, +1.89] | +0.38 [−2.27, +3.03] | +0.76 [−1.89, +3.79] | 0.00 [−3.41, +3.03]  | +1.14 [−1.14, +3.41] | +1.89 [−0.76, +4.55] |

### 8.5 Final verdict

1. **venue02 route 2026 effectiveness**: not confirmed at gate-grade, and the
   v1 apparent confirmation (+5.0pp [LB95 +1.67]) does not survive the
   fidelity fix — v2's venue02 routed-vs-champion_only is +2.50pp with
   LB95 touching exactly 0.00, i.e. a plausible small positive effect
   consistent with (not independently confirming) the existing WF evidence
   for `kyoso_joken_code=703` (+0.782pp [LB95 +0.270], n=3710,
   `jra-summer4-cell-baseline-2026-07-17.md` §3), but this 264-race, ~1-month
   2026 sample is too small to move that WF-level conclusion on its own in
   either direction.
2. **True 2026 summer-3-venue local accuracy vs market**: also not
   distinguishable from market at this sample size, in either the routed or
   champion-only configuration, on any of rank1-6, in any of the venue/
   surface/distance_band/class_label cuts examined. This is a genuinely
   different conclusion from v1's "both arms robustly underperform market"
   (§2.2) — that finding was substantially an artifact of the 7.2%
   missing-feature gap, not a real 2026 signal.
3. **What this local replay newly establishes with confidence**: the
   reproduction pipeline itself (feature build + cell routing + scoring) is
   now validated as high-fidelity (ρ=0.93 vs the one known-genuine production
   sample) and is available for reuse by other agents wanting a clean,
   serving-defect-free 2026 dataset — see §7 for artifact paths. The
   `finish_position_features_duckdb.py` fix (commit `2326bf1f`) is a
   permanent, tested improvement independent of this specific campaign.
