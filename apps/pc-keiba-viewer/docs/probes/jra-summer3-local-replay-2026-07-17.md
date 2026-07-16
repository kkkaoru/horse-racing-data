# JRA 夏3場 (Hakodate/Fukushima/Kokura) 264-Race Local Serve-Parity Replay (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position — local reproduction of production-identical scoring for the 264 confirmed 2026-06-01..07-12 races at venues 02/03/10, per user instruction relayed by team-lead (follow-on to `jra-summer4-cell-baseline-2026-07-17.md` and `jra-serving-audit-jun-jul-2026-07-17.md`).
- **Goal**: (1) locally reproduce the same cell-routed predictions the Cloudflare Container would genuinely serve (no Neon writes — local analysis only), bypassing the write-cluster serving defect documented in the sister audit; (2) compare routed-vs-champion-only, both-vs-market; (3) validate the reproduction against the one confirmed-healthy production sample (the 2026-07-11 Mac-batch, "Cluster A").
- **Headline result — a validation caveat, not a clean answer**: the reproduction pipeline was built and fully scores all 264 races, and shows a **robust, positive routed-vs-champion-only effect** (pooled top1 +4.17pp [LB95 +1.89]), replicated at all 3 venues individually (venue02 +5.0pp [LB95 +1.67], venue03 +4.17pp [LB95 0.00], venue10 +2.78pp [LB95 0.00]). **But** a parity check against the one known-genuinely-healthy production sample (07-11 Mac-batch, 21 races, plain champion) found only **weak agreement** (mean Spearman ρ=0.30, top1 exact-match 4.8%, mean |rank diff| 3.7) — i.e. this reproduction captures directional signal but is **not a faithful bit-exact replica** of true serve-time scoring. Every number below should be read with that caveat; see §5.

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

- `apps/pc-keiba-viewer/tmp/candidate-jra-summer3-local-replay-2026-07-17/build_harvest_layers.py` — 15-layer harvest builder (v2: full-base-input, fixed from a v1 that used an overly narrow minimal projection)
- `.../score_and_compare.py` — cell routing reimplementation + scoring + all comparisons
- `.../scored.parquet` — per-horse scored output (both routed and champion_only ranks)
- `.../comparison.json` — full metric tables (all cells, all metrics, LB95/UB95)
- `.../features/` — base per-race-builder output (0613-0712, all JRA venues)
- `.../work/out/*` — the 13 successful harvest layers
- `.../harvest_v3.log`, `base_build3.log` — build logs (layer failures included, for reproducibility)
- Reused unchanged: `tmp/candidate-jra-2026-scoring/eval/build_full_harvest.py` (pattern source), `finish-position/lookups/course-numerical-features.parquet` (baked course lookup)
- Local PG: `127.0.0.1:15432` (Apple container runtime instance, verified not the colima shadow twin). Neon: `NEON_PRIMARY_URL`, read-only, used only for the §4 Cluster-A parity comparison. No writes, no deletes, no admin API calls.
