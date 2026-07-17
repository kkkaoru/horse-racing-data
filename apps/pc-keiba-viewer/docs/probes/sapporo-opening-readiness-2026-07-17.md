# Sapporo (venue01) 2026 Opening Readiness (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position serving readiness — read-only desk check, no execution-system changes
- **Task**: 次サイクル調査③ (team-lead, USER's standing "待機中は常に次サイクル調査を複数並列で" instruction). Sapporo (venue01) is the only one of the 4 goal venues (01/02/03/10) not yet exercised against 2026 data — it opens this month. Verify the pipeline handles it correctly before opening day, and design a watch plan for when it does.

## 0. Opening date — sourced externally, not from JVD (methodology note)

**第1回札幌競馬(2026) opens Saturday 2026-07-25**, running through 2026-08-16
(1st meeting) with a 2nd meeting continuing into September per JRA's summer
program overview. Confirmed via two independent sources: `jra-fun.jp`'s
event page (explicit "7.25[土]〜8.16[日]" listing) and a JRA-calendar-
anchored web search. **This came from an external web search, not JVD**:
the local PG replica has no forward-looking kaisai-schedule table —
`jvd_cs` (only other `keibajo_code`-bearing table matching a "schedule"-like
name search) is actually a 119-row course-geometry/description master (same
shape as `course-numerical-features.parquet`, one row per venue×distance),
not a future schedule, and `jvd_ra` naturally has zero 2026 venue=01 rows
today because those races have not happened. Cross-checked against
historical pattern (`jvd_ra`, `kaisai_nen`/`keibajo_code='01'`): opening
dates were 2023-07-22, 2024-07-20, 2025-07-26 — all Saturdays in the same
3-week window, consistent with 07-25. **8 days of lead time from today.**

## 1. Pipeline readiness — checked layer by layer, no gap found

| Layer                                | Check                                                                                                                                                            | Finding                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cell routing                         | `apps/finish-position-predict-container/src/predict_lib/cell_routing.json` — any `venue=01` rule?                                                                | **No venue01 rule exists** (only `venue=02`→`jockey_pedigree_703` is venue-gated). Sapporo races fall to `default_variant: sim` (the champion) unless they independently match the two non-venue-gated rules (`kyoso_joken_code=703`, or `surface=dirt & field_band=f_le10 & kyoso_joken_code=005`) — same treatment as venue 03/10 already receive today. **Intended, not a gap**: venue01 was never meant to get special routing; only venue02 earned one, from the earlier serve-defect/Cluster-B diagnosis chain. |
| RS model + corner-features history   | `race_running_style_model_predictions` / `race_entry_corner_features` row counts for `keibajo_code='01'` (local PG)                                              | RS: 18,819 rows all-time. Corner-features: 82,975 rows — comparable to or higher than the already-serving summer venues (venue02: 78,005; venue03: 142,223; venue10: 156,025). Healthy historical base for every lookback-window feature (career/style rates, past-corner averages, etc.) from Sapporo's very first 2026 race.                                                                                                                                                                                        |
| Feature builder — turf code handling | Sapporo's actual `track_code` values, 2023-2025 (`jvd_ra`)                                                                                                       | Only two codes appear: `17` (turf/洋芝, 297 races) and `24` (dirt, 207 races). Both already fall inside the **existing, generic** `JRA_TURF_CODES` (10-22) / `JRA_DIRT_CODES` (23-29) ranges used everywhere (`_surface_expr`, `derive_surface` in `serve_health_check.py`, `cell_router.py`'s own surface derivation). No Sapporo-specific branch exists or is needed — 洋芝 is not a separate code family, just an ordinary member of the turf range.                                                               |
| Course-constant lookup               | `finish-position/lookups/course-numerical-features.parquet` — Sapporo row present?                                                                               | **Yes.** All 10 JRA venues (01-10) present, 119 total rows. The `course` harvest layer resolves correctly for Sapporo from race 1; no missing-lookup fallback will trigger.                                                                                                                                                                                                                                                                                                                                           |
| Realtime odds / weight-fetch path    | `apps/sync-realtime-data/src/*.ts` (read-only inspection only — package is off-limits to edit this session, per campaign rule) — any Sapporo-specific exclusion? | **None found.** The only venue-conditional logic in the worker is `JRA_PRIORITY_VENUE_CODES = ["05", "08"]` (Tokyo/Kyoto), which reorders _within-cron_ weight-fetch scheduling so those two highest-traffic venues' headline races (5R/11R) get fetched first — a fetch-order optimization, not a coverage gate. Sapporo gets the same standard-tier fetch path already proven live for Hakodate/Fukushima/Kokura in 2026.                                                                                           |

**Verdict: no pipeline readiness gap found at any layer.** Every code path
Sapporo will exercise is a shared, already-validated path (proven by the
other 3 summer venues' live 2026 serving and by the 264-race replay) — none
of it is Sapporo-specific or untested logic waiting to fire for the first
time on opening day.

## 2. Sapporo-watch measurement plan (opening week)

Two existing tools, no new tooling needed — both are venue-agnostic by
design, so "watching Sapporo" is just "keep running them and look at the
venue=01 rows once they exist."

**Day-of, every Sapporo race day** (`src/scripts/serve_health_check.py`,
built 2026-07-17 for the 07-12 Cluster-B incident — 5 checks: coverage
gaps, predicted-score quality collapse, routing parity, write-burst
detection, D1 self-heal activity):

```sh
uv run python src/scripts/serve_health_check.py --date 20260725 --category jra
uv run python src/scripts/serve_health_check.py --date 20260726 --category jra
```

Expect **exit 0** both days. Any exit 1 (anomaly found) or 2 (tool failure)
→ investigate same-day, before assuming it's just the accepted deficit
(section 3) rather than a genuine new serving defect.

**After Sapporo races start settling (roughly weekend 1, ~10-15 races)**:
re-run the same Neon query pattern `serve_2026_eval.py` (wave1 baseline,
2026-07-17) already used for the 2026 YTD summer-4-venue serve-realistic
eval, restricted to `keibajo_code='01'`, comparing champion top1 against
market. Interpretation grid, against the **already-closed** WF baseline
(`docs/probes/jra-sapporo-top1-deficit-diagnosis-2026-07-17.md`: pooled
3-fold-stable top1 delta **−2.910pp [LB95 −4.762, UB95 −1.124]**, verdict
"market information edge, not fillable, accepted deficit" after a
JRA-wide connections-family scale-up + permutation test, p=0.7448, closed):

| Live reading (once n is large enough to read at all, ~n≥20-30)                                              | Interpretation                                                                                                                                                                           |
| ----------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Delta roughly in the −1pp to −5pp range                                                                     | **Reproduces** the accepted deficit — no action, consistent with the closed diagnosis, do not re-open it over normal single-weekend variance                                             |
| Delta far below the WF range (e.g. worse than −8 to −10pp) or paired with a `serve_health_check.py` anomaly | **Not** what the closed diagnosis predicted — treat as a candidate new serving defect (Cluster-B-shaped), check the health tool first before attributing it to "just Sapporo being hard" |
| Delta flat or positive                                                                                      | Also log it, but a single opening weekend's n is too small to revise a WF-baseline verdict built on 3 blind years — informational only                                                   |

No new alert thresholds needed: `serve_health_check.py`'s existing
calibrated cutoffs (quality stddev < 0.3, > 10 races/minute burst) are
venue-agnostic — Sapporo race rows simply appear alongside every other
venue's rows in that date's check, nothing Sapporo-specific to tune.

## 3. Opening-week challenger comparison — flip-gate blind accumulation

**Confirmed, more precisely than "presumably": Sapporo is genuinely blind
to both models, not just old vs. new champion in the abstract.** Live
champion (`jra-cb-v9-sim-2013-clean`, MLflow v13) trained through
`TRAIN_END=2025-12-31` — no 2026 Sapporo data exists at all before that
cutoff. Challenger (`fresh2026h1`, MLflow v34, registered-not-deployed,
`docs/probes/jra-champion-fresh2026h1-retrain-2026-07-17.md`) trained
through `TRAIN_END=2026-07-12` — even this freshest possible cutoff has
**zero** 2026 Sapporo rows, confirmed directly from that retrain's own
harvest log (`features_base` build over `20260101..20260712`): "Sapporo's
2026 meet had not started as of this run — 0 rows, not filtered out."
Both models' only Sapporo exposure, at any training cutoff achievable
today, is 2013-2025 history. Sapporo's opening week is the one population
in this entire campaign that is simultaneously blind to the deployed
champion, the freshness-retrained challenger, and any future retrain run
before 07-25.

**The mechanism (`tmp/candidate-jra-champion-fresh2026h1-2026-07-17/
blind_gate_runbook.py`, per team-lead's own §7.1 decision today to
"blind-gate the weekend")**: scores a `--from-date`/`--to-date` window with
both champion and challenger, appends per-race hit outcomes to a persistent
`race_id`-deduped accumulator, and gates a production flip on n≥200
accumulated blind races with no LB95 regression and ≥1 significantly
positive primary metric. Its window-builder calls the standard JRA feature
pipeline with **only a date range, no venue filter** — Sapporo races falling
inside any scored window are automatically included, no code change needed.

**The one real risk is operational, not code**: this runbook is a
single manual command per race day/weekend, not a cron job. Nothing
currently guarantees anyone runs it for 07-25/07-26 specifically. Per
team-lead's decision, the cadence already in motion (weekend 1: 07-18/19)
would naturally reach Sapporo's opening as "weekend 2" if the same cadence
continues — but this is worth stating explicitly as a **must-run window**,
since a missed weekend is recoverable later (idempotent by `race_id`) but
would silently delay the one genuinely-blind Sapporo read this campaign
will get for months:

```sh
uv run python tmp/candidate-jra-champion-fresh2026h1-2026-07-17/blind_gate_runbook.py \
    --from-date 20260725 --to-date 20260726
```

**Venue-bias caution (as requested)**: the flip gate's pass/fail is
**pooled across all JRA venues**, not gated per-venue. Two distinct,
opposite-direction risks to keep in mind once Sapporo enters the
accumulator:

1. If `fresh2026h1` inherits the _same_ Sapporo weakness as the champion
   (plausible — gates (c)/(d) of the retrain doc found "same recipe, more
   data," no structural change to the market-information-gap mechanism the
   closed diagnosis identified as unfillable) — the deficit would be
   roughly equal for both models on Sapporo races and **cancel out in the
   paired delta**. A pooled `FLIP-READY: YES` would then correctly mean
   "fresh2026h1 is not worse than champion," but it does **not** mean
   "Sapporo is fixed" — those are different claims. Recommend checking the
   runbook's own per-venue cell report (`blind_gate/latest_cell_report.json`,
   `venue=01`, n≥20) once it has enough rows, purely as an informational
   cross-check — not a proposal to change the gate's binding criterion.
2. Conversely, a small early Sapporo sample (n≈10-15 in its first weekend)
   swinging on ordinary small-n noise could visibly move that weekend's
   pooled reading without being a real signal about either model. The
   runbook's own `interim_safety_check_n50plus` / `flip_gate_n200plus`
   fields already require accumulated-n thresholds before treating a
   reading as decisive, so this is already handled by the existing design
   — flagged here only so it isn't mistaken for a new finding when Sapporo
   first appears in the numbers.

## 4. Summary

No pipeline gap, no code change needed, no execution-system files touched
(read-only investigation as scoped). Three concrete, actionable outputs for
whoever is on duty during opening week:

1. Run `serve_health_check.py --date 20260725/20260726 --category jra`
   daily; expect exit 0.
2. Once ~10-15+ Sapporo races settle, re-run the wave1 serve-eval query
   restricted to `venue=01` and compare against the accepted −2.910pp
   baseline using the interpretation grid in section 2 — reproduction is
   the expected, unremarkable outcome; a much larger deviation or a
   `serve_health_check.py` anomaly is the actual trigger to investigate.
3. Make sure `blind_gate_runbook.py` gets run for the 07-25/07-26 window
   as part of the ongoing weekend blind-gate cadence — this is the one
   opportunity for a genuinely-blind Sapporo read against both the current
   champion and the freshness-retrained challenger; check the per-venue
   cell report once populated, informationally, alongside (not instead of)
   the pooled flip-gate verdict.
