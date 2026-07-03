# JRA Summer-Venue Odds-Divergence (Upset) Profile + Feature-Candidate Probe (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position, upset/odds-divergence diagnostics + feature hypothesis
- **Trigger**: follow-up to `jra-summer-venue-cell-focus-2026-07-04.md`, which found
  summer venues (Sapporo `01` / Hakodate `02` / Fukushima `03` / Kokura `10`) run
  hotter upset rates than JRA-wide (Fukushima 69.7%, Kokura 69.0% vs 66.4% JRA-wide,
  2023-2025 serve-realistic slice) and that the model's top1 collapses to 3.5% on
  upset races. This doc profiles **who wins** those upset races (2025+2026 actuals)
  and odds-controlled-probes the most promising patterns as feature candidates.
- **Scope note**: runs alongside three other in-flight workstreams this same session
  — meetingday×waku engineered features, jockey win-rate interactions, and pedigree
  win-rate features are each being WF-tested separately. This doc does **not**
  duplicate those; where a pattern here overlaps, it says so explicitly and defers
  to the parallel workstream's result.

## Method

**Data**: local Postgres (`horse_racing`, port 15432, read-only), not the offline
parquet store (which has no 2026 rows). Two pulls, both JRA (`keibajo_code`
`01`-`10`) and both built from a single materialized `starters` table
(`jvd_se` join `jvd_ra`, 2015-2026, 558,906 rows) so that prior-race /
venue-experience window functions are computed **once**, locally, strictly-prior
(as-of `race_date`, no leakage):

- `summer_2025_2026.parquet` — 4 summer venues, 2025+2026, **the upset-profile
  dataset** (1,167 finished races).
- `jra_all_2023_2025.parquet` — all 10 JRA venues, 2023-2025, **the probe
  dataset** (used for the odds-controlled partial-Spearman checks).

Engineered strictly-prior columns (all via DuckDB window functions over the
materialized history, no leakage): `layoff_days`, `class_diff_existing_style`
(reuses the exact class-ordinal `CASE` from
`src/scripts/finish_position_features_duckdb.py`'s `HORSE_HISTORY_BASE_SELECT`,
for apples-to-apples comparison with the deployed `last_race_class_diff`),
`distance_diff`, `first_time_this_venue`, `prior_hokkaido_turf_starts` (career
count of turf starts at Sapporo **or** Hakodate — pooled, since both share the
same 洋芝 turf strain, distinct from mainland 野芝), `prior_finish_position`,
and expanding (as-of-date) `jockey_prior_career_win_rate` /
`jockey_prior_venue_win_rate`.

**Definitions**:

- `upset_race` — the market favorite (`tansho_ninkijun==1`) did not finish 1st.
- `upset_winner` — restricted further: the actual winner's own
  `tansho_ninkijun >= 4` (excludes the common case where the #2/#3 market
  choice merely beat the favorite — this isolates genuine longshot wins).
- **Control** = non-upset-winner races (winner had `ninkijun` 1-3) unless noted.

Scripts: `tmp/candidate-jra-summer-upset/build_upset_data.py` (PG pull +
strictly-prior feature build), `profile_upsets.py` (Part 1 quantified
profile), `probe_candidates.py` (Part 2 odds-controlled probe). Reports:
`profile_report.json`, `probe_report.json`.

## Part 1 — quantified upset-winner profile, 2025+2026 (n=1,167 races)

Pooled: **69.5% of races are upset races**, **36.9% have a genuine longshot
(ninki≥4) winner**. By venue: Sapporo 62.5%/28.6%, Hakodate 65.7%/36.1%,
Fukushima 73.2%/38.4%, Kokura 71.4%/39.4% — consistent with the same
Fukushima/Kokura-run-hotter pattern from the parallel cell-focus doc, now
confirmed in the 2025-2026 window specifically.

### 1. Waku/umaban — mixed, not a clean signal

| Venue     | Upset winners inner(1-2)% | Control inner(1-2)% | Direction                           |
| --------- | ------------------------- | ------------------- | ----------------------------------- |
| Sapporo   | 18.75 (n=48)              | 14.17 (n=120)       | upset winners slightly _more_ inner |
| Hakodate  | 14.10 (n=78)              | 26.09 (n=138)       | upset winners _less_ inner (-12pp)  |
| Fukushima | 17.83 (n=129)             | 24.15 (n=207)       | upset winners _less_ inner (-6.3pp) |
| Kokura    | 23.60 (n=161)             | 24.19 (n=248)       | flat                                |

No consistent cross-venue direction — sign flips between Sapporo and
Hakodate/Fukushima. **Not a usable single-direction lever** (this is also the
same conclusion the parallel same-day draw-affinity retest reached: no
positive-LB95 draw lever anywhere except Tokyo — consistent, do not re-test).

### 2. Meeting-day clustering — no "late-meet turf wear" effect on upsets

| Venue     | Upset winners late(6+)% | Control late(6+)% |
| --------- | ----------------------- | ----------------- |
| Sapporo   | 33.33                   | 26.67             |
| Hakodate  | 43.59                   | 44.93             |
| Fukushima | 22.48                   | 20.77             |
| Kokura    | 48.45                   | 50.81             |

Flat-to-negative at 3 of 4 venues (control is _equal or higher_ late-meet
share than upset winners at Hakodate/Kokura). The "turf wears down through the
meet → more upsets late" hypothesis is **not supported** by this data. (Task
#3 in this session is separately WF-testing meetingday×waku engineered
features — this descriptive check is consistent with low expectations there
but is not a substitute for that WF result.)

### 3. Running style of upset winners — real, consistent, but already retested

Post-hoc `kyakushitsu_hantei` of the actual race (descriptive only — this is
determined _from_ the race itself, not a serve-time feature):

| Style (upset winners vs control) | Pooled upset winners % | Pooled control % |
| -------------------------------- | ---------------------- | ---------------- |
| senkou (先行)                    | 46.63                  | 57.50            |
| sashi (差し)                     | **25.00**              | **15.71**        |
| oikomi (追込)                    | **4.57**               | **1.96**         |
| nige (逃げ)                      | 23.80                  | 24.82            |

Closers (sashi+oikomi combined: 29.6% vs 17.7% control) are consistently
**overrepresented among upset winners at all 4 venues** (sashi share higher
at every single venue: Sapporo 22.9 vs 10.8, Hakodate 19.2 vs 14.5, Fukushima
24.0 vs 19.3, Kokura 29.2 vs 15.7); senkou consistently underrepresented at
all 4. This is the single cleanest, most consistent pattern in the whole
profile. **However**: this is exactly the mechanism the parallel Part-3
hypothesis in `jra-summer-venue-cell-focus-2026-07-04.md`
(`closer_x_straight`/`front_x_straight` = self running-style rate × short
home straight) already tested via a full 3-fold × 3-seed WF
(`tmp/candidate-summer-pace-hypo/straight_closer_wf.py`) **the same day** —
result: no LB95>0 primaries, summer-restricted deltas flip sign across seeds
(top1 +0.49/+0.12/+0.49pp but place2/4/5 negative in 2 of 3 seeds). The
descriptive pattern is real; the natural feature engineering of it (self-rate
× straight length) does not survive WF. armB already also carries
race-level `field_nige_pressure`/`field_sashi_pressure`/`field_senkou_pressure`/
`field_oikomi_pressure` (pace-context, not just self-history), so this channel
is not under-fed with ingredients — it looks structurally saturated.
**Recommendation: do not re-test this angle** (DO-NOT-RETEST, confirmed twice
same day via two independent methods).

### 4. Class/distance upset density — "E"-grade (特別/tokubetsu) races are the hot spot at every venue

Deepening the earlier Kokura×mile finding: grouping by grade code instead
shows a much cleaner, fully consistent pattern — **`grade_code == 'E'`
(tokubetsu / featured non-graded stakes) races have 8-15pp higher
upset-winner rates than ordinary condition races (`grade_code==' '`), at
every one of the 4 venues**:

| Venue     | E-grade upset-winner% (n) | Ordinary upset-winner% (n) | Δ     |
| --------- | ------------------------- | -------------------------- | ----- |
| Fukushima | 46.05 (79)                | 36.25 (260)                | +9.8  |
| Kokura    | 45.45 (102)               | 36.88 (311)                | +8.6  |
| Hakodate  | 44.00 (53)                | 31.87 (169)                | +12.1 |
| Sapporo   | 32.50 (40)                | 27.05 (122)                | +5.5  |

Deeper, E-grade × distance combinations are the worst pockets found in this
whole analysis: Fukushima×intermediate(1800-2199m)×E **59.1%** (n=23),
Hakodate×intermediate×E **52.6%** (n=20), Kokura×sprint×E **51.4%** (n=37),
Kokura×mile×E **48.0%** (n=25). E-grade races draw more evenly-matched fields
(shippers from main tracks mixing with summer-circuit regulars in featured
races), which plausibly compresses the market's confidence gap without
compressing true quality variance — a market-inefficiency mechanism distinct
from draw/pace. **Caveat**: `grade_code` doesn't vary within a race (it's a
race-level constant), so it cannot be probed as a per-horse ranking feature
the way the Part 2 candidates below are — this is a **segment/calibration**
finding (which cells are hardest), not a new column candidate. Handing off as
context: if a per-cell calibration or routing pass is ever revisited, E-grade
summer races are the single most concentrated pocket of model overconfidence
found in this campaign, ahead of Kokura×mile or venue×meetingday alone.
Directly re-testing "E-grade × venue routing" as its own model split is very
likely to hit the same wall as the broader venue-routing REJECT already on
record (`project_venue_cell_round2_2026_06_20` / `project_jra_rs_cell_routing_reject_2026_07_03`)
— not proposed as a new WF candidate here for that reason.

### 5. Prior-race patterns — real differences, but all already inside armB's existing feature set

| Metric (median)               | Upset winners  | Control        | Already in armB?                                                                                        |
| ----------------------------- | -------------- | -------------- | ------------------------------------------------------------------------------------------------------- |
| layoff_days                   | 49 days        | 29 days        | yes — `days_since_last_race`, `is_returning_from_layoff`, `days_since_last_race_log`                    |
| class_diff (existing ordinal) | 0 (mean +0.05) | 0 (mean -0.19) | yes — `last_race_class_diff` (**direction is opposite the "class-drop" hypothesis** — see caveat below) |
| distance_diff                 | 0 (mean -7.1)  | 0 (mean -10.4) | yes — `last_race_distance_diff`                                                                         |
| prior_finish_position         | 7 (mean 7.4)   | 3 (mean 4.1)   | yes — `last_race_finish_norm`, `finish_trend_5`, `last_3_avg_finish_norm`                               |

Upset winners come off a **much worse prior finish** (median 7th vs 3rd) and
a **notably longer layoff** (median 49 vs 29 days) than typical winners — a
"quietly-freshened bounce-back horse" profile. The class-drop hypothesis is
**not supported** in the expected direction (upset winners are marginally
_less_ likely to be dropping class than control winners, using the existing
ordinal) — worth flagging: this ordinal (`000→0,005→1,010→2,016→3,701→4,703→5,999→6`
in `finish_position_features_duckdb.py`) ranks newcomer/maiden races (701/703)
_above_ 1-3win allowance races (005/010/016), which is not a monotonic class-strength
scale in the usual JRA sense — a possible pre-existing encoding quirk, **out of
scope to fix here** (affects all of JRA, not summer-specific) but flagged for
whoever owns `last_race_class_diff` next.

All four of these prior-race metrics are **already represented by existing
armB features** in name and substance. See Part 2 for whether they carry
_summer-specific_ incremental signal beyond what the model already sees
JRA-wide.

### 6. Venue-switch / first-time-at-venue — weak, wrong direction

Upset winners are _first time at this exact venue_ 50.96% of the time vs
56.52% for control winners (**upset winners are less likely to be
first-timers**, opposite of the "market underprices the unknown shipper"
hypothesis). Hokkaido-turf-specific experience (pooled Sapporo+Hakodate,
turf races only, n=227): upset winners average _more_ prior Hokkaido-turf
starts (1.84 vs 1.43) though are also somewhat more often zero-experience
(38.2% vs 33.9%, a weak ~4pp gap on n=227) — internally inconsistent, not a
clean signal.

### 7. Jockey local-specialist — hypothesis refuted

Expanding (strictly-prior) jockey win rates: upset winners' jockeys actually
have **lower** venue win rate (7.42% vs 9.82% control) and lower career win
rate (6.02% vs 8.24%) — expected, since longshot horses are disproportionately
ridden by lower-win-rate jockeys generally. The key test is the _relative_
gap (venue win rate minus career win rate, i.e. "does this jockey
over-perform their own baseline specifically at this venue"): upset winners
+1.40pp vs control +1.59pp — **essentially identical, no evidence that
upset-winning jockeys are disproportionately local-circuit specialists**.
This directly refutes the "summer-circuit local specialist catches upsets"
hypothesis. (Task #4 this session is separately WF-testing jockey win-rate
interaction features on other grounds — this finding does not support adding
a summer-specific jockey lever on top of that.)

## Part 2 — odds-controlled partial-Spearman probe (2023-2025, JRA)

Methodology matches `tmp/candidate-jra-oddsfree-ability/probe.py`: within-race
demeaned ranks, `pr_mkt` = partial Spearman vs `finish_position` controlling
jointly for `tansho_ninkijun` rank + `tansho_odds` rank. **Important
difference from that reference probe**: this probe does **not** control for
the model's own `predicted_score` (not available outside the offline
feature-store pipeline for a raw-PG pull) — so a nonzero `pr_mkt` here means
"odds-free signal exists," not "incremental over the deployed model." Given
that all candidates below already overlap named armB features, this
distinction matters a lot for the verdict.

Promising bar per task brief: `|pr_mkt| >= 0.02` with stable sign across
2023/2024/2025 — **with the explicit caveat (confirmed by this campaign's own
history) that even candidates clearing this bar have previously failed full
WF**.

| Candidate                   | Scope                      | 2023 pr_mkt | 2024 pr_mkt | 2025 pr_mkt | Stable sign?                 | Summer vs non-summer pr_mkt (sanity check)                           | Verdict                                                                                                                                                                                  |
| --------------------------- | -------------------------- | ----------- | ----------- | ----------- | ---------------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `layoff_days`               | summer venues only         | 0.080       | 0.058       | 0.065       | yes (n=30,613)               | summer 0.068 vs **non-summer 0.067** — identical                     | **REJECT for a summer-specific slot** — real signal, but it's the same generic JRA-wide effect already carried by `days_since_last_race`/`is_returning_from_layoff`; no summer elevation |
| `prior_finish_position`     | summer venues only         | 0.073       | 0.055       | 0.064       | yes (n=30,613)               | summer 0.064 vs **non-summer 0.081** — summer is _lower_, not higher | **REJECT for a summer-specific slot** — already covered by `last_race_finish_norm`/`finish_trend_5`; summer shows _less_ signal than the rest of JRA, not more                           |
| `hokkaido_turf_first_timer` | Sapporo+Hakodate turf only | -0.005      | 0.042       | 0.041       | **no** (2023 flips sign, ≈0) | n/a (Hokkaido-turf-only, no non-summer analogue)                     | **REJECT** — the one candidate genuinely absent from armB, but sign is unstable and magnitude marginal even in the two positive years (n≈2,088-2,100/yr)                                 |

The `rho_odds` values (candidate's own correlation with market odds, not
tabled above for space) confirm these are largely market-anticipated signals:
`prior_finish_position` correlates 0.55 with odds rank pooled (bettors
clearly price in recent form), `layoff_days` correlates a modest 0.02-0.12
with odds (increasing over 2023→2025), `hokkaido_turf_first_timer` correlates
weakly (0.04-0.07).

## Ranked handoff / recommendation

1. **No candidate from this pass clears the bar for a full WF slot.** The two
   candidates with stable, clearing-threshold `pr_mkt` (`layoff_days`,
   `prior_finish_position`) both (a) substantially duplicate named armB
   features and (b) show **no summer-specific elevation** when checked
   against the non-summer JRA population — the direct test this task asked
   for. That sanity check is the single most decisive result in this probe:
   it rules out "summer venues have unexploited layoff/form signal" as a
   real, addressable gap.
2. The one structurally-novel candidate (`hokkaido_turf_first_timer`) fails
   the stable-sign bar outright — genuine REJECT, not a scope/redundancy
   issue.
3. **The running-style (closer-overrepresentation) pattern is the strongest
   descriptive finding in this profile** — real and consistent across all 4
   venues — but it was independently tested via a full WF the same day
   (`straight_closer_wf.py`, self-rate × straight-length) and rejected; armB
   already carries both self-history rates and race-level pace-pressure
   features. **DO-NOT-RETEST** this mechanism a third way without a
   genuinely different feature construction (none identified here).
4. **E-grade (特別/tokubetsu) summer races are a real, consistent (all 4
   venues), previously-undocumented segment-level weak spot** (+5.5 to
   +12.1pp upset-winner rate vs ordinary condition races at the same venue),
   concentrated further at intermediate distance. This is not a per-horse
   feature candidate (grade is race-constant) and per-venue/per-segment
   routing has an established REJECT track record in this campaign
   (`project_venue_cell_round2_2026_06_20`,
   `project_jra_rs_cell_routing_reject_2026_07_03`) — **not recommending a new
   WF slot for this**, but flagging it as the most concrete, reusable
   fact from this analysis for anyone doing calibration or confidence-shrinkage
   work (e.g. widening the model's uncertainty on its #1 pick specifically
   for E-grade races at these 4 venues, a display/calibration-layer change
   rather than a training-feature change).
5. **Waku/draw, meeting-day, venue-switch, and jockey-local-specialist
   hypotheses are all REJECTed** by this profile (either no consistent
   direction or hypothesis-refuting direction) — consistent with, and adding
   independent confirmation to, this campaign's existing draw-affinity and
   venue-routing REJECTs.

**Bottom line for the 2026-07-04 focus**: this pass does not surface a new
feature worth a WF slot. The summer-venue upset gap looks best explained by
(a) genuinely harder market conditions in E-grade races (a calibration
question, not a feature-engineering one) and (b) the model's already-known,
now twice-confirmed structural inability to catch pace-driven closer upsets
— not by any missing strictly-prior signal this profile could locate.

## Artifacts

- Data build: `tmp/candidate-jra-summer-upset/build_upset_data.py` (local PG,
  read-only, `starters`/`starters_enriched`/`jockey_enriched` → parquet)
- Profile: `tmp/candidate-jra-summer-upset/profile_upsets.py` →
  `profile_report.json`
- Probe: `tmp/candidate-jra-summer-upset/probe_candidates.py` →
  `probe_report.json`
- Related: `jra-summer-venue-cell-focus-2026-07-04.md` (parent diagnosis,
  cell accuracy + straight×closer WF), `jra-masked-lever-clean-retest-2026-07-04.md`
  (same-day draw-affinity/track-bias retest, corroborating REJECT)
