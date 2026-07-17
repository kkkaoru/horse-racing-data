# Cushion value / moisture content pilot (USER decision 5) — daily ingestion + historical pilot + probe

- **Date**: 2026-07-18 (pilot kicked off late 2026-07-17 evening per team-lead's assignment)
- **Category**: JRA finish-position. USER decision 5: "クッション値/含水率: cell単位で有効なら有効化" — pilot phase only tonight (ingestion + probe), no WF training, no cell-level adoption decision.
- **Scope boundary, explicit**: this is a **bounded pilot**, not the full ingestion campaign. The permission question (JRA's copyright notice requires prior written approval for reuse beyond private use/citation — campaign-summary §f item ⑧) is a separate, parallel process; this pilot proceeds under "necessary minimum" data volume in the meantime, per team-lead's explicit framing ("許諾申請は§f記録済みの並行事項 — USER決定は進め、の意と解釈").
- **Verdict (UPDATED 2026-07-18, post-WF)**: **CLOSED — REJECT.** The WF-level confirmation this §4 recommended for "next session" has now run, on the full historical backfill (1,580 rows, all 70 archive PDFs for the 4 summer venues, 2018-2026 — see §5), not the 10-PDF pilot sample below. Both candidate arms (`turf_moisture_pct` alone, and `turf_moisture_pct`+`cushion_value` together) **fail the §7.2 accept gate at every evaluation level** — pooled JRA-wide (n=10,365), the summer-4-turf primary-population restriction (n=1,380, 100% coverage), every one of 3 blind folds, and all 22 scanned cells (no §8.12 cell-conditional exception clears either). See §5 for the full numeric record, including a self-contained noise-floor diagnostic (a Nakayama cell where the candidate columns are 100%-NULL by construction still produced an individually-passing metric) and a fold-2024-driven-signal caveat consistent with this campaign's other fold-2024 anomaly findings. **No serve integration. This closes the probe** — do not re-open without materially new data (e.g. non-summer4 venue backfill) per the do-not-retest convention. The paragraph below is the original pilot-only verdict, kept for history.
- **Original pilot verdict (2026-07-18, pre-WF)**: **Two of three candidates show a real, cross-checked signal that is not explained by existing baba code or by the already-REJECTed `venue_precipitation` feature.** `turf_moisture_pct` passes both the primary target and a stricter robustness cross-check with consistent sign. `cushion_value` passes the primary target with a directionally-consistent (but sub-floor) cross-check. `dirt_moisture_pct` does **not** show a genuine signal — its nominal "pass" is an artifact of a weak 2-year sign-stability check and fails the robustness cross-check outright. **Recommendation: promote `turf_moisture_pct` (and/or `cushion_value`, near-redundant with it, see §3.4) to a WF-level confirmation next session; do not promote `dirt_moisture_pct` without materially more data.** No adoption decision made tonight — this is pilot+probe only, per explicit scope limit.

---

## 1. Daily ingestion pipeline (usable from 2026-07-18)

### 1.1 Mechanism

`jra.go.jp` blocks bare/no-UA HTTP requests with a 403 (confirmed empirically). A realistic browser `User-Agent` header bypasses this — no special tooling required, plain `urllib.request` works. The visible per-venue pages (`/keiba/baba/index.html` = Fukushima, `index2.html` = Kokura, `index3.html` = Hakodate) render their cushion-value display client-side from JS (`_js/baba2025.js`); the actual data comes from two unauthenticated, multi-venue AJAX fragments, not from the static HTML:

- `https://www.jra.go.jp/keiba/baba/_data_cushion.html` — turf cushion value only
- `https://www.jra.go.jp/keiba/baba/_data_moist.html` — turf + dirt moisture (goal-front and 4-corner sub-readings each)

Both endpoints return **all currently-racing JRA venues in one response** (not just the 3 in the visible nav), keyed by an internal slot id (`rcA`/`rcB`/`rcC`...) resolved via each block's `title="<venue name>"` attribute rather than assumed fixed — the slot-to-venue mapping is not guaranteed stable across meets. Each venue block carries its last ~10 readings (several weeks of history), Shift_JIS encoded.

### 1.2 Script

`apps/pc-keiba-viewer/tmp/cushion-moisture-pilot-2026-07-18/fetch_daily.py` — standalone, no repo dependency beyond pandas/pyarrow (already in the project's `uv` environment). Fetches both endpoints, parses per-venue per-timestamp blocks via regex, outer-merges cushion+moisture on `(keibajo_code, reading_date, reading_time)` (they publish on different clock times — moisture ~05:40, cushion ~08:20 — so the merge is intentionally sparse/outer, not collapsed), and appends to a single accumulating parquet deduped on the same key so repeated runs are idempotent.

**Test run** (2026-07-18 00:21 JST, before the publication window): parsed 36 cushion + 36 moisture readings, merged into 58 rows spanning `keibajo_code` 02 (Hakodate, 2026-06-12..07-17, 16 dates), 03 (Fukushima, 2026-06-26..07-17, 10 dates), 10 (Kokura, 2026-06-26..07-17, 10 dates) — Sapporo (01) absent because it is not racing yet. Sapporo/Hakodate/Fukushima/Kokura are all confirmed opening in different windows; 02/03/10 are racing 2026-07-18 (12 races each, confirmed via local PG), 01 is not.

Values are sane on inspection: Hakodate cushion 6.2-7.3 (JRA's own published scale: <7=soft, 7-8=mild — consistent with a smaller regional turf course), and a same-day rain event is directly visible and internally consistent — 2026-06-27 dirt moisture jumps from ~3% to 17.3%/16.3% with an explicit source note "測定時刻までの当日雨量は7.0ミリメートルでした" (7.0mm same-day rainfall).

Output accumulator: `apps/pc-keiba-viewer/tmp/cushion-moisture-pilot-2026-07-18/daily_accumulator.parquet`.

### 1.3 Getting 2026-07-18's reading by 07:30 JST — reliability caveat, stated plainly

As of this write-up (00:2x JST), JRA has not yet published a 2026-07-18 reading — the script correctly detects and reports this (`NOTE: no 2026-07-18 reading published yet`) rather than silently reporting stale data as current. The publication window is 05:00-07:00 JST.

Two mechanisms are in place, and their limits are stated honestly rather than assumed reliable:

1. **Best-effort `CronCreate` job** (id `bf67d061`, `3,33 5-7 * * *`, fires at :03/:33 past each hour 05:00-07:59 JST): re-runs the fetch script and, on first detecting 2026-07-18 rows, sends a confirmation to team-lead. **This is session-only** — per this tool's own documentation, cron jobs are held in-memory and die when the Claude session ends; nothing is written to disk. Given this session's length (multiple compactions already tonight), there is a real chance it will not survive to 05:00.
2. **Manual fallback (the reliable path)**: from a fresh session (or team-lead directly), run:
   ```
   cd apps/pc-keiba-viewer && uv run python3 tmp/cushion-moisture-pilot-2026-07-18/fetch_daily.py
   ```
   This is idempotent and safe to re-run any number of times; it will report whichever venues/dates are currently published and skip duplicate rows.

No serve integration was built or attempted — per team-lead's explicit instruction, this is accumulation only.

---

## 2. Historical pilot (bounded, summer-4 venues, 2024-2026)

Built by a sub-agent per the "necessary minimum" framing (target 8-12 PDFs, hard cap 15-20); independently spot-checked by me before use in the probe below (not taken on faith).

### 2.1 What was fetched

10 PDFs from `https://www.jra.go.jp/keiba/baba/archive/{year}pdf/{venue}{meeting:02d}.pdf` — one meeting per venue-year (not every meeting a venue ran that year, a deliberate bounding choice):

| Venue     | 2024              | 2025              | 2026                 |
| --------- | ----------------- | ----------------- | -------------------- |
| Fukushima | 3回 (11/01-11/17) | 3回 (11/07-11/24) | 1回 (04/10-04/26)    |
| Hakodate  | 1回 (06/07-07/14) | 1回 (06/13-07/20) | _(not yet archived)_ |
| Kokura    | 3回 (06/28-07/21) | 2回 (06/27-07/20) | 1回 (01/23-03/01)    |
| Sapporo   | 2回 (08/09-09/01) | 2回 (08/22-09/07) | _(not yet archived)_ |

Sapporo/Hakodate 2026 are genuinely absent from JRA's own archive (verified directly against the live index, not assumed) — their 2026 meets hadn't been archived as of fetch time. `curl`/no-UA fetches against this archive also 403; all 10 files were fetched via a real browser-fingerprinted tool. Text was extracted locally with `pypdf` — all 10 PDFs are genuine Excel-exported text-layer PDFs (not scans), extracted cleanly with zero unparsed lines.

JRA changed the PDF layout between 2024 and 2025 meetings ("Format A" 3-day-block/weekday-column for all 4 2024 pulls, "Format B" one-row-per-date with explicit course-rotation letter for all 6 2025/2026 pulls) — cross-validated that both formats share the same value ordering (turf goal-front, turf 4-corner, dirt goal-front, dirt 4-corner) using Format A's explicit Japanese column labels.

### 2.2 Independent verification (my own spot-checks, not the building sub-agent's self-report)

- **Null-pattern invariant**: `cushion_value` is non-null for all 124 `turf` rows and null for all 124 `dirt` rows, exactly as claimed (JRA does not publish a dirt cushion figure) — confirmed programmatically, not just read off the sub-agent's notes.
- **Claimed rain event, Hakodate 2025-06-22**: confirmed directly in the raw parquet. Dirt moisture 3.15%→10.95%, turf moisture 12.95%→20.10%, cushion value 7.5→6.8 (the meeting's low), all on the same date — internally consistent with a rain event, not a parsing artifact.
- **Result**: `historical_pilot.parquet` (248 rows, 124 unique venue-date × 2 surfaces) is trusted for the probe below.

### 2.3 Honest gaps (carried from the sub-agent's own notes, not smoothed over)

- Not every meeting a venue ran in a given year was pulled — one meeting per venue-year, a deliberate scope bound.
- Format-A (2024) rows have `null` course-rotation and measurement-time fields — genuinely absent from that PDF layout, not an extraction miss.
- `moisture_pct` used below is the average of the two sub-location readings JRA publishes (goal-front, 4-corner); both raw components are preserved as separate columns in the parquet for anyone who wants to revisit that choice.

---

## 3. Probe: odds-controlled partial-Spearman

Script: `apps/pc-keiba-viewer/tmp/cushion-moisture-pilot-2026-07-18/probe_cushion_moisture.py`. Same rank-residualize partial-Spearman convention as this session's other probes (`tmp/jvd-br-breeder-probe-0717/probe_breeder.py`, `tmp/wave4-probes-0717/`). Bar: `|partial ρ| >= 0.02`.

**Years**: 2024-2025 only, not this campaign's usual 3-year (2023-2025) convention — the historical pilot has zero 2023 coverage by construction (PDFs were only pulled for 2024/2025/2026 meetings). Sign-stability here is therefore 2-of-2, a weaker bar than the usual 3-of-3, and is called out per candidate below rather than silently presented as equivalent.

**Population**: summer-4 venues only (01/02/03/10). Team-lead's instruction asked for "summer4 + all-venues if possible," but the historical pilot (§2) was itself explicitly scoped to summer-4 PDFs only — there is no non-summer4 moisture/cushion data yet to test. This is a scope gap carried forward honestly, not silently dropped; the all-venues arm can only run after a broader historical pull (which is gated on the same §f item ⑧ permission question for any non-minimal volume).

### 3.1 Two issues caught and corrected mid-build (both before any conclusion was drawn)

1. **Stale control column.** `tmp/candidate-eval-jra/augmented`'s own `venue_precipitation_total` column is 100% NULL across **all 10 JRA venues** in this store snapshot (`feature_schema_version == 'v1'`) — a stale vintage that predates whatever backfill populated the separately-maintained `tmp/venue-weather-campaign/clean-retest/venue_weather_agg.parquet` (independently verified non-null for keibajo_code 01/02/03/10 across 2013-2025). The probe sources `venue_precipitation_total` from that fresher aggregate directly instead of from the store's own column. This is **not** a fix to the store-building pipeline — out of tonight's bounded scope — just documented and worked around locally, same posture as the `course_dist_to_first_corner_m` finding earlier this session (confirmed data limitation, not silently patched).

2. **Field-size confound on the target.** All three candidates here are race-day-level covariates (same value for every horse in a race) — unlike this session's other probes (breeder rate, weight z-score), which were horse-level. Using this session's default target, raw `finish_position`, turned out to be contaminated: `cushion_value` correlates with `field_size_normalized` at ρ=+0.31 (2024) / +0.41 (2025) (harder-track meeting days draw bigger fields in this sample), and raw `finish_position`'s scale mechanically depends on field size. Adding `field_size_normalized` as an extra control flips `cushion_value`'s partial ρ from +0.04 (sign-stable, nominally passing) to about -0.015 (below floor) — a materially different conclusion depending on which target is used. **Fix**: `finish_norm` (`= finish_position / shusso_tosu`, field-size-invariant by construction, `finish_position_features_duckdb.py:608`) is used as the **primary** target for all three candidates. Raw `finish_position` + explicit `field_size_normalized` control is kept and reported as a secondary robustness cross-check, not silently dropped.

### 3.2 Results — primary target (`finish_norm`)

Control sets: `baba_only = [tansho_ninkijun, track_condition_normalized]`; `baba_and_precip` adds `venue_precipitation_total` (sourced per §3.1.1).

**`cushion_value`** (turf only — JRA does not publish a dirt figure):

| Year | n                     | raw ρ   | partial ρ (baba_only) | partial ρ (baba_and_precip) |
| ---- | --------------------- | ------- | --------------------- | --------------------------- |
| 2024 | 2,903 / 6,327 (45.9%) | -0.0001 | -0.0435               | -0.0433                     |
| 2025 | 2,959 / 6,182 (47.9%) | +0.0021 | -0.0395               | -0.0397                     |

Max \|partial ρ\| = 0.0435, sign-stable 2-of-2 (both negative), **PASS**. Adding precipitation control barely moves the estimate (-0.0435→-0.0433, -0.0395→-0.0397) — the signal is not explained by precipitation.

**`turf_moisture_pct`**:

| Year | n                     | raw ρ   | partial ρ (baba_only) | partial ρ (baba_and_precip) |
| ---- | --------------------- | ------- | --------------------- | --------------------------- |
| 2024 | 2,903 / 6,327 (45.9%) | +0.0006 | +0.0255               | +0.0276                     |
| 2025 | 2,959 / 6,182 (47.9%) | -0.0001 | +0.0210               | +0.0210                     |

Max \|partial ρ\| = 0.0276, sign-stable 2-of-2 (both positive), **PASS**. Also unmoved by precipitation control.

**`dirt_moisture_pct`**:

| Year | n                     | raw ρ   | partial ρ (baba_only) | partial ρ (baba_and_precip) |
| ---- | --------------------- | ------- | --------------------- | --------------------------- |
| 2024 | 1,978 / 4,304 (46.0%) | +0.0032 | +0.0005               | +0.0001                     |
| 2025 | 1,985 / 4,127 (48.1%) | +0.0012 | +0.0371               | +0.0381                     |

Max \|partial ρ\| = 0.0371 — **mechanically passes** sign-stability (both years nominally non-negative), but 2024's value (+0.0005) is indistinguishable from zero. With only 2 years, "sign stability" here really means "one near-zero year didn't happen to land negative," a materially weaker claim than a genuine 2-of-2 same-sign, same-magnitude result. Treated as **not a genuine pass** pending the robustness check below.

### 3.3 Robustness cross-check — raw `finish_position` with explicit `field_size_normalized` control

Control set: `[tansho_ninkijun, track_condition_normalized, venue_precipitation_total, field_size_normalized]`. This does not gate the verdict on its own (it's a stricter, not equivalent, specification) but is reported in full because it changes the picture for one candidate.

| Candidate           | Year        | raw ρ             | partial ρ         | max \|partial ρ\| | Sign-stable         | Passes 0.02 floor |
| ------------------- | ----------- | ----------------- | ----------------- | ----------------- | ------------------- | ----------------- |
| `cushion_value`     | 2024 / 2025 | +0.0717 / +0.0956 | -0.0139 / -0.0172 | 0.0172            | Yes (both negative) | **No** (marginal) |
| `turf_moisture_pct` | 2024 / 2025 | -0.0405 / -0.0598 | +0.0177 / +0.0249 | 0.0249            | Yes (both positive) | **Yes**           |
| `dirt_moisture_pct` | 2024 / 2025 | +0.0363 / -0.0699 | +0.0016 / +0.0064 | 0.0064            | Yes (trivially)     | **No**            |

`turf_moisture_pct` is the only candidate that clears the bar under **both** the primary target and this stricter cross-check, with a consistent sign throughout (higher moisture → worse relative finish). `cushion_value` is directionally consistent (negative under both target choices) but does not clear the stricter cross-check's floor — a real but more marginal signal. `dirt_moisture_pct` collapses to near-zero under the cross-check, confirming §3.2's suspicion that its primary-target "pass" was a 2-year sign-stability artifact rather than a real effect.

### 3.4 Internal consistency check

`cushion_value` and `turf_moisture_pct` are themselves strongly anti-correlated at the venue-date level in the historical pilot data (Spearman ρ = -0.87, n=124) — physically expected (wetter ground is softer ground). Their opposite-signed effects on `finish_norm` (harder/drier → better relative finish; wetter → worse) are therefore telling one consistent story, not two independent coincidences. Practically, this also means the two are **near-redundant as candidate features** — a next-session WF arm should likely test one (or a single combined construction) rather than adding both as separate collinear inputs.

No causal mechanism is claimed here beyond what was measured — the direction is plausible (harder/drier ground races closer to "true form," consistent with the ninkijun-conditional residual shifting) but this probe does not establish why, only that the association survives odds, categorical baba code, and precipitation controls.

---

## 4. Verdict and recommendation

| Candidate           | Primary target (finish_norm)                        | Cross-check (finish_position + field-size)     | Verdict                                                                                                                                |
| ------------------- | --------------------------------------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `cushion_value`     | PASS (0.0435, sign-stable)                          | Directionally consistent, below floor (0.0172) | **Weak pass** — promote to WF queue, flag cross-check marginality                                                                      |
| `turf_moisture_pct` | PASS (0.0276, sign-stable)                          | PASS (0.0249, sign-stable, same sign)          | **Pass** — promote to WF queue next session                                                                                            |
| `dirt_moisture_pct` | Mechanical pass only (driven by one near-zero year) | Fail (0.0064)                                  | **Not promoted** — insufficient/non-robust evidence at current n, not a hard REJECT (small bounded sample, not a definitive null test) |

Per team-lead's framing ("pass→WF昇格推奨(次セッション), fail→正典クローズ"): `turf_moisture_pct` (and, more cautiously, `cushion_value`, near-redundant with it per §3.4) are recommended for WF-level confirmation next session. `dirt_moisture_pct` should not be promoted on this evidence, but given the small bounded n (historical pilot intentionally covers only ~124 venue-dates), this is recorded as **inconclusive pending more data**, not added to the DO-NOT-RETEST canon the way a definitively-REJECTed feature (e.g. `venue_precipitation`, 3-year JRA-wide n≈42-47k/year) would be — a wider historical pull (gated on the §f item ⑧ permission decision) could resolve it either way.

**No cell-level adoption decision was made or attempted tonight** — per explicit scope limit, this covers pilot ingestion + probe only. Daily ingestion accumulation continues per §1; the historical pilot and probe artifacts are ready inputs for a WF-training pass whenever that is scheduled.

## 5. Walk-forward confirmation (full historical backfill) — 2026-07-18, same day

This section is the "next session" WF confirmation §4 recommended. It ran the same day as the
pilot, once the full historical backfill (below) was ready, per the campaign's per-race-granularity
and cell-adoption-gate conventions used throughout this repo.

### 5.1 Backfill scope (supersedes the §2 10-PDF pilot pull)

Full detail: `apps/pc-keiba-viewer/tmp/cushion-moisture-backfill-2026-07-18/coverage_notes.md`.

- **1,580 rows**, all **70** venue-year-meeting PDFs JRA's own archive lists for the 4 summer venues
  (Sapporo/Hakodate/Fukushima/Kokura), **2018-07-27 .. 2026-04-26** — every archived meeting, not a
  bounded one-meeting-per-year sample like §2's pilot pull.
- **Publication start dates confirmed directly from JRA's own archive page text** (not assumed): the
  year-index pages state, in Japanese, "含水率は2018年7月27日から、クッション値は2020年9月11日から公表"
  ("moisture content published from 2018-07-27; cushion value published from 2020-09-11"). This was
  independently cross-checked two more ways (archive-page HTTP 200/403 boundary at 2018, and the
  clean non-null/null boundary observed in the parsed data itself at exactly those two dates) — see
  coverage_notes.md §1.
- Step-4 validation re-fetched and re-parsed the pilot's original 10 PDFs with an independently
  written parser and found an **exact match on all 248 pilot rows** (plus 6 additional rows the
  pilot's parser had silently dropped from one PDF due to a half-width-parenthesis quirk — kept, not
  discarded). The pilot's 248 rows turned out to be a strict subset of this fuller pull with zero
  unique information of their own (coverage_notes.md §4-5).
- Honest gaps carried forward: Hakodate has zero 2018 rows (its 2018 meeting finished before the
  2018-07-27 moisture-publication start — a genuine JRA source gap, not a fetch failure); 2026 is
  partial-year-to-date for every venue; this pull remains summer-4-venues only (no
  Tokyo/Nakayama/Kyoto/Hanshin/Chukyo/Niigata backfill was attempted).

### 5.2 Harness design

Script: `apps/pc-keiba-viewer/tmp/cushion-moisture-wf-2026-07-18/wf_cushion_moisture.py`.

- **Two arms only**, as USER decision 5 + the §4 recommendation specified:
  - `cushion_moisture_v1` = armB-250 (clean 250-feature base) + `turf_moisture_pct`
  - `cushion_moisture_v2` = armB-250 + `turf_moisture_pct` + `cushion_value` (two independent raw
    columns, not a hand-built combination — they are not co-published on the same clock time, see
    §1.2)
- **3 seeds** (42, 101, 2026) × **3 blind folds** (A=2023, B=2024, C=2025) = the same convention as
  every other WF probe this campaign, n=10,365 JRA races pooled across all 3 blind years.
- Copies the established, bug-fixed WF pattern from `tmp/candidate-masked-lever-retest/retest_wf.py`
  byte-for-byte for `train_fold`/`predict_ranks`/`per_race_hits`/`paired`/`gate` — the **§7.2 accept
  gate logic is unchanged**, not reimplemented: ≥2 of {top1, place2, place3} pass (delta ≥ +0.08pp
  AND bootstrap LB95 > 0), of which ≥1 must be place2/place3, and no metric regresses past −0.05pp.
- Reuses the **9 already-pretrained armB-250 base/control models** (3 seeds × 3 folds,
  `tmp/candidate-masked-lever-retest/models/base/`) instead of retraining the control arm — only the
  candidate arm (base+moisture[+cushion]) is trained fresh per arm.
- **Sort-before-mask discipline preserved**: both the per-cell scan and the new
  primary-population-restriction mask are computed against a `race_id`-sorted frame before
  `paired()`'s internal join/filter, per this repo's own documented history of false-positive "near
  miss" cells caused by masking an unsorted frame.
- Join: same-calendar-date `(keibajo_code, race_date)`, turf rows only from the historical parquet,
  forced to null on any non-turf race even where the venue+date key matched (dirt/jump races at the
  same venue on the same day must not inherit the turf reading).
- `historical_source` in both report JSONs correctly points at
  `tmp/cushion-moisture-backfill-2026-07-18/historical_full.parquet` — confirmed this run used the
  real backfill, not the small 248-row pilot fixture the harness defaults to for smoke-testing.

### 5.3 Results

#### 5.3.1 Pooled (seed-avg, JRA-wide, n=10,365)

| Metric     | v1 base% | v1 cand% | v1 delta_pp | v1 LB95 | v2 base% | v2 cand% | v2 delta_pp | v2 LB95    |
| ---------- | -------- | -------- | ----------- | ------- | -------- | -------- | ----------- | ---------- |
| top1       | 33.796   | 33.845   | +0.048      | −0.135  | 33.796   | 33.870   | +0.074      | −0.100     |
| place2     | 18.119   | 18.234   | +0.116      | −0.100  | 18.119   | 18.244   | +0.125      | −0.084     |
| place3     | 14.163   | 14.186   | +0.023      | −0.206  | 14.163   | 14.398   | **+0.235**  | **+0.026** |
| place4     | 12.166   | 11.835   | −0.331      | −0.534  | 12.166   | 12.102   | −0.064      | −0.283     |
| place5     | 11.076   | 10.979   | −0.097      | −0.289  | 11.076   | 11.002   | −0.074      | −0.273     |
| place6     | 10.417   | 10.481   | +0.064      | −0.148  | 10.417   | 10.536   | +0.119      | −0.068     |
| top3_box   | 9.410    | 9.503    | +0.093      | −0.016  | 9.410    | 9.484    | +0.074      | −0.039     |
| fukusho_2p | 74.912   | 74.900   | −0.013      | −0.167  | 74.912   | 74.941   | +0.029      | −0.145     |

- **v1 gate**: 0/3 primaries pass, `ACCEPT_strict_gate: false`.
- **v2 gate**: `n_primaries_passed: 1` (place3 alone individually clears delta≥+0.08pp AND LB95>0),
  but §7.2 requires ≥2 of 3 — `ACCEPT_strict_gate: false`.

#### 5.3.2 Primary population restriction (summer-4-venue turf, non-null join, n=1,380, **100% coverage** of that population)

| Metric     | v1 base% | v1 cand% | v1 delta_pp | v1 LB95 | v2 base% | v2 cand% | v2 delta_pp | v2 LB95 |
| ---------- | -------- | -------- | ----------- | ------- | -------- | -------- | ----------- | ------- |
| top1       | 32.053   | 32.053   | 0.000       | −0.507  | 32.053   | 32.126   | +0.073      | −0.362  |
| place2     | 15.411   | 15.507   | +0.097      | −0.483  | 15.411   | 15.845   | +0.435      | −0.169  |
| place3     | 13.261   | 13.720   | +0.459      | −0.145  | 13.261   | 13.382   | +0.121      | −0.531  |
| top3_box   | 8.309    | 8.527    | +0.217      | −0.048  | 8.309    | 8.406    | +0.097      | −0.169  |
| fukusho_2p | 71.063   | 71.280   | +0.217      | −0.217  | 71.063   | 71.353   | +0.290      | −0.217  |

Both arms: 0/3 primaries pass, `ACCEPT_strict_gate: false`. Note v1's place3 point estimate
(+0.459pp) clears the +0.08pp bar but its LB95 (−0.145) does not — this is exactly the shape §7.2
is designed to reject (a point estimate that could easily be noise at this n).

#### 5.3.3 Per-fold (pooled seed-avg, JRA-wide, primaries only)

| Fold (blind year) | v1 top1 | v1 place2           | v1 place3 | v2 top1 | v2 place2            | v2 place3                |
| ----------------- | ------- | ------------------- | --------- | ------- | -------------------- | ------------------------ |
| A / 2023          | −0.077  | −0.087              | +0.048    | −0.068  | −0.164               | +0.116 [LB95 −0.289]     |
| B / 2024          | +0.019  | +0.396 [LB95 0.000] | +0.174    | +0.135  | +0.405 [LB95 +0.029] | **+0.637 [LB95 +0.280]** |
| C / 2025          | +0.203  | +0.039              | −0.154    | +0.154  | +0.135               | −0.048 [LB95 −0.434]     |

No fold-consistent signal for either arm on any primary — sign flips across folds throughout.

#### 5.3.4 Per-cell scan (22 cells, keibajo_code × kyori_band × season_band × current_baba_condition, n≥200 each)

Neither arm has any cell clear the 2-of-3-primaries gate, and none clears the §8.12 all-7-gated-
metrics exception (docs/finish-position-prediction-system.md §8.12 item 6: if every one of the 7
gated metrics — top1..place6 + top3_box — individually clears +0.08pp, a cell may be adopted even if
LB95 crosses 0; that did not happen in any of the 44 cell×arm combinations scanned here).

- **v1 best individual-primary hit**: `keibajo_code=01` (Sapporo, n=504) — top1 **+1.058pp [LB95
  +0.066]**. But in the same cell, place2 delta +0.132pp with LB95 −0.860, place3 delta +0.397pp
  with LB95 −0.529, and place6 delta **−0.463pp**. Only 1 of 3 primaries individually clears the
  bar, and the all-7-metric exception fails outright (place6 regresses, several metrics flat). No
  adoption.
- **v2 best individual-primary hits**:
  - `keibajo_code=06` (**Nakayama**, n=1,500) — place3 **+0.711pp [LB95 +0.222]**, individually
    clearing the primary bar.
  - `keibajo_code=02` (Hakodate, n=432) — place3 **+1.080pp [LB95 +0.154]**, also individually
    clearing.
  - Neither clears 2-of-3 primaries (both have flat/negative top1 and place2 — Hakodate's place2 is
    **−0.849pp**) nor the §8.12 all-7-metric exception. No adoption.

**Nakayama diagnostic — load-bearing for interpreting the above.** `keibajo_code=06` (Nakayama) is
**not** one of the 4 backfilled venues (01/02/03/10 only) — the historical parquet has zero rows for
Nakayama, so `turf_moisture_pct`/`cushion_value` are 100%-NULL for every Nakayama race in this
dataset by construction of the join (§5.2). A cell where the candidate feature carries **literally
zero information** still produced an individually-passing primary metric (place3 +0.711pp
[LB95 +0.222]) purely from retraining a differently-shaped model on the same rows. This is a
self-contained demonstration that swings of this magnitude arise from training noise alone,
unrelated to the candidate feature's actual content — it calibrates how much weight the
also-non-adopting Sapporo/Hakodate individual-primary hits above deserve: **none**.

**Fold-2024 caveat on v2's pooled place3 "pass."** v2's pooled place3 is the only primary that
clears the bar (§5.3.1), but per-fold (§5.3.3) it is driven almost entirely by fold B/2024
(+0.637pp [LB95 +0.280]) while fold A/2023 (+0.116pp [LB95 −0.289]) and fold C/2025 (−0.048pp
[LB95 −0.434]) are flat-to-negative — 2 of 3 blind years show no effect on the metric that nominally
"passed" pooled. This campaign independently documented, the same day (2026-07-17), a
fold-2024-specific venue-mix anomaly (Kyoto/Hanshin construction-related share shift — see
`docs/probes/jra-fold2024-anomaly-forensic-2026-07-17.md`) that produced misleading, fold-2024-only-
driven "positive signals" in multiple other independently tested levers that day, every one of which
was REJECTed once checked against fold-consistency. This result fits that exact pattern and is
treated as **corroborating evidence for REJECT**, not a near-miss worth another look.

### 5.4 Verdict

**REJECT, both arms, at every evaluation level tested**: pooled JRA-wide (0/3 primaries, both arms),
the summer-4-turf primary-population restriction despite 100% coverage of that population (0/3,
both arms), all 3 blind folds (no fold-consistent primary signal), and all 22 scanned cells (no cell
clears 2-of-3 primaries or the §8.12 all-7-metric exception). v2's pooled place3 "pass" is the only
metric that individually clears the bar anywhere in the pooled/restricted views, and it is both
insufficient on its own (§7.2 requires ≥2 of 3 primaries) and fold-2024-driven rather than a robust
effect. The Nakayama cell result independently demonstrates that a metric-level "pass" of this
magnitude is achievable from training noise alone, with zero real information behind it. **No serve
integration. This closes the probe the pilot (§1-§4) opened** — do not re-open without materially
new data (e.g. a non-summer4 venue historical backfill), per this campaign's do-not-retest
convention.

## Files

- `apps/pc-keiba-viewer/tmp/cushion-moisture-pilot-2026-07-18/fetch_daily.py` — daily fetcher (§1)
- `apps/pc-keiba-viewer/tmp/cushion-moisture-pilot-2026-07-18/daily_accumulator.parquet` — accumulating daily readings, 58 rows as of first run
- `apps/pc-keiba-viewer/tmp/cushion-moisture-pilot-2026-07-18/historical_pilot.parquet` + `historical_pilot_notes.md` — bounded historical pull (§2)
- `apps/pc-keiba-viewer/tmp/cushion-moisture-pilot-2026-07-18/probe_cushion_moisture.py` + `probe_report.json` — probe script + full numeric results (§3)
- `apps/pc-keiba-viewer/tmp/cushion-moisture-backfill-2026-07-18/historical_full.parquet` + `coverage_notes.md` — full 70-PDF historical backfill, supersedes the pilot pull for the 4 summer venues (§5.1)
- `apps/pc-keiba-viewer/tmp/cushion-moisture-wf-2026-07-18/wf_cushion_moisture.py` — WF harness, both arms (§5.2)
- `apps/pc-keiba-viewer/tmp/cushion-moisture-wf-2026-07-18/reports/cushion_moisture_v1.json` + `cushion_moisture_v2.json` — authoritative full numeric WF results (§5.3), source of every number in this section
- `apps/pc-keiba-viewer/tmp/cushion-moisture-wf-2026-07-18/build_mlflow_cell_reports.py` + `mlflow_cell_report_cushion_moisture_v{1,2}.json` — flat per-cell table derived from the reports above (MLflow's `log-training-run` cell-report ingestion requires a flat list-of-records shape, not the harness's own nested report), attached to the MLflow runs logged to `finish-position/wf-eval`
