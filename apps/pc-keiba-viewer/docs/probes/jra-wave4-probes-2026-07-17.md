# Wave-4 probes: weight_zscore (lever bank #4) + gate-geometry — both REJECT

- **Date**: 2026-07-17
- **Category**: JRA finish-position, two lightweight probe-first candidates assigned by team-lead.
- **Verdict**: **Both REJECT at probe stage.** Lever bank #4 (`weight_zscore`) closes cleanly, completing full lever-bank consumption. Gate-geometry closes per its own pre-registered "probe fail → immediate close" rule — no WF slot used for either.
- **Method**: odds-controlled partial Spearman (rank-residualize candidate and target on a control set, Pearson of residuals), bar `|partial ρ| >= 0.02` and 3-year (2023/2024/2025) sign stability, same convention as this campaign's other probe-first candidates (`apps/pc-keiba-viewer/tmp/venue-jockey-probe/probe_partial_rho.py`). Source: `tmp/candidate-eval-jra/augmented`, `n≈47,000/year` JRA-wide before per-probe `dropna`. Script/report: `apps/pc-keiba-viewer/tmp/wave4-probes-0717/{probe_two_candidates.py,probe_report.json}`.

---

## Probe 1 — `weight_zscore` (lever bank #4, last unconsumed item)

**Dedup**: this is the final item in `apps/pc-keiba-viewer/tmp/frontier-scout/lever_bank.md`'s ranked bank (item #4) that had not yet been formally tested. The bank's own prior was explicit: "almost certainly redundant — arithmetically close to `weight_diff_from_avg` normalized by `weight_volatility_5`, both already present [in the champion], so a tree ensemble likely already reconstructs the same information (same reasoning that killed `weight_diff_distance_interaction`/`weight_diff_grade_interaction`, REJECT, 2026-07-02 block)." Not previously probed or WF-tested; genuinely open only in the sense of never having been formally closed.

**Control**: `tansho_ninkijun` (odds) + `weight_diff_from_avg` (nearest already-in-champion weight feature, per the bank's own redundancy hypothesis).

| Year | n      | raw ρ   | partial ρ |
| ---- | ------ | ------- | --------- |
| 2023 | 42,564 | -0.0304 | +0.0039   |
| 2024 | 41,909 | -0.0263 | -0.0008   |
| 2025 | 42,601 | -0.0289 | -0.0028   |

Max \|partial ρ\| = 0.0039 (5x below the 0.02 bar), **sign unstable** (2023 positive, 2024/2025 negative). Raw Spearman is real and consistently negative (-0.026 to -0.030, heavier-than-average horses finish slightly better) but collapses almost entirely once `weight_diff_from_avg` is controlled for — exactly the redundancy the bank predicted, confirmed empirically rather than assumed. **REJECT, probe stage, DO-NOT-RETEST.**

**This closes lever bank #4 and, with it, full consumption of `lever_bank.md`'s ranked list** (items #1 volatility-tiered-fusion and #2 season-conditional-jockey/trainer REJECTed 2026-07-11; #3 cross-pool-odds-divergence closed 2026-07-17 elsewhere this session; #4 here). No further bank items remain.

---

## Probe 2 — gate-geometry (waku × first-corner distance × field size)

**Hypothesis**: outer draw × short run to the first corner × large field = elevated "gate-squeeze" risk — horses drawn wide have less time and space to establish position before the first turn compresses the field into single file. Motivated by a structural fact from today's Sapporo diagnosis (`jra-sapporo-top1-deficit-diagnosis-2026-07-17.md` §3.1): Sapporo's distance to the first corner is 289.9m, versus 418.6m at Hakodate, 366.7m at Fukushima, 350.6m at Kokura — Sapporo horses reach the first turn ~130m sooner than any other summer venue, with field sizes (12-15 average) comparable across all 4.

**Dedup (per team-lead's explicit instruction to document this carefully, given family adjacency)**: this is NOT a restatement of either prior REJECT.

- `draw_ablation` (`apps/pc-keiba-viewer/docs/probes/jra-summer-venue-cell-focus-2026-07-04.md` and `tmp/candidate-masked-lever-retest/`) tested `wakuban` alone plus a `venue×dist` draw-zone-edge interaction — a 2-way construction with no course-geometry term.
- `meetingday×waku` (`jra-meetingday-waku-clean-2026-07-04.md`) tested waku crossed with meeting-day lateness — a different second variable entirely (calendar position within the meet, not physical course layout).
- This candidate is a **genuine 3-way physical-geometry interaction** (draw position × actual measured course distance-to-first-corner × field size) that neither prior construction contains. `course_dist_to_first_corner_m` is itself already a champion feature (confirmed via `metadata.json`, present in all 250), so the novelty claim rests entirely on the _interaction_ carrying signal beyond the marginal main effects — which is exactly what the control set below is designed to test, not just odds.
- Given the family adjacency, team-lead's own framing set a low prior and a strict promotion bar (magnitude + sign-stability + summer-4 reproduction) — both were applied.

**Construction**: `wakuban` is not present in the augmented store (only `umaban`, which is finer-grained and is itself a champion feature); draw position uses `draw_pct = (umaban-1)/(shusso_tosu-1)` (0=innermost, 1=outermost), comparable across field sizes. `gate_geometry_risk = draw_pct × shusso_tosu / course_dist_to_first_corner_m`. Target: `finish_norm` (chakujun/field_size), chosen over raw `finish_position` for field-size comparability, matching this campaign's convention for field-size-sensitive candidates (e.g. the cross-pool-divergence probe). Control set: `[tansho_ninkijun, umaban, shusso_tosu, course_dist_to_first_corner_m]` — odds plus all 3 raw components of the interaction, so a pass would reflect interaction-specific signal, not a rediscovery of an already-tested or already-in-model main effect.

**Data-quality caveat, found before interpreting anything**: `course_dist_to_first_corner_m` has a severe, venue-skewed coverage gap in this store — 31.6% overall, ranging from 0% at Chukyo(07)/Kyoto(08) and 13% at Tokyo(05)/Hanshin(09) to 76% at Nakayama(06). The 4 summer venues sit in a comparable 47-64% band (Sapporo 48%, Hakodate 47%, Fukushima 49%, Kokura 64%), so a summer-4-restricted arm is meaningfully interpretable, but a naive "JRA-wide" arm is not representative of JRA once restricted to non-null rows — realized venue composition after `dropna` skews toward Nakayama/Kokura/Niigata and structurally excludes Chukyo/Kyoto entirely. Both arms are reported; **the summer-4 arm is treated as primary** given this skew and given summer-4 is the population the hypothesis is actually about. This is a data-completeness note about the existing store column, not something built or fixed for this probe.

**Results — JRA-wide** (realized venue mix, not representative JRA-wide — reported for completeness):

| Year | n      | raw ρ   | partial ρ |
| ---- | ------ | ------- | --------- |
| 2023 | 14,956 | -0.0026 | -0.0213   |
| 2024 | 14,328 | +0.0077 | +0.0110   |
| 2025 | 14,375 | +0.0016 | +0.0187   |

Max \|partial ρ\| = 0.0213 (nominally clears the 0.02 bar) but **sign flips** between 2023 (negative) and 2024/2025 (positive) — fails sign-stability outright.

**Results — summer-4-venue restricted** (the population the hypothesis is actually about):

| Year | n     | raw ρ   | partial ρ |
| ---- | ----- | ------- | --------- |
| 2023 | 5,878 | -0.0044 | -0.0157   |
| 2024 | 6,005 | +0.0613 | -0.0055   |
| 2025 | 5,769 | +0.0428 | +0.0199   |

Max \|partial ρ\| = 0.0199 (just under the 0.02 bar) and **also sign-unstable** (negative 2023/2024, positive 2025). Neither arm clears both the magnitude and sign-stability bars simultaneously; the summer-4 arm — the one that actually matters for this hypothesis — fails both criteria outright. **REJECT, probe stage.**

**Verdict**: per the pre-registered protocol ("probe fail → immediate close, WF promotion requires both 3-year sign stability and summer-4 reproduction"), this closes now. **DO-NOT-RETEST this exact construction** (`draw_pct × shusso_tosu / course_dist_to_first_corner_m`, controlling for odds + the 3 main effects). Not tested and left open if ever revisited: alternate functional forms of the same physical hypothesis (e.g. additive rather than multiplicative combination, decile-bucketed course-geometry tiers instead of a continuous ratio, or restricting to only the most extreme draw×geometry cells) — none were attempted here, consistent with the tight probe-stage budget and the low prior this family-adjacent hypothesis started with.

---

## Follow-up: `course_dist_to_first_corner_m` coverage-gap investigation (2026-07-17, ~40min)

Team-lead asked for a deeper look at the venue-skewed coverage gap surfaced above (Chukyo/Kyoto 0%, Nakayama 76%, overall 31.6%), specifically: provenance, train/serve parity, fillability, and — only if the cause turns out to be a join bug on otherwise-available data — a real fix with a regression test. Verdict up front: **not a join bug** (verified empirically below), so no code change was made; this section documents provenance/parity/fillability per the "document, don't fill+retrain today" instruction.

### 1. Provenance

`course_dist_to_first_corner_m` is one of 7 static course-physical attributes baked into `apps/pc-keiba-viewer/finish-position/lookups/course-numerical-features.parquet` (119 rows, keyed on `(keibajo_code, kyori, track_code)`), introduced by the iter14 model (`docs/finish-position-accuracy/history/iter14.md`, commit `f58cdc94`, +0.16pp top1/place3 accept — the _first_ JRA accept since iter9). Per that doc, the lookup was built via **regex extraction of `pg.jvd_cs.course_setsumei`** — a JV-Data table carrying a free-text Japanese course-description field per `(keibajo_code, kyori, track_code)` — not a structured numeric source, and not computed at feature-build or serve time (`add-course-numerical-features.py`'s own docstring: "no PG read is needed" — it only reads the pre-baked parquet). The build script that ran the regex itself no longer exists in the repository; only its output (the parquet) and the iter14 build-note doc survive.

**Why Chukyo(07)/Kyoto(08) are 0% specifically for this one column, while other course columns on the same rows are partially populated**: confirmed empirically, not assumed — the join is not the problem. Directly compared raw `(keibajo_code, kyori, track_code)` keys between the augmented store and the lookup parquet for venues 07/08: **24 keys intersect correctly** (e.g. `('07', 1400, '23')`, `('08', 1200, '17')`), ruling out a type/encoding mismatch. Inspecting the lookup's own content for those 34 combined rows (07: 19, 08: 15) shows `course_dist_to_first_corner_m` is NaN for **all 34**, while `course_elevation_diff_m` is populated for 63%/33% of the same rows respectively — the join succeeds and delivers the row, but the source table's own content for this specific attribute was never extracted for these two venues. Reading the raw `course_setsumei` text directly (5-row spot check) surfaced the likely reason: at least one Chukyo course configuration (dirt 1000m) **starts the race already past the 2nd turn**, so the text naturally describes distance-to-corner as `"３コーナーまでは３８０ｍ"` ("380m to the 3rd corner") rather than referencing a "1st corner" at all — a regex anchored to a literal "第１コーナー" (corner #1) pattern would systematically return nothing for this and any similarly-shaped course, even though the semantically-equivalent measurement is present in the text under a different corner number. Other spot-checked rows (07/1200/track11 ×2 variants, 07/1300/track11) genuinely state no explicit corner-distance figure at all (only qualitative "十分な距離" phrasing or unrelated slope-gradient distances) — a real source-text gap no regex could recover. **Net: this is a regex-extraction limitation with a partially-identified mechanism (course-specific corner-numbering), not a join/key defect** — it does not meet the "join miss on otherwise-joinable data" bar that would trigger a same-day fix+test.

### 2. Train/serve parity

**Confirmed harmless by construction**, not just "probably fine": `apps/finish-position-predict-container/Dockerfile` does `COPY apps/pc-keiba-viewer/finish-position/lookups /app/lookups`, baking the exact same committed file used to build the offline training store directly into the serving image (`predict_lib.pipeline_args.COURSE_LOOKUP_PATH = /app/lookups/course-numerical-features.parquet`, same file, not a mirrored/duplicated copy). Train-side and serve-side read byte-identical content — there is no separate serve-time computation path that could drift from the training-time one, so the 31.6% NULL rate is seen identically by both. The only theoretical drift vector is editing the lookup file without a container redeploy, which is a generic any-baked-artifact risk, not something specific to this column. Additionally: **this exact 30.1% figure for `course_dist_to_first_corner_m` was already measured and documented at iter14's original build time** (`iter14.md`'s own coverage table matches almost exactly) — the champion has been trained on, and accepted with, this precise NULL rate since the feature was introduced. This is a known, long-standing, already-validated characteristic of the current champion, not a new or surprise regression.

### 3. Fillable?

**Partially, via improved regex — not via a different join** (the join is already correct). At least some rows have the underlying measurement present in `course_setsumei` but unextracted because of course-specific corner-numbering (see §1); other rows have no extractable figure in the source text at all regardless of regex quality. A full fillability estimate would require auditing all 119 rows' raw text (this investigation spot-checked 5), which was not attempted given the time budget. **No fill or regex change was made today**, consistent with instructions — filling this (even without retraining) would silently change what the live serving container sees for every JRA race going forward, which is exactly the kind of ungated change to a production artifact this system prohibits without a validated accept-gate pass.

**Requirements for a future pass** (not executed): (a) locate/rewrite a `course_setsumei` regex that captures "distance to whichever corner number is first mentioned" rather than a literal corner-#1 anchor, informed by this investigation's finding about mid-oval start positions; (b) re-run the extraction across all 119 lookup rows and re-audit resulting coverage by venue; (c) re-bake `course-numerical-features.parquet`; (d) re-run the offline feature-store build and a full WF retrain to validate any actual accuracy change before touching the serving artifact — coverage improvement alone is not evidence of an accuracy gain.

**Expected effect — genuinely uncertain, not quantified today**: the only directly documented anchor is iter14's own historical accept gate, +0.16pp top1/place3 for the _entire_ 7-column course-numerical bundle at its original (multi-column) coverage rates. This investigation cannot isolate what fraction of that gain came specifically from `course_dist_to_first_corner_m` versus the other 6 columns, nor estimate incremental gain from improved coverage on this one column, without an actual ablation — that requires the retrain in (d) above, which is explicitly out of scope for today. Treat this as an open, plausible, unvalidated opportunity, not a promised win.

### 4. Verdict

No code change, no fix, no regression test — the join-bug condition that would have triggered one was checked and ruled out empirically. Filed as a documented future data-completeness → possible-retrain candidate per instructions, not executed.

---

## Artifacts

`apps/pc-keiba-viewer/tmp/wave4-probes-0717/probe_two_candidates.py`, `probe_report.json` (full per-year detail, coverage-by-venue table, realized venue composition). Follow-up investigation used ad-hoc read-only queries against the local Postgres mirror (`pg.jvd_cs`) and the committed lookup parquet directly; no new script artifact was produced for that section.
