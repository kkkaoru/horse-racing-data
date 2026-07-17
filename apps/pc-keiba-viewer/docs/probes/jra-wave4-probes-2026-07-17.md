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

## Artifacts

`apps/pc-keiba-viewer/tmp/wave4-probes-0717/probe_two_candidates.py`, `probe_report.json` (full per-year detail, coverage-by-venue table, realized venue composition).
