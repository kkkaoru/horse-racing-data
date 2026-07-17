# going-nowcast lane assessment — 2026-07-17

- **Date**: 2026-07-17
- **Category**: JRA finish-position, dedup + probe of the one lane `project_weather_leak_clean_reconfirmed_2026_07_11`'s index summary records as "surviving" the venue-weather campaign closure.
- **Verdict**: **REJECT the specific construction assigned; the "surviving" lane it descends from was already substantially weakened by the same session that coined it.** Close, no WF slot used.

---

## 1. What "going-nowcast" actually refers to, and why it "survived"

**Primary source**: `apps/pc-keiba-viewer/tmp/venue-weather-campaign/lever-design/plan.md` ("rev 2" of the venue-weather lever-design plan, uncommitted — sits in `tmp/`, not `docs/probes/`, no formal REJECT doc was ever written for it). This is the direct antecedent of the memory index's "生存=going-nowcast" line; no other doc uses this term.

**(a) What it predicts**: not what the name suggests at first read. The plan's own rev-1 flagship was a much bigger claim — that JRA/NAR FP predictions might be silently scoring on a `0.0 = 良` (firm) placeholder for the target race's official baba/going code, rather than the real value, because the target race hasn't run yet. That would make a same-day empirical "nowcast" of the _true_ current track state a real exploitable edge (the model would be blind to it otherwise). **This flagship claim is explicitly refuted in the same document** (§0.1): a sibling's code audit (`finish_position_features_duckdb.py:641-648`) confirmed the official baba code is _not_ on the NULL-for-unrun-target-race guard list — it's read straight from the same `jvd_ra`/`nvd_ra` row already used for other race-day fields, no placeholder fallback, verified at the SQL level across every category branch, and confirmed available in local-PG well before any FP prediction window runs. **What survives is much smaller**: "Design 3, downgraded from rev-1's rank 1" — not "the model is blind to today's going," but "the single official announcement captured at the daily mirror sync could itself be a stale _morning_ reading that JRA revises intraday, so a handful of late-card races on rainy days could be scored on a stale-but-real (not placeholder) value."

**(b) Why it "survived" when the rest of the campaign died**: not because it showed positive evidence — because it was the one item [RA] (a red-team review in the same document) explicitly declined to fully refute, calling it "worth a footnote, not worth a lever." Everything else in the campaign (raw weather levels, hour-resolved precip/wind trajectories, meet-day cumulative precip, wind×venue-geometry, summer heat) was directly null-probed and closed in the same document (§ "Closed — do not re-pursue"). Going-nowcast's survival is a "not yet disproven" status, not a "showed promise" one.

**(c) What was left untested, and what the same document's own evidence already shows about the tested part**: the plan's own execution order (step 4) gates any nowcast MODEL behind a cheap descriptive frequency check first ("how often does in-racing-hours precip actually revise the official baba value... kill outright if revisions are rare"). A classifier was in fact already built and run in the same `tmp/` tree (`trackstate-probe/baba_nowcast.py` → `baba_nowcast_results.json`) — a 4-class ordinal model predicting the official baba class from precipitation trajectory features. Result, read directly from the JSON: turf race-day-only model, accuracy 0.8141 vs a trivial majority-class baseline of 0.7835 (≈3pp lift); the richer pre-race+day-of feature set, 0.8373 vs baseline 0.8172 (≈2pp lift). **This is exactly the "weak, barely-better-than-trivial" result the plan's own narrative (§ Design 3 item 1) predicted and flagged as a reason to temper expectations before building anything more sophisticated** — and it's for a _categorical baba-class_ target, not the continuous track-speed signal team-lead's brief asked me to probe.

**Conclusion for step 1**: not cleanly closed by a formal REJECT doc (none exists), but the flagship claim it grew from is refuted, and the one concrete model the same investigation built already shows weak results. This is closer to "an abandoned mid-investigation thread whose own early evidence points toward null" than to "a genuinely promising surviving candidate." Proceeding to step 2 anyway because team-lead's specific proposed construction — same-day **clocked race times**, not precipitation — is a different signal from anything actually tested above, so it isn't foreclosed by the evidence just described.

---

## 2. Probe: same-day pace-deviation nowcast (team-lead's construction)

**Mechanism**: `course_baseline_time(keibajo_code, kyori, track_code)` = strictly-prior expanding median winning time for that exact course configuration (JRA 2013-2025, 46,696 races with a valid baseline). For each race, `same_day_pace_deviation` = the average `(actual_winning_time − course_baseline_time)` over that same card's _earlier_ races only (`race_bango` order, same venue+date) — a same-day empirical read of whether the track is running faster or slower than its own historical norm today, independent of any official code. **Computable only from the 2nd race of a card onward — 91.6% of races have at least 1 prior same-day race (42,794/46,696); the missing 8.4% is essentially exactly "1 ÷ average card size," i.e. each day's opening race at each venue, confirmed as the expected mechanical floor, not a data gap.**

**Dedup from `sameday_bias` (REJECT)**: that family (`docs/finish-position-prediction-system.md` §11, masked-lever #2/`tmp/candidate-masked-lever-retest/`) tested a _track-bias residual_ — which running-style/draw-position wins more within a race, a positional/pace-position favoritism signal. This construction is a _track-speed level_ signal — how fast the surface itself is playing relative to its own historical baseline today, independent of who specifically wins. Different mechanism, different construction; not a rehash.

**Candidate feature**: `nowcast_pace_edge = same_day_pace_deviation × (past_sashi_rate_self + past_oikomi_rate_self)` — the hypothesized mechanism is that a track running faster/slower than baseline shifts which running style is favored, and closers (差し/追込) are the group most exposed to that shift. Uses the same "closer" definition as the already-REJECTed `closer_x_straight` interaction for direct comparability. `soha_time` (absolute finish time) was independently decoded here and confirmed 100% populated — this is _not_ the corrupted column (that's `time_sa`, the signed-margin field with the known regex bug, a different column entirely).

**Probe** (odds-controlled partial Spearman vs `finish_norm`, control = `[tansho_ninkijun, closer_tendency]`, 3-year sign check):

| Population                | 2023    | 2024    | 2025    | max\|partial ρ\| | Sign-stable?                         | Pass? |
| ------------------------- | ------- | ------- | ------- | ---------------- | ------------------------------------ | ----- |
| JRA-wide                  | +0.0083 | +0.0018 | -0.0030 | 0.0083           | No (sign flips)                      | No    |
| Summer-4-venue restricted | +0.0073 | -0.0096 | -0.0098 | 0.0098           | No (2023 positive, 2024/25 negative) | No    |

Both arms fail on magnitude (well below the 0.02 bar) and on sign-stability (flip between years in both populations). Match coverage against the augmented store was 91-94% in every year/population cut — no join-quality caveat needed here, unlike earlier candidates today. **REJECT.**

**Verdict**: per the pre-registered probe-first discipline, this closes now. **DO-NOT-RETEST this exact construction** (`same_day_pace_deviation × closer_tendency`, controlling for odds + the closer main effect). Not attempted, and left open only if a sibling has a concretely different mechanism in mind: testing `same_day_pace_deviation`'s main effect (not the closer-interaction) against a race-level market-calibration outcome instead of a horse-level finish target; or the plan's own still-nominally-open Design 3 item 2 (RS T-1 forecast placeholder), which is an RS-side, not FP-side, gap and already independently flagged as low-importance by an RS retrain's own feature-importance ranking (rank 113/146, 122/146) — not pursued here, out of this task's FP-focused scope.

---

## Artifacts

`apps/pc-keiba-viewer/tmp/going-nowcast-0717/{probe_nowcast.py,nowcast_2023_2025.parquet,probe_report.json}`. Prior-session artifacts read (not modified): `apps/pc-keiba-viewer/tmp/venue-weather-campaign/{lever-design/plan.md,trackstate-probe/baba_nowcast_results.json}`.
