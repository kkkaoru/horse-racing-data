# Weekend 2026-07-18 Racecard × Routing Pre-Registration (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position serving — read-only, lightweight I/O only (wave7 training is CPU-saturated; no DuckDB/CatBoost work in this task)
- **Task**: 次サイクル調査④ (team-lead). Pre-register tomorrow's (2026-07-18, Sat) expected `cell_routing.json` variant per race, so that once the racecard loads and cron jobs run, verification is a pure diff — no live analysis needed under time pressure.

## 0. Racecard status: **not yet loaded** — verified directly, not assumed

Checked both backends directly (read-only), right now:

```sql
SELECT count(*) FROM jvd_ra WHERE kaisai_nen='2026' AND kaisai_tsukihi='0718'
```

**Neon: 0 rows. Local PG replica: 0 rows.** Both backends' max `kaisai_tsukihi`
for 2026 is currently `0712` (last Sunday) — consistent with simply no
JRA racing having occurred since (JRA races Sat/Sun; nothing scheduled
Mon-Fri this week), not a sync lag on either side.

**When will it load — honest answer, not a guess dressed as one.** I could
not confirm the exact base-JVD racecard (出馬表) ingestion mechanism/timing
within this task's read-only scope. What I _did_ confirm:

- `apps/sync-realtime-data/src/worker.ts`'s `MULTI_DAY_PREP_CRON`
  (`"5 11 * * *"` = UTC 11:05 = **JST 20:05, daily**) calls
  `prewarmRaceDataForDates` with `MULTI_DAY_PREP_OFFSET_DAYS = [1, 2, 3]` —
  i.e. every evening it prepares the _next 3 days_, so tonight's run
  (2026-07-17, 20:05 JST) is the run whose offset+1 target is tomorrow.
  This is the closest internal signal to "when does tomorrow's card become
  actionable," but I did not verify (would require reading
  `prewarmRaceDataForDates`'s body, out of this task's lightweight-I/O
  scope) whether this cron writes `jvd_ra` itself or only prepares
  Worker-side scheduling state (`realtime_race_sources`, weight-fetch
  queues) on top of a `jvd_ra` racecard that arrives via a separate,
  external JRA-VAN feed path not represented in this TypeScript package.
  `jvd_ra`'s own `data_sakusei_nengappi` field on the most recent past
  card (07-12) reads `20260713` — i.e. it reflects _results_
  finalization, one day after the race, not original racecard issuance,
  so it's not usable as a pre-race timing signal either.
- **Recommendation**: re-check after tonight's 20:05 JST tick (say,
  21:00 JST as a safety margin); if still empty, the next candidate
  window is `TODAY_BACKFILL_CRON` (`"10 0 * * *"` = JST 09:10) tomorrow
  morning, ahead of the 09:15/09:25-09:30 window team-lead specified for
  the corner-refresher/JRA cron. One-line check:
  `SELECT count(*) FROM jvd_ra WHERE kaisai_nen='2026' AND kaisai_tsukihi='0718';`
  against Neon. **Proceeding under team-lead's explicit fallback: this
  is a "not yet, report and it's fine to wait" outcome**, not a blocker —
  sections 1-3 below are fully prepared in advance so applying them the
  moment the card lands is a pure lookup, not new work.

## 1. Venue02 (Hakodate) status — correction to the "already finished" assumption

Team-lead's message flagged this as needing verification ("函館の最終日は
いつか — 要確認"). Checked directly: **Hakodate's 2026 meeting (1回函館)
has very likely NOT finished, and is likely to continue tomorrow.**

`kaisai_kai`/`kaisai_nichime` for `keibajo_code='02'`, both years starting
from `kai='01'`:

| Year | Dates (kai=01 only)                                                                          | Max nichime observed  |
| ---- | -------------------------------------------------------------------------------------------- | --------------------- |
| 2025 | 06-14, 06-15, 06-21, 06-22, 06-28, 06-29, 07-05, 07-06, 07-12, 07-13, 07-19, 07-20 (12 days) | 12 (meeting complete) |
| 2026 | 06-13, 06-14, 06-20, 06-21, 06-27, 06-28, 07-04, 07-05, 07-11, 07-12 (10 days **so far**)    | 10 (data ends here)   |

**2026 is running exactly one day earlier than 2025 at every single one of
the 10 observed dates** (06-13 vs 06-14, 06-14 vs 06-15, ... 07-11 vs
07-12, 07-12 vs 07-13) — a precise, consistent one-day offset, not a
coincidental partial match. If this holds (as it has for all 10 dates so
far), the two remaining days to complete a 12-day meeting (nichime 11-12)
would land on **2026-07-18 and 2026-07-19** — this weekend, not a
finished meeting. (2024 also reached nichime=12 for its Hakodate meeting;
2023 shows a different max, likely a two-kai year — not used as evidence
either way, flagged rather than silently discarded.)

**Practical consequence**: budget for venue02 races appearing on
tomorrow's card, not their absence. If the card, once loaded, shows zero
venue02 rows, that would itself be a small surprise worth a second look
(not expected, but not impossible — a meeting length isn't contractually
fixed).

## 2. cell_routing.json — pre-registered decision logic (ready now, no card needed)

`apps/finish-position-predict-container/src/predict_lib/cell_routing.json`'s
`jra` rules, in file order (**first-match-wins** — verified from
`cell_router.py`'s `resolve_variant` docstring convention, same logic
`serve_health_check.py`'s check 3 reimplements):

```text
for each confirmed/entered race:
  1. if kyoso_joken_code == "703":                          -> jockey_pedigree_703  (jra-cb-v9-sim-2013-clean-jockey-pedigree269)
  2. elif surface=="dirt" and field_band=="f_le10"
        and kyoso_joken_code == "005":                       -> prior_corner_dirt_smallfield_005 (jra-cb-v10-prior-corner274-2013)
  3. elif venue == "02":                                     -> jockey_pedigree_703
  4. else:                                                    -> sim (jra-cb-v9-sim-2013-clean, the champion)
```

**Subtlety worth stating explicitly** (the kind a hasty "venue02 always
gets 269" shortcut would get wrong): rule 2 is checked **before** rule 3.
A venue02 race that happens to be dirt + field≤10 + `kyoso_joken_code=005`
routes to `prior_corner_dirt_smallfield_005`, **not**
`jockey_pedigree_703` — the venue rule only catches venue02 races that
fall through both of the first two conditions. In practice this means:
"venue02 → 269" is correct for the overwhelming majority of Hakodate
races, but not unconditionally — check `kyoso_joken_code`/`surface`/
`field_band` per race, don't shortcut it.

Expected-variant table to fill in once the card loads (mechanical
application of the 4-step logic above to each race's `venue`/
`kyoso_joken_code`/`surface`/`shusso_tosu`):

| Venue                                                                               | Expected default (no special condition) | Exceptions to check                                                                                      |
| ----------------------------------------------------------------------------------- | --------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| 02 Hakodate (if racing)                                                             | `jockey_pedigree_703` for every race    | UNLESS dirt + `f_le10` + `kyoso_joken_code=005` → `prior_corner_dirt_smallfield_005` instead             |
| 01/03/10 (Sapporo not yet racing this weekend per readiness doc, Fukushima, Kokura) | `sim` (champion)                        | `kyoso_joken_code=703` → `jockey_pedigree_703`; dirt+`f_le10`+`005` → `prior_corner_dirt_smallfield_005` |

## 3. The diff command — one line, ready for tomorrow

No new tool needed. `serve_health_check.py`'s **check 3 (routing parity)**
already does exactly "apply this same first-match-wins logic to confirmed
races and diff against the `model_version` rows actually written" —
built 2026-07-17, already proven against live data:

```sh
uv run python src/scripts/serve_health_check.py --date 20260718 --category jra
```

Read the `[3] Routing parity` section of the output: `OK (0 mismatches)`
means every confirmed race got its cell-routing-expected `model_version`
row; any listed mismatch names the race and its expected variant directly
(no manual cross-referencing against section 2's table needed — the tool
already encodes the same logic). This also runs checks 1/2/4/5 (coverage,
quality-collapse, burst, D1 self-heal) in the same pass, so this single
command **is** the full "expected vs. actual" verification team-lead
asked for, not just the routing slice of it. Re-run per race as it
settles, or once for the whole day's card after racing concludes.

## 4. Summary for whoever picks this up tomorrow

1. Racecard for 07-18 does not exist yet as of this writing (verified in
   both Neon and local PG) — re-check after ~21:00 JST tonight, then
   again ~09:10-09:30 JST tomorrow if still absent. This is the expected,
   reported "not yet, wait" outcome per team-lead's own fallback
   instruction, not a defect.
2. Hakodate (venue02) is likely still racing tomorrow — do not assume its
   meeting is over; the day-count/offset evidence in section 1 points the
   other way from team-lead's working assumption.
3. The moment the card loads, section 2's 4-step logic (or, more simply,
   just running the command in section 3) tells you the expected
   `model_version` per race with no further analysis required.
