# JRA 2026-06-14 Serve Pre-flight Check

**Date run**: 2026-06-13 (JST ~02:05)
**Race day**: 2026-06-14 (Sat) — first real-world test of the deployed serve-skew fixes
**Expected recovery**: ~+12.9pp top1 (NEW condition 26.09% vs OLD 4.35%, cf. serve-combined-recovery-measurement.md)
**Inspector**: pre-flight SubAgent (read-only, no deploys)

---

## Checklist

| #   | Item                                                                       | Status         | Evidence / Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| --- | -------------------------------------------------------------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | launchd loaded, 09:30 entry present, last exit 0                           | PASS           | `launchctl list` → `-  0  com.kkk4oru.finish-position-predict`; plist installed at `~/Library/LaunchAgents/com.kkk4oru.finish-position-predict.plist`; two `StartCalendarInterval` entries confirmed (03:00 JST + 09:30 JST).                                                                                                                                                                                                                                                                                                                  |
| 2a  | 03:00 JST auto-scope: nar,ban-ei only                                      | PASS           | Wrapper script: `if JST_HOUR <= 8 → PREDICT_CATEGORIES=nar,ban-ei`. Observed in today's 6/12 23:xx JST run log: `PREDICT_CATEGORIES=<all> (JST_HOUR=23 >= 09)`. Logic confirmed correct.                                                                                                                                                                                                                                                                                                                                                       |
| 2b  | 09:30 JST: all categories including JRA                                    | PASS           | Wrapper logic `JST_HOUR >= 09 → PREDICT_CATEGORIES=<all>`. 09:30 entry in plist. On Sat 6/14 this will be the first run that hits the JRA mirror + D1 advance odds.                                                                                                                                                                                                                                                                                                                                                                            |
| 3   | JRA D1 advance odds for 6/14                                               | EXPECTED-LATER | D1 `odds_snapshots` for `jra:2026:0614:%`: **0 rows** as of 02:05 JST 6/13. This is **expected** — the advance-odds prep window for next-day JRA opens at **19:00 JST tonight (6/13 Fri)** (`JRA_BETTING_OPEN_PREP_HOUR = 19` in `polling-window-gate.ts`). By 09:30 JST 6/14 there should be 12+ h of snapshots. The latest present key is `jra:2026:0613:09:10` fetched at 2026-06-13T01:26 JST — scraper is healthy.                                                                                                                        |
| 4   | Local PG mirror (jvd_ra/jvd_se) for 6/14                                   | EXPECTED-LATER | Mirror lands ~09:03 JST. At 02:05 JST 6/13 jvd_ra has 0 rows for `kaisai_tsukihi='0614'` (expected). The 09:30 cron fires after the ~09:03 mirror, so it will find 6/14 JRA races.                                                                                                                                                                                                                                                                                                                                                             |
| 5   | Completeness guard wired + MUST_BE_PRESENT covers odds/market-signal/futan | PASS           | `upcoming_feature_completeness_guard.py` MUST_BE_PRESENT list: `inverse_odds_implied_prob`, `inverse_odds_market_share`, `odds_score_diff_from_race_avg`, `inverse_odds_rank_in_race`, `popularity_rank_in_race`, `odds_score`, `popularity_score`, `futan_juryo`, `futan_juryo_rank_in_race`, `past_futan_juryo_avg5`, `shusso_tosu`, `umaban_norm`. Guard calls `assert_upcoming_feature_completeness` from `pipeline_runner.build_pipeline` and hard-fails on violation. RANK_FEATURES_ALL_EQUAL_FORBIDDEN catches the bogus all-1 pattern. |
| 6   | Model meta: JRA = iter14-jra-cb-pacestyle-course-v8                        | PASS           | `model_meta.py` `MODEL_VERSION_BY_CATEGORY["jra"] = "iter14-jra-cb-pacestyle-course-v8"`, FEATURE_COUNT=241, arch=catboost. Unchanged. Model files baked in split2 at `/models/finish-position/jra/iter14-jra-cb-pacestyle-course-v8/{model.json,metadata.json}`.                                                                                                                                                                                                                                                                              |
| 7   | LAYER_CHAIN JRA unchanged in split2                                        | PASS           | `pipeline_args.py` (baked in split2) LAYER_CHAIN["jra"] = 13-layer chain: race-internal → market-signal → sectional-weight → futan-juryo → workout → near-miss → lineage → head-to-head → baba-pedigree → trainer → pacestyle → course-numerical → relationship. This matches the expected v8 production chain.                                                                                                                                                                                                                                |
| 8   | **CRITICAL: split2 image contains Fix 2 (market-signal parquet fallback)** | **FAIL**       | split2 built **2026-06-10T11:06 JST**. Fix 2 commit `5c3aa12` is **2026-06-11T15:12 JST**. The `add-market-signal-features.py` baked into split2 still reads `tansho_odds` solely from `pg.race_entry_corner_features` (no `stage_parquet_odds` / `merge_odds_tables` COALESCE). For UPCOMING 6/14 races (absent from rec), all 5 market-signal features will be NULL → zeroed at `_coerce()` → ~21.5pp SHAP importance lost.                                                                                                                  |
| 9   | **CRITICAL: split2 image contains Fix 3 (futan COALESCE from jvd_se)**     | **FAIL**       | `ebd4636` committed **2026-06-11T15:44 JST** (after split2 build). `add-futan-juryo-features.py` in split2 has no `COALESCE(rec.futan_juryo, se.futan_juryo)` fallback for upcoming races. `futan_juryo`, `futan_juryo_rank_in_race`, `past_futan_juryo_avg5` will be NULL for 6/14 upcoming rows → ~5.2pp SHAP importance lost.                                                                                                                                                                                                               |
| 10  | **CRITICAL: split2 image contains BinderException fix (b8d45d2)**          | **FAIL**       | `b8d45d2` committed **2026-06-11T16:29 JST** (after split2 build). This fix adds `cast(rec.tansho_odds as double) as tansho_odds` and `cast(rec.tansho_ninkijun as int) as tansho_ninkijun` to `base_features_select_sql()` in `finish_position_features_duckdb.py`, which Fix 2's `stage_parquet_odds()` depends on. Without b8d45d2, Fix 2 would throw a `BinderException` (column not found). Currently moot because Fix 2 is not in split2, but both must be in the rebuilt image.                                                         |
| 11  | Recent commits do NOT break JRA chain (NAR exotic layer)                   | PASS           | `5bb13b8` adds `add_exotic_odds_features.py` to NAR chain in the source `pipeline_args.py`. But split2's baked `pipeline_args.py` has no exotic entry for NAR — no crash risk. JRA LAYER_CHAIN is unaffected in both source and split2.                                                                                                                                                                                                                                                                                                        |
| 12  | Colima + Docker available for image rebuild                                | PASS           | `colima status` → running (macOS Virtualization.Framework, aarch64, docker, virtiofs). Rebuild command available.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 13  | Recent failures.log                                                        | INFO           | 10 entries, last 2026-06-12T04:37 (SSL connection drop during NAR run). No JRA-specific errors; SSL drops are pre-existing transient NAR issue. All 6/11+ runs for 6/12 date have exited code=0.                                                                                                                                                                                                                                                                                                                                               |

---

## Overall Verdict

**NO-GO in current state. BLOCKER: split2 image must be rebuilt before 09:30 JST 2026-06-14.**

Without the rebuild, the 09:30 JST cron will run in the "MID" condition (Fix 1 only):

- market-signal features (5 columns, ~21.5% SHAP): NULL → zeroed for all UPCOMING 6/14 rows
- futan features (3 columns, ~5.2% SHAP): NULL → zeroed for all UPCOMING 6/14 rows
- Expected top1 ~21.74% vs target "NEW" ~26.09% (gap ~4.35pp)
- Expected place2 ~4.35% vs target ~13.04% (gap ~8.69pp)

---

## Required Fix (not yet done — report only)

**Rebuild split2 image** (read from repo root, all 3 fixes present in current source):

```sh
cd apps/finish-position-predict-container
docker build -f Dockerfile -t finish-position-predict-local:split2 ../..
```

**Verify** after rebuild:

```sh
docker run --rm --entrypoint /bin/bash finish-position-predict-local:split2 \
  -c "grep -c 'stage_parquet_odds\|merge_odds_tables' /app/pipeline/finish-position-features/add-market-signal-features.py"
# Expected: 2

docker run --rm --entrypoint /bin/bash finish-position-predict-local:split2 \
  -c "grep -c 'COALESCE\|_source_literal_from_se_table' /app/pipeline/finish-position-features/add-futan-juryo-features.py"
# Expected: >= 2

docker run --rm --entrypoint /bin/bash finish-position-predict-local:split2 \
  -c "grep -c 'cast.*rec.tansho_odds.*as tansho_odds' /app/pipeline/finish_position_features_duckdb.py"
# Expected: 1

docker inspect finish-position-predict-local:split2 --format '{{.Created}}'
# Expected: > 2026-06-11T16:29 JST
```

**Timing window**: Rebuild takes ~10 min (mostly uv pip install). Must complete before **09:30 JST 2026-06-14** (UTC 00:30 6/14). The 03:00 JST 6/14 cron is NAR+Ban-ei only (auto-scope) so will not be affected.

---

## Advance Odds Pre-conditions (automatic, no action needed)

- **19:00 JST 6/13 (tonight)**: prep window opens; hot worker begins scraping JRA 6/14 advance odds into D1
- **~09:03 JST 6/14**: PC-Keiba daily mirror delivers jvd_ra / jvd_se rows for 6/14 JRA races
- **09:30 JST 6/14**: launchd fires → split2 runs → `realtime_odds_fetcher.py` reads D1 → parquet contains live tansho_odds → market-signal + futan layers find real data → all 241 features live → iter14 scores with full feature vector

---

## 6/15 Measurement Procedure (post-race verification)

**When to run**: 6/15 morning after ~09:03 JST mirror delivers kakutei results.

### Step 1 — Identify the 09:30 JST 6/14 prediction set

```sh
# In Neon (race_finish_position_model_predictions)
# SELECT races where run_date='20260614' AND category='jra' AND model_version='iter14-jra-cb-pacestyle-course-v8'
# Confirm run_timestamp is AFTER 09:30 JST 6/14 (UTC 00:30 6/14)
# If multiple runs exist (guard cadence re-predictions), use the LAST run per race key
# (UPSERT semantics; later run = fresher odds)
```

### Step 2 — Verify features were live (not zeroed)

Check logs for 2026-06-14:

```sh
grep "completeness\|violation\|ERROR" ~/Library/Logs/finish-position-predict/20260614.log
# Expected: no violations
grep "realtime-odds.*jra.*races=" ~/Library/Logs/finish-position-predict/20260614.log
# Expected: races >= 1 (not 0), bataiju > 0
```

### Step 3 — Compute realized accuracy on 6/15 morning

After jvd_se kakutei_chakujun is mirrored (~09:03 JST 6/15):

```sql
-- top1 accuracy: fraction of races where rank-1 prediction = actual winner
SELECT
  COUNT(*) FILTER (WHERE actual_chakujun = 1 AND predicted_rank = 1) AS top1_hits,
  COUNT(DISTINCT race_key) AS total_races,
  COUNT(*) FILTER (WHERE actual_chakujun = 1 AND predicted_rank = 1)::float
    / COUNT(DISTINCT race_key) AS top1_rate
FROM race_finish_position_model_predictions p
JOIN jvd_se r USING (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban)
WHERE p.run_date = '20260614'
  AND p.category = 'jra'
  AND p.model_version = 'iter14-jra-cb-pacestyle-course-v8';
```

For place2/place3/fukusho_2p:

- **place2**: actual_chakujun <= 2 AND predicted_rank <= 2 (at-least-one-in-top-2 per race)
- **place3**: actual_chakujun <= 3 AND predicted_rank <= 3
- **fukusho_2p**: fraction of races where BOTH actual top-2 finishers appear in predicted top-2

### Step 4 — Compare against baselines

| Condition                    | Expected top1        | Expected place2 | Expected fukusho_2p |
| ---------------------------- | -------------------- | --------------- | ------------------- |
| WF baseline (walk-forward)   | ~24–25%              | ~10–12%         | ~55–65%             |
| NEW (all 3 fixes, target)    | ~26.09% (1-day n=23) | ~13.04%         | ~65.22%             |
| MID (Fix 1 only, no rebuild) | ~21.74%              | ~4.35%          | ~47.83%             |
| OLD (pre-all-fixes)          | ~4.35%               | ~4.35%          | ~13.04%             |

Single-day n is small (8–12 JRA venues × ~12 races = ~100 races on a typical Sat); expect high variance.
Accept as "GO" if top1 >= 20% and fukusho_2p >= 50%.
Accept as "BLOCKER CONFIRMED" if top1 < 15% or fukusho_2p < 30% (indicates split2 was not rebuilt).

### Step 5 — Record result

Append outcome to `docs/finish-position-accuracy/history/jra-0614-result.md` (create new file).
Include: run_timestamp, n_races, top1, place2, place3, fukusho_2p, comparison vs MID/NEW/WF,
and whether the split2 rebuild was confirmed (grep for `stage_parquet_odds` in 6/14 log).

---

---

## 2026-06-13 Hotfix: Exotic Layer Unwired from NAR Chain (commit ba26d3f)

**Time**: ~04:30 JST 2026-06-13
**Trigger**: Production smoke of split2-candidate (d88f031fbf81) failed with
`ArrowTypeError: Unable to merge: Field race_year has incompatible types: int64 vs dictionary<values=int32, indices=int32, ordered=0>`

**Root cause**: Commit 5bb13b8 wired `add_exotic_odds_features.py` into `LAYER_CHAIN['nar']` in
`pipeline_args.py`. The script's parquet writer emits `race_year` with a schema incompatible with
the rest of the chain. The sanrenpuku model is not deployed (task #289 pending); the chain-wiring
was premature.

**Fix applied (minimal-risk unwire)**:

- `apps/finish-position-predict-container/src/predict_lib/pipeline_args.py`: removed `EXOTIC_SCRIPT`
  from `LAYER_CHAIN['nar']` tuple. All constants, `SCRIPTS_WITH_PG_URL` entry, `EXOTIC_CATEGORY_BY_CATEGORY`,
  `_exotic_category_args()` helper, and `build_layer_argv` call retained for #289 re-wiring.
- `apps/finish-position-predict-container/tests/test_pipeline_args.py`: updated two tests —
  `test_layer_chain_nar_is_light_v6_plus_v7_plus_trainer_plus_pacestyle` (removed exotic entry from
  expected list); renamed `test_exotic_script_is_in_layer_chain_nar` →
  `test_exotic_script_is_not_in_layer_chain_nar` (assertion flipped to `not in`). 460 tests pass,
  100% coverage.

**Docker rebuild**: `finish-position-predict-local:split2-candidate2`
Image ID: `sha256:d81c4deb343062e9da51a99ad68a6cc9adc811827a7a266440dcbb4f631946ea`
Built at: 2026-06-13T04:29:00 JST

**Smoke test** (`RUN_DATE=20260612 bash scripts/launchd/finish-position-predict-daily.sh`):

- Exit code: **0**
- `[predict-upcoming] ok run_date=20260612 races_predicted=382`
- NAR: 382 rows predicted successfully (36 races × ~10.6 horses)
- Ban-ei: 0 rows (no ban-ei races on 20260612, correct skip)
- JRA: auto-scoped out (JST_HOUR=04 < 09, expected)
- No ArrowTypeError, no ERROR lines in final run
- Realtime odds fetched: 380 rows, bataiju=380/380

**Final split2 image**: `sha256:d81c4deb343062e9da51a99ad68a6cc9adc811827a7a266440dcbb4f631946ea`
(candidate2 retagged as split2 — same image as candidate2)

**Verdict for 09:30 JST 2026-06-14**: **GO** — split2 now carries all serve-skew fixes (Fix 1/2/3,
BinderException fix) and the exotic-layer ArrowTypeError is resolved. JRA chain unchanged (13 layers,
241 features, iter14). The 09:30 cron will use this image for JRA + NAR + Ban-ei.

---

## Other Notes

- **NAR exotic layer (5bb13b8)**: The current source `pipeline_args.py` adds `add_exotic_odds_features.py` to the NAR chain, but split2's baked `pipeline_args.py` does not. This is NAR-only; no JRA impact. A rebuild will bring the exotic layer into NAR as well (the script exists in current source at `apps/pc-keiba-viewer/src/scripts/finish-position-features/add_exotic_odds_features.py` — NEW as of 5bb13b8, needs to be present before rebuild).
- **Pedigree-staging fix (65ad49e)**: silent-NULL sire/damsire for NAR/Ban-ei. Included in a rebuild. No JRA impact.
- **realtime-weight fetch failures**: `[realtime-weight] fetch failed` errors for all JRA/NAR races are pre-existing (bataiju endpoint requires specific timing — race-day only). bataiju falls back to se.bataiju (historical). Not a blocker.
- **NAR SSL drops**: `psycopg.OperationalError: consuming input failed` appears in 6/12 logs. Pre-existing; NAR prediction retries on next guard cycle. Not a JRA blocker.
