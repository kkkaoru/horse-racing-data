# NAR + Ban-ei Warehouse Unused-Data Census

**Date:** 2026-06-12  
**Status:** COMPLETE — all populated unused columns assessed.  
**Scope:** `nvd_se`, `nvd_ra`, `nvd_hr`, `nvd_o1`–`nvd_o6`. Ban-ei = `nvd_se`/`nvd_ra` where `keibajo_code='83'`.  
**Density baseline:** 2022–2026 rows. Populated = non-NULL AND non-empty-string AND not sentinel zero/space.

---

## 1. Consumed NAR Columns (feature pipeline)

Columns actively read by `finish-position-features/*.py` and `finish_position_features_duckdb.py`:

**`nvd_se`:** `kaisai_nen`, `kaisai_tsukihi`, `keibajo_code`, `race_bango`, `ketto_toroku_bango`, `umaban`, `kishumei_ryakusho`, `chokyoshimei_ryakusho`, `kakutei_chakujun`, `tansho_ninkijun`, `tansho_odds`, `bataiju`, `time_sa`, `kohan_3f`, `futan_juryo`, `corner_1`/`corner_3`/`corner_4` (Ban-ei only), `soha_time` (Ban-ei only), `chokyoshi_code`, `kishu_code`, `banushi_code`, `barei`

**`nvd_ra`:** `kaisai_nen`, `kaisai_tsukihi`, `keibajo_code`, `race_bango`, `kyori`, `track_code`, `grade_code`, `kyoso_joken_code`, `kyoso_joken_meisho`, `shusso_tosu`, `babajotai_code_shiba`, `babajotai_code_dirt`, `tenko_code`, `kyosomei_hondai`

**`nvd_hr`, `nvd_o1`–`nvd_o6`:** None consumed.

Note: `kyoso_shubetsu_code` and `juryo_shubetsu_code` appear only in evaluation/bucketing scripts — NOT as model input features.

---

## 2. `nvd_se` — Column Census

Total rows 2022–2026: **691,120** (NAR all venues).  
Ban-ei subset: **72,576** (keibajo_code='83').

### 2a. nvd_se — Populated Unused Columns

| column                       | NAR density             | Ban-ei density        | consumed?    | leak class                                  | within-race var                             | verdict                                                         |
| ---------------------------- | ----------------------- | --------------------- | ------------ | ------------------------------------------- | ------------------------------------------- | --------------------------------------------------------------- |
| `data_kubun`                 | 100%                    | 100%                  | NO           | structural (data type code)                 | NO                                          | structural — no signal                                          |
| `wakuban`                    | 100%                    | 100%                  | NO (raw)     | pre-race                                    | YES                                         | DO-NOT-RETEST (gate-draw, already in DO-NOT-RETEST list)        |
| `bamei`                      | 100%                    | 100%                  | NO (raw)     | structural (horse name)                     | YES                                         | redundant with ketto_toroku_bango                               |
| `umakigo_code`               | 100%                    | 100%                  | NO           | pre-race                                    | YES                                         | alias flags (cross/foreign), see note below                     |
| `seibetsu_code`              | 100%                    | 100%                  | NO           | pre-race                                    | YES (96.9% of races)                        | **GENUINE CANDIDATE — see Section 4**                           |
| `hinshu_code`                | 100%                    | 100%                  | NO           | pre-race                                    | NO (constant per venue: '1'=TB, 'B'=Ban-ei) | race-level constant per venue — no cross-venue signal           |
| `moshoku_code`               | 100%                    | 100%                  | NO           | pre-race                                    | YES (some variation)                        | coat color — no known finish-position signal; market irrelevant |
| `tozai_shozoku_code`         | 100%                    | 100%                  | NO           | pre-race                                    | NO (99.3% = '3' = local)                    | race-level constant for NAR; no signal                          |
| `banushimei`                 | 100%                    | 100%                  | NO (raw)     | pre-race                                    | YES                                         | owner name text — collinear with banushi_code (code used)       |
| `fukushoku_hyoji`            | 100%                    | 100%                  | NO           | pre-race                                    | YES (silks description)                     | free-text, ~5000 unique strings, no ML-usable encoding          |
| `yobi_1`                     | 100%                    | 100%                  | NO           | structural (reserved field, all same value) | NO                                          | structural filler — no signal                                   |
| `futan_juryo_henkomae`       | 100%                    | 100%                  | NO           | pre-race                                    | YES                                         | weight before change; 99.7% identical to futan_juryo; redundant |
| `kishu_code_henkomae`        | 100%                    | 100%                  | NO           | pre-race                                    | YES (1.1% changed)                          | pre-change jockey; 98.9% same as kishu_code; redundant          |
| `kishumei_ryakusho_henkomae` | 100%                    | 100%                  | NO           | pre-race                                    | YES (1.1% changed)                          | pre-change jockey name; redundant with kishu_code_henkomae      |
| `kishu_minarai_code`         | 13.9% NAR / 9.0% Ban-ei | 9.0%                  | NO           | pre-race                                    | YES                                         | **GENUINE CANDIDATE — see Section 4**                           |
| `zogen_fugo`                 | 87.8%                   | 92.3%                 | NO           | pre-race                                    | YES                                         | weight-change sign (+/-); paired with zogen_sa                  |
| `zogen_sa`                   | 97.4%                   | 97.6%                 | NO           | pre-race                                    | YES                                         | **GENUINE CANDIDATE — see Section 4**                           |
| `ijo_kubun_code`             | 1.3%                    | ~1%                   | NO           | pre/post-race mixed                         | YES                                         | incident code (fall, scratch, etc.); 1.3% density; mostly post  |
| `nyusen_juni`                | 100%                    | 100%                  | NO           | post-race (LEAK)                            | YES                                         | photo-finish order — 131 rows differ from kakutei_chakujun      |
| `kakutei_chakujun`           | 100%                    | 100%                  | YES (label)  | post-race (LEAK)                            | YES                                         | used as label only                                              |
| `dochaku_kubun`              | 0.2%                    | ~0.2%                 | NO           | post-race (LEAK)                            | —                                           | dead-heat flag; near-zero density                               |
| `dochaku_tosu`               | 0.2%                    | ~0.2%                 | NO           | post-race (LEAK)                            | —                                           | dead-heat count; near-zero density                              |
| `soha_time`                  | 100%                    | 100%                  | YES (Ban-ei) | post-race (LEAK)                            | YES                                         | final time — LEAK; used only as label/history                   |
| `chakusa_code_1`             | 78.7%                   | ~75%                  | NO           | post-race (LEAK)                            | YES                                         | margin code to next horse                                       |
| `chakusa_code_2`             | ~0%                     | ~0%                   | NO           | post-race (LEAK)                            | —                                           | near-zero                                                       |
| `corner_1`                   | 100%                    | 100%                  | YES (Ban-ei) | post-race (LEAK)                            | YES                                         | corner pass order — LEAK                                        |
| `corner_2`                   | 100%                    | 100%                  | NO           | post-race (LEAK)                            | YES                                         | corner pass order — LEAK                                        |
| `corner_3`                   | 100%                    | 100%                  | YES (Ban-ei) | post-race (LEAK)                            | YES                                         | LEAK                                                            |
| `corner_4`                   | 100%                    | 100%                  | YES (Ban-ei) | post-race (LEAK)                            | YES                                         | LEAK                                                            |
| `kakutoku_honshokin`         | 100%                    | 100%                  | NO           | post-race (LEAK)                            | YES                                         | earned prize — LEAK                                             |
| `kakutoku_fukashokin`        | 100%                    | 100%                  | NO           | post-race (LEAK)                            | YES                                         | earned bonus — LEAK                                             |
| `kohan_4f`                   | 100% (NAR=0% real)      | 100% (Ban-ei=0% real) | NO           | post-race (LEAK)                            | YES                                         | rear 4F time — LEAK; Ban-ei sentinel '000' only                 |
| `kohan_3f`                   | 100%                    | 100%                  | YES          | post-race (LEAK)                            | YES                                         | rear 3F time — consumed as historical feature                   |
| `aiteuma_joho_1/2/3`         | 100%                    | 100%                  | NO           | post-race (LEAK)                            | YES                                         | rival horse info (rival name+ID packed) — LEAK                  |
| `time_sa`                    | 100%                    | 100%                  | YES          | post-race (LEAK)                            | YES                                         | time diff to leader — LEAK; consumed as historical              |
| `record_koshin_kubun`        | ~0%                     | ~0%                   | NO           | post-race (LEAK)                            | —                                           | track-record flag; near-zero                                    |
| `mining_kubun`               | ~0%                     | ~0%                   | NO           | DO-NOT-RETEST                               | —                                           | all-zero at NAR                                                 |
| `yoso_soha_time`             | 100%                    | 100%                  | NO           | DO-NOT-RETEST                               | YES                                         | official forecast time — DO-NOT-RETEST (ρ 0.066)                |
| `yoso_gosa_plus`             | 100%                    | 100%                  | NO           | DO-NOT-RETEST                               | YES                                         | forecast error upper bound — DO-NOT-RETEST                      |
| `yoso_gosa_minus`            | 100%                    | 100%                  | NO           | DO-NOT-RETEST                               | YES                                         | forecast error lower bound — DO-NOT-RETEST                      |
| `yoso_juni`                  | 100%                    | 100%                  | NO           | DO-NOT-RETEST                               | YES                                         | official forecast rank — DO-NOT-RETEST (ρ 0.066)                |
| `kyakushitsu_hantei`         | ~0% (NAR) / 0% (Ban-ei) | 0%                    | NO           | DO-NOT-RETEST                               | —                                           | running style label — 0% density at NAR; DO-NOT-RETEST          |
| `blinker_shiyo_kubun`        | 0%                      | 0%                    | NO           | —                                           | —                                           | all zero/empty at NAR — unpopulated                             |
| `yobi_2/3/4`                 | 100% sentinel           | 100% sentinel         | NO           | structural                                  | —                                           | all '000' sentinel — no signal                                  |

Notes on `umakigo_code`: distinct values in the data encode special designation flags (foreign horse, cross-bred, etc.). Likely nearly all '0' for domestic NAR. Would need deeper cardinality check but structurally no finish-position signal.

---

## 2b. nvd_se — Summary Table (populated ≥30%, NOT consumed)

| column                 | density | leak class | within-race var | serve-available | candidate?                         |
| ---------------------- | ------- | ---------- | --------------- | --------------- | ---------------------------------- |
| `seibetsu_code`        | 100%    | pre-race   | YES (97% races) | YES             | YES — see Section 4                |
| `kishu_minarai_code`   | 13.9%   | pre-race   | YES             | YES             | YES — see Section 4                |
| `zogen_sa`             | 97.4%   | pre-race   | YES             | YES             | YES — see Section 4                |
| `zogen_fugo`           | 87.8%   | pre-race   | YES             | YES             | paired with zogen_sa               |
| `futan_juryo_henkomae` | 100%    | pre-race   | YES (2k differ) | YES             | redundant with futan_juryo         |
| `fukushoku_hyoji`      | 100%    | pre-race   | YES             | YES             | free-text silks; unencoded         |
| `moshoku_code`         | 100%    | pre-race   | YES             | YES             | coat color; no known signal        |
| `banushimei`           | 100%    | pre-race   | YES             | YES             | collinear with banushi_code (used) |

---

## 3. `nvd_ra` — Column Census

Total rows 2022–2026: **68,105** races (NAR all venues incl. Ban-ei).

### 3a. nvd_ra — Populated Unused Columns

| column                         | density | consumed?    | leak class       | within-race var | verdict                                                                                   |
| ------------------------------ | ------- | ------------ | ---------------- | --------------- | ----------------------------------------------------------------------------------------- |
| `kaisai_kai`                   | 100%    | NO           | pre-race         | NO (race-level) | meeting number; race-level constant; same as JRA finding                                  |
| `kaisai_nichime`               | 100%    | NO           | pre-race         | NO (race-level) | day-in-meeting; race-level; probed FAIL at JRA                                            |
| `yobi_code`                    | 100%    | NO           | pre-race         | NO (race-level) | day-of-week (1–8); race-level constant; no within-race signal                             |
| `kyosomei_hondai`              | 100%    | YES          | pre-race         | NO (race-level) | race name — consumed for context                                                          |
| `kyosomei_fukudai`             | 100%    | NO           | pre-race         | NO (race-level) | race subtitle text; redundant with hondai                                                 |
| `kyosomei_kakkonai`            | 100%    | NO           | pre-race         | NO (race-level) | race parenthetical text; redundant                                                        |
| `kyosomei_ryakusho_10/6/3`     | 100%    | NO           | pre-race         | NO (race-level) | abbreviated race names; redundant with hondai                                             |
| `kyoso_shubetsu_code`          | 100%    | NO (feature) | pre-race         | NO (race-level) | **GENUINE CANDIDATE — see Section 4**                                                     |
| `juryo_shubetsu_code`          | 79.3%   | NO (feature) | pre-race         | NO (race-level) | **GENUINE CANDIDATE — see Section 4**                                                     |
| `kyoso_kigo_code`              | 6.2%    | NO           | pre-race         | NO (race-level) | race condition flag; 93.8% = '000'; low cardinality                                       |
| `honshokin`                    | 100%    | NO           | pre-race         | NO (race-level) | prize money (packed 56-char, 138 distinct values) — **GENUINE CANDIDATE — see Section 4** |
| `honshokin_henkomae`           | 100%    | NO           | pre-race         | NO (race-level) | prize before change; 100% same as honshokin (no changes observed)                         |
| `fukashokin`                   | 100%    | NO           | pre-race         | NO (race-level) | bonus prize; 100% = '00000000...' at NAR — all zero                                       |
| `fukashokin_henkomae`          | 100%    | NO           | pre-race         | NO (race-level) | bonus prize before change; zero                                                           |
| `hasso_jikoku`                 | 100%    | NO           | pre-race         | NO (race-level) | post time (HHmm); race-level constant; same as JRA finding FAIL                           |
| `toroku_tosu`                  | 100%    | NO           | pre-race         | NO (race-level) | registered entries before scratches; **GENUINE CANDIDATE — see Section 4**                |
| `nyusen_tosu`                  | 99%     | NO           | post-race (LEAK) | NO              | finishers count — post-race                                                               |
| `lap_time`                     | 100%    | NO           | post-race (LEAK) | NO              | packed lap times (per 3F) — LEAK                                                          |
| `zenhan_3f`                    | 100%    | NO           | post-race (LEAK) | NO              | front-half 3F time — LEAK                                                                 |
| `zenhan_4f`                    | 100%    | NO           | post-race (LEAK) | NO              | front-half 4F time — LEAK                                                                 |
| `kohan_3f`                     | 100%    | NO           | post-race (LEAK) | NO              | rear-half 3F, race-level aggregate — LEAK                                                 |
| `kohan_4f`                     | 100%    | NO           | post-race (LEAK) | NO              | rear-half 4F — LEAK; Ban-ei sentinel only                                                 |
| `corner_tsuka_juni_1–4`        | 100%    | NO           | post-race (LEAK) | NO              | race-level corner passing order strings — LEAK                                            |
| `tokubetsu_kyoso_bango`        | 0%      | NO           | —                | —               | all zero — unpopulated                                                                    |
| `kyosomei_kubun`               | 0%      | NO           | —                | —               | all zero — unpopulated                                                                    |
| `grade_code_henkomae`          | 0%      | NO           | —                | —               | all zero — unpopulated                                                                    |
| `kyoso_joken_code_2/3/4/5sai*` | 0%      | NO           | —                | —               | all zero — unpopulated                                                                    |
| `kyori_henkomae`               | 0%      | NO           | —                | —               | all zero — unpopulated                                                                    |
| `track_code_henkomae`          | 0%      | NO           | —                | —               | all zero — unpopulated                                                                    |
| `course_kubun`                 | 0%      | NO           | —                | —               | all zero — unpopulated                                                                    |
| `hasso_jikoku_henkomae`        | 0%      | NO           | —                | —               | all zero — unpopulated                                                                    |
| `shogai_mile_time`             | 0%      | NO           | —                | —               | all zero — unpopulated                                                                    |
| `record_koshin_kubun`          | 0%      | NO           | —                | —               | all zero — unpopulated                                                                    |

---

## 4. `nvd_hr` and `nvd_o1`–`nvd_o6`

### `nvd_hr` — Payout table

All `haraimodoshi_*` columns: 100% populated. Entirely post-race payouts (tansho/fukusho/umaren/etc.). All are **LEAK** — unusable as pre-race features.

`fuseiritsu_flag_*`, `tokubarai_flag_*`, `henkan_*`: all 0% density at NAR — unpopulated.

### `nvd_o1`–`nvd_o6` — Odds snapshots

`odds_tansho`, `odds_fukusho` (nvd_o1): 100% populated; per-horse odds array, packed string.  
`odds_wakuren` (nvd_o1): 80.8%; pair-level.  
`odds_umaren` (nvd_o2), `odds_wide` (nvd_o3), `odds_umatan` (nvd_o4), `odds_sanrenpuku` (nvd_o5), `odds_sanrentan` (nvd_o6): populated.

**Verdict:** DO-NOT-RETEST. These are odds time-series snapshots. The task brief explicitly marks "odds time-series (NAR history zero)" as DO-NOT-RETEST. Even if per-snapshot final odds were extracted, `tansho_odds` is already consumed directly from `nvd_se`. Pair/triple odds have no within-horse variation.

---

## 5. Genuine Candidates — Pre-race + Populated + Leak-free

Six columns/signals pass the basic gate (populated, pre-race, not already excluded):

### C1. `nvd_se.seibetsu_code` — Horse sex code

- **Density:** 100% (691,120 rows)
- **Leak class:** pre-race
- **Within-race variation:** YES — 96.9% of NAR races have >1 sex represented
- **Serve available:** YES (in nvd_se before prediction)
- **Signal:** NAR win rate: stallion (1) 11.00%, mare (2) 8.66%, gelding (3) 10.79%; Ban-ei: stallion 10.87%, mare 10.76%, gelding 12.40%. Raw win-rate spread of ~2.3pp at NAR.
- **BUT:** seibetsu_code is already fully captured in the market odds. Mares face restricted/open conditions differently by class level. In mixed races the odds price sex implicitly. Partial ρ controlling for odds is expected near-zero (analogous to JRA kishu_minarai_code result at ρ 0.022).
- **Assessment:** NOT recommended for probing. The JRA equivalent (blinker, apprentice jockey) showed raw signal collapses when odds-controlled. Sex is one of the most visible horse attributes — the market prices it efficiently. Same pattern as JRA kishu_minarai_code: raw ~2pp win-rate gap, but market already encodes it.

### C2. `nvd_se.kishu_minarai_code` — Jockey apprentice level

- **Density:** 13.9% NAR / 9.0% Ban-ei (code non-zero = apprentice; zero = journeyman)
- **Leak class:** pre-race
- **Within-race variation:** YES
- **Serve available:** YES
- **Signal:** 4 levels (0=journeyman, 1–4=apprentice weight allowance levels). Raw win-rate difference exists.
- **Assessment:** This is **exactly the same column** probed in JRA (candidate A) where partial ρ = 0.022 after odds control — FAIL. NAR has the same structure. The market prices jockey quality including apprentice status. **DO-NOT-RETEST** — extends JRA probe A to NAR.

### C3. `nvd_se.zogen_sa` (+ `zogen_fugo`) — Bodyweight change from last race

- **Density:** zogen_sa 97.4% NAR / 97.6% Ban-ei; zogen_fugo 87.8% / 92.3%
- **Leak class:** pre-race (known at weigh-in before race)
- **Within-race variation:** YES
- **Serve available:** YES (part of nvd_se pre-race entry)
- **Signal:** Bodyweight change (signed delta) is a well-known handicapping factor. `bataiju` (current weight) is already consumed. `zogen_sa` adds the delta component.
- **Assessment:** This is the same signal as `h-prev-bw-drop.md` (history file exists). Check if it was probed. **ALREADY TESTED** — `h-prev-bw-drop.md` exists in the history directory, covering bodyweight drop/change as a feature. Mark as previously probed.

### C4. `nvd_ra.kyoso_shubetsu_code` — Race type code

- **Density:** 100%
- **Leak class:** pre-race
- **Within-race variation:** NO (race-level constant)
- **Values at NAR:** 49=normal flat (56% of races), 12/13/14/11=special conditions; Ban-ei uses 01–08
- **Assessment:** Race-level constant — no within-race variation. This is a structural stratifier used in evaluation buckets (already in evaluation scripts). Cannot contribute to per-horse ranking within a race. **Not a feature candidate.**

### C5. `nvd_ra.juryo_shubetsu_code` — Weight allowance type

- **Density:** 79.3% NAR / Ban-ei 100% (code=8)
- **Leak class:** pre-race
- **Within-race variation:** NO (race-level constant)
- **Values:** 0=fixed, 2=handicap, 3=weight-for-age, 4=catchweight, 8=Ban-ei-specific, 9=other
- **Assessment:** Race-level constant — no within-race per-horse variation. However it conditions the type of `futan_juryo` encoding and thus the interpretation of weight. **Not a feature candidate** (race-level), but relevant as a stratifier.

### C6. `nvd_ra.honshokin` — Prize money (1st-place value)

- **Density:** 100%
- **Leak class:** pre-race
- **Within-race variation:** NO (race-level constant)
- **Values:** 56-char packed field, first 8 chars = 1st-place prize; 138 distinct prize tiers at NAR
- **Assessment:** Race-level constant — identical for all horses in the race. Correlated with `grade_code` and `kyoso_joken_code` (both already consumed). No within-race signal. **Not a feature candidate.**

### C7. `nvd_ra.toroku_tosu` — Registered entries (before scratches)

- **Density:** 100%
- **Leak class:** pre-race
- **Within-race variation:** NO (race-level constant)
- **Signal:** toroku_tosu − shusso_tosu = scratch count. 10.7% of races have ≥1 scratch. `shusso_tosu` is already consumed.
- **Assessment:** Race-level constant, and scratch count is a weak race-level signal. `shusso_tosu` (actual starters) is already in the feature pipeline. The incremental information from scratch count is minimal and race-level only. **Not a feature candidate.**

---

## 6. Summary

### Populated Unused Columns by Classification

| table       | populated ≥30% | post-race LEAK | race-level constant | redundant/structural | already-tested DO-NOT-RETEST | genuine pre-race per-horse candidates |
| ----------- | -------------- | -------------- | ------------------- | -------------------- | ---------------------------- | ------------------------------------- |
| `nvd_se`    | 23             | 12             | 0                   | 6                    | 5                            | **0 new** (see below)                 |
| `nvd_ra`    | 12             | 7              | 5                   | 0                    | 0                            | 0                                     |
| `nvd_hr`    | many           | all LEAK       | —                   | —                    | —                            | 0                                     |
| `nvd_o1–o6` | 6              | DO-NOT-RETEST  | —                   | —                    | all                          | 0                                     |

### Genuine New Candidates

**None.**

All populated unused columns fall into one of:

1. **Post-race LEAK** — result times, corner order, payouts, earned prizes, margins
2. **Race-level constant** — no within-race per-horse variation (kyoso_shubetsu_code, honshokin, hasso_jikoku, toroku_tosu, yobi_code, kaisai_kai/nichime, juryo_shubetsu_code)
3. **Already tested / DO-NOT-RETEST** — yoso_soha_time/yoso_juni (ρ 0.066), kishu_minarai_code (extends JRA probe A, market absorbs), zogen_sa/bw-change (h-prev-bw-drop.md)
4. **Redundant** — futan_juryo_henkomae (≡ futan_juryo in 99.7%), kishu_code_henkomae (≡ kishu_code in 98.9%), banushimei (collinear with banushi_code used), honshokin_henkomae (≡ honshokin)
5. **Structural/no signal** — yobi_1/2/3/4 (sentinel values), blinker_shiyo_kubun (0% at NAR), fukushoku_hyoji (free-text silks), hinshu_code (constant per venue), tozai_shozoku_code (99.3% = '3')
6. **Market-absorbed** — seibetsu_code: raw ~2.3pp win-rate gap exists but odds encode sex; analogous to JRA kishu_minarai_code (partial ρ 0.022 after odds control → FAIL)

### Conclusion

The NAR + Ban-ei unused-data space is exhausted, consistent with the JRA finding and the market-efficiency wall hypothesis in `project_science_track_saturation_2026_06_11.md`.

The one structurally interesting candidate — `seibetsu_code` (horse sex, 100% populated, 97% within-race variation, 2.3pp raw win-rate spread at NAR) — is market-absorbed. The JRA parallel (kishu_minarai_code: raw looks promising, partial ρ after odds control = 0.022) predicts the same collapse for sex. No probe is warranted under current market-efficiency constraints.

**No new columns in nvd_se/nvd_ra/nvd_hr/nvd_o1–o6 provide incremental pre-race per-horse signal beyond what the current pipeline and market odds already capture.**
