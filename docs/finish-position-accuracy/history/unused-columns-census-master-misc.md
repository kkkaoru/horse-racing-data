# Unused Column Census — Master/Misc Tables

Date: 2026-06-12

## Scope

Census of POPULATED columns NOT consumed by the finish-position ML feature pipeline,
across JRA/NAR master and misc tables only.
Skips: jvd_se / nvd_se / jvd_ra / nvd_ra / jvd_hr / nvd_hr / all odds tables (o1–o6, oa).

Pipeline definition: `finish_position_features_duckdb.py` +
`src/scripts/finish-position-features/*.py` + `*.ts` SQL builders.

---

## Tables Covered

| table  | logical name            | rows       | consumed by pipeline?                                                           |
| ------ | ----------------------- | ---------- | ------------------------------------------------------------------------------- |
| jvd_um | JRA horse master        | 212,988    | PARTIAL (only ketto_joho_01a/04a/01b/05b + ketto_toroku_bango + banushi_code)   |
| nvd_nu | NAR horse master        | 121,030    | PARTIAL (same as jvd_um)                                                        |
| jvd_ks | JRA jockey master       | 1,559      | NO (codes used only from jvd_se)                                                |
| nvd_ks | NAR jockey master       | 1,872      | NO                                                                              |
| nvd_nk | NAR local jockey        | 2,356      | NO                                                                              |
| jvd_ch | JRA trainer master      | 1,475      | NO (codes used only from jvd_se)                                                |
| nvd_ch | NAR trainer master      | 1,385      | NO                                                                              |
| nvd_nc | NAR local trainer       | 1,809      | NO                                                                              |
| jvd_bn | JRA owner master        | 8,707      | NO (owner code from jvd_se only)                                                |
| nvd_bn | NAR owner master        | 9,970      | NO                                                                              |
| nvd_nn | NAR local owner         | 9,485      | NO                                                                              |
| jvd_br | JRA breeder master      | 10,734     | NO                                                                              |
| nvd_nb | NAR breeder master      | 5,542      | NO                                                                              |
| jvd_hn | Breeding horse master   | 161,437    | NO                                                                              |
| jvd_bt | Bloodline info          | 92         | NO                                                                              |
| jvd_cs | Course info             | 119        | NO                                                                              |
| jvd_hc | Sloped track workout    | 11,809,246 | PARTIAL (only lap_time_1-4f + time_gokei_2-4f + tracen_kubun + chokyo_nengappi) |
| jvd_wc | Woodchip workout        | 742,151    | NO (viewer display only)                                                        |
| jvd_ys | Race schedule           | 7,817      | NO                                                                              |
| jvd_dm | Data-mining predictions | 84,231     | NO                                                                              |
| jvd_rc | Course records          | 2,128      | NO                                                                              |
| jvd_hs | Sale price              | 52,900     | DO-NOT-RETEST (ρ=−0.016)                                                        |
| jvd_hy | Horse name origins      | 175,024    | NO                                                                              |
| jvd_jg | Scratch/exclusion info  | 823,161    | NO                                                                              |
| nvd_nr | NAR ability test race   | 3,576      | NO                                                                              |
| nvd_ns | NAR ability test entry  | 17,264     | NO                                                                              |
| jvd_we | JRA weather/going       | 0          | EMPTY                                                                           |
| nvd_we | NAR weather/going       | 0          | EMPTY                                                                           |
| jvd_wh | Horse weight sokuho     | 0          | EMPTY                                                                           |
| nvd_wh | Horse weight sokuho     | 0          | EMPTY                                                                           |
| nvd_um | NAR horse master (alt)  | 0          | EMPTY                                                                           |

---

## Column Census

### jvd_um / nvd_nu (Horse Masters)

Consumed columns: ketto_toroku_bango (PK), ketto_joho_01a (sire ID), ketto_joho_04a (damsire ID),
ketto_joho_01b (sire name), ketto_joho_05b (damsire name), banushi_code.

All columns below are 100% populated (density = 100% for all unless noted).

| table  | column                      | density% | consumed? | leak_class | within_race_var | notes                                                                                   |
| ------ | --------------------------- | -------- | --------- | ---------- | --------------- | --------------------------------------------------------------------------------------- |
| jvd_um | ketto_toroku_bango          | 100      | YES       | structural | YES             | PK, consumed                                                                            |
| jvd_um | ketto_joho_01a              | 100      | YES       | pre-race   | YES             | sire ID, consumed (pedigree_staging)                                                    |
| jvd_um | ketto_joho_04a              | 100      | YES       | pre-race   | YES             | damsire ID, consumed                                                                    |
| jvd_um | ketto_joho_01b              | 100      | YES       | pre-race   | YES             | sire name, consumed (locality probe)                                                    |
| jvd_um | ketto_joho_05b              | 100      | YES       | pre-race   | YES             | damsire name, consumed (locality probe)                                                 |
| jvd_um | banushi_code                | 100      | YES       | pre-race   | YES             | owner code, consumed (banushi_fade)                                                     |
| jvd_um | moshoku_code                | 100      | NO        | pre-race   | YES             | coat color; display only in viewer                                                      |
| jvd_um | seibetsu_code               | 100      | NO        | pre-race   | YES             | sex in jvd_se (race entry), not master; DO-NOT-RETEST (see jvd_se census)               |
| jvd_um | hinshu_code                 | 100      | NO        | pre-race   | NO              | breed type; all JRA = Thoroughbred = zero variation                                     |
| jvd_um | sanchimei                   | 100      | NO        | pre-race   | YES             | production region (Hokkaido/Honshu/overseas)                                            |
| jvd_um | tozai_shozoku_code          | 100      | NO        | pre-race   | YES             | east/west stable affiliation                                                            |
| jvd_um | seisansha_code              | 100      | NO        | pre-race   | YES             | breeder code; see below                                                                 |
| jvd_um | seinengappi                 | 100      | NO        | pre-race   | YES             | birth date (age derived from barei in jvd_se)                                           |
| jvd_um | ketto_joho_02a (dam)        | 100      | NO        | pre-race   | YES             | dam ID (1st cross, deeper pedigree)                                                     |
| jvd_um | ketto_joho_03a..14a         | 100      | NO        | pre-race   | YES             | grandparent+ pedigree IDs (2nd–14th cross) — DO-NOT-RETEST (pedigree counterproductive) |
| jvd_um | ketto_joho_02b..14b (names) | 100      | NO        | pre-race   | YES             | grandparent+ pedigree names — same as above                                             |
| jvd_um | heichi_honshokin_ruikei     | 100      | NO        | post-race  | YES             | cumulative prize money — LEAK (updates after each race result)                          |
| jvd_um | shogai_honshokin_ruikei     | 100      | NO        | post-race  | YES             | obstacle prize money — LEAK                                                             |
| jvd_um | heichi_fukashokin_ruikei    | 100      | NO        | post-race  | YES             | added prize cumulative — LEAK                                                           |
| jvd_um | heichi_shutokushokin_ruikei | 100      | NO        | post-race  | YES             | earned prize cumulative — LEAK                                                          |
| jvd_um | sogo                        | 100      | NO        | post-race  | YES             | overall career finish count breakdown — LEAK (snapshot updated post-race)               |
| jvd_um | chuo_gokei                  | 100      | NO        | post-race  | YES             | central career finish count — LEAK                                                      |
| jvd_um | shiba_choku..dirt_long      | 100      | NO        | post-race  | YES             | per-surface/direction finish breakdown — LEAK                                           |
| jvd_um | shiba_short/middle/long     | 100      | NO        | post-race  | YES             | per-distance finish breakdown — LEAK                                                    |
| jvd_um | dirt_short/middle/long      | 100      | NO        | post-race  | YES             | per-distance dirt finish breakdown — LEAK                                               |
| jvd_um | kyakushitsu_keiko           | 100      | NO        | post-race  | YES             | pace-style tendency code from career — LEAK (derived from results)                      |
| jvd_um | toroku_race_su              | 100      | NO        | post-race  | YES             | registered race count — LEAK                                                            |
| jvd_um | massho_kubun                | 100      | NO        | structural | YES             | retired/active flag — not useful for prediction                                         |
| jvd_um | umakigo_code                | 100      | NO        | pre-race   | YES             | horse symbol/mark code                                                                  |
| jvd_um | zaikyu_flag                 | 100      | NO        | structural | NO              | JRA-facility stabled flag                                                               |
| jvd_um | chokyoshi_code              | 100      | NO        | pre-race   | YES             | trainer code; consumed from jvd_se (chokyoshimei_ryakusho)                              |
| jvd_um | seisanshamei                | 100      | NO        | pre-race   | YES             | breeder name (redundant with seisansha_code)                                            |
| jvd_um | banushimei                  | 100      | NO        | pre-race   | YES             | owner name (redundant with banushi_code, consumed from jvd_se)                          |
| jvd_um | shotai_chiikimei            | 100      | NO        | pre-race   | YES             | invitation region name                                                                  |
| nvd_nu | (same columns as jvd_um)    | 100      | —         | —          | —               | mirrors jvd_um structure; all same verdicts apply                                       |
| nvd_nu | chiho_gokei                 | 100      | NO        | post-race  | YES             | regional career finish breakdown — LEAK                                                 |
| nvd_nu | honshokin_ruikei            | 100      | NO        | post-race  | YES             | prize money cumulative (NAR) — LEAK                                                     |

### jvd_ks / nvd_ks / nvd_nk (Jockey Masters)

Note: kishu_code and kishumei_ryakusho come from jvd_se/nvd_se (race entries), not from these master tables.
The pipeline computes all jockey career stats from historical jvd_se/nvd_se rows.
No join to jvd_ks/nvd_ks/nvd_nk is performed in the pipeline.

| table  | column               | density% | consumed? | leak_class | within_race_var | notes                                                                     |
| ------ | -------------------- | -------- | --------- | ---------- | --------------- | ------------------------------------------------------------------------- |
| jvd_ks | kishu_code           | 100      | NO        | structural | YES             | PK; code already in jvd_se                                                |
| jvd_ks | seinengappi          | 100      | NO        | pre-race   | YES             | jockey birth date → age/experience proxy                                  |
| jvd_ks | massho_kubun         | 100      | NO        | structural | YES             | retired flag                                                              |
| jvd_ks | menkyo_kofu_nengappi | 100      | NO        | pre-race   | YES             | license grant date → years licensed                                       |
| jvd_ks | kijo_shikaku_code    | 100      | NO        | pre-race   | YES             | riding qualification (flat/obstacle/both)                                 |
| jvd_ks | kishu_minarai_code   | 100      | NO        | pre-race   | YES             | apprentice grade — DO-NOT-RETEST (probed in jvd_se census, ρ=+0.022 FAIL) |
| jvd_ks | tozai_shozoku_code   | 100      | NO        | pre-race   | YES             | east/west affiliation                                                     |
| jvd_ks | seiseki_joho_1/2/3   | 100      | NO        | post-race  | YES             | packed win/place results blob — LEAK (post-race update)                   |
| nvd_ks | (same fields)        | 100      | NO        | —          | —               | NAR jockey master; same verdicts                                          |
| nvd_nk | (same fields)        | 100      | NO        | —          | —               | NAR local jockey; same verdicts                                           |

### jvd_ch / nvd_ch / nvd_nc (Trainer Masters)

Note: chokyoshimei_ryakusho comes from jvd_se (consumed). All career stats in pipeline are
computed window-function style from historical race entries, not from the seiseki_joho blobs.

| table  | column                | density% | consumed? | leak_class | within_race_var | notes                                        |
| ------ | --------------------- | -------- | --------- | ---------- | --------------- | -------------------------------------------- |
| jvd_ch | chokyoshi_code        | 100      | NO        | structural | YES             | PK; trainer code from jvd_se                 |
| jvd_ch | seinengappi           | 100      | NO        | pre-race   | YES             | trainer birth date                           |
| jvd_ch | menkyo_kofu_nengappi  | 100      | NO        | pre-race   | YES             | license date → years licensed                |
| jvd_ch | tozai_shozoku_code    | 100      | NO        | pre-race   | YES             | east/west affiliation                        |
| jvd_ch | seibetsu_kubun        | 100      | NO        | pre-race   | YES             | trainer gender                               |
| jvd_ch | seiseki_joho_1/2/3    | 100      | NO        | post-race  | YES             | packed career result blob — LEAK             |
| jvd_ch | jushoshori_joho_1/2/3 | 100      | NO        | post-race  | YES             | recent graded wins — LEAK (post-race update) |
| nvd_ch | (same fields)         | 100      | NO        | —          | —               | NAR trainer master; same verdicts            |
| nvd_nc | (same fields)         | 100      | NO        | —          | —               | NAR local trainer; same verdicts             |

### jvd_bn / nvd_bn / nvd_nn (Owner Masters)

Note: banushi_code is consumed from jvd_se and used for banushi_fade_rate features.
The master tables themselves are not joined.

| table  | column           | density% | consumed? | leak_class | within_race_var | notes                               |
| ------ | ---------------- | -------- | --------- | ---------- | --------------- | ----------------------------------- |
| jvd_bn | banushimei       | 100      | NO        | pre-race   | YES             | owner name; display only            |
| jvd_bn | fukushoku_hyoji  | 100      | NO        | pre-race   | YES             | jockey silks description; free text |
| jvd_bn | seiseki_joho_1/2 | 100      | NO        | post-race  | YES             | owner career results blob — LEAK    |
| nvd_bn | (same)           | 100      | NO        | —          | —               | NAR owner; same verdicts            |
| nvd_nn | (same)           | 100      | NO        | —          | —               | NAR local owner; same verdicts      |

### jvd_br / nvd_nb (Breeder Masters)

Note: seisansha_code exists in jvd_um but is NOT consumed by the feature pipeline.

| table  | column                      | density% | consumed? | leak_class | within_race_var | notes                                           |
| ------ | --------------------------- | -------- | --------- | ---------- | --------------- | ----------------------------------------------- |
| jvd_br | seisansha_code              | 100      | NO        | structural | YES             | PK                                              |
| jvd_br | seisanshamei                | 100      | NO        | pre-race   | YES             | breeder name                                    |
| jvd_br | seisansha_jusho_jichishomei | 100      | NO        | pre-race   | NO              | farm address prefecture (e.g. 北海道 vs others) |
| jvd_br | seiseki_joho_1/2            | 100      | NO        | post-race  | YES             | breeder career results — LEAK                   |
| nvd_nb | seisanshamei                | 100      | NO        | pre-race   | YES             | NAR breeder name                                |
| nvd_nb | seiseki_joho_1              | 100      | NO        | post-race  | YES             | NAR breeder results — LEAK                      |

### jvd_hn (Breeding Horse Master)

| table  | column                | density% | consumed? | leak_class | within_race_var | notes                                |
| ------ | --------------------- | -------- | --------- | ---------- | --------------- | ------------------------------------ |
| jvd_hn | hanshoku_toroku_bango | 100      | NO        | structural | YES             | PK (breeding registration)           |
| jvd_hn | ketto_toroku_bango    | 100      | NO        | structural | YES             | link to race horse master            |
| jvd_hn | bamei                 | 100      | NO        | pre-race   | YES             | sire/dam name                        |
| jvd_hn | seibetsu_code         | 100      | NO        | pre-race   | YES             | sex of breeding horse                |
| jvd_hn | moshoku_code          | 100      | NO        | pre-race   | YES             | coat color                           |
| jvd_hn | mochikomi_kubun       | 100      | NO        | pre-race   | YES             | imported vs domestic born flag       |
| jvd_hn | yunyu_nen             | 100      | NO        | pre-race   | YES             | import year (foreign sires)          |
| jvd_hn | sanchimei             | 100      | NO        | pre-race   | YES             | production region                    |
| jvd_hn | ketto_joho_01a/02a    | 100      | NO        | pre-race   | YES             | grandparent pedigree — DO-NOT-RETEST |

### jvd_bt (Bloodline Info)

| table  | column         | density% | consumed? | leak_class | within_race_var | notes                                                        |
| ------ | -------------- | -------- | --------- | ---------- | --------------- | ------------------------------------------------------------ |
| jvd_bt | keito_id       | 100      | NO        | pre-race   | YES             | bloodline ID (Eclipse, Northern Dancer, etc.) — 92 rows only |
| jvd_bt | keito_mei      | 100      | NO        | pre-race   | YES             | bloodline name                                               |
| jvd_bt | keito_setsumei | 100      | NO        | pre-race   | YES             | bloodline description text                                   |

### jvd_cs (Course Info)

| table  | column                 | density% | consumed? | leak_class | within_race_var | notes                                       |
| ------ | ---------------------- | -------- | --------- | ---------- | --------------- | ------------------------------------------- |
| jvd_cs | keibajo_code           | 100      | NO        | structural | NO              | PK                                          |
| jvd_cs | kyori                  | 100      | NO        | structural | NO              | race distance (same for all horses in race) |
| jvd_cs | track_code             | 100      | NO        | structural | NO              | turf/dirt/obstacle track                    |
| jvd_cs | course_kaishu_nengappi | 100      | NO        | structural | NO              | last renovation date                        |
| jvd_cs | course_setsumei        | 100      | NO        | structural | NO              | course description text                     |

### jvd_hc (Sloped Track / CW Workout) — PARTIAL consumption

Pipeline consumes: ketto_toroku_bango, chokyo_nengappi, tracen_kubun (CW location),
lap_time_1f, lap_time_2f, lap_time_3f, lap_time_4f, time_gokei_2f, time_gokei_3f, time_gokei_4f.
All 14 columns in jvd_hc are consumed or are admin keys.
Note: jvd_hc has only 14 columns in this local DB (no 5f–10f columns); those are in jvd_wc.

| table  | column               | density% | consumed? | leak_class | within_race_var | notes                                                                           |
| ------ | -------------------- | -------- | --------- | ---------- | --------------- | ------------------------------------------------------------------------------- |
| jvd_hc | chokyo_jikoku        | 100      | NO        | pre-race   | YES             | workout session timestamp (used for dedup ordering in pipeline, not as feature) |
| jvd_hc | record_id/data_kubun | 100      | NO        | structural | NO              | admin fields                                                                    |

### jvd_wc (Woodchip Workout) — 742k rows, NOT in ML pipeline

Pipeline uses jvd_hc (sloped track) only. jvd_wc is used in viewer display (queries.ts) for
recent workouts table but is NOT joined in any finish-position feature script.

| table  | column             | density% | consumed? | leak_class | within_race_var | notes                          |
| ------ | ------------------ | -------- | --------- | ---------- | --------------- | ------------------------------ |
| jvd_wc | course             | 100      | NO        | pre-race   | YES             | WC course designation          |
| jvd_wc | babamawari         | 100      | NO        | pre-race   | YES             | clockwise/counter flag         |
| jvd_wc | time_gokei_2f..10f | 100      | NO        | pre-race   | YES             | woodchip cumulative lap times  |
| jvd_wc | lap_time_1f..10f   | 100      | NO        | pre-race   | YES             | woodchip individual lap splits |

### jvd_ys (Race Schedule)

| table  | column           | density% | consumed? | leak_class | within_race_var | notes                                         |
| ------ | ---------------- | -------- | --------- | ---------- | --------------- | --------------------------------------------- |
| jvd_ys | yobi_code        | 100      | NO        | pre-race   | NO              | day-of-week code; same for all horses in race |
| jvd_ys | jusho_joho_1/2/3 | 100      | NO        | pre-race   | NO              | race meeting award info (trophy type)         |
| jvd_ys | kaisai_kai       | 100      | NO        | pre-race   | NO              | meeting number in season                      |
| jvd_ys | kaisai_nichime   | 100      | NO        | pre-race   | NO              | day-in-meeting number                         |

### jvd_dm (JRA Data-Mining Predictions) — 84k rows

Built by JRA internal system. mining_yoso_01..10 are ~93–100% populated; 11–18 sparse.

| table  | column             | density% | consumed? | leak_class | within_race_var | notes                           |
| ------ | ------------------ | -------- | --------- | ---------- | --------------- | ------------------------------- |
| jvd_dm | mining_yoso_01     | 100      | NO        | pre-race   | YES             | JRA proprietary ranking score 1 |
| jvd_dm | mining_yoso_02     | 100      | NO        | pre-race   | YES             | JRA proprietary ranking score 2 |
| jvd_dm | mining_yoso_03     | 100      | NO        | pre-race   | YES             | score 3                         |
| jvd_dm | mining_yoso_04     | 100      | NO        | pre-race   | YES             | score 4                         |
| jvd_dm | mining_yoso_05     | 100      | NO        | pre-race   | YES             | score 5                         |
| jvd_dm | mining_yoso_06     | 100      | NO        | pre-race   | YES             | score 6                         |
| jvd_dm | mining_yoso_07     | 100      | NO        | pre-race   | YES             | score 7                         |
| jvd_dm | mining_yoso_08     | 100      | NO        | pre-race   | YES             | score 8                         |
| jvd_dm | mining_yoso_09     | 100      | NO        | pre-race   | YES             | score 9                         |
| jvd_dm | mining_yoso_10     | 93       | NO        | pre-race   | YES             | score 10 (93% populated)        |
| jvd_dm | mining_yoso_11..14 | 30–66    | NO        | pre-race   | YES             | scores 11–14 (66% → 30%)        |
| jvd_dm | mining_yoso_15..18 | 0–8      | NO        | pre-race   | YES             | scores 15–18 sparse (<30%)      |

### jvd_rc (Course Records) — 2128 rows

| table  | column      | density% | consumed? | leak_class | within_race_var | notes                                      |
| ------ | ----------- | -------- | --------- | ---------- | --------------- | ------------------------------------------ |
| jvd_rc | record_time | 100      | NO        | structural | NO              | course record time per keibajo/kyori/track |
| jvd_rc | grade_code  | 56       | NO        | structural | NO              | grade of record-setting race               |

### jvd_hs (Sale Price) — DO-NOT-RETEST

| table  | column          | density% | consumed? | leak_class | within_race_var | notes                                   |
| ------ | --------------- | -------- | --------- | ---------- | --------------- | --------------------------------------- |
| jvd_hs | torihiki_kakaku | ~90      | NO        | pre-race   | YES             | DO-NOT-RETEST: already probed, ρ=−0.016 |

### jvd_hy (Horse Name Origins) — 175k rows

| table  | column          | density% | consumed? | leak_class | within_race_var | notes                                         |
| ------ | --------------- | -------- | --------- | ---------- | --------------- | --------------------------------------------- |
| jvd_hy | bamei_imi_yurai | 100      | NO        | pre-race   | YES             | free text etymology — no structured ML signal |

### jvd_jg (Scratch/Exclusion Info) — 823k rows

| table  | column            | density% | consumed? | leak_class | within_race_var | notes                                                                     |
| ------ | ----------------- | -------- | --------- | ---------- | --------------- | ------------------------------------------------------------------------- |
| jvd_jg | shusso_kubun      | 100      | NO        | pre-race   | YES             | start category (normal vs excluded)                                       |
| jvd_jg | jogai_jotai_kubun | 100      | NO        | pre-race   | YES             | exclusion state code — horses that scratched do not appear in jvd_se rows |

### nvd_nr / nvd_ns (NAR Ability Tests) — pre-race qualification tests for NAR horses

| table  | column             | density% | consumed? | leak_class | within_race_var | notes                                    |
| ------ | ------------------ | -------- | --------- | ---------- | --------------- | ---------------------------------------- |
| nvd_nr | lap_time           | 100      | NO        | pre-race   | NO              | ability test race lap times — race-level |
| nvd_nr | zenhan_3f          | 100      | NO        | pre-race   | YES             | ability test front 3f time               |
| nvd_nr | kohan_3f           | 100      | NO        | pre-race   | YES             | ability test back 3f time                |
| nvd_nr | tenko_code         | 100      | NO        | pre-race   | NO              | weather at test                          |
| nvd_ns | juni               | 100      | NO        | pre-race   | YES             | ability test finish rank                 |
| nvd_ns | soha_time          | 100      | NO        | pre-race   | YES             | total time in ability test               |
| nvd_ns | kohan_3f           | 100      | NO        | pre-race   | YES             | back 3f in ability test                  |
| nvd_ns | kyakushitsu_hantei | 100      | NO        | pre-race   | YES             | pace style at ability test               |

---

## Genuine New Candidates (pre-race + within-race-varying + populated + not DO-NOT-RETEST)

After removing: DO-NOT-RETEST columns, post-race leaks, structural/admin fields, zero-within-race columns, columns collinear with already-consumed fields.

### Candidate 1: jvd_dm.mining_yoso_01..10 (JRA proprietary predictions)

- **Table:** jvd_dm (84k rows, JRA only)
- **Density:** 93–100%
- **leak_class:** pre-race
- **within_race_varying:** YES (per-horse position predictions)
- **serve_available:** YES (available before race via JV-Link data feed)
- **Assessment:** JRA's own in-house data-mining scoring system. Contains up to 10 ranked
  scores per race with 100% density for scores 1–9. These are computed by JRA before the
  race and would be available at inference time.
  **HOWEVER:** These are effectively public predictions — the market odds already incorporate
  this information (and more). Adding a noisy approximation of what the market already priced
  in is expected to either be redundant or adversely calibrated. The science-track saturation
  finding (project_science_track_saturation_2026_06_11) confirmed odds-decoupling is
  counterproductive. The mining_yoso signals should correlate strongly with odds.
  **Verdict: LOW PRIORITY — high correlation with odds, likely redundant. Probe only if
  a non-odds baseline is being explored.**

### Candidate 2: jvd_wc woodchip workout times (analogous to jvd_hc but different surface)

- **Table:** jvd_wc (742k rows, JRA only)
- **Columns:** time_gokei_2f..4f, lap_time_1f..4f (identical schema to jvd_hc)
- **Density:** 100%
- **leak_class:** pre-race
- **within_race_varying:** YES
- **serve_available:** YES
- **Assessment:** jvd_hc (sloped/坂路 workout) is already consumed. jvd_wc covers woodchip
  (ウッドチップ) track workouts at the same training centers. Many horses alternate between
  CW (jvd_hc) and WC (jvd_wc). The feature pipeline currently has only ~JRA coverage for
  workout features (jvd_hc). Adding jvd_wc would double the workout observation source for
  JRA horses, especially those that prefer woodchip and have low jvd_hc density.
  **Verdict: MODERATE INTEREST — extends workout coverage for JRA horses. Worth checking
  coverage (what % of JRA horses in training have jvd_wc rows but sparse jvd_hc rows).**

### Candidate 3: jvd_ys.yobi_code (day-of-week of race meeting)

- **Table:** jvd_ys (7,817 rows)
- **Density:** 100%
- **leak_class:** pre-race
- **within_race_varying:** NO (same for all horses in a race meeting)
- **Verdict: DISQUALIFIED — not within-race varying. Same day-of-week for all horses
  in a meeting. No differential signal.**

### Candidate 4: jvd_br.seisansha_jusho_jichishomei (breeder prefecture)

- **Table:** jvd_br (10,734 rows)
- **Density:** 100%
- **leak_class:** pre-race
- **within_race_varying:** YES (different horses have different breeders)
- **serve_available:** YES
- **Assessment:** Prefecture of breeding farm (北海道 vs 青森 etc.). Proxies for breeding
  operation quality and environment. However: (a) most JRA horses are Hokkaido-bred —
  very low variance; (b) seisansha_code is already in jvd_um but was not consumed — if it
  were useful, a breeder career-fade feature would be the right representation (computed from
  race history, not this static table). The static address field has no direct predictive
  theory.
  **Verdict: LOW PRIORITY — low variance, no predictive theory beyond what seisansha career
  stats would capture.**

### Candidate 5: nvd_ns ability test performance (NAR horses only)

- **Table:** nvd_ns (17,264 rows), nvd_nr (3,576 rows)
- **Columns:** juni (finish rank), soha_time (total time), kohan_3f, kyakushitsu_hantei
- **Density:** 100%
- **leak_class:** pre-race
- **within_race_varying:** YES
- **serve_available:** YES
- **Assessment:** NAR horses undergo ability tests (能力試験) before their first race.
  The test times and rank could proxy initial speed ceiling. Coverage is limited to horses
  that recently passed qualification — mostly first-season runners. NAR dataset is ~1.9M rows;
  17k ability test entries means very sparse coverage relative to total race population.
  **Verdict: SPARSE COVERAGE (<1% of NAR race entries) — useful only for maiden-first-run
  horses. Niche signal. Not worth broad integration.**

---

## DO-NOT-RETEST Columns (already investigated)

| column                           | table(s)      | reason                                                                                            |
| -------------------------------- | ------------- | ------------------------------------------------------------------------------------------------- |
| ketto_joho_01a/01b (sire)        | jvd_um/nvd_nu | pedigree features already tested, counterproductive (project_finish_position_frontier_2026_06_11) |
| ketto_joho_04a/05b (damsire)     | jvd_um/nvd_nu | same as above                                                                                     |
| ketto_joho_03a..14a (3rd-cross+) | jvd_um/nvd_nu | deeper pedigree — same verdict                                                                    |
| seibetsu_code (sex)              | jvd_se/jvd_um | DO-NOT-RETEST (jvd_se census: consumed, in active model)                                          |
| barei (age)                      | jvd_se        | consumed, in active model                                                                         |
| bataiju (body weight)            | jvd_se        | consumed, in active model                                                                         |
| kishu_minarai_code               | jvd_se/jvd_ks | probed, ρ=+0.022 FAIL                                                                             |
| blinker_shiyo_kubun              | jvd_se        | probed, ρ=+0.018 FAIL                                                                             |
| torihiki_kakaku (sale price)     | jvd_hs        | probed, ρ=−0.016 FAIL                                                                             |
| chokyoshi+kishu combo            | jvd_se        | tested as feature, consumed                                                                       |
| banushi_code (owner)             | jvd_se/jvd_um | banushi_fade features consumed and active                                                         |
| kNN/pgvector horse embedding     | n/a           | already probed, counterproductive                                                                 |

---

## Empty / Sparse Tables

| table  | rows | note                                                       |
| ------ | ---- | ---------------------------------------------------------- |
| jvd_we | 0    | JRA weather real-time update; always empty in local mirror |
| nvd_we | 0    | NAR weather; same                                          |
| jvd_wh | 0    | Sokuho horse weight; always empty                          |
| nvd_wh | 0    | Same                                                       |
| nvd_um | 0    | NAR horse master (alternate); use nvd_nu instead           |
| jvd_cc | 0    | Course change records; no active events                    |
| jvd_jc | 0    | Jockey change (JRA); no active events                      |
| nvd_jc | 0    | Jockey change (NAR); no active events                      |
| jvd_tc | 0    | Start time change; no active events                        |
| nvd_tc | 0    | Start time change; no active events                        |
| jvd_bt | 92   | Bloodline info — very sparse (92 stallion entries)         |
| jvd_cs | 119  | Course info — 119 unique keibajo/kyori/track combos        |

---

## Summary

- **Tables covered:** 31 master/misc tables (excluding result tables se/ra/hr and odds)
- **Populated but unconsumed:** ~180+ columns across 20+ populated tables
- **Post-race leaks:** jvd*um career-stat family (sogo, shiba*\*, heichi_honshokin_ruikei, etc.) — all LEAK
- **Genuine new candidates:** 2 actionable candidates (jvd_dm mining scores, jvd_wc workouts)
- **Verdict:** Master/misc tables do NOT contain untested pre-race signals with high expected
  value. The closest candidates are jvd_dm (redundant with odds) and jvd_wc (extends workout
  coverage, moderate interest). The career stat columns in jvd_um/nvd_nu that look "populated"
  are all post-race snapshots and therefore leak. The overall finding aligns with
  project_science_track_saturation_2026_06_11: the data space is exhausted.
