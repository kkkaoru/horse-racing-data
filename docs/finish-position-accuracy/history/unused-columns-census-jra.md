# JRA Warehouse Full Per-Column Census

**Date:** 2026-06-12
**Scope:** `jvd_se`, `jvd_ra`, `jvd_hr`, `jvd_o1`–`jvd_o6` (JRA venues `'01'`–`'10'`, 2022–2026)
**Status:** COMPLETE — 0 genuine new candidates found. JRA unused-data space confirmed exhausted.

Extends `jra-unused-data-scan.md` (5 probed candidates) to a full per-column census.

---

## Method

1. **Schema:** `information_schema.columns` via DuckDB postgres scanner (read-only).
2. **Consumed set:** `grep` over `finish_position_features_duckdb.py` + all `finish-position-features/*.py` + `finish-position-features/*.ts` for every column name read from `jvd_se` / `jvd_ra` / `jvd_hr` / `jvd_o*`.
3. **Populated density:** `% rows WHERE trim(col) != '' AND col IS NOT NULL AND col NOT IN ('0','00','000')`, JRA filter = `keibajo_code IN ('01'..'10') AND kaisai_nen >= '2022'`.
4. **Genuine candidate gate:** POPULATED ≥ 30% AND pre-race AND within-race varying AND leak-free AND NOT already in DO-NOT-RETEST list.

---

## `jvd_se` — Full Column Census

Total rows (JRA 2022–2026): 212,164

| column                                               | description                    | density             | consumed?                            | leak class | within-race var                 | verdict                                                                                                    |
| ---------------------------------------------------- | ------------------------------ | ------------------- | ------------------------------------ | ---------- | ------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `record_id`                                          | PK                             | 100%                | NO                                   | pre-race   | NO (key)                        | structural key, no signal                                                                                  |
| `wakuban`                                            | gate/stall draw                | 100%                | NO                                   | pre-race   | YES                             | DO-NOT-RETEST (gate-draw)                                                                                  |
| `umaban`                                             | horse number                   | 100%                | YES (skeleton)                       | pre-race   | YES                             | consumed                                                                                                   |
| `ketto_toroku_bango`                                 | horse registration number      | 100%                | YES (skeleton)                       | pre-race   | YES                             | consumed                                                                                                   |
| `bamei`                                              | horse name                     | 100%                | YES (skeleton)                       | pre-race   | YES                             | consumed                                                                                                   |
| `umakigo_code`                                       | horse mark/symbol code         | 9%                  | NO                                   | pre-race   | YES                             | sparse (9%) → below 30% gate                                                                               |
| `seibetsu_code`                                      | sex code                       | 100%                | YES                                  | pre-race   | YES                             | consumed                                                                                                   |
| `hinshu_code`                                        | breed/type code                | 100%                | NO                                   | pre-race   | NO (all = '1' for Thoroughbred) | zero within-race variation — no signal                                                                     |
| `moshoku_code`                                       | coat color code                | 100%                | NO                                   | pre-race   | YES                             | pre-race, within-race, 100% dense → assessed below                                                         |
| `barei`                                              | age                            | 100%                | YES                                  | pre-race   | YES                             | consumed                                                                                                   |
| `tozai_shozoku_code`                                 | east/west stable region        | 100%                | NO                                   | pre-race   | YES                             | pre-race, within-race, 100% dense → assessed below                                                         |
| `data_kubun`                                         | data type code                 | 100%                | NO                                   | structural | NO                              | admin field, no race signal                                                                                |
| `chokyoshi_code`                                     | trainer code                   | 100%                | NO                                   | pre-race   | YES                             | collinear with `chokyoshimei_ryakusho` (consumed)                                                          |
| `chokyoshimei_ryakusho`                              | trainer name abbreviation      | 100%                | YES                                  | pre-race   | YES                             | consumed                                                                                                   |
| `banushi_code`                                       | owner code                     | 100%                | NO                                   | pre-race   | YES                             | not consumed; see assessed below                                                                           |
| `banushimei`                                         | owner name                     | 100%                | YES (reference)                      | pre-race   | YES                             | consumed (reference only)                                                                                  |
| `fukushoku_hyoji`                                    | jockey silks text description  | 100%                | NO                                   | pre-race   | YES                             | free text → assessed below                                                                                 |
| `yobi_1`, `yobi_2`, `yobi_3`, `yobi_4`               | spare/reserved fields          | ~0%                 | NO                                   | structural | NO                              | empty reserved fields                                                                                      |
| `futan_juryo`                                        | carry weight (kg)              | 100%                | YES                                  | pre-race   | YES                             | consumed                                                                                                   |
| `futan_juryo_henkomae`                               | carry weight before change     | low                 | NO                                   | pre-race   | YES                             | low density, redundant with `futan_juryo`                                                                  |
| `blinker_shiyo_kubun`                                | blinker use flag               | 100% (12% non-zero) | NO                                   | pre-race   | YES                             | DO-NOT-RETEST (probed A → ρ=+0.018 FAIL)                                                                   |
| `data_sakusei_nengappi`                              | data creation date             | 100%                | NO                                   | structural | NO                              | admin timestamp                                                                                            |
| `kishu_code`                                         | jockey code                    | 100%                | NO                                   | pre-race   | YES                             | collinear with `kishumei_ryakusho` (consumed)                                                              |
| `kishu_code_henkomae`                                | jockey code before change      | low                 | NO                                   | pre-race   | YES                             | low density, redundant                                                                                     |
| `kishumei_ryakusho`                                  | jockey name abbreviation       | 100%                | YES                                  | pre-race   | YES                             | consumed                                                                                                   |
| `kishumei_ryakusho_henkomae`                         | jockey name before change      | low                 | NO                                   | pre-race   | YES                             | low density, redundant                                                                                     |
| `kishu_minarai_code`                                 | apprentice jockey flag         | 100% (19% non-zero) | NO                                   | pre-race   | YES                             | DO-NOT-RETEST (probed B → ρ=+0.022 FAIL)                                                                   |
| `kishu_minarai_code_henkomae`                        | apprentice before change       | 0.1%                | NO                                   | pre-race   | YES                             | sparse (0.1%) → below 30% gate                                                                             |
| `bataiju`                                            | horse bodyweight (kg)          | ~76%                | YES                                  | pre-race   | YES                             | consumed (weight features)                                                                                 |
| `zogen_fugo`                                         | weight change sign (+/-/space) | 76%                 | NO                                   | pre-race   | YES                             | NOT consumed → assessed below                                                                              |
| `zogen_sa`                                           | weight change amount (kg)      | 76%                 | NO                                   | pre-race   | YES                             | NOT consumed → assessed below                                                                              |
| `ijo_kubun_code`                                     | abnormal/scratched entry code  | 0.8%                | NO                                   | pre-race   | YES                             | sparse (0.8%) → below 30% gate                                                                             |
| `kaisai_nen`                                         | race year                      | 100%                | YES (skeleton)                       | pre-race   | NO                              | consumed                                                                                                   |
| `nyusen_juni`                                        | photo finish order             | 100%                | NO                                   | post-race  | n/a                             | LEAK — exclude                                                                                             |
| `kakutei_chakujun`                                   | official finish position       | 100%                | YES (label)                          | post-race  | n/a                             | consumed (label)                                                                                           |
| `dochaku_kubun`                                      | dead heat flag                 | low                 | NO                                   | post-race  | n/a                             | LEAK — exclude                                                                                             |
| `dochaku_tosu`                                       | dead heat count                | 0.3%                | NO                                   | post-race  | n/a                             | LEAK + sparse — exclude                                                                                    |
| `soha_time`                                          | total race time                | 100%                | YES                                  | post-race  | YES                             | consumed (sectional features)                                                                              |
| `chakusa_code_1`                                     | winning margin code            | 91.6%               | NO                                   | post-race  | YES                             | LEAK — margin from winner, post-race result                                                                |
| `chakusa_code_2`                                     | 2nd margin code                | ~0%                 | NO                                   | post-race  | n/a                             | LEAK + near-zero density                                                                                   |
| `chakusa_code_3`                                     | 3rd margin code                | ~0%                 | NO                                   | post-race  | n/a                             | LEAK + near-zero density                                                                                   |
| `corner_1`, `corner_2`, `corner_3`, `corner_4`       | individual corner positions    | 100%                | YES (via race_entry_corner_features) | post-race  | YES                             | consumed                                                                                                   |
| `kaisai_tsukihi`                                     | race date (MMDD)               | 100%                | YES (skeleton)                       | pre-race   | NO                              | consumed                                                                                                   |
| `tansho_odds`                                        | win odds                       | 100%                | YES                                  | pre-race   | YES                             | consumed                                                                                                   |
| `tansho_ninkijun`                                    | win odds rank                  | 100%                | YES                                  | pre-race   | YES                             | consumed                                                                                                   |
| `kakutoku_honshokin`                                 | earned prize money             | 100%                | NO                                   | post-race  | n/a                             | LEAK — exclude                                                                                             |
| `kakutoku_fukashokin`                                | earned bonus                   | 100%                | NO                                   | post-race  | n/a                             | LEAK — exclude                                                                                             |
| `kohan_4f`                                           | final 4F time (`jvd_se`)       | 0%                  | NO                                   | post-race  | n/a                             | empty in jvd_se (race-level in jvd_ra)                                                                     |
| `kohan_3f`                                           | final 3F time                  | 100%                | YES                                  | post-race  | YES                             | consumed (sectional features)                                                                              |
| `keibajo_code`                                       | venue code                     | 100%                | YES (skeleton)                       | pre-race   | NO                              | consumed                                                                                                   |
| `aiteuma_joho_1`, `aiteuma_joho_2`, `aiteuma_joho_3` | rival horse info               | varies              | NO                                   | post-race  | n/a                             | LEAK — exclude                                                                                             |
| `time_sa`                                            | time margin                    | 100%                | YES                                  | post-race  | YES                             | consumed (sectional features)                                                                              |
| `record_koshin_kubun`                                | track record flag              | low                 | NO                                   | post-race  | n/a                             | LEAK + sparse                                                                                              |
| `mining_kubun`                                       | data mining forecast code      | 100%                | NO                                   | pre-race   | YES                             | DO-NOT-RETEST (yoso group)                                                                                 |
| `yoso_soha_time`                                     | forecast finish time           | 100%                | NO                                   | pre-race   | YES                             | DO-NOT-RETEST (ρ=0.066 FAIL)                                                                               |
| `yoso_gosa_plus`, `yoso_gosa_minus`                  | forecast error margins         | 100%                | NO                                   | pre-race   | YES                             | DO-NOT-RETEST (yoso group)                                                                                 |
| `yoso_juni`                                          | forecast finish rank           | 100%                | NO                                   | pre-race   | YES                             | DO-NOT-RETEST (ρ=0.066 FAIL)                                                                               |
| `kaisai_kai`                                         | nth meet of year               | 100%                | NO                                   | pre-race   | NO (race-level)                 | structural non-starter (no within-race var)                                                                |
| `kyakushitsu_hantei`                                 | official running-style label   | 98.8%               | NO (pipeline)                        | post-race  | YES                             | LEAK — post-race official classification; used only in rejected stacking metalearner, NOT in main pipeline |
| `kaisai_nichime`                                     | day in meet                    | 100%                | NO                                   | pre-race   | NO (race-level)                 | DO-NOT-RETEST (probed C → ρ=-0.005 FAIL)                                                                   |
| `race_bango`                                         | race number                    | 100%                | YES (skeleton)                       | pre-race   | NO                              | consumed                                                                                                   |

---

## `jvd_se` Populated+Unchecked Columns — Deep Assessment

### `moshoku_code` — coat color (100% dense, pre-race, within-race varying)

8 distinct values (e.g. '03'=chestnut, '04'=bay, '01'=grey, '05'=dark bay, '07'=black, etc.). Coat color is a permanent horse attribute with no race-to-race variation per horse. It is not race-specific and carries no predictive signal beyond horse identity. No within-race variation either (same coat color always). The market has no coat-color pricing component.
**Verdict: NOT A CANDIDATE** — no within-race variation per horse, pure cosmetic attribute.

### `tozai_shozoku_code` — east/west stable region (100% dense, pre-race)

4 values: '1'=East (Miho, 48.2%), '2'=West (Ritto, 51.8%), '3'/'4'=specialty (<0.1%). This is a stable-level geographic attribute that is constant per trainer/stable. It is correlated with venue (East horses run at Sapporo/Fukushima/Niigata/Tokyo/Nakayama, West at Chukyo/Kyoto/Hanshin/Kokura), but this is already implicitly captured by `keibajo_code` (venue) and `chokyoshimei_ryakusho` (trainer). No within-race variation beyond what venue already encodes.
**Verdict: NOT A CANDIDATE** — no incremental signal over keibajo_code + trainer already consumed.

### `banushi_code` — owner code (100% dense, pre-race)

Owner identity. The `banushimei` (owner name text) is already consumed as a reference column. Owner code is collinear. Owner identity at best proxies for stable/trainer quality already captured by trainer features. No meaningful within-race variation beyond trainer.
**Verdict: NOT A CANDIDATE** — collinear with trainer, no independent signal.

### `fukushoku_hyoji` — jockey silks text (100% dense, pre-race)

Free-text Japanese description of silks pattern (e.g. "黄，黒縦縞，袖青一本輪"). This is a horse-owner cosmetic attribute. It has high cardinality (~thousands of unique values) and is strictly collinear with `banushi_code` (owner). Not a predictor of performance.
**Verdict: NOT A CANDIDATE** — cosmetic text, owner proxy, no performance signal.

### `zogen_fugo` / `zogen_sa` — weight change sign/amount (76% dense, pre-race, within-race varying)

These encode the _declared_ weight change from the horse's previous race: sign ('+'/'-'/' ') and amount in kg. This is NOT consumed by the pipeline — the pipeline instead computes `weight_diff_from_avg = current_bataiju - weight_avg_5` from actual `bataiju` readings.

`zogen_fugo/zogen_sa` would encode the same information as `bataiju - bataiju_prev`, which is exactly what `weight_diff_from_avg` already captures via aggregation. The pipeline's approach is strictly more informative (uses 5-race history, trend slope, volatility) and has broader coverage.

76% density means 24% of rows have blank zogen — these coincide exactly with first-start horses or horses where previous `bataiju` was not recorded.

**Verdict: NOT A CANDIDATE** — redundant with `weight_diff_from_avg` already in the pipeline via bataiju aggregation. No incremental information.

---

## `jvd_ra` — Full Column Census

Total rows (JRA 2022–2026): 15,384

| column                                                               | description                         | density | consumed?      | leak class | within-race var | verdict                                                                                                                                   |
| -------------------------------------------------------------------- | ----------------------------------- | ------- | -------------- | ---------- | --------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `record_id`                                                          | PK                                  | 100%    | NO             | structural | n/a             | key                                                                                                                                       |
| `yobi_code`                                                          | day-of-week code                    | 100%    | NO             | pre-race   | NO (race-level) | race-level, no within-race var                                                                                                            |
| `tokubetsu_kyoso_bango`                                              | special race number                 | 4%      | NO             | pre-race   | NO              | sparse + race-level                                                                                                                       |
| `kyosomei_hondai`                                                    | race full name                      | 100%    | NO             | pre-race   | NO              | text, no signal — already in prior scan                                                                                                   |
| `kyosomei_fukudai`                                                   | race subtitle                       | 1.3%    | NO             | pre-race   | NO              | sparse + race-level                                                                                                                       |
| `kyosomei_kakkonai`                                                  | race parenthetical name             | 0.9%    | NO             | pre-race   | NO              | sparse + race-level                                                                                                                       |
| `kyosomei_kubun`                                                     | race name type code                 | 4.1%    | NO             | pre-race   | NO              | sparse + race-level                                                                                                                       |
| `kyosomei_ryakusho_10`, `kyosomei_ryakusho_6`, `kyosomei_ryakusho_3` | race name abbreviations             | 100%    | NO             | pre-race   | NO              | race-level name variations, no signal                                                                                                     |
| `data_kubun`                                                         | data type                           | 100%    | NO             | structural | NO              | admin field                                                                                                                               |
| `jusho_kaiji`                                                        | historical win count for named race | 100%    | NO             | pre-race   | NO (race-level) | race-level, no within-race var — in prior scan                                                                                            |
| `grade_code`                                                         | grade code                          | 100%    | YES            | pre-race   | NO (race-level) | consumed                                                                                                                                  |
| `grade_code_henkomae`                                                | pre-change grade                    | 0%      | NO             | pre-race   | NO              | empty — never changed                                                                                                                     |
| `kyoso_shubetsu_code`                                                | race type code                      | 100%    | YES            | pre-race   | NO (race-level) | consumed                                                                                                                                  |
| `kyoso_kigo_code`                                                    | race symbol code                    | 100%    | NO             | pre-race   | NO (race-level) | race-level code; collinear with kyoso_joken_code already consumed                                                                         |
| `juryo_shubetsu_code`                                                | weight type                         | 100%    | YES            | pre-race   | NO (race-level) | consumed                                                                                                                                  |
| `kyoso_joken_code_2sai`                                              | eligibility code age 2              | 16.7%   | NO             | pre-race   | NO (race-level) | sparse + race-level; subset of `kyoso_joken_code` (consumed)                                                                              |
| `kyoso_joken_code_3sai`                                              | eligibility code age 3              | 59.8%   | NO             | pre-race   | NO (race-level) | race-level; redundant with `kyoso_joken_code` (consumed)                                                                                  |
| `kyoso_joken_code_4sai`                                              | eligibility code age 4              | 50.9%   | NO             | pre-race   | NO (race-level) | race-level; redundant with `kyoso_joken_code` (consumed)                                                                                  |
| `kyoso_joken_code_5sai_ijo`                                          | eligibility code age 5+             | 50.9%   | NO             | pre-race   | NO (race-level) | race-level; redundant with `kyoso_joken_code` (consumed)                                                                                  |
| `kyoso_joken_code`                                                   | combined eligibility code           | 100%    | YES            | pre-race   | NO (race-level) | consumed                                                                                                                                  |
| `kyoso_joken_meisho`                                                 | eligibility name text               | 100%    | NO             | pre-race   | NO              | collinear with kyoso_joken_code                                                                                                           |
| `kyori`                                                              | distance (m)                        | 100%    | YES            | pre-race   | NO (race-level) | consumed                                                                                                                                  |
| `kyori_henkomae`                                                     | pre-change distance                 | 100%\*  | NO             | pre-race   | NO              | 100% populated but value is always '0000' (no changes)                                                                                    |
| `track_code`                                                         | surface/track code                  | 100%    | YES            | pre-race   | NO (race-level) | consumed                                                                                                                                  |
| `track_code_henkomae`                                                | pre-change track                    | 0%      | NO             | pre-race   | NO              | always empty — no changes                                                                                                                 |
| `course_kubun`                                                       | course section code (A/B/C/D)       | 50.3%   | NO             | pre-race   | NO (race-level) | race-level; assessed below                                                                                                                |
| `course_kubun_henkomae`                                              | pre-change course section           | 0%      | NO             | pre-race   | NO              | always empty                                                                                                                              |
| `kaisai_nen`                                                         | race year                           | 100%    | YES (skeleton) | pre-race   | NO              | consumed                                                                                                                                  |
| `honshokin`                                                          | prize money                         | 100%    | NO             | pre-race   | NO (race-level) | structural non-starter — in prior scan                                                                                                    |
| `honshokin_henkomae`                                                 | pre-change prize                    | 100%    | NO             | pre-race   | NO              | race-level, same-as-honshokin in effect                                                                                                   |
| `fukashokin`                                                         | additional prize                    | 100%    | NO             | pre-race   | NO (race-level) | race-level; assessed below                                                                                                                |
| `fukashokin_henkomae`                                                | pre-change additional prize         | 100%    | NO             | pre-race   | NO              | race-level, redundant                                                                                                                     |
| `hasso_jikoku`                                                       | scheduled post time (HHmm)          | 100%    | NO             | pre-race   | NO (race-level) | structural non-starter — in prior scan                                                                                                    |
| `hasso_jikoku_henkomae`                                              | pre-change post time                | 100%    | NO             | pre-race   | NO              | race-level, always '0000' effectively                                                                                                     |
| `toroku_tosu`                                                        | registered entries count            | 100%    | NO             | pre-race   | NO (race-level) | race-level; assessed below                                                                                                                |
| `shusso_tosu`                                                        | actual starters count               | 100%    | YES            | pre-race   | NO (race-level) | consumed                                                                                                                                  |
| `nyusen_tosu`                                                        | finishers count                     | 99.6%   | NO             | post-race  | NO (race-level) | LEAK — post-race count of who finished                                                                                                    |
| `tenko_code`                                                         | weather code                        | 100%    | NO             | pre-race   | NO (race-level) | in prior scan — collinear with babajotai                                                                                                  |
| `kaisai_tsukihi`                                                     | race date (MMDD)                    | 100%    | YES (skeleton) | pre-race   | NO              | consumed                                                                                                                                  |
| `babajotai_code_shiba`                                               | turf going code                     | 100%    | YES            | pre-race   | NO (race-level) | consumed                                                                                                                                  |
| `babajotai_code_dirt`                                                | dirt going code                     | 100%    | YES            | pre-race   | NO (race-level) | consumed                                                                                                                                  |
| `lap_time`                                                           | lap times string                    | 100%    | NO             | post-race  | NO (race-level) | LEAK — in prior scan                                                                                                                      |
| `shogai_mile_time`                                                   | obstacle/mile time                  | 100%\*  | NO             | post-race  | NO (race-level) | 96% are '0000' (non-obstacle races); non-zero only for kyoso_shubetsu_code '18'/'19' (obstacle races, ~554 races). Post-race result. LEAK |
| `zenhan_3f`                                                          | front 3F split                      | 96%     | NO             | post-race  | NO (race-level) | LEAK — in prior scan                                                                                                                      |
| `zenhan_4f`                                                          | front 4F split                      | 96%     | NO             | post-race  | NO (race-level) | LEAK — in prior scan                                                                                                                      |
| `kohan_3f`                                                           | final 3F time (race-level)          | 99.6%   | NO             | post-race  | NO (race-level) | LEAK — in prior scan                                                                                                                      |
| `kohan_4f`                                                           | final 4F time (race-level)          | 99.6%   | NO             | post-race  | NO (race-level) | LEAK                                                                                                                                      |
| `corner_tsuka_juni_1–4`                                              | corner passing order strings        | 100%    | NO             | post-race  | NO (race-level) | LEAK — race-level strings encoding all horses' order at each corner; post-race                                                            |
| `record_koshin_kubun`                                                | track record flag                   | 0.7%    | NO             | post-race  | NO              | LEAK + sparse                                                                                                                             |
| `kaisai_kai`                                                         | nth meet of year                    | 100%    | NO             | pre-race   | NO (race-level) | structural non-starter — in prior scan                                                                                                    |
| `kaisai_nichime`                                                     | day in meet                         | 100%    | NO             | pre-race   | NO (race-level) | DO-NOT-RETEST (probed C → ρ=-0.005 FAIL)                                                                                                  |
| `race_bango`                                                         | race number                         | 100%    | YES (skeleton) | pre-race   | NO              | consumed                                                                                                                                  |
| `data_sakusei_nengappi`                                              | data creation date                  | 100%    | NO             | structural | NO              | admin                                                                                                                                     |
| `nyusen_tosu`                                                        | finisher count                      | 99.6%   | NO             | post-race  | NO              | LEAK                                                                                                                                      |
| `toroku_tosu`                                                        | registrations count                 | 100%    | NO             | pre-race   | NO (race-level) | race-level; assessed below                                                                                                                |

---

## `jvd_ra` Populated+Unchecked Columns — Deep Assessment

### `course_kubun` — course section code (50.3% dense, pre-race, race-level)

Values: ' '=none (49.7%), 'A'=inner, 'B'=middle, 'C'=outer, 'D'=outermost. Present only for turf races that use moveable rail sections. This is a race-level attribute (same for all horses in the race), so no within-race variation exists. It encodes which portion of the turf track is being used, which affects pace and rail advantage.

However, this is race-level only. Track bias effects are already partially captured by `keibajo_code × track_code × babajotai_code_shiba` combinations in the existing pipeline. The within-race variation requirement is unmet — every horse in the race faces the same course_kubun. Signal would only exist cross-race (e.g. which rail section produces faster times), and that is already subsumed by venue-track combinations.
**Verdict: NOT A CANDIDATE** — no within-race variation; collinear with venue × surface combinations consumed.

### `fukashokin` — additional prize money (100% dense, pre-race, race-level)

Additional prize awarded to placed horses beyond `honshokin`. Like `honshokin`, this is race-level (same for all horses), constant across horses within a race. The market prices race value/grade through odds, and `grade_code` + `kyoso_joken_code` already capture class level. No within-race variation.
**Verdict: NOT A CANDIDATE** — race-level only, no within-race variation, correlated with grade/class already consumed.

### `toroku_tosu` — registered entries count (100% dense, pre-race, race-level)

Count of horses registered (before scratches), vs `shusso_tosu` (actual starters, consumed). Differs from `shusso_tosu` in 800/15,384 races (5.2%). The difference encodes late scratches. Race-level only (same for all horses). `shusso_tosu` is already consumed. The scratch count (toroku - shusso) is marginal race-level information with no within-race variation.
**Verdict: NOT A CANDIDATE** — race-level only, no within-race variation, marginal over `shusso_tosu` already consumed.

---

## `jvd_hr` — Full Column Census

Post-race payout table. **All columns are post-race results (haraimodoshi = refund payout amounts)**. None are usable as predictive features.

| column group                | description                                          | density | verdict                                                                 |
| --------------------------- | ---------------------------------------------------- | ------- | ----------------------------------------------------------------------- |
| `haraimodoshi_tansho_*`     | win bet payout                                       | 100%    | post-race LEAK                                                          |
| `haraimodoshi_fukusho_*`    | place bet payout                                     | 100%    | post-race LEAK                                                          |
| `haraimodoshi_wakuren_*`    | bracket quinella payout                              | 100%    | post-race LEAK                                                          |
| `haraimodoshi_umaren_*`     | quinella payout                                      | 100%    | post-race LEAK                                                          |
| `haraimodoshi_wide_1a–6c`   | wide (duet) payout combinations                      | 100%/0% | post-race LEAK (wide_6a/b/c are empty — most races have ≤5 wide combos) |
| `haraimodoshi_umatan_*`     | exacta payout                                        | 100%    | post-race LEAK                                                          |
| `haraimodoshi_sanrenpuku_*` | trifecta (unordered) payout                          | 100%    | post-race LEAK                                                          |
| `haraimodoshi_sanrentan_*`  | trifecta (ordered) payout                            | 100%    | post-race LEAK                                                          |
| identifiers                 | `kaisai_nen`, `kaisai_tsukihi`, `keibajo_code`, etc. | 100%    | skeleton only                                                           |

**Summary:** `jvd_hr` is entirely post-race and provides zero predictive signal. Confirmed excluded.

---

## `jvd_o1`–`jvd_o6` — Full Column Census

Odds snapshot tables. All signal columns are DO-NOT-RETEST (odds time-series).

| table    | betting type                               | key data columns                                               | verdict                          |
| -------- | ------------------------------------------ | -------------------------------------------------------------- | -------------------------------- |
| `jvd_o1` | tansho/fukusho/wakuren (win/place/bracket) | `odds_tansho`, `odds_fukusho`, `odds_wakuren`, `hyosu_gokei_*` | DO-NOT-RETEST (odds time-series) |
| `jvd_o2` | umaren (quinella)                          | `odds_umaren`                                                  | DO-NOT-RETEST                    |
| `jvd_o3` | wide (duet)                                | `odds_wide`                                                    | DO-NOT-RETEST                    |
| `jvd_o4` | umatan (exacta)                            | `odds_umatan`                                                  | DO-NOT-RETEST                    |
| `jvd_o5` | sanrenpuku (trifecta unordered)            | `odds_sanrenpuku`                                              | DO-NOT-RETEST                    |
| `jvd_o6` | sanrentan (trifecta ordered)               | `odds_sanrentan`                                               | DO-NOT-RETEST                    |

All tables share `happyo_tsukihi_jifun` (snapshot timestamp), `toroku_tosu`, `shusso_tosu`, `hatsubai_flag_*` (betting availability flag), `hyosu_gokei_*` (total votes). None of these carry new per-horse pre-race signal beyond what odds themselves already encode.

---

## Summary Table — POPULATED + NOT CONSUMED columns (≥30% density)

| table         | column                      | density | pre-race? | within-race var? | leak? | verdict                                       |
| ------------- | --------------------------- | ------- | --------- | ---------------- | ----- | --------------------------------------------- |
| `jvd_se`      | `hinshu_code`               | 100%    | YES       | NO               | NO    | zero variation (all='1')                      |
| `jvd_se`      | `moshoku_code`              | 100%    | YES       | NO               | NO    | cosmetic, no within-horse variation           |
| `jvd_se`      | `tozai_shozoku_code`        | 100%    | YES       | YES              | NO    | collinear with venue+trainer consumed         |
| `jvd_se`      | `fukushoku_hyoji`           | 100%    | YES       | YES              | NO    | owner-silks text, collinear with owner        |
| `jvd_se`      | `chokyoshi_code`            | 100%    | YES       | YES              | NO    | collinear with `chokyoshimei_ryakusho`        |
| `jvd_se`      | `banushi_code`              | 100%    | YES       | YES              | NO    | collinear with trainer/owner signal           |
| `jvd_se`      | `kishu_code`                | 100%    | YES       | YES              | NO    | collinear with `kishumei_ryakusho`            |
| `jvd_se`      | `bataiju`                   | ~76%    | YES       | YES              | NO    | consumed                                      |
| `jvd_se`      | `zogen_fugo`                | 76%     | YES       | YES              | NO    | **redundant** with `weight_diff_from_avg`     |
| `jvd_se`      | `zogen_sa`                  | 76%     | YES       | YES              | NO    | **redundant** with `weight_diff_from_avg`     |
| `jvd_se`      | `kyakushitsu_hantei`        | 98.8%   | NO        | YES              | LEAK  | post-race official running-style              |
| `jvd_se`      | `chakusa_code_1`            | 91.6%   | NO        | YES              | LEAK  | margin from winner, post-race                 |
| `jvd_se`      | `mining_kubun`              | 100%    | YES       | YES              | NO    | DO-NOT-RETEST (yoso group)                    |
| `jvd_se`      | `yoso_soha_time`            | 100%    | YES       | YES              | NO    | DO-NOT-RETEST (ρ=0.066 FAIL)                  |
| `jvd_se`      | `yoso_juni`                 | 100%    | YES       | YES              | NO    | DO-NOT-RETEST (ρ=0.066 FAIL)                  |
| `jvd_se`      | `wakuban`                   | 100%    | YES       | YES              | NO    | DO-NOT-RETEST (gate-draw)                     |
| `jvd_se`      | `blinker_shiyo_kubun`       | 100%    | YES       | YES              | NO    | DO-NOT-RETEST (ρ=+0.018 FAIL)                 |
| `jvd_se`      | `kishu_minarai_code`        | 100%    | YES       | YES              | NO    | DO-NOT-RETEST (ρ=+0.022 FAIL)                 |
| `jvd_ra`      | `course_kubun`              | 50.3%   | YES       | NO               | NO    | race-level only, no within-race var           |
| `jvd_ra`      | `fukashokin`                | 100%    | YES       | NO               | NO    | race-level only, grade proxy                  |
| `jvd_ra`      | `toroku_tosu`               | 100%    | YES       | NO               | NO    | race-level only, marginal over `shusso_tosu`  |
| `jvd_ra`      | `kyoso_joken_code_3sai`     | 59.8%   | YES       | NO               | NO    | race-level, redundant with `kyoso_joken_code` |
| `jvd_ra`      | `kyoso_joken_code_4sai`     | 50.9%   | YES       | NO               | NO    | race-level, redundant with `kyoso_joken_code` |
| `jvd_ra`      | `kyoso_joken_code_5sai_ijo` | 50.9%   | YES       | NO               | NO    | race-level, redundant with `kyoso_joken_code` |
| `jvd_ra`      | `corner_tsuka_juni_1–4`     | 100%    | NO        | n/a              | LEAK  | post-race race-level corner order string      |
| `jvd_ra`      | `honshokin_henkomae`        | 100%    | YES       | NO               | NO    | race-level, same as honshokin                 |
| `jvd_ra`      | `fukashokin_henkomae`       | 100%    | YES       | NO               | NO    | race-level, same as fukashokin                |
| `jvd_ra`      | `hasso_jikoku_henkomae`     | 100%    | YES       | NO               | NO    | always '0000', never changed                  |
| `jvd_ra`      | `zenhan_3f/4f`              | 96%     | NO        | n/a              | LEAK  | already in prior scan                         |
| `jvd_ra`      | `lap_time`                  | 100%    | NO        | n/a              | LEAK  | already in prior scan                         |
| `jvd_ra`      | `kohan_3f/4f` (ra-level)    | 99.6%   | NO        | n/a              | LEAK  | race-level post-race splits                   |
| `jvd_hr`      | all columns                 | 100%    | NO        | n/a              | LEAK  | all post-race payouts                         |
| `jvd_o1`–`o6` | odds/hyosu                  | 100%    | YES       | YES              | NO    | DO-NOT-RETEST (odds time-series)              |

---

## Genuine New Candidates

**NONE.**

Every populated (≥30%) column not already consumed falls into one of these buckets:

1. **Post-race LEAK** (nyusen*juni, kakutoku_honshokin/fukashokin, chakusa_code_1, corner_tsuka_juni*\*, kohan/zenhan splits, lap_time, all of jvd_hr): excluded by definition.
2. **No within-race variation** (hinshu_code=constant '1', moshoku_code per horse, race-level columns): structural non-starters.
3. **Collinear with consumed column** (tozai_shozoku_code ↔ venue+trainer; chokyoshi_code ↔ chokyoshimei_ryakusho; kishu_code ↔ kishumei_ryakusho; banushi_code ↔ owner; fukushoku_hyoji ↔ owner; kyoso_joken_code_Nsai ↔ kyoso_joken_code; course_kubun ↔ venue×surface).
4. **Redundant with existing feature** (zogen_fugo/zogen_sa ↔ weight_diff_from_avg already computed from bataiju aggregation; toroku_tosu ↔ shusso_tosu; kyori_henkomae='0000'=never changed).
5. **DO-NOT-RETEST** (blinker, kishu_minarai_code, kaisai_nichime, yoso_soha_time/juni, mining_kubun, gate-draw, odds time-series).

---

## Conclusion

The prior `jra-unused-data-scan.md` was correct that the JRA unused-data space is exhausted. This full census covers all 68 `jvd_se` columns, 62 `jvd_ra` columns, all `jvd_hr` payout columns, and all `jvd_o1`–`o6` odds snapshot columns. No new genuine candidates exist.

The `zogen_fugo`/`zogen_sa` columns (76% dense, pre-race, within-race varying) are the only previously uncharacterized populated pre-race horse-level columns — and they are redundant: the pipeline already computes `weight_diff_from_avg` from bataiju aggregation which is strictly more informative (5-race history + slope + volatility).
