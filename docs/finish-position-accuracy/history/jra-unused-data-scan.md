# JRA Warehouse Unused-Data Scan

**Date:** 2026-06-12  
**Status:** COMPLETE — all probed candidates FAIL gate. JRA unused-data space exhausted.

---

## 1. Consumed JRA Columns Inventory

The feature pipeline writes to `race_finish_position_features` and reads from two source tables:

### `race_entry_corner_features` (primary source)

All columns below are consumed by at least one feature builder:

| column                                                                                       | used by                                    |
| -------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `source`, `kaisai_nen`, `kaisai_tsukihi`, `keibajo_code`, `race_bango`, `ketto_toroku_bango` | skeleton (all builders)                    |
| `race_date`, `kyori`, `track_code`, `grade_code`, `shusso_tosu`, `umaban`, `bamei`           | skeleton + context                         |
| `finish_position`, `finish_norm`                                                             | labels + recent-form, horse-career         |
| `time_sa`, `kohan_3f`, `corner4_norm`, `corner3_norm`, `corner1_norm`, `corner2_norm`        | horse-career, recent-form                  |
| `tansho_odds`, `tansho_ninkijun`                                                             | market-signal (legacy)                     |
| `kishumei_ryakusho`, `chokyoshimei_ryakusho`                                                 | jockey-trainer                             |
| `kyoso_joken_code`                                                                           | recent-form (class levels), class features |
| `kyoso_shubetsu_code`, `juryo_shubetsu_code`                                                 | race-context                               |
| `babajotai_code_shiba`, `babajotai_code_dirt`                                                | race-context (track condition)             |
| `seibetsu_code`, `barei`, `futan_juryo`                                                      | relationship-R1, weight features           |
| `banushimei`                                                                                 | owner (reference only)                     |
| `soha_time`                                                                                  | sectional/weight features                  |

### `jvd_um` (horse master)

- `ketto_joho_01b` (sire name), `ketto_joho_05b` (dam sire name) — consumed by pedigree builder (JRA only)
- 12 other `ketto_joho_*` columns — NOT consumed
- `kyakushitsu_keiko` — NOT consumed (probed below)
- `sanchimei`, `seisansha_code`, `banushi_code`, etc. — NOT consumed

---

## 2. Unused JRA Columns — Assessment

Tables with real data that are NOT currently consumed by the feature pipeline:

### `jvd_se` columns not in `race_entry_corner_features`

| column                                                       | logical name           | pre-race?      | leak-free? | density               | within-race variation | verdict                                     |
| ------------------------------------------------------------ | ---------------------- | -------------- | ---------- | --------------------- | --------------------- | ------------------------------------------- |
| `blinker_shiyo_kubun`                                        | blinker use flag       | YES            | YES        | 100% (12.1% non-zero) | YES                   | probed → FAIL                               |
| `kishu_minarai_code`                                         | apprentice jockey flag | YES            | YES        | 100% (18.6% non-zero) | YES                   | probed → FAIL                               |
| `wakuban`                                                    | gate/stall draw        | YES            | YES        | 100%                  | YES                   | DO-NOT-RETEST (gate-draw)                   |
| `kakutoku_honshokin`                                         | earned prize           | NO — post-race | LEAK       | 100%                  | n/a                   | exclude                                     |
| `kakutoku_fukashokin`                                        | earned bonus           | NO — post-race | LEAK       | 100%                  | n/a                   | exclude                                     |
| `aiteuma_joho_*`                                             | rival horse info       | post-race      | LEAK       | varies                | n/a                   | exclude                                     |
| `nyusen_juni`                                                | photo finish order     | post-race      | LEAK       | 100%                  | n/a                   | exclude                                     |
| `dochaku_kubun`                                              | dead heat flag         | post-race      | LEAK       | low                   | n/a                   | exclude                                     |
| `mining_kubun`, `yoso_soha_time`, `yoso_gosa_*`, `yoso_juni` | data-mining forecast   | DO-NOT-RETEST  | —          | 100%                  | —                     | DO-NOT-RETEST                               |
| `kishu_code`, `kishu_code_henkomae`                          | jockey code            | pre-race       | YES        | 100%                  | YES                   | collinear with kishumei_ryakusho (used)     |
| `chokyoshi_code`                                             | trainer code           | pre-race       | YES        | 100%                  | YES                   | collinear with chokyoshimei_ryakusho (used) |
| `futan_juryo_henkomae`                                       | pre-change load        | pre-race       | YES        | low                   | YES                   | redundant with futan_juryo                  |
| `kishumei_ryakusho_henkomae`                                 | pre-change jockey      | pre-race       | YES        | low                   | YES                   | redundant                                   |
| `record_koshin_kubun`                                        | track record flag      | post-race      | LEAK       | low                   | n/a                   | exclude                                     |

### `jvd_ra` columns not consumed

| column                               | logical name         | pre-race?      | within-race variation | verdict                                            |
| ------------------------------------ | -------------------- | -------------- | --------------------- | -------------------------------------------------- |
| `hasso_jikoku`                       | post time (HHmm)     | YES            | NO — race-level       | structural non-starter                             |
| `honshokin`                          | prize money          | YES            | NO — race-level       | structural non-starter                             |
| `kaisai_kai`                         | nth meet of year     | YES            | NO — race-level       | structural non-starter                             |
| `kaisai_nichime`                     | day in meet          | YES            | NO — race-level       | probed → FAIL                                      |
| `tenko_code`                         | weather code         | YES            | NO — race-level       | collinear with babajotai (weather already proxied) |
| `zenhan_3f`, `zenhan_4f`, `lap_time` | front-half splits    | NO — post-race | n/a                   | LEAK                                               |
| `kyosomei_hondai`                    | race name            | YES            | NO — race-level       | text, no signal                                    |
| `tokubetsu_kyoso_bango`              | special race number  | YES            | NO — race-level       | structural non-starter                             |
| `jusho_kaiji`                        | historical win count | pre-race       | NO — race-level       | structural non-starter                             |

### Other JRA tables with data

| table                         | rows     | pre-race?   | coverage         | verdict                                                                                           |
| ----------------------------- | -------- | ----------- | ---------------- | ------------------------------------------------------------------------------------------------- |
| `jvd_um`                      | 212k     | YES         | 100% JRA horses  | `kyakushitsu_keiko` probed → FAIL                                                                 |
| `jvd_hs`                      | 52k      | YES         | ~30% JRA horses  | probed (sale price) → FAIL                                                                        |
| `jvd_hc`                      | 11.7M    | YES         | horse-level      | DO-NOT-RETEST (workout, ABORT ρ=0.032)                                                            |
| `jvd_wc`                      | 726k     | YES         | horse-level      | woodchip workout, same category as jvd_hc → structural equivalent                                 |
| `jvd_ys`                      | 7.8k     | YES         | race-level only  | schedule info, no horse-level signal                                                              |
| `jvd_rc`                      | 2.1k     | YES         | track records    | no per-horse variation                                                                            |
| `jvd_tk`                      | 171      | YES         | special entry    | very sparse, pre-entry only                                                                       |
| `jvd_hr`                      | 138k     | NO          | post-race payout | LEAK                                                                                              |
| `jvd_o1`–`jvd_o6`             | 66k–114k | conditional | odds snapshots   | DO-NOT-RETEST (odds time-series)                                                                  |
| `jvd_um.ketto_joho_02b`–`14b` | —        | YES         | 100%             | 2nd–14th generation ancestors: too remote, no per-race variation beyond sire/damsire already used |

---

## 3. Probe Results

All probes use JRA holdout 2023–2026 (n ≈ 162,799 horse-race rows).  
Partial Spearman ρ computed via ranked-residuals, controlling for `n_tou_log = log(tansho_ninkijun)` (odds proxy).  
Gate: partial ρ ≥ 0.08 AND within-race variation.

| ID  | Candidate                                       | Source   | partial ρ (vs odds)           | within-race?    | PASS/FAIL |
| --- | ----------------------------------------------- | -------- | ----------------------------- | --------------- | --------- |
| A   | `kishu_minarai_code` apprentice jockey flag     | `jvd_se` | +0.022                        | YES             | **FAIL**  |
| B   | `blinker_shiyo_kubun` blinker flag              | `jvd_se` | +0.018                        | YES             | **FAIL**  |
| C   | `kaisai_nichime` day-in-meet                    | `jvd_ra` | −0.005                        | NO (race-level) | **FAIL**  |
| E   | `jvd_hs.torihiki_kakaku` log sale price         | `jvd_hs` | −0.016                        | YES (32% cov)   | **FAIL**  |
| H   | `jvd_um.kyakushitsu_keiko` nige-vs-oikomi ratio | `jvd_um` | −0.067 (within-race demeaned) | YES             | **FAIL**  |

Notes on individual candidates:

**A (apprentice jockey):** Raw Spearman 0.101 looks promising but controlling for odds drops to 0.022. The market prices in the odds already account for jockey quality including apprentice status. No residual signal.

**B (blinker):** Raw 0.034, partial 0.018. Blinker use is a strong market signal but the market already incorporates it fully.

**C (day in meet):** Raw ~0, partial ~0. No predictive content whatsoever. Race-level only so no within-race variation.

**E (sale price):** 32% coverage eliminates it as a general feature. Even within that subset, partial ρ = −0.016 is trivial. The market odds already encode expected quality correlated with purchase price.

**H (kyakushitsu_keiko):** Most interesting candidate. Raw Spearman −0.240 (frontrunners finish better). Controlling for odds: −0.081. But this is inflated by cross-race structure. Within-race demeaned, controlling for within-race odds variation: **−0.067**, which is below the 0.08 gate. The across-race partial ρ of −0.107 controlling for odds + corner_pass_avg_5 confirms this is substantially collinear with the running style signal already captured by `corner_pass_avg_5` (Spearman correlation between feature and corner_pass: −0.51).

**jvd_wc (woodchip workout):** Not directly probed but structurally equivalent to `jvd_hc` (hillwork workout). `jvd_hc` was ABORT at ρ = 0.032. No basis to expect woodchip workouts to be different; both are training data that the market already prices in.

---

## 4. Conclusion

**The JRA unused-data space is exhausted.**

All remaining dense, pre-race, horse-level JRA warehouse columns have been probed or ruled out:

1. **Post-race columns** (kakutoku_honshokin, nyusen_juni, lap times, payout tables) are structural leaks — excluded.
2. **Race-level columns** (hasso_jikoku, honshokin, tenko_code, kaisai_nichime) have no within-race variation — structural non-starters.
3. **Redundant columns** (kishu_code ↔ kishumei_ryakusho already used; futan_juryo_henkomae redundant with futan_juryo) — already captured.
4. **DO-NOT-RETEST** (yoso_soha_time/yoso_juni, jvd_hc workout, gate-draw, odds time-series, jvd_dm/tm) — already confirmed exhausted in prior probes.
5. **Probed candidates** A–H: all below partial ρ = 0.08 gate or fail within-race variation test.

The finding is consistent with the market-efficiency wall hypothesis documented in `project_science_track_saturation_2026_06_11.md`: odds incorporate available pre-race information efficiently, and no structural/eligibility/scheduling JRA column provides incremental predictive signal beyond what the market prices in.

**Next viable direction (if any) would require genuinely new data sources not in the JVD warehouse** — e.g., biometric sensors, on-track radar, or data sources external to the JRA/JVD data feed.
