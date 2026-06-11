---
feas_entry: true
domain: timing-data
date: 2026-06-11
status: COMPLETE
verdict_summary: Per-horse furlong splits DO NOT EXIST at source (JV-Data/NVD). Two GO candidates available in PG with zero ingestion needed.
---

# Timing Data Feasibility — 2026-06-11

## Context

The finish-position model levers are saturated. The B4 speed-fade probe (H-SPEED-FADE-INDEX)
ABORTed with partial ρ = 0.0725 (bar 0.08) because the only available per-horse timing is
`kohan_3f`; `zenhan_3f` is race-level (RA), not per-horse (SE). This investigation determines
whether richer per-horse timing can be acquired.

---

## 1. PG Deep Schema Audit

### `jvd_se` / `nvd_se` — 馬毎レース情報 (per-horse, 70 columns each)

All timing columns that exist per horse:

| column               | logical name       | granularity                  | already used as feature?                                         |
| -------------------- | ------------------ | ---------------------------- | ---------------------------------------------------------------- |
| `corner_1`           | 1コーナー通過順位  | per-horse ORDER (rank 01–18) | YES — as `corner1_norm`                                          |
| `corner_2`           | 2コーナー通過順位  | per-horse ORDER              | PARTIAL — `corner2_norm` computed but not in finish-pos features |
| `corner_3`           | 3コーナー通過順位  | per-horse ORDER              | YES — as `corner3_norm`                                          |
| `corner_4`           | 4コーナー通過順位  | per-horse ORDER              | YES — as `corner4_norm`                                          |
| `kohan_3f`           | 後3ハロンタイム    | per-horse time (0.1s units)  | YES — as `kohan3f_avg_5`                                         |
| `kohan_4f`           | 後4ハロンタイム    | per-horse time (0.1s units)  | NO — in PG, never extracted                                      |
| `soha_time`          | 走破タイム         | per-horse total race time    | YES (via `time_sa` difference)                                   |
| `time_sa`            | タイム差           | per-horse gap to winner      | YES — as `speed_index_avg_5`                                     |
| `kyakushitsu_hantei` | 今回レース脚質判定 | per-horse JV-Data label      | PARTIAL — used in stacking metalearner only                      |

**Confirmed absent**: there are NO per-horse per-furlong split columns in either `jvd_se` or
`nvd_se`. The schema has 70 columns; timing stops at 後3F + 後4F aggregates.

### `jvd_ra` / `nvd_ra` — レース詳細 (race-level, 62 columns each)

| column                  | logical name        | granularity                                    | already used?                                     |
| ----------------------- | ------------------- | ---------------------------------------------- | ------------------------------------------------- |
| `lap_time`              | ラップタイム        | RACE-LEVEL packed string (char 75, lead horse) | NO (only zenhan_3f/kohan_3f used)                 |
| `zenhan_3f`             | 前3ハロン           | RACE-LEVEL (lead horse pace)                   | YES — in speed-fade probe; partial ρ 0.0725       |
| `zenhan_4f`             | 前4ハロン           | RACE-LEVEL (lead horse pace)                   | NO                                                |
| `kohan_3f`              | 後3ハロン           | RACE-LEVEL (lead horse pace)                   | YES (used in speed-fade probe)                    |
| `kohan_4f`              | 後4ハロン           | RACE-LEVEL (lead horse pace)                   | NO                                                |
| `corner_tsuka_juni_1-4` | コーナー通過順位1-4 | RACE-LEVEL packed string (all horses, char 72) | NO — individual orders extracted from jvd_se only |

**Key insight**: `lap_time` in RA is a 75-char packed string of race-level (lead horse) furlong
splits, e.g. 5 chars per 200m segment. It is NOT per horse. Only 7 of the ~10–15 furlong slots
are populated depending on race distance.

### `jvd_hc` — 坂路調教 (per-horse TRAINING data)

- 11.75M rows, 23 years, JRA only (~100% JRA coverage, ~57% NAR, ~0% ban-ei)
- Per-horse per-session lap_time_1f through lap_time_4f (200m segments during hillwork training)
- Already partially integrated into `add-workout-features.py`
- Not a race-timing signal; captures training fitness, not race pace

---

## 2. Source Spec Verdict — Do Per-Horse Furlong Splits Exist?

**DEFINITIVE ANSWER: NO. Per-horse per-furlong split times do not exist in the JV-Data (JRA)
or NVD (NAR/地方競馬) source feeds at any historical depth.**

Evidence chain:

1. **JRA official terminology** (https://www.jra.go.jp/kouza/yougo/w311.html): ハロンタイムは
   先頭の馬が基準 — the lead horse defines the furlong time. No individual horse measurement.

2. **Yahoo知恵袋 practitioner Q&A**
   (https://detail.chiebukuro.yahoo.co.jp/qa/question_detail/q12290844137): expert respondent:
   「各馬のラップタイムは公式な計測はされていません」. Individual horse lap times are not
   officially measured. Practitioners calculate per-horse splits manually from position gaps
   (1 horse-length ≈ 0.2s). This is estimation, not measurement.

3. **JRA-VAN TARGET frontier Ave-3F FAQ**
   (https://targetfaq.jra-van.jp/faq/detail?site=SVKNEGBV&id=600): explicitly states
   「JRA-VANデータにはテンの3ハロンのタイムデータはない」. Even the front-pace split (zenhan_3f)
   is absent from SE; it only exists at race level (RA). Ave-3F in TARGET is a derived
   approximation formula, not source data.

4. **PG schema** (pc-keiba-postgresql-reference.md, generated 2026-05-06 from actual DB):
   jvd_se (70 cols) contains `corner_1-4` (ORDER only), `kohan_3f`, `kohan_4f`, `soha_time`,
   `time_sa`. No per-horse per-furlong fields anywhere.

5. **Error log confirmation** (tmp/running-style-20260524-v2.log): `column se.zenhan_3f does not
exist` — zenhan_3f is RA-level, not SE-level. Directly confirms the warehouse mirrors the
   source.

6. **Kochi GPS exception**: Kochi Keiba (2022+) began per-horse GPS sectional display on live
   YouTube only (https://kochikeiba-memo.site/16348-2/23312-2). Not archived, not in any data
   feed, 1 of ~15 NAR venues, post-2022 only. Not ingestion-viable for a 21-year training set.

**Conclusion**: the sectional avenue is SOURCE-BLOCKED, not just warehouse-blocked. Acquiring
per-horse furlong splits would require either (a) video-based manual timing (not scalable to
millions of races) or (b) waiting for GPS infrastructure to be deployed and data released —
not a viable path for production model improvement in the current cycle.

---

## 3. Acquirable Per-Horse Timing Signals (Ranked)

### Rank 1 — CORNER_POSITION_GAIN (GO, zero ingestion effort)

**Signal**: Per-horse corner position trajectory = `corner4_norm - corner1_norm` (and intermediate
deltas: corner3_norm - corner1_norm). Measures how many positions a horse gained or lost through
the race.

- **Source**: `jvd_se.corner_1-4`, `nvd_se.corner_1-4` — already in PG, already extracted into
  `race_entry_corner_features` as `corner1_norm`, `corner2_norm`, `corner3_norm`, `corner4_norm`
- **Already used**: `corner1_norm`, `corner3_norm`, `corner4_norm` — but the DELTA between them
  is NOT a feature. Historical averages of deltas are also not features.
- **Within-race-varying**: YES (each horse has a unique trajectory)
- **Historical depth**: 21+ years (JRA and NAR), training-usable
- **Coverage**: JRA high, NAR medium-high, ban-ei low (2-corner only)
- **Ingestion effort**: NONE — data is already in `race_entry_corner_features`; just add derived
  columns in `finish_position_features_duckdb.py`
- **Orthogonality**: `corner1_norm` level is already used; the CHANGE (trajectory) is not. A horse
  that runs 4th at corner 1 and 1st at corner 4 has a very different stamina profile from one that
  runs 1st then 4th. The trajectory delta is logically orthogonal to the level.
- **Verdict: GO — highest priority, zero cost**

### Rank 2 — KOHAN_4F_UNUSED (GO, low ingestion effort)

**Signal**: Per-horse `kohan_4f` (final 4F time) is in both `jvd_se` and `nvd_se` but has never
been extracted into the feature parquet. The delta `kohan_4f - kohan_3f` gives the per-horse
time for the single furlong immediately before the final 3F sprint — a proxy for how early the
horse began its final run.

- **Source**: `jvd_se.kohan_4f`, `nvd_se.kohan_4f` — in PG
- **Historical depth**: 21+ years (same as kohan_3f)
- **Coverage**: high for both JRA and NAR
- **Ingestion effort**: LOW — add `kohan_4f` to `build-corner-feature-table.ts` SQL extraction
  and propagate through feature pipeline. Mirrors existing `kohan_3f` handling exactly.
- **Orthogonality**: `kohan_3f` is already a feature; `kohan_4f - kohan_3f` is the 4th-from-last
  furlong time, never used. Captures whether a horse's finishing burst started early or late.
- **Verdict: GO — low effort, genuine new signal**

### Rank 3 — KYAKUSHITSU_HANTEI_HISTORY (CONDITIONAL-GO, low effort)

**Signal**: `jvd_se.kyakushitsu_hantei` / `nvd_se.kyakushitsu_hantei` is a 1-char JV-Data editorial
post-race running style label (今回レース脚質判定). A historical aggregation over last N races
gives the JV-Data-assigned running style distribution.

- **In PG**: YES. Used only in stacking metalearner (JRA-only), not in main LGBM finish-pos features.
- **Orthogonality concern**: corner1_norm already captures running style from raw data.
  kyakushitsu_hantei is an independent editorial source. Overlap likely ~80%; marginal signal
  uncertain. Warrants a sci-track probe before committing pipeline work.
- **NAR coverage**: NAR provides this field but coverage may be lower than JRA.
- **Verdict: CONDITIONAL-GO — sci-track probe recommended first**

### Rank 4 — RA_LAP_TIME_RACE_LEVEL_PACE (CONDITIONAL-GO, medium effort)

**Signal**: Full parse of `jvd_ra.lap_time` / `nvd_ra.lap_time` packed string into individual
furlong splits (race-level, lead horse). The speed-fade probe (B4) used only zenhan_3f/kohan_3f;
the full lap_time string enables pace shape features for furlongs 4-6 of a 1600-2000m race
(currently completely unused territory). These race-level features can be joined to per-horse
corner positions to create interaction features (e.g., horse-position × mid-race-pace-deceleration).

- **Effort**: MEDIUM — requires parsing the 75-char packed string (format: up to 15 × 5-char
  segments per race distance) and adding to pipeline. Not trivial but well-defined.
- **Coverage NAR**: patchy — zenhan_3f already has 100% NAR coverage but full lap_time
  per-furlong fill rate across all NAR venues is unknown.
- **Verdict: CONDITIONAL-GO — after Rank 1 and 2 are deployed**

### Rank 5 — CORNER_TSUKA_JUNI_DENSITY (CONDITIONAL-GO, medium effort)

**Signal**: `jvd_ra.corner_tsuka_juni_1-4` / `nvd_ra.corner_tsuka_juni_1-4` are race-level
72-char packed strings encoding the FULL order of all horses at each corner (including
group/bunching markers). Individual per-horse orders are already extracted from jvd_se; the
packed RA string adds the RACE-LEVEL density/bunching dimension: how many horses were in the
front group, spread from 1st to last, etc.

- **Effort**: MEDIUM-HIGH — format spec for the 72-char packed string requires careful research
  (group separators, sequential positions, etc.)
- **Note**: Several race-internal features (field_nige_candidate_count, field_spread) already
  derived from past-race stats. Current-race bunching is different but may have limited
  pre-race predictive value (it's a result, not a pre-race input).
- **Verdict: CONDITIONAL-GO — lower priority than Ranks 1-4**

### Rank 6 — PER_HORSE_FURLONG_SPLITS (NO-GO, source-blocked)

Per-horse per-furlong timing does not exist in the JV-Data or NVD source feeds. The avenue is
source-blocked. See Section 2 for full evidence.

---

## 4. Candidate Summary Table

| Rank | ID                          | Verdict     | In PG? | Ingestion Effort | Novel Signal               | Coverage             |
| ---- | --------------------------- | ----------- | ------ | ---------------- | -------------------------- | -------------------- |
| 1    | CORNER_POSITION_GAIN        | GO          | YES    | NONE             | trajectory (Δcorner norm)  | JRA+NAR 21yr         |
| 2    | KOHAN_4F_UNUSED             | GO          | YES    | LOW              | per-horse 4F final, ratio  | JRA+NAR 21yr         |
| 3    | KYAKUSHITSU_HANTEI_HISTORY  | CONDITIONAL | YES    | LOW              | JV editorial style history | JRA high, NAR medium |
| 4    | RA_LAP_TIME_RACE_LEVEL_PACE | CONDITIONAL | YES    | MEDIUM           | mid-race pace shape        | JRA+NAR 21yr         |
| 5    | CORNER_TSUKA_JUNI_DENSITY   | CONDITIONAL | YES    | MEDIUM           | current-race bunching      | JRA+NAR 21yr         |
| 6    | PER_HORSE_FURLONG_SPLITS    | NO-GO       | NO     | INFEASIBLE       | source does not exist      | —                    |

---

## 5. Recommended Next Actions

1. **Immediate**: Add `corner4_norm - corner1_norm` (and corner3_norm - corner1_norm) as derived
   features in `finish_position_features_duckdb.py` — zero new data needed.
2. **Short-term**: Extract `kohan_4f` into `race_entry_corner_features` and add to feature
   pipeline (mirrors existing kohan_3f path).
3. **Sci-track probe**: Run a partial-ρ probe on kyakushitsu_hantei history before committing
   pipeline work.
4. **Do not pursue**: per-horse furlong splits — source-blocked globally for JRA/NAR.

---

## Sources

- JRA Terminology: https://www.jra.go.jp/kouza/yougo/w311.html
- Yahoo知恵袋 (per-horse lap time Q&A): https://detail.chiebukuro.yahoo.co.jp/qa/question_detail/q12290844137
- JRA-VAN TARGET Ave-3F FAQ: https://targetfaq.jra-van.jp/faq/detail?site=SVKNEGBV&id=600
- JRA-VAN SDK (JV-Data spec): https://developer.jra-van.jp/t/topic/45
- Kochi GPS sectional: https://kochikeiba-memo.site/16348-2/23312-2
- PG schema reference: apps/local-postgresql/docs/pc-keiba-postgresql-reference.md
- Corner feature table DDL: apps/pc-keiba-viewer/src/scripts/build-corner-feature-table.ts
- Speed-fade probe: docs/finish-position-accuracy/history/sci-track-2026-06-11-h-speed-fade-index.md
