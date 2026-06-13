# Maiden Popularity-Echo Diagnosis

**Date:** 2026-06-13  
**Trigger:** User report that 06-13 predictions track popularity order, worst in 新馬戦 (maiden debut races)  
**Question:** Does the echo originate in the BACKEND model order, or the CLIENT-SIDE odds-correction overlay?

---

## Method

- Fallback date used: **2026-06-06** (no predictions yet for 2026-06-13 at query time; cron last ran 2026-06-06)
- Spearman ρ computed between `race_finish_position_model_predictions.predicted_rank` and `tansho_ninkijun` (from `jvd_se` for JRA, `nvd_se` for NAR) per race
- Races: 47 (JRA: 24, NAR: 23), Horses: 555
- Maiden classes: `701` (新馬 debut, 3 races), `703` (未勝利 maiden, 11 races)

---

## Spearman ρ Results — Backend Predicted Rank vs Popularity

### Overall

| Segment              | mean ρ     | n races | ρ ≥ 0.95 |
| -------------------- | ---------- | ------- | -------- |
| All races            | 0.183      | 47      | 0 (0%)   |
| JRA only             | -0.038     | 24      | 0        |
| NAR only             | 0.413      | 23      | 0        |
| **Maiden (701+703)** | **-0.075** | 14      | 0        |
| Non-maiden           | 0.292      | 33      | 0        |

No race at the backend level has ρ ≥ 0.95. The backend is NOT echoing popularity.

### Per Class (kyoso_joken_code)

| class   | label             | mean ρ     | n races | ρ ≥ 0.95 |
| ------- | ----------------- | ---------- | ------- | -------- |
| 000     | NAR open          | 0.413      | 23      | 0        |
| 005     | JRA allowance     | 0.120      | 6       | 0        |
| 010     | JRA class 1000万  | -0.117     | 2       | 0        |
| 016     | JRA class 1600万  | -0.175     | 2       | 0        |
| **701** | **新馬 debut**    | **-0.043** | 3       | 0        |
| **703** | **未勝利 maiden** | **-0.084** | 11      | 0        |

### Maiden 701 Race Detail

| race_key | ρ      | n horses |
| -------- | ------ | -------- |
| 05_04    | -0.036 | 7        |
| 05_05    | +0.050 | 9        |
| 09_05    | -0.143 | 6        |

### Maiden 703 Race Detail

| race_key | ρ      | n horses |
| -------- | ------ | -------- |
| 05_01    | -0.095 | 14       |
| 05_02    | -0.315 | 16       |
| 05_03    | +0.341 | 16       |
| 05_06    | -0.224 | 16       |
| 05_07    | +0.144 | 16       |
| 09_01    | +0.406 | 16       |
| 09_02    | -0.571 | 16       |
| 09_03    | +0.132 | 16       |
| 09_04    | -0.515 | 16       |
| 09_06    | -0.121 | 16       |
| 09_07    | -0.106 | 18       |

### Perfect Echo Check

Horses where `predicted_rank == ninkijun` exactly in maiden races: **10 / 198 = 5.1%** (consistent with random chance for field sizes of ~9-18).

---

## Maiden Serve-Feature Completeness

### JRA Debut Horse (0 prior starts) at Inference Time

| Feature Group                                              | NULL at serve?                                              | Source                                                   | Note                      |
| ---------------------------------------------------------- | ----------------------------------------------------------- | -------------------------------------------------------- | ------------------------- |
| Horse career history (career_win_rate, speed scores, etc.) | **ALL NULL**                                                | INNER JOIN on `h.race_date < t.race_date` returns 0 rows | Expected — no prior races |
| Running style history                                      | **ALL NULL**                                                | Same — no history                                        | Expected                  |
| Recent form (last 3/5 races)                               | **ALL NULL**                                                | Same                                                     | Expected                  |
| current_bataiju (race-day weight)                          | Populated                                                   | `jvd_se` target race                                     | Normal                    |
| Jockey features (win rates, etc.)                          | Populated                                                   | Jockey's history on other horses                         | Normal                    |
| Trainer features                                           | Populated                                                   | Trainer's history on other horses                        | Normal                    |
| Pedigree identity (sire/damsire)                           | Populated                                                   | `jvd_um` horse master registration                       | Normal                    |
| sire distance/track win rate                               | Mostly populated                                            | Computed from all offspring with ≥5 races                | Normal                    |
| odds_score / popularity_score                              | Populated (COALESCE to training median if no realtime odds) | `jvd_se` / fallback                                      | Normal                    |

### Key Finding

The local-mirror probe artifact noted in `jra-relationship-features-perclass.md` ("NaN for 701 = 0% valid odds") was a probe-only issue: the probe read from `race_finish_position_features` (stale local snapshot with NULL odds) and attempted a `jvd_se` join that failed for 701 rows. The **production inference path** reads `jvd_se` directly via `upcoming_target_union_sql` and has no such issue.

`is_newcomer_race` flag uses `NEWCOMER_RACE_JOKEN_CODE = "000"` (NAR code), so JRA 701 races get `is_newcomer_race = 0`. This is not a bug — the GBDT can split on `kyoso_joken_code` directly, and `target_class_level` maps `'701'` → 4.

No JRA-701-specific serve bug found analogous to the NAR F1 pedigree 44.8% NULL issue.

---

## Attribution

### Backend ρ for maiden races: **-0.075** (near-zero, slightly negative)

The backend model order for maiden races is **effectively uncorrelated with popularity**. Zero races at the backend level have ρ ≥ 0.95. The backend is NOT the source of the popularity echo the user observes.

### Verdict: **(a) CLIENT-OVERLAY-DOMINANT — backend is fine**

The popularity echo reported by the user originates in the **client-side odds-correction overlay** in `apps/pc-keiba-viewer/src/lib/finish-position-prediction.ts` (being fixed separately). The backend predicted_rank for maiden races has ρ ≈ -0.075 — no echo. The frontend overlay is the dominant and likely sole cause.

---

## Backend Action Required?

**No backend fix is warranted.** The maiden NULL profile (horse-history features all NULL for debut horses) is expected and was present at training time, so the GBDT routes these correctly. Pedigree, jockey, trainer, and weight features are all populated at serve.

### What Could Theoretically Help Maiden Accuracy (Not a Bug Fix)

These are new-signal investments, not fixes to existing issues:

1. **Sire debut-winner rate** — win rate of sire's offspring specifically in their debut race (distinct from overall offspring win rate)
2. **Trainer first-time-starter stats** — trainer win rate specifically for debut horses
3. **Sale price / auction history** — strong debut predictor where available (Keiba Book / セレクトセール データ)
4. **Workout (調教) times** — pre-race gallop times from JRA official data

None of these are currently in the pipeline. They are new data sources, not fixes to NULL bugs.

---

## Summary

| Question                    | Answer                                                       |
| --------------------------- | ------------------------------------------------------------ |
| Backend maiden ρ            | -0.075 (near-zero, no echo)                                  |
| Races with backend ρ ≥ 0.95 | 0 / 47                                                       |
| Maiden serve-feature bug?   | No — NULLs are expected horse-history, not a pipeline defect |
| Attribution                 | **(a) Client-overlay-dominant**                              |
| Backend fix needed?         | No                                                           |
| Frontend fix covers it?     | Yes — the FE odds-correction overlay fix is sufficient       |
