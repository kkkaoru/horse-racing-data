# Triplet Verification P1 — Rank 1 & Rank 10 (2026-06-12)

## Context

Per-class Spearman ρ probe on two triplets from `triplet-ideation-ranked.md`.
READ-ONLY on local PG mirror (`postgresql://horse_racing:***@127.0.0.1:15432/horse_racing`).
Holdout: 2023-01-01 to 2026-05-31. Gate: ρ ≥ 0.08 any class + not-redundant (max |corr| vs
existing features ≤ 0.85). Probe script: `/tmp/run_probe_v2.py` (not tracked).

---

## Rank 1: `kohan_3f` × `corner4_norm` × `babajotai_code`

### Hypothesis

Late-section speed (`kohan_3f`) conditioned on going quality (`babajotai_code` → firm/soft
bucket) captures a horse-specific environmental sensitivity not present in the unconditional
`kohan3f_avg_5`. When combined with the late-corner field position (`corner4_norm`), the joint
trajectory reveals horses that specifically accelerate in their late section under favourable
(or unfavourable) going.

### Column names confirmed

- `jvd_se.kohan_3f` — last 3-furlong sectional (tenths of seconds; ÷10 → seconds)
- `jvd_se.corner_4` — absolute last-corner position integer
- `jvd_ra.shusso_tosu` — field size → `corner4_norm = corner_4 / shusso_tosu`
- `jvd_ra.babajotai_code_shiba` (turf, track_code 10-22) / `babajotai_code_dirt` (dirt, 51-59) — going ordinal 1-4
- Going bucket: firm = codes 1-2, soft = codes 3-4

### Temporal aggregation (leak-free)

All features computed over `ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING` ordered by `race_date`
within `PARTITION BY horse_id`. This covers the horse's last ≤5 prior completed races.

| Feature              | Construction                                                        |
| -------------------- | ------------------------------------------------------------------- |
| `kohan3f_firm_avg5`  | `AVG(kohan_3f) WHERE is_firm=1` over last 5 prior                   |
| `kohan3f_soft_avg5`  | `AVG(kohan_3f) WHERE is_firm=0` over last 5 prior                   |
| `kohan3f_going_diff` | `kohan3f_soft_avg5 − kohan3f_firm_avg5` (positive = slower on soft) |
| `c4_going_diff5`     | `AVG(corner4_norm) soft − firm` over last 5 prior                   |
| `kohan_c4_corr5`     | `CORR(kohan_3f, corner4_norm)` over last 5 prior                    |
| `triplet_high_joint` | `kohan3f_going_diff × c4_avg5`                                      |

### Per-class Spearman ρ table (vs `finish_norm`, holdout 2023-2026)

| Category | Feature              | ρ           | p-value  | n               |
| -------- | -------------------- | ----------- | -------- | --------------- |
| jra      | `kohan3f_firm_avg5`  | **+0.1286** | 0.00e+00 | 102,058         |
| jra      | `kohan3f_soft_avg5`  | **+0.0977** | 2.28e-44 | 20,339          |
| jra      | `kohan3f_going_diff` | +0.0174     | 1.88e-02 | 18,242          |
| jra      | `c4_going_diff5`     | +0.0246     | 8.57e-04 | 18,401          |
| jra      | `kohan_c4_corr5`     | +0.0415     | 2.91e-57 | 147,705         |
| jra      | `triplet_high_joint` | +0.0362     | 1.03e-06 | 18,242          |
| nar      | —                    | n/a         | —        | 0 (holdout gap) |
| ban-ei   | —                    | n/a         | —        | 0 (holdout gap) |

> NAR and ban-ei holdout rows are 0 because `race_finish_position_features` only has
> substantial holdout coverage for JRA (2016-2026). NAR/ban-ei coverage exists only for the
> last 2-3 days of the holdout range and was filtered out by `finish_norm IS NOT NULL`.

### Redundancy analysis

| Feature              | max ρ vs existing | vs feature          |
| -------------------- | ----------------- | ------------------- |
| `kohan3f_firm_avg5`  | +0.72             | `kohan3f_avg_5`     |
| `kohan3f_soft_avg5`  | +0.59             | `kohan3f_avg_5`     |
| `kohan3f_going_diff` | +0.07             | `kohan3f_avg_5`     |
| `c4_going_diff5`     | +0.03             | `corner_pass_avg_5` |
| `kohan_c4_corr5`     | −0.06             | `corner_pass_avg_5` |
| `triplet_high_joint` | +0.28             | `corner_pass_avg_5` |

Key observation: `kohan3f_firm_avg5` and `kohan3f_soft_avg5` are correlated with `kohan3f_avg_5`
at ρ = 0.72 and 0.59 respectively (below the 0.85 redundancy threshold). They represent the
going-conditional breakdown of the unconditional average — genuinely new information. The
differential and joint features have ≤0.28 max correlation with any existing feature.

### Verdict

**PROCEED** — gate met (JRA: `kohan3f_firm_avg5` ρ = +0.1286, `kohan3f_soft_avg5` ρ = +0.0977)
and not redundant (max existing-feature correlation = 0.72 < threshold 0.85).

**Recommended features for implementation:**

1. `kohan3f_firm_avg5` — primary signal; ρ = +0.13, n = 102k (firm-going data is abundant)
2. `kohan3f_soft_avg5` — secondary; ρ = +0.10, n = 20k (soft-going data is sparse)
3. `kohan3f_going_diff` — conditional differential; ρ sub-gate but structurally novel and low-redundancy
4. `kohan_c4_corr5` — rolling correlation of late speed and field position; ρ = +0.04, high coverage (147k)

Within-race variation: the going-conditional averages vary substantially within a race since
different horses have different histories on firm vs. soft.

---

## Rank 10: `barei` × `career_win_rate` × `keibajo_code`

### Hypothesis

Older horses that have historically won at specific venues may retain venue-specific advantage
even as overall career win rate declines. The age × venue trajectory (prime-age venue win rate
minus current win rate) would surface this degradation signal.

### Column names confirmed

- `jvd_se.barei` — horse age (integer, years)
- `jvd_se.kakutei_chakujun` — finish position
- `jvd_ra.keibajo_code` — venue code (2-char)

### Temporal aggregation (leak-free)

Venue-specific career aggregates using `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`
within `PARTITION BY (horse_id, keibajo_code)`.

| Feature                 | Construction                                                |
| ----------------------- | ----------------------------------------------------------- |
| `venue_win_rate_prior`  | wins ÷ starts at same venue before this race (min 2 starts) |
| `venue_age_degradation` | `venue_win_rate_prior − venue_prime_wr` (prime = age 5-6)   |
| `age_first_venue_win`   | age at first win at this venue (from prior races)           |

### Per-class Spearman ρ table (vs `finish_norm`, holdout 2023-2026)

| Category | Feature                 | ρ           | p-value   | n               |
| -------- | ----------------------- | ----------- | --------- | --------------- |
| jra      | `venue_win_rate_prior`  | **−0.1181** | 1.01e-280 | 91,213          |
| jra      | `venue_age_degradation` | −0.0061     | 3.93e-01  | 19,348          |
| jra      | `age_first_venue_win`   | +0.0197     | 8.35e-05  | 40,033          |
| nar      | —                       | n/a         | —         | 0 (holdout gap) |
| ban-ei   | —                       | n/a         | —         | 0 (holdout gap) |

### Redundancy analysis

| Feature                 | max ρ vs existing | vs feature              |
| ----------------------- | ----------------- | ----------------------- |
| `venue_win_rate_prior`  | **+0.9983**       | `same_keibajo_win_rate` |
| `venue_age_degradation` | +0.26             | `same_keibajo_win_rate` |
| `age_first_venue_win`   | −0.21             | `career_win_rate`       |

Critical finding: `venue_win_rate_prior` has ρ = +0.9983 with `same_keibajo_win_rate` (the
existing feature). They are essentially the same quantity — `same_keibajo_win_rate` already
computes the horse's career win rate at the current venue using the same leak-free prior-race
restriction. The triplet's primary feature is therefore redundant by construction.

`venue_age_degradation` and `age_first_venue_win` are structurally novel (max corr ≤ 0.26)
but failed the gate (|ρ| < 0.08 with no significance for `venue_age_degradation`, p = 0.39).

### Verdict

**ABORT — gate met but `venue_win_rate_prior` is redundant** (max_corr_vs_existing = +0.9983).

The two non-redundant features (`venue_age_degradation`, `age_first_venue_win`) fail the gate.
This confirms the ideation doc's prediction: the model already approximates age × venue
interaction via `same_keibajo_win_rate` + implicit age handling. No net signal gain expected.

---

## Summary

| Triplet                                        | Rank | Best ρ  | Best class | Best feature           | Verdict     |
| ---------------------------------------------- | ---- | ------- | ---------- | ---------------------- | ----------- |
| `kohan_3f` × `corner4_norm` × `babajotai_code` | 1    | +0.1286 | JRA        | `kohan3f_firm_avg5`    | **PROCEED** |
| `barei` × `career_win_rate` × `keibajo_code`   | 10   | −0.1181 | JRA        | `venue_win_rate_prior` | **ABORT**   |

### Notes

1. The JRA holdout is 226k rows (2023-2026); NAR and ban-ei have no substantial holdout coverage
   in `race_finish_position_features` — verification for those categories must await feature
   regen.
2. TRIPLET HIGH features (`kohan3f_firm_avg5`, `kohan3f_soft_avg5`) should be added to the
   feature pipeline for JRA. The differential (`kohan3f_going_diff`) is low-redundancy and worth
   including even if below the gate individually.
3. TRIPLET LOW is confirmed as lowest-value triplet: `venue_win_rate_prior` ≈ `same_keibajo_win_rate`
   (ρ_corr = 0.998) with zero incremental signal from the age-conditioning.
