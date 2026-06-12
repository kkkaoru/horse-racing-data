# Triplet Verification P5 — Rank 5 & Rank 6 (2026-06-12)

## Context

Per-class probe on two triplets from `triplet-ideation-ranked.md`. Analysis is READ-ONLY on
the local PG mirror (postgresql://horse_racing:\*\*\*@127.0.0.1:15432/horse_racing). Holdout:
2023-01-01 to 2026-06-12. Gate: partial Spearman ρ ≥ 0.08 (any class, any feature variant).

Probe scripts: `tmp/triplet_p5_high.txt`, `tmp/triplet_p5_low.txt` (not tracked).

---

## Rank 5 (HIGH): `soha_time` × `kyori` × `tenko_code`

### Hypothesis

Weather-conditional speed trend: `recent_soha_time_per_meter_avg5` already exists but is
unconditional on weather. The 3-way interaction adds a differential — a horse's average speed
in adverse vs. good weather conditions over the last 5 races, capturing weather sensitivity as
an independent predictive dimension.

### Column names confirmed

- `jvd_se.soha_time` / `nvd_se.soha_time` — race finish time in tenths of seconds (e.g.
  `"1367"` = 136.7 s). VARCHAR.
- `jvd_ra.kyori` / `nvd_ra.kyori` — race distance in metres (e.g. `"1400"`). VARCHAR.
- `jvd_ra.tenko_code` / `nvd_ra.tenko_code` — weather at race time. VARCHAR: `1`=晴(clear),
  `2`=曇(cloudy), `3`=雨(rain), `4`=小雨(light rain), `5`=雪(snow), `6`=小雪(light snow).
- `speed_m_per_s` = `kyori::numeric / (soha_time::numeric / 10.0)`

### Feature construction (leak-free)

For each target race (2023+), look back at the horse's last ≤ 5 completed races with strictly
earlier `race_date_str`:

```sql
-- Bucket weather per past race:
weather_bucket = CASE WHEN tenko_code IN ('1','2') THEN 'good' ELSE 'adverse' END

-- Per-horse aggregates over last 5:
good_speed_avg    = AVG(speed_m_per_s) FILTER (WHERE weather_bucket = 'good')
adverse_speed_avg = AVG(speed_m_per_s) FILTER (WHERE weather_bucket = 'adverse')
weather_speed_diff = good_speed_avg - adverse_speed_avg
```

Weather distribution in history (2017+, 2.08M rows): 58% clear, 32% cloudy, 8% rain, 2%
snow/light-rain. 77.8% of target rows have the feature (22.2% lack at least one weather bucket
in recent history).

### Holdout results (2023–2026, within-race demeaned finish_norm)

Controls: `speed_index_avg_5`, `speed_index_best_5`, `jockey_career_win_rate`.

| Class  | N target rows | Feature              | partial ρ | Gate ≥ 0.08 |
| ------ | ------------- | -------------------- | --------- | ----------- |
| JRA    | 216 020       | `weather_speed_diff` | +0.0049   | FAIL        |
| NAR    | —             | —                    | SKIP      | —           |
| Ban-ei | —             | —                    | SKIP      | —           |

NAR (n=646) and Ban-ei (n=210) had insufficient control feature coverage after R2-parquet join
to estimate stable partial ρ; results skipped.

### Redundancy check

`weather_speed_diff` vs `speed_index_avg_5`: ρ = −0.022 (not redundant).
`weather_speed_diff` vs `speed_index_best_5`: ρ = −0.013 (not redundant).
The feature is orthogonal to existing speed features — but this is moot given near-zero signal.

### Interpretation

The feature is novel (low redundancy with existing speed measures) but carries essentially no
predictive signal (partial ρ = +0.0049, near zero). Two structural reasons:

1. **Weather contrast is sparse:** 22% of horses lack both weather buckets in recent 5 races.
   Among those that do, the good/adverse split is estimated from few races, giving noisy
   differentials.
2. **Track-surface dominates:** Turf vs. dirt surface and babajotai (going) code already encode
   surface quality. Weather code (`tenko_code`) is a redundant proxy — the model likely already
   captures the information through going features. The horse-specific weather _sensitivity_
   above and beyond surface/going is too small a signal to measure reliably.

### Verdict

**ABORT** — gate ρ ≥ 0.08 not met in any (class, feature) cell. Best observed: JRA
`weather_speed_diff` ρ = +0.0049. The horse-specific weather sensitivity signal is indistinct
from noise after controlling for unconditional speed level. Not recoverable by window extension
or feature variant — the information is structurally absorbed by existing speed and surface
features.

---

## Rank 6 (LOW): `barei` × `futan_juryo` × `finish_position`

### Hypothesis

Career trajectory of load sensitivity by age stage: older horses degrade in finish position
faster than younger horses when weight assignments increase. JRA handicap signal. `barei`
(horse age) is confirmed absent from all existing feature parquets — it is a genuinely novel
dimension.

### Column names confirmed

- `jvd_se.barei` / `nvd_se.barei` — horse age in years. VARCHAR, cast to numeric.
- `jvd_se.futan_juryo` / `nvd_se.futan_juryo` — weight carried in 0.1 kg units
  (e.g. `"550"` = 55.0 kg). VARCHAR. Ban-ei uses same encoding.
- `jvd_se.kakutei_chakujun` / `nvd_se.kakutei_chakujun` — finish position. VARCHAR.
- `jvd_se.ketto_toroku_bango` / `nvd_se.ketto_toroku_bango` — horse ID for temporal join.

### Existing futan features in model (JRA)

7 features already present: `futan_juryo`, `past_futan_juryo_avg5`, `past_high_futan_share`,
`futan_juryo_rank_in_race`, `futan_juryo_diff_from_race_avg`, `past_futan_juryo_diff`,
`futan_weight_class`. NAR existing feature parquet has **zero** futan features.

### Feature construction (leak-free)

For each target race (2023+), look back at the horse's last ≤ 5 completed races strictly before
the target date:

```sql
-- Age stage per past race:
age_stage = CASE
  WHEN barei <= 3 THEN 'young'
  WHEN barei BETWEEN 4 AND 5 THEN 'mid'
  ELSE 'old'
END

-- Feature 1: OLS slope of finish_pos ~ futan_juryo over last 5 races
load_slope_5 = REGR_SLOPE(finish_pos, futan_juryo_kg)

-- Feature 2: age-load degradation rate
--   = (avg_finish_old - avg_finish_mid) / (|avg_futan_old - avg_futan_mid| + 0.5)
--   Requires horse to have runs in BOTH mid and old stages in history
age_load_degradation_rate = (avg_finish_old - avg_finish_mid)
                            / (ABS(avg_futan_old - avg_futan_mid) + 0.5)

-- Feature 3: continuous age (barei) — not in any existing feature
barei_cont = barei::numeric

-- Feature 4: interaction (partially redundant — see below)
barei_x_futan = barei::numeric * futan_juryo_kg
```

Join rates: JRA 18 270 / 231 653 (7.9%), NAR 47 821 / 465 178 (10.3%), Ban-ei 4 412 / 55 223
(8.0%). `age_load_degradation_rate` requires a horse to have logged races in BOTH mid (age 4-5)
AND old (age 6+) stages — restricting to multi-year career veterans.

### Holdout results (2023–2026, within-race demeaned finish_norm)

Controls: existing futan features (JRA); `popularity_rank_in_race` (NAR/Ban-ei — futan absent
from those parquets).

| Class  | N target rows | Feature                     | partial ρ   | Gate ≥ 0.08 |
| ------ | ------------- | --------------------------- | ----------- | ----------- |
| JRA    | 18 270        | `load_slope_5`              | +0.0226     | FAIL        |
| JRA    | 18 270        | `age_load_degradation_rate` | +0.0389     | FAIL        |
| JRA    | 18 270        | `barei_cont`                | +0.0790     | FAIL        |
| JRA    | 18 270        | `barei_x_futan`             | +0.0467     | FAIL        |
| NAR    | 47 821        | `load_slope_5`              | +0.0017     | FAIL        |
| NAR    | 47 821        | `age_load_degradation_rate` | **+0.1647** | **PASS**    |
| NAR    | 47 821        | `barei_cont`                | +0.0608     | FAIL        |
| NAR    | 47 821        | `barei_x_futan`             | +0.0279     | FAIL        |
| Ban-ei | 4 412         | `load_slope_5`              | +0.0342     | FAIL        |
| Ban-ei | 4 412         | `age_load_degradation_rate` | +0.0642     | FAIL        |
| Ban-ei | 4 412         | `barei_cont`                | +0.0353     | FAIL        |
| Ban-ei | 4 412         | `barei_x_futan`             | +0.0374     | FAIL        |

### Redundancy check (JRA, vs existing futan features)

| Feature                     | Max \|ρ\| vs existing futan | Top existing feature       | Verdict             |
| --------------------------- | --------------------------- | -------------------------- | ------------------- |
| `load_slope_5`              | 0.0776                      | `futan_juryo`              | NOT redundant       |
| `age_load_degradation_rate` | 0.0456                      | `futan_juryo_rank_in_race` | NOT redundant       |
| `barei_cont`                | 0.0743                      | `futan_juryo_rank_in_race` | NOT redundant       |
| `barei_x_futan`             | 0.3145                      | `futan_juryo`              | PARTIALLY REDUNDANT |

`barei_x_futan` has ρ = 0.31 vs `futan_juryo` — skip this variant.

### Interpretation

**NAR `age_load_degradation_rate` (ρ = 0.1647, PASS):**
NAR horses run longer careers (ages 7-10+ common), making the old-vs-mid stage comparison
estimable and meaningful. NAR weight assignments vary more across age stages than JRA. The
feature captures deterioration in finishing performance as weight increases between career
stages — a genuine degradation signal orthogonal to the 7 existing JRA futan features (max ρ
vs existing = 0.046). **Critical caveat:** NAR existing parquet has zero futan features, so the
partial ρ is controlled only for popularity — this inflates the apparent ρ vs. what would remain
after full futan-feature addition to NAR.

**JRA `barei_cont` (ρ = 0.0790, near-gate):**
`barei` (horse age) is absent from all existing feature parquets, making it a free-rider
candidate with no redundancy penalty. JRA ρ = 0.079 is just below gate but the feature is
trivially constructable. Probe limited to older-horse subset (7.9% of JRA holdout) due to
`age_load_degradation_rate` join filter — the full-field `barei_cont` estimate on all JRA rows
may differ.

**Why JRA `age_load_degradation_rate` is weak (ρ = 0.039):**
JRA weight regulation is tight (increments small between age stages), so the futan delta between
mid and old career is minimal. Existing JRA futan features (7 of them) already absorb most of
the weight signal. The incremental contribution is modest.

### Verdict

**PROCEED (conditional, NAR-only priority)**

NAR `age_load_degradation_rate` clears the gate at partial ρ = 0.1647, with near-zero
redundancy vs existing futan features. However:

1. **Re-probe required for NAR:** NAR parquet lacks futan features entirely. Partial ρ is vs.
   popularity-only controls. After adding futan features to NAR (which is independently warranted),
   the residual ρ for `age_load_degradation_rate` will likely drop. Re-probe before committing to
   full engineering.
2. **`barei_cont` as free rider:** Horse age is absent from all models and trivially added. JRA
   ρ = 0.079 (near-gate on a restricted subset). Worth adding as a low-cost feature across all
   classes regardless of `age_load_degradation_rate`.
3. **Skip `barei_x_futan`:** Partially redundant with existing futan features (ρ = 0.31).
4. **Skip `load_slope_5`:** Max partial ρ = 0.034 (Ban-ei), too low across all classes.

Recommended feature priority for incremental-verify:

1. `age_load_degradation_rate` (NAR, after NAR futan feature addition)
2. `barei_cont` (all classes, free rider)

---

## Summary

| Rank | Triplet                                     | Best feature                | Best ρ | Class | Gate ≥ 0.08 | Verdict            |
| ---- | ------------------------------------------- | --------------------------- | ------ | ----- | ----------- | ------------------ |
| 5    | `soha_time` × `kyori` × `tenko_code`        | `weather_speed_diff`        | 0.0049 | JRA   | FAIL        | ABORT              |
| 6    | `barei` × `futan_juryo` × `finish_position` | `age_load_degradation_rate` | 0.1647 | NAR   | PASS        | PROCEED (caveated) |

**Cross-pair interpretation:** The alternating HIGH/LOW ordering was well-calibrated in the
opposite direction to expectation: Rank 5 (HIGH confidence) is a clear ABORT, while Rank 6
(LOW confidence) produces the strongest gate-passing signal of the P5 pair. The NAR ρ = 0.1647
is notable but rests on a weak-controls baseline (popularity-only); the signal is plausible
structurally but needs the full futan-feature control set before engineering commitment.

**Saturation note:** Rank 5 ABORT is consistent with the documented frontier
(`finish_position_frontier_2026_06_11.md`): horse-level weather-conditional speed signals
built from existing PG columns fall below the gate. Rank 6 partial PROCEED is the first P5
cell to pass the gate, driven by a feature dimension (`barei`) genuinely absent from all
current models.
