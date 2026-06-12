# Triplet Ideation — Ranked (2026-06-12)

Three-column temporal-trajectory + interaction patterns for finish-position improvement.  
Each entry: 3 PG columns, temporal aggregation, relationship form, target category, leak-free sketch, rationale, redundancy risk.

---

## Ranking Summary

| Rank | Triplet (3 columns)                                         | One-line rationale                                                                                                                                                                                        |
| ---- | ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | `kohan_3f`, `corner4_norm`, `babajotai_code`                | Late-section speed × finishing position × track condition trajectory; unifies sectional pace with environment — existing features track these independently, never jointly                                |
| 2    | `futan_juryo`, `bataiju`, `kyori`                           | Weight-carrying-capacity-to-distance efficiency trend; R1 features give static cross-sectional ratios but never the SLOPE of futan/bataiju ratio across distances over time                               |
| 3    | `tansho_odds`, `corner1_norm`, `finish_position`            | Market-positioning divergence: when the market liked a horse but it ran front (nige) and faded — the trajectory of this mismatch                                                                          |
| 4    | `kohan_3f`, `time_sa`, `corner1_norm`                       | Pace-style split: late speed relative to overall margin, partitioned by starting position; identifies horses that systematically benefit from running free vs. covered                                    |
| 5    | `soha_time`, `kyori`, `tenko_code`                          | Weather-adjusted speed trend; `recent_soha_time_per_meter_avg5` already exists but ignores weather; the 3-way interaction adds a new dimension                                                            |
| 6    | `barei`, `futan_juryo`, `finish_position`                   | Age-load interaction trend: do older horses degrade faster under heavy weight? New signal for JRA where age effects are clear                                                                             |
| 7    | `babajotai_code`, `corner1_norm`, `finish_position`         | Surface-condition × running-style × outcome: does this horse's style preference shift on heavy/firm going?                                                                                                |
| 8    | `zogen_sa` (weight delta), `tansho_odds`, `finish_position` | Market-weight-condition triangle: odds at last race vs. current weight change vs. prior finish                                                                                                            |
| 9    | `kyori`, `corner4_norm`, `finish_position`                  | Distance × late-position × result: does the horse specifically peak at certain distances in finishing position? Weak because distance features and corner features are already well-covered independently |
| 10   | `barei`, `career_win_rate`, `keibajo_code`                  | Age × venue-specific career form: older horses that keep winning at specific venues — expected to add very little given existing `same_keibajo_win_rate` + age already implicit in model                  |

---

## Detailed Entries

### Rank 1 — `kohan_3f` × `corner4_norm` × `babajotai_code`

**3 columns (PG source):**

- `jvd_se.kohan_3f` / `nvd_se.kohan_3f` — last-3f sectional time (post-race, used as label in horse history)
- `jvd_se.corner4_norm` — final corner normalised position (= corner4 / shusso_tosu)
- `jvd_ra.babajotai_code` / `babajotai_code_dirt` — track condition ordinal (1=firm/良 … 4=heavy/不)

**Temporal aggregation:**
Over horse's last 5 past races (strictly `race_date < target_date`):

- `avg(kohan_3f)` by `babajotai_code` bucket (2 groups: firm=1-2, soft=3-4) → call `kohan_3f_avg_firm5`, `kohan_3f_avg_soft5`
- `regr_slope(corner4_norm, -recent_rank)` → finishing-position-in-race trend, conditioned on going
- `kohan_3f_avg_firm5 - kohan_3f_avg_soft5` → differential: late speed premium/penalty on soft ground

**Relationship form:**
Conditional average + slope: late-section speed trajectory partitioned by going quality, plus interaction `Δkohan_3f_by_going × corner4_norm_trend`.

**Target category:** JRA and NAR (both have `babajotai_code`); Ban-ei has `babajotai_code_dirt` — applicable there too.

**Leak-free construction:**
All three columns are POST-race signals from prior starts; joined to the CURRENT race via horse id with strict `<` date filter. The current race's going is pre-race knowable (race conditions published before start).

**Expected-contribution rationale:**
`kohan3f_avg_5` exists but is unconditional on going. The key insight is that some horses have a strongly condition-dependent finishing kick: a horse that runs kohan 35.0 on firm but 36.5 on heavy has a very different profile than one that is stable at 35.5. The joint slope of `corner4_norm` (how well it finishes in the field) under each going type is not captured anywhere. This triplet provides the most novel signal gap relative to existing features.

**Redundancy risk vs existing features:**
`kohan3f_avg_5` and `last_3_avg_kohan_3f` exist (unconditional). `corner_pass_avg_5` exists (unconditional). `babajotai_code` appears only as a static race-level ordinal in `tenko_code` / race-internal features. The CONDITIONAL TRAJECTORY (kohan×going×finishing-position) is entirely new.

---

### Rank 2 — `futan_juryo` × `bataiju` × `kyori`

**3 columns (PG source):**

- `jvd_se.futan_juryo` / `nvd_se.futan_juryo` — jockey/equipment weight (kg)
- `jvd_se.bataiju` — horse bodyweight (kg)
- `jvd_ra.kyori` / `nvd_ra.kyori` — race distance (m)

**Temporal aggregation:**
Over horse's last 5 past races:

- `regr_slope(futan_juryo / bataiju, -recent_rank)` → carrying-ratio trend over time
- `avg(finish_position)` partitioned by `kyori` bucket (short ≤1400m, middle 1400-2000m, long ≥2000m) for top-2 recent distance buckets
- `(futan_juryo / bataiju) × (1 / kyori)` averaged → effective load density history

**Relationship form:**
Ratio product: `(futan/bataiju) / kyori_norm` — how the carrying load density (total carried weight relative to horse mass, per metre) evolves over the horse's recent starts, and whether that trajectory predicts the current race outcome.

**Target category:** JRA (handicap races, clear futan variation), NAR (also varies), Ban-ei (futan hex fix already in; futan_class already covered by existing features → partial redundancy for Ban-ei only).

**Leak-free construction:**
All values from prior races; current race futan is pre-race knowable (weight declaration published before race). Current kyori is pre-race knowable.

**Expected-contribution rationale:**
`past_speed_kg_normalized_avg5` and `past_speed_futan_normalized_avg5` in R1 features compute `soha_time/kyori × bataiju` / `× futan` — but those are SPEED × LOAD, not LOAD TREND. The slope of futan/bataiju ratio tells you whether a horse is being asked to carry more and more weight relative to its own mass, and whether that correlates with performance degradation. This is structurally different from existing features and particularly valuable in JRA handicaps.

**Redundancy risk:**
`bataiju_futan_ratio`, `futan_minus_bataiju_zscore_in_race`, `joint_ratio` exist as WITHIN-RACE (current race relative) features. The TIME-SERIES SLOPE of this ratio is absent.

---

### Rank 3 — `tansho_odds` × `corner1_norm` × `finish_position`

**3 columns (PG source):**

- `jvd_se.tansho_odds` / `nvd_se.tansho_odds` — win odds (prior race values from history)
- `jvd_se.corner1_norm` — first corner position normalised
- `jvd_se.kakutei_chakujun` → `finish_position` — final result

**Temporal aggregation:**
Over horse's last 5 past races:

- `corr(1/tansho_odds, finish_position)` → market accuracy correlation over recent runs (negative = market was right more often)
- `avg(corner1_norm)` for races where `tansho_odds` was in top-3 popularity (favourite races) vs. out-of-favour races
- `regr_slope(finish_position, corner1_norm × 1/tansho_odds)` → combined momentum: when market liked it AND it ran front, did it perform?

**Relationship form:**
Conditional average + linear interaction: market-positioning × running-style mismatch trajectory. Three-way interaction captures whether this horse's pace placement has improved or worsened relative to market expectation.

**Target category:** JRA and NAR (both have odds); Ban-ei (limited utility; odds less liquid).

**Leak-free construction:**
All three values are from prior completed races. Current race odds are pre-race knowable. Current race corner1 is not yet knowable — so feature uses HISTORY only (not current-race corner).

**Expected-contribution rationale:**
`inverse_odds_implied_prob`, `popularity_rank_in_race`, `past_corner_1_norm_avg_5`, and `past_nige_rate_self` all exist. But the JOINT TRAJECTORY — did this horse's tendency to run front improve its market-accuracy? Does the market increasingly mis-price this horse's running style? — is captured nowhere. This surfaces the "market has been systematically wrong about this horse's front-running ability" signal.

**Redundancy risk:**
Overlap with `past_corner_1_norm_avg_5` (corner trajectory, unconditional) and `tansho_odds_raw` (static current-race). The conditional market-accuracy trajectory is new.

---

### Rank 4 — `kohan_3f` × `time_sa` × `corner1_norm`

**3 columns (PG source):**

- `jvd_se.kohan_3f` — last 3f sectional
- `jvd_se.time_sa` — margin to winner (seconds)
- `jvd_se.corner1_norm` — first corner position

**Temporal aggregation:**
Over horse's last 5 past races:

- `regr_slope(kohan_3f / nullif(time_sa + 0.001, 0), -recent_rank)` → late-kick efficiency trend (how much of the margin comes from late acceleration)
- `avg(kohan_3f)` partitioned into front-runner (corner1_norm ≤ 0.2) vs. off-pace (corner1_norm > 0.5) buckets
- `(kohan_3f_front_avg - kohan_3f_offpace_avg)` → pace-split kick differential

**Relationship form:**
Conditional average slope: late-section speed as a function of running position, tracked over time.

**Target category:** JRA and NAR; Ban-ei has no reliable kohan_3f sectionals.

**Leak-free construction:**
All from prior race history. Current race: none of these three are knowable pre-race (they are post-race for the current entry), so feature is built only from historical windows.

**Expected-contribution rationale:**
`kohan3f_avg_5` is unconditional. `past_corner_1_norm_avg_5` is unconditional. The JOINT signal — how does late speed change depending on whether the horse ran front vs. off-pace, and is this differential increasing — is not captured. A horse improving its late kick specifically when NOT front-running is a genuine maturation signal.

**Redundancy risk:**
Partially overlaps `kohan3f_avg_5` and `past_corner_1_norm_avg_5`. The CONDITIONAL difference is new.

---

### Rank 5 — `soha_time` × `kyori` × `tenko_code`

**3 columns (PG source):**

- `jvd_se.soha_time` / `nvd_se.soha_time` — total race time (seconds)
- `jvd_ra.kyori` — distance (m)
- `jvd_ra.tenko_code` — weather code (1=clear/晴 … 5=snow/雪)

**Temporal aggregation:**
Over horse's last 5 past races:

- `avg(soha_time / kyori)` grouped by `tenko_code` bucket (good weather: 1-2 vs. adverse: 3-5)
- `(speed_good - speed_adverse)` → weather speed differential
- `regr_slope(soha_time / kyori, -recent_rank)` conditioned on weather bucket

**Relationship form:**
Conditional average: speed trajectory partitioned by weather regime; surface: how much does adverse weather slow this horse vs. peers.

**Target category:** JRA (all weather); NAR (also has tenko); Ban-ei (tenko available in nvd_ra).

**Leak-free construction:**
All from prior race history. Current race weather is pre-race knowable (forecast published).

**Expected-contribution rationale:**
`recent_soha_time_per_meter_avg5` exists but is unconditional. Weather-conditional speed history is absent. A horse that runs 0.03 s/m slower in rain vs. good is categorically different from one that is weather-neutral; this interacts with the current race's forecast.

**Redundancy risk:**
`recent_soha_time_per_meter_avg5` and `same_distance_soha_time_per_meter_avg5` exist (unconditional). `tenko_code` appears as a static current-race field. The weather-conditional speed trajectory is new but likely a weak effect given race-level noise.

---

### Rank 6 — `barei` × `futan_juryo` × `finish_position`

**3 columns (PG source):**

- `jvd_se.barei` — horse age (years, at race time)
- `jvd_se.futan_juryo` — carried weight (kg)
- `jvd_se.kakutei_chakujun` → `finish_position`

**Temporal aggregation:**
Over horse's career history (all past races, strictly before target):

- `regr_slope(finish_position, futan_juryo)` grouped by `age_bucket` (young ≤4, prime 5-6, veteran ≥7)
- `avg(finish_position)` at each `(age_bucket, futan_class)` cell
- Delta from prime to current: `finish_position_at_futan_current_age - finish_position_at_futan_prime_age`

**Relationship form:**
Conditional regression: finish sensitivity to weight load, as a function of age stage, tracked over career.

**Target category:** JRA (clear age effects, handicap variety), NAR (moderate); Ban-ei (partially covered by existing `horse_futan_class_career_win_rate` — high redundancy for Ban-ei).

**Leak-free construction:**
All values from prior races. Current race `futan_juryo` and `barei` are pre-race knowable.

**Expected-contribution rationale:**
`career_win_rate` and `same_grade_win_rate` exist. `bataiju_futan_ratio` is a within-race static ratio. The CAREER TRAJECTORY of load-sensitivity as a function of aging is absent. Older horses degrade faster under load; JRA handicap re-allocations exploit this; the model doesn't see it.

**Redundancy risk:**
Overlaps `horse_futan_class_career_win_rate` for Ban-ei. For JRA/NAR the explicit age×load trajectory is new but noisier; weaker expected gain than ranks 1-5.

---

### Rank 7 — `babajotai_code` × `corner1_norm` × `finish_position`

**3 columns (PG source):**

- `jvd_ra.babajotai_code` / `nvd_ra.babajotai_code_dirt` — track surface condition ordinal
- `jvd_se.corner1_norm` — normalised first corner position
- `jvd_se.kakutei_chakujun` → `finish_position`

**Temporal aggregation:**
Over horse's last 10 past races:

- `avg(finish_position)` partitioned by `(babajotai_bucket, style_bucket)` (2×2: firm/soft × front/off)
- `finish_diff = finish_soft_front - finish_firm_front` → style-condition interaction magnitude
- `regr_slope(finish_position, babajotai_code_ord)` conditioned on front-runner history

**Relationship form:**
2×2 conditional average: does this horse's running style (front vs. off) change effectiveness under different going? Interaction: `babajotai × corner1 → finish trajectory`.

**Target category:** JRA and NAR; Ban-ei (`babajotai_code_dirt`).

**Leak-free construction:**
All from prior race history. Current going is pre-race knowable.

**Expected-contribution rationale:**
`past_corner_1_norm_avg_5` and running style rates exist. `babajotai_code` exists as a race-level static. The CONDITIONAL style-effectiveness by going is absent. A front-runner on firm may be a closer on soft — detecting this switch is potentially valuable.

**Redundancy risk:**
High overlap with `past_nige_rate_self` × `babajotai_code` combination (which the model may already learn implicitly via feature interactions). Signal is real but likely small — ranked 7.

---

### Rank 8 — `zogen_sa` × `tansho_odds` × `finish_position`

**3 columns (PG source):**

- `jvd_se.zogen_sa` / `nvd_se.zogen_sa` — weight change from previous race (kg, signed)
- `jvd_se.tansho_odds` — win odds
- `jvd_se.kakutei_chakujun` → `finish_position`

**Temporal aggregation:**
Over horse's last 5 past races:

- `avg(finish_position)` conditioned on `sign(zogen_sa)` (gaining weight vs. losing)
- `corr(zogen_sa, tansho_odds_change)` → does market react to weight change?
- `avg(tansho_odds × abs(zogen_sa))` → market-times-condition signal trajectory

**Relationship form:**
Conditional correlation + product average: market responsiveness to weight-condition signal.

**Target category:** JRA and NAR.

**Leak-free construction:**
`zogen_sa` and `tansho_odds` from prior races. Current race `zogen_sa` is pre-race knowable (post-weigh-in); current odds are pre-race.

**Expected-contribution rationale:**
`weight_diff_from_avg` and `weight_trend_5` exist (pure weight trajectory). `tansho_odds_raw` and market signal features exist. But the JOINT signal — does the market correctly price this horse's weight change, and has this market-condition responsiveness been historically right? — is new. A horse that the market prices down after weight gain but consistently outperforms that signal may be systematically undervalued.

**Redundancy risk:**
Overlaps `weight_diff_from_avg` (weight trajectory) and `inverse_odds_implied_prob` (market). The three-way interaction is novel but likely weak — weight effects are partially captured already and market efficiency is near-frontier.

---

### Rank 9 — `kyori` × `corner4_norm` × `finish_position`

**3 columns (PG source):**

- `jvd_ra.kyori` — distance (m)
- `jvd_se.corner4_norm` — final corner position (normalised)
- `jvd_se.kakutei_chakujun` → `finish_position`

**Temporal aggregation:**
Over horse's last 10 past races:

- `avg(corner4_norm)` by `kyori_bucket` (short/middle/long)
- `avg(finish_position)` by `kyori_bucket`
- `finish_pos_long - finish_pos_short` → distance preference differential

**Relationship form:**
Bucket conditional average: at which distance does this horse achieve the best late-race position AND result?

**Target category:** All three categories.

**Leak-free construction:**
All from prior race history. Current kyori is pre-race knowable.

**Expected-contribution rationale:**
`same_distance_win_rate` and `same_distance_soha_time_per_meter_avg5` capture distance preference on result and speed. `corner_pass_avg_5` captures late-corner position unconditionally. The JOINT `corner4_norm × distance` preference is partially new, but the model almost certainly learns the underlying pattern from existing features via interactions.

**Redundancy risk:**
High — very similar to `same_distance_win_rate` (result by distance) and `past_corner_1_norm_avg_5` (position trajectory). Ranked 9 because the added joint information is marginal.

---

### Rank 10 — `barei` × `career_win_rate_proxy` × `keibajo_code`

**3 columns (PG source):**

- `jvd_se.barei` — horse age at race time
- Derived from `jvd_se.kakutei_chakujun` — career win indicator
- `jvd_ra.keibajo_code` — venue code

**Temporal aggregation:**
Over horse's full career history:

- `sum(finish_position=1)` grouped by `(keibajo_code, age_bucket)` → venue × age win count
- `win_rate_at_venue_in_prime - win_rate_at_venue_now` → venue-specific aging degradation
- `age_of_first_win_at_venue` → learning curve indicator

**Relationship form:**
Conditional career aggregate: venue-specific win rate trajectory as a function of age stage.

**Target category:** JRA (many venues, clear age distribution); NAR (some venues); Ban-ei (only one venue — keibajo_code is constant, making this degenerate).

**Leak-free construction:**
All from prior completed races with strict date filter.

**Expected-contribution rationale:**
`same_keibajo_win_rate` already captures venue-specific career form unconditionally. `career_win_rate` captures overall career form. The AGE-CONDITIONAL venue trajectory adds very little new information: the model can implicitly approximate `barei × same_keibajo_win_rate` via feature interactions. This triplet is expected to be redundant or even noisy due to sparse cells (few horses have many starts at many venues across multiple age stages).

**Redundancy risk:**
Very high — directly overlaps `same_keibajo_win_rate` + implicit age handling. Ranked last intentionally.

---

## Construction Notes (shared across all triplets)

**Strict leak-free pattern (all triplets):**

```sql
WHERE rh.race_date < bi.race_date   -- strictly before target race
  AND rh.kakutei_chakujun IS NOT NULL  -- completed races only
```

**Window size convention:**

- Recent trajectory features use `recent_rank <= 5` or `<= 10` (explicit in each entry).
- Career conditionals use `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING` ordered by `race_date`.

**NULL handling:**

- All aggregates use `FILTER (WHERE col IS NOT NULL)` to avoid NULL propagation.
- LightGBM handles NULLs natively; don't impute (per project memory: impute/flag addition is counterproductive for NULL train-time features).

**Verification pairing (alternating high/low):**
Intended dispatch order: (1,10), (2,9), (3,8), (4,7), (5,6).
