# The Effect of Age on the Racing Speed of Thoroughbred Racehorses

## Metadata

| Field                          | Value                                                                                                |
| ------------------------------ | ---------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 26(2): 43–48, 2015                                                                    |
| docid                          | `26_1506`                                                                                            |
| Article type                   | Original Article                                                                                     |
| Authors                        | Toshiyuki TAKAHASHI                                                                                  |
| Affiliations                   | Sport Science Division, Equine Research Institute, Japan Racing Association, Tochigi 320-0856, Japan |
| Received / Accepted / Released | February 4, 2015 / March 19, 2015 / 2015                                                             |
| Keywords                       | Thoroughbred, race, speed, growth, age                                                               |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/26/2/26_1506/_pdf/-char/en                                  |

## Abstract (verbatim)

> The running performance of Thoroughbred racehorses has been reported to peak when they are between 4 and 5 years old. However, changes in their racing speed by month or season have not been reported. The purposes of this study were to reveal the average racing speed of Thoroughbreds, and observe changes in their average speed with age. The surveyed races were flat races on turf and dirt tracks with firm or standard track conditions held by the Japan Racing Association from January 1st, 2002 to December 31st, 2010. The racing speed of each horse was calculated by dividing the race distance (m) by the horse's final time (sec). Average speeds per month for each age and distance condition were calculated for each gender group when there were 30 or more starters per month for each age and distance condition for each gender group. The common characteristic change for all conditions was an average speed increase up until the first half of the age of 4 years old. The effect of increased carry weight on average speed was small, and average speed increased with the growth of the horse. After the latter half of the age of 4 years old, the horses' average speed remained almost constant, with little variation. It is speculated that decreases in the weight carried; and the retirement of less well performing horses; are responsible for the maintenance of average speed.

## Relevance to finishing-position (着順) prediction

Feature family **G (statistical modelling/age)** with direct relevance to the JRA target domain.

This is one of the most directly applicable papers in the corpus for a finishing-position model: it provides empirical age-speed curves at monthly resolution for all 10 major JRA race distance/surface combinations, separately by sex. Key engineerable features: (1) monthly age encoded non-linearly (speed rises steeply March–November of age 3, peaks first half of age 4, then plateaus); (2) a three-way interaction term `age_group × distance_bucket × surface`; (3) a `weight_deviation` feature (actual carry weight minus age/sex norm from JRA tables); and (4) a `survivor_selection_flag` for horses age ≥4.5 where the population average speed is stable due to retirement of slower horses.

The finding that peak speed occurs at age 4.0–4.5 and does not decline thereafter (in the population average) is crucial for calibrating age-based features: simple linear age is wrong; a piecewise or spline encoding peaking at ~age 4.0–4.5 months is supported. The paper also confirms that carry-weight effects are secondary to growth effects for ages 2–4.

## Background & objective

Previous reports (Gramm and Marksteiner 2010 for US; Mota et al. 2005 for Brazil; Oki et al. 1994 for Japan) showed peak performance at age 4–5 but reported only annual averages without monthly resolution. JRA races implement a weight allowance for 3-year-olds that is removed between January and March of age 4 by race length. The objective was to compute monthly average racing speed per age × distance × surface × sex and observe the speed development trajectory.

## Materials & methods

**Dataset:** All JRA flat races on turf and dirt, firm or standard condition only (to control for track-condition effects on final time), January 1, 2002 – December 31, 2010.

**Distances surveyed:** Top-5 by starter count on each surface:

- Turf: 1,200 m, 1,400 m, 1,600 m, 1,800 m, 2,000 m
- Dirt: 1,000 m, 1,200 m, 1,400 m, 1,700 m, 1,800 m

**Timing:** Official JRA final time measured by video in 0.1-second increments.

**Speed calculation:** racing speed (m/s) = distance (m) / final time (s).

**Sex grouping:** Females vs. Males & Geldings (geldings grouped with males due to low count).

**Aggregation rule:** Monthly average speed computed only when ≥30 starters in that age × distance × sex × month cell.

**Weight data:** Average weight carried per month per age group per sex group, computed where ≥30 starters.

**Statistics:** Descriptive only; no modelling. Comparisons made visually/narratively.

## Results (detailed — reproduce ALL numbers)

**Starter counts by distance and sex (turf):**

| Distance (m) | Males & Geldings | Females | Total  |
| ------------ | ---------------- | ------- | ------ |
| 1,200        | 22,024           | 24,227  | 46,251 |
| 1,400        | 8,761            | 8,310   | 17,071 |
| 1,600        | 16,006           | 11,009  | 27,015 |
| 1,800        | 21,514           | 13,100  | 34,614 |
| 2,000        | 22,013           | 8,962   | 30,975 |

**Starter counts by distance and sex (dirt):**

| Distance (m) | Males & Geldings | Females | Total  |
| ------------ | ---------------- | ------- | ------ |
| 1,000        | 6,217            | 6,264   | 12,481 |
| 1,200        | 18,017           | 13,835  | 31,852 |
| 1,400        | 13,560           | 7,036   | 20,596 |
| 1,700        | 20,912           | 9,848   | 30,760 |
| 1,800        | 23,431           | 8,245   | 31,676 |

Geldings constituted only 2.7% of turf starters and 2.8% of dirt starters.

Male:female ratio increases with distance: approximately 2:1 at 2,000 m turf and 1,800 m dirt.

**Weight carried by age and sex (JRA rules):**

| Gender/Age       | Until September (2-yr) | Oct–Dec (2-yr) | Until September (3-yr) | Oct–Dec (3-yr) |
| ---------------- | ---------------------- | -------------- | ---------------------- | -------------- |
| Males & Geldings | 54 kg                  | 55 kg          | 56 kg                  | 57 kg          |
| Females          | 54 kg                  | 55 kg          | —                      | —              |

(Females carry same weight as males at age 2 but have a weight allowance at age 3 in mixed races.)

**Age-speed trajectory (all distances, both surfaces, both sexes):**

1. **Age 2 (debut) – February of age 3:** Average speed increases slowly. Carry weight increases during this period for males, yet females (no weight change) show the same slow pattern — so growth dominates weight effect.
2. **March – September of age 3:** Continuous speed increase. Average weight carried decreases in June of age 3 (weight allowance for 3-year-olds in mixed-age races starts) but speed growth continues unabated.
3. **October – November of age 3:** Rapid speed jump. Authors attribute this to maiden horses (slower) retiring after maiden races end at age 3 in JRA, leaving only faster horses. Total starter counts drop sharply in this period.
4. **Age 3 (December) – first half of age 4:** Continuous increase; weight allowance stepwise removed; growth effect stronger than weight inhibition.
5. **Second half of age 4 onwards:** Speed plateaus and remains almost constant with little variation. No measurable decline with aging (contrary to Beyer Speed Figure data from the US which show a post-4.75 decline, but that US study selected only multi-year horses, creating survivorship bias).

**Absolute speed values (approximate, from figures, post-age-4.5 plateau):**

- Turf 1,200 m: ~17.2 m/s (fastest)
- Turf 1,400 m: ~17.1 m/s
- Turf 1,800 m: ~16.6 m/s
- Turf 2,000 m (slowest turf): ~16.5 m/s
- Dirt 1,000 m: ~16.5 m/s (fastest dirt)
- Dirt 1,200 m / 1,400 m: close to 1,000 m (some courses include ~100 m turf zone after start, inflating dirt-1,200 m speeds)
- Dirt 1,700 m / 1,800 m: ~15.3–15.7 m/s
- Turf speed difference (1,400 m minus 1,800 m): ~0.5 m/s
- Dirt speed difference (1,400 m minus 1,800 m): ~0.7 m/s (turns have greater effect on dirt)

**Turn effect:** 1,600 m turf → 1,800 m turf gap is larger than other consecutive distance pairs because most 1,600 m JRA courses use one turn while most 1,800 m use two turns. Similarly, dirt 1,400 m → 1,700 m gap (300 m difference) is larger than the 1,000–1,400 m gaps (400 m total range) due to the introduction of a second turn.

## Discussion & interpretation

The authors attribute the slow speed increase at age 2–3 not to carry-weight effects (females without weight increase show the same pattern) but to group composition: debut horses are untrained newcomers mixed with faster experienced horses, pulling the average down. The rapid October–November jump at age 3 is a statistical artefact of maiden-horse retirement rather than true physiological growth.

The post-4.5 speed plateau contradicts US Beyer Speed Figure data showing decline. The authors argue this is because the JRA analysis includes all starters (not just horses with multi-year records), so the retirement of declining horses keeps the population average stable — a survivor selection bias. This is an important calibration note for any age feature: an older JRA horse that is still racing is, by selection, a relatively fast one.

Average carry weight decreases slightly after age 4.5 due to handicap weight reductions for lower-performing horses in mixed-weight races, which also partially counteracts any true aging speed decline.

The comparison across surfaces reveals that dirt track limits top speed relative to turf (dirt 1,000 m ≈ turf 1,200 m in speed), and turning radius is smaller on dirt (inside of turf), making turn deceleration larger on dirt.

## Limitations

- Speed measure (distance / final time) does not control for field quality, pace, or race type (handicap vs. weight-for-age), only for track condition.
- Grouping males with geldings obscures any gelding-specific trajectory; gelding proportion is small (2.7–2.8%) so effect is minor.
- Monthly averages ≥30 starters rule means some age × distance cells are missing for very young (age 2, late year) and very old (age ≥7) horses.
- No individual-level modelling; inter-horse variance within each age-distance cell not reported.
- JRA dirt surface (mountain sand base + 9 cm loose sand) differs from US dirt and synthetic surfaces; surface-specific speed values not transferable.

## Feature-engineering notes for the model

- `age_months` — exact age in months at race date (= (race_date − birth_date) / 30.44) — source: birth records in JRA data — expected effect: nonlinear; positive until ~48 months (~4 yr), plateau thereafter — use spline or piecewise linear with knots at ~36 m (March age 3), ~42 m (Oct age 3), ~48 m (April age 4)
- `age_group_4cat` — categorical: {2yr, 3yr_early (Jan–Sep), 3yr_late (Oct–Dec), 4yr_plus} encoding the speed-phase breakpoints — derivation: encode from age_months — expected effect: 3yr_late and 4yr_plus are faster pools due to survivor selection
- `weight_deviation_kg` — actual carry weight minus JRA age/sex weight norm (Table 3 of paper) — source: JRA carry weight records, JRA weight-for-age tables — expected effect: negative (heavier → slower); estimated as secondary to growth
- `distance_turn_count` — integer: number of turns in race distance (1 vs. 2 as per JRA course layouts) — derivation: course/distance lookup — expected effect: each additional turn reduces average speed ~0.5 m/s (turf) or ~0.7 m/s (dirt)
- `surface_speed_baseline` — float: population average speed for this distance × surface × age group (use paper's Fig 1–2 values as prior) — derivation: look up from this paper's empirical table — expected effect: race context baseline; useful for speed deviation feature
- `sex_distance_interaction` — binary: is_female × is_long_distance (≥1800 m) — derivation: sex × distance — expected effect: negative for females at long distances (male:female ratio ≈ 2:1 at longest distances reflects lower female performance there)
- `survivor_selection_age_flag` — binary: age ≥ 4.5 yr — derivation: age_months ≥ 54 — expected effect: this flag indicates the horse is a survivor of retirement selection; population-level speed is stable but individual variance may be higher; do NOT use raw age effect beyond this threshold without survivor correction

Do NOT: apply a simple linear age feature without the nonlinear transformation; linear age will underestimate peak performance at age 4 and incorrectly penalise older horses who are still racing.

## Key references / follow-up leads

- Gramm M. and Marksteiner R. (2010) J. Equine Sci. 21:73–78 — US Thoroughbred age–performance (Beyer index), peak at 4.25–4.75 real age; individual-horse longitudinal
- Oki H., Sasaki Y. and Willham R.L. (1994) J. Anim. Breed. Genet. 111:128–137 — Japanese Thoroughbred racing time genetics; sex, age, weight effects (peak at age 5 in that study)
- Mota M.D., Abrahão A.R. and Oliveira H.N. (2005) J. Anim. Breed. Genet. 122:393–399 — Brazilian Thoroughbred racing time: peak at age 4
- Bugislaus A.E. et al. (2006) J. Anim. Breed. Genet. 123:239–246 — German Trotters random regression age-speed; peak at 4–6 yr
- Martin G.S., Strand E. and Kearney M.T. (1996) J. Am. Vet. Med. Assoc. 209:1900–1906 — statistical models for racing performance in Thoroughbreds
- Tan H. and Wilson A.M. (2011) Proc. Biol. Sci. 278:2105–2111 — grip and limb force limits to turning in competition horses
- Setterbo J.J. et al. (2009) Am. J. Vet. Res. 70:1220–1229 — hoof accelerations on dirt/synthetic/turf; propulsive force larger on turf
