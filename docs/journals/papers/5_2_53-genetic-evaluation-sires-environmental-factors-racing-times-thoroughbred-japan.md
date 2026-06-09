# Genetic Evaluation of Sires and Environmental Factors Influencing Best Racing Times of Thoroughbred Horses in Japan

## Metadata

| Field                          | Value                                                                                                                                                                                                                                               |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 5(2): 53–58, 1994                                                                                                                                                                                                                    |
| docid                          | `5_2_53`                                                                                                                                                                                                                                            |
| Article type                   | Research Article                                                                                                                                                                                                                                    |
| Authors                        | Yasuyoshi Moritsu, Hiromi Funakoshi, Shun Ichikawa                                                                                                                                                                                                  |
| Affiliations                   | ¹Laboratory of Animal Breeding, Department of Dairy Science, Hokkaido College of Arts and Sciences, Ebetsu, Hokkaido 069, Japan; ²Laboratory of Animal Breeding, Department of Dairy Science, Rakuno Gakuen University, Ebetsu, Hokkaido 069, Japan |
| Received / Accepted / Released | Submitted April 14, 1994; accepted June 25, 1994                                                                                                                                                                                                    |
| Keywords                       | best racing times, environmental factors, heritability, sire evaluation, thoroughbred horse                                                                                                                                                         |
| PDF                            | https://www.jstage.jst.go.jp/article/jes1994/5/2/5_2_53/_pdf/-char/en                                                                                                                                                                               |

## Abstract (verbatim)

> The sire effect and the fixed effects of sex, training center, type of course, specific racetrack, horse age and track condition were evaluated regarding best racing times of thoroughbred horses. The records of best racing times at the Japan Racing Association were collected from the electronic racing book. The data set was edited so that each sire was required to have at least twenty progeny in the racing distances of 1200m and 1800m. The total numbers of sires and progeny were 34 and 1486 for 1200m and 34 and 1520 for 1800m. First, the least-squares analysis of variance was carried out by using the linear model which included the sire effect and the six fixed effects. Second, heritabilities for best racing times were calculated, and additionally sire evaluations were carried out by using the sire model of the BLUP method. As a result of analysis of variance, almost all effects included in the linear model were significant for both racing distances. The sire, type of course, horse age and interaction between the type of course and track condition had a highly significant effect on best racing times. Estimates of heritability were 0.11 and 0.09 for 1200 m and 1800 m, respectively. Rankings of sires' breeding values based on progeny records were quite different for the two racing distances. The Spearman rank correlation coefficient for these rankings was 0.350 (P<0.05).

## Relevance to finishing-position (着順) prediction

Feature family: **D (genetics/pedigree)** and **G (statistical modelling / fixed environmental effects)**.

This is among the most directly pipeline-relevant papers in the collection. It uses JRA racing records from 1985–1991 to quantify the magnitude, direction, and significance of each major environmental fixed effect on best racing times at two distances (1,200 m and 1,800 m). Every effect the paper tests — course type (turf/dirt), track condition, horse age, sex, training centre, specific racetrack — is a feature already present or readily derivable in the JRA/NAR race record database.

Key practical takeaways for feature engineering: (1) The **turf/dirt surface × track condition interaction** is significant and changes sign between surfaces — turf times are shorter under good than poor conditions, while dirt times are paradoxically shorter under poor ("heavy/bad") than good conditions. This non-linear interaction must be encoded explicitly, not as additive main effects. (2) **Horse age** has a monotonic significant effect at both distances — older horses (4-year-olds) are faster than 3-year-olds who are faster than 2-year-olds; a numeric age or binned-age feature is well-validated. (3) **Sex** matters at 1,800 m (males faster) but NOT at 1,200 m — a distance-stratified sex feature or a sex × distance interaction is warranted. (4) **Training centre** is significant at 1,200 m (Ritto center is faster) but not at 1,800 m — likely a proxy for track characteristics at that distance. (5) **Specific racetrack** effects differ between distances, confirming track-level intercept adjustments are necessary.

The heritability estimates (h² = 0.11 at 1,200 m; 0.09 at 1,800 m) set a quantitative prior: genetic factors (sire/pedigree) explain about 10% of best-time variance after adjusting for all environmental fixed effects. This is small but non-zero, supporting the use of pedigree-derived features as weak signals. Most critically, the near-zero cross-distance sire ranking correlation (Spearman r = 0.350) argues strongly against a single "speed index" for all distances — sprint (1,200 m) and middle-distance (1,800 m) aptitude are almost independent genetic traits, strongly motivating distance-stratified models or a pedigree × distance interaction feature.

## Background & objective

Racing time is the only direct speed measure and correlates with racing performance, but it is influenced by all circumstances of that race. Heritabilities for times are known to be low in the international literature [refs 3, 5, 6, 16], but for Japan the only prior report was Watanabe (1969) [ref 19] who estimated h² = 0.116 for adjusted 1,600 m times of 500 four-year-olds (1964–1966). The JRA had not previously been systematically modelled with a mixed model separating sire genetic effects from fixed environmental effects.

The paper's threefold objective: (1) evaluate fixed environmental effects on best racing time at 1,200 m and 1,800 m; (2) estimate heritability of best racing time by REML after adjusting for fixed effects; (3) compute BLUP sire estimated progeny differences (EPDs) and examine whether sire rankings are consistent across distances.

## Materials & methods

**Data source:** JRA racing records from the Electronic Racing Book of Horse Racing Records edited summer 1992 by TV Tokyo Station, covering all individual race records 1985–1991. Best racing time = lifetime-best time achieved for active horses at time of compilation.

**Data editing:** Only sires with ≥20 progeny at each distance included. Data confined to JRA registered horses.

**Sample sizes (Table 1):**

| Distance | Number of sires | Number of progeny | Distribution properties   |
| -------- | --------------- | ----------------- | ------------------------- |
| 1,200 m  | 34              | 1,486             | Right-skewed, leptokurtic |
| 1,800 m  | 34              | 1,520             | Right-skewed, leptokurtic |

CV was "almost the same" for both distances. Both distributions were right-skewed and more peaked than normal.

**Statistical model — linear mixed model:**

Y_ijklmnop = μ + S_i + A_j + B_k + C_l + D_m + E_n + F_o + (CF)\_lo + e_ijklmnop

Where:

- S_i = random sire effect (i = 1…34); assumed NIID(0, σ²s); all sires assumed unrelated
- A_j = fixed sex effect (j = 1, 2)
- B_k = fixed training centre effect (k = 1…3)
- C_l = fixed course type effect (l = 1, 2: turf vs. dirt)
- D_m = fixed specific racetrack effect (m = 1…10)
- E_n = fixed horse age effect (n = 1…3)
- F_o = fixed track condition effect (o = 1…4)
- (CF)\_lo = interaction between course type and track condition
- e = residual, NIID(0, σ²e)

**Effects excluded after testing:** Racing season (correlated with specific racetrack — Sapporo races only in summer, Tokyo not in summer) and load weight (concentrated at 53 and 55 kg with insufficient spread).

**Procedures:** SAS GLM procedure for ANOVA; SAS VARCOMP procedure (REML) for variance components; SAS MIXED procedure for BLUP. Heritability SE computed by the Swiger et al. method [ref 14]. Non-significant interactions dropped except (CF)\_lo.

**Heritability formula:** h² = 4σ²s / (σ²s + σ²e) based on intraclass correlation of paternal half-sibs.

**BLUP EPD:** One-half of sire's breeding value, expressed in seconds. Correlation of sire EPDs across distances evaluated by Spearman rank correlation.

## Results (detailed — reproduce ALL numbers)

**ANOVA significance (Table 2):**

| Effect                        | 1,200 m          | 1,800 m     |
| ----------------------------- | ---------------- | ----------- |
| Sire                          | \*\*\* (P<0.001) | \*\*\*      |
| Sex (A_j)                     | N.S.             | Significant |
| Training centre (B_k)         | Significant      | N.S.        |
| Course type (C_l)             | \*\*\*           | \*\*\*      |
| Specific racetrack (D_m)      | Significant      | Significant |
| Horse age (E_n)               | \*\*\*           | \*\*\*      |
| Track condition (F_o)         | Significant      | Significant |
| Course × Track condition (CF) | \*\*\*           | \*\*\*      |

**Least squares means — fixed effect directions (Table 3):**

_Course type:_

- Turf times shorter than dirt times at both distances (turf is faster surface)

_Track condition × course type interaction:_

- **Turf:** times shorter under good conditions than under poor/heavy conditions (better ground = faster times)
- **Dirt:** times shorter under poor/heavy conditions than under good conditions (counterintuitive — opposite sign to turf)

_Horse age:_

- Times become shorter (improve) as age increases: 4-year-olds (age group 3) fastest, 2-year-olds slowest; monotonic improvement at both distances

_Sex (1,800 m only):_

- Males (colts/horses) have shorter times than females (fillies/mares) at 1,800 m
- Sex effect NOT significant at 1,200 m

_Training centre (1,200 m only):_

- Ritto Training Center horses had shorter times than Miho or other centres at 1,200 m
- Training centre NOT significant at 1,800 m

_Specific racetrack:_

- Effects differed between the two distances; no consistent direction across all 10 tracks (track-specific intercept differences)

**Heritability estimates (Table 4):**

| Distance | h²   | SE                        |
| -------- | ---- | ------------------------- |
| 1,200 m  | 0.11 | (small, similar for both) |
| 1,800 m  | 0.09 | (small, similar for both) |

Both very small SEs ("almost the same value for both distances"). h² slightly higher at 1,200 m than 1,800 m. Consistent with prior literature (e.g., Watanabe 1969: h² = 0.116 for adjusted 1,600 m times; international reviews report similarly low values [refs 3, 5, 6, 16]).

**BLUP sire EPD correlation across distances (Figure 1):**

- Scatterplot of 34 sires' EPD at 1,200 m (x-axis) vs. 1,800 m (y-axis): no visible linear trend
- Spearman rank correlation r = 0.350 (P<0.05) — statistically significant but very low
- "Large differences occurred between the rankings of sires estimated for each racing distance"
- 5 of the 34 sires showed higher EPD values (better) at both distances — a small subset of "dual-aptitude" sires

## Discussion & interpretation

The direction of fixed effects is broadly consistent with practitioner expectations: turf is faster than dirt, older horses are faster than younger ones, males are faster than females at middle distance. The surprise is the track condition × surface interaction: on dirt, poor/heavy track is faster than good track. Authors offer no mechanistic explanation but note this is widely accepted by Japanese racing practitioners. The Ritto Training Center effect at 1,200 m (but not 1,800 m) may be attributable to the sloped training track at Ritto (known to develop specific muscular adaptations for sprint racing), and the absence of effect at 1,800 m may reflect that endurance conditioning is less venue-specific.

The sire × course interaction (which horsemen believe is important) was NOT significant in this model, indicating that sire genetic effect did not differ between turf and dirt courses after adjusting for all other fixed effects. Oki (1984) [ref 10] previously reported a birthplace-of-sire × course interaction, but the present model's fuller treatment of fixed effects may have absorbed this apparent interaction.

The low heritability (~0.10) matches international estimates for Thoroughbreds (Langlois 1980 [ref 5]: review of h² for racing ability; More O'Ferrall & Cunningham 1974 [ref 6]; Hintz 1980 [ref 3]). The implication is that most variance in best racing time is environmental (training quality, race conditions, nutrition, etc.) rather than genetic, consistent with the high significance of the fixed environmental effects.

The low cross-distance EPD correlation (r = 0.35) is interpreted as reflecting different body types and muscle fibre compositions for sprinters vs. milers — the authors note the Thoroughbred industry classifies horses into sprinter (<1,200 m), miler (1,600 m), and stayer (>2,200 m) types with different conformations. Moritsu et al. (in press at time of publication) [ref 8] also reported that body weight is more heritable than racing performance, consistent with body type (conformation) being more genetically determined than speed per se. The practical implication for breeding is that BLUP sire evaluations should be conducted separately for sprint and middle distances.

## Limitations

- All sires assumed unrelated in the model — an acknowledged statistical limitation; incorporating additive relationship matrix (animal model) would increase accuracy, especially for sires with few progeny
- Best time is a cumulative measure (lifetime best at time of compilation, 1992), mixing horses of different career lengths and ages — a horse with only 2 starts may have a very different best time than one with 30 starts
- Only two distances studied (1,200 m and 1,800 m); generalisability to sprint (<1,000 m), miler (1,600 m), or stayer (>2,000 m) distances untested
- Racing season and load weight excluded as fixed effects due to collinearity/concentration; their residual influence may bias other effect estimates
- Data from 1985–1991 only; current genetic structures and training methods may have changed
- Cross-distance EPD correlation significant (P<0.05) but very low (r = 0.35); the 5 "dual-aptitude" sires represent ~15% of the 34-sire sample — small for generalisations

## Feature-engineering notes for the model

- `surface` — turf (1) vs. dirt (0) — source: `race.course_type` field — expected effect: turf is faster; negative coefficient on finishing time; strong validated fixed effect; include as categorical feature
- `going_x_surface` — interaction of track condition × surface — source: `race.track_condition` × `race.course_type` — expected effect: on dirt, heavy/poor → SHORTER times (opposite to turf); must be encoded as interaction, NOT additive; one of the most important non-linear features validated here
- `horse_age_years` — horse age at time of race in years (integer or fractional) — source: `horse.birth_date` + `race.date` — expected effect: times improve monotonically from 2 to 4+ years; include as numeric or binned feature (2yo/3yo/4yo+)
- `sex_x_distance` — sex × distance interaction — source: `horse.sex` × `race.distance` — expected effect: males faster than females at ≥1,800 m; no significant sex effect at 1,200 m; encode as interaction term or distance-stratified sex dummy
- `training_centre` — Ritto vs. Miho vs. other JRA training centres — source: `horse.training_centre` — expected effect: Ritto advantage at 1,200 m; no effect at 1,800 m; include as categorical with distance interaction
- `racetrack_id` — specific racetrack (10 JRA venues) — source: `race.venue` — expected effect: racetrack-level intercept differences significant at both distances; encode as track embedding or dummy; effects differ by distance so consider distance × track interaction
- `sire_epd_sprint` — BLUP estimated progeny difference for the horse's sire at ~1,200 m — source: JBBA pedigree × JRA best-time records — expected effect: weak (~10% of variance explained by all sire variance); h² = 0.11; worthwhile as prior, not primary signal
- `sire_epd_middle` — BLUP EPD for sire at ~1,800 m — source: as above — expected effect: nearly independent from sprint EPD (r = 0.35); use distance-specific EPD, not a single speed index
- `distance_aptitude_class` — derived sire/horse classification: sprinter / miler / stayer (based on EPD pattern or known breed type) — source: JBBA pedigree + historical performance — expected effect: interaction with race distance; cross-distance r = 0.35 confirms this is informative
- **CAUTION**: Do not use a single "genetic speed index" across all distances — the near-zero cross-distance sire ranking correlation (r = 0.35) confirms sprint and middle-distance aptitude are almost independent; a pooled index will be dominated by noise

## Key references / follow-up leads

- Watanabe, Y. 1969. Timing as a measure of selection in Thoroughbred breeding. _Jpn. J. Zootech. Sci._ 40: 271–276 — foundational Japan-specific heritability estimate (h² = 0.116 for 1,600 m adjusted times)
- Oki, H. 1984. Genetic study of racing performance in the Thoroughbred horse 1. Differences in racing times between turf and dirt-course. _Jap. J. Zootech. Sci._ 55: 215–219 — sire birthplace × course type interaction; compare with present null finding
- Langlois, B. 1980. Heritability of racing ability in thoroughbreds — a review. _Livest. Prod. Sci._ 7: 591–605 — international heritability review
- Ojala, M., Van Vleck, L.D., and Quass, R.L. 1987. Factors influencing best annual racing time in Finnish horses. _J. Anim. Sci._ 64: 109–116 — parallel study for Finnish trotters; best annual time validated as most useful sire-evaluation measure
- Moritsu, Y., Ohtsuka, T., and Ichikawa, S. 1994. Heritability of body weight for 3-year-old Thoroughbred horses. (in press at time of publication) — body weight more heritable than racing performance; conformation feature motivation
- Klemetsdal, G. 1990. Breeding for performance in horses — a review. _Proc. 4th World Congress on Genetics Applied to Livestock Production_ 184–193, Edinburgh — review of BLUP/animal model adoption in horse breeding
- Buttram, S.T., Willham, R.L., Wilson, D.E., and Heird, J.C. 1988. Genetics of racing performance in the American Quarter Horse 1. _J. Anim. Sci._ 66: 2791–2799 — analogous Quarter Horse study for comparison
