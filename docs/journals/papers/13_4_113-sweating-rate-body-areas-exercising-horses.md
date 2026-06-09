# Differences in Unit Area Sweating Rate among Different Areas of the Body in Exercising Horses

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Journal                        | J. Equine Sci. 13(4): 113–116, 2002                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| docid                          | `13_4_113`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Article type                   | Original                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Authors                        | Akira Matsui, Tomoko Osawa, Hirofumi Fujikawa, Yo Asai, Tohru Matsui, Hideo Yano                                                                                                                                                                                                                                                                                                                                                                                                           |
| Affiliations                   | Equine Science Division, Hidaka Yearling Training Farm, Japan Racing Association, 535-13 Aza-Nishicha, Urakawa-cho, Urakawa-gun, Hokkaido 057-0171; Kyoto Racing Course, 32 Yoshijimawatashiba-Cho, Fushimi-ku, Kyoto City, Kyoto 612-8265; Roppongi office, Japan Racing Association, 6–11–1 Roppongi Minato-ku, Tokyo 106-8401; Division of Applied Biosciences, Graduate School of Agriculture, Kyoto University, Kitashirakawaoiwake-cho, Sakyou-ku, Kyoto City, Kyoto 606-8502, Japan |
| Received / Accepted / Released | Accepted January 31, 2003                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Keywords                       | exercise, neck, sweat, total sweating rate                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/13/4/13_4_113/_pdf/-char/en                                                                                                                                                                                                                                                                                                                                                                                                                       |

## Abstract (verbatim)

> The objective of this preliminary study, conducted before a study to determine the total sweating rate (SR) of a horse during exercise, was to estimate the differences in the SR through unit area among different areas of the body of a horse during exercise, and to determine the existence of correlations, if any, among the values. The unit area sweating rate on the neck, shoulders, back, loins and flanks was monitored using filter paper layers fitted within a plastic capsule. The values of the SR through unit area on the neck and shoulders were found to be higher than those in other areas of the body. The varying sweat rates in different areas of the body indicates that it is difficult to estimate the total SR of an exercising horse by multiplying the SR through unit area in one area of the body by the total body surface area. On the other hand, the SR through unit area on the neck was correlated significantly with that in other areas of the body, suggesting the potential possibility of estimating the total SR from the SR through unit area determined on the neck region. In addition, measurement of the SR on the neck was also thought to be appropriate from the point of view of ensuring absence of interference with the rider.

## Relevance to finishing-position (着順) prediction

This paper belongs to feature family **E (environment/heat)** with methodological implications for **C (exercise-physiology/fitness)**. It is the foundational validation study establishing that neck sweating rate is a reliable proxy for whole-body thermoregulatory load in exercising horses. The paper's primary contribution to the prediction pipeline is methodological: it justifies using single-site (neck) sweat measurements in field conditions as an integrative index of total sweating rate, which in turn quantifies the thermoregulatory stress a horse experiences during training.

The critical quantitative finding is the ~2.4× variation in unit-area sweat rate across body sites (shoulder 7.47 g vs. loins 3.13 g), confirming that any attempt to extrapolate from a single non-neck site would introduce substantial bias. The neck site (6.84 g) was significantly correlated with all four other sites and with their sum, making it the recommended field measurement site. The companion paper (13_4_109) uses this calibration to build the total SR formula: Total SR (kg) = 0.2676 × (neck unit-area SR, g) + 0.6735.

For the finishing-position model, this paper does not provide directly predictive features but validates the measurement chain from neck sweat → total SR → electrolyte deficit → performance impact. If any JRA training centre or veterinary system records post-exercise neck sweat data, those measurements can be treated as total SR proxies and converted to electrolyte deficit estimates using the mineral concentrations from 13_4_109.

The finding that shoulder SR (~2.4× loins) is highest reflects higher sweat gland density in areas subject to saddle/rider contact and muscular heat production. This anatomical observation also indirectly supports the use of body-surface-area-normalised weight (馬体重/body size) as a thermoregulation capacity feature: larger surface area horses dissipate heat more efficiently at moderate exercise intensities.

## Background & objective

Existing methods for total SR measurement (body weight difference before/after exercise minus respiratory moisture loss) are impractical in field conditions because the flow-system for respiratory moisture measurement must be attached to the horse and interferes with riding. The SR through unit area also varies across body sites due to differences in sweat gland density, making simple single-site × total body surface area extrapolation unreliable. This study aimed to: (1) quantify differences in unit-area SR across five body sites; (2) determine which site correlates best with other sites; and (3) identify a practical field measurement site that does not interfere with rider.

## Materials & methods

**Subjects:** 2 healthy Thoroughbreds (1 male, 1 female; mean age 4.5 years; body weights 476 and 453 kg respectively). Maintained on grass hay, oats, and mineral supplement with free water access. Housed individually. Conditioned for 3 months by 5 days/week walking, trotting, cantering on treadmill (Mustang-2200, Kagra, Switzerland).

**Measurement sites (5 sites simultaneously):**

1. Neck
2. Shoulder
3. Back
4. Loins
5. Flank

**Sweat collection method:** Skin areas ~5 cm diameter clipped, shaved, and washed with distilled water. 50 sheets of Whatman No. 41 filter paper (5 cm diameter) placed in a plastic capsule held by wide adhesive tape across each body site to prevent movement and evaporation. Weight measured before and after exercise; weight difference = unit-area SR (g) at that site.

**Standardised exercise test (SET) × 3 per horse:** Different intensities to avoid intensity-dependent SR bias. Treadmill protocol:

- Level trot at 3 m/s for 5 min
- Canter at 3% incline at either 5.0, 5.5, or 6.0 m/s for 5 min (each SET used one speed; ~80–90% HRmax)
- Level walk at 1.5 m/s for 10 min recovery

**Environmental conditions:** Mean ambient temperature 28.3 ± 1.9°C SD; humidity 42.2 ± 25.1% SD.

**Statistical analysis:** Student's t-test to compare SR between body sites. Pearson correlation between sites. All values expressed as mean ± SE. Significance threshold P<0.05.

## Results (detailed — reproduce ALL numbers)

**Unit-area sweating rates by body site (Figure 5):**

| Body site | Mean unit-area SR (g) |
| --------- | --------------------- |
| Shoulder  | 7.47                  |
| Neck      | 6.84                  |
| Flank     | 4.91                  |
| Back      | 4.70                  |
| Loins     | 3.13                  |

Shoulder and neck SR were significantly higher than SR at all other sites (P<0.05). Shoulder SR was approximately 2.6× loin SR (7.47 / 3.13 = 2.39, text states ~2.6×, rounding from graph values). The selection of measurement site therefore significantly affects total SR determination.

**Inter-site correlations (Table 1):**

|          | Neck | Shoulder | Back   | Loin | Flank  |
| -------- | ---- | -------- | ------ | ---- | ------ |
| Neck     | —    | \*       | \*\*   | \*\* | \*\*   |
| Shoulder | \*   | —        | NS     | NS   | —      |
| Back     | —    | —        | —      | \*   | \*\*\* |
| Loin     | —    | —        | \*     | —    | —      |
| Flank    | \*\* | —        | \*\*\* | —    | —      |

(**\*P<0.001, **P<0.01, \*P<0.05, NS=not significant)

Neck was significantly correlated with all four other sites (P<0.05 to P<0.01). Neck SR was also significantly correlated with the sum of SR at the other four sites (Shoulder + Back + Loins + Flank) — Figure 6 shows this relationship (regression shown but r and equation values not stated numerically in text beyond "significant").

Loin SR was also significantly correlated with each other site, but neck was selected over loins as the recommended site because: (1) neck correlates with all sites including the sum; (2) neck position does not interfere with rider; (3) neck does not move excessively during exercise (unlike flank or loins with respiratory/locomotor motion).

## Discussion & interpretation

The variation in unit-area SR across body sites (shoulder 7.47 g vs. loins 3.13 g) reflects regional differences in sweat gland density. The shoulder and neck regions, which are areas of high muscular activity and under rider-contact heating, have higher gland density. The loins and back, which are less metabolically active per unit area during canter, have lower gland density.

Because simple multiplication of single-site SR by total body surface area would yield highly biased estimates depending on which site is chosen, the authors correctly conclude that only a calibrated regression (neck SR → total SR) is a valid field method. The formula from the companion paper (13_4_109) — Total SR = 0.2676 × neck SR + 0.6735 — provides this calibration.

The study used only 2 horses, which the authors call a "preliminary investigation." The 3 repeated SETs per horse at different intensities were designed to capture the range of SR values across exercise intensities while minimising individual horse effects on the correlation analysis, but the sample size limits statistical power. The companion paper (13_4_109) with n=12 provides more robust estimates of mean total SR under specific conditions.

Practical implication for racing: horses returning to work after a lay-off may have reduced sweat gland responsiveness (detraining effect on thermoregulatory fitness), which this methodology could capture via reduced neck SR at a standardised exercise intensity. This is an indirect fitness indicator.

## Limitations

- n=2 horses only (1 male, 1 female); individual variation cannot be separated from site variation
- Only one exercise type (treadmill canter); relationship between sites might differ under actual race conditions (higher speed, shorter duration)
- Exercise intensity effect on SR not examined — the 3 SETs used different intensities specifically to prevent biasing the correlation analysis, not to characterise the intensity-SR relationship
- Humidity varied substantially (42.2 ± 25.1% SD) across the sessions, which could affect individual data points
- Correlation between neck SR and total body SR (not just the sum of the 4 other sites) requires the formula from 13_4_109, which was validated on a separate larger sample

## Feature-engineering notes for the model

- `neck_sweat_rate_g` — unit-area SR at neck (g per 5 cm² area) — measured via filter paper capsule at training centre post-exercise — primary field measurement enabling total SR estimation; not currently in JRA race databases
- `total_SR_estimated_kg` — estimated total body sweating rate (kg) — derived: 0.2676 × neck_sweat_rate_g + 0.6735 — thermoregulatory load proxy; expected negative association with next-race performance if accumulated deficit not addressed
- `shoulder_neck_ratio` — shoulder SR / neck SR — derived from dual-site measurement — could flag horses with atypical sweat distribution (unusual thermoregulatory physiology); not routinely measured
- **Caution:** This paper validates a measurement methodology but does not provide predictive features directly. The actionable pipeline feature is `ambient_temp × race_distance` as a proxy for total thermoregulatory load when direct sweat measurements are unavailable.
- **Body weight** (馬体重) interacts with surface area and metabolic heat production: heavier horses have proportionally less surface area per kg, reducing heat dissipation efficiency; 馬体重 increase signals fitness but also increased metabolic heat per exercise bout

## Key references / follow-up leads

- Matsui A, Osawa T, Fujikawa H, Asai Y, Matsui T, Yano H. 2002. Estimation of total sweating rate and mineral loss through sweat during exercise in 2-year-old horses at cool ambient temperature. _J. Equine Sci._ 13: 109–112. [companion paper using this calibration — docid 13_4_109]
- Hodgson DR, McCutcheon LJ, Byrd SK, Brown WS, Brengelmann GL, Gollmick PD. 1993. Dissipation of metabolic heat in the horse during exercise. _J. Appl. Physiol._ 74: 1161–1170. [foundational metabolic heat dissipation in exercise]
- Kingston JK, Geor RJ, McCutcheon LJ. 1997. Use of dew-point hygrometer, direct sweat collection, and measurement of body water losses to determine sweating rates in exercising horses. _Am. J. Vet. Res._ 58: 175–181. [alternative sweat measurement methods]
- Mcconaghy FF, Hodgson DR, Evans DL, Rose AI. 1995. Effect of two types of training on sweat composition. _Equine Vet. J. Suppl._ 18: 285–288.
- Johnson KG. 1970. Sweating rate and the electrolyte content of skin secretions of Bos taurus and Bos indicus cross-bred cows. _J. Agric. Sci._ 75: 397–402. [bovine precedent for capsule method]
