# Relationship between the Total Sweating Rate and the Unit Area Sweating Rate at the Neck during Exercise in Horses

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                    |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Journal                        | J. Equine Sci. 14(1): 1–3, 2003                                                                                                                                                                                                                                          |
| docid                          | `14_1_1`                                                                                                                                                                                                                                                                 |
| Article type                   | Original                                                                                                                                                                                                                                                                 |
| Authors                        | Akira MATSUI, Tomoko OSAWA, Hirofumi FUJIKAWA, Yo ASAI, Tohru MATSUI, Hideo YANO                                                                                                                                                                                         |
| Affiliations                   | Equine Science Division, Hidaka Yearling Training Farm, JRA, Urakawa, Hokkaido; Kyoto Racing Course, JRA, Fushimi-ku, Kyoto; Roppongi Office, JRA, Minato-ku, Tokyo; Division of Applied Biosciences, Graduate School of Agriculture, Kyoto University, Sakyou-ku, Kyoto |
| Received / Accepted / Released | Accepted January 31, 2003                                                                                                                                                                                                                                                |
| Keywords                       | estimation, horse garments, total sweating rate                                                                                                                                                                                                                          |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/14/1/14_1_1/_pdf/-char/en                                                                                                                                                                                                       |

## Abstract (verbatim)

> Our previous study suggested that measurement of the unit area sweating rate (SR) at the neck in horses was suitable for estimation of the total sweating rate of a horse during exercise. We developed a special garment made of high-absorption polymer sheets to be fitted on the horses for measuring the total SR, and examined the relationship between the unit area SR at the neck and the total SR in a horse. The correlation between the total SR and the unit area SR at the neck was found to be highly significant (r=0.9254, P<0.01). Total sweating rate (kg) = 0.2676 X (unit area sweating rate at the neck (g)) + 0.6735 (r² = 0.8563 P<0.01). We considered that the total SR could be calculated from the unit area SR at the neck based on the results of our previous study, and derived the aforementioned formula.

## Relevance to finishing-position (着順) prediction

Feature family: **E (environment/heat-stress / thermoregulation fitness)**. This paper validates a regression formula for estimating whole-body sweat loss from a single neck measurement, confirming that neck sweating rate explains 86% of the variance in total sweat output (r=0.9254, r²=0.8563). The immediate practical value for race-outcome modelling is not direct (no horse-to-horse performance comparison is made), but two indirect pathways are meaningful.

First, total sweat loss is a proxy for thermoregulatory demand during exercise. Ambient temperature and humidity drive sweat loss: horses cantered at 32.7°C/63% RH (August) lost 2.07 ± 0.87 kg vs 1.92 ± 0.27 kg at 14.0°C/73.5% RH (November). If raceday WBGT or wet-bulb temperature is included as a feature, this paper provides the mechanistic rationale: high ambient heat increases whole-body fluid loss, potentially impairing cardiovascular function and pace-holding in longer races.

Second, the paper demonstrates that inter-individual variation in sweat output is substantial (SD of 0.87 kg in August; coefficient of variation ~42%), implying that horses differ in their thermoregulatory efficiency. This individual variation is consistent with heat-tolerance being a performance-relevant trait in hot-weather racing. Combined with body weight loss features (pre/post-race 馬体重 change from JRA/NAR records), the magnitude of sweat-related weight loss is an observable, actionable signal.

The interaction between exercise intensity and ambient conditions is also confirmed: the horses had to work harder (65% HR_max, 5.3 m/s) in November's cooler weather to achieve the same heart-rate target as in August (60% HR_max, 5.0 m/s), yet produced slightly less total sweat. This implies temperature-correction is necessary when comparing training-log workout speeds across seasons — directly relevant to normalising time-trial (タイム) features.

## Background & objective

Horse sweat contains high concentrations of electrolytes; prolonged sweating causes mineral loss requiring replacement. Accurately quantifying total sweat rate during exercise is needed to compute mineral budgets. Prior methods — full-body fluid balance from body-weight difference minus respiratory water loss — require a flow system attached to the horse's nose (impractical in the field). Kingston et al. (1997) proposed extrapolating local surface sweat rate to total sweat rate, but Matsui et al. (2002) showed that unit-area SR varies significantly across body sites. However, the same prior study showed that the neck is representative of the average of four major body-surface sites (shoulder, back, loin, flank). The objective of the present paper is therefore to directly validate a garment-based total SR measurement and establish a regression equation from neck SR alone.

## Materials & methods

**Subjects:** 3 healthy Thoroughbred horses (2 males, 1 female; 3.5–5.5 yr; 475–516 kg body weight). Maintained on grass hay, oats, mineral supplement; free water access; individually stalled.

**Conditioning:** 3 months on treadmill (Mustang-2200, Kagra, Switzerland), 5 days/week, walk/trot/canter programme before experimental trials.

**Exercise protocol (Fig. 1 in paper):**

- August session: canter at mean 60% HR_max (5.0 m/s, 5 min); ambient 32.7°C, 63.3% RH
- November session: canter at mean 65% HR_max (5.3 m/s, 6 min); ambient 14.0°C, 73.5% RH
- Intensity differed because higher ambient temperature/humidity in August precluded the same absolute intensity as November

**Sweat collection — total SR:**
Special horse garment (Newsofurapiren polymer sheeting, Taketora, Japan) covering entire body held close by elastic. Garment weighed before and after exercise; weight difference = total SR. Preliminary validation: the polymer absorbed 99.3% of water applied to the body surface; evaporation from the garment was 1.7%/25 min (negligible).

**Sweat collection — unit-area neck SR:**
Filter paper layers placed on the neck region (same method as Matsui et al. 2002).

**Body-weight reference method:** Total body water loss estimated as: total body water loss (kg) = 0.85 × body weight difference (kg) (from Heilemann et al. 1990), yielding an independent estimate of ~2.4 kg for comparison.

**Statistics:** Pearson correlation and linear regression between unit-area neck SR (g) and total garment SR (kg); both August and November data pooled (n = pooled observations from 3 horses × 2 seasons).

## Results (detailed — reproduce ALL numbers)

| Season        | Ambient conditions | Exercise intensity         | Total SR (kg) mean ± SD |
| ------------- | ------------------ | -------------------------- | ----------------------- |
| August        | 32.7°C, 63.3% RH   | 60% HR_max, 5.0 m/s, 5 min | 2.07 ± 0.87             |
| November      | 14.0°C, 73.5% RH   | 65% HR_max, 5.3 m/s, 6 min | 1.92 ± 0.27             |
| Combined mean | —                  | —                          | 1.99                    |

**Regression (pooled across both seasons, 3 horses):**

- Correlation: r = 0.9254, P < 0.01
- Coefficient of determination: r² = 0.8563, P < 0.01
- Equation: Y = 0.2676X + 0.6735 (Y = total SR in kg; X = unit-area neck SR in g)

**Garment validation:**

- Water absorption efficiency: 99.3% (polymer sheeting retained nearly all sweat)
- Evaporation rate: 1.7%/25 min (negligible; can be ignored in field conditions)

**Reference comparison:**

- Body-weight-difference method (with respiratory correction) yielded ~2.4 kg total body water loss, compared to ~1.99 kg garment estimate; the difference attributed to respiratory water loss (~17% of total fluid loss at moderate intensity), confirming the garment method is conservative but valid for sweat specifically.

## Discussion & interpretation

The authors confirmed that neck SR is a valid proxy for whole-body sweat rate (r²=0.86), consistent with the prior finding that neck SR is representative of the average across four body sites. The formula Y = 0.2676X + 0.6735 enables total sweat rate estimation from a single easily accessible measurement point, making field application feasible.

The difference in exercise intensity between seasons (60% vs 65% HR_max) was imposed by the thermal environment: at 32.7°C, horses could not sustain 65% HR_max without over-stressing thermoregulation. This observation is important — it means that fixed-speed training protocols produce different physiological loads in different seasons, complicating direct comparison of training data across months without temperature-humidity correction.

Kingston et al. (1995) had previously shown that local SR on the thorax could estimate total SR during prolonged steady-state exercise at 40% VO₂max. The present study extends this to neck SR and to moderate-to-high intensities (60–65% HR_max), broadening practical applicability.

## Limitations

- Very small sample (n=3 horses). The r²=0.86 must be interpreted cautiously; individual variation in the August dataset is high (SD = 0.87 kg, CV ≈ 42%), reflecting genuine between-horse differences in thermoregulatory response.
- Only 2 temperature/humidity conditions tested; the formula's validity across a wider range of WBGT conditions (e.g., 40°C+, or cold winter) is not established.
- Only canter exercise tested; the relationship at racing intensities (90%+ HR_max) is extrapolated, not measured.
- Horses were conditioned animals; untrained or elite horses may have different neck-to-total SR ratios.
- The body-weight-difference reference gave a higher estimate (~2.4 kg vs ~2.0 kg), suggesting the garment method may underestimate true total fluid loss (respiratory losses not captured by the garment).

## Feature-engineering notes for the model

- `race_day_temperature` — dry bulb temperature (°C) at race venue on race day — source: JRA/NAR weather data or JMA records — positive effect on sweat loss, expected negative effect on performance especially in longer races (≥1800m) and for horses with history of heat-related performance degradation
- `race_day_humidity` — relative humidity (%) — same source — interacts with temperature; WBGT = f(T_dry, T_wet) is preferred composite feature
- `race_day_WBGT` — wet-bulb globe temperature (°C), or approximation — combines temperature and humidity into single heat-stress index — expected negative relationship with finishing position quality in longer-distance races, nonlinear (threshold near 28°C for performance impairment)
- `pre_post_race_weight_change` — difference in 馬体重 before vs after race (kg) — available in JRA/NAR 馬体重 records — proxy for total sweat loss during race; larger negative values indicate higher thermoregulatory burden; note: also captures urination, defecation — noisy proxy
- `season_month` — month or season flag — interacts with baseline fitness-measurement comparability across time of year; temperature correction needed when comparing training-log times across seasons
- **Caution:** Neck SR measurement is not available from standard JRA/NAR records. The formula is primarily useful for mechanistic understanding; ambient temperature/humidity features are the practical operationalisation.

## Key references / follow-up leads

- Kingston, J.K., Geor, R.J., and McCutcheon, L.J. 1997. Use of dew-point hygrometer, direct sweat collection, and measurement of body water losses to determine sweating rates in exercising horses. _Am. J. Vet. Res._ 58: 175–181. (original local-SR extrapolation method; companion to this paper)
- Matsui, A., Osawa, T., Fujikawa, H., Asai, Y., Matsui, T., and Yano, H. 2002. Differences in the body sites of the unit area sweating rate in exercising horses. _J. Equine Sci._ 13: 113–116. (prior paper establishing neck SR as representative; key methodological basis)
- Hodgson, D.R., McCutcheon, L.J., Byrd, S.K., Brown, W.S., Brengelmann, G.L., and Gollnick, P.D. 1993. Dissipation of metabolic heat in the horse during exercise. _J. Appl. Physiol._ 74: 1161–1170. (earlier heat dissipation framework)
- Heilemann, M., Woakes, A.J., and Snow, D.H. 1990. Investigations on the respiratory water loss in horses at rest and during exercise. In Meyer H (ed): Contributions to Water and Mineral Metabolism of the Horse. (source of the 0.85× body weight correction factor)
- Andrews, F.M. et al. 1995. Weight loss, water loss and cation balance during the endurance test of a 3-day event. _Equine Vet. J. Suppl._ 18: 294–297. (alternative sweat quantification method)
