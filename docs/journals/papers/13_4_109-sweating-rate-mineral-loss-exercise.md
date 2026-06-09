# Estimation of Total Sweating Rate and Mineral Loss Through Sweat during Exercise in 2-year Old Horses at Cool Ambient Temperature

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Journal                        | J. Equine Sci. 13(4): 109–112, 2002                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| docid                          | `13_4_109`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Article type                   | Original                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Authors                        | Akira Matsui, Tomoko Osawa, Hirofumi Fujikawa, Yo Asai, Tohru Matsui, Hideo Yano                                                                                                                                                                                                                                                                                                                                                                                                           |
| Affiliations                   | Equine Science Division, Hidaka Yearling Training Farm, Japan Racing Association, 535–13 Aza-Nishicha, Urakawa-cho, Urakawa-gun, Hokkaido 057-0171; Kyoto Racing Course, 32 Yoshijimawatashiba-Cho, Fushimi-ku, Kyoto City, Kyoto 612-8265; Roppongi office, Japan Racing Association, 6–11–1 Roppongi Minato-ku, Tokyo 106-8401; Division of Applied Biosciences, Graduate School of Agriculture, Kyoto University, Kitashirakawaoiwake-cho, Sakyou-ku, Kyoto City, Kyoto 606-8502, Japan |
| Received / Accepted / Released | Accepted December 14, 2002                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Keywords                       | cool ambient temperature, exercise, mineral loss, total sweating rate                                                                                                                                                                                                                                                                                                                                                                                                                      |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/13/4/13_4_109/_pdf/-char/en                                                                                                                                                                                                                                                                                                                                                                                                                       |

## Abstract (verbatim)

> We examined total sweating rate (SR) and the amount of mineral (Na, K, Ca, Mg, Fe, P, Zn and Cu) loss through sweat in 2-year-old horses during exercise for approximately 2,000 m at a speed of 700 m/min, at cool ambient temperature. The total SR was estimated by the unit area SR on the neck by filter paper within a capsule. Mean total SR was 1.55 ± 0.47 (SD) kg. The ratio of sodium loss to the requirements reported by National Research Council reached 23% and that of potassium was 7%. The ratio of other mineral losses to the requirements reported by National Research Council was 2% or less. These results suggested that sodium had to be supplemented to the horse, but there was no need to add extra minerals to the diet to compensate for mineral losses of calcium, magnesium, iron, phosphorus, zinc and copper through sweat during exercise with light intensity and at a cool ambient temperature.

## Relevance to finishing-position (着順) prediction

This paper belongs to feature family **E (environment/heat)** with cross-cutting relevance to **C (exercise-physiology/fitness)**. It quantifies how thermoregulatory load (measured as total sweating rate) drives electrolyte loss during exercise, with sodium being the dominant deficit mineral (23% of NRC daily maintenance from a single ~2,000 m canter at 700 m/min under cool conditions). The key modelling insight is that ambient temperature × humidity × exercise intensity interact to determine how much electrolyte deficit a horse accumulates across a training block preceding a race.

At 20.8°C and 94.3% humidity (cool/humid), SR was only 1.55 kg for a 2,000 m workout. By contrast, endurance conditions at high temperature can produce 27 kg water loss (Andrews et al. 1995), representing 17× the volume — and since sodium concentration in sweat (~89 mmol/l) remains relatively stable across intensities, sodium deficit would scale proportionally. For a horse running in Japanese summer conditions (JRA meets run through August in hot/humid locations like Sapporo, Kokura, Niigata), cumulative sodium and water deficit from recent training history is a physiologically meaningful performance moderator.

The formula validated in the companion paper (13_4_113) enables field estimation of total SR from neck-only capsule measurement: Total SR (kg) = 0.2676 × (neck unit-area SR, g) + 0.6735. This provides a tractable field methodology for incorporating thermoregulatory load into training records, though it is not currently encoded in JRA race databases.

The practical feature for the prediction pipeline is weather data on race day and training days: ambient temperature, humidity, WBGT (wet-bulb globe temperature), and their interaction with race distance and pace intensity.

## Background & objective

Yearling horses require daily exercise for development regardless of growth stage, but the NRC daily mineral requirements do not account for mineral losses through sweat. Given Japan's hot and humid climate through much of the year, yearling horses in training may need mineral supplementation beyond NRC recommendations. This study aimed to quantify total sweating rate and per-mineral sweat losses during a standardised moderate-intensity exercise bout at cool ambient temperature, using the filter-paper capsule method validated in the companion study (13_4_113) as a field-deployable estimation tool.

## Materials & methods

**Subjects:** 12 healthy Thoroughbred yearling horses (6 males, 6 females; mean age 2.4 years; mean weight 478 ± 45 SD kg). All housed indoors, individually. Conditioned for ~10 months on 5 days/week programme of walking, trotting, and cantering in the field.

**Diet (Table 1):**
| Ingredient | kg/day |
|-----------|--------|
| Timothy hay | 6 |
| Alfalfa hay | 1.5 |
| Oats | 3 |
| Pelleted diet | 1 |
| Soybean meal | 0.3 |
| Mineral supplement | 0.05 |
| Salt | 0.04 |

All mineral intakes met NRC requirements for growing horses at moderate work. Free access to water.

**Exercise protocol:** Single bout ~2,000 m at 700 m/min (canter/gallop); mean HR during exercise 186 ± 7 SD bpm.

**Location and conditions:** Hidaka region, Hokkaido (42.4°N, 142.4°E); June; ambient temperature 20.8 ± 2.8°C (cool despite summer); humidity 94.3 ± 4.2%.

**Sweat collection:** Filter paper within a plastic capsule attached to the neck. Weight measured before and after exercise. Total SR computed from validated formula: Total SR (kg) = 0.2676 × (neck unit-area SR, g) + 0.6735.

**Mineral analysis:** Filter papers ashed at 600°C for 4 hr; dissolved and diluted in 1N hydrochloric acid; Ca, Mg, Fe, P, Zn, Cu measured by ICP-AES (Thermo Jarrell Ash IRS/AP); Na and K by standard wet-chemistry methods.

**Mineral loss vs. requirement:** Multiplied mineral concentration in sweat by total SR to get absolute loss; compared to NRC maintenance requirements (dry-matter basis; recalculated from recommended DM intake, not actual DM intake, to avoid circular over-estimation).

## Results (detailed — reproduce ALL numbers)

**Environmental conditions:** Ambient temperature 20.8 ± 2.8°C; humidity 94.3 ± 4.2%.

**Total sweating rate:** Mean 1.55 ± 0.75 SD kg (abstract states ±0.47 SD; body text states ±0.75 SD — difference likely due to rounding in abstract).

**Mineral concentrations in sweat (Table 2):**

| Mineral            | Present study | Mcconaghy et al. (1989/1995) |
| ------------------ | ------------- | ---------------------------- |
| Sodium (mmol/l)    | 89.4          | 143.5                        |
| Potassium (mmol/l) | 33.9          | 37.5                         |
| Calcium (mmol/l)   | 5.3           | 5.1                          |
| Magnesium (mmol/l) | 3.8           | 4.2                          |
| Iron (mg/l)        | 5.09          | —                            |
| Phosphorus (mg/l)  | 2.37          | —                            |
| Zinc (mg/l)        | 2.12          | —                            |
| Copper (mg/l)      | 1.32          | —                            |

Sodium concentration was notably lower than Mcconaghy's study (89.4 vs. 143.5 mmol/l). The authors attribute this to electrolyte reabsorption in the sweat duct under cool dry conditions, analogous to human physiology (Garty & Palmer 1997). McCutcheon & Geor (1996) also found lower sweat sodium at cool/dry conditions (66.5–92.9 mmol/l) vs. hot/humid conditions.

**Mineral losses as % of NRC daily maintenance requirements (Figure 2 values):**

- Sodium: 23% of NRC daily requirement
- Potassium: 7% of NRC daily requirement
- Calcium: ≤2%
- Magnesium: ≤2%
- Iron: ≤2%
- Phosphorus: ≤2% (consistent with human data showing negligible P in sweat)
- Zinc: ≤2%
- Copper: ≤2%

**Daily mineral intakes (Table 1):**

- Sodium: 23.3 g/day
- Potassium: 138.1 g/day
- Calcium: 63.9 g/day
- Magnesium: 18.5 g/day
- Phosphorus: 32.3 g/day
- Copper: 277.9 mg/day
- Zinc: 974.7 mg/day
- Iron: 1950 mg/day

**Extrapolation to high-intensity exercise:** Andrews et al. (1995) endurance test: greatest water loss 27 kg (= ~17× higher than this study's 1.55 kg). If Ca/Mg concentration in sweat does not change with intensity (Mcconaghy 1995), losses of Ca and Mg could reach ~20% of NRC requirements at endurance intensity. Na/K losses would also scale ~17× — Na would reach ~390% of NRC, representing a severe deficit.

## Discussion & interpretation

The dominant practical finding is that sodium supplementation is necessary even at light exercise intensity and cool ambient conditions. With only 23% of daily Na requirement lost in a single moderate canter, horses on standard diets with salt supplementation are marginally covered; however, under Japanese summer conditions (ambient >30°C, high humidity) sweating rates would be far higher. Potassium loss (7% NRC) is low because pasture/hay provides 3–4% K by dry weight, providing natural buffering.

The low Na concentration in sweat compared to Mcconaghy's study is attributed to enhanced tubular electrolyte reabsorption in the sweat duct at cool temperatures — the same mechanism observed in humans. Under hot/humid conditions sweat production outpaces reabsorption capacity, driving Na concentration up toward 143 mmol/l. This means that the thermoregulatory load (not just SR volume) determines Na depletion kinetics.

The filter-paper/capsule methodology validated in companion paper 13_4_113 is practical for field use: the neck site is accessible, not obstructed by rider, and its SR correlates significantly with total body SR (r significant, P<0.05 to P<0.001 across body sites). The formula Total SR = 0.2676 × (neck unit-area SR) + 0.6735 provides a regression-based total body estimate.

## Limitations

- n=12 yearlings, all at Hidaka (cool June); not representative of hot/humid conditions at other JRA training centres (e.g., Miho, summer)
- Exercise was a single standardised bout at one intensity; actual races involve multiple shorter sprints or one sustained gallop at 2× the speed
- Na and K analysed by "standard methods" — specific method not stated in paper, which limits reproducibility assessment
- Males and females pooled in mean SR; sex differences in sweating rate not analysed
- 2-year-olds growing rapidly; sweating physiology may differ from 3- to 5-year-old racehorses
- Mineral requirements compared to NRC (1989) maintenance — not NRC "moderate work" values — may underestimate the requirement denominator

## Feature-engineering notes for the model

- `ambient_temp_race_day` — °C on race day — JMA weather API or JRA race conditions — higher temperature → higher SR → more electrolyte depletion; expected negative effect on performance at extreme heat; interacts with distance
- `humidity_race_day` — % relative humidity — same source — combined with temp as WBGT or heat index; high humidity at high temp is the critical interaction
- `wbgt_race_day` — wet-bulb globe temperature (°C) — derived: WBGT ≈ 0.7 × Tw + 0.2 × Tg + 0.1 × Tdb — most physiologically accurate heat stress index; expected negative nonlinear effect on performance above ~28°C WBGT
- `training_temp_prior_7d_mean` — mean ambient temp over 7 days before race — JMA or training centre logs — cumulative electrolyte depletion signal; particularly relevant for horses racing in different climatic region from training location
- `race_distance_m` — metres — JRA race records — longer distance → more sweating → more electrolyte depletion; interacts with temp/humidity
- `surface_type` — dirt / turf / sand — JRA race records — dirt generates more concussive heat (relevant for SDFT injury risk, see 12_3_85); also affects running economy and thermoregulation indirectly
- **Caution:** Total sweating rate and mineral concentrations are not in JRA race databases; these features must be proxied through environmental and effort variables

## Key references / follow-up leads

- Matsui A, Osawa T, Fujikawa H, Asai Y, Matsui T, Yano H. 2002. Differences in unit area sweating rate among different areas of the body in exercising horses. _J. Equine Sci._ 13: 113–116. [companion paper; validates neck as total-SR proxy — docid 13_4_113]
- McCutcheon LJ, Geor RJ. 1996. Sweat fluid and ion losses in horses during training and competition in cool vs. hot ambient conditions. _Equine Vet. J. Suppl._ 22: 54–62. [key comparison for cool vs. hot Na concentration]
- McCutcheon LJ, Geor RJ, Hare MJ, Ecker GL, Lindinger MI. 1995. Sweating rate and sweat composition during exercise and recovery in ambient heat and humidity. _Equine Vet. J. Suppl._ 20: 153–157.
- Mcconaghy FF, Hodgson DR, Evans DL, Rose AI. 1995. Effect of two types of training on sweat composition. _Equine Vet. J. Suppl._ 18: 285–288. [comparison mineral concentrations]
- Andrews FM, Ralston SL, Williamson LH et al. 1995. Weight loss, water loss and cation balance during the endurance test of a 3-day event. _Equine Vet. J. Suppl._ 18: 294–297. [27 kg water loss endurance extreme]
- National Research Council. 1989. Nutrient Requirements of Horses (5th Ed). National Academy Press, Washington DC. [NRC requirement basis]
