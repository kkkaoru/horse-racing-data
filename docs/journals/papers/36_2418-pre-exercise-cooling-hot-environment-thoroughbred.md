# Effects of Pre-Exercise Cooling in Hot Environments on Performance and Physiological Responses in Thoroughbred Horses

## Metadata

| Field                          | Value                                                                                                 |
| ------------------------------ | ----------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. **36**(1): 19–23, 2025                                                                 |
| docid                          | `36_2418`                                                                                             |
| Article type                   | Full Paper                                                                                            |
| Authors                        | Hajime OHMURA, Yusaku EBISUDA, Yuji TAKAHASHI, Kazutaka MUKAI                                         |
| Affiliations                   | Sports Science Division, Equine Research Institute, Japan Racing Association, Tochigi 329-0412, Japan |
| Received / Accepted / Released | 2024-10-21 / 2025-01-08 / —                                                                           |
| Keywords                       | body weight loss, heat stress, pulmonary artery temperature                                           |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/36/1/36_2418/_pdf/-char/en                                   |

## Abstract (verbatim)

> Pre-exercise cooling may prevent exertional heat illness in horses. We hypothesized that pre-exercise cooling before warm-up in a hot environment would not affect performance but would mitigate reductions in body weight and increases in body temperature following exercise. Six trained Thoroughbred horses were studied using a randomized, crossover design with three pre-treatments: 30-min walk on a treadmill at 1.7 m/sec (WALK), 30 min of standing (REST), and a 10-min pre-cooling shower at 26.2 ± 0.8°C (SHOWER). All horses underwent each pre-treatment, followed by a warm-up and main exercise in a hot environment (wet-bulb globe temperature: 32–33°C). After warming up by cantering at 10.0 m/sec for 30 sec, horses exercised on a treadmill with a 6% incline and a speed eliciting exhaustion within 2 min, which was approximately 115% V̇O2max (relative intensity; 13.5–14.3 m/sec). Run time to exhaustion was recorded, with body weight measured before pre-treatment and after main exercise to calculate weight loss. Heart rate was measured from before pre-treatment to after the main exercise. Plasma lactate concentration (Lac) and pulmonary arterial temperature (a measure of body temperature) were assessed before and after pre-treatment and after the warm-up and main exercise. Weight loss in SHOWER was significantly reduced compared with the other treatments. Pulmonary artery temperatures in WALK after pre-treatment were significantly higher than in the other treatments. No significant differences were observed in heart rate, Lac, or run time. These results suggest that pre-cooling mitigates reductions in body weight and increases in body temperature without affecting performance.

## Relevance to finishing-position (着順) prediction

**Feature family E (environment/heat) and F (body weight).** This study tests whether a pre-race cooling intervention alters a short, high-intensity treadmill performance (~2 min at ~115% V̇O2max, 32–33°C WBGT) in JRA Thoroughbreds. Two key findings feed the prediction pipeline:

1. **Body-weight delta is thermally mediated on hot days.** SHOWER reduced weight loss from −7.5 ± 1.5 kg (WALK) and −5.8 ± 2.0 kg (REST) to −1.8 ± 1.1 kg. On race days with WBGT ≥ 28°C, the observed 馬体重 difference between the last listed weight and actual race-day weight may reflect thermal sweat loss of 4–6 kg rather than true conditioning change. This confounds the widely used "体重変化" feature; a WBGT × weight-delta interaction term would partially de-noise it.

2. **Sprint-distance performance (≤2 min) was NOT improved by pre-cooling.** Run times (WALK: 72.0 ± 18.5 s, REST: 73.7 ± 17.2 s, SHOWER: 69.1 ± 22.0 s) were statistically indistinguishable, with HR and lactate also showing no treatment effect. Therefore, a pre-race cooling regime does not by itself predict a speed advantage; its value is heat-illness protection.

3. **Walking warm-up raised core temperature without performance gain.** Pulmonary artery temperature after WALK pre-treatment (39.0 ± 0.3°C) significantly exceeded REST (37.6 ± 0.4°C) and SHOWER (37.7 ± 0.4°C), and remained elevated through warm-up (39.8 vs. 38.5/38.4°C) and main exercise (41.8 vs. 40.7/40.4°C). Walking in heat as a pre-exercise activity is physiologically counterproductive; if pre-race routine data were available, WALK-in-heat observations could be flagged as elevated EHI risk.

4. **Rectal temperature (T_REC) did not track the pulmonary artery temperature (T_PA) differences**, confirming that routinely measured T_REC (e.g., pre-race veterinary checks) is an insensitive proxy for true core temperature change during short intense exercise.

Net: standalone predictive value is low, but this study grounds the heat/WBGT/body-weight interaction feature family and points to ref [14] (Takahashi & Takahashi 2020, Equine Vet. J.) for EHI risk-factor modelling.

## Background & objective

Thoroughbred racehorses compete in hot humid conditions in Japanese summer (WBGT routinely 28–33°C). Pre-exercise cooling has been studied in human athletes and in eventing horses (moderate intensity), but its effect on short-duration, supramaximal sprint performance (as in flat racing) was unknown. A key concern is that cooling lowers muscle temperature, potentially reducing power output and negating the thermoregulatory benefit. Authors aimed to determine whether 10-min shower cooling before a standardized warm-up: (a) changes run time to exhaustion at ~115% V̇O2max, and (b) prevents body weight loss and core temperature rise compared with walking or resting pre-treatments.

## Materials & methods

**Animals:** 6 clinically healthy Thoroughbreds from the JRA Equine Research Institute herd (3 geldings, 3 females; mean age 5.8 ± 1.9 years SD; mean BW 492 ± 38 kg). Pre-study training: treadmill moderate-intensity exercise (1.7, 4.0, 7.0, 10.0 m/s × 2 min each at 6% incline) twice/week for 4 weeks, plus 1 hr/day walking 5 days/week.

**Instrumentation:** Swan-Ganz catheter (131F7, Edwards) via 8-Fr introducer in right jugular vein, tip positioned in pulmonary artery; pressure confirmed by tip pressure transducer; pulmonary artery temperature (T_PA) recorded via cardiac output computer (COM2, Baxter). 14-G jugular catheter for blood collection. HR monitor (Polar S810). WBGT portable monitor (WBGT-213B, Kyoto Electronics). Treadmill (SÄTO AB) for main exercise; separate treadmill (Mustang 2200, Ansorix) for WALK pre-treatment.

**Protocol:** Randomized crossover, 1 trial/week. Hot humid room: WBGT 32.5 ± 0.5°C. Three pre-treatments (30 min each):

- WALK: treadmill at 1.7 m/s, 6% incline
- REST: standing in horse stock
- SHOWER: 10 min tap water (26.2 ± 0.8°C) via 5 hoses (0.4 L/s × 5 = 2.0 L/s total)

Then warm-up: canter at 10.0 m/s × 30 s on 6% incline + 5 min walking. Main exercise: 6% incline, speed set at ~115% V̇O2max (13.5–14.3 m/s, individually determined from incremental test); run to volitional exhaustion. Exhaustion = horse cannot maintain position at front of treadmill despite encouragement.

**Measurements:** Run time (stopwatch), BW (before pre-treatment and after main exercise; fecal weight subtracted), HR (mean of last 30 s of each phase), plasma lactate (Biosen S-line analyzer), T_PA and T_REC (before pre-treatment, after pre-treatment, after warm-up, after main exercise).

**Statistics:** ANOVA among pre-treatments; Tukey HSD post hoc; significance at P ≤ 0.05; JMP 16.1.0.

## Results (detailed — reproduce ALL numbers)

**Run time to exhaustion:**

| Pre-treatment | Mean (s) | SD   |
| ------------- | -------- | ---- |
| WALK          | 72.0     | 18.5 |
| REST          | 73.7     | 17.2 |
| SHOWER        | 69.1     | 22.0 |

No significant differences (P > 0.05).

**Body weight loss:**

| Pre-treatment | Mean (kg) | SD  | Significance                                 |
| ------------- | --------- | --- | -------------------------------------------- |
| WALK          | −7.5      | 1.5 | Significantly greater than SHOWER (P < 0.05) |
| REST          | −5.8      | 2.0 | Significantly greater than SHOWER (P < 0.05) |
| SHOWER        | −1.8      | 1.1 | —                                            |

**Heart rate during main exercise (bpm):**

| Pre-treatment | Mean | SD   |
| ------------- | ---- | ---- |
| WALK          | 217  | 8.2  |
| REST          | 210  | 8.3  |
| SHOWER        | 205  | 11.7 |

No significant differences.

**Plasma lactate after main exercise (mmol/L):**

| Pre-treatment | Mean | SD  |
| ------------- | ---- | --- |
| WALK          | 24.7 | 5.4 |
| REST          | 26.1 | 7.2 |
| SHOWER        | 25.4 | 6.3 |

No significant differences.

**Pulmonary artery temperature T_PA (°C):**

| Measurement point   | WALK       | REST       | SHOWER     | Significance                   |
| ------------------- | ---------- | ---------- | ---------- | ------------------------------ |
| After pre-treatment | 39.0 ± 0.3 | 37.6 ± 0.4 | 37.7 ± 0.4 | WALK > REST, SHOWER (P < 0.05) |
| After warm-up       | 39.8 ± 0.6 | 38.5 ± 0.4 | 38.4 ± 0.4 | WALK > REST, SHOWER (P < 0.05) |
| After main exercise | 41.8 ± 0.5 | 40.7 ± 0.7 | 40.4 ± 0.6 | WALK > REST, SHOWER (P < 0.05) |

**Rectal temperature T_REC after main exercise (°C):**

| Pre-treatment | Mean | SD  |
| ------------- | ---- | --- |
| WALK          | 39.2 | 0.7 |
| REST          | 38.6 | 0.4 |
| SHOWER        | 38.4 | 0.3 |

No significant differences for T_REC despite T_PA differences.

## Discussion & interpretation

Authors attribute the body weight difference between SHOWER and other treatments to reduced sweating during the 10-min shower: direct water-contact cooling reduces the core-to-skin temperature gradient that drives cutaneous sweat secretion, thereby limiting pre-exercise fluid loss. The T_PA elevation in WALK (treadmill walking in the hot environment) persisted through the warm-up and main exercise, consistent with prior work showing that ambient heat + light exercise creates a cumulative thermal burden. Because the WALK protocol pre-heated horses before their warm-up, they started the main bout already thermally loaded, yet this did not translate to a shorter run time — supporting the hypothesis that sprint performance is largely anaerobic and less sensitive to a 1–2°C core temperature elevation than endurance performance.

The finding that T_REC failed to mirror T_PA differences aligns with Ebisuda et al. (2023, Comp. Exerc. Physiol. 19:159–167) and Marlin et al. (1996), who noted that rectal temperature lags behind pulmonary artery temperature after short, intense exercise. Clinically, this means veterinarians relying on rectal temperature at trackside may underestimate actual core temperature after a sprint race on a hot day.

The authors compare their finding to the human literature (Bongers et al. 2015, Br. J. Sports Med.): pre-cooling improves endurance performance in heat but does not meaningfully affect sprint performance because anaerobic power and neuromuscular function dominate at supramaximal intensities and cooling reduces muscle contractile speed (Bigland-Ritchie et al. 1992). The warm-up (10.0 m/s × 30 s) was sufficient to restore muscle temperature after SHOWER without eliminating the thermoregulatory benefit.

## Limitations

- Very small n = 6 (all from single institution); crossover design partially compensates
- All horses were treadmill-trained; track fitness and race-day logistics differ
- Treadmill exercise at fixed speed to exhaustion may not replicate variable-pace race dynamics
- Only short-sprint exercise (~115% V̇O2max, 2 min) tested; effects on longer-distance races (≥ 1,600 m) are unknown and may differ
- WBGT 32–33°C is a specific heat stress level; not tested at lower thresholds (e.g., 28°C) common in Japanese summer flat races
- No cortisol, no muscle biopsy; mechanisms inferred from temperature and lactate only

## Feature-engineering notes for the model

- `wbgt_race_day` — wet-bulb globe temperature on race day — JRA/NAR race-day weather records — expected positive effect on heat-related EHI risk and body-weight change confounding — available from Japan Meteorological Agency / JRA track records
- `bw_delta_heat_adjusted` — 馬体重 change adjusted for WBGT (subtract estimated thermal sweat loss of ~4–6 kg per hot-day effort) — derived from body-weight change × WBGT flag — reduces noise in the standard 体重変化 feature on hot days
- `wbgt_x_bw_delta` — interaction: body-weight delta × WBGT — captures confounding of thermal sweat with true conditioning weight change — should be tried as an explicit interaction feature
- `wbgt_flag_summer` — binary indicator: WBGT ≥ 28°C or race month in July–September — proxy for hot conditions when exact WBGT not available — expected to reduce weight-change signal reliability; use as a modulator
- **Do NOT use** T_REC as a proxy for true core-temperature change during sprint; it is insensitive and lags T_PA by several degrees post-sprint
- **Practical limit:** cooling protocol information (whether a horse was showered pre-race) is not currently recorded in JRA race entry data, so this variable cannot be directly engineered; only WBGT-adjusted body-weight features are actionable

## Key references / follow-up leads

- **Takahashi Y. & Takahashi T. 2020** — "Risk factors for exertional heat illness in Thoroughbred racehorses in flat races in Japan (2005–2016)," Equine Vet. J. 52:364–368 — **highest priority**: directly models EHI risk in JRA flat races, same population
- **Ebisuda Y., Mukai K., Takahashi Y., Ohmura H. 2023** — "Effect of high ambient temperature on physiological responses during incremental exercise in thoroughbred horses," Comp. Exerc. Physiol. 19:159–167 — incremental exercise at 40°C vs. 20°C; weight loss −4.0 vs. −2.3 kg
- **Klous L. et al. 2020** — "Effects of pre-cooling on thermophysiological responses in elite eventing horses," Animals (Basel) 10:1664 — eventing (moderate intensity); pre-cooling mitigated rectal/skin temperature elevation
- **Brownlow M.A., Dart A.J., Jeffcott L.B. 2016** — "Exertional heat illness: a review of the syndrome affecting racing Thoroughbreds in hot and humid climates," Aust. Vet. J. 94:240–247 — EHI review, dehydration and sweating mechanism
- **Marlin D.J. et al. 1996** — physiological responses at 30°C/80% RH, Equine Vet. J. Suppl. 28:70–84 — seminal hot-humid physiology reference
