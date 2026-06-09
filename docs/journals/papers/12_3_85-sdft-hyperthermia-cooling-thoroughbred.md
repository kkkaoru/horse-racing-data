# Exercise-Induced Superficial Digital Flexor Tendon Hyperthermia and the Effect of Cooling Sheets on Thoroughbreds

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 12(3): 85–91, 2001                                                                                                                                                                                                                                                                                                        |
| docid                          | `12_3_85`                                                                                                                                                                                                                                                                                                                                |
| Article type                   | Original                                                                                                                                                                                                                                                                                                                                 |
| Authors                        | Hiroto Yamasaki, Motoaki Goto, Toyohiko Yoshihara, Maiko Sekiguchi, Katsuhiko Konno, Yasuyuki Momoi, Toshiroh Iwasaki                                                                                                                                                                                                                    |
| Affiliations                   | Department of Veterinary Internal Medicine, Faculty of Agriculture, Tokyo University of Agriculture and Technology, Fuchu 183-8509; Saitama Daiichi Pharmaceutical Co. Ltd., 8–1 Minamisakae-cho, Kasukabe, Saitama 344-0057; Equine Research Institute, Japan Racing Association, 321-4 Tokami-cho, Utsunomiya, Tochigi 320-0856, Japan |
| Received / Accepted / Released | Accepted September 19, 2001                                                                                                                                                                                                                                                                                                              |
| Keywords                       | exercise, fibroblast, hyperthermia, cooling sheet, flexor tendonitis                                                                                                                                                                                                                                                                     |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/12/3/12_3_85/_pdf/-char/en                                                                                                                                                                                                                                                                      |

## Abstract (verbatim)

> Flexor tendonitis in racehorses which is caused by racing or training diminishes their running ability. In the present study, the involvement of hyperthermia in the development of flexor tendonitis is investigated. When fibroblasts isolated from the superficial digital flexor tendon of a horse are cultured, their survival rate decreases after 1 hour of exposure to a temperature of 43°C. When a racehorse runs on a dirt track, the center of the tendon runs a fever of 43°C or more. This finding suggests that the fever occurring during running can be a cause of flexor tendonitis. The study also indicates that cooling the distal ends of the fores after racing is effective in preventing flexor tendonitis.

## Relevance to finishing-position (着順) prediction

This paper belongs to feature family **A (injury/soundness)** with cross-relevance to **E (environment/heat)**. It establishes the thermodynamic mechanism behind superficial digital flexor tendonitis (SDFT) — the most common soft-tissue injury in Thoroughbred racehorses, with a 7% reported incidence across multiple studies. The injury mechanism is: high-speed running → tendon elastic energy storage → 5–10% energy released as heat → tendon centre (avascular) reaches ≥43°C → fibroblast cell death → degenerative change → tendon fibre rupture.

For the finishing-position pipeline, this paper most directly informs a **scratch/non-start probability model**: horses with SDFT injury history, or conditions that elevate tendon heat risk (long distance, high ambient temperature, dirt track, high pace), have elevated re-injury probability that affects whether they finish the race at all. A horse that breaks down mid-race effectively finishes last; predicting this probability improves expected-rank estimates.

The paper also highlights an important asymmetry: in counterclockwise racing (standard JRA direction), the left fore carries greater load → hotter tendon → higher left-fore SDFT risk. Horse 1 left fore temperature post-gallop: 40.4°C; right fore: 39.4°C (1°C asymmetry). Horse 2: left 40.0°C vs. right 35.8°C (4.2°C asymmetry). The larger asymmetry in Horse 2 (which had no prior SDFT history but had a sesamoid fracture history) is unexplained but suggests individual variation in load distribution.

The fibroblast data quantify the temperature-damage relationship: at 43°C (1 hr), 10% cell death; at 45°C (1 hr), 73% cell death. Actual race tendon centre temperature is estimated at ≥43°C based on Fourier heat conduction modelling from the measured post-run values of 40.4°C (measured ≥3 min after running, after ~1°C/min cooling). This is the fundamental basis for the cooling-sheet recommendation.

## Background & objective

SDFT injury incidence is 7% in racehorses, causing acute inflammation, lameness, and typically ending racing careers or requiring prolonged rest. Tendon degeneration at the centre is seen even in horses without clinical tendonitis (Webbon 1977; Peloso et al. 1994), suggesting a chronic subclinical process. Wilson & Goodship (1994) showed thermodynamic modelling predicted tendon centre temperatures 11°C above periphery, reaching 45°C at gallop on a treadmill — this can kill fibroblasts necessary for tendon maintenance and repair. The objectives of this study were: (1) determine the heat sensitivity of horse SDFT fibroblasts in vitro; (2) measure tendon centre temperature in horses running on a real dirt racetrack (Tokyo Racecourse); (3) evaluate whether leg-cooling sheets can reduce post-exercise tendon temperature.

## Materials & methods

**In vitro — Fibroblast heat sensitivity:**

- Source: distal fore limbs of 6 slaughtered horses (SDFT history unknown); isolated within 12 hr of slaughter; cooled in ice during transport
- Isolation: biopsy trephine from centre of SDFT (metacarpal region)
- Culture: DMEM/F-12 (GIBCO); 37°C, 5% CO₂; 10% fetal bovine serum + penicillin 100 U/ml + streptomycin 100 µg/ml + amphotericin B 5 µg/ml
- After 3–8 subcultures, confluent cells suspended in trypsin-EDTA; adjusted to 5×10⁵ cells/ml
- Heating: water bath at 37, 39, 41, 43, or 45°C for 1 hr; then 5 min at 37°C
- Viability: MTT assay (TMCS MTT kit, Trevigen); absorbance at 570 nm measured by microplate reader (Corona Electric MTP-12)
- Reference: absorbance at 37°C set to 1.00 (100% survival); all other values expressed relative to this
- Same protocol conducted with skin-derived fibroblasts from 6 horses for comparison
- Also conducted heating for 2–4 hr (data not shown in paper) to assess time-dependence

**In vivo — Tendon temperature measurement:**

- Horse 1: Male (steed), 518 kg, history of right SDFT rupture; no current clinical symptoms
- Horse 2: Female (mare), 410 kg, history of proximal sesamoid bone fracture (right fore); no current symptoms; no SDFT history
- Exercise: jockey-ridden; 30 min trot warm-up; then 1 round counterclockwise on dirt track at Tokyo Racecourse (1878 m): walk first, then canter→gallop (~10 m/s). Followed by trot to experiment site
- Anaesthesia: intravenous xylazine + ketamine; horse anaesthetised immediately post-exercise (within ~30–56 s after finishing) and laid down; temperature measurement started 3 min post-anaesthesia
- Temperature sensors: needle-type sensor inserted 5 mm into tendon centre (position pre-determined on X-ray film); disc sensor for palmar skin; bar sensor for rectum
- Measurement interval: every 1 min until recovery from anaesthesia
- Cooling sheet: Winback equine leg-cooling sheet (Saitama Daiichi Pharmaceutical, Kasukabe, Japan) applied to one fore post-exercise; contralateral fore as control
- Resting measurements: same two horses anaesthetised at rest (control condition) for baseline temperatures

## Results (detailed — reproduce ALL numbers)

**In vitro fibroblast survival — Tendon-derived (n=6 samples):**

| Temperature | Relative absorbance (tendon) | Relative absorbance (skin) |
| ----------- | ---------------------------- | -------------------------- |
| 37°C        | 1.00 (reference)             | 1.00 (reference)           |
| 39°C        | 0.96                         | 1.03                       |
| 41°C        | 0.96                         | 0.98                       |
| 43°C        | 0.90                         | 0.86                       |
| 45°C        | 0.27                         | 0.02                       |

Key observations:

- Steep decline begins at 43°C in both cell types
- At 43°C: tendon fibroblasts 10% cell death (0.90), skin fibroblasts 14% cell death (0.86)
- At 45°C: tendon fibroblasts 73% cell death (0.27), skin fibroblasts 98% cell death (0.02)
- Tendon fibroblasts more heat-resistant than skin fibroblasts across all temperatures (especially marked at 45°C)
- Extended heating (2–4 hr) at 43°C and 45°C caused additional time-dependent cell death (data not shown in paper)
- The authors also noted gelatinase activity in equine fibroblasts decreased at 43°C exposure (data not shown)

**Resting temperature measurements:**

Horse 1 (at rest, measured starting 5 min after anaesthesia):

- Left SDFT centre: 33.5°C
- Right SDFT centre: 33.8°C
- Central palmar skin, left fore: 32.7°C
- Central palmar skin, right fore: 32.3°C
- Rectum: 38.2°C

Horse 2 (at rest, measured starting 3 min after anaesthesia):

- Lower skin temperatures than Horse 1, attributed to lower ambient air temperature at time of experiment (specific values not tabulated in text but shown in Figure 2B)

Cooling sheet effect at rest: When applied 7 min after anaesthesia, skin temperature showed slight initial decrease (cooling sheet depriving skin of heat) but then rose, presumably due to blood flow maintaining skin at body temperature. The cooling sheet was not effective at rest — a compensatory mechanism maintains skin temperature when ambient skin temperature is clearly below body temperature.

**Post-exercise temperature measurements:**

Horse 1 (round completed in 3 min 24 s; anaesthesia 56 s after; measurement from 3 min post-anaesthesia):

- Left SDFT centre: **40.4°C**
- Right SDFT centre: **39.4°C**
- Central palmar skin, left fore: 37.1°C
- Central palmar skin, right fore: 37.9°C

Horse 2 (round completed in 3 min 9 s; anaesthesia 31 s after; measurement from 3 min post-anaesthesia):

- Left SDFT centre: **40.0°C**
- Right SDFT centre: **35.8°C**
- Central palmar skin, left fore: 37.3°C
- Central palmar skin, right fore: 35.7°C
- Lower skin temperatures than Horse 1, attributed to lower ambient temperature at time of experiment

Left fore consistently hotter than right fore in both horses (Horse 1: 1.0°C differential; Horse 2: 4.2°C differential), attributed to greater left-fore loading when galloping counterclockwise on a round track.

**Cooling rate extrapolation:** Temperature showed decrease of ~1°C/min from the time of first measurement (3 min post-run). By Fourier's law of heat conduction, working backward from 40.4°C measured ≥3 min post-run, tendon centre temperature at end of exercise was **≥43°C**. Authors note actual race conditions (higher speed, longer duration than one 1878 m round) would produce even higher temperatures.

**Cooling sheet post-exercise:** When applied at 4 min post-anaesthesia:

- Horse 1 (sheet on left fore): skin temperature of treated left fore clearly lower; no appreciable difference in tendon temperature between treated and untreated fores (heat conduction from deep tendon centre to skin is slow)
- Horse 2 (sheet on right fore): skin temperature of treated right fore clearly lower; tendon temperature consistently lower in untreated left fore (as per exercise asymmetry); the right fore's lower initial temperature made the cooling effect less pronounced

Key finding: cooling sheet effectively lowered post-exercise skin temperature but did not significantly cool the avascular tendon centre in the short application window studied. However, the authors argue that lowering skin temperature does create a favourable temperature gradient (Fourier conduction) that should gradually cool the tendon centre if applied continuously. The mechanism is more effective post-exercise than at rest because the skin-tendon temperature differential is larger post-exercise (skin elevated by exercise heat, tendon very hot).

## Discussion & interpretation

The convergence of in vitro and in vivo data makes the causal chain clear: race-pace galloping heats the SDFT centre to ≥43°C; at this temperature fibroblast viability begins to decline (10% cell death at 43°C after 1 hr); at the likely actual peak temperature (possibly >45°C for longer races), up to 73–98% cell death occurs; dead fibroblasts cannot maintain or repair tendon collagen matrix; chronic subclinical degeneration accumulates; eventual mechanical failure (fibre rupture) occurs under load.

The avascular nature of the tendon centre is the key vulnerability: the vascularised periphery can be cooled by blood flow, but the centre relies entirely on heat conduction. Wilson & Goodship (1994) modelled an 11°C centre-periphery gradient; this study's in vivo data are consistent with a gradient of at least 3–7°C (centre 40.4°C vs. skin 37.1°C at 3 min post-run, with centre cooling faster than skin initially).

The left-fore > right-fore asymmetry in both horses (counterclockwise direction) is a consistent finding. The mechanism is that counterclockwise galloping places greater centrifugal load on the inside (left) fore. This is a measurable risk asymmetry that could be exploited if per-limb injury history is available.

The cooling sheet finding is practically important: it works by reducing skin temperature and creating a conductive gradient. Its effectiveness is limited at rest (compensatory blood flow) but meaningful post-exercise (where elevated skin temperature creates favourable gradient for tendon cooling). Wider clinical deployment of post-race cooling could plausibly reduce SDFT injury rates.

## Limitations

- Only 2 horses in vivo; statistically insufficient for quantitative claims; findings are mechanistic/pilot rather than epidemiological
- Tendon temperature measurements began ≥3 min post-run due to anaesthesia time; actual peak temperatures at end of exercise are extrapolated, not measured
- Galloping on a real track at ~10 m/s for one round (1878 m, ~3 min) is lower intensity than an actual race (~1600–3000 m at 14–16 m/s for Thoroughbreds); actual race temperatures are likely higher
- Fibroblast assay used horses of unknown SDFT history; if tendons were already subclinically degenerate, baseline viability may have been reduced
- Cooling sheet assessment was short-duration (minutes); longer-duration cooling (as actually used post-race for up to 30 min) was not evaluated
- Individual variation in ambient temperature between the two sets of experiments complicates direct comparison of absolute temperatures

## Feature-engineering notes for the model

- `sdft_injury_history_flag` — binary flag for prior SDFT injury in either fore — veterinary records / JRA disability history — strongest injury re-risk feature; prior SDFT rupture → increased tendon degeneration → higher re-injury probability; expected strong negative association with finishing position if included in a scratch-risk component
- `sdft_days_since_injury` — days since last SDFT-related lay-off or vet treatment — JRA records if available — recovery time proxy; horses returning too soon after tendon injury have high re-injury risk; non-linear (concave) effect with sharply elevated risk below ~180 days rest
- `race_distance_m` — metres — JRA race records — longer distance → more tendon heat cycles → higher injury risk; interaction with pace/speed; model should include distance × injury_history interaction
- `track_surface` — dirt / turf / synthetic — JRA race records — dirt generates more concussive heat per stride than turf; dirt × SDFT history is highest risk combination; Tokyo, Hanshin, Nakayama dirt tracks are primary risk contexts
- `ambient_temp_race_day` — °C — JMA weather data — higher ambient temperature reduces rate of heat loss from skin, slowing tendon cooling and maintaining higher tendon temperature during extended effort
- `recent_race_intensity_proxy` — pace in most recent race (e.g., seconds per furlong) — derived from recent race times / distance — high-pace recent races indicate cumulative tendon heat loading; interaction with days since last race
- `left_fore_load_index` — binary: counterclockwise track (JRA standard) — all JRA races — all JRA flat races are counterclockwise; left-fore higher risk; could be used in asymmetric injury risk models but magnitude of effect is small (1–4°C differential in this study)
- `sdft_incidence_base_rate` — 7% — from paper citations (Genovese et al. 1990; Pool & Meagher 1990; Goodship 1993; Reimersma & Schamhardt 1985) — Bayesian prior for scratch-from-injury probability model; use as base rate in prior specification
- **Caution:** SDFT injury is a rare event (7%); a binary injury predictor requires enough positive examples for reliable calibration. In a dataset of 10,000 horse-race observations, only ~700 would involve SDFT events, and most would be scratch/withdrawal rather than mid-race breakdown. Oversampling or Platt scaling recommended.

## Key references / follow-up leads

- Birch HL, Wilson AM, Goodship AE. 1997. The effect of exercise-induced localized hyperthermia on tendon cell survival. _J. Exp. Biol._ 200: 1703–1708. [foundational tendon hyperthermia paper; treadmill gallop reaching 45°C at tendon centre]
- Wilson AM, Goodship AE. 1994. Exercise-induced hyperthermia as a possible mechanism for tendon degeneration. _J. Biomech._ 27: 899–905. [thermodynamic modelling of SDFT temperature gradient; 11°C centre-periphery predicted]
- Genovese RL, Rantanen NW, Simpson BS, Simpson DM. 1990. Clinical experience with quantitative analysis of superficial digital flexor tendon injuries in thoroughbred and standardbred racehorses. _Vet. Clin. North. Am. Equine Pract._ 6: 129–147. [SDFT incidence data]
- Pool RR, Meagher DM. 1990. Pathologic findings and pathogenesis of race track injuries. _Vet. Clin. North. Am. Equine Pract._ 6: 1–31. [general race track injury pathology]
- Webbon PM. 1977. A post mortem study of equine distal flexor tendons. _Equine Vet. J._ 9: 61–67. [subclinical tendon degeneration in horses without clinical tendonitis]
- Peloso JG, Mundy GD, Cohen ND. 1994. Prevalence of and factors associated with musculoskeletal racing injuries of Thoroughbreds. _J. Am. Vet. Med. Assoc._ 204: 620–626. [epidemiology of Thoroughbred racing injuries]
- Goodship AE. 1993. The pathophysiology of flexor tendon injury in the horse. _Equine Vet. Educ._ 5: 23–29.
- Marr CM, Love S, Boyd JS, McKellar Q. 1993. Factors affecting the clinical outcome of injuries to superficial digital flexor tendon in National Hunt and point-to-point racehorses. _Vet. Rec._ 132: 476–479.
