# Free Radical Formation after Intensive Exercise in Thoroughbred Skeletal Muscles

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                             |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 22(2): 21–28, 2011                                                                                                                                                                                                                                                                                 |
| docid                          | `22_2_21`                                                                                                                                                                                                                                                                                                         |
| Article type                   | Original Article                                                                                                                                                                                                                                                                                                  |
| Authors                        | Yoshio Minami, Minako Kawai, Taiko C. Migita, Atsushi Hiraga, Hirofumi Miyata                                                                                                                                                                                                                                     |
| Affiliations                   | (1) Biological Science, Graduate School of Medicine, Yamaguchi University, Yoshida 1677-1, Yamaguchi 753-8515; (2) Biological Chemistry, Faculty of Agriculture, Yamaguchi University (same address); (3) The Equine Research Institute, Japan Racing Association, 321-4 Tokami-cho, Utsunomiya, Tochigi 320-0856 |
| Received / Accepted / Released | Accepted: February 16, 2011                                                                                                                                                                                                                                                                                       |
| Keywords                       | free radical, EPR, Thoroughbred, training                                                                                                                                                                                                                                                                         |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/22/2/22_2_21/_pdf/-char/en                                                                                                                                                                                                                                               |

## Abstract (verbatim)

> Although high oxygen consumption in skeletal muscle may result in severe oxidative stress, there are no direct studies that have documented free radical production in horse muscles after intensive exercise. To find a new parameter indicating the muscle adaptation state for the training of Thoroughbred horses, we examined free radical formation in the muscle by using electron paramagnetic resonance (EPR). Ten male Thoroughbred horses received conventional training for 18 weeks. Before and after the training period, all horses performed an exhaustive incremental load exercise on a 6% incline treadmill. Muscle samples of the middle gluteal muscle were taken pre-exercise and 1 min, 1 hr, and 1 day after exercise. Muscle fiber type composition was also determined in the pre-exercise samples by immunohistochemical staining with monoclonal antibody to myosin heavy chain. We measured the free radical in the muscle homogenate using EPR at room temperature, and the amount was expressed as relative EPR signal intensity. There was a significant increase in Type IIA muscle fiber composition and a decrease in Type IIX fiber composition after the training period. Before the training period, the mean value of the relative EPR signal intensity showed a significant increase over the pre-exercise value at 1 min after the exercise and an incomplete recovery at 24 hr after the exercise. While no significant changes were found in the relative EPR signal intensity after the training period. There was a significant relationship between percentages of Type IIA fiber and change rates in EPR signal intensity at 1 min after exercise. The measurement of free radicals may be useful for determining the muscle adaptation state in the training of Thoroughbred horses.

## Relevance to finishing-position (着順) prediction

Feature family: **C — exercise-physiology / fitness** (with secondary relevance to F — conformation/muscle fibre composition).

This is the first paper to directly measure free radicals (by EPR) in horse skeletal muscle before and after exercise, and before and after an 18-week training programme. The key findings for the prediction pipeline are:

1. **Training-induced IIX→IIA fibre shift** (Type IIA: +14.1 percentage points; Type IIX: −15.6 pp after 18 weeks) correlates with suppressed post-exercise free-radical burst — a mechanistic link between training adaptation and oxidative resilience.
2. **Post-exercise EPR does not increase** in trained horses — reflecting enhanced antioxidant capacity and reduced mitochondrial ROS emission from IIA vs IIX fibres.
3. **Chronic resting free-radical accumulation** after training (resting EPR higher post-training than pre-training) indicates that long-term intense training produces sustained oxidative load at rest, relevant to recovery timing between races.

For the model: the paper provides a strong physiological rationale for using race-interval features (days since last race), career-race-count, and training-history patterns as proxies for the IIA/IIX fibre ratio and the associated oxidative-stress/recovery state. Horses with longer continuous training histories (low scratch/withdrawal rates, consistent short-interval race patterns) likely have higher IIA fractions, faster acute oxidative recovery, and thus lower residual fatigue going into subsequent races. The 24-hour incomplete recovery in pre-training horses supports using a "fatigue accumulation" feature keyed to whether the last race was ≤6 days ago.

## Background & objective

Thoroughbred VO₂max can reach up to 200 ml O₂/kg/min — more than 3× the human value. The resulting high oxygen flux through active muscle produces massive mitochondrial electron leakage and free-radical generation. While indirect oxidative stress markers (lipid peroxides, glutathione oxidation, protein oxidation) had been studied in horses, no prior work had applied the direct method — electron paramagnetic resonance (EPR) spectroscopy — to horse skeletal muscle. EPR detects unpaired electrons (free radicals) by their characteristic g-value signal at g ≈ 2.005. Prior EPR work existed in rodents (Davies et al., 1982) and humans (Bailey et al., 2007) but not horses. The study also aimed to test whether training adaptation modifies the exercise-induced free-radical burst, and whether the IIX→IIA fibre type transformation induced by training mediates any such change.

## Materials & methods

**Subjects:** 10 male Thoroughbred horses. Before the exercise test, all horses stayed on pasture in daytime and were acclimatised to treadmill running for a short period. n=8 had full pre/post data (2 horses had ND post-training values).

**Exercise test:** Exhaustive incremental load treadmill (6% slope). Warm-up: 2 min at 1.8 m/sec. Incremental phase: 2 min each at 4, 6, 8, 10 m/sec, then 2 min each at 11, 12, 13, 14, ... m/sec until horse could not maintain position.

**Muscle sampling:** Middle gluteal muscle biopsy (5 cm depth) at 4 time points: pre-exercise, 1 min post, 1 hr post, 1 day post. Immediately frozen in liquid N₂, stored at −80°C.

**Training protocol:** 18 weeks, 5 days/week:

- Weeks 1–3: 3 min treadmill at 75% VO₂max
- Weeks 4–6: 3 min at 90% VO₂max
- Weeks 7–10: 3 min at 100–110% VO₂max
- Weeks 11–18: 2 min at 110–115% VO₂max (2 days/week) + 3 min at 90% VO₂max (3 days/week)

**EPR measurement:** Bruker BioSpin X-band E500 spectrometer. Settings: microwave power 10 mW; frequency 9.85 GHz; modulation frequency 100 kHz; modulation amplitude 5.0 G; field centre 3,510 G; scan width 100 G; scan time 20 sec; time constant 0.02 sec (1 sweep). ~50 mg frozen muscle homogenised in 500 µl cold buffer (40 mM Tris-HCl + 300 mM sucrose). Signal intensity normalised to muscle tissue concentration (g/L); expressed in arbitrary units (AU) relative to pre-exercise value.

**Immunohistochemistry:** Two 10-µm transverse sections per sample. Primary antibodies: anti-Fast Myosin (anti-MHC-II, 1:1,000) and SC-71 (anti-MHC-IIa, 1:1,000). Secondary antibody: HRP-labelled (1:1,000). Chromogen: diaminobenzidine tetrahydrochloride. Fibre classification: Type I (MHC-I expressing), Type IIA (MHC-IIa expressing), Type IIX (MHC-II positive but not IIa). Type IIB was absent (consistent with prior equine studies).

**Statistics:** Paired t-test for pre/post exercise differences and pre/post training comparisons. Linear regression for correlation between fibre type % and EPR change rate. P<0.05 significant. Values reported as mean ± SD.

## Results (detailed — all numbers reproduced)

**Muscle fibre composition (Table 1, individual horse data):**

| Horse    | Pre-training Type I% | Pre-training IIA% | Pre-training IIX% | Post-training Type I% | Post-training IIA% | Post-training IIX% |
| -------- | -------------------- | ----------------- | ----------------- | --------------------- | ------------------ | ------------------ |
| 1        | 7.0                  | 41.2              | 51.8              | 12.7                  | 47.0               | 40.3               |
| 2        | 7.3                  | 36.1              | 56.6              | 8.2                   | 53.4               | 38.4               |
| 3        | 11.2                 | 46.4              | 42.4              | ND                    | ND                 | ND                 |
| 4        | 2.8                  | 46.6              | 50.6              | 9.4                   | 67.5               | 23.1               |
| 5        | 18.2                 | 42.2              | 39.6              | 14.1                  | 63.7               | 22.3               |
| 6        | 10.7                 | 46.6              | 42.7              | ND                    | ND                 | ND                 |
| 7        | 10.9                 | 38.7              | 50.4              | 9.4                   | 51.6               | 39.0               |
| 8        | 13.7                 | 44.9              | 41.4              | 15.3                  | 58.2               | 26.5               |
| 9        | 11.3                 | 45.6              | 43.1              | 15.5                  | 59.8               | 24.7               |
| 10       | 12.0                 | 45.6              | 42.4              | 11.1                  | 59.2               | 29.7               |
| **Mean** | **10.5**             | **43.4**          | **46.1**          | **12.0**              | **57.5\***         | **30.5\***         |
| **SD**   | **4.1**              | **3.7**           | **5.7**           | **2.9**               | **6.6**            | **7.6**            |

\*P<0.05 vs. pre-training (paired t-test, n=8). ND = not determined. Type I change was not significant.

**Change in fibre composition after 18 weeks of training (n=8):**

- Type I: 10.5 → 12.0% (+1.5 pp); not significant
- Type IIA: 43.4 → 57.5% (**+14.1 pp**; P<0.05)
- Type IIX: 46.1 → 30.5% (**−15.6 pp**; P<0.05)

Note: The decrease in IIX in the current study was similar to Rivero et al. (2007), but the increase in IIA was larger, attributed to the classification difference (Rivero used 4 types including IIA/IIX hybrid category; the current study grouped most IIA/IIX hybrids into IIA).

**EPR signal intensity (free radical levels):**

_Pre-training:_

- At 1 min post-exercise: **significant increase** vs. pre-exercise (P<0.05, paired t-test)
- At 1 hr post-exercise: not significant vs. pre-exercise, but did not fully recover to pre-exercise level
- At 1 day post-exercise: not significant vs. pre-exercise, but **incomplete recovery** (EPR still above pre-exercise baseline numerically at 24 hr)

_Post-training:_

- At 1 min, 1 hr, 1 day post-exercise: **no significant change** at any time point vs. pre-exercise
- Resting EPR at rest after training: **significantly higher** than the pre-training resting EPR value (P<0.05; denoted †p<0.05 compared to pre-training resting level in Figure 3)

**Correlation between fibre type % and EPR change rate at 1 min:**

- Type IIA% vs. EPR change rate: **significant negative correlation** (r and p-value from figure; described as "significant" at P<0.05 in text; combined pre- and post-training data points shown in Fig. 4)
- Type IIX% vs. EPR change rate: **significant positive correlation** (P<0.05)

Interpretation: horses/time points with higher %IIA had smaller post-exercise EPR spike; horses with higher %IIX had larger post-exercise EPR spike. After training (shift to higher %IIA, lower %IIX), the post-exercise EPR spike is abolished.

## Discussion & interpretation

The free-radical EPR signal (g=2.005) is attributed by analogy with prior rodent and human studies to ubiquinone radicals (g≈2.004 in those studies), though the room-temperature measurement (vs. 77K in prior work) means species identification requires further confirmation.

The mechanism for the training-induced suppression of post-exercise free-radical burst involves two complementary adaptations:

1. **Type IIX→IIA fibre shift**: Type IIX (equivalent to IIB in other species) fibres have unique mitochondrial ultrastructure properties that potentiate O₂⁻ and H₂O₂ generation (Anderson & Neufer, 2006); Type IIA fibres have higher ROS removal capacity (Hollander et al., 1999) and stronger reinforcement of antioxidant enzymes by training.
2. **Enhanced antioxidant capacity**: Training increases antioxidant capacity in equine blood and muscle (Avellini et al., 1999); trained mitochondria also show lower H₂O₂ release rates (Venditti et al., 1999 in rats).

The finding that resting EPR is **higher** post-training contradicts the acute exercise response — it indicates chronic oxidative load accumulation at rest from long-term training, consistent with elevated plasma CK and lipid peroxidation in trained weightlifters (Liu et al., 2005). This suggests that proper recovery periods are needed to prevent chronic oxidative damage even in well-conditioned horses.

## Limitations

- n=10 (only n=8 with complete pre/post data); small sample reduces statistical power and makes individual variation hard to separate from training effect.
- All horses were male; sex effects on fibre type distribution and free-radical response are unknown in this model.
- EPR measurement at room temperature (not 77K standard); signal species not definitively identified as ubiquinone.
- No performance outcome data (race results, speed); the study is purely physiological.
- The training protocol is the same for all horses; individual variation in adaptability is not modelled.
- Biopsy from middle gluteal only; different muscles used in racing may show different fibre-type compositions and free-radical profiles.

## Feature-engineering notes for the model

- `days_since_last_race` — integer days; source: race history — expected non-linear effect: <6 days → incomplete EPR recovery (residual free-radical burden, per the 24-hr incomplete recovery finding) → higher finishing number; >60 days → possible deconditioning; optimal ~10–21 days — high availability
- `race_interval_category` — categorical: <7 days (acute fatigue), 7–14 (normal), 15–28 (extended), >28 (long break) — captures the non-linear recovery shape
- `consecutive_races_L90d` — count of races in prior 90 days — proxy for chronic oxidative accumulation at rest (elevated resting EPR post-training); very high counts → elevated chronic oxidative load → potentially impaired performance — source: race history
- `training_weeks_before_debut` — months from first recorded training activity to debut race — proxy for IIA/IIX ratio at debut; longer well-paced training → higher IIA fraction → better aerobic oxidative resilience — source: registration data
- `withdrawal_rate_L1y` — fraction of entered races withdrawn from in the past year — proxy for health/fatigue disruption; high withdrawal rate signals chronic health issues or over-training — source: race entry withdrawal records
- **Fibre type %IIA, %IIX, and EPR signal intensity** are NOT available from JRA/NAR race records; do not use directly. The above indirect proxies are the actionable feature set.

## Key references / follow-up leads

- Rivero et al. (2007) J. Appl. Physiol. 102: 1871–1882 — effects of training intensity and duration on muscular responses in Thoroughbred racehorses; IIX→IIA fibre transformation with long-term training
- Avellini et al. (1999) Comp. Biochem. Physiol. B 123: 147–154 — training increases antioxidant capacity in extracellular fluids and blood cells of horses
- Anderson & Neufer (2006) Am. J. Physiol. 290: C844–851 — Type II skeletal myofibers (IIB/IIX) possess unique properties potentiating mitochondrial H₂O₂ generation
- Yamano et al. (2002) Am. J. Vet. Res. 63: 1408–1412 — growth and training muscle adaptation in Thoroughbred horses; fibre composition changes
- Davies et al. (1982) Biochem. Biophys. Res. Commun. 107: 1198–1205 — first EPR demonstration of exercise-induced free radicals in rodent skeletal muscle
- Bailey et al. (2007) Free Radic. Res. 41: 182–190 — EPR evidence of exercise-induced free radical accumulation in human skeletal muscle
- Hollander et al. (1999) Am. J. Physiol. 277: R856–862 — superoxide dismutase gene expression in skeletal muscle: fibre-specific adaptation to endurance training; IIA > IIB ROS removal capacity
- Venditti et al. (1999) Arch. Biochem. Biophys. 372: 315–320 — training reduces mitochondrial H₂O₂ release rate in rat skeletal muscle
