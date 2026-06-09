# Sarcoplasmic Reticulum Ca2+-ATPase Activity and Glycogen Content in Various Fiber Types after Intensive Exercise in Thoroughbred Horses

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                              |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 20(3): 33–40, 2009                                                                                                                                                                                                                                                                  |
| docid                          | `20_3_33`                                                                                                                                                                                                                                                                                          |
| Article type                   | Original                                                                                                                                                                                                                                                                                           |
| Authors                        | Yoshio Minami, Seiko Yamano, Minako Kawai, Atsushi Hiraga, Hirofumi Miyata                                                                                                                                                                                                                         |
| Affiliations                   | Biological Sciences, Graduate School of Medicine, Yamaguchi University, Yoshida 1677–1, Yamaguchi 753-8515; Science Research Center, Yamaguchi University, Yoshida 1677–1, Yamaguchi 753-8511; Equine Research Institute, Japan Racing Association, 321–4 Tokami-cho, Utsunomiya, Tochigi 320-0856 |
| Received / Accepted / Released | Accepted April 27, 2009                                                                                                                                                                                                                                                                            |
| Keywords                       | glycogen recovery, sarcoplasmic reticulum Ca2+-ATPase, Thoroughbred                                                                                                                                                                                                                                |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/20/3/20_3_33/_pdf/-char/en                                                                                                                                                                                                                                |

## Abstract (verbatim)

> To find a new parameter indicating muscle fitness in Thoroughbred horses, we examined time-dependent recovery of glycogen content and sarcoplasmic reticulum (SR) Ca2+-ATPase activity of skeletal muscle after intensive treadmill running. Two repeated 50-sec running sessions (13 m/sec) were performed on a flat treadmill (approximately 90%V̇O2max). Muscle samples of the middle gluteal muscle were taken before exercise (pre) and 1 min, 20 min, 60 min, and 24 hr after exercise. Muscle fiber type composition was determined in the pre muscle samples by immunohistochemical staining with monoclonal antibody to myosin heavy chain. SR Ca2+-ATPase activity of the muscle and glycogen content of each muscle fiber type were determined with biochemical analysis and quantitative histochemical staining, respectively. As compared to the pre value, the glycogen content of each muscle fiber type was reduced by 15–27% at 1 min, 20 min, and 60 min after the exercise and recovered to the pre value at 24 hr after exercise test. These results indicate that 24 hr is enough time to recover glycogen content after short-term intensive exercise. The mean value of the SR Ca2+-ATPase activity showed a slight decrease (not significant) immediately after exercise, and complete recovery at 60 min after exercise. There were no significant relationship between the changes in glycogen content of each muscle fiber type and SR Ca2+-ATPase. Although further studies are needed, SR Ca2+-ATPase is not a useful parameter to detect muscle fitness, at least in Thoroughbred horses.

## Relevance to finishing-position (着順) prediction

Feature family: **C — exercise physiology / fitness**. This study characterises glycogen depletion and recovery dynamics in Thoroughbred skeletal muscle after near-maximal (~90% VO2max) exercise at the fibre-type level. The central finding — that 24 hours is sufficient for full glycogen recovery after short-duration (~50 sec × 2 at 13 m/sec) high-intensity work — provides a concrete physiological prior for encoding inter-race recovery features in the prediction pipeline.

For finishing-position modelling, the most actionable implication is the `days_since_last_race` feature: a horse racing within 24 hours of equivalent high-intensity work would face a ~15–27% muscle glycogen deficit across all fibre types, which is expected to impair late-race performance. In practice, JRA races are never held daily for the same horse, but short inter-race intervals (e.g., 7 days vs. 14 days) modulate accumulated fatigue beyond single glycogen cycles. The data also imply that exercise duration (and thus race distance) matters more than intensity for determining depletion magnitude — supporting a `distance-adjusted recovery index` feature.

The null result on SR Ca2+-ATPase as a fitness indicator is important for negative feature selection: despite being a plausible candidate, Ca2+-ATPase activity shows neither a consistent post-exercise change nor any correlation with glycogen depletion. This prevents wasted effort trying to derive Ca2+-ATPase proxies from training data.

The fibre composition finding — >80% Type II (45% IIA by number, 45% IIX by area) in Thoroughbred middle gluteal muscle — underpins the relevance of power-related features (sprint capacity, distance aptitude) as proxies for fibre type distribution, which cannot be directly observed from race records.

## Background & objective

Thoroughbred racehorses run at >60 km/h for several minutes, causing muscle fatigue; accumulated fatigue raises injury risk. Substrate exhaustion (particularly glycogen) is a primary cause of muscle fatigue, and SR Ca2+ handling is essential to excitation-contraction coupling. Two prior horse studies examined glycogen and SR Ca2+-ATPase separately (Byrd et al. 1989; Pratt et al. 2007) but no study had examined the relationship between both parameters at the fibre-type level during recovery in Thoroughbreds. The study aims to: (1) characterise time-dependent recovery of glycogen by fibre type; (2) measure SR Ca2+-ATPase activity changes; (3) test the hypothesis that glycogen depletion drives SR Ca2+-ATPase dysfunction.

## Materials & methods

**Subjects:** 6 male Thoroughbred horses, 476 ± 32 kg (range 436–531 kg); pasture-maintained before study; treadmill-accustomed.

**VO2max determination:** Incremental tests on both flat and 10% inclined treadmills (walk 2 min at 1.8 m/s → 3.6 m/s for 5 min → 6 m/s for 1 min → increments of 1 m/s per minute until exhaustion). Open-flow respirometry (Vice Medical, Chiba). VO2 determined from last 15 sec at each speed; VO2max from regression at levelling-off. Running speed at 90% VO2max calculated per horse from regression; all horses fell within 12–14 m/s range so 13 m/s was adopted uniformly.

**Exercise protocol:** Warm-up: 3 min walk + 1 min trot + 3 min walk. Main bout: **2 × 50 sec gallop at 13 m/sec on flat treadmill**, separated by 10 min walking. Cool-down: 10 min walk.

**Biopsy:** Middle gluteal muscle sampled at 5 cm depth using Bergstrom needle at pre-exercise, then **1 min, 20 min, 60 min, and 24 hr** post-exercise. Portion frozen in liquid N2 at −80°C; portion immediately used for SR Ca2+-ATPase assay.

**Fibre typing:** 8 μm transverse cryosections; immunohistochemistry with monoclonal antibodies: BA-D5 (1:1000, MHC-I), SC-71 (1:1000, MHC-IIa), BF-35 (1:1000, MHC-I+IIa → identifies IIx by exclusion), BF-F3 (1:1000, MHC-IIb — none found). Four types identified: I, IIA, IIA/IIX, IIX. Type I/IIA hybrids excluded (<0.1% of fibres).

**Glycogen histochemistry:** PAS staining; optical density (OD-PAS) measured in ≥25 fibres (type I, IIA/IIX) or ≥50 fibres (type IIA, IIX). Images captured with Leica DC100 at constant illumination; luminosity calibrated with 4 filters (100, 16, 8, 2% transmissivity).

**SR Ca2+-ATPase assay:** ~100 mg wet muscle homogenised in 40 mM Tris-HCl + 300 mM sucrose; spectrophotometric coupled enzyme assay (HEPES pH 7.5, EGTA, KCl, MgCl2, CaCl2, sodium azide, NADH, phosphoenol pyruvate, pyruvate kinase, lactate dehydrogenase); reaction started with ATP (4 mM final); total − basal ATPase (inhibited by 20 mM CaCl2) = SR Ca2+-ATPase. Units: μmol/min/g muscle. Performed at 37°C.

**Statistics:** Paired t-test (Stat View-J 5.0) vs. pre-exercise baseline; P < 0.05 significance; values as mean ± SD.

## Results (detailed — reproduce ALL numbers)

**Fibre composition (pre-exercise, middle gluteal):**

| Fibre type   | Area (μm²)  | % by number | % by area  |
| ------------ | ----------- | ----------- | ---------- |
| Type I       | 2,923 ± 578 | 14.5 ± 2.7  | 10.7 ± 1.6 |
| Type IIA     | 3,239 ± 576 | 45.0 ± 5.1  | 37.4 ± 6.7 |
| Type IIA/IIX | 3,909 ± 466 | 6.7 ± 0.9   | 6.7 ± 0.8  |
| Type IIX     | 5,196 ± 628 | 33.9 ± 2.7  | 45.2 ± 5.4 |

No type IIB fibre found. Type IIX has the **largest area** and **highest pre-exercise glycogen** (OD-PAS 0.28 ± 0.03).

**Pre-exercise glycogen (OD-PAS):**

- Type I: 0.21 ± 0.01
- Type IIA: 0.26 ± 0.02
- Type IIA/IIX: 0.26 ± 0.03
- Type IIX: 0.28 ± 0.03

**Glycogen recovery time course (expressed as % of pre-exercise value, mean ± SD):**

| Timepoint  | Type I  | Type IIA | Type IIA/IIX | Type IIX |
| ---------- | ------- | -------- | ------------ | -------- |
| Pre        | 100%    | 100%     | 100%         | 100%     |
| 1 min\*    | 82 ± 4% | 79 ± 5%  | 82 ± 6%      | 74 ± 7%  |
| 20 min\*   | 84 ± 5% | 82 ± 5%  | 85 ± 5%      | 79 ± 6%  |
| 60 min\*   | 80 ± 4% | 77 ± 4%  | 78 ± 5%      | 73 ± 4%  |
| 24 hr (ns) | 97 ± 8% | 98 ± 9%  | 99 ± 10%     | 97 ± 14% |

\*P < 0.05 vs. pre-exercise. Depletion: 15–27% across all fibre types at 1 min.
**Full recovery by 24 hr in all fibre types** (values not significantly different from pre-exercise).

Note: Authors state reduction depends on exercise **time** rather than intensity — comparison to prior studies: Byrd et al. 1989 (24% depletion after VO2max until fatigue, recovery by 60 min); Pratt et al. 2007 (44% depletion after incremental run to fatigue, only 67% restored at 24 hr). The present study shows incomplete recovery at 60 min but full recovery at 24 hr, consistent with moderate (not exhaustive) depletion.

**SR Ca2+-ATPase activity (μmol/min/g muscle):**

| Timepoint    | Mean ± SD    |
| ------------ | ------------ |
| Pre-exercise | 17.61 ± 2.02 |
| 1 min post   | 16.75 ± 1.10 |
| 20 min post  | 16.87 ± 1.72 |
| 60 min post  | 18.34 ± 1.68 |
| 24 hr post   | 18.58 ± 2.66 |

None significantly different from pre-exercise. Individual variation: 4/6 horses showed decreasing trend; 2/6 showed increasing trend. No significant correlation between SR Ca2+-ATPase and glycogen change in any fibre type at any timepoint.

**Mechanistic note:** Mishima et al. 2006 reported that 30% reduction in muscle glycogen did not influence SR Ca2+-ATPase activity — consistent with the present ~18–27% depletion being insufficient to affect Ca2+-ATPase. SR glycogen depletion (local to SR membrane) was not measured; authors speculate only SR-local glycogen depletion would affect Ca2+-ATPase via glycogen phosphorylase conformational changes (Lees et al. 2001, 2004).

## Discussion & interpretation

The glycogen results confirm that short-duration high-intensity exercise (~50 sec at 90% VO2max) in Thoroughbreds produces 15–27% depletion recoverable within 24 hr. This contrasts with longer or more exhaustive exercise (Pratt et al.: still 33% depleted at 24 hr after incremental fatigue protocol). The implication is that exercise duration, not intensity per se, determines recovery timecourse for glycogen. The SR Ca2+-ATPase null result is discussed in the context of conflicting literature across species and protocols (increases, decreases, and no changes all reported). Authors invoke multiple confounding factors: phosphorylation state, NO/cGMP signalling, HSP72 upregulation, Ca2+-ATPase isoform switching, pH changes from lactate, body temperature rise, and reactive oxygen species — making Ca2+-ATPase a poor standardisable marker. The recommendation is that glycogen-based or substrate-availability approaches remain the practical route to muscle fatigue assessment in horses.

## Limitations

- Very small n = 6 horses; all males; no control for fitness level beyond pasture maintenance.
- Single exercise protocol (50 sec × 2 at 13 m/s); real races range from ~60 sec (1,200 m sprint) to >3 min (3,000 m staying race) — these durations exceed the study protocol, implying greater glycogen depletion and potentially longer recovery.
- SR Ca2+-ATPase measured from whole-muscle homogenate; cannot attribute to specific fibre types.
- SR glycogen was not measured — the proposed mechanism (SR-local glycogen → Ca2+-ATPase) remains untested.
- All horses rested on pasture beforehand; fitness-level heterogeneity in a real training population may alter timecourses.

## Feature-engineering notes for the model

- `days_since_last_race` — integer days between current race date and previous race date — derivable from JRA race records — expected effect: nonlinear, with diminished returns beyond ~14 days; acute (<7 days) associated with residual fatigue; threshold at ~1–2 days for maximal acute depletion risk — data availability: fully available from race history
- `distance_adjusted_recovery_index` — `days_since_last_race` weighted by previous race distance (longer distance → more depletion → longer recovery needed); e.g., `days_since / (prev_distance_m / 1200)` — derivable from race records — expected sign: larger index → better recovery state — data availability: fully available
- `cumulative_race_load_30d` — total race distances (m) in last 30 days — derivable from race history — expected effect: higher cumulative load → more residual fatigue → negative finishing position effect — data availability: fully available
- `fibre_type_proxy_sprint_ratio` — fraction of career wins at ≤1,400 m vs. ≥2,000 m, or pedigree sprint index — proxies for IIX vs. I fibre proportion — data availability: computable from race records / pedigree
- **Do NOT use** SR Ca2+-ATPase as a feature candidate — paper establishes it is not a reliable fitness marker in Thoroughbreds.

## Key references / follow-up leads

- Byrd, S.K. et al. 1989. "Altered sarcoplasmic reticulum function after high-intensity exercise." _J. Appl. Physiol._ 67: 2072–2077.
- Pratt, S.E. et al. 2007. "Time course of insulin sensitivity and skeletal muscle glycogen synthase activity after a single bout of exercise in horses." _J. Appl. Physiol._ 103: 1063–1069.
- Rivero, J.L.L. et al. 2007. "Effects of intensity and duration of exercise on muscular responses to training of thoroughbred racehorses." _J. Appl. Physiol._ 102: 1871–1882.
- Lees, S.J. et al. 2001. "Glycogen and glycogen phosphorylase associated with sarcoplasmic reticulum: effects of fatiguing activity." _J. Appl. Physiol._ 91: 1638–1644.
- Mishima, T. et al. 2006. "Effects of reduced glycogen on structure and in vitro function of rat sarcoplasmic reticulum Ca2+-ATPase." _Pflugers Arch._ 452: 117–123.
- Rivero, J.L.L. et al. 1993. "Muscle fiber type composition and fiber size in successfully and unsuccessfully endurance-raced horses." _J. Appl. Physiol._ 75: 1758–1766.
- Yamano, S. et al. 2002. "Effect of growth and training on muscle adaptation in Thoroughbred horses." _Am. J. Vet. Res._ 63: 1408–1412.
