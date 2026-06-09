# Differences in Muscle Fiber Recruitment Patterns between Continuous and Interval Exercises

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                         |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 21(4): 59–65, 2010                                                                                                                                                                                                                                                             |
| docid                          | `21_4_59`                                                                                                                                                                                                                                                                                     |
| Article type                   | Original Article                                                                                                                                                                                                                                                                              |
| Authors                        | Seiko Yamano, Minako Kawai, Yoshio Minami, Atsushi Hiraga, Hirofumi Miyata                                                                                                                                                                                                                    |
| Affiliations                   | (1) Science Research Center, Yamaguchi University, Yoshida 1677-1, Yamaguchi 753-8515; (2) Biological Sciences, Graduate School of Medicine, Yamaguchi University (same address); (3) The Equine Research Institute, Japan Racing Association, 321-4 Tokami-cho, Utsunomiya, Tochigi 320-0856 |
| Received / Accepted / Released | Accepted: July 26, 2010                                                                                                                                                                                                                                                                       |
| Keywords                       | glycogen, interval exercise, muscle fiber, Thoroughbred                                                                                                                                                                                                                                       |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/21/4/21_4_59/_pdf/-char/en                                                                                                                                                                                                                           |

## Abstract (verbatim)

> We evaluated differences in muscle fiber recruitment patterns between continuous and interval training to develop an optimal training program for Thoroughbred horses. Five well trained female thoroughbred horses (3–4 years old) were used. The horses performed two different exercises on a 10% inclined treadmill: 90%V̇O₂max for 4 min (continuous) and 90% V̇O₂max for 2 min × 2 times with 10-min interval (interval). Muscle samples were obtained from the middle gluteal muscle before and immediately after the exercises. Four muscle fiber types (type I, IIA, IIA/X, and IIX) were immunohistochemically identified, and the optical density of periodic acid Schiff staining (OD-PAS) in each fiber type and glycogen content of the muscle sample were determined by quantitative histochemical and biochemical procedures, respectively. No significant differences were found in the OD-PASs and glycogen contents between the continuous and interval exercises, but the decreases in OD-PAS of fast-twitch muscle fibers were obvious after interval as compared to continuous exercise. Interval exercise may be a more effective training stimulus for the glycolytic capacity of fast-twitch muscle fiber. The data about muscle fiber recruitment can provide significant insights into the optimal training program not only for thoroughbred horses, but also for human athletes.

## Relevance to finishing-position (着順) prediction

Feature family: **C — exercise-physiology / fitness**.

This study characterises glycogen depletion patterns across all four middle-gluteal muscle fibre types (Types I, IIA, IIA/IIX, IIX) during both continuous and interval exercise at 90% VO₂max in Thoroughbreds. The key prediction-relevant findings are:

1. **Baseline fibre composition** (Type I 11.7%, IIA 38.3%, IIA/IIX 4.4%, IIX 45.6%) establishes that nearly half the middle gluteal in well-trained Thoroughbreds is fast-twitch glycolytic (IIX) fibre — underpinning the breed's sprint/anaerobic performance capacity.
2. **Interval exercise depletes fast-fibre glycogen more than continuous** exercise at the same intensity and total duration: IIX OD-PAS decreases to 66.1% (interval) vs. 77.4% (continuous), a ~11 pp greater depletion of the sprint-relevant fibre pool (P=0.08–0.15, not significant, but consistent trend across all types).
3. **Speed at 90% VO₂max = 8.9 m/sec** (mean; individual range 8.4–9.4 m/sec) — this speed is the practical threshold for triggering meaningful fast-fibre glycogen depletion in race conditioning. Racing speeds in sprint races (~14–17 m/sec for JRA) far exceed this, meaning actual race exercise depletes IIX fibres substantially more than these training bouts.

For the pipeline: the paper provides physiological rationale for using race intensity (finish speed, margin, speed rating relative to class) as a proxy for glycogen depletion and recovery demand. Horses running at high intensity in their last race may need longer recovery. The IIX-fibre–glycogen mechanism also supports the hypothesis that a horse running in a shorter/faster race (sprint) may experience more IIX depletion per unit time than one running a longer/slower race (route), justifying a `last_race_distance_intensity` interaction feature.

## Background & objective

Thoroughbred racehorses typically run at full speed for under 3 minutes. Effective training must safely stimulate fast-twitch (Type II) fibre adaptations without causing overtraining injury. Continuous high-intensity training at 100% VO₂max for 4 min was previously shown by the same group (Yamano et al., 2006) to sufficiently recruit Type II fibres, but this protocol carries injury risk for practical use. The question is whether dividing the same intensity and total duration into two shorter interval bouts (2 min × 2 with 10-min walking recovery) achieves equivalent fibre recruitment. If yes, the interval protocol can replace continuous high-intensity training with less injury risk while still delivering the anaerobic stimulus.

## Materials & methods

**Subjects:** n=5 well-trained female Thoroughbred horses, aged 3–4 years, body weight 449 ± 8 kg. All acclimatised to treadmill exercise for 3 months of conventional training before experiments. All procedures approved by the Animal Experiment Committee, Equine Research Institute, Tochigi Branch.

**VO₂max measurement:** Incremental exercise test (IET) one week before experiments. Protocol: 2 min walking at 1.8 m/sec + 5 min trotting at 3.6 m/sec + incremental cantering starting at 6 m/sec, then 8 m/sec, then +1 m/sec/minute until exhaustion. Open-flow O₂ and CO₂ measurement system (Vice Medical, Chiba). VO₂max determined at levelling-off point by regression line analysis.

**Individual horse characteristics (Table 1):**

| Horse | BW (kg) | Age (yr) | VO₂max (ml/min/kg) | Speed at 90%VO₂max (m/sec) |
| ----- | ------- | -------- | ------------------ | -------------------------- |
| 1     | 457     | 4        | 181                | 9.1                        |
| 2     | 436     | 3        | 184                | 8.7                        |
| 3     | 452     | 3        | 185                | 9.4                        |
| 4     | 452     | 3        | 191                | 9.1                        |
| 5     | 449     | 3        | 186                | 8.4                        |
| Mean  | 449     | —        | **185**            | **8.9**                    |
| SD    | 8       | —        | 4                  | 0.4                        |

**Exercise protocols (10% incline treadmill):**

- **Continuous:** 90% VO₂max for **4 min** total
- **Interval:** 90% VO₂max for **2 min** × 2 bouts with **10-min walking recovery** (1.8 m/sec) between bouts
- Same speed for both protocols (horse-specific 90% VO₂max speed). Protocols separated by 4 days.

**Muscle sampling:** Middle gluteal biopsy (5 cm depth), same anatomical location as Bergstrom et al. (1967) method. Pre-exercise (Pre) and immediately post-exercise (Post) in both protocols. Frozen in melting isopentane / liquid nitrogen; stored at −80°C; analysed simultaneously.

**Histochemical and immunohistochemical analysis:**

- Eight 8-µm transverse cryosections per sample
- 4 sections: PAS staining for glycogen (periodic acid 0.5%, 5 min; Schiff's reagent, 15 min)
- Optical density of PAS staining (OD-PAS): luminosity at 256 grey levels using Nikon DS-U1 image system; calibrated with 4 filters (100, 25, 6, 1.5% transmissivity); expressed as relative value to pre-exercise OD-PAS
- 4 sections: immunohistochemistry with antibodies: BA-D5 (anti-MHC-I), SC-71 (anti-MHC-IIa), BF-F3 (anti-MHC-IIb), BF-35 (anti-MHC-IIx). MHC-IIb isoform was absent in all horses. Fibre classification: Type I, IIA, IIA/IIX, IIX. OD-PAS and fibre area measured in ≥25 fibres (Types I and IIA/IIX) and ≥50 fibres (Types IIA and IIX) per sample. Type I/IIA fibres (<0.1%) were excluded.

**Biochemical analysis:**

- Glycogen content: Anthrone method after alkaline digestion (boiled in 30% KOH, 100°C, 30 min → ethanol precipitation → centrifugation → dilution → anthrone reagent → OD at 620 nm)
- Calibrated with glucose standard (0, 12.5, 25, 50, 100 µg/ml); measured in triplicate

**Statistics:** Non-paired t-test for exercise stage (pre vs. post) and exercise type (continuous vs. interval) comparisons. P<0.05 significant. Values reported as mean ± SD.

## Results (detailed — all numbers reproduced)

**Baseline fibre composition (pre-exercise, first bout):**

- Type I: 11.7 ± 4.1%
- Type IIA: 38.3 ± 5.9%
- Type IIA/IIX: 4.4 ± 0.7%
- Type IIX: 45.6 ± 9.1%

Fast-twitch (Type II) total: ~88.3% of all fibres. Type IIX alone comprises 45.6% — the single largest fibre-type category, underscoring the Thoroughbred's anaerobic sprint capacity.

**OD-PAS relative values post-exercise (% of pre-exercise):**

| Fibre type   | Continuous | Interval | Difference (pp) |
| ------------ | ---------- | -------- | --------------- |
| Type I       | 71.6%      | 60.4%    | −11.2 pp        |
| Type IIA     | 71.7%      | 59.9%    | −11.8 pp        |
| Type IIA/IIX | 77.9%      | 65.1%    | −12.8 pp        |
| Type IIX     | 77.4%      | 66.1%    | −11.3 pp        |

- All post-exercise values were **significantly lower than pre-exercise** in both protocols (paired t-test, each P<0.05)
- No **significant differences** between continuous and interval protocols (unpaired t-test: P=0.08–0.15 for each fibre type)
- However, interval consistently showed lower OD-PAS (more glycogen depletion) in every fibre type, with fast fibres (IIA/IIX and IIX) showing the largest absolute tendency toward greater depletion under interval

**Glycogen content (biochemical, whole-muscle):**

- Continuous: 69.5% of pre-exercise value (P<0.05 vs. pre)
- Interval: 55.6% of pre-exercise value (P<0.05 vs. pre)
- Difference between protocols: P=0.07 (not significant, but interval consistently lower)
- Both significant decreases from pre-exercise confirmed biochemically (Anthrone assay)

**Summary:** Both protocols produced statistically significant glycogen depletion. Interval produced ~14 pp greater whole-muscle glycogen depletion than continuous (55.6% vs. 69.5%) and systematically greater OD-PAS reduction in all fibre types, but differences did not reach statistical significance (n=5, insufficient power).

## Discussion & interpretation

The authors attribute the greater fast-fibre glycogen depletion in interval exercise to the **oxygen deficit** at the restart of each interval bout: the cardiorespiratory system cannot supply sufficient oxygen fast enough at the onset of high-intensity exercise, forcing working muscles into anaerobic (glycolytic) metabolism, which preferentially recruits IIX fibres rich in glycolytic enzymes. This is not present in the continuous protocol, where the aerobic system has already reached steady state before the critical recruitment of IIX fibres.

The practical training implication: interval training at 90% VO₂max can deliver a greater anaerobic stimulus to fast-twitch fibres than continuous exercise at the same intensity and total duration, with the additional safety benefit of two shorter high-intensity bouts (each 2 min, less cumulative fatigue than 4 continuous minutes) — consistent with Lovell & Rose (1991) showing interval training increased lactate dehydrogenase activity while continuous training did not, and Harkins & Kammerling (1991) reporting 7 months of interval training in Thoroughbreds without injury.

The study also notes that fast-fibre (Type IIX) depletion is slightly lower than slow-fibre (Type I and IIA) depletion within each protocol (77.4% vs. 71.7% for continuous; 66.1% vs. 59.9% for interval) — this is consistent with the mixed aerobic-anaerobic nature of 90% VO₂max exercise and is not simply an "all-fast-twitch" bout.

## Limitations

- Very small sample size (n=5), all female, aged 3–4 years. Sex and age effects on fibre-type composition and glycogen kinetics are unknown.
- Exercise protocols involve only 90% VO₂max; race speeds (typically 120–150% VO₂max equivalent in thoroughbreds) would produce substantially greater and faster IIX depletion.
- No post-exercise time course: only immediate post-exercise biopsies were taken; glycogen resynthesis rate is not characterised.
- No performance outcome linked to these training protocols; the model's implication (interval → better sprint performance) is inferred, not directly tested.
- Biopsy from middle gluteal only; muscles critical for stride generation (hindquarters, hamstrings) may differ.
- P-values for the key between-protocol comparisons are 0.07–0.15 (non-significant); with n=5, even true differences of 10–15 pp would require n~20 for 80% power.

## Feature-engineering notes for the model

- `last_race_speed_percentile` — finishing speed (distance / finishing time) normalised to class/distance-adjusted percentile — proxy for the intensity level at which the horse last exercised; sprint races at high percentile → high IIX glycogen depletion → greater recovery demand — source: finishing times, distances from race records
- `last_race_distance_type` — categorical: sprint (<1,400 m), middle (1,400–2,000 m), long (>2,000 m) — longer races at lower intensity deplete IIA more than IIX; shorter sprint races deplete IIX; recovery profile differs — source: race distance from records
- `days_since_last_race` — integer days — the glycogen resynthesis window: complete IIX repletion after near-maximal depletion typically requires 48–72+ hr; very short intervals (<5 days) may leave IIX glycogen sub-optimal — interaction with last_race_speed_percentile — source: race history
- `interval_training_proxy` — fraction of recent races separated by 7–14 days (typical interval training rhythm) vs. continuous long-rest pattern — horses raced in consistent short-interval patterns may have better interval-trained fast-fibre conditioning — source: race entry history
- `vo2max_proxy` — best recent speed rating relative to course record — proxy for VO₂max (higher conditioned fitness level); mean VO₂max in this study at peak conditioning: 185 ml/min/kg at 8.9 m/sec — source: speed ratings from race records
- **DO NOT** use glycogen depletion or OD-PAS directly (not available from race records). All features above are indirect proxies validated by the physiological mechanism described in this paper.

## Key references / follow-up leads

- Yamano et al. (2006) Res. Vet. Sci. 80: 109–115 — recruitment patterns of muscle fibre types at 60–100% VO₂max in Thoroughbreds; direct predecessor establishing that 100% VO₂max × 4 min recruits Type II fibres
- Eto et al. (2004) Res. Vet. Sci. 76: 139–144 — high-intensity training effects on anaerobic capacity of middle gluteal muscle in Thoroughbreds; glycolytic enzyme activity increases
- Lovell & Rose (1991) In: Equine Exercise Physiology 3, pp. 215–222 — interval training (not continuous) increases lactate dehydrogenase activity in horse skeletal muscle
- Harkins & Kammerling (1991) J. Equine Vet. Sci. 11: 237–242 — 7-month interval treadmill training without injury in Thoroughbreds
- Rivero et al. (1993) J. Appl. Physiol. 75: 1758–1766 — fibre size in successful vs. unsuccessful endurance-raced horses; muscle fibre composition as performance predictor
- Hodgson et al. (1984) Res. Vet. Sci. 36: 169–173 — glycogen depletion patterns in horses performing maximal exercise; foundational reference
- McCarthy & Jeffcott (1988) Equine Vet. J. Suppl. 6: 88–92 — treadmill exercise effects on bone quality monitored non-invasively; interval training improves metacarpal bone quality in Standardbreds
