# Diurnal Rhythms of R-R Interval and R-R Interval Variability in the Young Thoroughbred Horse

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 5(3): 83–86, 1994                                                                                                                                                                                                                                                                                                         |
| docid                          | `5_3_83`                                                                                                                                                                                                                                                                                                                                 |
| Article type                   | Research Article                                                                                                                                                                                                                                                                                                                         |
| Authors                        | Yoshiki Yamaya, Katsuyoshi Kubo, Akio Amada                                                                                                                                                                                                                                                                                              |
| Affiliations                   | ¹Laboratory of Veterinary Surgery, College of Agriculture and Veterinary Medicine, Nihon University, 1866 Kameino, Fujisawa-shi, Kanagawa 252, Japan; ²Equine Research Institute, Japan Racing Association, 5–27–7 Tsurumaki, Setagaya-ku, Tokyo 154, Japan; ³Blood Horse Training Center, 1–1–10 Toranomon, Minato-ku, Tokyo 105, Japan |
| Received / Accepted / Released | Submitted August 23, 1994; accepted November 11, 1994                                                                                                                                                                                                                                                                                    |
| Keywords                       | R-R interval variability, young horse                                                                                                                                                                                                                                                                                                    |
| PDF                            | https://www.jstage.jst.go.jp/article/jes1994/5/3/5_3_83/_pdf/-char/en                                                                                                                                                                                                                                                                    |

## Abstract (verbatim)

> Diurnal rhythms of the mean R-R interval and the coefficient of variation (CV) of R-R intervals in eleven young Thoroughbred horses (2-year-old) were examined by periodic analysis to see whether the periodicity of the rhythms change with growing and physical training. On the diurnal rhythm of the mean R-R interval, a single spectrum (14.5 hr–19.3 hr periodicity) was extracted and the power, amplitude and the middle estimating static of rhythm (MESOR) of this periodicity increased with growing and physical training. On the other hand, two periodicities (14.5 hr–15.8 hr and 5.6 hr–7.5 hr periodicity) were found in the diurnal rhythm of the R-R interval variability but no alteration with growing and physical training was recognized. This suggests that the diurnal rhythm of the mean R-R interval and that of the R-R interval variability are extracted and reflect the different factors affecting autonomic nervous activity. Monitoring the diurnal rhythm of the R-R interval, furthermore, may enable us to understand the progression of growing and physical training effects.

## Relevance to finishing-position (着順) prediction

Feature family: **C (exercise-physiology/fitness)**.

This paper tracks cardiac autonomic adaptation in 2-year-old Thoroughbreds longitudinally across their first training season. The central finding — that the MESOR of the mean R-R interval (i.e., mean resting cardiac cycle length, the inverse of resting heart rate) progressively lengthens from 1,419 ms (January, ~42 bpm) to 1,556 ms (August, ~39 bpm) with training — provides the physiological basis for using resting heart rate and heart rate–speed relationship slope as within-season fitness features. The amplitude of the diurnal R-R rhythm grows from 42.8 ms to 226.9 ms, reflecting increasing vagal tone as aerobic conditioning advances.

For a finishing-position prediction pipeline that uses 2-year-old JRA/NAR horses, "months since training start" or "age in months at race date" partially captures this cardiac maturation trajectory. The exercise heart rate slope (beats/min per m/min of running speed) decreases from 0.23 in January to 0.18 in July, a 22% improvement in cardiac economy — this slope from workout data, if available, is the most direct fitness biomarker supported by this paper.

Critically, the R-R interval variability (HRV/CV) does NOT change with training (two stable periodicities at ~15–16 h and ~5.6–7.5 h, powers 3–13%, no progression), meaning HRV is a structural autonomic property rather than a fitness state indicator. This argues against using HRV as a primary performance feature while supporting resting heart rate (MESOR) and HR-speed slope as the more informative cardiac metrics.

The practical implication for race-day models: a horse in its first training season (2-year-old in January) has substantially higher resting heart rate and a steeper HR-speed slope than the same horse in August, independent of individual genetic differences. Age-in-months or season-stage features encode this — the largest fitness change occurs between January and August of the first training year.

## Background & objective

Horses at rest have heart rates of 30–40 bpm; after pharmacological autonomic blockade, intrinsic rate is 90–100 bpm; during sub-maximal exercise, >220 bpm. Heart rate is regulated by the autonomic nervous system and therefore fluctuates with sympathetic/parasympathetic balance. Circadian and ultradian rhythms in HR are well characterised in humans and reflect autonomic variability; the authors had previously developed a MESA criterion for diurnal rhythm analysis in a single horse [ref 17].

In 2-year-old Japanese Thoroughbreds, intensive physical training begins for the first time. It is known that foals are regulated more sympathetically than mares [ref 8] but the developmental trajectory from sympathetic dominance to the vagal dominance observed in trained adult horses was uncharacterised.

**Hypothesis:** The periodicity, power, amplitude, and MESOR of the diurnal R-R rhythm will change with physical training and growing as 2-year-olds mature into race-ready horses. R-R variability (CV) may show a different pattern from mean R-R.

**Aim:** Longitudinal documentation of diurnal R-R rhythm at three time points (beginning, middle, and end of first training season) using MESA, with concurrent exercise HR testing.

## Materials & methods

**Subjects:** 11 Thoroughbred horses (5 males, 6 females), all 2-year-olds. Mean body weights: January 441 ± 24 kg (20 ± 1 months old), May 445 ± 28 kg (24 ± 1 months old), August 450 ± 33 kg (27 ± 1 months old).

**Housing and management:** Horses individually stabled; free movement within standard stalls. Feeding at 06:00 and 16:30; water ad libitum. Training: 09:00–11:00, Monday to Friday.

**ECG recording protocol:** Cardio Tape System (Fukuda Denshi Co., Japan) recorder. Bipolar chest lead: positive electrode on left thorax at highest point of olecranon; negative electrode on left side of withers. Recorded intermittently for **30 seconds every 10 minutes** from **15:00 to 08:00** (17-hour window) at each of three time points: January, May, and August 1991.

**R-R measurement:** Tape played back on ECG recorder; R-R interval measured with R-R interval analyser (TM-55, Cerx Co., Japan). Mean R-R interval and CV of R-R intervals calculated from each 30-second epoch. Time series for MESA = means of the 11-horse group at each 10-minute interval.

**MESA parameters:** Order of autoregression model = number of data points divided by 2. Power (spectral density) normalised by total sample variance. Amplitude and MESOR of each dominant periodicity computed by non-linear cosine fitting method [ref 12].

**Concurrent exercise tests:** Treadmill or track exercise test at each time point to establish the HR vs. running speed relationship:

- January: Y = 0.23X + 6.10 (X range: 350–600 m/min)
- March: Y = 0.24X + 46.14 (X range: 350–600 m/min)
- May: Y = 0.21X + 53.08 (X range: 500–800 m/min)
- July: Y = 0.18X + 68.69 (X range: 500–800 m/min)

where Y = heart rate (beats/min), X = running speed (m/min).

## Results (detailed — reproduce ALL numbers)

**Exercise heart rate response (training adaptation):**

| Month   | HR–speed slope (beats/min per m/min) | Speed range (m/min) | Intercept |
| ------- | ------------------------------------ | ------------------- | --------- |
| January | 0.23                                 | 350–600             | 6.10      |
| March   | 0.24                                 | 350–600             | 46.14     |
| May     | 0.21                                 | 500–800             | 53.08     |
| July    | 0.18                                 | 500–800             | 68.69     |

The slope decreased from 0.23 (January) to 0.18 (July), indicating that each unit increase in running speed elicited progressively less heart rate increase — the hallmark of aerobic training adaptation. The speed range also shifted upward (350–600 → 500–800 m/min), reflecting higher attainable speeds with training.

**Diurnal rhythm of MEAN R-R interval (Fig. 1 — MESA power spectra; Fig. 2 — cosine curves):**

| Month   | Periodicity (hr) | Spectral power (%) | Amplitude (ms) | MESOR (ms) |
| ------- | ---------------- | ------------------ | -------------- | ---------- |
| January | 14.5             | 10.6               | 42.8           | 1,418.7    |
| May     | 19.3             | 40.2               | 89.8           | 1,526.3    |
| August  | 17.4             | 95.1               | 226.9          | 1,555.5    |

Key observations:

- A **single** dominant spectral peak was extracted at all three time points (MESA detected one primary rhythm in mean R-R interval)
- Power increased dramatically: 10.6% → 40.2% → 95.1% — from a weak irregular signal to a strongly periodic rhythm dominating >95% of total variance
- Amplitude increased ~5.3-fold (42.8 → 226.9 ms)
- MESOR increased by 136.8 ms (1,419 → 1,556 ms), equivalent to resting heart rate decreasing from approximately 42.3 bpm to approximately 38.6 bpm — progressive training bradycardia
- **Temporal pattern (consistent all 3 months):** Mean R-R interval minimally shortened at approximately 19:00–21:00 (highest sympathetic tone, lowest parasympathetic) and maximally lengthened toward approximately 03:00–05:00 (highest vagal tone, lowest HR)

**Diurnal rhythm of R-R INTERVAL VARIABILITY / CV (Fig. 3 — MESA spectra; Fig. 4 — cosine curves):**

| Month   | Periodicity 1 (hr) | Power 1 (%) | Amplitude 1 / MESOR 1 | Periodicity 2 (hr) | Power 2 (%) | Amplitude 2 / MESOR 2 |
| ------- | ------------------ | ----------- | --------------------- | ------------------ | ----------- | --------------------- |
| January | 15.8               | 9.3         | 0.6% / 5.1%           | 6.4                | 9.5         | 0.6% / 5.1%           |
| May     | 15.8               | 13.0        | 0.6% / 4.3%           | 7.5                | 4.2         | 0.4% / 4.3%           |
| August  | 14.5               | 3.4         | 0.8% / 6.1%           | 5.6                | 8.5         | 0.9% / 6.0%           |

Key observations:

- **Two** spectral peaks consistently extracted from R-R variability (vs. one for mean R-R interval)
- Powers remained low (3.4–13.0%) and showed no consistent progression with training — unlike the dramatic 10.6% → 95.1% progression for mean R-R
- Amplitude values were small (0.4–0.9%) relative to MESORs (4.3–6.1%) — the variability rhythm is weak and irregular
- R-R interval variability decreased around 17:00–20:00 and increased toward 03:00–05:00 (similar temporal pattern to mean R-R but much less pronounced)
- **Conclusion: R-R interval variability (HRV/CV) did NOT change systematically with growing and physical training**

## Discussion & interpretation

The marked increase in spectral power of the mean R-R diurnal rhythm (10.6% → 95.1%) reflects the emergence of a strong, regular cardiac autonomic cycle as training progresses — in January, resting heart rate shows little diurnal organisation; by August, the rhythm accounts for essentially all diurnal variance. This is attributed to increasing parasympathetic (vagal) tone and/or decreasing sympathetic tone as training-induced bradycardia develops, paralleling observations in human athletes [refs 4, 5, 15].

The prolongation of minimal mean R-R interval (i.e., resting bradycardia) during morning hours (03:00–05:00) is the driver of the MESOR increase. The authors connect this to the known progression from sympathetically-dominated newborn foal heart rate [ref 8] toward the vagally-dominated adult Thoroughbred with well-documented exercise-induced bradyarrhythmias [refs 3, 6, 10].

The lack of change in R-R interval variability (CV) is interpreted as reflecting that CV predominantly reflects **structural** autonomic properties — specifically, parasympathetic (vagal) tone mediating beat-to-beat HR fluctuations — which are determined by intrinsic cardiac mechanisms and differ from the **state-dependent** adaptation reflected in mean HR. The CV shows circadian rhythmicity (comparable to humans [ref 13] and diabetic autonomic neuropathy assessment [ref 16]) but this rhythm does not progress with training. The two periodic components (~15–16 h and ~5.6–7.5 h) in R-R variability may reflect both central (hypothalamic circadian clock [ref 18]) and peripheral (postural/postural change-induced) autonomic modulation — the drowsing/standing alternation known in horses [ref 2] would produce posture-dependent HR variability changes.

## Limitations

- Group-level MESA analysis: the time series analysed was the mean of 11 horses — individual-horse diurnal patterns were not examined; between-horse variance in adaptation rate is unknown
- 17-hour recording window (15:00–08:00) means the apparent "periodicity" (14.5–19.3 hr) is constrained by the window length rather than representing a true circadian period; cannot confirm whether the rhythm is truly sub-circadian or whether the full 24-hour cycle would show a different dominant period
- Exercise test data available for January/March/May/July but ECG recording only at January/May/August — misalignment between HR-speed slope assessment and ECG recording time points
- n=11 horses; body weight changed little (441 → 450 kg) suggesting the group was relatively homogeneous in growth; broader variation in developmental rate may produce different results
- No control group (unexercised horses) to separate growing effects from training effects

## Feature-engineering notes for the model

- `hr_speed_slope` — regression coefficient of HR (bpm) on running speed (m/min) from workout tests — source: JRA workout treadmill/track data (if available) — expected effect: slope decreases from ~0.23 to ~0.18 over first training season (22% reduction); lower slope = better aerobic fitness = positive for performance; most direct fitness biomarker from this paper
- `resting_hr_mesor` — mean resting heart rate from overnight ECG recording — source: JRA monitoring data (limited availability) — expected effect: decreases from ~42 bpm to ~39 bpm over first season; lower resting HR = greater vagal tone = better training adaptation
- `age_months_at_race` — horse age in months at race date — source: `horse.birth_date` + `race.date` — expected effect: 2-year-olds (24 months) have substantially higher resting HR and lower cardiac economy than same horses at 27 months; largest fitness change occurs Jan–Aug of first training year; use as numeric feature, not just integer years
- `season_stage_first_year` — ordinal: early (Jan–Feb, ~20–22 mo), mid (Mar–May, ~22–25 mo), late (Jun–Aug, ~25–28 mo) of first training season — source: derived from race date and birth date — expected effect: late > mid > early in cardiac economy; distinguishes within-year fitness progression
- `training_months_elapsed` — months since first trackwork/official training start — source: JRA training records — expected effect: monotonically improves cardiac fitness; interaction with `age_months` (training effect separate from pure maturation)
- `rr_variability_cv` — coefficient of variation of R-R intervals — source: cardiac monitoring — expected effect: NOT a training-state indicator (constant across training); structural autonomic property; low predictive value for within-season performance change; use with caution
- **NOTE**: The HR–speed slope decrease from January (0.23) to July (0.18) corresponds to the same horse progressing from race-debut to mid-season fitness; a proxy derived from age + training duration should partially capture this without requiring actual HR measurement

## Key references / follow-up leads

- Yamaya, Y., Kubo, K., Amada, A., and Sato, K. 1993. Criterion for maximum entropy spectral analysis of heart rate diurnal rhythm in a horse. _Jpn. J. Equine Sci._ 4: 73–77 — methodological precursor by same first author; MESA criterion development
- Matsui, K., Sugano, S., and Amada, A. 1986. Heart rate and ECG response to twitching in Thoroughbred foals and mares. _Jpn. J. Vet. Sci._ 48: 305–312 — foals more sympathetically regulated than mares; cited for baseline autonomic state
- Matsui, K., and Sugano, S. 1989. Relation of intrinsic heart rate and autonomic nervous tone to resting heart rate in the young and the adult of various domestic animals. _Jpn. J. Vet. Sci._ 51: 29–34 — intrinsic heart rate vs. autonomic modulation across ages
- Ekblom, B., Kilbom, A., and Soltysiak, J. 1973. Physical training, bradycardia and the autonomic nervous system. _Scand. J. Clin. Lab. Invest._ 32: 251–256 — training-induced bradycardia and autonomic mechanism (human)
- Akselrod, A. et al. 1981. Power spectrum analysis of heart rate fluctuation: a quantitative probe of beat-to-beat cardiovascular control. _Science_ 213: 220–222 — foundational HRV spectral analysis
- Wheeler, T., and Watkins, P.J. 1973. Cardiac denervation in diabetes. _Br. Med. J._ 4: 584–586 — HRV/CV as autonomic function index in diabetic neuropathy; referenced to explain CV methodology
