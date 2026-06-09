# Identification of metabolomic changes in horse plasma after racing by liquid chromatography-high resolution mass spectrometry as a strategy for doping testing

## Metadata

| Field                          | Value                                                                                                                                                                   |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 30(3): 55–61, 2019                                                                                                                                       |
| docid                          | `30_1916`                                                                                                                                                               |
| Article type                   | Full Paper                                                                                                                                                              |
| Authors                        | Toshiki Ueda, Teruaki Tozaki, Satoshi Nozawa, Kenji Kinoshita, Hitoshi Gawahara                                                                                         |
| Affiliations                   | Drug Analysis Department, Laboratory of Racing Chemistry, Tochigi 320-0851, Japan; Genetic Analysis Department, Laboratory of Racing Chemistry, Tochigi 320-0851, Japan |
| Received / Accepted / Released | May 29, 2019 / August 1, 2019 / 2019                                                                                                                                    |
| Keywords                       | doping control, gene and cell doping, horse racing, liquid chromatography-high resolution mass spectrometry, metabolomics                                               |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/30/3/30_1916/_pdf/-char/en                                                                                                     |

## Abstract (verbatim)

> Recently, the illegal use of novel technologies, such as gene and cell therapies, has become a great concern for the horseracing industry. As a potential way to control this, metabolomics approaches that comprehensively analyze metabolites in biological samples have been gaining attention. However, it may be difficult to identify metabolic biomarkers for doping because physiological conditions generally differ between resting and exercise states in horses. To understand the metabolic differences in horse plasma between the resting state at training centres and the sample collection stage after racing for doping test (SAD), we took plasma samples from these two stages (n=30 for each stage) and compared the metabolites present in these samples by liquid chromatography-high resolution mass spectrometry. This analysis identified 5,010 peaks, of which 1,256 peaks (approximately 25%) were annotated using KEGG analysis. Principal component analysis showed that the resting state and SAD groups had entirely different metabolite compositions. In particular, the levels of inosine, xanthosine, uric acid, and allantoin, which are induced by extensive exercise, were significantly increased in the SAD group. In addition, many metabolites not affected by extensive exercise were also identified. These results will contribute to the discovery of biomarkers for detecting doping substances that cannot be detected by conventional methods.

## Relevance to finishing-position (着順) prediction

Feature family: **C — exercise-physiology/fitness**. Although the primary purpose of this paper is doping control, it characterises the exercise-induced metabolomic shift in JRA Thoroughbred plasma with high quantitative precision. The principal finding — a systematic 7-metabolite increase in purine catabolism products after racing, with inosine rising 35-fold and xanthosine 127-fold — establishes the biochemical basis for post-race ATP depletion as a measurable proxy for in-race aerobic and anaerobic work intensity.

For finishing-position prediction, three implications arise:

1. **Post-race purine markers as exertion intensity proxies.** The magnitude of xanthosine/inosine elevation post-race is proportional to ATP turnover during the race. If post-race blood samples from JRA training centres were accessible, horses with systematically higher purine elevation relative to race pace would indicate lower metabolic efficiency (more ATP spent per speed unit) — a potential "fitness ceiling" signal. Currently, these data are held internally by the Laboratory of Racing Chemistry and JRA and are not in standard public race databases.

2. **Exercise-stable metabolites as baseline health markers.** Of 1,256 KEGG-annotated metabolites, 884 (70%) did not change significantly between resting and post-race states. These metabolites are candidates for stable fitness and health biomarkers, suitable for pre-race screening without exercise-state confounding.

3. **Inter-individual resting variability.** PCA shows that resting-state samples are broadly scattered (high inter-individual variability) while post-race SAD samples are tightly clustered. This confirms that horse-level fixed effects are essential in any model using blood-based features: the same metabolite level means very different things across horses at rest, but post-exercise readings converge to a common response pattern.

Practical limit: all samples in this paper are either resting (at training centre) or post-race (within 1 hour of finish). Pre-race samples — the most useful for prediction — were not collected. This paper motivates but does not enable pre-race biomarker feature engineering without further research.

## Background & objective

Conventional doping detection uses mass spectrometry for known low-molecular-weight compounds (caffeine, furosemide, peptides, heavy metals like cobalt) and PCR for specific transgene sequences. Neither method can detect unknown designer drugs or novel gene/cell therapies. Metabolomics — comprehensive profiling of low-molecular-weight metabolites (<1,000 Da) — could indirectly detect unknown doping by identifying abnormal metabolite patterns. However, exercise itself dramatically alters the plasma metabolome, making it difficult to distinguish doping-induced changes from physiological exercise responses. This study characterises those exercise-induced changes to separate them from stable biomarkers suitable for doping detection.

## Materials & methods

**Ethics:** All plasma samples provided by JRA; JRA approved use for this study.

**Samples:**

- Resting state: n=30 plasma samples from Thoroughbreds at JRA training centres (resting conditions)
- SAD (sample after doping test, i.e., post-race): n=30 samples taken within 1 hour of race finish (30 JRA Thoroughbreds); 1 SAD sample excluded due to poor measurement data → n=29 in final analysis.
- Storage: −30°C.

**Sample preparation:** 100 µl plasma + 300 µl methanol, centrifuged 15,800×g for 15 min; supernatant dried in centrifugal vacuum evaporator for 18 hr; dissolved in 100 µl ultrapure water; recentrifuged; 90 µl transferred to vial. QC sample and PBS prepared as controls.

**LC-HRMS system:**

- HPLC: Thermo Dionex Ultimate 3000
- Column: ACQUITY BEH C18 (100 mm × 2.1 mm, 1.7 µm); 50°C column oven; 0.3 ml/min flow
- Mobile phase: A = 0.1% formic acid in water; B = 0.1% formic acid in methanol
- Gradient: 0% B at 1 min → 100% B at 15 min, held 2 min, back to initial in 2 min, held 2 min; total 22 min; injection volume 5 µl
- MS: Q Exactive HF Quadrupole-Orbitrap (Thermo), polarity switching mode; capillary 250°C, heater 400°C, spray voltage ±3.00 kV; resolution 60,000 FWHM; scan range m/z 100–1,000; AGC target 1×10⁶
- MS/MS: ddMS² and PRM modes; resolution 15,000; NCE 30, 80, or 100; isolation window 2.0 or 1.4

**QC strategy:** QC samples measured after every 5 samples to normalise peak areas and identify stable peaks.

**Data processing:** Compound Discoverer 2.1 (Thermo); alignment model = adaptive curve, max shift 0.2 min, mass tolerance 5 ppm. Peak inclusion criteria: QC sample coverage ≥50%, relative SD of QC peak areas <20%. Peak areas linearly corrected using QC sample areas.

**Statistics:**

- PCA using all 5,010 peaks (normalised areas) in Compound Discoverer 2.1
- Volcano plot: Student's t-test with Benjamini–Hochberg FDR correction; threshold: adjusted P<0.05, fold change >2× for "significantly different" metabolites; adjusted P<1.0×10⁻²⁰ and fold change >4× for "most characteristic" metabolites

**Metabolite identification:**

- KEGG COMPOUND Database match by accurate mass (±5 ppm) using in-house Python software
- MS/MS confirmation: mzCloud database or published spectra
- Conclusively identified: inosine, xanthosine, uric acid, allantoin (MS/MS confirmed)
- Not conclusively identified: hypoxanthine and xanthine (in-source fragments of inosine and xanthosine); deoxyinosine (MS/MS mismatch with mzCloud)

**MSEA and pathway analysis:** MetaboAnalyst (KEGG COMPOUND IDs); KEGG metabolic pathways in Equus caballus.

## Results (detailed — reproduce ALL numbers)

### Peak detection

- Total peaks detected: 5,010
- KEGG-annotated peaks: 1,256 (≈25%)
- Unannotated peaks: ≈3,754 (≈75%; complex structures or outside KEGG database)

### PCA

- PC1 completely separated resting state (n=30) and SAD (n=29) groups
- PC2 explained within-group individual variation; resting state samples were broadly distributed on PC2; SAD samples were tightly clustered

### Volcano plot analysis of 1,256 KEGG-annotated metabolites

- Significantly elevated in SAD: **247 metabolites** (adjusted P<0.05, fold change >2×)
- Significantly decreased in SAD: **125 metabolites** (adjusted P<0.05, fold change >2×)
- Not significantly different: **884 metabolites** (70.6% of annotated metabolites)
- "Most characteristic" SAD metabolites (adjusted P<1.0×10⁻²⁰, fold change >4×): **49 metabolites**; of these, 3 showed a similar increase in resting state; the remaining 46 were SAD-specific

### Purine metabolism pathway (7 of the 49 most-characteristic metabolites)

All mapped to the purine catabolism pathway (ATP → AMP → IMP → inosine → hypoxanthine → xanthine → xanthosine → uric acid → allantoin):

| Metabolite   | Position in pathway | Fold change (SAD/rest) | Conclusive ID           |
| ------------ | ------------------- | ---------------------- | ----------------------- |
| Xanthosine   | Upstream            | **127×**               | Yes (MS/MS)             |
| Inosine      | Upstream            | **35×**                | Yes (MS/MS)             |
| Allantoin    | Downstream          | **7.6×**               | Yes (MS/MS)             |
| Uric acid    | Downstream          | **6.4×**               | Yes (MS/MS)             |
| Hypoxanthine | —                   | Elevated               | No (in-source fragment) |
| Xanthine     | —                   | Elevated               | No (in-source fragment) |
| Deoxyinosine | —                   | Elevated               | No (MS/MS mismatch)     |

- Upstream metabolites (xanthosine, inosine) showed much larger fold changes than downstream metabolites (uric acid, allantoin), interpreted as reflecting multi-stage ATP degradation accumulation.
- The 49 "most characteristic" SAD metabolites with adjusted P<1.0×10⁻²⁰ and fold change >4× represent the most robust exercise biomarkers identified.
- MSEA did not identify any metabolic pathway significantly different at P<0.05 overall (because of multi-KEGG-number annotations for some peaks); the purine pathway finding was identified by KEGG pathway mapping, not MSEA alone.

## Discussion & interpretation

PCA complete separation of resting vs. SAD confirms that the post-race metabolome is radically different from the resting state. The 7-metabolite purine pathway cluster confirms that extensive racing causes massive ATP turnover (consistent with prior studies on purine loss after sprint exercise in humans and lactate/purine in Standardbred horses). Upstream intermediates (inosine, xanthosine) have higher fold changes than downstream products (uric acid, allantoin) because ATP is degraded in stages and the upstream metabolites accumulate transiently at the highest concentrations. The authors interpret this as evidence that many metabolites in the 247-metabolite "elevated in SAD" group are exercise-induced noise that would confound doping marker identification, and the 884 stable metabolites are the candidates for genuine doping biomarkers.

The broad scattering of resting-state samples in PCA (vs. tight clustering of SAD) demonstrates that individual metabolic baselines at rest are highly variable — influenced by feeding time, training schedule, housing, and genetics — while the exercise-induced response is relatively uniform across horses.

## Limitations

- Resting and SAD samples are not paired from the same horses; this is a cross-sectional design, not within-horse tracking. Horse identity, age, sex, and race conditions are not reported.
- Sample collection time post-race is "within 1 hour" — exact time not standardised; some purine metabolites may have already begun clearing by sample collection.
- n=30/29 is small for a metabolomic study; within-individual replicate measurements are absent.
- Not pre-race: the study cannot be used directly as a pre-race predictor without pre-race sampling.
- 75% of detected peaks lack KEGG annotation, limiting pathway interpretation.
- Confirmed identification only for 4 of 7 purine metabolites; the other 3 remain putative.

## Feature-engineering notes for the model

- `plasma_inosine_post_race` — inosine in plasma within 1 hour of race finish; 35× elevation vs. rest is a proxy for in-race ATP turnover intensity. Not directly usable as a pre-race feature but establishes the biochemical foundation for exercise biomarkers. Data: internal JRA/Laboratory of Racing Chemistry only.
- `plasma_xanthosine_post_race` — xanthosine; 127× elevation, the strongest fold change in the study. Same caveats.
- `plasma_uric_acid_post_race` — uric acid; 6.4× elevation; more stable assay target than inosine. Available from standard clinical chemistry panels.
- `plasma_allantoin_post_race` — allantoin; 7.6× elevation. Less commonly measured but can be quantified by standard LC-MS/MS.
- **For training data:** if training-session blood samples are available (not race-day), the 884 exercise-stable metabolites could be used as fitness-state features without exercise-state confounding. The key bottleneck is data access.
- **Horse fixed effects:** the broad resting-state metabolome variability mandates horse-level intercepts in any model that uses blood-panel features. A simple mean-centred feature per horse per 30-day rolling window is recommended.
- **Do NOT use post-race biomarkers as direct features** in a model predicting the current race's outcome — they are outcomes, not predictors, and would introduce data leakage.
- **Potential use case:** if post-race biomarker trajectories over a horse's career are available, a time-series of uric acid elevation could indicate cumulative anaerobic fitness trends (improving vs. declining).

## Key references / follow-up leads

- Joré C et al. 2017. LC-HRMS-based metabolomic approach for the detection of Continuous Erythropoiesis Receptor Activator effects in horse doping control. J. Chromatogr. A 1521: 90–99. — prior horse plasma metabolomics for doping; directly comparable methodology.
- Wang Y et al. 2016. LC-MS-based metabolomics discovers purine endogenous associations with low-dose salbutamol in urine collected for antidoping tests. Anal. Chem. 88: 2243–2249. — human purine metabolomics and doping.
- Stathis CG, Zhao S, Carey MF, Snow RJ. 1999. Purine loss after repeated sprint bouts in humans. J. Appl. Physiol. 87: 2037–2042. — foundational: purine loss in sprint exercise.
- Stefano B, Franco T, Oberosler R. 1999. Plasma lactate and purine derivatives accumulation after exercise of increasing intensity in standardbred horses. J. Equine Vet. Sci. 19: 463–468. — equine purine measurement with exercise intensity.
- Tozaki T et al. 2018. Digital PCR detection of plasmid DNA administered to the skeletal muscle of a microminipig. BMC Res. Notes 11: 708. — gene doping detection by same group.
