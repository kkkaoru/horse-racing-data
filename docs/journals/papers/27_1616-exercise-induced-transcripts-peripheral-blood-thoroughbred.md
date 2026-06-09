# Profiling of exercise-induced transcripts in the peripheral blood cells of Thoroughbred horses

## Metadata

| Field                          | Value                                                                                                                                                                                        |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 27(4): 157–164, 2016                                                                                                                                                          |
| docid                          | `27_1616`                                                                                                                                                                                    |
| Article type                   | Original Article                                                                                                                                                                             |
| Authors                        | Teruaki TOZAKI, Mio KIKUCHI, Hironaga KAKOI, Kei-ichi HIROTA, Kazutaka MUKAI, Hiroko AIDA, Seiji NAKAMURA, Shun-ichi NAGATA                                                                  |
| Affiliations                   | Genetic Analysis Department, Laboratory of Racing Chemistry, Tochigi 320-0851; Equine Research Institute, Japan Racing Association, Tochigi 329-0412; DNA Chip Research Inc., Tokyo 105-0022 |
| Received / Accepted / Released | July 14, 2016 / September 14, 2016 / 2016                                                                                                                                                    |
| Keywords                       | exercise, horses, Thoroughbred, transcriptome                                                                                                                                                |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/27/4/27_1616/_pdf/-char/en                                                                                                                          |

## Abstract (verbatim)

> Transcriptome analyses based on DNA microarray technology have been used to investigate gene expression profiles in horses. In this study, we aimed to identify exercise-induced changes in the expression profiles of genes in the peripheral blood of Thoroughbred horses using DNA microarray technology (15,429 genes on 43,603 probes). Blood samples from the jugular vein were collected from six horses before and 1 min, 4 hr, and 24 hr after all-out running on a treadmill. After the normalization of microarray data, a total of 26,830 probes were clustered into four groups and 11 subgroups showing similar expression changes based on k-mean clustering. The expression level of inflammation-related genes, including interleukin-1 receptor type II (IL-1R2), matrix metallopeptidase 8 (MMP8), protein S100-A8 (S100-A8), and serum amyloid A (SAA), increased at 4 hr after exercise, whereas that of c-Fos (FOS) increased at 1 min after exercise. These results indicated that the inflammatory response increased in the peripheral blood cells after exercise. Our study also revealed the presence of genes that may not be affected by all-out exercise. In conclusion, transcriptome analysis of peripheral blood cells could be used to monitor physiological changes induced by various external stress factors, including exercise, in Thoroughbred racehorses.

## Relevance to finishing-position (着順) prediction

This paper belongs to feature family **C (exercise physiology/fitness)**, with a secondary contribution to **A (injury/soundness)** through the musculoskeletal damage pathway. It provides the first transcriptome characterisation of exercise-induced gene expression changes in peripheral blood of JRA Thoroughbreds — demonstrating that whole-blood "liquid biopsy" is a valid approach for monitoring exercise-induced physiological stress.

The four key findings translate to practical monitoring features:

1. **SAA (serum amyloid A) at 4 hr post-exercise (+5.7-fold):** SAA is already measured in clinical equine practice as an acute-phase protein and is used in JRA veterinary monitoring. Elevated SAA between training sessions (above age-sex reference) may indicate accumulating musculoskeletal microtrauma, as confirmed by the GO enrichment results (collagen catabolic processes). SAA is the most clinically actionable marker from this paper.

2. **S100-A8 (+15.8-fold), IL-1R2 (+14.2-fold), MMP8 (+12.6-fold) at 4 hr post-exercise:** These neutrophil-expressed inflammation markers peak at 4 hr — the window of peak neutrophil activation after high-intensity exercise. Sustained elevation between races (elevated at pre-race measurement) indicates incomplete recovery from prior exercise-induced inflammation.

3. **c-Fos (FOS) at 1 min post-exercise (+3.7-fold):** The immediate-early gene FOS responds within 1 minute to exercise stress. As a gene-expression marker, it cannot currently be used in standard blood panels; however, if targeted real-time PCR becomes routine, FOS expression at 1 min post-gallop could discriminate hard from easy work.

4. **Stable genes (CL4: the majority of 26,830 probes):** The identification that most transcripts are NOT altered by all-out exercise provides background context: blood transcriptomics has sufficient signal-to-noise ratio for exercise monitoring.

For the model, the most practical implication is that **post-exercise SAA** is an existing clinical marker (measured at JRA Miho/Ritto) whose elevation before a race day predicts musculoskeletal injury risk and potentially sub-optimal performance. The broader transcriptomic framework justifies investing in blood-based biomarker features from routine JRA veterinary bloodwork.

## Background & objective

Thoroughbred skeletal muscle mass exceeds 55% of body mass, making exercise-induced muscle damage a primary concern. Previous transcriptome studies used muscle biopsy, which carries tissue damage risk. Liquid biopsy of peripheral blood avoids this risk, and blood transcriptomics had been validated for cancer monitoring and neurological disease tracking in humans. The horse genome is well-sequenced (Broad Institute: 2.7 billion bp, >1.1 million SNPs, 20,322 protein-coding genes by Ensembl pipeline), enabling commercial microarray platforms (Agilent 44K equine microarray). Two prior whole-blood transcriptome studies in horses (Coleman et al. 2010 by RNA-seq; Park et al. 2012) were limited in scope. The objective was to profile blood transcriptome changes across four time points (pre, 1 min, 4 hr, 24 hr post-exercise) after all-out treadmill exercise in 6 Thoroughbreds.

## Materials & methods

**Ethics:** All procedures approved by Animal Care Committee of the Equine Research Institute, JRA.

**Subjects:** n=6 Thoroughbred horses (3 geldings, 3 females; age 5 years; weight 475–524 kg). All clinically healthy and well-trained for treadmill exercise.

**Exercise protocol:** Standardised incremental-step treadmill exercise to exhaustion:

- Walk: 4 m/s
- Trot: 6 m/s
- Canter steps: 8 m/s → 10 m/s → 12 m/s → 13 m/s
- Treadmill incline: 6%
  (Protocol is a standard all-out incremental test to voluntary exhaustion)

**Blood sampling:** Jugular venipuncture at 4 time points:

- T0: before exercise (pre)
- T1: 1 minute after exercise ends
- T4: 4 hours after exercise
- T24: 24 hours after exercise

**WBC counts:** 3.0 ml blood into heparinised tube; measured within 2 hr using K-4500 system (Sysmex, Kobe). Statistics: paired t-test with Bonferroni correction (corrected P-value <0.0166).

**Microarray:** 2.5 ml blood into PAXgene Blood RNA Tubes; kept at 18–25°C for 3 hr; stored at −80°C. RNA extracted with PAXgene Blood RNA Kit (Qiagen). 100 ng RNA labelled with Cy3-CTP (Agilent One-color Microarray-based Gene Expression Analysis, Quick Amp Labeling Kit). Hybridisation on 44K microarray (15,429 genes, 43,603 probes) at 65°C for 17 hr. Scanned with SureScan Microarray Scanner. No haemoglobin mRNA elimination protocol used.

**Data analysis:**

1. Quantile normalisation (GeneSpring GX12, Agilent)
2. Probe-level data normalised against T0 expression intensity
3. Only probes with data available in all 24 microarrays (4 time points × 6 horses) included
4. K-means clustering with Davies-Bouldin Validity Index (cclust package, R v3.1.2)
5. Subgroup clustering within each main group
6. Only probes clustering into common subgroup at each time point in ALL horses retained

**Gene annotation:** BLAST v2.2.22 bidirectional best hit against human genes (27,876) and horse genes (35,847 sequences); e-value threshold 1.0E-4. Gene identified by bidirectional best hit = orthologous between humans and horses.

**GO analysis:** Ensembl 56 (GO term structure); GeneSpring GX12 GO option; corrected P-value <0.1 for significance.

## Results (detailed — reproduce ALL numbers)

### WBC counts (Table 1)

| Time point       | WBC (×10²/µl, mean ± SD) |
| ---------------- | ------------------------ |
| T0 (pre)         | 68.3 ± 12.1              |
| T1 (1 min post)  | 88.0 ± 14.8              |
| T4 (4 hr post)   | 82.3 ± 11.3              |
| T24 (24 hr post) | 60.3 ± 11.1              |

WBC counts higher at T1 and T4 than T0, but differences **not statistically significant** (Bonferroni-corrected). Peak varied: T1 peak in 4/6 horses; T4 peak in 2/6 horses.

### Clustering structure

- Total probes after normalisation: **160,980**
- Probes in final common-subgroup analysis: **26,830** (clustered into 4 groups, 11 subgroups)

| Group | Pattern             | Subgroups  | Probes | Genes |
| ----- | ------------------- | ---------- | ------ | ----- |
| CL1   | Downregulated at T4 | CL1A, CL1B | 12     | 7     |
| CL2   | Upregulated at T4   | CL2A, CL2B | 613    | 226   |
| CL3   | Upregulated at T1   | CL3A, CL3B | 21     | 10    |
| CL4   | No change           | CL4A–CL4E  | 48     | 30    |

Key subgroups:

- CL1A: 1 probe, 1 gene (downregulated at T4)
- CL1B: 11 probes, 6 genes (downregulated at T4)
- **CL2A: 48 probes, 21 genes (strongly upregulated at T4)**
- CL2B: 565 probes, 205 genes (moderately upregulated at T4)
- **CL3A: 1 probe, 1 gene (strongly upregulated at T1)**
- CL3B: 20 probes, 9 genes (moderately upregulated at T1)
- CL4A–E: 48 probes total, 30 genes (no change)

### CL2A genes: strongly upregulated at T4 (4 hr post-exercise) (Table 2)

| Gene                                                       | Fold change (T4/T0) | Function                                                 |
| ---------------------------------------------------------- | ------------------- | -------------------------------------------------------- |
| Resistin-like                                              | **24.1**            | Adipokine/inflammation                                   |
| Protein S100-A8-like                                       | **15.8**            | Neutrophil surface protein; acute inflammatory response  |
| Interleukin-1 receptor type II (IL-1R2)                    | **14.2**            | Decoy receptor; suppresses IL-1 inflammatory signalling  |
| Haptoglobin-like                                           | **12.8**            | Haemoglobin binding; acute-phase protein                 |
| Matrix metallopeptidase 8 / MMP8 (neutrophil collagenase)  | **12.6**            | Collagenase in neutrophils; ECM degradation              |
| TBC1 domain family member 2A-like                          | 8.2                 | GTPase activating protein                                |
| Scrapie-responsive protein 1-like                          | 8.2                 | Stress-response gene                                     |
| Thy-1 cell surface antigen                                 | 8.0                 | T-cell / fibroblast surface marker                       |
| Myc target 1                                               | 7.2                 | Transcription factor target                              |
| Peptidoglycan recognition protein 1                        | 6.8                 | Innate immunity                                          |
| 2-acylglycerol O-acyltransferase 1-like                    | 6.7                 | Lipid metabolism                                         |
| Uridine phosphorylase 1-like                               | 6.2                 | Nucleotide metabolism                                    |
| Protein S100-A9-like                                       | 5.9                 | Neutrophil surface protein (S100-A8/A9 complex)          |
| Serum amyloid A1 (SAA)                                     | **5.7**             | Major acute-phase protein; musculoskeletal injury marker |
| Uncharacterised LOC100066570                               | 5.7                 | Unknown                                                  |
| Matrix metallopeptidase 1 (MMP1, interstitial collagenase) | 5.7                 | Collagen degradation                                     |
| Matrix metallopeptidase 27                                 | 5.3                 | Collagen degradation                                     |
| Maltase-glucoamylase (alpha-glucosidase)                   | 5.2                 | Carbohydrate metabolism                                  |
| Interferon-induced transmembrane protein 1-like            | 5.2                 | Antiviral/inflammation                                   |
| Inositol monophosphatase 2-like                            | 4.8                 | Signal transduction                                      |
| N-formyl peptide receptor 2-like                           | 4.5                 | Chemotaxis/inflammation                                  |

Fold changes calculated as T4/T0 using normalised expression intensity averages across all probe-gene annotations.

### CL3A gene: strongly upregulated at T1 (1 min post-exercise) (Table 2)

| Gene                            | Fold change (T1/T0) | Function                                                  |
| ------------------------------- | ------------------- | --------------------------------------------------------- |
| Proto-oncogene c-Fos-like (FOS) | **3.7**             | Immediate-early transcription factor; acute stress marker |

### Gene ontology enrichment (Table 3): CL2A

| GO term         | Term name                                                | Corrected P-value |
| --------------- | -------------------------------------------------------- | ----------------- |
| GO:0030574 (BP) | Collagen catabolic process                               | **6.66×10⁻⁴**     |
| GO:0032963 (BP) | Collagen metabolic process                               | **6.66×10⁻⁴**     |
| GO:0044243 (BP) | Multicellular organismal catabolic process               | **6.66×10⁻⁴**     |
| GO:0044259 (BP) | Multicellular organismal macromolecule metabolic process | **6.73×10⁻⁴**     |
| GO:0044236 (BP) | Multicellular organismal metabolic process               | 1.02×10⁻³         |
| GO:0004222 (MF) | Metalloendopeptidase activity                            | 2.06×10⁻²         |
| GO:0008237 (MF) | Metallopeptidase activity                                | 8.94×10⁻²         |
| GO:0005053 (MF) | Peroxisome matrix targeting signal-2 binding             | 1.84×10⁻²         |

BP = biological process; MF = molecular function. Top enriched processes: collagen catabolism and ECM remodelling — consistent with neutrophil-mediated tissue remodelling after exercise-induced muscle microtrauma.

## Discussion & interpretation

The authors interpret the findings through the neutrophil activation pathway:

**Mechanism at 4 hr (CL2A):** Exercise-induced muscle damage triggers catecholamine/cortisol secretion → neutrophil mobilisation → neutrophils express IL-1R2 (IL-1 decoy receptor; inhibits inflammatory cascade), MMP8 (collagenase; facilitates neutrophil migration through ECM), S100-A8 and S100-A9 (neutrophil surface proteins that induce neutrophil chemotaxis and adhesion), and SAA (acute-phase protein). The GO enrichment confirms that collagen catabolism and ECM remodelling are the dominant biological processes — consistent with muscular and connective tissue repair after high-intensity exercise.

Comparison with endurance exercise (Cappelli et al. 2009): MMP-1 and IL-8 were upregulated in endurance horses (50–200 km). Both all-out sprint and endurance exercise upregulate collagen-degradation MMPs, suggesting these are universal exercise-stress markers regardless of intensity modality.

**Mechanism at 1 min (CL3A):** c-Fos (FOS) is a proto-oncogene and transcription factor that is one of the fastest-responding immediate-early genes to cellular stress. Upregulation at 1 min post-exercise reflects the acute stress signal before the inflammatory cascade is triggered; FOS acts as a "sentinel" gene whose expression amplitude may reflect exercise intensity.

**WBC non-significance:** Despite gene expression changes suggestive of neutrophil activation, WBC counts were not significantly increased. The authors propose that gene expression changes reflect transcriptional reprogramming of circulating WBCs rather than changes in WBC numbers — supporting liquid biopsy as a more sensitive indicator than cell counts.

The paper also notes that blood transcriptomics could serve as a **doping detection tool**: alien genes introduced via viral vectors would induce immune responses detectable by transcriptome analysis.

## Limitations

- **n=6 horses:** Extremely small sample, limiting statistical power and generalisability. Individual variation is high (WBC peak at T1 in 4 horses but T4 in 2).
- Single exercise bout under treadmill conditions; field exercise (actual race or gallop) may produce different gene expression patterns.
- No haemoglobin mRNA elimination protocol was used; alpha- and beta-globin mRNAs (which dominate red-cell-contaminated RNA) may mask lower-abundance transcripts.
- No direct link to race performance outcome: the study shows exercise induces these genes, but whether inter-individual variation in gene expression predicts finishing position is not tested.
- Time points (1 min, 4 hr, 24 hr) may miss peak expression for some genes; SAA typically peaks at 4–12 hr; the 4 hr time point may underestimate peak fold change.
- Only genes showing **uniform** clustering across all 6 horses were retained; individual-level variation in gene expression was not characterised.

## Feature-engineering notes for the model

- `saa_pre_race_ug_ml` — serum amyloid A concentration before race day — source: JRA Miho/Ritto vet bloodwork (SAA routinely measured as acute-phase protein) — expected effect: elevated SAA (>10 µg/ml; or >age-sex reference) → ongoing inflammation/microtrauma → positive predictor of underperformance or scratch in next race; SAA >100 µg/ml indicates severe injury
- `saa_post_race_fold_change` — ratio of post-race SAA to pre-race SAA — source: paired pre/post race bloodwork — expected effect: larger fold change → greater exercise-induced tissue stress; may predict recovery time needed before next optimal performance
- `ck_post_race_5hr` — CK activity at 5 hr post-race — source: vet bloodwork — expected effect: >500 U/l = significant muscle damage; peak CK at 5 hr is confirmed by 27_1604 muscle enzyme study; interaction with days-to-next-race
- `mmp8_serum` — MMP8 protein level post-exercise — source: specialised lab; not routine in JRA — expected effect: elevated MMP8 = neutrophil collagenase activity = soft-tissue remodelling; more useful as research marker than routine feature
- `s100_a8_fold_change` — S100-A8 in peripheral blood post-exercise — source: research assay — expected effect: +15.8-fold after all-out exercise; day-to-day variation in baseline S100-A8 may reflect chronic inflammation burden
- `fos_expression_t1` — c-Fos mRNA in peripheral blood 1 min post-gallop — source: qPCR/transcriptomics; not routine — expected effect: higher FOS response = higher exercise intensity perceived by the horse; could discriminate hard vs. easy gallops
- `inflammatory_burden_score` — composite derived from SAA + CK + (if available) S100-A8 — expected effect: positive predictor of injury scratch risk in next 1–2 races; negative predictor of optimal performance
- `days_to_next_race` — continuous — source: race records — interaction with SAA/CK: elevated post-race SAA but short days to next race = elevated injury/under-recovery risk
- **Do NOT use** raw fold-change values from microarray without translating to protein/assay units; the fold changes in this paper are relative to individual horses' own pre-exercise baseline, not absolute concentrations

## Key references / follow-up leads

- Cappelli K et al. 2009. Exercise-induced up-regulation of MMP-1 and IL-8 genes in endurance horses. BMC Physiol. 9: 12. (endurance exercise comparison; confirms MMP upregulation across exercise types)
- McGivney BA et al. 2010. Characterization of the equine skeletal muscle transcriptome identifies novel functional responses to exercise training. BMC Genomics 11: 398. (skeletal muscle RNA-seq after exercise training)
- Park KD et al. 2012. Whole transcriptome analyses of six Thoroughbred horses before and after exercise using RNA-Seq. BMC Genomics 13: 473. (RNA-seq comparison in same species)
- Turło A et al. 2015. The effect of different types of musculoskeletal injuries on blood concentration of serum amyloid A in Thoroughbred racehorses. PLoS One 10: e0140673. (SAA in horses with musculoskeletal injury; validates SAA as injury marker)
- Ryckman C et al. 2003. Proinflammatory activities of S100 proteins: S100A8, S100A9 induce neutrophil chemotaxis and adhesion. J. Immunol. 170: 3233–3242. (S100-A8 biology)
- Wade CM et al. 2009. Genome sequence of the domestic horse. Science 326: 865–867. (horse genome reference used for gene annotation)
- Companion genetics paper: 27_1611 (LCORL genotype and body composition; same first author Tozaki T)
