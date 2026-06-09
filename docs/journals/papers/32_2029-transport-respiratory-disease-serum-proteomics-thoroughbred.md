# Comparison of the Proteomes in Sera between Healthy Thoroughbreds and Thoroughbreds with Respiratory Disease Associated with Transport Using Mass Spectrometry-Based Proteomics

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                   |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. **32**(1): 11–15, 2021                                                                                                                                                                                                                                   |
| docid                          | `32_2029`                                                                                                                                                                                                                                                               |
| Article type                   | Note                                                                                                                                                                                                                                                                    |
| Authors                        | Yohei MINAMIJIMA, Hidekazu NIWA, Eri UCHIDA, Kazuo YAMAMOTO                                                                                                                                                                                                             |
| Affiliations                   | 1 Laboratory of Racing Chemistry, Tochigi 320-0851, Japan; 2 Equine Research Institute, Japan Racing Association, Tochigi 329-0412, Japan; 3 Department of Integrated Biosciences, Graduate School of Frontier Sciences, The University of Tokyo, Chiba 277-8562, Japan |
| Received / Accepted / Released | 2020-10-13 / 2020-10-21 / —                                                                                                                                                                                                                                             |
| Keywords                       | liquid chromatography-tandem mass spectrometry, proteome, respiratory disease associated with transport, Thoroughbred                                                                                                                                                   |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/32/1/32_2029/_pdf/-char/en                                                                                                                                                                                                     |

## Abstract (verbatim)

> In the past decade, mass spectrometry has become an important technology for protein identification. Recent developments in mass spectrometry allow a large number of identifications in samples; therefore, mass-spectrometry-based techniques have been applied to the discovery of biomarkers. Here, we conducted a proteomic study to compare the proteomes in sera between healthy Thoroughbreds and Thoroughbreds with respiratory disease associated with transport (RDT). We found that four proteins, apolipoprotein F, lipopolysaccharide binding protein, lysozyme and protein S100-A8, were upregulated, while keratin 1 was downregulated in the RDT group. It is assumed that inflammation and immune response are involved in the changes of these proteins. The findings suggested that these proteins are potentially useful for elucidating the mechanism of development of RDT.

## Relevance to finishing-position (着順) prediction

**Feature family B (respiratory) and C (exercise-physiology/fitness).** Respiratory disease associated with transport (RDT, "shipping fever") is a direct mechanism linking transport history to DNS (Did Not Start) and degraded race-day performance. This study identifies five serum biomarkers distinguishing healthy JRA Thoroughbreds from RDT cases, with four upregulated (apolipoprotein F, LBP, lysozyme, S100-A8) and one downregulated (keratin 1) in the disease group. Specific relevance:

1. **LBP (lipopolysaccharide binding protein) is the strongest differentiator** (log₂ fold-change +3.29, P = 0.001), representing a 10× protein-level increase in RDT horses. LBP binds lipopolysaccharide (Gram-negative bacterial outer membrane glycolipid) and triggers immune responses. Its magnitude of change strongly implicates Gram-negative bacterial lower respiratory tract infection as a component of RDT pathogenesis (consistent with _Streptococcus zooepidemicus_, _Mannheimia_, and Gram-negative pathogens reported in RDT cases).

2. **SAA (serum amyloid A) showed ~250-fold elevation** in RDT but could not be formally identified by unique peptides (missed by the bioinformatics pipeline). The authors note this is likely the most responsive single biomarker. SAA is already measured by immunoassay (ELISA) in equine clinical practice; if SAA measurements are available in JRA veterinary records, they are a high-value health flag.

3. **S100-A8 (calprotectin subunit)** — damage-associated molecular pattern (DAMP); released from stressed or damaged cells; upregulated +1.23 log₂ (P = 0.013). Previously shown to be upregulated in exercise-induced transcriptomics in equine blood cells (Tozaki et al. 2016).

4. **Lysozyme** (+1.90 log₂, P < 0.0001) — antimicrobial enzyme secreted by macrophages/neutrophils in response to Gram-positive bacterial infection; consistent with bacterial lower respiratory tract infection.

5. **Transport duration ~18 hr average** for RDT cases in this study. The key trigger is long-distance transport (Maeda & Oikawa 2019: shipping fever incidence and rectal temperature elevation increase with transport time and distance). Transport duration is a computable feature from stable-venue geography.

Practical implication: if routine blood biochemistry (SAA, LBP, haptoglobin, fibrinogen) is collected for JRA horses post-transport, these values are directly usable as health-status features in the prediction pipeline. Transport distance/duration as a proxy for RDT risk is the most actionable indirect feature.

## Background & objective

RDT ("shipping fever") is typically caused by stress + microaspiration of upper-respiratory tract bacteria during head-elevated transport, leading to pneumonia or pleuritis. It disrupts JRA training and racing schedules, and in severe cases causes fatal pneumonia or pleuritis. Standard inflammatory markers (haptoglobin, SAA) are known to rise post-transport, but comprehensive proteomic characterisation of the serum response to RDT had not been performed. Authors aimed to: (a) compare serum proteomes of healthy vs. RDT Thoroughbreds using untargeted LC-MS/MS, (b) identify candidate biomarkers beyond haptoglobin/SAA, and (c) infer pathomechanisms from protein functions.

## Materials & methods

**Subjects:**

- Healthy group: 36 JRA Thoroughbreds (16 males, 18 females, 2 geldings; age 4–10 years)
- RDT group: 12 JRA Thoroughbreds (6 males, 5 females, 1 gelding; age 2–5 years). RDT defined as: fever with recent long-distance transport history, diagnosed by JRA veterinarians as requiring treatment. Average transport duration ~18 hr. Blood collected within 24 hr after transport.

Ethics: JRA Animal Care and Use Committee approval 20180425 (approved 25 April 2018).

**Sample processing:**

1. Blood collected into standard serum separation tube; centrifugation 1,940 × g × 10 min; serum stored −80°C
2. 5 µL serum + 10 µL 10 mM dithiothreitol (DTT) + 5 µL 5% sodium deoxycholate → 80°C × 10 min
3. Reduction: 5 µL 50 mM DTT → 60°C × 20 min
4. Alkylation: 5 µL 100 mM iodoacetamide → room temperature × 30 min (dark)
5. Trypsin digestion: 5 µL of 100 µg/mL trypsin in 100 mM ammonium bicarbonate → 37°C overnight
6. Quench: 5 µL 10% trifluoroacetic acid (TFA)
7. Phase separation: 500 µL ethyl acetate, shake, centrifuge 14,000 × g × 5 min (organic phase removed)
8. Dry under vacuum; reconstitute in 200 µL 4% acetonitrile / 0.1% TFA
9. SPE C-tip purification (Nikkyo Technos)
10. Elute with 5 µL 60% acetonitrile / 0.1% TFA; dilute with 35 µL 4% acetonitrile / 0.1% TFA
11. Inject 5 µL into LC-MS/MS

**LC-MS/MS system:**

- EASY-nLC 1200 (Thermo Fisher Scientific)
- Q Exactive HF Orbitrap mass spectrometer (Thermo Fisher Scientific)
- Columns: Acclaim PepMap 100 C18 (75 µm × 20 mm, 3 µm particle, 100 Å pore; trap) + Acclaim PepMap RSLC C18 (75 µm × 500 mm, 2 µm, 100 Å; analytical)
- Gradient: 1.5%–40% acetonitrile over 120 min; flow rate 300 nL/min
- Data acquisition: data-dependent analysis (DDA); full MS scan 400–2,000 m/z; 27 V collision energy; top 20 most intense ions → MS/MS scan 200–2,000 m/z

**Database search:** Proteome Discoverer v2.2 (Thermo Fisher Scientific); UniProt Equus caballus (49,800 sequences, downloaded 27 December 2019); peptide FDR ≤ 0.05.

**Quantification:** Precursor ion intensity sum for identified proteins.

**Statistical analysis:** PCA (on all identified proteins). Mann-Whitney U-test for each protein (RDT vs. healthy), with multiple testing correction. Significant if: adjusted P < 0.05 AND |log₂ fold-change| ≥ 1 (i.e., ≥ 2-fold change in either direction).

## Results (detailed — reproduce ALL numbers)

**Total proteins identified and quantified: 239**

**PCA results:**

- PC1: 16.7% of variance — cleanly separated RDT from healthy group
- PC2: 4.5% — one healthy horse was a potential outlier on PC2 but included in analysis (PC1 separation sufficient)

**Significant differentially expressed proteins (Table 1 — all 5, verbatim):**

| UniProt Accession | Protein                                  | Group comparison (Healthy vs. RDT) log₂ FC | P-value |
| ----------------- | ---------------------------------------- | ------------------------------------------ | ------- |
| F7B504            | Apolipoprotein F                         | +1.262 (upregulated in RDT)                | 0.00580 |
| F7C0D2            | Lipopolysaccharide binding protein (LBP) | +3.293 (upregulated in RDT)                | 0.00120 |
| F7CU94            | Lysozyme                                 | +1.898 (upregulated in RDT)                | 0.00002 |
| F6SN37            | Protein S100-A8                          | +1.225 (upregulated in RDT)                | 0.01280 |
| F7B504            | Keratin 1                                | −2.669 (downregulated in RDT)              | 0.00020 |

Note: The original paper has "F7B504" listed for both Apolipoprotein F and Keratin 1 — this appears to be a typographical error in the published table (keratin 1 likely has a different accession number). The five protein identities and their direction/magnitude are unambiguous from the text.

**SAA (serum amyloid A) — additional finding (from discussion):**

- ~250-fold elevation in RDT group (highest elevation of all detected proteins)
- Did NOT meet formal identification criteria due to lack of unique peptides in the bioinformatic pipeline
- Authors flag SAA as "likely the most responsive biomarker" based on this fold-change estimate
- Previous immunoassay studies (Casella et al. 2012; Leadon et al. 2008) had already documented SAA elevation post-transport

**Haptoglobin — notable non-finding:**

- Did NOT show significant change between healthy and RDT groups
- Authors explain: sampling timing (blood collected within 24 hr post-transport), variable transport duration, and horse condition prior to transport may have masked the expected haptoglobin elevation
- Haptoglobin is a known acute-phase protein that rises 2–3 days post-infection; early sampling within 24 hr may miss its peak

## Discussion & interpretation

**Apolipoprotein F** (log₂ FC +1.26): Functions as a lipid transfer inhibitory protein (LTIP). Elevation in RDT may indicate disorders of liver lipid metabolism caused by RDT — analogous to its upregulation in human non-alcoholic fatty liver disease/liver fibrosis (Kumar et al. 2017). This is a novel finding without clear direct mechanistic link to respiratory infection, possibly reflecting systemic metabolic stress.

**LBP** (log₂ FC +3.29, i.e., ~10× elevation): Well-established innate immune mediator. Binds gram-negative bacterial LPS and presents it to CD14/TLR4 complex, triggering NFκB-mediated inflammatory response. Its dramatic elevation strongly supports bacterial lower respiratory tract infection as the proximate cause of RDT. Prior equine study (Pihl et al. 2016): horses with colic showed large LBP increases, confirming its responsiveness to bacterial challenges.

**Lysozyme** (log₂ FC +1.90): Cleaves bacterial cell wall peptidoglycan; produced by neutrophils and macrophages in response to infection. Upregulation consistent with active neutrophil/macrophage response to pulmonary bacterial invasion. Pellegrini et al. (1991) characterised equine neutrophil lysozyme activity against gram-positive and gram-negative bacteria.

**S100-A8** (log₂ FC +1.23): Part of the calprotectin heterodimer (S100-A8/A9); acts as a DAMP released from damaged or activated immune cells. Foell et al. (2007): S100 proteins are damage-associated molecular patterns released by phagocytes. Tozaki et al. (2016): S100-A8 was upregulated at the transcriptomic level in equine peripheral blood cells after exercise — the protein elevation in RDT may reflect both exercise/stress and bacterial DAMP signalling combined.

**Keratin 1** (log₂ FC −2.67): Cytoskeletal protein; in humans participates in inflammatory networks and keratin 1 knockout leads to S100-A8 upregulation (Roth et al. 2012). The paper acknowledges that keratin is a common laboratory contaminant (from skin, hair, dust), so this result should be interpreted cautiously — contamination during sample preparation is a plausible alternative explanation.

**SAA non-detection:** SAA lacks unique tryptic peptides distinguishable by mass spectrometry (highly conserved protein; many isoforms with similar sequences), so LC-MS/MS cannot formally identify it despite the ~250-fold elevation observed. Immunoassay (ELISA) is required for quantitative SAA measurement.

## Limitations

- Very small RDT group (n = 12); limited statistical power; multiple testing correction may over-correct with this sample size
- Age difference between groups (healthy: 4–10 y; RDT: 2–5 y) could confound protein expression differences — younger horses may have different baseline inflammatory responses
- All samples collected within 24 hr post-transport; this is early for some acute-phase proteins (haptoglobin, fibrinogen typically peak at 48–72 hr) but appropriate for SAA (peaks at 12–24 hr)
- Transport duration varied (average ~18 hr); no systematic breakdown by duration given
- RDT confirmed by fever + transport history + JRA veterinarian diagnosis, not by bacterial culture — diagnosis may include non-bacterial causes
- Keratin 1 downregulation is potentially a laboratory contamination artifact; authors acknowledge this caveat
- No validation cohort; candidate biomarkers require independent validation before clinical or predictive use
- Only serum proteomics; does not cover cellular or transcriptomic responses in airways (the primary site of RDT)

## Feature-engineering notes for the model

- `transport_duration_hr` — hours of van transport before race day — from stable location + race venue geography + JRA travel logistics — proxy for RDT risk; ~18 hr threshold appears to be clinically significant for immune suppression; expected negative effect on race performance — availability: computable from known stable-venue pairs
- `transport_distance_km` — straight-line or road distance from training stable to race venue — derivable from venue-stable pairs — correlated with transport duration; simpler to compute
- `serum_saa_post_transport` — serum amyloid A concentration measured post-transport — JRA veterinary health records — **highest priority health biomarker** if available; ~250-fold elevation in RDT; even moderate elevation (10–50×) likely indicates compromised respiratory health — data availability: JRA routinely performs blood work on racehorses; unclear if SAA is part of routine panel
- `serum_lbp_post_transport` — LBP serum concentration — veterinary records — 10× elevation in confirmed RDT; gram-negative bacterial infection marker — lower practical availability than SAA (immunoassay required)
- `serum_lysozyme_post_transport` — lysozyme concentration — ~4× elevation in RDT; antimicrobial enzyme active during bacterial infection — lower priority than LBP/SAA
- `rdt_history_flag` — binary: has horse been diagnosed with RDT (shipping fever, transport-associated pneumonia/pleuritis) in veterinary records? — from JRA medical records — expected strong negative effect on subsequent race performance; horses that develop RDT often miss 2–6 weeks of training and racing
- `days_since_transport` — days elapsed since last long-distance (> 4 hr) transport before race — from travel records — horses within 3 days of long-distance transport are in the high-RDT-risk window
- **Composite feature:** `transport_duration_hr × days_since_transport` — joint indicator of transport stress exposure and recovery time
- **Do NOT use** haptoglobin measured within 24 hr as a transport-stress indicator — this study and Casella et al. (2012) both show haptoglobin may not be elevated at that timepoint; SAA is more reliable in early sampling

## Key references / follow-up leads

- **Maeda Y. & Oikawa M.A. 2019** — Front. Vet. Sci. 6:27 — "Patterns of Rectal Temperature and Shipping Fever Incidence in Horses Transported Over Long-Distances" — **highest priority follow-up**: directly quantifies shipping fever incidence as a function of transport time and distance; key feature engineering reference
- **Tozaki T. et al. 2016** — J. Equine Sci. 27:157–164 — "Profiling of exercise-induced transcripts in the peripheral blood cells of Thoroughbred horses" — transcriptomic context; S100-A8 upregulated post-exercise
- **Bannai H. et al. 2021** — J. Equine Vet. Sci. 103:103665 — decreased EHV-1 neutralizing antibodies in nasal secretions after 12-hr transport; immune suppression mechanism and timeframe
- **Casella S. et al. 2012** — Res. Vet. Sci. 93:914–917 — serum acute-phase proteins (SAA, haptoglobin, fibrinogen) post-transport; established SAA as responsive transport stress marker via immunoassay
- **Pihl T.H. et al. 2016** — J. Vet. Emerg. Crit. Care 26:664–674 — LBP as acute-phase biomarker in equine colic; validates LBP responsiveness to bacterial challenges
- **Endo Y. et al. 2017** — J. Vet. Med. Sci. 79:464–466 — pre-shipping enrofloxacin reduces fever and blood changes post-transport in Thoroughbreds — pharmacological intervention reference
- **Ohmura H. et al. 2022** — J. Equine Sci. 33:13–17 — same cohort type (JRA Thoroughbreds), physiological transport stress (HR/HRV) — companion paper in this collection (33_2202)
