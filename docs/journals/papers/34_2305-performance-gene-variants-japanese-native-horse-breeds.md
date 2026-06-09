# Genetic Characterization of Japanese Native Horse Breeds by Genotyping Variants that are Associated with Phenotypic Traits

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. **34**(4): 115–120, 2023                                                                                                                                                                                                                                                                                                                                                                       |
| docid                          | `34_2305`                                                                                                                                                                                                                                                                                                                                                                                                     |
| Article type                   | Note                                                                                                                                                                                                                                                                                                                                                                                                          |
| Authors                        | Ibuki SAITO, Kotono NAKAMURA, Teruaki TOZAKI, Kazuki HANO, Masaki TAKASU                                                                                                                                                                                                                                                                                                                                      |
| Affiliations                   | 1 Department of Veterinary Medicine, Faculty of Applied Biological Sciences, Gifu University, Gifu 501-1193, Japan; 2 Genetic Analysis Department, Laboratory of Racing Chemistry, Tochigi 320-0851, Japan; 3 Gifu University Institute for Advanced Study, Gifu University, Gifu 501-1193, Japan; 4 Center for One Medicine Innovative Translational Research (COMIT), Gifu University, Gifu 501-1193, Japan |
| Received / Accepted / Released | 2023-03-30 / 2023-08-23 / —                                                                                                                                                                                                                                                                                                                                                                                   |
| Keywords                       | breeding, conservation, insertion and deletion, single-nucleotide variant                                                                                                                                                                                                                                                                                                                                     |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/34/4/34_2305/_pdf/-char/en                                                                                                                                                                                                                                                                                                                                           |

## Abstract (verbatim)

> Concerns have been raised about the loss of genetic diversity in Japanese native horses because of their declining populations. In this study, we investigated the genetic variation of four genes, myostatin (MSTN), ligand-dependent nuclear receptor corepressor like (LCORL), doublesex and mab-3 related transcription factor 3 (DMRT3), and 5-hydroxytryptamine receptor 1A (HTR1A), which are associated with horse phenotypic traits, in six Japanese horse breeds (Hokkaido, Kiso, Noma, Misaki, Tokara, and Yonaguni). MSTN, LCORL, DMRT3, and HTR1A showed polymorphisms in the Kiso; Hokkaido and Noma; Hokkaido; and Kiso, Tokara, and Yonaguni breeds, respectively. The Misaki did not show polymorphisms in any of the genes. This study may serve as a basis for developing future breeding strategies focusing on traits in Japanese native horses.

## Relevance to finishing-position (着順) prediction

**Feature family D (genetics/pedigree).** Although the subjects are non-racing Japanese native breeds (not Thoroughbreds), this paper genotypes four **molecularly validated performance and phenotype loci** that directly apply to finishing-position prediction in Thoroughbreds:

1. **MSTN (myostatin) SINE insertion + g.66608679T>C SNV** — the most validated molecular predictor of racing distance aptitude in Thoroughbreds (Hill et al. 2010, PLoS One; Hill et al. 2010, BMC Genomics). In Thoroughbreds these two variants are in **complete linkage disequilibrium (LD)** as Del-T / In-C haplotypes. In-C/In-C homozygotes: high fast-twitch fibre proportion, heavy body weight, short-distance optimum (5–7 furlongs); Del-T/Del-T: high slow-twitch fibre proportion, lighter body weight, long-distance optimum (1 mile+); heterozygotes intermediate. **If MSTN genotype is available, it is the single strongest molecular predictor of optimal racing distance.**

2. **LCORL g.68603064A>G** — affects expression of LCORL ~60 kb upstream; G-allele associated with greater withers height and body weight in Thoroughbreds (Tozaki et al. 2016, 2017). G/G and G/A horses are taller and heavier than A/A; this is a genetic proxy for the conformation features discussed in 35_JES2311 and is a distance/stamina modifier (bigger horses tend toward distance races).

3. **DMRT3 g.22391254C>A** — nonsense mutation (Ser301STOP) causing alternative gait (ambling/pacing) in horses with A/A genotype. Expected to be near-monomorphic in Thoroughbreds (almost all racing Thoroughbreds are C/C). If a Thoroughbred horse carries the A-allele, it may have altered locomotion and spinal circuit function that affects stride mechanics and performance.

4. **HTR1A g.10175848G>A** — G→A substitution at position 237 of the serotonin receptor 1A protein (Gly237Arg); A-allele in Thoroughbreds is associated with **reduced tractability** (Hori et al. 2016, Anim. Genet.). Horses with low tractability may display pre-race anxiety, poor gate behaviour, or erratic running behaviour — observable features that could correlate with this marker.

This paper's primary value for the pipeline is: (a) providing **TaqMan genotyping assay details** (primer/probe sequences in Table 1) for all five variants, enabling implementation in a wet-lab pipeline; and (b) confirming that the two MSTN variants are NOT in complete LD in non-Thoroughbred populations — meaning MSTN genotyping in Thoroughbreds requires the SINE insertion assay specifically (not just the SNV).

## Background & objective

The eight registered Japanese native horse breeds (including the 6 studied here) have declined to populations of 40–150 individuals each (except Hokkaido: > 1,000). Genetic diversity is threatened. This study aimed to survey allele frequencies of four phenotype-associated loci in all six breeds (all or nearly all living individuals in each breed), providing a baseline for conservation genetics. The secondary purpose — relevant here — is methodological: it establishes the genotyping protocols and allele frequency baselines for the same loci that are functional in Thoroughbreds.

## Materials & methods

**Subjects:** 482 horses across 6 Japanese native breeds:

- Hokkaido: n = 84 (randomly sampled; total population > 1,000)
- Kiso: n = 58 (all living/breeding individuals)
- Noma: n = 48 (all)
- Misaki: n = 72 (all)
- Tokara: n = 123 (all)
- Yonaguni: n = 97 (all)

Ethics: Gifu University Animal Care Committee (2021-132, 2022-068, 17207) for Kiso, Yonaguni, Misaki, Noma. Hokkaido/Tokara: samples from clinical blood draws.

**Genomic DNA extraction:** DNeasy Blood & Tissue Kit (QIAGEN) from whole blood.

**Variants genotyped (5 variants in 4 genes, coordinates in EquCab3.0 / GCA_002863925.1):**

1. MSTN rs397152648 — g.66608679T>C — Chr18:66608679 — intron 1 SNV
2. MSTN g.66619237delinsSINE — Chr18:66610287 — promoter SINE insertion (227 bp)
3. LCORL rs68603064 — g.68603064A>G — Chr3:107374136 — 60 kb upstream of LCORL
4. DMRT3 rs1150690013 — g.22391254C>A — Chr23:22391254 — Ser301STOP coding variant
5. HTR1A rs1148692440 — g.10175848G>A — Chr21:10175848 — Gly237Arg coding variant

**Method:** TaqMan allelic discrimination (QuantStudio 5, Thermo Fisher Scientific; TaqMan GTXpress Master Mix). Primer/probe sequences from Table 1 of paper (detailed below).

**Table 1 — TaqMan assay sequences (verbatim from paper):**

| Gene/Variant              | Reference/Location                                  | Probes and Primers                                                                                                              |
| ------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| MSTN g.66608679T>C        | rs397152648, Chr18:66608679, intron 1 SNV           | VIC probe: AATGCACCAAGTAATTT; FAM probe: ATGCACCAAATAATTT; Fwd: CCAGGACTATTTGATAGCAGAGTCA; Rev: GACACAACAGTTTCAAAATATTGTTCTCCTT |
| MSTN g.66619237delinsSINE | No ref number, Chr18:66610287, promoter SINE 227 bp | VIC probe: ATAAAAAGCCACTTGGAATACAGTA; FAM probe: CCCCGTGGCCGAGT; Fwd: CAATCATAGATCCTGACGACACTTGT; Rev: ACAACTTGCCACACCAGTGAAT   |
| LCORL g.68603064A>G       | rs68603064, Chr3:107374136, 60 kb upstream          | VIC probe: CATTCCAGCTTATTTCTGTA; FAM probe: CATTCCAGTTTATTTCTGTAC; Fwd: CCAAATTTGCCTGGCTAGAGA; Rev: TGTTCCCTGTGATTCTGCCTTT      |
| DMRT3 g.22391254C>A       | rs1150690013, Chr23:22391254, Ser301STOP            | VIC probe: CTGCCGAAGTTCG; FAM probe: CTCTGCCTAAGTTCG; Fwd: CCTCTCCAGCCGCTCCT; Rev: TCAAAGATGTGCCCGTTGGA                         |
| HTR1A g.10175848G>A       | rs1148692440, Chr21:10175848, Gly237Arg             | VIC probe: CCGCTCCCTTCTTC; FAM probe: TCCGCTCTCTTCTTC; Fwd: CCGCAAGACAGTCAAGAAGGT; Rev: CGCCATTTGCGCTCTTCTT                     |

## Results (detailed — reproduce ALL numbers)

**Table 2: Genotype counts and allele frequencies (complete, verbatim from paper):**

**MSTN g.66608679T>C:**

| Breed    | n   | T/T | C/T | C/C | T freq | C freq |
| -------- | --- | --- | --- | --- | ------ | ------ |
| Hokkaido | 84  | 69  | 14  | 1   | 0.90   | 0.10   |
| Kiso     | 58  | 17  | 33  | 8   | 0.58   | 0.42   |
| Noma     | 48  | 48  | 0   | 0   | 1.00   | 0.00   |
| Misaki   | 72  | 72  | 0   | 0   | 1.00   | 0.00   |
| Tokara   | 123 | 117 | 5   | 1   | 0.97   | 0.03   |
| Yonaguni | 97  | 73  | 24  | 0   | 0.88   | 0.12   |

**MSTN g.66619237delinsSINE:**

| Breed    | n   | Del/Del | Del/In | In/In | Del freq | In freq |
| -------- | --- | ------- | ------ | ----- | -------- | ------- |
| Hokkaido | 84  | 84      | 0      | 0     | 1.00     | 0.00    |
| Kiso     | 58  | 24      | 32     | 2     | 0.69     | 0.31    |
| Noma     | 48  | 48      | 0      | 0     | 1.00     | 0.00    |
| Misaki   | 72  | 72      | 0      | 0     | 1.00     | 0.00    |
| Tokara   | 123 | 123     | 0      | 0     | 1.00     | 0.00    |
| Yonaguni | 97  | 97      | 0      | 0     | 1.00     | 0.00    |

Note: MSTN SINE and SNV are in complete LD in Thoroughbreds (Del-T / In-C haplotype), but NOT in native horses: Hokkaido shows C-allele (SNV) without In-allele (SINE); Kiso shows both polymorphisms but imperfect LD.

**LCORL g.68603064A>G:**

| Breed    | n   | A/A | A/G | G/G | A freq | G freq |
| -------- | --- | --- | --- | --- | ------ | ------ |
| Hokkaido | 84  | 81  | 3   | 0   | 0.98   | 0.02   |
| Kiso     | 58  | 58  | 0   | 0   | 1.00   | 0.00   |
| Noma     | 48  | 43  | 5   | 0   | 0.95   | 0.05   |
| Misaki   | 72  | 72  | 0   | 0   | 1.00   | 0.00   |
| Tokara   | 123 | 123 | 0   | 0   | 1.00   | 0.00   |
| Yonaguni | 97  | 97  | 0   | 0   | 1.00   | 0.00   |

**DMRT3 g.22391254C>A:**

| Breed    | n   | C/C | C/A | A/A | C freq | A freq |
| -------- | --- | --- | --- | --- | ------ | ------ |
| Hokkaido | 84  | 7   | 24  | 53  | 0.23   | 0.77   |
| Kiso     | 58  | 58  | 0   | 0   | 1.00   | 0.00   |
| Noma     | 48  | 48  | 0   | 0   | 1.00   | 0.00   |
| Misaki   | 72  | 72  | 0   | 0   | 1.00   | 0.00   |
| Tokara   | 123 | 123 | 0   | 0   | 1.00   | 0.00   |
| Yonaguni | 97  | 97  | 0   | 0   | 1.00   | 0.00   |

Note: Yonaguni historically had A-allele (Chandra Paul et al. 2020 study using decades-old samples) but current population is monomorphic C/C — A-allele was lost to drift after bottleneck.

**HTR1A g.10175848G>A:**

| Breed    | n   | G/G | G/A | A/A | G freq | A freq |
| -------- | --- | --- | --- | --- | ------ | ------ |
| Hokkaido | 84  | 84  | 0   | 0   | 1.00   | 0.00   |
| Kiso     | 58  | 57  | 1   | 0   | 0.99   | 0.01   |
| Noma     | 48  | 48  | 0   | 0   | 1.00   | 0.00   |
| Misaki   | 72  | 72  | 0   | 0   | 1.00   | 0.00   |
| Tokara   | 123 | 10  | 47  | 66  | 0.27   | 0.73   |
| Yonaguni | 97  | 88  | 9   | 0   | 0.95   | 0.05   |

Note: High A-allele frequency in Tokara (0.73) attributed to population bottleneck (only ~10 individuals when rediscovered); not related to selection for low tractability.

## Discussion & interpretation

**MSTN — key mechanistic insight:** The paper explicitly states that in Thoroughbreds, g.66619237delinsSINE and g.66608679T>C are in **complete LD** as Del-T / In-C haplotypes (citing Rooney et al. 2018, PLoS One 13:e0205664). The SINE insertion upstream of MSTN creates a CpG island that reduces MSTN expression, leading to higher proportions of fast-twitch (Type IIb) muscle fibres, heavier body weight, and sprint aptitude. Del-T (MSTN expressed normally) → more slow-twitch fibres, lighter build, stamina aptitude. In native horses, the two variants are NOT in complete LD (Kiso has both C-allele and In-allele separately), which the authors attribute to historical crossbreeding of Kiso with European horses.

**LCORL:** The G-allele (associated with taller horses) occurs at only 2–5% in native breeds, consistent with their smaller body sizes (Noma withers height ~110 cm vs. Hokkaido ~130 cm). LCORL affects body size via altered transcription factor binding ~60 kb upstream of the gene.

**DMRT3:** The A/A genotype (nonsense mutation, protein truncated by 174 amino acids) disrupts spinal locomotor circuit interneurons controlling limb coordination; horses with A/A can display pace (both legs on same side synchronised), tölt, or other non-standard gaits. Hokkaido horses are known amblers (A-allele freq 0.77). In Thoroughbreds, near-monomorphic C/C expected; any A-allele carrier would have unusual locomotion.

**HTR1A:** G→A Gly237Arg substitution in the serotonin 1A receptor. In Thoroughbreds, A-allele associates with reduced tractability — horses are harder to handle, show more anxiety. Tokara's high A-allele frequency (0.73) is attributed to founder effect from a severely bottlenecked population. Tractability affects gate behaviour, race-day stress responses, and training compliance.

## Limitations

- Subjects are non-racing native horses; allele effects in these breeds are inferred from Thoroughbred literature, not measured here
- No phenotypic measurements taken for individual horses in this study; allele-phenotype associations are cited from prior Thoroughbred research (Hill et al. 2010; Tozaki et al. 2016, 2017; Hori et al. 2016), not measured in this cohort
- Population sizes for some breeds are very small (Noma: 48, Tokara: 123 representing entire population); genetic drift, not selection, explains most allele frequency variation
- LD between MSTN variants in native breeds does not directly inform Thoroughbred genotyping, but the finding that LD breaks down in non-Thoroughbred populations confirms that both assays should be run independently in any new population survey

## Feature-engineering notes for the model

- `mstn_haplotype` — MSTN SINE/SNV haplotype: In-C/In-C (sprint), In-C/Del-T (intermediate), Del-T/Del-T (stamina) — from Laboratory of Racing Chemistry genotyping database (Tozaki et al.) — **strongest molecular predictor of optimal racing distance**; In-C/In-C horses perform best at ≤ 1,400 m; Del-T/Del-T at ≥ 1,800 m; interaction with `distance_m` is the primary model feature
- `mstn_sine_genotype` — MSTN promoter SINE insertion specifically: Del/Del, Del/In, In/In — same source — In-allele carriers have elevated fast-twitch fibre proportion; SINE assay required separately from SNV in Thoroughbreds (though they are in complete LD, so one suffices if LD confirmed)
- `lcorl_genotype` — LCORL g.68603064A>G: A/A, A/G, G/G — JRA/JBBA genotype data — G-allele carriers are taller/heavier; interaction with `distance_m` (heavier horses favour longer distances) and `surface` (heavier horses may prefer turf)
- `dmrt3_genotype` — DMRT3 g.22391254C>A — Laboratory of Racing Chemistry — expected near-monomorphic C/C in Thoroughbreds; if A-allele present, flag for possible gait anomaly; interaction with `trainer_comment` or gate draw observations
- `htr1a_genotype` — HTR1A g.10175848G>A — Laboratory of Racing Chemistry — A-allele carriers in Thoroughbreds show reduced tractability; potential interaction with `gate_number` (outside gates may amplify anxiety in A-allele carriers) and `race_type` (maiden races vs. stakes; inexperienced horses with A-allele may be more erratic)
- **Data availability:** The Laboratory of Racing Chemistry (Tochigi) performs MSTN and LCORL genotyping for Japanese Thoroughbreds (Tozaki is affiliated there); the JRA may have MSTN data for racehorses, though access requires institutional collaboration
- **Do NOT apply** allele effects observed in native horses (e.g., Tokara HTR1A A-allele frequency) to Thoroughbred performance predictions — different selection pressures, population histories, and LD structures

## Key references / follow-up leads

- **Hill E.W. et al. 2010** — PLoS One 5:e8645 — "A sequence polymorphism in MSTN predicts sprinting ability and racing stamina in thoroughbred horses" — foundational MSTN-performance study; key numbers: In-C/In-C optimum 5–7 furlongs, Del-T/Del-T optimum 1 mile+
- **Hill E.W. et al. 2010** — BMC Genomics 11:552 — genome-wide SNP study confirming MSTN SNV as strongest predictor of optimum racing distance in Thoroughbreds
- **Rooney M.F. et al. 2018** — PLoS One 13:e0205664 — "The 'speed gene' effect of myostatin arises in Thoroughbred horses due to a promoter proximal SINE insertion" — mechanistic explanation of SINE→MSTN downregulation→fast-twitch fibre enrichment
- **Tozaki T. et al. 2016** — J. Equine Sci. 27:107–114 — LCORL variants and body composition in Thoroughbreds under training; most directly applicable LCORL performance study
- **Tozaki T. et al. 2017** — J. Equine Sci. 28:127–134 — GWAS for body weight in Japanese Thoroughbreds; chromosomes 3, 9, 15, 18
- **Hori Y. et al. 2016** — Anim. Genet. 47:62–67 — "Evidence for the effect of serotonin receptor 1A gene (HTR1A) polymorphism on tractability in Thoroughbred horses" — HTR1A A-allele reduces tractability
- **Andersson L.S. et al. 2012** — Nature 488:642–646 — DMRT3 mutations affect locomotion in horses and spinal circuit function in mice; foundational gait genetics
- **Hill E.W. et al. 2012** — J. Appl. Physiol. 112:86–90 — MSTN genotype (g.66493737C/T) association with speed indices in Thoroughbred racehorses (note: different coordinate system from EquCab1 vs. EquCab3)
