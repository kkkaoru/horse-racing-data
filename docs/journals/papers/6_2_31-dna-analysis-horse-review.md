# The Recent Studies on DNA Analysis in the Horse

## Metadata

| Field                          | Value                                                                                            |
| ------------------------------ | ------------------------------------------------------------------------------------------------ |
| Journal                        | J. Equine Sci. 6(2): 31–53, 1995                                                                 |
| docid                          | `6_2_31`                                                                                         |
| Article type                   | Review                                                                                           |
| Authors                        | Yoshizane Maeda, Tsutomu Hashiguchi                                                              |
| Affiliations                   | Department of Animal Science, Faculty of Agriculture, Kagoshima University, Kagoshima 890, Japan |
| Received / Accepted / Released | Not stated in paper                                                                              |
| Keywords                       | DNA analysis, horse                                                                              |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/6/2/6_2_31/_pdf/-char/en                                |

## Extraction note

PDF is image-scanned; full text obtained via OCR (tesseract 5.5.2). 23 pages, 176 references. Text quality is good for the body sections; some figure captions have OCR artefacts.

## Abstract (verbatim)

No separate abstract is printed. The paper opens directly with a contents table and Introduction. The following paragraph from the Introduction serves as the scope statement:

> The present paper reviewed research reports up to 1994 on DNA polymorphism, sequencing of structural genes and gene mapping in the horse.

The paper's Summary states:

> Recent studies on horse DNA analysis were reviewed in this paper. Research reports on DNA analysis in the horse are few compared to those on the pig, cattle and chicken. The first paper on horse fingerprinting by minisatellite probes and oligonucleotide probes was published in 1988. Polymorphism of microsatellite DNA in the horse by PCR appeared in 1992, and 29 microsatellite loci were detected by 1994. DNA fingerprint and microsatellite DNA may be available for many applications including individual identification for registration, paternity testing, linkage analysis and research on population genetics in the horse. For the DNA fingerprint and microsatellite DNA, formulas for mean band sharing probability (x), mean population frequency of the resolvable allele (q), average heterozygosity (H), proportion of polymorphic loci (Ppoly), probability of identical DNA fingerprints, relative magnitude of gene differentiation among population and genetic distance (D_r) were described. On the structural gene, studies of gonadotropin, ELA, mtDNA, skeletal muscle sodium channel α-subunit gene, CD44, interleukin-2 and oncogene were referred to. In these studies, the elucidation of gene structure, gene mapping, molecular diagnosis and molecular evolution were illustrated.

## Relevance to finishing-position (着順) prediction

Feature family **D (genetics/pedigree)**. This review is a methodological survey of DNA analysis techniques available for horses as of 1994. Its primary subject is parentage testing, individual identification, and population genetics — not racing performance prediction. The key relevance to the pipeline is indirect but worth noting:

1. **Microsatellite parentage verification**: The review establishes that by 1994, 29 microsatellite loci were characterised in the horse, enabling high-accuracy parentage tests. Correct parentage is a prerequisite for pedigree-based features (`sire_id`, `dam_id`, `broodmare_sire_id`). Any pedigree feature in the finishing-position model depends on registrations being accurate, and this paper documents the molecular basis for that accuracy.

2. **Heritability of racing performance**: The Introduction explicitly states that "genetic parameters were estimated for racing performance [refs 111, 160] and pulling ability [73]." Reference 111 is Hintz R.L. 1980 (_J. Anim. Sci._ 51: 582–594, "Genetics of performance in the horse") and reference 125 is Moritsu Y., Funakoshi H., and Ichikawa S. 1994 (_J. Equine Sci._ 5: 53–58, "Genetic evaluation of sires and environmental factors influencing best racing times of thoroughbred horses in Japan") — the latter being a direct JRA racing time heritability study worth fetching.

3. **ELA (Equine Leukocyte Antigen) / MHC mapping**: The review covers ELA-MHC chromosomal localisation (chromosome 20q14-q22). MHC variation affects immune function and disease susceptibility, which connects to transport-induced respiratory disease risk (paper 6_1_25). No direct performance association is claimed in this review.

4. **Free amino acid types**: Referenced in the Introduction as a known physiological and biochemical variation ([67, 68] = Hanzawa 1995). Free amino acid type of erythrocytes (BA/GSH system) is the subject of companion paper 6_2_61, which directly addresses RBC physiology.

The review does **not** describe quantitative trait loci (QTL) for speed or stamina — this literature did not yet exist at the time of writing (pre-1994). Its relevance to the finishing-position model is therefore primarily as a methodological foundation for pedigree data quality, and as a pointer to the JRA performance genetics literature (Moritsu et al. 1994).

## Background & objective

Horse DNA analysis lagged behind cattle, pigs, and chickens in 1995. Blood typing and protein electrophoresis were the standard tools for parentage and population studies. The paper provides the first comprehensive Japanese-language-accessible review of emerging DNA fingerprint and microsatellite tools. The objective is to synthesise: (I) DNA polymorphism detection methods, (II) population analyses using DNA markers, (III) structural gene sequencing (gonadotropin, MHC, mtDNA, sodium channel, CD44, IL-2, oncogenes), and (IV) gene mapping.

## Materials & methods

Review methodology. Literature surveyed: reports published up to 1994 on horse DNA analysis. 176 references cited.

**Contents structure:**

- I. Introduction
- II. DNA Polymorphism
  - 1. Detection (a. Minisatellite DNA; b. Microsatellite PCR)
  - 2. Population Analysis (band sharing, heterozygosity, genetic distance formulas)
- III. Analysis of Structural Gene
  - 1. Gonadotropin (LH α/β subunit sequences)
  - 2. MHC/ELA (chromosomal localisation, DNA analysis)
  - 3. Adult Skeletal Muscle Sodium Channel α-Subunit Gene
  - 4. CD44 Protein
  - 5. Interleukin-2
  - 6. Oncogenes
  - 7. Mitochondrial DNA
- IV. Gene Mapping (linkage groups, in situ hybridisation)
- V. Summary
- VI. References

## Results (detailed — reproduce ALL numbers)

### Minisatellite DNA fingerprinting

DNA fingerprint probes characterised in horses by 1994 (Table 1 of paper): 11 probe/enzyme combinations identified, producing 6–49 resolved fragments per combination. Probability of identical fingerprints estimated at 3.2×10⁻¹⁴ (Georges et al. 1988 Belgian halfbred family with 4 minisatellite probes).

**Table 1 data (key entries):**
| Probe | Restriction enzyme | No. of fragments |
|-------|-------------------|-----------------|
| M13 bacteriophage | HaeIII | — |
| uPCJ | HaeIII | 21 |
| Jeffreys 33.6 | HaeIII | 10 |
| (TGC)₁₀ | HinfI | 13 |
| (TG)₁₀ | HinfI | 7 |
| (TG)n | HinfI | 25–32 |
| (GT)n | HinfI | 21 |
| (GTG)₅ | HinfI | 9 |
| (GTG)₅ | HinfI | 16 |
| (TCC)₅ | HinfI | 6 |
| (GGAT)n | HinfI | 14 |
| SNAP | HinfI | 7 |

### Microsatellite PCR

29 microsatellite loci detected in the horse by 1994. First horse microsatellite PCR polymorphism published 1992. Polymorphic enough for:

- Individual identification / parentage testing
- Linkage analysis
- Population genetics

Key statistical metrics reviewed: mean band sharing probability (x̄), mean population frequency of resolvable allele (q̄), average heterozygosity (H), proportion of polymorphic loci (Ppoly), genetic distance (D_r).

### Population analyses using DNA markers

- Four horse breeds studied with fingerprint probes (Andalusian, Mongolian, Quarter horse, Thoroughbred): statistically significant differences in band frequency between breeds found, indicating breed-differentiating power.
- Bernoco and Byrne used non-radioactive SNAP and (TG)₁₀ probes across 4 breeds with 7 band patterns.

### Gene mapping

- Only 5 autosomal linkage groups comprising 22 loci established in horse as of 1993 (vs. much more in cattle/pig).
- 57 loci assigned to linkage groups; 34 located on specific chromosomes.
- First microsatellite linkage: Marklund et al. (1994) assigned HTG₉ to linkage group U₅; established new linkage group U₅ (HTG₇ and HTG₁₂, with albumin at 15.7 cM, chestnut gene E at 6.9/32.7 cM, esterase Es).
- ELA (MHC): localised to chromosome 20q14–q22 by in situ hybridisation (Ansari et al. 1988, Makinen et al. 1989).
- Bkm sex-determining sequence: Y chromosome + chromosomes 3, 4, 30.
- GPI (glucosephosphate isomerase): chromosome 10pter.

### Structural genes

- Gonadotropin (LH): α-subunit (96 amino acids, 3 exons); β-subunit sequences determined.
- Sodium channel α-subunit gene: adult skeletal muscle isoform cloned from horse; relevant to hyperkalemic periodic paralysis (HYPP) — caused by mutation in this gene.
- MtDNA: cytochrome b and 12S rRNA sequenced; horse groups with tapir within order Perissodactyla.
- HYPP: sodium channel mutation, Mendelian dominant, equine breed-specific.

## Discussion & interpretation

The review reflects the state of equine molecular genetics at the inflection point between blood typing and DNA-based methods. The authors highlight that horse DNA research was sparse relative to other livestock, and that microsatellite-based parentage testing would rapidly replace blood typing. They acknowledge that applications to performance prediction (QTL mapping) were not yet established, though heritability of racing performance was noted as an existing quantitative genetics literature thread.

## Limitations

- Review up to 1994; massive developments in equine genomics (SNP chips, EquCab reference genome, GWAS for performance traits) occurred after this cutoff
- No data on racing performance QTL or speed/stamina gene markers — these did not yet exist
- Summary section is brief; the value is in the reference list (176 citations) as a bibliography for 1988–1994 horse genetics

## Feature-engineering notes for the model

- **Parentage accuracy**: This paper confirms that microsatellite parentage testing (available in JRA from the 1990s onward) provides near-certain identification, validating `sire_id`, `dam_id`, and `broodmare_sire_id` pedigree features in the pipeline
- **Pedigree depth**: The gene mapping results show that the horse genome was poorly mapped as of 1994; modern pedigree features should use post-2000 EquCab-based resources, not this paper's mapping data
- **HYPP flag**: Sodium channel mutation (hyperkalemic periodic paralysis) is a single-gene disease causing muscle weakness; relevant as a performance-limiting flag for specific bloodlines, though rare in JRA Thoroughbreds (more common in Quarter horses)
- **MHC / ELA alleles**: No direct performance association shown here; ELA typing for disease susceptibility is a research tool only

## Key references / follow-up leads

- Moritsu Y., Funakoshi H., and Ichikawa S. 1994. Genetic evaluation of sires and environmental factors influencing best racing times of thoroughbred horses in Japan. _J. Equine Sci._ 5: 53–58. (JRA racing time heritability — directly actionable for sire-effect features)
- Hintz R.L. 1980. Genetics of performance in the horse. _J. Anim. Sci._ 51: 582–594. (classic review of performance heritability)
- Tolley E.A., Notter D.R., and Marlow T.J. 1985. A review of the inheritance of racing performance in horses. _Anim. Breed. Abst._ 53: 163–185. (comprehensive heritability review)
- Marklund S., Ellegren H., Eriksson S., Sandberg K., and Andresson L. 1994. Parentage testing and linkage analysis in the horse using a set of highly polymorphic microsatellites. _Anim. Genet._ 25: 19–23. (first microsatellite linkage group assignment in horses)
- Hanzawa K. 1995. Physiogenetical studies on free amino acid types and membrane transport of amino acid in horse red cells. _Memoirs Tokyo Univ. Agric._ 36: 71–105. (free amino acid type reference [67] — foundational paper for companion paper 6_2_61)
