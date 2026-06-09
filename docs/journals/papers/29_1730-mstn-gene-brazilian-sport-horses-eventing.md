# Effect of selection for eventing on the MSTN gene in Brazilian sport horses

## Metadata

| Field                          | Value                                                                                                                                                                                                   |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 29(1): 21–24, 2018                                                                                                                                                                       |
| docid                          | `29_1730`                                                                                                                                                                                               |
| Article type                   | Note                                                                                                                                                                                                    |
| Authors                        | Felipe Gomes Ferreira Padilha, Kênia Balbi El-Jaick, Liane de Castro, Aline Dos Santos Moreira, Ana Maria Reis Ferreira                                                                                 |
| Affiliations                   | Universidade Federal Fluminense, Niterói 24230-340, Brasil; Universidade Federal do Estado do Rio de Janeiro, Rio de Janeiro 20211-040, Brasil; Fundação Oswaldo Cruz, Rio de Janeiro 21040-900, Brasil |
| Received / Accepted / Released | August 7, 2017 / November 13, 2017 / 2018                                                                                                                                                               |
| Keywords                       | Brazilian horse, myostatin, performance, skeletal muscle                                                                                                                                                |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/29/1/29_1730/_pdf/-char/en                                                                                                                                     |

## Abstract (verbatim)

> Polymorphisms in MSTN have previously been associated with equine performance. Therefore, the aim of this study was to identify variants in MSTN intron 1 in 16 Brazilian Sport Horses selected for competition in eventing and their possible effects of selection on performance. Among the nine variants identified, eight had already been reported in previous studies or genomic databases, although they showed differences in frequencies when compared with other horse breeds. Moreover, a new mutation was identified in two horses, both in heterozygous form. Considering the absence of molecular studies in this valuable Brazilian breed, these findings represent an important contribution to the characterization of its genetic profile and may possibly aid in further genotype-phenotype association studies.

## Relevance to finishing-position (着順) prediction

Feature family: **D — genetics/pedigree**. This paper characterises the MSTN (myostatin) intron-1 polymorphisms in Brazilian Sport Horses selected for 3-day eventing, and provides a direct breed-contrast reference for the most powerful known genomic predictor of optimal racing distance in Thoroughbreds: the g.66493737C>T SNP (chr18 position 66,493,737 on EquCab2.0).

The g.66493737C>T variant is the single strongest genomic predictor of optimal racing distance in Thoroughbreds (Hill et al. 2010 BMC Genomics 11:552; Hill et al. 2010 PLoS One 5:e8645): CC genotype → sprint, CT → middle distance, TT → endurance/stamina. In this Brazilian Sport Horse (BSH) population (n=16):

- CC (sprint genotype): 1/16 horses (6.3%)
- CT (middle distance): 6/16 horses (37.5%)
- TT (stamina/endurance): 9/16 horses (56.2%)
- T allele frequency: 75% (vs. 43% in Thoroughbreds from Hill et al.)
- C allele frequency: 25% (vs. 57% in Thoroughbreds from Hill et al.)

For Thoroughbreds by comparison (Hill et al. n=140):

- CC: 42/140 = 30%
- CT: 75/140 = 54%
- TT: 23/140 = 16%

The contrast is highly significant (Fisher's exact test P<0.01). Quarter Horses show an almost completely opposite profile — nearly all CC.

For the finishing-position prediction pipeline:

1. **If MSTN g.66493737C>T genotype data are available for JRA/NAR horses** (from JBBA genomic databases or the Laboratory of Racing Chemistry genomic archive), encode CC=0, CT=1, TT=2 as an ordinal distance-preference prior. Expected interaction with race distance: CC horses underperform at distances ≥2,000 m; TT horses underperform at distances ≤1,200 m.
2. **Without direct genotyping**, pedigree-based breed composition (Thoroughbred fraction vs. other breeds) carries implicit MSTN signal since allele frequencies diverge sharply by breed. A horse with a high Quarter Horse ancestor fraction is almost certainly CC; a horse with a high endurance-breed fraction is likely TT.
3. **Linkage disequilibrium:** several variants in intron 1 are in LD with g.66493737C>T (Fig. 1 in paper); a haplotype-level feature may be more informative than the single SNP alone.

This paper also documents a second variant with divergent allele frequencies: g.66493582T>G shows G allele frequency 0.19 in BSH vs. only 0.01 in Thoroughbreds (Fisher P significant), suggesting this variant may have a BSH-specific effect on performance not yet characterised.

## Background & objective

Brazilian Sport Horses (BSH) were established in 1977 by crossbreeding Thoroughbred, Hanoverian, Westfalen, Holsteiner, and Trakehner breeds to create a national breed for show jumping, dressage, and eventing. The MSTN gene encodes GDF8 (myostatin), a TGF-β family member that negatively regulates skeletal muscle mass. Variants in MSTN intron 1 affect gene expression regulation (not coding sequence); the g.66493737C>T SNP is 1,605 bp from a 227 bp insertion in the MSTN promoter that is in concordance with the C allele and causes a 5–6 fold decrease in MSTN transcription (Santagostino et al. 2015), suggesting the C allele is associated with lower myostatin expression and thus more fast-twitch muscle fibre production. Prior studies established the genotype-performance associations in Thoroughbreds; the evolutionary origin of the C allele in the Thoroughbred was traced to a single introduction from a local British horse at a foundation stage (Bower et al. 2012 Nat. Commun. 3:643).

**Objective:** characterise MSTN intron 1 variants in 16 actively competing BSH horses and compare allele/genotype frequencies with published Thoroughbred data.

## Materials & methods

**Ethics:** approved by Ethics Committee for the Use of Animals, Universidade Federal Fluminense (protocol 276/2013).

**Subjects:** n=16 Brazilian Sport Horses, all in training and regularly competing in 3-day events. Sample carefully chosen to minimise inter-individual relatedness (inbreeding coefficient ≈0 for this breed).

**Genomic region:** MSTN intron 1, chr18:66,493,261–66,494,452 on NCBI EquCab2.0 (NC_009161.2). Two primer pairs: MSTN_10 (amplifying 66493261–66493840) and MSTN_11 (amplifying 66493779–66494452), as described by Hill et al. (2010).

**Method:** Sanger DNA sequencing of the PCR products from both primer pairs.

**Variant nomenclature:** HGVS standards (Den Dunnen et al. 2016).

**Statistical tests:**

- Hardy–Weinberg equilibrium (HWE): standard chi-square test on observed vs. expected genotype frequencies
- Allele and genotype frequency comparisons with Thoroughbreds: Fisher's exact test
- Linkage disequilibrium: haplotype analysis visualised in Fig. 1

**Comparison dataset:** Hill et al. (2010 PLoS One 5:e8645) for the g.66493737C>T genotypes (n=140 Thoroughbreds) and Hill et al. (2010 BMC Genomics 11:552) for other variants.

## Results (detailed — reproduce ALL numbers)

### All nine variants identified in BSH intron 1 (Table 1)

| Genomic position | Substitution | BSH WT hom n (%) | BSH het n (%) | BSH mut hom n (%) | BSH p (WT)   | BSH q (mut)  | TB p          | TB q          | Fisher P             |
| ---------------- | ------------ | ---------------- | ------------- | ----------------- | ------------ | ------------ | ------------- | ------------- | -------------------- |
| 18:66493519      | G>A          | 14 GG (87.5%)    | 2 GA (12.5%)  | 0 AA (0%)         | 0.937        | 0.063        | —             | —             | — (novel, not in TB) |
| 18:66493525      | T>G          | 13 TT (81.3%)    | 3 TG (18.7%)  | 0 GG (0%)         | 0.906        | 0.094        | 0.969         | 0.031         | significant          |
| 18:66493582      | T>G          | 10 TT (62.5%)    | 6 TG (37.5%)  | 0 GG (0%)         | 0.813        | **0.187**    | 0.989         | **0.011**     | **significant**      |
| 18:66493737      | C>T          | 1 CC (6.3%)      | 6 CT (37.5%)  | 9 TT (56.2%)      | **0.25** (C) | **0.75** (T) | **0.568** (C) | **0.432** (T) | **P<0.01**           |
| 18:66493745      | A>G          | 14 AA (87.5%)    | 2 AG (12.5%)  | 0 GG (0%)         | 0.937        | 0.063        | 0.973         | 0.027         | —                    |
| 18:66493775      | A>G          | 13 AA (81.3%)    | 3 AG (18.7%)  | 0 GG (0%)         | 0.906        | 0.094        | 0.976         | 0.024         | —                    |
| 18:66494218      | A>C          | 9 AA (56.3%)     | 6 AC (37.5%)  | 1 CC (6.3%)       | 0.750        | 0.250        | 0.675         | 0.325         | —                    |
| 18:66494302      | A>G          | 14 AA (87.5%)    | 2 AG (12.5%)  | 0 GG (0%)         | 0.937        | 0.063        | —             | —             | — (BSH only)         |
| 18:66494367      | G>A          | 9 GG (56.3%)     | 7 GA (43.7%)  | 0 AA (0%)         | 0.781        | 0.219        | —             | —             | — (BSH only)         |

TB sample sizes per variant: n=145–146 (MSTN_10 region) and n=137 (18:66494218), as reported by Hill et al.

**Key quantitative comparison for g.66493737C>T:**

- BSH: T allele = 24/32 alleles = **75%**; C allele = 8/32 = **25%**
- Thoroughbred (Hill et al. n=140): C allele = 159/280 = **56.8%**; T allele = 121/280 = **43.2%**
- Genotype frequencies BSH: CC=1 (6.3%), CT=6 (37.5%), TT=9 (56.2%)
- Genotype frequencies TB: CC=42 (30%), CT=75 (53.6%), TT=23 (16.4%)

**g.66493582T>G (second most divergent variant):**

- BSH: G allele frequency 0.187 (6/32 alleles)
- TB: G allele frequency 0.011 (3/276 alleles)
- P significant by Fisher's exact test

**Hardy–Weinberg equilibrium:** BSH genotype distribution does NOT conform to HWE for g.66493737C>T, consistent with directional selection for endurance phenotype in this breed.

**Novel variant:** g.66493519G>A — found in 2/16 BSH in heterozygous form (GA); not reported in literature or GenBank databases at the time of publication.

**Linkage disequilibrium:** Fig. 1 in paper shows haplotypes across the 9 variants; several variants co-segregate, confirming LD structure within BSH intron 1.

**Comparison with other breeds:**

- Quarter Horses: almost all CC at g.66493737 (Pereira et al. 2016; only one non-CC individual in the study)
- Chinese native horses (Li et al. 2014): lower T allele frequency than BSH
- Icelandic horses (Velie et al. 2015): MSTN polymorphism frequencies correlate with intended use (pacing/speed vs. leisure)

## Discussion & interpretation

The very high T allele frequency in BSH (75% vs. 43% in Thoroughbreds) reflects directional selection pressure for endurance performance in 3-day eventing (which includes a cross-country phase requiring stamina over 5–8 km). The deficit of HWE in BSH confirms non-random genotype frequencies consistent with artificial selection for TT (endurance). The C allele in Thoroughbreds was introduced by a single British horse at a foundation stage and subsequently selected for sprint racing, raising its frequency from the low ancestral level (comparable to modern TT-dominant populations) to 57%.

The g.66493582T>G variant showing G allele frequency 0.187 in BSH vs. 0.011 in TB may be a breed-specific variant or be in LD with performance-relevant variants not fully characterised in eventing breeds. The novel g.66493519G>A (heterozygous in 2 horses) has unknown functional significance.

The paper corroborates Bower et al. (2012) evolutionary model: before the C-allele introduction into Thoroughbreds, the ancestral horse population (and by extension BSH, which was founded from non-sprint European warmbloods) had a high T allele frequency, reflecting the general-purpose/endurance phenotype.

## Limitations

- n=16 is very small; allele frequency estimates have wide confidence intervals. Genotype frequencies (1 CC, 6 CT, 9 TT) are based on a carefully chosen but not population-representative sample.
- Only intron 1 sequenced; the coding sequence, promoter (including the 227 bp insertion in concordance with C allele), and other regulatory regions were not assessed.
- No direct performance measurements (race times, finish positions) linked to genotypes in this study.
- Thoroughbred comparison data are from Hill et al. (2010), measured in Irish/UK Thoroughbreds; Japanese Thoroughbred allele frequencies may differ.
- BSH inbreeding coefficient ≈0 (by breed design), but exact genealogical verification not reported.

## Feature-engineering notes for the model

- `mstn_g66493737_genotype` — CC=0 (sprint), CT=1 (middle distance), TT=2 (endurance/stamina) ordinal encoding. Source: JBBA genomic records or Laboratory of Racing Chemistry genotyping archive. Expected effect: interact with `race_distance`; CC horses optimised for ≤1,400 m (JRA sprint), TT horses optimised for ≥2,400 m (JRA long distance). Expected sign: `mstn_genotype × race_distance` positive interaction (TT bonus increases with distance; CC bonus decreases with distance).
- `mstn_c_allele_count` — count of C alleles (0, 1, 2); continuous proxy for sprint genetic potential. Alternative encoding to ordinal.
- `breed_tb_fraction` — fraction of Thoroughbred ancestry from pedigree; higher TB fraction implies higher expected C allele frequency. Source: JBBA pedigree database. Use as a surrogate when direct genotype unavailable.
- `breed_quarter_horse_fraction` — Quarter Horse ancestry; nearly always CC; high QH fraction is a strong sprint prior.
- **Interaction terms:**
  - `mstn_genotype × race_distance_m` — the primary interaction; distance-optimality by genotype.
  - `mstn_genotype × surface` — dirt vs. turf; some evidence sprint-type horses may prefer dirt (shorter distances typical at NAR dirt tracks).
  - `mstn_genotype × going` — firm going favours sprint types; soft going may attenuate the distance-genotype interaction.
- **Linkage disequilibrium note:** if only a proxy variant (e.g., g.66493582T>G with G allele as partial endurance signal) is available, use it with reduced weight; it is not as validated as g.66493737C>T.
- **Do NOT use** this feature to assign races to horses (trainer decisions); use only as a predictor within the horse's given race entry.

## Key references / follow-up leads

- Hill EW, McGivney BA, Gu J, Whiston R, Machugh DE. 2010. A genome-wide SNP-association study confirms a sequence variant (g.66493737C>T) in the equine myostatin (MSTN) gene as the most powerful predictor of optimum racing distance for Thoroughbred racehorses. BMC Genomics 11: 552–561. — the primary GWAS paper; essential for feature engineering.
- Hill EW, Gu J, Eivers SS et al. 2010. A sequence polymorphism in MSTN predicts sprinting ability and racing stamina in thoroughbred horses. PLoS One 5: e8645. — companion paper confirming association with race outcome data.
- Bower MA, McGivney BA, Campana MG et al. 2012. The genetic origin and history of speed in the Thoroughbred racehorse. Nat. Commun. 3: 643. — evolutionary history of the C allele in Thoroughbreds; confirms single-foundation introduction.
- Santagostino M, Khoriauli L, Gamba R et al. 2015. Genome-wide evolutionary and functional analysis of the Equine Repetitive Element 1: an insertion in the myostatin promoter affects gene expression. BMC Genet. 16: 126. — 227 bp promoter insertion in LD with C allele causes 5–6× reduced MSTN transcription.
- Velie BD, Jäderkvist K, Imsland F et al. 2015. Frequencies of polymorphisms in myostatin vary in Icelandic horses according to the use of the horses. Anim. Genet. 46: 467–468. — breed-specific frequencies in non-racing horses.
- Pereira GL, de Matteis R, Regitano LCA et al. 2016. MSTN, CKM and DMRT3 gene variants in different lines of Quarter Horses. J. Equine Vet. Sci. 39: 33–37. — Quarter Horse nearly all CC; strong breed contrast.
- Binns MM, Boehler DA, Lambert DH. 2010. Identification of the myostatin locus (MSTN) as having a major effect on optimum racing distance in the Thoroughbred horse in the USA. Anim. Genet. 41(Suppl 2): 154–158. — US Thoroughbred confirmation.
