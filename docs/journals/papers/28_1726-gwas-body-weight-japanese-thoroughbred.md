# A genome-wide association study for body weight in Japanese Thoroughbred racehorses clarifies candidate regions on chromosomes 3, 9, 15, and 18

| Field        | Value                                                                                |
| ------------ | ------------------------------------------------------------------------------------ |
| Journal      | J. Equine Sci. Vol. 28, No. 4, pp. 127–134, 2017                                     |
| docid        | `28_1726`                                                                            |
| Article type | Full Paper                                                                           |
| Authors      | Teruaki TOZAKI, Mio KIKUCHI, Hironaga KAKOI, Kei-ichi HIROTA, Shun-ichi NAGATA       |
| Affiliation  | Genetic Analysis Department, Laboratory of Racing Chemistry, Tochigi 320-0851, Japan |
| Received     | July 3, 2017                                                                         |
| Accepted     | August 21, 2017                                                                      |
| Keywords     | body weight, LCORL, MSTN, Thoroughbred, ZFAT                                         |
| PDF          | https://www.jstage.jst.go.jp/article/jes/28/4/28_1726/_pdf/-char/en                  |
| License      | CC-BY-NC-ND 4.0                                                                      |

## Abstract

> Body weight is an important trait to confirm growth and development in humans and animals. In Thoroughbred racehorses, it is measured in the postnatal, training, and racing periods to evaluate growth and training degrees. The body weight of mature Thoroughbred racehorses generally ranges from 400 to 600 kg, and this broad range is likely influenced by environmental and genetic factors. Therefore, a genome-wide association study (GWAS) using the Equine SNP70 BeadChip was performed to identify the genomic regions associated with body weight in Japanese Thoroughbred racehorses using 851 individuals. The average body weight of these horses was 473.9 kg (standard deviation: 28.0) at the age of 3, and GWAS identified statistically significant SNPs on chromosomes 3 (BIEC2_808466, P=2.32E-14), 9 (BIEC2_1105503, P=1.03E-7), 15 (BIEC2_322669, P=9.50E-6), and 18 (BIEC2_417274, P=1.44E-14), which were associated with body weight as a quantitative trait. The genomic regions on chromosomes 3, 9, 15, and 18 included ligand-dependent nuclear receptor compressor-like protein (LCORL), zinc finger and AT hook domain containing (ZFAT), tribbles pseudokinase 2 (TRIB2), and myostatin (MSTN), respectively, as candidate genes. LCORL and ZFAT are associated with withers height in horses, whereas MSTN affects muscle mass. Thus, the genomic regions identified in this study seem to affect the body weight of Thoroughbred racehorses. Although this information is useful for breeding and growth management of the horses, the production of genetically modified animals and gene doping (abuse/misuse of gene therapy) should be prohibited to maintain horse racing integrity.

## Relevance to finishing-position (着順) prediction

This paper belongs to **feature family D (genetics/pedigree)** and provides a quantitative genomic foundation for the body-weight feature that JRA already records at every race entry (馬体重). The GWAS confirms that body weight in Japanese Thoroughbreds has heritability h² = 0.27 ± 0.04, and that four SNP loci — on chromosomes 3 (LCORL), 9 (ZFAT), 15 (TRIB2), and 18 (MSTN) — jointly explain 17.4% of weight variance and 30.0% when sex is added. This means roughly 13% of weight variation explained by sex alone, and ~17% by the four SNP loci independently of sex.

The most directly actionable finding for prediction models is that MSTN (myostatin, chromosome 18) controls skeletal muscle mass and has been separately shown by the same group (Tozaki et al. 2010, 2012) to influence optimal race distance and racing performance in Japanese Thoroughbreds. The BIEC2_417274 SNP at chr18:65,868,604 bp (MAF=0.478) has body weight range 457.5–482.1 kg across genotypes (ANOVA P<1.0E-4) and was previously identified as a candidate SNP for racing performance. If MSTN genotype data are available for a horse (through the Lab of Racing Chemistry "Speed Gene Test"), it is a direct genomic feature encoding muscle fibre type composition (Type I/IIa/IIx ratio), aerobic/anaerobic capacity, and distance optimum.

LCORL (chromosome 3, BIEC2_808466, MAF=0.112) shows the largest individual-locus body weight range (470.1–510.0 kg, 39.9 kg spread) and controls skeletal frame size and withers height. Stride length — a direct determinant of speed — scales with withers height; thus the LCORL signal provides a genetic proxy for conformation-based speed potential. ZFAT (chromosome 9) and TRIB2 (chromosome 15) show smaller but still significant effects (genotype BW ranges 18.4 and 18.5 kg respectively).

For the standard JRA prediction pipeline without SNP data, the raw race-day body weight (馬体重) feature already captures the phenotypic expression of these loci. The paper supports using body-weight change between races (Δ馬体重) as an additional feature, since the GWAS confirms a genetic baseline against which training-induced weight flux is meaningful. Age×sex interactions on body weight should also be modelled explicitly: mean BW rises 468.8→473.9→478.8 kg from age 2 to 4, and sex is the single largest environmental predictor, adding 12.6% to variance explained beyond the four genetic loci.

## Background & objective

The Thoroughbred breed was developed in the early 18th century from a small number of Arabian, Barb, and Turkmen stallions and British native mares; all modern horses trace to this narrow founding population (~300 years ago). About 100,000 foals are born worldwide annually, almost all used in racing.

Body weight in mature Thoroughbreds typically ranges 400–600 kg. Japanese Thoroughbreds average 455 kg at first start (mean age 897 days); Korean Thoroughbreds average 460 kg (stallions), 454 kg (geldings), 441 kg (mares). Heritability of body weight is moderate: Japanese Thoroughbreds h²=0.27±0.04; Brazilian army cavalry horses h²=0.40±0.034; Korean stallions/geldings/mares h²=0.578/0.472/0.555 respectively. This genetic component, combined with the completion of the Horse Genome Project and availability of >1 million SNP markers, makes GWAS tractable. However, the genetic architecture of body weight — a complex polygenic trait — was not yet described for Japanese Thoroughbreds.

**Objective:** Perform GWAS using the Equine SNP70 BeadChip on 851 Japanese JRA Thoroughbreds to identify genomic regions associated with body weight as a quantitative trait, and characterise candidate genes in those regions.

## Materials & methods

### Subjects and phenotype collection

- **n = 851** Japanese Thoroughbred racehorses: **619 males** (stallions/geldings) and **232 females**
- All horses were well trained and had racing experience in the Japan Racing Association (JRA)
- Additional age cohorts (same population, different N): 535 horses at age 2; 734 horses at age 4
- **Body weight source:** Official JRA race records — measured at each horse's race attendance. Body weights used were from the **final race** at each age. Most measurements were taken **September to December** (consistent seasonal window)
- GWAS was performed on **3-year-old** data (largest cohort, n=851; horses nearly mature at this age, minimising residual growth confounding)
- Mean body weight at age 3: **473.9 ± 28.0 kg**

### Genotyping

- **Platform:** EquineSNP70 Genotyping BeadChip (Illumina, San Diego, CA, USA)
- **Total SNPs on array:** 65,157 SNPs ascertained from the EquCab2 horse genome database
- **Genotyping laboratory:** Neogen GeneSeek, Lincoln, NE, USA
- **DNA extraction:** Blood samples collected and stored at −40°C; genomic DNA extracted using MFX-2000 MagExtractor System (Toyobo, Osaka, Japan)
- **DNA quality:** Assessed by spectrophotometric absorbance; 260/230 and 260/280 ratios validated
- **Minimum genotyping rate per sample:** >90%

### Quality control

SNPs excluded if:

1. Genotyping completion rate < 99%
2. Significant deviation from Hardy-Weinberg equilibrium (P < 1.0E-6)
3. Monomorphic
4. Minor allele frequency (MAF) < 5%

**SNPs retained after QC: 44,306** (from original 65,157; ~32% excluded)

### Statistical analysis

- **GWAS software:** PLINK version 1.07 (Purcell et al. 2007; http://pngu.mgh.harvard.edu/purcell/plink/)
- **Test:** Quantitative trait association (linear regression of body weight on SNP genotype)
- **Significance thresholds:** P < 1.00E-8 (genome-wide significant) and P < 1.00E-5 (suggestive)
- **Visualisation:** Manhattan and Q-Q plots generated using qqman R package (Turner 2014)
- **Variance explained:** Estimated using normal linear model by comparing residual variance of null model (sex only, VN) to full model (sex + relevant markers, VF): proportion = 1 − (VF/VN)
- **Genotype group comparisons:** One-way ANOVA for body weight differences across genotype classes at each candidate SNP
- **Descriptive statistics:** Mean, SD, quartiles, minimum, maximum calculated per age group in R

## Results

### Body weight variation by age

**Table 1. Body weight of Japanese Thoroughbred racehorses at three ages**

| Statistic                     | 2 years old (n=535) | 3 years old (n=851) | 4 years old (n=734) |
| ----------------------------- | ------------------- | ------------------- | ------------------- |
| Mean (kg)                     | 468.8               | 473.9               | 478.8               |
| SD (kg)                       | 26.1                | 28.0                | 27.6                |
| Minimum (kg)                  | 382                 | 390                 | 395                 |
| First quartile (kg)           | 452                 | 456                 | 460                 |
| Second quartile (median) (kg) | 468                 | 473                 | 478                 |
| Third quartile (kg)           | 485                 | 492                 | 496                 |
| Maximum (kg)                  | 568                 | 568                 | 604                 |

Body weight increased significantly with age (ANOVA: P<0.01). The minimum observed was 382 kg (2-year-old); maximum was 604 kg (4-year-old, representing an extreme 36 kg gain at that age). Quartile patterns tracked the mean trend.

### GWAS results

**Significant genomic regions** (P < 1.00E-8 and/or P < 1.00E-5):

- **Chromosome 3:** 9 SNPs spanning 104,403,770–108,176,636 bp (3.77 Mb)
- **Chromosome 9:** 7 SNPs spanning 74,756,685–77,100,429 bp (2.34 Mb)
- **Chromosome 15:** 1 SNP at 80,921,871 bp
- **Chromosome 18:** 30 SNPs spanning 59,614,257–78,131,099 bp (18.52 Mb)

**Table 2. Complete GWAS results — all 47 significant SNPs (PLINK linear regression)**

| Chr | SNP               | Position (bp)   | BETA       | SE        | R²          | T         | P            |
| --- | ----------------- | --------------- | ---------- | --------- | ----------- | --------- | ------------ |
| 3   | BIEC2_808344      | 104,403,770     | 8.639      | 1.539     | 0.03577     | 5.612     | 2.71E-08     |
| 3   | BIEC2_808426      | 104,941,985     | 12.180     | 1.735     | 0.05489     | 7.018     | 4.62E-12     |
| 3   | BIEC2_808432      | 104,986,284     | 9.279      | 1.656     | 0.03572     | 5.601     | 2.87E-08     |
| 3   | **BIEC2_808466**  | **105,163,077** | **17.140** | **2.206** | **0.06633** | **7.766** | **2.32E-14** |
| 3   | BIEC2_808608      | 105,875,809     | −6.841     | 1.389     | 0.02783     | −4.924    | 1.02E-06     |
| 3   | BIEC2_808617      | 105,876,397     | 8.985      | 1.385     | 0.04728     | 6.487     | 1.49E-10     |
| 3   | BIEC2_808640      | 105,947,243     | 7.933      | 1.584     | 0.02873     | 5.009     | 6.67E-07     |
| 3   | BIEC2_808825      | 106,710,385     | 9.541      | 1.491     | 0.04599     | 6.397     | 2.61E-10     |
| 3   | BIEC2_809156      | 108,176,636     | 6.923      | 1.429     | 0.02695     | 4.844     | 1.51E-06     |
| 9   | BIEC2_1165712     | 74,756,685      | 7.002      | 1.474     | 0.02590     | 4.749     | 2.40E-06     |
| 9   | BIEC2_1105370     | 74,795,013      | 6.905      | 1.440     | 0.02639     | 4.794     | 1.93E-06     |
| 9   | BIEC2_1105377     | 74,798,143      | 6.905      | 1.440     | 0.02639     | 4.794     | 1.93E-06     |
| 9   | BIEC2_1165849     | 75,245,697      | 7.284      | 1.460     | 0.02853     | 4.987     | 7.43E-07     |
| 9   | **BIEC2_1105503** | **75,374,649**  | **7.975**  | **1.486** | **0.03282** | **5.368** | **1.03E-07** |
| 9   | BIEC2_1105810     | 76,167,910      | 6.908      | 1.361     | 0.02944     | 5.075     | 4.76E-07     |
| 9   | BIEC2_1105995     | 77,100,429      | 7.948      | 1.601     | 0.02826     | 4.966     | 8.27E-07     |
| 15  | **BIEC2_322669**  | **80,921,871**  | **8.871**  | **1.991** | **0.02303** | **4.455** | **9.50E-06** |
| 18  | BIEC2_416419      | 59,614,257      | 8.170      | 1.713     | 0.02634     | 4.770     | 2.17E-06     |
| 18  | BIEC2_416680      | 62,049,912      | 7.233      | 1.500     | 0.02682     | 4.822     | 1.68E-06     |
| 18  | BIEC2_416681      | 62,054,146      | 6.872      | 1.491     | 0.02444     | 4.609     | 4.67E-06     |
| 18  | BIEC2_416683      | 62,115,222      | 6.724      | 1.490     | 0.02351     | 4.513     | 7.28E-06     |
| 18  | BIEC2_416689      | 62,117,055      | 6.686      | 1.504     | 0.02277     | 4.445     | 9.94E-06     |
| 18  | BIEC2_416704      | 62,148,769      | 7.220      | 1.505     | 0.02654     | 4.797     | 1.90E-06     |
| 18  | BIEC2_438369      | 63,193,752      | −6.071     | 1.345     | 0.02344     | −4.514    | 7.24E-06     |
| 18  | BIEC2_416921      | 63,708,318      | 8.945      | 1.389     | 0.04701     | 6.441     | 1.99E-10     |
| 18  | BIEC2_416992      | 64,245,075      | −6.740     | 1.356     | 0.02836     | −4.972    | 8.03E-07     |
| 18  | BIEC2_438541      | 64,252,426      | 7.601      | 1.344     | 0.03645     | 5.657     | 2.10E-08     |
| 18  | BIEC2_417010      | 64,391,698      | 9.084      | 1.372     | 0.04930     | 6.620     | 6.40E-11     |
| 18  | BIEC2_417030      | 64,481,405      | 6.039      | 1.347     | 0.02320     | 4.482     | 8.40E-06     |
| 18  | BIEC2_417075      | 64,725,066      | 8.965      | 1.349     | 0.04946     | 6.646     | 5.37E-11     |
| 18  | BIEC2_417120      | 64,919,213      | −6.439     | 1.412     | 0.02399     | −4.560    | 5.86E-06     |
| 18  | BIEC2_417187      | 65,565,128      | −7.881     | 1.332     | 0.03963     | −5.915    | 4.80E-09     |
| 18  | **BIEC2_417274**  | **65,868,604**  | **10.150** | **1.297** | **0.06737** | **7.831** | **1.44E-14** |
| 18  | UKUL3221          | 65,969,033      | 9.934      | 1.290     | 0.06531     | 7.698     | 3.85E-14     |
| 18  | BIEC2_417291      | 66,010,474      | −8.987     | 1.306     | 0.05281     | −6.880    | 1.16E-11     |
| 18  | BIEC2_417308      | 66,155,365      | −7.025     | 1.328     | 0.03211     | −5.291    | 1.55E-07     |
| 18  | BIEC2_438865      | 66,158,121      | 10.090     | 1.295     | 0.06666     | 7.787     | 2.00E-14     |
| 18  | BIEC2_417372      | 66,539,967      | −7.253     | 1.377     | 0.03180     | −5.268    | 1.75E-07     |
| 18  | BIEC2_417423      | 66,819,091      | −7.847     | 1.370     | 0.03726     | −5.729    | 1.40E-08     |
| 18  | BIEC2_438994      | 66,862,437      | −6.296     | 1.412     | 0.02289     | −4.460    | 9.29E-06     |
| 18  | BIEC2_438997      | 66,892,380      | −6.771     | 1.449     | 0.02508     | −4.673    | 3.45E-06     |
| 18  | BIEC2_417454      | 66,996,889      | 7.087      | 1.399     | 0.02951     | −5.066    | 5.00E-07     |
| 18  | BIEC2_417524      | 67,545,703      | −8.047     | 1.351     | 0.04012     | −5.957    | 3.76E-09     |
| 18  | BIEC2_417704      | 69,258,120      | −7.792     | 1.356     | 0.03750     | −5.748    | 1.26E-08     |
| 18  | BIEC2_417806      | 69,969,173      | −7.272     | 1.324     | 0.03468     | −5.493    | 5.24E-08     |
| 18  | BIEC2_418057      | 71,300,017      | −7.885     | 1.540     | 0.02997     | −5.121    | 3.75E-07     |
| 18  | BIEC2_421048      | 78,131,099      | −6.230     | 1.395     | 0.02297     | −4.465    | 9.11E-06     |

BETA = regression coefficient (kg per minor allele); SE = standard error; R² = per-SNP regression r-squared; T = Wald test statistic; P = Wald asymptotic P-value.

### Annotated genes in candidate regions

**Table 3. Number of annotated genes in each candidate genomic region**

| Gene category        | Chr 3 (3.77 Mb) | Chr 9 (2.34 Mb) | Chr 15 | Chr 18 (18.52 Mb) |
| -------------------- | --------------- | --------------- | ------ | ----------------- |
| Protein-coding genes | 19              | 2               | 1      | 108               |
| Noncoding RNAs       | 7               | 2               | 0      | 11                |
| MicroRNAs            | 0               | 2               | 0      | 1                 |
| Pseudogenes          | 1               | 0               | 0      | 12                |
| **Total**            | **27**          | **6**           | **1**  | **132**           |

### Genotype effect on body weight at candidate SNPs

**Table 4. Body weight by genotype at each lead candidate SNP (ANOVA)**

| Chr | SNP           | Position (bp) | MAF   | P-value  | Candidate gene | Homozygote major (kg) | Heterozygote (kg) | Homozygote minor (kg) | ANOVA P |
| --- | ------------- | ------------- | ----- | -------- | -------------- | --------------------- | ----------------- | --------------------- | ------- |
| 3   | BIEC2_808466  | 105,163,077   | 0.112 | 2.32E-14 | LCORL          | 470.1                 | 487.1             | 510.0                 | 0.00024 |
| 9   | BIEC2_1105503 | 75,374,649    | 0.273 | 1.03E-07 | ZFAT           | 470.0                 | 476.4             | 488.4                 | <1.0E-4 |
| 15  | BIEC2_322669  | 80,921,871    | 0.133 | 9.50E-06 | TRIB2          | 471.7                 | 480.4             | 490.2                 | <1.0E-4 |
| 18  | BIEC2_417274  | 65,868,604    | 0.478 | 1.44E-14 | MSTN           | 457.5                 | 479.4             | 482.1                 | <1.0E-4 |

MAF = minor allele frequency (note: LCORL MAF stated as 0.112 in Table 4, though the discussion mentions 0.0125 — the 0.112 value from Table 4 is the published table value).

### Variance explained

- **Four SNP loci alone:** explain **17.4%** of body weight variance in the 851 three-year-old Japanese Thoroughbreds
- **Four SNP loci + sex:** explain **30.0%** of body weight variance (sex contributes ~12.6% on top of the four loci)
- Heritability of body weight in Japanese Thoroughbreds: h² = 0.27 ± 0.04 (Moritsu et al. 1997)
- Consistent significant GWAS results also observed in 2-year-old (n=535) and 4-year-old (n=734) cohorts (data not shown in paper), confirming the four loci are not age-specific artefacts

## Discussion & interpretation

**LCORL (chr 3):** Encodes a transcription factor functioning in spermatogenesis; affects withers height in horses. The most significant SNP for withers height in previous studies is BIEC2_808543 (105,547,002 bp), 63.3 kb upstream of LCORL; the present study's top SNP BIEC2_808466 (105,163,077 bp) is 447.3 kb upstream. No protein-coding genes are found between BIEC2_808466 and LCORL, supporting LCORL as the effector gene. A previous study (Tozaki et al. 2016) showed BIEC2_808543 near LCORL associated with withers height and cannon circumference in training Thoroughbreds (1–2-year-olds), affecting body weight; the same logic applies to mature horses. LCORL is also associated with skeletal frame and body size in humans and cattle (Soranzo et al. 2009; Lindholm-Perry et al. 2011). LCORL genotype spans the largest weight range of the four loci (39.9 kg, 470.1–510.0 kg). The LRC now provides a commercial "withers height test" based on LCORL genotyping.

**ZFAT (chr 9):** Encodes a transcriptional regulator involved in apoptosis, cell survival, and hematopoietic differentiation. Associated with body height in Japanese and Korean human populations (Takeuchi et al. 2009), and with withers height in multiple horse breeds (Makvandi-Nejad et al. 2012). Two SNPs (BIEC2_1105370 and BIEC2_1105377) were also identified in previous horse conformation GWAS. ZFAT contributes less than LCORL to withers height in Thoroughbreds; further analysis required to dissect its independent contribution to body weight.

**TRIB2 (chr 15):** One of three members of the Tribbles pseudokinase family; encodes a scaffold protein in intracellular signal transduction. Associated in humans with internal fat area and pericardial fat volume (Nakayama et al. 2013). A positive natural selection signature for TRIB2 exists in East Asian populations, suggesting metabolic/fat deposition relevance. The single identified SNP BIEC2_322669 is 178.9 kb downstream of TRIB2; another suggestive SNP BIEC2_323180 (P=2.70E-5) is 350.3 kb upstream. No SNPs in the region directly containing TRIB2 (80,950,328–81,428,857 bp) reached significance, so the causal mechanism remains unclear for horses.

**MSTN (chr 18):** A member of the TGF-β superfamily that negatively regulates skeletal muscle cell proliferation and differentiation. Mutations cause increased muscle mass in humans and mammals; heterozygous loss-of-function in Greyhounds enhances racing performance (Mosher et al. 2007). Three top SNPs (BIEC2_417274, UKUL3221, BIEC2_438865) cluster around the MSTN locus; BIEC2_438865 is 332 kb downstream. Critically, BIEC2_417274 was previously identified as a candidate SNP for racing performance and optimal race distance in Japanese Thoroughbreds (Tozaki et al. 2012 cohort study). MSTN affects muscle fibre type ratios (I/IIa/IIx), thereby influencing both aerobic endurance and anaerobic sprint capacity and hence optimal distance. Previous work (Tozaki et al. 2011) showed SNPs near MSTN associated with muscle mass at withers height in 1–2-year-old Thoroughbreds in training. The LRC provides the "Speed Gene Test" (Japanese Patent No. 5667057) to genotype MSTN for racing clients.

**Non-identified loci:** HMGA2 (chr 6) and LASP1 (chr 11) — candidates for withers height in other studies — were not significant in this GWAS, suggesting these variants may be absent from Japanese Thoroughbreds or contribute less than LCORL/ZFAT to weight in this population.

**Integrity implications:** LCORL, ZFAT, TRIB2, and MSTN are useful for feeding management, training optimisation, and breeding decisions. However, their identification raises concerns about genetically modified racehorses and MSTN-targeted gene doping; IFHA prohibits both genetic modification and gene doping, and monitoring/inspection methods for gene doping are needed.

## Limitations

- Body weights collected from a narrow seasonal window (September–December) for each age cohort; small birth-month vs. measurement-month age differences may introduce minor confounding (considered negligible at age 3 since horses are nearly mature)
- Single-country cohort (JRA Japanese Thoroughbreds); generalisability to other national Thoroughbred populations requires verification
- BIEC2_808543 (the most significant LCORL SNP for withers height in prior studies) was excluded during QC in the present study, which may have slightly reduced power for the LCORL region
- For TRIB2, no SNPs within the gene body reached statistical significance; the causal variant is not resolved
- Chromosome 18 spans a very large 18.5 Mb region with 108 protein-coding genes; MSTN is the prioritised candidate but other genes in this region cannot be excluded
- Sample sizes within individual genotype classes for rare SNPs (e.g., LCORL MAF=0.112) are small, affecting precision of genotype-specific mean weight estimates
- Environmental factors (facility, feeding management, training intensity) and their interaction with genotype are not modelled
- Only body weight from official race records is available; other weight measurements (training, post-race) that might better capture fitness state are not analysed

## Feature-engineering notes

- **馬体重 (race_weight)** — Official JRA body weight in kg at each race entry. Source: JRA public race data. Expected effect: non-linear U-shaped around breed optimum (~470–490 kg); very heavy or light horses may underperform. Data availability: 100% for JRA; available as time-series across a horse's career.
- **delta_race_weight** — Change in body weight from previous race (kg). Source: JRA race data, computed as difference between consecutive race entries. Expected effect: negative extreme drops (>10 kg) may signal illness/injury; positive extreme gains (>8 kg) may signal reduced training load. Data availability: 100% for JRA with ≥2 career starts.
- **race_weight_z_age** — Body weight z-scored within age×sex group (normalised against the mean and SD from Table 1 of this paper). Source: JRA race data. Expected effect: captures whether a horse is over/under-weight relative to age/sex peers, removing age-related growth trend.
- **mstn_genotype** — MSTN SNP BIEC2_417274 genotype (0/1/2 copies of minor allele). Source: LRC Speed Gene Test; not publicly available for all horses. Expected effect: heterozygotes/minor homozygotes have higher muscle mass, better sprint capacity, and shorter optimal race distance. Body weight range 457.5–482.1 kg across genotypes.
- **lcorl_genotype** — LCORL SNP BIEC2_808466 genotype. Source: LRC withers height test. Expected effect: minor allele homozygotes average 510.0 kg vs 470.1 kg for major homozygotes (39.9 kg difference); larger skeletal frame → longer stride → better at longer distances. MAF=0.112 means minor homozygotes are rare (≈1.25%).
- **zfat_genotype** — ZFAT SNP BIEC2_1105503 genotype. Source: Research/LRC genotyping only. Expected effect: 18.4 kg weight range across genotypes; associated with withers height.
- **gwas_bw_polygenic_score** — Linear combination of LCORL + ZFAT + TRIB2 + MSTN SNP allele counts, weighted by BETA coefficients from Table 2. Source: SNP data if available. Expected effect: explains 17.4% of body weight variance; captures genetic baseline weight independent of training status.
- **sex_weight_interaction** — Product of sex (M=1, F=0) × race weight. Source: JRA race data. Expected effect: sex explains ~12.6% of body weight variance on top of the four genetic loci; the sex×weight interaction likely has different implications for performance (stallions heavier but not necessarily faster).
- **body_weight_heritability_context** — h²=0.27±0.04 for JRA Japanese Thoroughbreds means ~27% of phenotypic variance is genetic; sire and dam lineage features (already in prediction models) implicitly capture this genetic variation even without direct SNP data.

## Key references / follow-up leads

- **Tozaki et al. 2012** (Anim. Genet. 43: 42–52) — Cohort study of racing performance in Japanese Thoroughbreds using ECA18 genome information; identified BIEC2_417274 (MSTN region) as candidate SNP for racing performance and optimal race distance. Direct follow-up for race distance optimum modelling.
- **Tozaki et al. 2010** (Anim. Genet. 41(Suppl 2): 28–35) — GWAS for racing performances in Thoroughbreds; clarified candidate region near MSTN gene. First major GWAS linking MSTN to race performance.
- **Tozaki et al. 2011** (J. Vet. Med. Sci. 73: 1617–1624) — MSTN sequence variants and body composition in Thoroughbreds under training; muscle mass at withers height in 1–2-year-olds.
- **Tozaki et al. 2016** (J. Equine Sci. 27: 107–114) — BIEC2_808543 near LCORL associated with body composition (withers height, cannon circumference) in training Thoroughbreds.
- **Makvandi-Nejad et al. 2012** (PLoS One 7: e39929) — "Four loci explain 83% of size variation in the horse" — broader cross-breed horse size GWAS including LCORL, ZFAT, HMGA2, LASP1.
- **Moritsu et al. 1997** (Anim. Sci. Agric. Hokkaido 39: 15–20, in Japanese) — Heritability estimates of age and body weight at first start in 3-year-old Japanese Thoroughbreds (h²=0.27±0.04); primary heritability source cited.
- **Cho et al. 2008** (J. Anim. Sci. Technol. Korea 50: 741–746) — Effects of body weight change on racing time in Thoroughbred racehorses; direct empirical link from Δ body weight to race speed.
- **Mosher et al. 2007** (PLoS Genet. 3: e79) — MSTN mutation increases muscle mass and enhances racing performance in heterozygous Greyhounds; cross-species validation of MSTN function in racing performance.
- **Onoda et al. 2011–2014** (multiple J. Equine Sci. and J. Anim. Sci. papers) — Empirical growth curve estimation for Japanese Thoroughbred body weight including seasonal compensatory growth models; relevant for age-standardised weight features.
