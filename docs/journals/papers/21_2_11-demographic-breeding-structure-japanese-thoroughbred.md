# Demographic Analysis of Breeding Structure in Japanese Thoroughbred Population

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 21(2): 11–16, 2010                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| docid                          | `21_2_11`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Article type                   | Original Article                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Authors                        | Jun Yamashita, Hironori Oki, Telhisa Hasegawa, Takeshi Honda, Tetsuro Nomura                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Affiliations                   | (1) Department of Biotechnology, Faculty of Engineering, Kyoto Sangyo University, Kita-ku, Kyoto 603-8555; (2) Equine Research Institute, Japan Racing Association, 321-4 Tokami-cho, Utsunomiya, Tochigi 320-0856; (3) Food Resources Education and Research Center, Graduate School of Agricultural Science, Kobe University, 1348 Uzurano-cho, Kasai, Hyogo 675-2103; (4) Department of Bioresource and Environmental Sciences, Faculty of Life Sciences, Kyoto Sangyo University, Kita-ku, Kyoto 603-8555 |
| Received / Accepted / Released | Accepted: April 14, 2010                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Keywords                       | breeding structure, demographic analysis, effective population size, Thoroughbred                                                                                                                                                                                                                                                                                                                                                                                                                             |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/21/2/21_2_11/_pdf/-char/en                                                                                                                                                                                                                                                                                                                                                                                                                                           |

## Abstract (verbatim)

> To investigate the breeding structure in the Japanese Thoroughbred population, we applied a demographic analysis to the populations of foals produced from 1978 to 2005. The migration rate estimated from the proportion of foals produced by imported breeding horses was around 40% over the investigated period. After early 1990s, the migration rate through stallions imported from USA sharply increased. The average generation interval was within range of 10.5–11.5 years. The longer generation interval of Thoroughbred was considered to be a reflection of the fact that Thoroughbred horses begin breeding only after completing their performance in races. After the peak of 729 in 1993, the number of sires of foals progressively declined to 358 in 2005. Although the coefficient of variation of the progeny number of sires was within range of 1.0–1.2 until early 1990s, it gradually increased and reached the value of 1.6–1.7 in recent years. The effective number of sires consistently decreased after the peak of 302.6 in 1992, and reached 120–130 in recent years, which is 25–30% of the actual number of sires. In parallel, the demographic estimate of the effective population size declined after early 1990s. The main cause of the observed change in the breeding structure was inferred to be the intensive use of a limited number of stallions for breeding.

## Relevance to finishing-position (着順) prediction

Feature family: **D — genetics / pedigree**.

This paper provides quantitative demographic baselines for the Japanese Thoroughbred gene pool from 1978–2005, establishing that: (1) the effective number of sires collapsed to 120–130 by 2005 (25–30% of census sires); (2) the annual effective population size (Ne,a) declined from ~1,000 to ~500 after 1993; and (3) a handful of US-import stallions — most notably Sunday Silence (imported 1991) — dominate the post-1993 genetic landscape via extreme progeny-number inequality (CV of progeny numbers per sire reached 1.6–1.7 by 2005, up from 1.0–1.2 before 1990).

For a prediction pipeline, these findings justify three pedigree-feature families:

**Lineage-concentration features:** The Sunday Silence effect (dominant US male line post-1993) and the concurrent skewing of Nm,e/Nm to 25–30% together mean that individual inbreeding coefficients (F) and kinship-to-population coefficients are strongly influenced by a small number of stallions. Any pedigree feature proportional to Sunday Silence genome fraction (or any top-5 sire's genome fraction) will capture a real population-level genetic signal. Expected effect on finishing position: complex — lineage dominance is correlated with both selection for performance (positive) and inbreeding depression (negative in extreme cases).

**Generation-interval prior:** ~11 years (10.5–11.5 range, consistent with English, French, Brazilian Thoroughbreds). When computing sire performance decay features (e.g., "sire's peak speed rating weighted by time since racing"), an ~11-year half-life is the appropriate decay constant.

**Temporal cohort effects:** The post-1993 gene-pool concentration implies that race-cohort year is a confounding demographic variable — horses foaled 1993+ share a narrower gene pool than those foaled before, potentially contributing to performance plateau (Cunningham, 1991 showed English classic race times plateaued). Including a `foal_year_era` indicator (pre-1993 / post-1993) or the annual Ne,a estimate as a population-level covariate can partial out this demographic shift.

## Background & objective

The Thoroughbred breed has been closed to outside genetics since the late 18th century, making continuing genetic diversity monitoring essential. The Japanese Thoroughbred industry experienced a significant structural shift in the 1990s driven by the horse-racing boom: massive importation of high-performance US stallions (particularly Sunday Silence, who won the 1989 Kentucky Derby and Preakness) led to intense concentration of breeding on a small number of imported sires. The authors aim to quantify this structural change using standard population-genetic demographic metrics: migration rate, generation interval, effective number of sires (Nm,e), and effective population size (Ne,a). Results are compared with the Japanese Black cattle population as a structural contrast (artificial insemination vs. natural service).

## Materials & methods

**Data:** Registry and pedigree records of all JRA foals born in Japan, 1978–2005, supplied by the Equine Research Institute (JRA). Total foals: ~232,247 (117,287 males + 114,960 females + 6 geldings; see Table 1 for annual breakdown).

**Migration rate:** Male migration rate (mm) = proportion of foals sired by imported stallions per year. Female migration rate (mf) = proportion of foals born to imported mares. Total migration rate m = (mm + mf)/2. Computed separately by birth country of imported parent (USA, Europe, Australia, etc.).

**Generation interval:** Four gametic pathways: sire→son (Lmm), sire→daughter (Lmf), dam→son (Lfm), dam→daughter (Lff). Computed from birth dates of foals and their sires/dams in each year. Average L = (Lmm + Lmf + Lfm + Lff)/4.

**Effective number of sires:** Nomura (2002) CV-based formula:

- Nm,e = Nm / (1 + CV²)
- where Nm = census number of sires per year, CV = coefficient of variation of progeny number per sire
- Nm,e equals Nm only when all sires contribute equally (CV=0); when CV>0, Nm,e < Nm.

**Effective population size:** Demographic estimate of annual Ne,a derived from Nm,e following Nomura (2002):

- Ne,a ≈ 4 Nm,e · Nf / (Nm,e + Nf) (effective size assuming unequal male contribution and equal female contribution)
- Values from the paper follow this formula; exact expression not given in the text explicitly but results align with this standard formula.

**Significance:** No statistical tests reported; this is a demographic descriptive analysis. All metrics are computed population-wide per year.

## Results (detailed — all numbers reproduced)

**Migration rate (Fig. 1–3):**

- Total migration rate: ~40% throughout 1978–2005 (stable over the period)
- Male migration rate (mm): 50–70% in the period 1980–1990 with a period of decline in 1978–1990
- Female migration rate (mf): consistently lower than mm; gradual increase after 1990
- US stallion contribution (Fig. 2): **sharp increase 1990–1997** driven by Sunday Silence importation in 1991 and subsequent success of his progeny in graded races. By mid-1990s, USA was the dominant source of male migration.

**Generation interval (Fig. 4):**

- Lmm and Lmf (sire pathways): ~10–11.5 years, no consistent trend
- Lfm and Lff (dam pathways): slight upward trend over the period
- Average L: 10.5–11.5 years throughout 1978–2005
- Comparable estimates: England ~11 years (Gaffney & Cunningham, 1988); France 10.6 years (Moureaux et al., 1996); Brazil 10.7 years (Taveira et al., 2004)

**Effective number of sires (Table 2, full data 1978–2005):**

| Year | Nm      | CV    | Nm,e      | Nm,e/Nm |
| ---- | ------- | ----- | --------- | ------- |
| 1978 | 459     | 0.989 | 232.0     | 0.505   |
| 1982 | 431     | 0.942 | 228.3     | 0.530   |
| 1985 | 430     | 1.007 | 213.5     | 0.496   |
| 1990 | 639     | 1.133 | 279.8     | 0.438   |
| 1991 | 680     | 1.165 | 288.4     | 0.424   |
| 1992 | 711     | 1.162 | **302.6** | 0.426   |
| 1993 | **729** | 1.216 | 294.2     | 0.404   |
| 1994 | 714     | 1.303 | 264.6     | 0.371   |
| 1995 | 695     | 1.435 | 227.1     | 0.327   |
| 1996 | 615     | 1.473 | 194.0     | 0.315   |
| 1997 | 560     | 1.532 | 167.3     | 0.299   |
| 2000 | 488     | 1.632 | 133.2     | 0.273   |
| 2001 | 472     | 1.633 | 128.7     | 0.273   |
| 2002 | 477     | 1.642 | 129.0     | 0.271   |
| 2004 | 466     | 1.680 | 121.9     | 0.262   |
| 2005 | 358     | 1.501 | 110.0     | 0.307   |

Key inflection: Nm,e peaked at **302.6 in 1992**, then fell monotonically to **~120–130 by 2001–2004** (25–30% of census Nm). Nm peaked at 729 in 1993 but CV simultaneously rose from 1.2 to 1.6–1.7. After 2000, sires with >100 foals/year numbered 15+; the most intensively used sire (Sunday Silence, 1996–2002) exceeded 160 foals per year.

**Most intensively used sire (Fig. 5):**

- Average foals per sire (k̄m): ~20 throughout the period (stable)
- Maximum foals for any single sire (km,max): **sharp increase after 1993**; post-2000 values exceed 160 foals/year
- Sunday Silence was the dominant sire 1996–2002

**Annual effective population size (Fig. 6):**

- Ne,a peaked over **1,000 in ~1992** (coincident with Nm,e peak)
- Declined to ~**500 by post-2000** — a 50% reduction
- Still ~10× larger than the Japanese Black cattle Ne,a (which fell below 20 by 2000 due to AI enabling extreme concentration on a few high-EBV bulls)

**Comparison with Japanese Black cattle (Table 3):**

| Year | JTB Nm | JTB CV | JTB Nm,e | JTB Nm,e/Nm |
| ---- | ------ | ------ | -------- | ----------- |
| 1985 | 1,004  | 3.312  | 83.9     | 0.084       |
| 1991 | 981    | 3.841  | 62.3     | 0.063       |
| 2000 | 732    | 7.933  | **11.4** | 0.016       |
| 2004 | 752    | 6.917  | 15.4     | 0.020       |

Japanese Black cattle CV exceeded 7.9 by 2000 vs. Thoroughbred CV of only 1.6–1.7 — natural service limits is the key constraint preventing the same extreme concentration in Thoroughbreds.

## Discussion & interpretation

The authors conclude that the breeding structure of the Japanese Thoroughbred population changed substantially after early 1990s, driven primarily by the success of Sunday Silence and ~15 other high-performing sires post-2000. However, the magnitude of change is much smaller than in Japanese Black cattle, because natural service restricts any single stallion to <200 foals/year in horses vs. >10,000/year in AI cattle. The declining Nm,e and Ne,a post-1993 indicate reduced genetic diversity — consistent with Cunningham's (1991) observation of plateauing English race performance times (possibly due to exhausted additive genetic variance for speed in a closed breed). The paper is explicitly a demographic baseline for subsequent pedigree-level diversity analysis (companion paper: gene dropping simulation, 21_3_39, same volume, same authors).

## Limitations

- The analysis is descriptive demographic analysis; no causal inference between population structure and racing performance is established.
- Migration rate computation treats all imported horses equally regardless of genomic contribution depth; some high-Nm imported stallions dominate more than their migration-rate fraction suggests.
- The comparison with Japanese Black cattle illuminates structural differences but the cattle data do not directly inform Thoroughbred feature engineering.
- Data ends at 2005; subsequent shifts (post-Sunday Silence era, introduction of Northern Dancer-line alternatives) are not captured.
- Effective population size formula assumes standard demographic derivation; actual Ne under selection for performance may differ from neutral demographic Ne.

## Feature-engineering notes for the model

- `sire_progeny_count_cohort_year` — number of foals sired by the horse's sire in the horse's foal year — captures lineage concentration; high values indicate a dominant-sire colt (Sunday Silence offspring in 1996–2002 had the highest values) — source: JBBA stud book or JRA pedigree data; expected interaction with `foal_year` (post-1993 high-concentration era)
- `sire_cv_cohort_year` — CV of progeny numbers for active sires in the horse's foal year — population-level inbreeding pressure index; higher CV → more concentrated gene pool → higher expected inbreeding coefficient for cohort — source: derived from annual foal records
- `generation_interval_sire` — age of sire at the time of foaling (= sire's age when this foal was born) — proxy for generational lag and sire-performance recency; JRA Thoroughbred mean ~11 years — source: sire birth year + foal birth year
- `foal_year_era` — categorical: pre-1990 (diverse pool), 1990–1993 (transition/peak import boom), 1994–2005 (Sunday Silence concentration), 2006+ (post-SS diversification) — captures the demographic regime shift — source: foal year from race registration
- `effective_sire_ratio_cohort` — Nm,e/Nm for the horse's foal year (from Table 2) — direct population-genetic inbreeding density indicator; values below 0.30 (post-1998) indicate high concentration era — source: look up Table 2 values by foal year
- **DO NOT** use raw CV or Nm as features without normalisation; they confound the absolute number of sires (Nm fell from 729 to 358 over the period) with the distribution-skew effect (CV rose).

## Key references / follow-up leads

- Yamashita et al. (2010) J. Equine Sci. 21: 39–45 — gene dropping companion paper (docid 21_3_39) with allelic diversity analysis of the same population
- Cunningham (1991) Sci. Amer. 264: 56–62 — plateauing winning times in English classic races; genetic-diversity argument
- Cunningham et al. (2001) Anim. Genet. 32: 360–364 — microsatellite diversity and founder lineage contributions in Thoroughbreds
- Nomura (2002) J. Anim. Breed. Genet. 118: 297–310 — effective population size formula with unequal sex ratio and variation in mating success (the Nm,e formula used here)
- Gaffney & Cunningham (1988) Nature 332: 722–723 — estimation of genetic trend in Thoroughbred racing performance; generation interval in England ~11 years
- Moureaux et al. (1996) Genet. Sel. Evol. 28: 83–102 — genetic variability within French race and riding horses; French Thoroughbred generation interval 10.6 years
- Taveira et al. (2004) J. Anim. Breed. Genet. 121: 384–391 — population parameters in Brazilian Thoroughbred; generation interval 10.7 years
- Kakoi et al. (2009) Equine Sci. 46: 36–48 — microsatellite DNA analysis of genetic diversity in recent Japanese Thoroughbred; molecular corroboration of the diversity decline
