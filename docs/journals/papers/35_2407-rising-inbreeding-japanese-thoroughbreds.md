# Rising Trends of Inbreeding in Japanese Thoroughbred Horses

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. **35**(4): 57–61, 2024                                                                                                                                                                                                                                                                                                                                           |
| docid                          | `35_2407`                                                                                                                                                                                                                                                                                                                                                                       |
| Article type                   | Note                                                                                                                                                                                                                                                                                                                                                                            |
| Authors                        | Momoko WATANABE, Fumio SATO, Hideki INNAN                                                                                                                                                                                                                                                                                                                                       |
| Affiliations                   | 1 Research Center for Integrative Evolutionary Science, SOKENDAI (The Graduate University for Advanced Studies), Shonan Village, Hayama, Kanagawa 240-0193, Japan; 2 Graduate Institute for Advanced Studies, SOKENDAI, Shonan Village, Hayama, Kanagawa 240-0193, Japan; 3 Equine Department, Japan Racing Association, 1-1-1 Nishishimbashi, Minato-ku, Tokyo 105-0003, Japan |
| Received / Accepted / Released | 2024-05-21 / 2024-09-26 / —                                                                                                                                                                                                                                                                                                                                                     |
| Keywords                       | breeding, genetics, inbreeding coefficient, Thoroughbred                                                                                                                                                                                                                                                                                                                        |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/35/4/35_2407/_pdf/-char/en                                                                                                                                                                                                                                                                                                             |

## Abstract (verbatim)

> We investigated the changes in inbreeding levels in Japanese Thoroughbred horses over the past 46 years. Our results show a significant increase in inbreeding over the past 30 years, primarily due to the influence of two sires, Northern Dancer and Sunday Silence. Northern Dancer's bloodline spread gradually through descendants like Northern Taste, leading to a slower increase in the inbreeding coefficient. In contrast, Sunday Silence was directly imported and became a leading sire, causing a rapid increase in his blood proportion and inbreeding coefficient. Our findings suggest that monitoring the trajectories of successful sires and considering historical factors can help predict and control potential inbreeding depression in the future.

## Relevance to finishing-position (着順) prediction

**Feature family D (genetics/pedigree).** This study quantifies the pedigree structure of 346,744 Japanese Thoroughbreds (1978–2023), computing two metrics per individual: (1) a 5-generation inbreeding coefficient (Wright 1922) and (2) per-sire blood proportions for 16 key ancestors. Both are directly computable from JBBA pedigree data already available in the modelling pipeline. Specific implications:

1. **Inbreeding coefficient as a performance prior.** The Wright inbreeding coefficient (probability of homozygosity from common ancestors) has risen significantly since 1990, now averaging meaningfully above zero for most registered horses. Inbreeding depression in mammals reduces reproductive fitness and performance capability proportionally to inbreeding load; the effect in Thoroughbreds is contested but the metric is computable and temporally varying. A horse born in 2022 has a much higher expected inbreeding coefficient than one born in 1988 — failure to account for this temporal trend creates apparent cohort-level non-stationarity in pedigree features.

2. **Sire blood proportion as a distance/style prior.** Concentrated Sunday Silence blood (homozygous or close-cross descendants) correlates with the "stamina over speed" phenotype (Sunday Silence was a classic-distance/route horse). Deep Impact blood proportions are currently rising and are expected to follow the same trajectory as Sunday Silence. Northern Dancer blood favours brilliance at shorter distances. Computing the 5-generation blood proportion of the top 16 sires per individual horse produces a quantitative ancestry-concentration vector that is more informative than a simple sire/dam ID dummy.

3. **Temporal non-stationarity warning.** The paper documents a monotonically rising inbreeding coefficient since 1990. Any pedigree-based features must be modelled with this trend in mind — e.g., year-of-birth detrending, or cohort-relative normalization of inbreeding coefficient.

## Background & objective

Thoroughbreds are a closed, selectively bred population with limited founder diversity. Inbreeding is used deliberately to concentrate favourable alleles, but excessive homozygosity can expose deleterious recessive alleles (inbreeding depression). Japan is a unique case because its two dominant 20th–21st century sires — Northern Dancer (imported via descendants) and Sunday Silence (directly imported 1990) — entered the gene pool through different pathways, producing qualitatively different inbreeding trajectories. The authors aimed to: (a) track inbreeding coefficient and blood proportion over 46 years for 346,744 horses, (b) decompose the inbreeding coefficient by sire lineage origin for 16 top ancestors, and (c) predict future inbreeding trajectories for emerging sires such as Deep Impact.

## Materials & methods

**Subjects:** 395,499 total Thoroughbreds with pedigree data from JAIRS (Japan Association for International Racing and Stud Book) and JBBA (Japan Bloodhorse Breeders' Association). Filtered to horses born 1978–2023 with at least 5-generation pedigrees traceable: n = 346,744 (average 7,537.91 per year; ≥ 94.2% coverage of all registered horses since 1978; pre-1978 coverage was lower, mean 73.9% before 1977, so excluded).

**Blood proportion computation:** Relative genetic contribution of a sire to focal individual summed over 5 generations. Parents: 50%, grandparents: 25%, great-grandparents: 12.5%, etc. For sires appearing multiple times in the pedigree, blood proportions are summed. Example from paper: in Do Deuce's pedigree, Sunday Silence blood proportion = 25%; Hail to Reason = 6.25% + 3.125% = 9.375% (appears twice).

**Inbreeding coefficient computation:** Wright (1922) formula — probability that a gene in a focal individual is homozygous due to inheritance from a common ancestor. For each common ancestor in the 5-generation pedigree, contribution = 1/2^(n_paternal + n_maternal + 1), where n = number of generation steps. Inbreeding coefficient = sum of all such contributions. Example: Do Deuce has inbreeding coefficient 0.01172 from Hail to Reason (1/2^8 = 0.00391) and Lyphard (1/2^7 = 0.00781). Also decomposed by parent-of-origin (paternal vs. maternal lineage) for the 16 selected sires.

**Top 16 sires identified:** ranked by blood proportion pooled across all years. Named in Table 1: Nearco, Nasrullah, Hyperion, Pharos, Princely Gift, Northern Dancer, Nearctic, Nijinsky, Native Dancer, Northern Taste, Sunday Silence, Halo, Mr. Prospector, Hail to Reason, King Kamehameha, Deep Impact.

**Cross-type analysis (Figure 3):** For Northern Dancer and Sunday Silence separately, counted the proportion of horses with 2×3, 2×4, 3×3, 3×4, 4×4, 4×5 crosses (notation: n × m means the ancestor appears n generations back on one line and m on the other). Plotted over the first 14-year period starting from 18 years after the birth year of the first crop for each sire.

## Results (detailed — reproduce ALL numbers)

**Annual sire blood proportion rankings (Table 1 — complete, 1978–2023):**

| Year     | 1st                | %                | 2nd             | %    | 3rd             | %              | 4th             | %    | 5th             | %    |
| -------- | ------------------ | ---------------- | --------------- | ---- | --------------- | -------------- | --------------- | ---- | --------------- | ---- |
| 1978     | Nearco             | 5.66             | Nasrullah       | 4.84 | Hyperion        | 3.44           | Pharos          | 3.14 | Princely Gift   | 2.57 |
| 1983     | Nearco             | 6.00             | Nasrullah       | 5.47 | Hyperion        | 3.47           | Princely Gift   | 3.01 | Northern Dancer | 2.50 |
| 1986     | Nasrullah          | 5.62             | Nearco          | 5.51 | Northern Dancer | 3.84           | Hyperion        | 3.15 | Princely Gift   | 3.02 |
| 1990     | Northern Dancer    | 5.34             | Nasrullah       | 5.12 | Nearco          | 4.11           | Nearctic        | 2.93 | Nijinsky        | 2.70 |
| 1993     | Northern Dancer    | 6.80             | Nasrullah       | 4.92 | Nearctic        | 3.69           | Nearco          | 3.46 | Nijinsky        | 2.97 |
| 1997     | Northern Dancer    | 8.52             | Nearctic        | 4.35 | Nasrullah       | 4.15           | Nijinsky        | 3.60 | Native Dancer   | 3.13 |
| 2000     | Northern Dancer    | 8.76             | Nearctic        | 4.28 | Nijinsky        | 3.91           | Sunday Silence  | 3.69 | Mr. Prospector  | 3.65 |
| 2003     | Northern Dancer    | 9.28             | Sunday Silence  | 6.70 | Halo            | 4.21           | Nearctic        | 4.19 | Mr. Prospector  | 3.89 |
| 2006     | Sunday Silence     | 8.76             | Northern Dancer | 8.70 | Mr. Prospector  | 5.35           | Halo            | 5.26 | Hail to Reason  | 4.25 |
| 2007     | Sunday Silence     | 10.14            | Northern Dancer | 9.08 | Halo            | 5.98           | Mr. Prospector  | 5.34 | Hail to Reason  | 4.58 |
| 2008     | Sunday Silence     | 11.62            | Northern Dancer | 8.71 | Halo            | 6.78           | Mr. Prospector  | 5.07 | Hail to Reason  | 5.01 |
| 2009     | Sunday Silence     | 12.47            | Northern Dancer | 8.48 | Halo            | 7.36           | Mr. Prospector  | 5.71 | Hail to Reason  | 5.12 |
| 2011     | Sunday Silence     | 13.40            | Northern Dancer | 8.21 | Halo            | 7.80           | Mr. Prospector  | 5.80 | Hail to Reason  | 5.15 |
| **2013** | **Sunday Silence** | **13.50** (peak) | Halo            | 8.02 | Northern Dancer | 7.72           | Mr. Prospector  | 5.69 | Hail to Reason  | 5.04 |
| 2018     | Sunday Silence     | 12.92            | Halo            | 7.29 | Northern Dancer | 5.79           | Mr. Prospector  | 5.66 | King Kamehameha | 5.41 |
| 2020     | Sunday Silence     | 12.52            | Halo            | 7.12 | Mr. Prospector  | 5.85           | Northern Dancer | 5.01 | Deep Impact     | 4.77 |
| 2021     | Sunday Silence     | 12.07            | Halo            | 6.91 | Mr. Prospector  | 5.91           | Deep Impact     | 5.42 | Northern Dancer | 4.75 |
| 2022     | Sunday Silence     | 11.74            | Halo            | 6.66 | Mr. Prospector  | 5.75           | Deep Impact     | 5.65 | Northern Dancer | 4.52 |
| **2023** | Sunday Silence     | 11.57            | Halo            | 6.53 | **Deep Impact** | **6.41** (3rd) | Mr. Prospector  | 5.59 | King Kamehameha | 4.27 |

**Key inbreeding coefficient milestones (from Figure 2A):**

- Northern Dancer inbreeding coefficient: peaked ~2003 at **0.00365** (yellow triangle in Figure 2A)
- Sunday Silence blood proportion: peaked at **13.5%** in 2013 (red triangle in Figure 2B); inbreeding coefficient peak occurred **several years after** blood proportion peak
- Overall average inbreeding coefficient: significant increase since 1990 (black arrow in Figure 2A), driven first by Northern Dancer then Sunday Silence

**Cross-type trends (Figure 3):**

- **Sunday Silence:** 3×3 crosses increasing sharply (both grandsire and great-grandsire); 3×4 crosses also rising rapidly
- **Northern Dancer:** 3×3 crosses less prevalent; 3×4 crosses increasing only gradually; 4×4 crosses historically dominant
- The difference is attributed to Northern Dancer's indirect entry (via descendants) vs. Sunday Silence's direct importation

**Predictive projection:** Deep Impact shows rising blood proportion (Figure 2B) but still low inbreeding coefficient as of 2023 — expected to follow Sunday Silence's trajectory, with inbreeding coefficient peak predicted ~5–10 years after blood proportion peak.

## Discussion & interpretation

The key mechanistic contrast is: Northern Dancer entered Japan only through offspring (Northern Taste etc.), so his alleles arrived dispersed and mixed — blood proportion rose slowly, and when 3×4 or 4×4 crosses eventually appeared, the inbreeding coefficient rose slowly from moderate starting levels. Sunday Silence arrived as a live sire, immediately dominated the covering lists (leading sire 1995–2007), and his sons became the next generation of top sires (Deep Impact, Stay Gold, etc.) — creating 2×3, 3×3, and 3×4 crosses within a single generation's timeframe.

Inbreeding depression (reduced fitness from homozygosity of deleterious alleles) is theoretically increasing in the Japanese Thoroughbred population. However, countervailing selection pressure (artificial selection for racing performance) may buffer the effect. The authors do not quantify the performance effect; they focus on the genetic epidemiology.

The paper notes that monitoring a sire's blood proportion trajectory is a leading indicator for his eventual inbreeding coefficient peak (the inbreeding coefficient lags the blood proportion peak by several years as close-cross matings accumulate). This has practical implications for breeding management.

## Limitations

- Inbreeding coefficient computed from **5-generation pedigrees only**; deeper pedigrees would give more accurate estimates — the Thoroughbred breed itself has ancient common founders (Byerley Turk, Darley Arabian, Godolphin Arabian) that contribute a baseline inbreeding level not captured in 5 generations
- Analysis is **population-level** (average annual inbreeding coefficient per birth cohort); individual horse inbreeding coefficients are used only to compute the annual average, not directly linked to individual performance
- The paper does **not** analyse the performance effect of inbreeding; it only characterises the genetic epidemiology — performance association must be inferred from external literature (Doekes et al. 2019 in dairy cattle; equine-specific studies needed)
- Data from JAIRS/JBBA; foreign-bred horses racing in Japan may have pedigrees not fully captured
- Blood proportion based on sire lineage only (not dam lineage), as is conventional in the horse breeding community, which is not a full genomic measure

## Feature-engineering notes for the model

- `inbreeding_coeff_5gen` — Wright (1922) inbreeding coefficient over 5-generation pedigree — computable from JBBA pedigree data per individual horse — expected weak negative effect on performance (inbreeding depression); temporal trend upward since 1990 means older cohorts have lower baseline than newer; include year_of_birth as a covariate when using this feature
- `blood_prop_sunday_silence` — 5-generation blood proportion of Sunday Silence (paternal lineage sum, summing duplicates) — from JBBA pedigree — proxy for stamina/classic-distance genetic endowment; Sunday Silence horses tend to route horses (1,600–2,400 m); interaction with `distance_m` expected
- `blood_prop_northern_dancer` — blood proportion of Northern Dancer — sprint/speed allele concentration; Northern Dancer horses historically favoured at shorter distances; interaction with `distance_m` (negative at longer distances)
- `blood_prop_deep_impact` — blood proportion of Deep Impact — rapidly rising (3rd ranked by 2023 at 6.41%); Deep Impact horses known for strong late-run finishing style; potential interaction with `running_style` (差し/追い込み)
- `blood_prop_king_kamehameha` — King Kamehameha blood proportion — dirt-racing specialty (strong NAR dirt performance); interaction with `surface` (dirt vs. turf) expected
- `inbreeding_decomposed_sunday_silence` — Wright coefficient decomposed to Sunday Silence lineage contribution — captures specifically Sunday Silence inbreeding load; higher values in horses with 3×3 or 3×4 Sunday Silence crosses
- **Temporal detrending:** normalize inbreeding coefficient by subtracting the cohort mean for the horse's birth year (from Figure 2A of this paper) to remove the secular trend; otherwise, birth-year acts as a confounder
- **Do NOT use** a single sire/dam ID dummy as the only pedigree feature: blood proportion captures the continuous contribution of an ancestor across positions in the pedigree, which is more informative than a single generation's sire ID

## Key references / follow-up leads

- **Wright S. 1922** — "Coefficients of inbreeding and relationship," Am. Nat. 56:330–338 — foundational formula for inbreeding coefficient used in this study
- **Cunningham E.P. et al. 2001** — Anim. Genet. 32:360–364 — microsatellite diversity and founder lineage contributions in Thoroughbreds; genomic diversity baseline
- **Gu J. et al. 2009** — PLoS One 4:e5767 — "A genome scan for positive selection in thoroughbred horses"; speed/stamina alleles under selection, including MSTN region
- **Doekes H.P. et al. 2019** — Genet. Sel. Evol. 51:54 — inbreeding depression due to recent and ancient inbreeding in Dutch Holstein-Friesian dairy cattle; methodological reference for performance-inbreeding analysis
- **Charlesworth D. & Willis J.H. 2009** — Nat. Rev. Genet. 10:783–796 — genetics of inbreeding depression; theoretical framework
- **Bosse M. et al. 2018** — Evol. Appl. 12:6–17 — deleterious alleles in context of domestication, inbreeding, and selection; domesticated species framework
