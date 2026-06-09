# Gene Dropping Analysis of Ancestral Contributions and Allele Survival in Japanese Thoroughbred Population

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 21(3): 39–45, 2010                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| docid                          | `21_3_39`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Article type                   | Case Report (designated by journal; substantively an original population-genetics study)                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Authors                        | Jun Yamashita, Hironori Oki, Telhisa Hasegawa, Takeshi Honda, Tetsuro Nomura                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Affiliations                   | (1) Department of Biotechnology, Faculty of Engineering, Kyoto Sangyo University, Kita-ku, Kyoto 603-8555; (2) Equine Research Institute, Japan Racing Association, 321-4 Tokami-cho, Utsunomiya, Tochigi 320-0856; (3) Food Resources Education and Research Center, Graduate School of Agricultural Science, Kobe University, 1348 Uzurano-cho, Kasai, Hyogo 675-2103; (4) Department of Bioresource and Environmental Sciences, Faculty of Life Sciences, Kyoto Sangyo University, Kita-ku, Kyoto 603-8555 |
| Received / Accepted / Released | Accepted: June 3, 2010                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Keywords                       | allele survival, allelic diversity, gene dropping simulation, genetic contribution, Thoroughbred                                                                                                                                                                                                                                                                                                                                                                                                              |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/21/3/21_3_39/_pdf/-char/en                                                                                                                                                                                                                                                                                                                                                                                                                                           |

## Abstract (verbatim)

> Genetic contributions of nine historically important ancestors and allelic diversity in the Japanese Thoroughbred population were examined by applying the gene dropping simulation to the foals produced from 1978 to 2005. Full pedigree records traced to ancestors (base animals) born around 1890 were used for the simulation. Alleles originated from some of the historically important ancestors were found to be at risk of future extinction, although their genetic contributions to the foal population have increased during the last three decades. The proportion of surviving alleles to the total alleles assigned to the base animals was 8.0% in the foal population in 2005, suggesting that a large part of genetic variability contained in the base animals is extinct in the current population.

## Relevance to finishing-position (着順) prediction

Feature family: **D — genetics / pedigree**.

This study quantifies, via 10,000-replicate Monte Carlo gene-dropping simulation, the actual genome fraction contributed by nine historically important ancestors to the Japanese Thoroughbred foal cohorts of 1978 and 2005, and tracks allelic diversity from 946.8 surviving alleles (1978) down to 426.6 (2005) — just 8.0% of the total base-population alleles. These results provide empirical weights for continuous pedigree-based features:

**Actionable features for the prediction pipeline:**

1. For any individual horse, the pedigree-traced genetic contribution from each of the nine ancestors (Sunday Silence: 3.47%, Northern Dancer: 4.58%, Nearco: 3.51%, etc.) can be computed as a continuous feature. These are not merely lineage-identity indicators — they capture the expected proportion of the individual's genome that came through a specific performance-correlated ancestral line.
2. The allelic diversity decline after 1993 (coincident with the Sunday Silence-driven concentration documented in companion paper 21_2_11) provides a year-by-year population-level diversity index. A horse's foal cohort year can be matched to the corresponding allelic-diversity curve value (Fig. 2) to give a per-cohort inbreeding-pressure covariate.
3. Ancestors with increasing extinction probability (St. Simon: Pr(lost) 2005 = 0.0401; Royal Charger: 0.0119) are passing through narrow genealogical bottlenecks — their presence in a pedigree may indicate residual rarity value (heterosis candidate) or alternatively shallow/duplicated pedigree depth.

The link to finishing-position prediction is indirect but grounded: inbreeding depression is documented across livestock species, and the concentrated gene pool (8% diversity survival) implies that individual inbreeding coefficients (F) computed against the full pedigree will vary meaningfully across horses and may explain variance in performance consistency and physical robustness.

## Background & objective

All modern Thoroughbreds derive from a small founding population in 18th-century England. The current world population exceeds 300,000 but shares a limited gene pool traceable to ~1890-era base animals. Monitoring which ancestral alleles have survived vs. gone extinct matters for two reasons: (1) performance-linked alleles may have been swept to fixation or lost during selective breeding; (2) conservation of genetic diversity is needed to sustain long-term performance improvement. Gene dropping simulation (Lacy, 1989) — assigning unique alleles to base animals and propagating them through actual pedigrees via Mendelian segregation — is a Monte Carlo method that overcomes the intractability of exact allele-frequency computation in large, complex pedigrees. The study applies this method to the JRA pedigree dataset (same as companion paper 21_2_11) to characterise the Japanese Thoroughbred population's genetic constitution in terms of ancestor contributions and allelic diversity.

## Materials & methods

**Pedigree dataset:** All JRA foals born in Japan, 1978–2005 (annual foal counts identical to 21_2_11; same dataset). Full pedigree traced to base animals born ~1890. Average generations to base population: 5.64 (1978 foal cohort), 9.09 (2005 foal cohort).

**Target ancestors:** 9 stallions selected as historically important for Japanese Thoroughbred breeding (based on Yoshizawa, 2001):

| Stallion        | Birth year | Birth country |
| --------------- | ---------- | ------------- |
| St. Simon       | 1881       | England       |
| Tourbillon      | 1928       | France        |
| Hyperion        | 1930       | England       |
| Nearco          | 1935       | Italy         |
| Nasrullah       | 1940       | England       |
| Royal Charger   | 1942       | England       |
| Native Dancer   | 1950       | USA           |
| Northern Dancer | 1961       | Canada        |
| Sunday Silence  | 1986       | USA           |

**Gene dropping simulation — Arm 1 (ancestor contributions):**

- Unique alleles assigned to each of the 9 target stallions
- Monte Carlo simulation of Mendelian segregation through the full pedigree
- 10,000 replicates per focal ancestor per foal-cohort year
- Output: distribution of allele frequencies for each ancestor in 1978 and 2005 foal cohorts (Fig. 1)
- Mean of distribution = genetic contribution of each ancestor
- Pr(allele lost) = proportion of replicates in which both alleles from the ancestor were absent from the foal population = extinction probability

**Gene dropping simulation — Arm 2 (allelic diversity):**

- Unique alleles assigned to ALL base animals (not just the 9 focal ancestors)
- 10,000 replicates per foal-cohort year (every year 1978–2005)
- Total alleles in base population: 4,800–5,500 (varies by year as pedigree depth increases)
- Output: average number of surviving alleles in each year's foal cohort = allelic diversity

**Note:** For Sunday Silence (born 1986), only the 2005 foal population was analysed (he was not yet breeding by 1978).

## Results (detailed — all numbers reproduced)

**Genetic contributions and extinction probabilities (Table 2):**

| Stallion        | GC 1978 | GC 2005    | Pr(lost) 1978 | Pr(lost) 2005 |
| --------------- | ------- | ---------- | ------------- | ------------- |
| St. Simon       | 0.0143  | 0.0156     | 0.0173        | **0.0401**    |
| Tourbillon      | 0.0046  | 0.0051     | 0.0024        | **0.0110**    |
| Hyperion        | 0.0160  | 0.0240     | 0             | 0             |
| Nearco          | 0.0255  | 0.0351     | 0             | 0             |
| Nasrullah       | 0.0221  | 0.0279     | 0             | 0             |
| Royal Charger   | 0.0026  | 0.0081     | 0.0004        | **0.0119**    |
| Native Dancer   | 0.0048  | 0.0235     | 0.0012        | 0.0001        |
| Northern Dancer | 0.0010  | **0.0458** | 0.0315        | 0             |
| Sunday Silence  | —       | **0.0347** | —             | 0             |

Key observations:

- **Northern Dancer**: largest genetic contribution increase (0.001→0.046); Pr(lost) fell from 0.032 to 0 — now firmly established in the population
- **Sunday Silence**: 3.47% contribution by 2005; Pr(lost) = 0; confirmed as a secure lineage
- **St. Simon**: contribution slightly increased (1.4→1.6%) but **extinction probability increased** from 1.73% to 4.01% — lineage connected to current population only through narrow bottlenecks
- **Royal Charger**: contribution increased (0.26→0.81%) but extinction probability increased from 0.04% to 1.19% — similar bottleneck problem
- **Nearco, Nasrullah, Hyperion**: Pr(lost) = 0 in both years — secure lineages with multiple genealogical pathways to the current population
- **Native Dancer**: secondary peak at allele frequency ~0.03 in 2005 (alleles transmitted through Northern Dancer, his grandson); the "original" Native Dancer contribution (not via Northern Dancer) is negligibly small

**Distribution shape classification (Fig. 1):**

- Type 1 (peak near zero): St. Simon, Tourbillon, Royal Charger — most alleles already extinct or at critically low frequency; high extinction risk
- Type 2 (peak at moderate-to-high frequency, Pr(lost)≈0): Hyperion, Nasrullah, Nearco, Northern Dancer, Sunday Silence — multiple genealogical pathways ensure fixation at moderate frequency

**Allelic diversity (Fig. 2):**

- 1978 foal cohort: **946.8 surviving alleles** (average over 10,000 replicates)
- 2005 foal cohort: **426.6 surviving alleles**
- Proportion surviving in 2005: **8.0%** of total base-population alleles (4,800–5,500 total)
- Temporal pattern: initial decline 1978–1984, plateau through ~1993, then **progressive decline after 1993** — exactly coincident with the Sunday Silence/effective-sire-number collapse documented in 21_2_11
- The allelic diversity decline post-1993 is corroborated by microsatellite data (Kakoi et al., 2009)

## Discussion & interpretation

The paradox that some ancestors (St. Simon, Royal Charger) show increasing genetic contribution yet increasing extinction probability is explained by genealogical bottleneck geometry: their alleles still contribute to the average foal via a few high-contribution pathways, but the pedigree bottlenecks mean that in many replicate populations those alleles would be absent by chance. This is the classic distinction between mean contribution and allelic survival — a lineage can be "important on average" while having a non-negligible probability of complete loss in any given generation.

The Native Dancer vs. Northern Dancer result demonstrates that the gene dropping method can trace allele paths through specific genealogical intermediaries: most of Native Dancer's presence in 2005 Japanese Thoroughbreds comes through Northern Dancer (his grandson), not directly. This is a practical feature-engineering insight: "Native Dancer contribution" without conditioning on Northern Dancer is largely redundant with "Northern Dancer contribution" in post-2000 Japanese horses.

The overall finding — 8.0% allelic diversity survival in 2005 — means that only 1 in ~12 ancestral alleles survives in the current population. Combined with the Ne,a ≈ 500 result from 21_2_11, this provides the empirical parameters needed to set priors for any pedigree-inbreeding simulation used in a Bayesian horse-modelling framework.

## Limitations

- Gene dropping treats all base-population alleles as selectively neutral — it cannot account for alleles that were intentionally selected or lost due to selection pressure for performance.
- The base population (horses born ~1890) has incomplete pedigree records, meaning some allele diversity in the true founding population is not captured; the 8.0% figure may understate the true diversity loss.
- 9 target ancestors were chosen a priori based on historical importance (Yoshizawa, 2001); other potentially important ancestors are not characterised.
- No performance data linkage; the genetic-contribution features are population-level constructs, not validated as predictive of individual race performance.
- Data ends at 2005; the Sunday Silence era has since wound down as his sons and grandsons spread his alleles more evenly, potentially altering the concentration dynamics.

## Feature-engineering notes for the model

- `gc_sunday_silence` — estimated proportion of genome from Sunday Silence in the target horse, computed by extending the gene dropping mean contributions through the pedigree; 2005 mean = 3.47%, range will vary individually — source: JBBA pedigree database; derivation requires multi-generation pedigree tracing
- `gc_northern_dancer` — analogous for Northern Dancer; 2005 mean = 4.58% — note: strongly correlated with gc_sunday_silence in post-2000 cohorts (SS sires often have Northern Dancer dams)
- `gc_nearco` / `gc_nasrullah` — Pr(lost)=0 ancestors with stable contributions; 2005 values 3.51% and 2.79% respectively — these represent secure, widely-distributed lineages
- `pedigree_diversity_cohort_year` — allelic diversity value for the horse's foal cohort year (from Fig. 2); range 427–947 over 1978–2005 — captures population-level inbreeding pressure; lower values indicate more inbred cohort era — source: look up by foal year from paper's Fig. 2
- `extinction_risk_lineage_count` — count of target ancestors with Pr(lost)>0.01 in the horse's pedigree — proxy for "narrow-bottleneck lineage depth"; higher values indicate pedigree paths through St. Simon/Tourbillon/Royal Charger bottlenecks — expected small positive effect on inbreeding load
- `native_dancer_via_northern_dancer_ratio` — fraction of Native Dancer contribution that flows through Northern Dancer — captures whether the horse's Native Dancer alleles come through broad (multiple pathway) or narrow (ND-only) channels; ratio near 1.0 = the allele is concentrated through ND pathway only
- **CAUTION:** gc_sunday_silence and gc_northern_dancer are highly collinear in post-1993 cohorts; include only one or use PCA/orthogonalisation — do not include both naively.

## Key references / follow-up leads

- Yamashita et al. (2010) J. Equine Sci. 21: 11–16 — companion demographic paper (docid 21_2_11): effective sire numbers, Ne,a, and migration rates for the same 1978–2005 JRA population
- Kakoi et al. (2009) Equine Sci. 46: 36–48 — microsatellite DNA analysis validating the allelic diversity decline in recent Japanese Thoroughbred; molecular corroboration of Fig. 2 decline
- Lacy (1989) Zoo Biol. 8: 111–123 — gene dropping simulation method; founder equivalents and founder genome equivalents concept
- Yoshizawa (2001) Pedigree of Racing Horses (NHK Press, Tokyo) — source for selection of the 9 historically important ancestor stallions
- Cunningham et al. (2001) Anim. Genet. 32: 360–364 — microsatellite diversity and pedigree relatedness in Thoroughbreds; founder lineage contributions
- Honda et al. (2002) Anim. Sci. J. 73: 105–111 — gene dropping analysis in Japanese Black cattle (methodological comparison)
- Rodriganez et al. (1998) Anim. Sci. 67: 573–582 — founder allele survival and inbreeding depression on litter size in closed pig line; inbreeding–performance link via allele survival
