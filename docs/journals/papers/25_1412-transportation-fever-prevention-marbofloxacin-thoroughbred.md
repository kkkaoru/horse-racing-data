# Comparison of the Occurrence of Transportation-associated Fever in 2 Years Old Thoroughbreds before and after Introduction of Prophylactic Marbofloxacin Administration

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 25(4): 79–81, 2014                                                                                                                                                                                                                                                                                                              |
| docid                          | `25_1412`                                                                                                                                                                                                                                                                                                                                      |
| Article type                   | Note                                                                                                                                                                                                                                                                                                                                           |
| Authors                        | Yoshiro Endo, Takeru Tsuchiya, Kentaro Akiyama, Naoya Takebe, Kenji Nakai, Kenji Korosue, Mutsuki Ishimaru, Nao Tsuzuki, Seiji Hobo                                                                                                                                                                                                            |
| Affiliations                   | (1) Hidaka Training and Research Center, Japan Racing Association, Hokkaido 057-0171, Japan; (2) Laboratory of Veterinary Surgery, Faculty of Agriculture, University of Miyazaki, Miyazaki 880-0036, Japan; (3) Joint Faculty of Veterinary Medicine, Kagoshima University, Kagoshima 890-0065, Japan. Endo and Tsuchiya contributed equally. |
| Received / Accepted / Released | Received: September 29, 2014 / Accepted: November 7, 2014                                                                                                                                                                                                                                                                                      |
| Keywords                       | horse, marbofloxacin, prevention, transportation-associated fever                                                                                                                                                                                                                                                                              |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/25/4/25_1412/_pdf/-char/en                                                                                                                                                                                                                                                                            |

## Abstract (verbatim)

> In order to reveal the preventive effect of marbofloxacin (MRFX) administration just before transportation, we compared the occurrence of transportation-associated fever before and after introduction of MRFX administration. After the introduction of prophylactic MRFX administration, the rectal temperatures of horses after transportation were significantly lower than before the introduction of MRFX administration (P<0.01) and the number of febrile horses was significantly lower than before the introduction of MRFX administration (P<0.01). In conclusion, these results show that prophylactic MRFX administration just before transportation is clinically effective at preventing transportation-associated fever.

## Relevance to finishing-position (着順) prediction

Feature family: **A — injury/soundness / health disruption**.

Transportation-associated fever (TAF) is a concrete health disruption event affecting 2-year-old Thoroughbreds at their first long-haul transport from breeding farms to racecourses. The paper quantifies the baseline incidence of TAF (19/99 horses = 19.2% febrile rate in the no-antibiotic era) and shows that prophylactic marbofloxacin (MRFX) cut this to 7/112 (6.3%) from 2012 onward. For a prediction pipeline, this is relevant in two ways. First, "long-haul transport event (>20 hr) within N days before debut race" is a candidate feature for young-horse scratch risk and below-form debut performance, primarily for horses originating from Hokkaido farms and entering Kanto/Kansai racecourses. Second, there is a temporal confound: pre-2012 debut horses faced a ~19% TAF incidence while post-2012 horses faced ~6%, so a transport-distance or transport-duration feature needs a post-2012 interaction term.

Specific JRA race-record derivation: Hokkaido origin is recorded in horse registration data; debut racecourse is in race entry records; debut date minus birth date gives an approximate earliest feasible transport date. Transport distance can be approximated by origin prefecture and first race venue (Hidaka→Hanshin ≈ 1,540 km, Hidaka→Nakayama ≈ 1,210 km). Expected sign: long-haul transport in the 2 weeks before debut → lower debut finishing position. The effect is concentrated in 2-year-olds at debut; it is unlikely to persist to later career races. Interaction with debut year (pre/post 2012) and with a "health clearance" proxy (absence of scratch within 3 days of transport) should be included.

## Background & objective

It has been established that the risk of fever increases markedly when Thoroughbred racehorses are transported by vehicle for more than 20 hours (Oikawa & Kusunose, 1995; Oikawa et al., 1995). TAF is caused primarily by infection of the bronchoalveolar regions with _Streptococcus equi_ subsp. _zooepidemicus_, a commensal resident in tonsillar tissues and trachea of healthy horses. Once TAF occurs, multiple IV antibiotic administrations (e.g., cephalothin sodium) are required; severe cases require bronchoalveolar lavage or thoracic drainage, and death may result from delayed treatment. Previous work showed that enrofloxacin (ERFX) and MRFX pre-transport reduced serum amyloid A (an inflammation marker) but did not significantly reduce rectal temperature in small controlled trials (Tsuchiya et al., 2012; Endo et al., 2014). The present study aimed to verify clinical efficacy of MRFX prophylaxis by comparing TAF incidence across four transport cohorts spanning 2007–2013.

## Materials & methods

**Subjects:** 211 healthy 2-year-old Thoroughbreds (106 males, 105 females) transported from Hidaka Training and Research Center, Hokkaido, to either Hanshin Racecourse, Hyogo (1,540 km, 36 hr) or Nakayama Racecourse, Chiba (1,210 km, 26 hr).

**Groups:**
| Group | Year | Route | n (M/F) | MRFX |
|-------|------|-------|---------|------|
| Control 1 | 2007 | Hokkaido→Hyogo | 50 (24/26) | None |
| Control 2 | 2008 | Hokkaido→Chiba | 49 (25/24) | None |
| MRFX 1 | 2012 | Hokkaido→Chiba | 56 (27/29) | 2 mg/kg IV pre-transport |
| MRFX 2 | 2013 | Hokkaido→Chiba | 56 (30/26) | 2 mg/kg IV pre-transport |

In 2008, 2012 and 2013 the same Hokkaido→Chiba route was used with timing adjusted so that all cohorts arrived at the same hour; the 2007 Hanshin cohort (longer route) was treated as equivalent because rectal temperature after transport did not significantly differ between the 36 hr and 26 hr cohorts in preliminary analysis.

**Drug:** Marbocyl 10% (Meiji, Tokyo); 2 mg/kg IV just before loading, based on pharmacokinetic studies showing efficacy >24 hr (Bousquet-Melou et al., 2002; Carretero et al., 2002).

**Outcome measures:**

- Rectal temperature before and after transport (mean ± SD)
- Number of febrile horses after transport (threshold: >38.5°C mild; >39.0°C severe with poor appetite)

**Treatment decisions:** Practitioners with ≥5 years experience reviewed clinical signs. Severe cases (temp >39.0°C + decreased/unsound appetite): cephalothin sodium 20 mg/kg IV every 6 hr (specific against _S. zooepidemicus_). Mild cases (temp >38.5°C but sound appetite): penicillin-streptomycin combination (Mycillin; penicillin 8,000 U/kg + streptomycin 10 mg/kg IM every 24 hr).

**Statistics:** Bartlett test for homogeneity → non-homogeneous → Kruskal-Wallis non-parametric test → Steel-Dwass multiple comparison for rectal temperatures. Chi-square for independence test for febrile horse counts. All tests upper-tailed; P<0.05 significant.

## Results (detailed — all numbers reproduced)

**Rectal temperature before transport:** No significant differences among the four groups (all ~37.9–38.0°C).

**Rectal temperature after transport:**

| Year | n   | Temp after (°C, mean ± SD) | MRFX |
| ---- | --- | -------------------------- | ---- |
| 2007 | 50  | 38.5 ± 0.40 ^a             | No   |
| 2008 | 49  | 38.6 ± 0.42 ^b             | No   |
| 2012 | 56  | 38.3 ± 0.26 ^b             | Yes  |
| 2013 | 56  | 38.3 ± 0.21 ^ab            | Yes  |

Superscripts indicate significant differences (P<0.01, Steel-Dwass): 2007 vs 2013, 2008 vs 2012, 2008 vs 2013 each P<0.01.

**Febrile horses after transport:**

| Era       | Years     | n   | Not febrile | Febrile total | Mild (A: pen-strep) | Severe (B: cephalothin) |
| --------- | --------- | --- | ----------- | ------------- | ------------------- | ----------------------- |
| Pre-MRFX  | 2007–2008 | 99  | 80          | 19 (19.2%)    | 14                  | 5                       |
| Post-MRFX | 2012–2013 | 112 | 105         | 7 (6.3%)      | 6                   | 1                       |

Reduction in febrile horses: P<0.01, chi-square for independence. Severe cases dropped from 5 to 1 (80% reduction). No horse showed adverse reactions (colic or shock) from MRFX injection.

**Summary:** Prophylactic MRFX reduced TAF incidence from 19.2% to 6.3%, with a marked reduction in severe cases requiring intensive IV therapy.

## Discussion & interpretation

The authors confirm that MRFX pharmacokinetics (effective bactericidal levels maintained >24 hr, low tissue irritation compared with ERFX) make it suitable for single pre-transport IV administration. Previous ERFX studies showed reduced serum amyloid A but non-significant rectal temperature changes, possibly due to small samples; the current larger cohort (n=211) had sufficient power to demonstrate both temperature and incidence reductions. The 2007 (36 hr transport) and 2008 (26 hr transport) control groups were treated as equivalent because no significant temperature difference was found post-hoc, despite the route-length difference — this is a potential confounder: the 36 hr route might inherently carry higher fever risk, meaning the MRFX effect may be slightly underestimated in the 36 hr group or overestimated comparatively. The authors acknowledged this limitation but concluded equivalence was reasonable given the statistical results. MRFX has been standard prophylactic practice at Hidaka since 2012.

## Limitations

- Small cohort sizes per year (49–56); the 2007 vs 2013 comparison conflates route length (1,540 km vs 1,210 km) with the intervention.
- No randomisation within cohort year; selection of 2007/2008 as controls and 2012/2013 as treated cohorts means unmeasured temporal confounders (breeding practices, horse constitution, vet staff) cannot be excluded.
- No microbiological confirmation of _S. zooepidemicus_ in the post-MRFX era febrile cases; etiology of breakthrough fever cases is unknown.
- Restricted to young (2-year-old) horses at their debut transport; generalisability to older horses or shorter transports is not established.
- Study focuses on 2 specific Hokkaido→Honshu routes; applicability to other Japanese regional transport routes is unclear.

## Feature-engineering notes for the model

- `transport_distance_km` — km from birth/training prefecture to debut racecourse, derived from horse origin data and race venue — expected effect: positive correlation with TAF risk and scratch probability at debut, primarily for 2-year-olds — available from registry + race entry data
- `transport_hours_est` — estimated one-way transport time (distance / average speed or lookup table for standard routes) — threshold effect at ~20 hr; non-linear encoding recommended — data availability: moderate (requires origin-to-venue time lookup)
- `is_hokkaido_origin` — binary flag: training/birth at Hokkaido stud farm → 1 — strong proxy for long-haul transport risk to Kanto/Kansai tracks — available from horse registration data
- `pre2012_long_haul` — interaction: is_hokkaido_origin × debut_year < 2012 — captures the higher pre-MRFX TAF baseline of ~19%; post-2012 rate ~6% — use as a temporal era correction
- `debut_race` — binary: this is the horse's first recorded JRA/NAR race — restricts transport-fever feature to debut context; no effect expected for subsequent races
- **DO NOT** apply this feature to races beyond the debut context; TAF is a one-time young-horse risk factor and should not be used as a general health feature for older horses.

## Key references / follow-up leads

- Oikawa & Kusunose (1995) J. Equine Sci. 6: 25–29 — epidemiology of transport-associated respiratory disease, risk beyond 20 hr
- Oikawa et al. (1995) J. Comp. Pathol. 113: 29–43 — pathology of equine respiratory disease with transport
- Tsuchiya et al. (2012) Am. J. Vet. Res. 73: 968–972 — single-dose enrofloxacin pre-transport: effects on body temperature and tracheobronchial neutrophil count
- Endo et al. (2014) J. Vet. Med. Sci. (in press at time of publication) — pre-shipping MRFX effects on fever and blood properties in Thoroughbreds
- Ito et al. (2001) J. Vet. Med. Sci. 63: 1263–1269 — bronchoalveolar lavage for diagnosis and treatment of transport pneumonia in Thoroughbred racehorses
- Bousquet-Melou et al. (2002) Equine Vet. J. 34: 366–372 — pharmacokinetics of marbofloxacin in horses
- Carretero et al. (2002) Equine Vet. J. 34: 360–365 — single IV/IM MRFX pharmacokinetics in mature horses
