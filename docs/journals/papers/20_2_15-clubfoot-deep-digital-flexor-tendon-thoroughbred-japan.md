# Survey of the Occurrence of Equine Deep Digital Flexor Tendon Contraction (Clubfoot) in the Main Thoroughbred Breeding Area in Japan

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                            |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 20(2): 15–17, 2009                                                                                                                                                                                                                                                                                                |
| docid                          | `20_2_15`                                                                                                                                                                                                                                                                                                                        |
| Article type                   | Note                                                                                                                                                                                                                                                                                                                             |
| Authors                        | Kosuke Tanaka, Yo Asai, Atsutoshi Kuwano                                                                                                                                                                                                                                                                                         |
| Affiliations                   | Equine Science Division, Hidaka Yearling Training Farm, JRA, 535–13 Nishicha, Urakawa-cho, Hokkaido 057-0171; Clinical Science & Pathobiology Division, Equine Research Institute, JRA, 321–4 Tokami-cho, Utsunomiya-shi, Tochigi 320-0856; Japan Bloodhorse Breeders' Association, Shizunai Stallion Station, Hokkaido 056-0144 |
| Received / Accepted / Released | Accepted March 30, 2009                                                                                                                                                                                                                                                                                                          |
| Keywords                       | clubfoot, foals, Japan, Thoroughbred                                                                                                                                                                                                                                                                                             |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/20/2/20_2_15/_pdf/-char/en                                                                                                                                                                                                                                                              |

## Abstract (verbatim)

> At 114 ranches in the Hidaka District of Hokkaido, a total of 1,118 Thoroughbred foals that were born from January to June 2003 were examined at different times from May to September of the same year to ascertain the occurrence of clubfoot. Clubfoot was seen in a total of 179 foals (16%) at 89 ranches (78%). Also, 124 of the 179 foals (69.3%) had clubfoot of grade II or higher, and it was found that grade I was likely to be overlooked or rapidly advance. In the present study, the occurrence of clubfoot was higher than expected in the investigated region. Hence, it is necessary to establish accurate diagnostic techniques and clubfoot guidelines to minimize the impact.

## Relevance to finishing-position (着順) prediction

Feature family: **A — injury/soundness**. Clubfoot (deep digital flexor tendon contraction, DDFTC) is a progressive orthopaedic condition of Thoroughbred foals that, if untreated or grade III+, renders the foal unsuitable for racing. This study quantifies the prevalence of clubfoot in Japanese Thoroughbred breeding stock (16% of foals, 69% at grade II or higher), directly informing injury/soundness risk estimates for horses at the start of their racing careers. Because the study population is Hidaka, Hokkaido — the primary source of JRA Thoroughbreds — the prevalence figures apply directly to the JRA target population.

For the pipeline, the key linkage is yearling sale veterinary records (or breeding farm notes) → `clubfoot_grade` (ordinal 0–4) as a pre-career health flag. A horse with confirmed grade II+ clubfoot in early life faces elevated scratch/attrition risk. The 93.8% of cases presenting at 2–4 months of age and the null incidence in hindlimbs (only forelimbs affected) are mechanistic constraints that inform which conformation measurements to prioritise. Hard-terrain ranches (66.7% incidence vs. ~10–30% on standard farms) suggest a ranch-level or soil-hardness covariate; if breeding-farm region within Hidaka is recorded, it could be added as a latent environmental factor.

The indirect path to finishing position runs through career length and attrition: severe DDFTC truncates racing careers, supporting a competing-risk or hurdle model component. Horses that enter the JRA dataset without prior farm/auction health records effectively have clubfoot grade unknown, but the 16% base rate sets the population prior for unseen injury history.

## Background & objective

Flexural deformities (DDFTC and superficial digital flexor tendon contraction, SDFTC) are common developmental conditions in foals. Multiple factors — nutritional, mechanical, and possibly myofibroblastic smooth-muscle contraction in the deep digital flexor tendon — interact in onset. In chronic cases, hoof angle exceeds 80° (grade IV). No prior report had surveyed clubfoot occurrence in Japanese Thoroughbred breeding areas, creating a gap for JRA veterinary management and modelling. The study aims to document occurrence rate, grade distribution, limb affected, and age at onset in the Hidaka district.

## Materials & methods

- **Subjects**: 1,118 Thoroughbred foals born January–June 2003 at 114 ranches in the Hidaka district of Hokkaido (the main Japanese Thoroughbred breeding region). Birth month breakdown: Jan 18, Feb 119, Mar 396, Apr 400, May 168, Jun 17.
- **Timing**: Monthly farrier visits May–September 2003 (five-month period; each ranch visited once per month).
- **Examination**: At the time of hoof trimming, digital axis and hoof conformation were examined for: flexural deformity of digits, gap between flat ground and weight-bearing surface of heel, coronet swelling, hoof wall curvature (dish), and change of dorsal hoof wall angle relative to the opposite normal foot.
- **Grading**: Redden's method (Proc. AAEP 34:321, 1988), four grades:
  - Grade I (G I): hoof angle 3–5° greater than opposite foot
  - Grade II (G II): hoof angle 5–8° greater, hoof ring interval wider at heel than toe
  - Grade III (G III): features of G II more notable, broken forward of interphalangeal joint progressing
  - Grade IV (G IV): hoof angle ≥80°, coronet altitude equal between toe and heel
- **Recording**: Age in months and grade at initial diagnosis per foal (each foal counted once).
- **No radiographic examination** was performed.
- **Statistical method**: Descriptive counts and percentages; no formal inferential statistics reported.

## Results (detailed — reproduce ALL numbers)

**Overall prevalence:**

- 179/1,118 foals (16.0%) diagnosed with clubfoot
- Affected at 89/114 ranches (78.1%); not seen at 25 ranches (21.9%)

**Grade distribution at initial diagnosis:**

| Grade     | n       | %          |
| --------- | ------- | ---------- |
| G I       | 55      | 30.7%      |
| G II      | 109     | 60.9%      |
| G III     | 15      | 8.4%       |
| G IV      | 0       | 0.0%       |
| **Total** | **179** | **100.0%** |

G II or higher: 124/179 = **69.3%**. Authors note G I was frequently "overlooked or rapidly advance" — implying true G I incidence is underestimated.

**Age at initial diagnosis (months):**

| Age (months) | G I    | G II    | G III  | G IV  | Total   | %     |
| ------------ | ------ | ------- | ------ | ----- | ------- | ----- |
| 1            | 4      | 4       | 0      | 0     | 8       | 4.5%  |
| 2            | 16     | 24      | 3      | 0     | 43      | 24.0% |
| 3            | 29     | 62      | 3      | 0     | 94      | 52.5% |
| 4            | 6      | 17      | 8      | 0     | 31      | 17.3% |
| ≥5           | 0      | 3       | 0      | 0     | 3       | 1.7%  |
| **Total**    | **55** | **109** | **15** | **0** | **179** |       |

Peak onset: 3 months (52.5%). The 2–4 month range: 43+94+31 = 168/179 = **93.8%** of all cases.

**Limb affected:**

- Exclusively forelimbs; 93 left, 86 right. No hindlimb cases in the entire sample.
- Authors attribute forelimb predominance to the horse's centre of gravity being weighted towards forelimbs.

**Ranch-level variation:**

- Most ranches: 10–30% incidence (overwhelming majority of breeding farms).
- Highest single ranch: **66.7%** — situated on a riverbed; grazing land covered with hard clay. Authors speculate hard soil damages developing limbs via unknown mechanism.

## Discussion & interpretation

Authors note the 16% prevalence is higher than expected in the Hidaka region, suggesting a need for systematic diagnostic standards. G I is likely underdiagnosed due to subtle presentation and rapid progression. The exclusive forelimb involvement aligns with biomechanical loading principles (forelimbs bear greater proportion of body weight). No causal mechanism is established; multiple factors (lameness, nutrition, myofibroblastic contraction) are invoked. The riverbed ranch finding implicates substrate hardness as an environmental risk factor. The study recommends collaboration among ranchers, farriers, and veterinarians to clarify onset factors and develop treatment guidelines. No follow-up outcome data (which foals became racehorses, treatment success rates) are provided within this paper.

## Limitations

- Single-year cohort (2003 foals) — no multi-year trend.
- No radiographic confirmation; classification is clinical/conformation-based, introducing inter-examiner variability.
- No outcome data: which affected foals entered training, raced, or were culled.
- Grading by visiting farriers, not veterinarians — potential for inconsistent application of Redden criteria.
- Sample limited to Hidaka; may not generalise to other Japanese or international breeding regions.

## Feature-engineering notes for the model

- `clubfoot_grade` — ordinal 0/1/2/3 from yearling sale veterinary examination or farm records — derivable from auction catalogue veterinary notes (JBBA, HAJ sales) — expected effect: higher grade → elevated scratch/career-absence risk, negative finishing-position prior — availability: uncertain for most JRA horses; where absent, impute from population base-rate 0.16 (any grade), 0.11 (grade II+)
- `clubfoot_forelimb_side` — binary left/right if grade ≥ I — source: same records — expected interaction with racing style: left-front defect may affect left-turning tracks (Nakayama, Chukyo); right-front on right-turning (Hanshin inner, Kokura) — data availability: very limited
- `ranch_terrain_type` — categorical (riverbed hard-clay vs. standard) — source: breeding farm registry — expected effect: proxy for foal joint loading; high-clay ranches produce more severe clubfoot — data availability: not in standard JRA race records; would require supplemental linkage
- **Do NOT use** clubfoot grade as a direct feature if derived only post-hoc from race records (circular). Use only pre-career veterinary records.

## Key references / follow-up leads

- Redden, R.F. 1988. "A method of treating club feet." _Proc. Am. Assoc. Equine Pract._ 34: 321. [grading method]
- Adams, S.B., and Santschi, E.M. 2000. "Management of congenital and acquired flexural limb deformities." _Proc. Am. Assoc. Equine Pract._ 46: 117–125. [management outcomes]
- Hartzel, D.K. et al. 2001. "Myofibroblasts in the accessory ligament and the deep digital flexor tendon of foals." _Am. J. Vet. Res._ 62: 823–827. [smooth-muscle mechanism]
- Springings, E., and Leach, D. 1986. "Standardised technique for determining the centre of gravity of body and limb segments of horses." _Equine Vet. J._ 18: 43–49. [forelimb loading rationale]
- Curtis, S. 2006. _Corrective Farriery Vol. II_, Newmarket Farrier Consultancy. [farriery treatment review]
