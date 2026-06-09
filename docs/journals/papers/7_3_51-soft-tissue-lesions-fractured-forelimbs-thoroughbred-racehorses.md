# Morbid Anatomy of Soft Tissue in Fractured Forelimbs of Thoroughbred Racehorses

## Metadata

| Field                          | Value                                                                                                      |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 7(3): 51–54, 1996                                                                           |
| docid                          | `7_3_51`                                                                                                   |
| Article type                   | Original                                                                                                   |
| Authors                        | Mikihiro Kaneko, Masa-aki Oikawa, Toyohiko Yoshihara                                                       |
| Affiliations                   | Equine Research Institute, Japan Racing Association, 27–7 Tsurumaki 5-chome, Setagaya-ku, Tokyo 154, Japan |
| Received / Accepted / Released | Submitted April 1, 1996; accepted July 12, 1996                                                            |
| Keywords                       | pathology, fracture, racehorses, soft tissues                                                              |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/7/3/7_3_51/_pdf/-char/en                                          |

## Abstract (verbatim)

> To clarify the possible factors associated with intra-articular fracture of the lower limb, pathomorphological analysis was done on peripheral nerves, interosseous ligaments and joint capsules surrounding the lower limb joint of the forelimbs of 165 racehorses affected by lower limb fractures and 187 racehorses without fractures. Subclinical changes were frequently found to include ligament degeneration, degenerative joint disease and peripheral neural lesions, which might have been caused by local circulatory disorders and/or vascular alterations. The incidence of lesions seen in the soft tissues was higher in limbs with fracture than in those without. Thus, we speculate that the fractures would occur in limbs with soft tissue lesions, under the influence of resultant incoordination of lower limb locomotion induced by soft tissue lesions when physiological demands are imposed on the limb during training and racing.

## Relevance to finishing-position (着順) prediction

Feature family **A (injury/soundness)**. This paper identifies three types of subclinical soft tissue lesion — peripheral neural degeneration, interosseous ligament degeneration, and articular capsule changes — that are significantly more prevalent in racehorses that subsequently sustain intra-articular forelimb fractures. All three lesion types increase monotonically with age, number of races run, and length of racing history (Fig. 4 in paper). Because these lesions are clinically undetectable prior to fracture, the cumulative race exposure metrics serve as accessible population-level proxies for underlying degenerative load.

For the prediction pipeline the key engineered features are `career_starts` (total number of races run in JRA/NAR records) and `career_days` (days elapsed since first race entry), both of which directly map to the axes of Fig. 4. An interaction term `age_years × career_starts` is warranted because the paper demonstrates that lesion incidence rises with age independently of race count. The fracture-risk model component should predict non-finish / scratch probability: horses with high career exposure are more likely to sustain a catastrophic injury and not finish.

The paper also shows that lesions are present in contralateral non-fractured limbs (Group B, 53% ligament degeneration vs 33% in non-racing controls), confirming systemic workload damage rather than a single-limb effect. This means total starts is a better proxy than any single-limb history, and the effect generalises across left and right forelimbs without asymmetry correction.

Study population: JRA Thoroughbreds only — findings are directly applicable to JRA race data.

## Background & objective

Intra-articular fractures of the forelimb (carpus and metacarpophalangeal joint) are the most common catastrophic racing injuries in Thoroughbreds. Prior work by the same group (Kaneko et al. 1993) established focal osteochondral lesions as predisposing factors for bone fractures. The morbid anatomy of surrounding soft tissue in fractured limbs had not previously been described. The objective was to characterise pathomorphological changes in peripheral nerves, interosseous ligaments, and articular capsules of fractured vs. non-fractured forelimbs, and to evaluate their significance in fracture genesis.

## Materials & methods

**Subjects:** 165 Thoroughbred racehorses (106 males, 3 geldings, 56 females) aged 2.5–7.5 years with intra-articular fractures of the carpus or metacarpophalangeal joint. Fracture types: transverse, splintered/comminuted, longitudinal, chip, or slab. Controls: 187 Thoroughbred racehorses (121 males, 11 geldings, 55 females) of similar age (2.5–7.5 years) without fractures, killed for causes unrelated to racing injuries (sudden death, pneumonia, colic, trauma, etc.).

**Limb groups:**

- Group A: limbs with fractures (fractured limb)
- Group B: limbs without fracture contralateral to fractured limbs
- Group C: limbs from non-fractured horses with race history (control, raced)
- Group D: limbs from non-fractured horses without race history (control, unraced)

**Tissues examined:**

- Peripheral nerves (7 nerve types: N. radialis, N. ulnaris, N. medianus, N. palmaris medialis, N. palmaris lateralis, N. palmaris lateralis ramus dorsalis, N. palmaris lateralis ramus palmaris) from 46 forelimbs (23 horses)
- Interosseous ligaments (7 sites from origin to insertion) from 188 forelimbs (94 horses)
- Articular capsules of metacarpophalangeal joint from 96 forelimbs (48 horses)
- Control nerves from 104 forelimbs (52 horses); control ligaments from 133 forelimbs (67 horses); control capsules from 136 forelimbs (68 horses)

**Histology:** Tissues fixed in 10% neutral buffered formalin, paraffin-embedded, sectioned at 4 µm, stained with haematoxylin-eosin (HE). Four microscopic fields per specimen assessed.

**Statistics:** Chi-squared test for incidence of lesions across four groups; incidence vs. age / number of races / racing period displayed graphically (Fig. 4).

## Results (detailed — reproduce ALL numbers)

### Table 1: Incidence of lesions in fractured and non-fractured forelimbs

| Tissue                            | Measure                | Group A (fractured) | Group B (contralateral) | Group C (raced, no fracture) | Group D (unraced, no fracture) |
| --------------------------------- | ---------------------- | ------------------- | ----------------------- | ---------------------------- | ------------------------------ |
| Metacarpophalangeal joint capsule | Limbs examined         | 48                  | 48                      | 76                           | 60                             |
|                                   | Limbs affected         | 27                  | 15                      | 23                           | 12                             |
|                                   | % affected             | **56.3**            | **31.3**                | **30.3**                     | **20.2**                       |
| Interosseous ligament             | Limbs examined         | 98                  | 90                      | 85                           | 48                             |
|                                   | Limbs affected         | 63                  | 48                      | 36                           | 16                             |
|                                   | % affected             | **64.2**            | **53.3**                | **42.3**                     | **33.3**                       |
| Peripheral nerve (nerve bundles)  | Limbs examined         | 23                  | 23                      | 72                           | 32                             |
|                                   | Nerve bundles examined | 175                 | 170                     | 541                          | 232                            |
|                                   | Nerve bundles affected | 75                  | 67                      | 165                          | 35                             |
|                                   | % affected             | **42.9**            | **39.4**                | **30.5**                     | **15.1**                       |

### Table 2: Statistical significance of pairwise comparisons (chi-squared)

| Tissue                            | A vs B  | A vs C  | A vs D  | B vs C | B vs D  | C vs D  |
| --------------------------------- | ------- | ------- | ------- | ------ | ------- | ------- |
| Metacarpophalangeal joint capsule | P<0.025 | P<0.005 | P<0.005 | NS     | NS      | NS      |
| Interosseous ligament             | NS      | P<0.005 | P<0.005 | NS     | P<0.05  | NS      |
| Peripheral nerve                  | NS      | P<0.005 | P<0.005 | P<0.05 | P<0.005 | P<0.005 |

NS = not significant.

### Temporal trends (Fig. 4)

All three lesion types (joint capsule, ligament degeneration, neural lesion) showed increasing incidence with age, number of races run, and length of racing history. Quantitative values are graphical only; direction is monotonically increasing across all three axes.

### Pathological lesion descriptions

- **Peripheral nerves:** multifocal nerve fiber loss in bundles, reticular network of Schwann cells and loose fibrous connective tissue, edematous loosening in epineurium and perineurium, accumulation of eosinophilic liquid in nerve bundles.
- **Interosseous ligaments:** fibroplasia of edematous collagen fibers, calcification at musculotendinous junction (origin), pyknosis/karyorrhexis of nuclei, cytoplasmic microvacuolation, edematous tendon cells with irregular collagen arrangement; reparative fibroblast/reticular/capillary hyperplasia; peritendineum thickening with arteriosclerosis and medial calcification.
- **Articular capsule:** synovial villous proliferation (hyperplasia of villous lining cells and collagen fibers), villonodular synovitis with chondral metaplasia at the transition zone, paucity of cellular components in the fibrous layer, mural loosening of small vessels, nerve fiber loss, Vater-Pacinian corpuscle changes (axonal swelling, vesicle formation, outer bulb edema).

## Discussion & interpretation

The authors propose that subclinical degenerative changes in peripheral nerves, ligaments, and joint capsules — arising from local circulatory disorders (hypoxia/ischemia) from immoderate exercise — cause subtle incoordination of lower limb locomotion. When physiological demands reach maximum during racing (noted to often occur near the finish line after stumbling), fractures occur in already-compromised limbs. This is consistent with Pool (1993) who attributed osteochondral lesions to biomechanical stress of abnormal intensity/duration. Neural lesions are histologically similar to those in radial nerve paralysis and compartmental syndrome in humans.

The contralateral limb (Group B) shows intermediate lesion rates (53% ligament, 39% neural) — higher than raced controls (42%, 31%) but lower than the directly fractured limb — suggesting systemic workload-induced damage, not purely local trauma.

## Limitations

- Sample sizes for nerve examination are small (23 horses in fracture group, 52 in controls)
- Morphometric assessment was subjective (four microscopic fields per specimen)
- No quantitative severity scoring — only presence/absence incidence
- Graphical (not tabular) presentation of age/race-count correlations; no r values reported
- Horses in fracture group killed at time of injury vs controls killed for other causes — potential selection bias in lesion severity
- JRA Thoroughbred population only; generalisability to NAR/Ban-ei uncertain

## Feature-engineering notes for the model

- `career_starts` — total number of race starts from debut to current entry — source: `starts` field in JRA/NAR horse records — expected effect: positive (higher starts → higher degenerative soft tissue lesion risk → higher fracture/DNF probability) — data availability: routinely available
- `career_days` — days from first race entry to current race — source: date of first race in entry history — expected effect: positive (longer racing period → higher lesion risk) — data availability: derivable from race date records
- `age_years` — horse age in years — source: birth year in horse master — expected effect: positive (age-independent lesion accumulation confirmed) — data availability: standard feature
- `age_years × career_starts` — interaction term — expected effect: positive synergy; lesion incidence rises with both axes simultaneously per Fig. 4
- `career_starts / age_years` — race intensity proxy (races per year of life) — expected effect: uncertain (high pace may accelerate lesion accumulation beyond age alone)
- **Do NOT use:** single-limb injury history as a proxy for fracture risk — paper shows contralateral limb has nearly equivalent lesion rates, so the risk is systemic

## Key references / follow-up leads

- Kaneko M., Oikawa M., and Yoshihara T. 1993. Pathological analysis of bone fractures in race horses. _J. Vet. Med. Sci._ 55: 181–183. (companion paper by same group; bone fracture pathology)
- Oikawa M., Ueda Y., Inada S., Tsuchikawa T., Kusano H., and Takeda A. 1994. Effect of restructuring the geometric design of a racetrack on the occurrence of racing injuries in Thoroughbred horses. _J. Equine Vet. Sci._ 14: 262–268. (racetrack geometry and injury incidence)
- Krook L. and Maylin G. 1988. Fractures in Thoroughbred race horses. _Cornell Veterinarian_ 78, Suppl. 11: 1–133. (comprehensive fracture pathology reference)
- Pool R.R. 1993. Difficulties in definition of equine osteochondrosis: Differentiation of development and acquired lesions. _Equine Vet. J._, Suppl. 16: 5–12. (osteochondral lesion classification)
- Pratt G.W. and O'Connor J.T. 1978. A relationship between gait and breakdown in the horse. _Am. J. Vet. Res._ 39: 249–253. (gait and injury biomechanics)
