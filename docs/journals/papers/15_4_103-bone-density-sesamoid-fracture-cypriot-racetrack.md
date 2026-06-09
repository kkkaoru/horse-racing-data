# The Role of Bone Density in the High Incidence of Proximal Sesamoid Bone Fracture at the Cypriot Race Track

## Metadata

| Field                          | Value                                                                                                                                                                          |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Journal                        | J. Equine Sci. 15(4): 103–106, 2004                                                                                                                                            |
| docid                          | `15_4_103`                                                                                                                                                                     |
| Article type                   | Note                                                                                                                                                                           |
| Authors                        | Kyriakos SIAKALIS, Nikolaos DIAKAKIS, Aggelos DESIRIS                                                                                                                          |
| Affiliations                   | Veterinary Surgeon Practitioner, Cyprus; Clinic of Surgery, Department of Clinical Sciences, Aristotle University of Thessaloniki, St. Voutyra 11, 546 27 Thessaloniki, Greece |
| Received / Accepted / Released | Accepted September 21, 2004                                                                                                                                                    |
| Keywords                       | bone density, proximal sesamoid bone fracture, horse                                                                                                                           |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/15/4/15_4_103/_pdf/-char/en                                                                                                           |

## Abstract (verbatim)

> The aim of this study was to investigate the high frequency of proximal sesamoid bone fracture in the horses at the Cypriot Hippodrome in relation to bone density. Sixty horses were included in the study, divided into three groups of twenty horses. The first subgroup included 20 Cypriot horses that were euthanised due to proximal sesamoid bone fractures. The second subgroup included 20 Cypriot horses that were euthanised or died due to reasons that were not related to fractures or any other bone disease. The third subgroup included 20 horses from the Greek race track that were euthanised or died due to reasons that were not related to fractures or any other bone disease. The bone density of the proximal sesamoid bones was determined with the aid of computed tomography. Statistical analysis of the collected data showed that there is a significant difference in bone density between Cypriot and Greek horses, which could be related to the high frequency of proximal sesamoid fracture.

## Relevance to finishing-position (着順) prediction

Feature family: **A (injury/soundness — catastrophic fracture risk)**. Proximal sesamoid bone fractures are among the most common catastrophic injuries in racehorses, accounting for 22% of race accidents (Ueda et al. 1993) and frequently causing euthanasia and always causing immediate scratch/DNF. This study identifies elevated bone density as a predisposing risk factor: Cypriot horses (both fracture and non-fracture) have significantly higher sesamoid bone density (~995–1,045 mg/cm³) than Greek horses (~933–947 mg/cm³, p < 0.05), and the difference is attributed to excessive dietary Ca/P intake (70–100 g Ca/day vs. recommended 25–40 g Ca/day).

The key insight for a JRA/NAR pipeline: sesamoid bone density — driven by diet, training intensity, and population history — is a predisposing (not causative) fracture factor. Because bone density is not directly measurable in a race database, the actionable features are:

1. `cumulative_high_intensity_training_starts` — proxy for training-induced bone remodelling
2. `diet_supplementation_flag` — excessive Ca/P supplementation (not available in JRA databases, but trainers at some stables known to over-supplement)
3. `catastrophic_fracture_flag_historical` — any horse with a proximal sesamoid fracture history is at greatly elevated re-fracture or stress-fracture risk in remaining competition

The A/B groups (Cypriot fracture vs. Cypriot non-fracture) show higher fracture-group density but not statistically significantly so — suggesting that bone density is a population-level predisposing factor, not a deterministic individual predictor. Bone density is one of many fracture pathogenesis factors.

Interactions: `age` (horses 2–4 years; bone remodelling active), `training_intensity` (strenuous training + high Ca/P diet = bone density increase), `surface_hardness` (hard track surface amplifies sesamoid flexural forces), `body_weight` and `futan/斤量` (higher load → more sesamoid stress), `speed_profile` (front-runners may have greater fetlock hyperextension at the start).

## Background & objective

Proximal sesamoid bone fractures are common, severe, and frequently fatal in racehorses (Johnson et al. 1994 — 22% of race-accident fractures; Ueda et al. 1993). The Cypriot Hippodrome experienced fracture rates higher than those at the Greek racetrack or "any other track in the world" (per the paper). Multiple aetiological factors are implicated: heredity, environment, dietary imbalance, bone density, hormonal factors, drugs, shoeing, training intensity, and track surface. This study isolates bone density as one potential contributor. The hypothesis is that high dietary Ca/P intake → excessive sesamoid bone density → brittle, inelastic bone → fracture under flexural forces of the fetlock during exercise.

## Materials & methods

**Subjects:** 60 Thoroughbred stallions, all active racehorses, age 2–4 years, divided into three subgroups of 20:

- **Group A:** 20 Cypriot horses euthanised due to proximal sesamoid bone fracture. Euthanised by IV overdose of barbiturates.
- **Group B:** 20 Cypriot horses euthanised or died from causes unrelated to fractures or bone disease.
- **Group C:** 20 horses from a Greek racetrack euthanised or died from causes unrelated to fractures or bone disease.

**Style of training and track type:** identical across Greek and Cypriot horses, according to the authors.

**Bone density measurement:** CT scan post-mortem. Each proximal sesamoid bone measured at 3 cross-section levels, each 1.5 mm thick, perpendicular to longitudinal axis:

- Proximal level: 5 mm above midpoint
- Middle level: at midpoint of longitudinal axis
- Distal level: 5 mm below midpoint

Density measured in mg/cm³. Mean of all sesamoid bones per group calculated per level.

**Statistical analysis:** Student's t-test (SPSS for Windows). Significance p < 0.05. Means compared pairwise across groups.

## Results (detailed — reproduce ALL numbers)

**Sesamoid bone density at proximal level (Table 1):**

| Group                    | n   | Mean (mg/cm³)  | SD      |
| ------------------------ | --- | -------------- | ------- |
| A — Cypriot, fracture    | 20  | **1034.409** ᵃ | 30.8988 |
| B — Cypriot, no fracture | 20  | **994.767** ᵃ  | 42.6662 |
| C — Greek, no fracture   | 20  | **947.173** ᵇ  | 19.6070 |

**Sesamoid bone density at middle level (Table 2):**

| Group                    | n   | Mean (mg/cm³)  | SD      |
| ------------------------ | --- | -------------- | ------- |
| A — Cypriot, fracture    | 20  | **1044.334** ᵃ | 36.7180 |
| B — Cypriot, no fracture | 20  | **1009.025** ᵃ | 36.2786 |
| C — Greek, no fracture   | 20  | **933.207** ᵇ  | 33.8478 |

**Sesamoid bone density at distal level (Table 3):**

| Group                    | n   | Mean (mg/cm³)  | SD      |
| ------------------------ | --- | -------------- | ------- |
| A — Cypriot, fracture    | 20  | **1045.264** ᵃ | 48.9456 |
| B — Cypriot, no fracture | 20  | **991.672** ᵃ  | 38.6554 |
| C — Greek, no fracture   | 20  | **933.826** ᵇ  | 65.5777 |

Groups sharing superscript ᵃ do not differ significantly from each other; ᵃ vs. ᵇ: p < 0.05 at all three levels.

**Key statistical summary:**

- Cypriot horses (Groups A + B combined) have significantly higher bone density than Greek horses (Group C) at ALL three levels (p < 0.05).
- Group A (fracture) shows the highest density at all levels, approximately 5–7% higher than Group B (Cypriot non-fracture), but this difference is **NOT statistically significant** (Groups A and B share superscript ᵃ).
- The A vs. B difference at proximal: 1034 – 995 = 39 mg/cm³; the B vs. C difference: 995 – 947 = 48 mg/cm³.

**Dietary context (personal communication from trainers):**

- Cypriot racehorses: 70–100 g Ca/day + 55–60 g P/day
- Recommended for a 400 kg horse in full training (≥3 yr): 25–40 g Ca/day + 17 g P/day (Hodgson & Rose 1994; Lewis 1995)
- Cypriot intake = 2–3× the recommended calcium level

**Training context:** Style and intensity described as identical for Greek and Cypriot horses; track surface identical in this comparison. Therefore, dietary differences are the most probable explanation for bone density differences.

## Discussion & interpretation

High cortical bone density makes bone more brittle and less elastic ("hard but inelastic" — Kanakoudis et al. 1995). Proximal sesamoid bones must absorb combined pressure and tension forces during extreme dorsiflexion of the fetlock at high speed; they require elasticity to resist fracture. A high Ca/P dietary ratio creates denser but less elastic bones. The strenuous training amplifies bone remodelling (Katayama et al. 2001 on bucked shin), further increasing density. The suspensory ligament exerts proximal tension and the distal sesamoidean ligaments exert distal tension on the sesamoids during galloping; under extreme dorsiflexion, the resulting force can exceed the fracture threshold of over-dense, inelastic bone.

The A vs. B non-significant difference (fracture vs. non-fracture Cypriot horses) is acknowledged, and the authors note that "bone density is only a predisposing and not a causative factor." Other factors — shoeing, style of riding, track surface, hormones, individual conformation — also contribute.

## Limitations

- Post-mortem case-control study; fracture-group horses (A) are selected by having fractured, creating survival bias.
- All horses are stallions only; no mares or geldings.
- n = 20 per group; insufficient power to detect the A vs. B difference as significant.
- Bone density measured by CT post-mortem; in vivo correlates (radiography, pQCT) would be needed for pre-race risk screening.
- "Training and track type identical" relies on trainer report; not objectively verified.
- Population entirely from Cyprus and Greece; not JRA/NAR horses. Ca/P supplementation practices in Japan may differ.
- No racing performance data beyond "fracture/no fracture"; finishing-position predictions are speculative.
- Other fracture pathogenesis factors (hormones, drugs, shoeing, track surface, conformation) not investigated.

## Feature-engineering notes for the model

- `fracture_history_flag` — binary; any prior proximal sesamoid or metacarpal fracture in race records; horses with fracture history have demonstrated bone vulnerability — source: JRA/NAR injury records — expected effect: very strong positive predictor of scratch/DNF — availability: JRA race records (injury/scratch reasons)
- `career_high_intensity_race_starts` — count of career race starts above a speed/distance threshold; proxy for cumulative bone-loading stress — source: race records — expected effect: positive predictor of fracture risk at high counts (non-linear, may plateau) — availability: standard
- `consecutive_races_without_rest` — number of races in last 90 days; insufficient rest → insufficient bone remodelling recovery → elevated fracture risk — source: race history — availability: standard
- `track_surface_hardness` — categorical or numeric; hard/firm going increases sesamoid impact force; cite: proximal sesamoid fractures more common on hard surfaces — source: official going reports — availability: standard
- `body_weight_heavy` — 馬体重 ≥ 480 kg; heavier horses carry more sesamoid stress at high speed; interacts with training intensity — source: race entry weight — availability: standard
- `carried_weight_斤量` — heavier weight carried amplifies sesamoid stress; interaction with horse body weight — source: race entry — availability: standard
- `age_young` — horses 2–4 years (active bone remodelling phase); combined with strenuous training → bone density increase fastest at this age; fracture risk elevated — source: race entry — availability: standard
- `diet_ca_p_excess_region` — population-level flag; if region of training origin is known to have excessive Ca/P supplementation practices, assign elevated fracture risk prior — availability: not directly available; regional indicator only
- **Caution:** CT-derived bone density is NOT available in any standard race or training database; all feature engineering must rely on indirect proxies (history, training load, weight, surface).

## Key references / follow-up leads

- Ueda, Y., Yoshida, K., Oikawa, M. 1993. Analyses of race accident conditions through use of patrol video. _J. Equine Vet. Sci._ 13: 707–710. [22% sesamoid fracture rate in JRA race accidents]
- Johnson, B.J. et al. 1994. Causes of death in racehorses over a 2 year period. _Equine Vet. J._ 26: 327–330. [Sesamoid fracture as cause of death]
- Richardson, D.W. 1999. Fractures of the proximal sesamoid bones. In: _Equine Medicine and Surgery_, 5th Ed. (Colahan et al.). Mosby. [Clinical management reference]
- Yoshihara, T. et al. 1990. Bone morphometry and mineral contents of the distal part of the fractured third metacarpal bone in Thoroughbred Racehorses. _Bull. Equine Res. Inst._ 27: 1–6. [JRA-origin metacarpal bone density and fracture data]
- Katayama, Y., Ishida, N., Kaneko, M., Yamaoka, S., Oikawa, M. 2001. The influence of exercise intensity on bucked shin complex in horses. _J. Equine Sci._ 12: 139–143. [Exercise intensity and bone remodelling in Japanese TBs]
- Tomioka, Y. et al. 1985. Bone mineral content of metacarpus in racehorses by photon absorption technique. _Bull. Equine Res. Inst._ 22: 21–29. [JRA metacarpal bone mineral content]
- Hodgson, D.R., Rose, R.J. 1994. Nutrient requirements of performance horse. In: _The Athletic Horse_. W.B. Saunders. [Ca/P dietary requirements — the standard reference for comparison with Cypriot over-supplementation]
