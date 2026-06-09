# Effects of Exercise on Plasma Haptoglobin Composition in Control and Splenectomized Thoroughbred Horses

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                      |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Journal                        | J. Equine Sci. 13(3): 89–92, 2002                                                                                                                                                                                                                                                                                                          |
| docid                          | `13_3_89`                                                                                                                                                                                                                                                                                                                                  |
| Article type                   | Note                                                                                                                                                                                                                                                                                                                                       |
| Authors                        | Kei Hanzawa, Atsushi Hiraga, Yutaka Yoshida, Hiromi Hara, Makoto Kai, Katsuyoshi Kubo, Seiki Watanabe                                                                                                                                                                                                                                      |
| Affiliations                   | Laboratory of Animal Physiology, Tokyo University of Agriculture, 1737 Funako, Atsugi, Kanagawa 243-0034; Hidaka Yearling Training Farm, Japan Racing Association, 535-13 Azanishisha, Urakawa-cho, Urakawa, Hokkaido 057-0171; Equine Research Institute, Japan Racing Association, 321-4 Togami-cho, Utsunomiya, Tochigi 320-0856, Japan |
| Received / Accepted / Released | Accepted October 28, 2002                                                                                                                                                                                                                                                                                                                  |
| Keywords                       | exercise, haptoglobin, horse, osmotic fragility, red cells                                                                                                                                                                                                                                                                                 |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/13/3/13_3_89/_pdf/-char/en                                                                                                                                                                                                                                                                        |

## Abstract (verbatim)

> In splenectomized Thoroughbred horses (Group S), the plasma haptoglobin (Hp) concentration was increased by the operation but thereafter significantly decreased to a lower level than before the surgery. The red cell haemolysis rate in a hypotonic salt solution and haemoglobin (Hb) binding Hp after exercise in Group S were significantly lower than in control horses (Group C). These results suggest that: 1) the spleen accelerates the fragility of the red cell membrane; 2) osmotic sensitive red cells are easy to haemolyze during exercise; and 3) the haemolysis during exercise increases the Hb-binding Hp level in plasma and then accelerates the synthesis of Hp in liver. Therefore, plasma Hp is a clinical indicator of red cell membrane fragility and haemolysis in a blood vessel during exercise.

## Relevance to finishing-position (着順) prediction

This paper belongs to feature family **C (exercise-physiology/fitness)**. It establishes plasma haptoglobin (Hp) — specifically the haemoglobin-binding fraction — as a blood biomarker of exercise-induced red-cell haemolysis in Thoroughbreds. Hb-binding Hp rises post-exercise in proportion to intravascular haemolysis, which itself reflects spleen-mediated red-cell membrane fragility and erythrocyte reserve. The finding that maximum treadmill speed differed substantially between control (9.8 m/s) and splenectomized (8.1 m/s) groups directly links splenic erythrocyte mobilisation to aerobic performance capacity. This is the physiological basis for using haematocrit (PCV) and haemoglobin as fitness-state features: the spleen can double circulating red-cell mass during a race, driving O2 delivery and speed.

For the finishing-position pipeline, if any pre-race blood data are available (e.g., from JRA training centre screening or veterinary records), PCV, Hb, and Hb-binding Hp are high-value features. The paper shows that Group C horses (normal spleen) achieved PCV of 57.2% post-exercise vs. 41.4% in splenectomised horses — a difference that mechanically translates to aerobic capacity. Peak HR was 199 bpm for Group C vs. 216 bpm for Group S at lower speed, demonstrating that splenectomised horses must work far harder for lower performance — an analogy for individual differences in haematopoietic reserve.

Since JRA race records do not include blood data directly, the nearest proxies are: (a) days-since-last-race as a recovery proxy, (b) recent race finishing times as a fitness-state signal, and (c) weight-carried × distance interactions that modulate aerobic demand. The Hb-binding Hp methodology (PAGE with o-dianisidine staining) described here is too laboratory-intensive for routine feature engineering, but the conceptual framework supports inclusion of any blood analytes available in training logs.

## Background & objective

Strenuous exercise imposes physical and chemical stresses on red cells, promoting membrane fragility and intravascular haemolysis. Plasma haptoglobin (Hp) is an acute-phase protein synthesised in the liver in response to free haemoglobin; it binds specifically to free Hb and facilitates hepatic clearance. Prior work by the same group showed that splenectomy decreases osmotic fragility of equine erythrocytes, suggesting that splenic passage accelerates red-cell ageing/fragility. This study aimed to compare exercise-induced changes in plasma Hp composition (total Hp vs. Hb-binding Hp) between intact (Group C) and splenectomised (Group S) Thoroughbreds to clarify the role of splenic erythrocyte cycling in exercise haemolysis.

## Materials & methods

**Subjects:** Six Thoroughbreds (5 two-year-olds, 1 five-year-old; 3 males, 1 gelding, 2 females; mean ± SEM body weight 462 ± 14 kg). Divided into two groups of three: control (Group C, sham surgery — 18th rib removed and wound sutured) vs. splenectomised (Group S, spleen surgically removed after removal of 18th rib on left side). Surgery performed 6 months before the exercise test.

**Conditioning:** Treadmill training (Mustang-2200, Kagra, Switzerland) started 1 month post-surgery; 5 days/week, 15 min sessions including 2 min of submaximal exercise, continued until exercise test period.

**Exercise test:** Incremental treadmill test — 1.5 min at 80% of peak speed (determined in preliminary test), 1 min at 90%, 1 min at 100%, then speed increased by 5% of peak speed every min until fatigue. Each horse performed two exercise tests within a 3-week period.

**Measurements:**

- Heart rate (HR): Polar PE-3000 recorder throughout
- Blood samples: jugular venipuncture, heparinised tubes, 30 min pre-exercise and immediately post-exercise (within 30 s)
- Blood lactate [La-]: YSI-1500 Sport analyser
- Packed cell volume (PCV): microhaematocrit centrifuge
- Haemolysis rate (HL): haemolysis in hypotonic 0.56% NaCl solution after washing erythrocytes with ice-cold Dulbecco's PBS
- Total Hp: single radial immunodiffusion (SRID) with anti-horse Hp serum; calibration: y = –0.1537 + 0.0143x, r = 0.9996 (P<0.001)
- Hb-binding Hp (1/2Hb-Hp and Hb-Hp fractions): 10% native polyacrylamide gel slab electrophoresis (pH 8.8), o-dianisidine staining for peroxidase activity; density measured by two-wave flying spot scanner (Shimadzu cs-9000); calibration: y = 0.00267 + 0.01189x, r = 0.9999 (P<0.001)

**Statistics:** Non-paired t-test for Group C vs. S comparisons; LSD for multiple comparisons; significance threshold P<0.05.

## Results (detailed — reproduce ALL numbers)

**Post-operative changes in total Hp (Table 1):**

| Time (months)     | Group C total Hp (g/l) | Group S total Hp (g/l) |
| ----------------- | ---------------------- | ---------------------- |
| –1 (pre-op)       | 0.88 ± 0.094           | 1.19 ± 0.11            |
| 0 (surgery)       | 0.94 ± 0.086           | 1.03 ± 0.10            |
| 0.25 (1st week)   | 2.53 ± 0.18            | 2.82 ± 0.26            |
| 2                 | 0.80 ± 0.093           | 0.18 ± 0.091           |
| 6 (exercise test) | 0.98 ± 0.092           | 0.20 ± 0.091           |

Both groups showed significantly elevated total Hp at 0.25 months (1 week post-op) due to haemolysis from surgery. Group C returned to pre-op levels by month 2; Group S fell to significantly lower levels (0.18–0.20 g/l vs. Group C 0.80–0.98 g/l, P<0.05, LSD). No significant difference between groups before and 1 week after surgery.

**Exercise test performance:**

- Maximum treadmill velocity: Group C 9.8 ± 0.48 m/s vs. Group S 8.1 ± 0.42 m/s (P<0.05)
- Running time during test: Group C 3.90 ± 0.24 min vs. Group S 4.33 ± 0.31 min
- Peak HR post-exercise: Group C 199.0 ± 1.8 bpm vs. Group S 216.3 ± 3.5 bpm (P<0.05; S horses worked harder for lower speed)
- Peak blood lactate [La-]: Group C 10.8 ± 1.8 mmol/l vs. Group S 11.7 ± 1.4 mmol/l (not significantly different)
- Post-exercise PCV: Group C 57.2 ± 0.64% vs. Group S 41.4 ± 0.56% (P<0.05; splenectomy removes erythrocyte reservoir)

**Exercise-induced changes in Hp and HL (Table 2):**

| Parameter           | Group C Before | Group C After | Group S Before | Group S After |
| ------------------- | -------------- | ------------- | -------------- | ------------- |
| HL (%)              | 52.7 ± 3.0     | 68.4 ± 11.5   | 12.0 ± 2.0     | 22.7 ± 6.3    |
| Total Hp (g/l)      | 0.98 ± 0.092   | 1.24 ± 0.12   | 0.20 ± 0.091   | 0.18 ± 0.093  |
| Hb-binding Hp (g/l) | 0.090 ± 0.081  | 0.51 ± 0.10   | 0.10 ± 0.082   | 0.13 ± 0.091  |

Group C: HL significantly higher than Group S both before and after exercise (P<0.05). Exercise significantly increased HL in both groups. Exercise significantly increased Hb-binding Hp in Group C (0.090 → 0.51 g/l, P<0.05); no significant change in Group S (0.10 → 0.13 g/l, NS). Total Hp did not change significantly with exercise in either group. After exercise, Hb-binding Hp in Group C was significantly higher than in Group S (0.51 vs. 0.13 g/l, P<0.05).

## Discussion & interpretation

The spleen plays a dual role: it accelerates red-cell membrane fragility through repeated sequestration cycles, and it provides an erythrocyte reservoir that is mobilised during exercise (PCV surge from ~35% resting to 57% post-exercise in intact horses vs. only 41% in splenectomised horses). This reserve is critical for aerobic performance — the 1.7 m/s speed difference between groups (9.8 vs. 8.1 m/s) arises primarily from this haematological difference. The higher post-exercise HR in Group S (216 vs. 199 bpm) at lower speed confirms these horses were working at a higher fraction of their capacity.

Hb-binding Hp (not total Hp) is the sensitive indicator of exercise-induced haemolysis because intravascular haemolysis releases Hb dimers (αβ) which are captured by Hp; the Hp-Hb complex does not raise total immunoreactive Hp appreciably in the short term, but does produce a visible PAGE band. Only when haemolysis is chronic does total Hp reflect the state of red-cell turnover (as seen in the chronic 6-month data post-splenectomy).

This paper confirms that in the absence of spleen function, horses lose both the osmotic-fragility "fitness signal" and the erythrocyte reserve that enables high-speed sustained effort. The mechanism underpins why recovery from illness affecting haematopoiesis, or conditions reducing splenic function, would predict below-expected performance.

## Limitations

- Very small n (3 per group); individual horse variation could dominate group statistics
- Horses conditioned by treadmill training, not actual race training; treadmill speeds ~8–10 m/s are below actual race speeds (~14–16 m/s for Thoroughbreds)
- Only male/female domestic Thoroughbreds at a yearling training farm — generalisation to race-day conditions requires extrapolation
- Time between blood sampling and exercise test meant only a 30-second post-exercise window; rapid splenic re-sequestration may have already begun
- Splenectomy is not a real-world racing variable; findings apply to understanding mechanism, not direct prediction

## Feature-engineering notes for the model

- `PCV_pre_race` — packed cell volume measured at training / pre-race blood screen — JRA veterinary records if available — higher PCV predicts greater O2 delivery; expected positive association with performance; likely confounded by race-day excitement (splenic contraction elevates PCV even at rest when stressed)
- `Hb_pre_race` — haemoglobin concentration (g/dl) — same source — higher Hb → higher O2-carrying capacity; correlates with PCV (r~0.85 typically)
- `days_since_last_race` — proxy for haematopoietic recovery — JRA race records (chakujun_date field) — should interact with race distance: short-rest + long-distance may deplete erythrocyte reserve; concave effect likely
- `blood_lactate_peak` — from incremental exercise tests at training centres — captures anaerobic threshold; not routinely available in JRA race records but available in research settings
- `Hb_binding_Hp` — Hb-binding haptoglobin (g/l) — research laboratory only — not derivable from standard race records; exclude from production pipeline
- **Caution:** Do NOT use splenectomy status as a feature — not recorded in race databases and would introduce selection bias

## Key references / follow-up leads

- Hanzawa K, Kubo K, Kai M, Hiraga A, Watanabe S. 1999. Effects of splenectomy for osmotic fragility of circulating red cells in Thoroughbred horses during exercise. _J. Equine Sci._ 10: 61–65. [foundational splenectomy + fragility paper]
- Hanzawa K, Orihara K, Kubo K, Hiraga A, Watanabe S. 2000. Changes of two-dimensional electrophoretic patterns of plasma with maximum exercises in young Thoroughbred horses. _Asian-Aus. J. Anim. Sci._ 13 Suppl. A: 152–155. [2D plasma electrophoresis post-maximal exercise]
- Hanzawa K, Orihara K, Kubo K, Yamanobe A, Hiraga A, Watanabe S. 1992. Changes of plasma amino acid and inorganic ion concentrations with maximum exercises in Thoroughbred young horses. _Jpn. J. Equine Sci._ 3: 157–162.
- Kent JE, Goodall J. 1991. Assessment of an immunoturbidimetric method for measuring equine serum haptoglobin concentrations. _Equine Vet. J._ 23: 59–66. [Hp as inflammation/haemolysis marker]
- Weight LM, Byrne MJ, Jacobs P. 1991. Haemolytic effects of exercise. _Clin. Sci._ 81: 147–152.
