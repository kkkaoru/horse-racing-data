# Prevalence of post-race exertional heat illness in Thoroughbred racehorses and climate conditions at racecourses in Japan

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 30(2): 17–23, 2019                                                                                                                                                                                                                                                                                                             |
| docid                          | `30_1901`                                                                                                                                                                                                                                                                                                                                     |
| Article type                   | Full Paper                                                                                                                                                                                                                                                                                                                                    |
| Authors                        | Motoi Nomura, Tomoki Shiose, Yuhiro Ishikawa, Fumiaki Mizobe, Satoshi Sakai, Kanichi Kusano                                                                                                                                                                                                                                                   |
| Affiliations                   | The Horse Racing School, Japan Racing Association, Chiba 270-1431, Japan; Equine Research Institute, Japan Racing Association, Fukushima 972-8325, Japan; Racehorse Clinic, Ritto Training Center, Japan Racing Association, Shiga 520-3085, Japan; Racehorse Clinic, Miho Training Center, Japan Racing Association, Ibaraki 300-0493, Japan |
| Received / Accepted / Released | January 21, 2019 / April 3, 2019 / 2019                                                                                                                                                                                                                                                                                                       |
| Keywords                       | exertional heat illness, heat stroke, racehorse, Thoroughbred, wet-bulb globe temperature                                                                                                                                                                                                                                                     |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/30/2/30_1901/_pdf/-char/en                                                                                                                                                                                                                                                                           |

## Abstract (verbatim)

> Despite growing recognition of post-race exertional heat illness (EHI) in the horse racing industry, reports on its prevalence are limited. The purpose of this study was to investigate the prevalence of post-race EHI and climate conditions at racecourses in Japan. The overall prevalence of EHI from 1999 to 2018 was 0.04% (387 cases for 975,247 starters) in races operated by the Japan Racing Association (JRA). The yearly prevalence has been increasing, exceeding 0.07% in the last four years of the studied period. The overall prevalence in summer (May–September) was 0.086% (352 cases for 409,908 starters). The monthly prevalence varied among the 10 JRA racecourses, which are distributed from latitude 34 to 43°N, ranging from no cases to 0.459%. During summer, prevalence of post-race EHI was high when the mean monthly ambient temperature was high at a racecourse. To evaluate climate conditions, we investigated the wet-bulb globe temperature (WBGT, °C) from 9 AM to 5 PM on sunny race days in July and August of 2017 and 2018 at three racecourses with a high prevalence of EHI among the 10 racecourses. The durations of time during which WBGT was between 28 and 33°C at these three courses were 95, 94, and 65% of the minutes measured, respectively. This result indicated that most races on the sunny summer days were held when WBGT was between 28 and 33°C at the three racecourses. These findings could be useful in developing the appropriate countermeasures to be taken during hot weather at each of the studied racecourses.

## Relevance to finishing-position (着順) prediction

Feature family: **E — environment/heat**. This paper provides the most comprehensive racecourse-level and temporal epidemiology of exertional heat illness (EHI) at all 10 JRA venues over 20 years. WBGT at race time is the strongest single environmental predictor of EHI risk at the racecourse level; it is also linked to physiological heat stress that impairs racing performance even without causing overt EHI.

For the finishing-position model, two environmental features are directly actionable:

1. **WBGT (continuous):** values between 28 and 33°C mark moderate-to-high EHI risk; values >33°C mark very high risk (but rare, only 2.5% of measured minutes at racecourse C). WBGT at race time can be obtained from JMA weather station data with a post-processing formula from standard temperature, humidity, and solar radiation.
2. **Mean monthly ambient temperature by racecourse:** a proxy when WBGT is unavailable; Table 2 in the paper provides the 20-year monthly means for all 10 venues. Racecourses G, H, I, J show July means of 27.0–28.7°C; racecourses A, B show July means of 20.5–21.1°C — a ~6°C gap that predicts ~4× difference in summer EHI prevalence.

The upward trend in EHI prevalence (0.030% in 1999–2014 vs. 0.080% in 2015–2018, P<0.05) suggests time-based climate drift should be included as an additional feature or model epoch.

EHI events are rare (absolute prevalence 0.04%) but DNF-adjacent: an affected horse may not complete the race or may perform far below true ability. Including racecourse × month × WBGT as an interaction feature in a DNF / performance degradation sub-model is recommended.

## Background & objective

Post-race EHI is an exercise-induced heat stroke resulting from core body temperature elevation in hot and humid racing environments. Inefficiency of evaporative cooling through sweating, combined with thermogenesis from intense exercise, can cause lethal physiological cascades. Despite growing concern in the Japanese racing industry (temperatures in Japan are rising faster than the global average; 1.19°C over the past 100 years), no prevalence data existed for JRA venues. The study aimed to (1) document 20-year EHI prevalence trends and (2) evaluate climate conditions at high-prevalence racecourses using WBGT.

## Materials & methods

**Data source:** Race and veterinary records for all 975,247 starters at 10 JRA racecourses from 1999 to 2018. Source: JARIS (Japan Racing Information System) digital database.

**EHI diagnosis criteria (clinical signs):** profuse sweating; rapid breathing (>60–100 breaths/min); elevated heart rate (>150 beats/min); unusual behaviour (head shaking, kicking out, pawing); gait abnormalities. JRA veterinarians stationed at track, saddling enclosures, and parade ring.

**WBGT measurement:** 15-minute interval readings from 9 AM to 5 PM on sunny race days (relative sunshine duration ≥40%) at racecourses C, G, and J (chosen as the three highest-prevalence courses based on preliminary survey to 2016). Measurement period: July–August 2017 and 2018. Instrument: WBGT-213B (Kyoto Electronics Manufacturing). Sensor height: approximately 120 cm above ground in a sunny location in the stable area.

**Ambient temperature:** 20-year monthly means from the Japan Meteorological Agency (JMA) weather station closest to each racecourse.

**Statistical analysis:** chi-squared test for differences in prevalence between two periods (1999–2014 vs. 2015–2018) and between summer and other seasons. P<0.05 threshold. Software: EZR (graphical interface for R v2.13.0).

**Sunny days included in WBGT analysis:** racecourse C: 6 of 11 measured July days; racecourse G: 9 of 11 July days; racecourse J: 14 of 16 July–August days.

## Results (detailed — reproduce ALL numbers)

### Overall EHI prevalence

- Total cases 1999–2018: 387 (337 horses once, 16 horses twice, 6 horses three times)
- Overall prevalence: 0.040% (387/975,247 starters)
- Period 1999–2014: 0.030%; period 2015–2018: 0.080% (P<0.05, significant increase)
- The yearly prevalence exceeded 0.07% every year from 2015 to 2018.
- Summer (May–September) prevalence: 0.086% (352/409,908 starters)
- Non-summer prevalence: 0.006% (P<0.05 vs. summer)

### Monthly prevalence by racecourse (Table 1, May–September 1999–2018)

| Racecourse | May % | Jun % | Jul %     | Aug % | Sep % | May–Sep % |
| ---------- | ----- | ----- | --------- | ----- | ----- | --------- |
| A          | —     | N.C.  | 0.104     | 0.033 | 0     | 0.024     |
| B          | —     | 0.043 | 0.053     | 0.019 | N.C.  | 0.045     |
| C          | 0     | 0.115 | 0.157     | N.C.  | N.C.  | 0.132     |
| D          | 0.028 | N.C.  | 0.022     | 0.095 | 0.087 | 0.063     |
| E          | 0.051 | 0.064 | N.C.      | —     | —     | 0.058     |
| F          | —     | N.C.  | N.C.      | —     | 0.079 | 0.079     |
| G          | 0.026 | 0.098 | **0.495** | —     | 0.082 | 0.210     |
| H          | 0.081 | 0.182 | 0.239     | —     | —     | 0.100     |
| I          | 0     | 0.154 | 0.045     | —     | 0.123 | 0.111     |
| J          | —     | —     | 0.138     | 0.121 | 0.058 | 0.116     |

N.C. = not calculated (total starters <1,000 in that month–racecourse combination); — = races not held.

Racecourses C and G–J reached ≥0.1% summer prevalence; A and B were <0.05%.

### Mean monthly ambient temperature (°C) at JMA observatory closest to each racecourse (Table 2, 20-year mean 1999–2018, selected summer months)

| Racecourse | Latitude group   | Jun  | Jul      | Aug      |
| ---------- | ---------------- | ---- | -------- | -------- |
| A          | Northern (~43°N) | 17.3 | 21.1     | 22.7     |
| B          | Northern         | 16.5 | 20.5     | 22.4     |
| C          | Mid-northern     | 21.0 | 24.6     | 25.6     |
| D          | Mid              | 21.1 | 25.1     | 26.8     |
| E          | Mid-southern     | 22.0 | 26.1     | 27.0     |
| F          | Mid-southern     | 21.6 | 25.6     | 26.7     |
| G          | Southern         | 23.1 | **27.2** | **28.4** |
| H          | Southern         | 23.4 | **27.5** | **28.5** |
| I          | Southern (~34°N) | 23.4 | **27.3** | **28.7** |
| J          | Southern         | 22.9 | **27.0** | **27.9** |

Full table also includes Jan–Dec; difference between northern (A, B) and southern (G–J) venues is approximately 6°C in summer.

### WBGT measurements at three high-prevalence racecourses (Table 3, sunny race days Jul–Aug 2017–2018)

| Statistic            | RC C (WBGT)  | RC C (Ambient) | RC G (WBGT)  | RC G (Ambient) | RC J (WBGT)  | RC J (Ambient) |
| -------------------- | ------------ | -------------- | ------------ | -------------- | ------------ | -------------- |
| Mean of daily means  | 30.4 ± 0.8°C | 33.8 ± 0.6°C   | 30.0 ± 0.7°C | 31.4 ± 1.9°C   | 28.4 ± 1.4°C | 31.2 ± 1.7°C   |
| Mean of daily maxima | 32.9 ± 0.8°C | 35.8 ± 0.9°C   | 31.9 ± 0.6°C | 33.0 ± 2.0°C   | 30.8 ± 1.3°C | 33.0 ± 1.9°C   |
| Mean of daily minima | 28.7 ± 1.0°C | 30.0 ± 0.7°C   | 27.8 ± 1.0°C | 29.1 ± 1.9°C   | 26.3 ± 1.6°C | 29.1 ± 1.6°C   |

### Distribution of WBGT ranges (% of total measured minutes 9 AM–5 PM) (Table 4)

| WBGT range | Racecourse C | Racecourse G | Racecourse J |
| ---------- | ------------ | ------------ | ------------ |
| <28°C      | 2.9%         | 6.0%         | 34.9%        |
| 28–33°C    | **94.6%**    | **94.0%**    | **65.1%**    |
| >33°C      | 2.5%         | 0.0%         | 0.0%         |

According to external hot weather policies (Australian, Northern Territory, Western Australia, NSW), WBGT 28–33°C = moderate-to-high risk; WBGT >33°C = very high risk.

## Discussion & interpretation

The prevalence increase from 0.030% (1999–2014) to 0.080% (2015–2018) is consistent with documented climate change in Japan (mean annual temperature +1.19°C over 100 years; increasing frequency of days with maximum temperature >35°C). Increased diagnostic awareness also likely contributed. The roughly 6°C ambient temperature difference between northern (A, B) and southern (G–J) venues explains the ~4× EHI prevalence gap. Racecourse G's exceptionally high July prevalence (0.495%) relative to racecourse D (0.022% in July) is unexplained by the available temperature data alone — other local factors (track orientation, wind patterns, humidity) may be relevant.

WBGT 28–33°C during 65–95% of racing time at the three high-prevalence venues confirms that moderate-to-high risk conditions are essentially constant during sunny summer race days, not just occasional spikes.

## Limitations

- WBGT measured at only 3 of 10 racecourses, only in July–August 2017–2018, and only on sunny days (≥40% sunshine duration). Cloudy and rainy days were excluded.
- EHI diagnosis relies on clinical observation by track veterinarians; subclinical heat stress reducing performance without overt EHI is not captured.
- The study does not test multivariate risk factors within races (e.g., individual horse characteristics, race distance, running position).
- Racecourse identities (A–J) are anonymised; mapping to specific JRA venues requires supplemental geographic information (Fig. 1: latitude 34–43°N map provided in paper).
- Only JRA racecourses studied; NAR (non-JRA) venues not included.

## Feature-engineering notes for the model

- `wbgt_race_time` — WBGT (°C) at the racecourse at the time of race (or at the nearest 15-min interval). Source: JMA AMeDAS data + WBGT calculation from dry bulb temperature, wet bulb temperature, and globe temperature; or from on-site sensors. Expected effect: positive curvilinear effect on DNF probability and negative effect on race pace relative to ability for WBGT >28°C. Threshold at 28°C and 33°C are interpretable cut points.
- `ambient_temp_race` — dry bulb ambient temperature (°C) at race time. Proxy for WBGT when globe temperature unavailable. Source: JMA observatory data.
- `racecourse_latitude` — encode latitude (approximately 34–43°N) or use racecourse fixed effects. Expected interaction: latitude × month × temperature.
- `ehi_prevalence_venue_month` — venue × month historical EHI rate (from this paper's Table 1). Informative Bayesian prior for EHI risk; values range 0–0.495%.
- `year_trend` — year as a continuous feature or post-2015 dummy to capture the documented 2015–2018 prevalence increase (climate-change drift adjustment).
- **Interaction:** `wbgt_race_time × racecourse_id` — because G's July prevalence is >3× that of D at similar temperatures, venue-specific WBGT effects are heterogeneous.
- **Do NOT use:** WBGT range classification (28–33 / >33) as a hard binary; use continuous WBGT instead, as the binary loses information across the 65-94% range that lies within the moderate-to-high bracket.

## Key references / follow-up leads

- Brownlow MA, Dart AJ, Jeffcott LB. 2016. Exertional heat illness: a review of the syndrome affecting racing Thoroughbreds in hot and humid climates. Aust. Vet. J. 94: 240–247. — comprehensive clinical review of EHI risk factors.
- Macdonald DM, Wheeler DP, Guthrie AJ. 2008. Post race distress syndrome in thoroughbred racing in South Africa. Proc. Int. Conf. Racing Analysts Vet. 17: 1–9. — only prior prevalence study at the time of publication.
- Budd GM. 2008. Wet-bulb globe temperature (WBGT)-its history and its limitations. J. Sci. Med. Sport 11: 20–32. — WBGT methodology.
- Japan Meteorological Agency climate change monitoring reports — source of temperature trend data.
