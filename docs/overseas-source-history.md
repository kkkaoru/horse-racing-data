# Overseas source histories

## Provenance

`oversea_horse_race_history` intentionally stores source-native rows from both `jra-van` and `netkeiba`. The same real-world race can therefore appear once per source. Consumers must select a source; they must not aggregate both sources as independent starts.

For the 2026 Prix Jacques le Marois viewer path, `getHorseRaceResults` explicitly selects `mapping.source = 'netkeiba'` for placeholder overseas runners. Runners with real JV registration numbers continue to use JV history. This prevents source duplication and duplication between supplemental and JV histories.

The viewer change has not been deployed. Deployment is deferred until after the 2026-08-15 race-day operations window.

## Scraping boundary

Source-specific table markers, route prefixes, and cell positions are supplied through an operator-owned ignored profile. They are not committed. Cached documents are reused, and requests for the target race were throttled.

The committed parser is profile-driven:

- horse results are stored in `oversea_horse_race_history` with `source = 'netkeiba'`;
- jockey, trainer, and owner results are stored in `oversea_person_race_history`;
- source mappings connect the target runners and people without inserting synthetic JV keys.

## 2026 Prix Jacques le Marois validation

The cached target documents produced:

| Entity   |        Pages | Rows |
| -------- | -----------: | ---: |
| Horses   |           10 |  104 |
| Jockeys  |           10 |  200 |
| Trainers |           10 |  176 |
| Owners   | 9 unique IDs |   78 |

Three owner pages were valid and explicitly reported `近走成績 (0件)`; no rows were fabricated for them. Four person-result rows did not publish a going value and retain `NULL`.

Cross-source horse validation matched 102 starts by mapped horse and date:

- finish position: 102/102 matched;
- distance: 97/102 matched;
- five distance differences reflect source distance conventions: 1630/1600, 1490/1500, 1450/1400 twice, and 1220/1200;
- two apparent unmatched rows were the same race with a one-day source date difference.

For all five distance differences, JRA-VAN retained a ten-metre conversion (1630, 1490, 1450, 1450, 1220), while netkeiba used a hundred-metre value (1600, 1500, 1400, 1400, 1200). These differences remain source-specific. They are not silently normalized or merged.

JRA-VAN contained 13 starts that netkeiba did not publish for these profiles:

- More Thunder: five 2024 maiden/condition/handicap starts;
- No Lunch: two condition races, two listed races, and the 2026 Prix du Muguet (G2);
- Sir Tommy Cen: two 2024 maiden/condition starts;
- Precise: one 2025 Breeders' Cup G1 cancellation.

The missing set is not limited to lower-class races: it includes listed, G2, and G1-cancellation records. There was no result-page pagination marker, and each netkeiba profile's published career total matched the parsed row count. The difference is therefore a source coverage limitation, not a missed second page.

Allowing a one-day date difference matches all 104 netkeiba horse rows to JRA-VAN with identical finishes. Two rows require that tolerance:

- Strauss is dated 2025-10-31 in the netkeiba horse table and 2025-11-01 in JRA-VAN. The netkeiba race page itself says 2025-11-01.
- More Thunder's Prix de la Forêt is dated 2025-10-06 in the netkeiba horse table and 2025-10-05 in JRA-VAN. Both source race pages identify the local date as 2025-10-05; JRA-VAN separately publishes a 2025-10-06 00:25 JST start.

Thus a profile date is not a stable cross-source race identity, and the normalization direction differs by record. Cross-source consumers must not use exact date alone as a join key.

## Person-result value

Of the stored recent rows, source race identifiers classified the following as overseas rather than domestic JV-shaped races:

- jockeys: 174/200 rows across 9 people;
- trainers: 136/176 rows across 8 people;
- owners: 58/78 rows across 5 owners.

This is the information not reliably available from domestic JV histories, including all 20 stored results for C. Lee, who has no resolved JV jockey code in the target race.
